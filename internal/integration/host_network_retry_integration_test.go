//go:build integration && hostnetwork

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestHostNetworkMonitorPortConflictRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	const address = "127.0.0.1"
	var blocker testcontainers.Container
	var candidates []int
	var createdIDs []string
	var failedState *container.State
	var failedLogs []byte
	var captureErr error
	customize := testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
		port, err := strconv.Atoi(req.Env["CEPH_MON_PORT_V2"])
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("MON request has invalid v2 port %q", req.Env["CEPH_MON_PORT_V2"])
		}
		candidates = append(candidates, port)
		req.LifecycleHooks = append(req.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
			PostCreates: []testcontainers.ContainerHook{func(_ context.Context, ctr testcontainers.Container) error {
				createdIDs = append(createdIDs, ctr.GetContainerID())
				return nil
			}},
		})
		if len(candidates) != 1 {
			return nil
		}
		// The allocator still owns this port. Start without a readiness wait so
		// the customizer can return and let bootstrap release its port lease.
		blocker, err = testcontainers.Run(ctx, image,
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) { hc.NetworkMode = "host" }),
			testcontainers.WithEntrypoint("python3"),
			testcontainers.WithCmd("-u", "-c", hostNetworkPortBlockerScript, address, strconv.Itoa(port)),
		)
		if blocker != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				if err := blocker.Terminate(cleanupCtx); err != nil {
					t.Errorf("terminate MON port blocker: %v", err)
				}
			})
		}
		if err != nil {
			return fmt.Errorf("start MON port blocker: %w", err)
		}
		// A TCP handshake makes the first MON wait for real socket ownership,
		// then executes the unchanged bootstrap script while the port is held.
		req.Entrypoint = []string{"python3", "-u", "-c", hostNetworkBlockedMonitorScript, address, strconv.Itoa(port)}
		req.Cmd = nil
		req.LifecycleHooks = append(req.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
			PreTerminates: []testcontainers.ContainerHook{func(hookCtx context.Context, ctr testcontainers.Container) error {
				failedState, captureErr = ctr.State(hookCtx)
				if captureErr != nil {
					return nil
				}
				logs, err := ctr.Logs(hookCtx)
				if err != nil {
					captureErr = err
					return nil
				}
				defer logs.Close()
				failedLogs, captureErr = io.ReadAll(logs)
				return nil
			}},
		})
		return nil
	})
	opts = append(opts, ceph.WithHostNetwork(), ceph.WithHostAddress(address),
		ceph.WithOSDCount(1), ceph.WithStartupTimeout(3*time.Minute), customize)
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		hostNetworkCleanupCluster(t, cluster)
	}
	if err != nil {
		t.Fatalf("bootstrap after actual MON port conflict: %v; failed MON logs:\n%s", err, failedLogs)
	}
	if len(candidates) != 2 || len(createdIDs) != 2 || createdIDs[0] == createdIDs[1] {
		t.Fatalf("expected two fresh MON attempts, got candidates=%v containers=%v", candidates, createdIDs)
	}
	if captureErr != nil || failedState == nil || failedState.Running || failedState.ExitCode == 0 {
		t.Fatalf("first MON did not exit unsuccessfully: state=%+v capture=%v", failedState, captureErr)
	}
	if !strings.Contains(strings.ToLower(string(failedLogs)), "address already in use") {
		t.Fatalf("first MON did not report the real bind failure:\n%s", failedLogs)
	}
	if err := wait.ForLog("TC_MON_PORT_BLOCKED").WithStartupTimeout(5*time.Second).WaitUntilReady(ctx, blocker); err != nil {
		t.Fatalf("blocker never acquired the first MON port: %v", err)
	}
	// Independently prove that the first candidate remains bound in the
	// Docker host namespace, rather than relying only on diagnostic strings.
	execCommand(t, ctx, blocker, "python3", "-c", `import errno, socket, sys
s = socket.socket()
try:
    s.bind((sys.argv[1], int(sys.argv[2])))
except OSError as e:
    assert e.errno == errno.EADDRINUSE, e
else:
    raise AssertionError("blocker did not retain the MON port")
finally:
    s.close()
`, address, strconv.Itoa(candidates[0]))
	config, keyring, err := cluster.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	finalEndpoint := net.JoinHostPort(address, strconv.Itoa(candidates[1]))
	if candidates[0] == candidates[1] || !strings.Contains(string(config), "v2:"+finalEndpoint+",") {
		t.Fatalf("connection config does not contain the retried MON port: candidates=%v config=%s", candidates, config)
	}
	data, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var monmap struct {
		Mons []struct {
			PublicAddrs struct {
				Addrvec []struct {
					Type string `json:"type"`
					Addr string `json:"addr"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"mons"`
	}
	if err := json.Unmarshal(data, &monmap); err != nil {
		t.Fatal(err)
	}
	if len(monmap.Mons) != 1 {
		t.Fatalf("expected one MON in final monmap: %s", data)
	}
	foundV2 := false
	for _, addr := range monmap.Mons[0].PublicAddrs.Addrvec {
		if addr.Type == "v2" {
			endpoint, _, _ := strings.Cut(addr.Addr, "/")
			if endpoint != finalEndpoint {
				t.Fatalf("monmap retained wrong v2 endpoint %q, expected %q", endpoint, finalEndpoint)
			}
			foundV2 = true
		}
	}
	if !foundV2 {
		t.Fatalf("final monmap lacks a v2 endpoint: %s", data)
	}
	status, err := cluster.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.MgrMap.Available || status.OSDMap.NumOSDs != 1 || status.OSDMap.NumUpOSDs != 1 || status.OSDMap.NumInOSDs != 1 {
		t.Fatalf("retried cluster is not ready: %+v", status)
	}
	// Reuse the blocker as an independent native librados client. It keeps
	// holding the failed candidate throughout the successful connection.
	if err := blocker.CopyToContainer(ctx, config, "/tmp/retry-ceph.conf", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := blocker.CopyToContainer(ctx, keyring, "/tmp/retry-admin.keyring", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, blocker, "python3", "-c", `import json, rados, sys
with rados.Rados(conffile="/tmp/retry-ceph.conf", conf={"keyring": "/tmp/retry-admin.keyring"}) as cluster:
    code, output, error = cluster.mon_command(json.dumps({"prefix": "status", "format": "json"}), b"")
    assert code == 0, (code, error)
    status = json.loads(output)
    assert status["fsid"] == sys.argv[1], status
    assert status["mgrmap"]["available"], status
    assert status["osdmap"]["num_up_osds"] == status["osdmap"]["num_in_osds"] == 1, status
`, status.FSID)
	t.Logf("actual EADDRINUSE on %s:%d caused two fresh MON attempts; config, monmap and native status use %s", address, candidates[0], finalEndpoint)
}

const hostNetworkPortBlockerScript = `import errno, signal, socket, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
address, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
deadline = time.monotonic() + 60
while True:
    try:
        s.bind((address, port))
        break
    except OSError as e:
        if e.errno != errno.EADDRINUSE or time.monotonic() >= deadline:
            raise
        time.sleep(0.01)
s.listen(8)
print("TC_MON_PORT_BLOCKED", flush=True)
while True:
    connection, _ = s.accept()
    with connection:
        try:
            connection.sendall(b"TC_MON_PORT_BLOCKED\n")
        except OSError:
            pass
`

const hostNetworkBlockedMonitorScript = `import os, socket, sys, time
address, port = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        with socket.create_connection((address, port), timeout=0.5) as connection:
            if connection.makefile("rb").readline(64) == b"TC_MON_PORT_BLOCKED\n":
                os.execv("/bin/sh", ["/bin/sh", "/tc/mon.sh"])
    except OSError:
        pass
    time.sleep(0.01)
raise RuntimeError("MON port blocker did not acquire the candidate")
`
