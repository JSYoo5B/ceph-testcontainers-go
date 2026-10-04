//go:build integration && topology

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMonitorManagerTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithStartupTimeout(3*time.Minute))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, err := ceph.Run(ctx, image, opts...)
			if cluster != nil {
				testcontainers.CleanupContainer(t, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(cluster.Monitors()) != 3 || len(cluster.Managers()) != 2 || cluster.ControlContainer().GetContainerID() == cluster.GetContainerID() {
				t.Fatal("HA fixture lacks separate control or daemon counts")
			}
			config, _, err := cluster.ConnectionConfig()
			if err != nil || strings.Count(string(config), "[v2:") != 3 {
				t.Fatalf("fresh client lacks three monitor addrvecs: %v: %s", err, config)
			}
			cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-topology", "8")
			cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-topology", "rados")
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
				testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
			if client != nil {
				testcontainers.CleanupContainer(t, client)
			}
			if err != nil {
				t.Fatal(err)
			}
			session := startTopologyRadosSession(t, ctx, client)
			topologyFreshConnection(t, ctx, client, false)
			monitors := cluster.Monitors()
			primary := monitors[0]
			stop := 3 * time.Second
			if err := primary.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 2
			})
			if err := cluster.WaitForQuorum(ctx); err != nil {
				t.Fatal(err)
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "one-monitor-down")
			t.Log("MON a stopped: CLI control, the retained native librados session, and fresh authenticated I/O survived on quorum b,c")
			if err := primary.Start(ctx); err != nil {
				t.Fatal(err)
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			session.check(t, ctx, client, "one-monitor-restarted")
			// Two stopped members remove the majority. Inspect the surviving
			// daemon's local admin socket: a quorum-dependent CLI would block.
			for _, mon := range monitors[:2] {
				if err := mon.Stop(ctx, &stop); err != nil {
					t.Fatal(err)
				}
			}
			topologyWait(t, ctx, func() bool {
				data := topologyExecOutput(t, ctx, monitors[2], "ceph", "--admin-daemon", "/var/run/ceph/ceph-mon."+monitors[2].DaemonName+".asok", "mon_status")
				var status struct {
					State  string `json:"state"`
					Quorum []int  `json:"quorum"`
				}
				if err := json.Unmarshal(data, &status); err != nil || status.Quorum == nil {
					t.Fatalf("decode surviving MON's local status: %v: %s", err, data)
				}
				return len(status.Quorum) == 0 && status.State != "leader" && status.State != "peon"
			})
			topologyFreshConnection(t, ctx, client, true)
			t.Log("Two of three MONs stopped: the surviving daemon has no quorum and a fresh native session cannot authenticate within the bounded probe; retained sessions are left idle")
			for _, mon := range monitors[:2] {
				if err := mon.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			topologyWait(t, ctx, func() bool {
				q, err := cluster.QuorumStatus(ctx)
				return err == nil && len(q.QuorumNames) == 3
			})
			topologyFreshConnection(t, ctx, client, false)
			session.check(t, ctx, client, "quorum-recovered")
			if _, err := cluster.AddMonitor(ctx, "replacement"); err != nil {
				t.Fatal(err)
			}
			if err := cluster.RemoveMonitor(ctx, "a"); err != nil {
				t.Fatal(err)
			}
			if len(cluster.Monitors()) != 3 {
				t.Fatal("replacement did not preserve three owned monitors")
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "monitor-replaced")
			t.Log("MON replacement: 3 -> 4 -> 3, primary a removed, both retained and fresh client sessions retained data")
			cephCommand(t, ctx, cluster, "mgr", "module", "enable", "volumes")
			fs, err := cluster.StartCephFS(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cephCommand(t, ctx, cluster, "fs", "subvolumegroup", "create", fs.FilesystemName, "before-failover")
			mgrStatus, err := cluster.ManagerStatus(ctx)
			if err != nil {
				t.Fatal(err)
			}
			previous := mgrStatus.ActiveName
			for _, mgr := range cluster.Managers() {
				if mgr.DaemonName == previous {
					if err := mgr.Stop(ctx, &stop); err != nil {
						t.Fatal(err)
					}
				}
			}
			topologyWait(t, ctx, func() bool {
				status, err := cluster.ManagerStatus(ctx)
				return err == nil && status.Available && status.ActiveName != previous
			})
			cephCommand(t, ctx, cluster, "fs", "subvolumegroup", "create", fs.FilesystemName, "after-failover")
			data, err := cluster.Ceph(ctx, "fs", "subvolumegroup", "ls", fs.FilesystemName, "--format", "json")
			var groups []struct{ Name string }
			if err != nil || json.Unmarshal(data, &groups) != nil || len(groups) != 2 {
				t.Fatalf("MGR volumes operation lost state after failover: %v: %s", err, data)
			}
			topologyRados(t, ctx, client, "verify")
			session.check(t, ctx, client, "manager-failed-over")
			session.finish(t, ctx, client)
			t.Log("MGR failover: active identity changed, native volumes command created and listed durable subvolume groups")
		})
	}
}

func topologyWait(t *testing.T, parent context.Context, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatalf("topology transition: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func topologyRados(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	topologyExecOutput(t, ctx, client, "python3", "-c", `import rados, sys
payload = bytes(range(256)) * 256
with rados.Rados(conffile="/etc/ceph/ceph.conf", name="client.admin", conf={"client_mount_timeout": "10", "rados_mon_op_timeout": "10", "rados_osd_op_timeout": "20"}) as cluster:
    with cluster.open_ioctx("tc-topology") as io:
        if sys.argv[1] == "seed":
            io.write_full("retained", payload)
        assert io.read("retained", len(payload)) == payload
        io.write_full("fresh", payload[::-1])
        assert io.read("fresh", len(payload)) == payload[::-1]
print("fresh native librados authenticated I/O passed")`, phase)
	t.Log(fmt.Sprintf("native RADOS topology %s passed", phase))
}

// Every native process and CLI probe has a container-side watchdog as well as
// the Docker request deadline. Cancelling Exec alone does not kill a C call.
func topologyExecOutput(t *testing.T, parent context.Context, ctr testcontainers.Container, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 50*time.Second)
	defer cancel()
	command := topologyCommandWithTimeout(45*time.Second, args...)
	code, reader, err := ctr.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("bounded topology probe exited %d: %v: %s", code, err, output)
	}
	return output
}

func topologyCommandWithTimeout(timeout time.Duration, args ...string) []string {
	return append([]string{"python3", "-c", topologyCommandTimeoutScript, strconv.FormatFloat(timeout.Seconds(), 'f', -1, 64)}, args...)
}

// subprocess.run kills and waits for the direct native command on timeout.
// Preserve its output and exit status without a shell or an external watchdog.
const topologyCommandTimeoutScript = `import subprocess, sys
try:
    result = subprocess.run(sys.argv[2:], timeout=float(sys.argv[1]), check=False)
except subprocess.TimeoutExpired:
    print('native command exceeded ' + sys.argv[1] + '-second watchdog', file=sys.stderr)
    sys.exit(124)
except OSError as error:
    print('native command launch failed: ' + type(error).__name__, file=sys.stderr)
    sys.exit(127)
sys.exit(result.returncode if result.returncode >= 0 else 128 - result.returncode)
`

type topologySession struct {
	PID        int    `json:"pid"`
	Connection uint64 `json:"connection"`
	Token      string `json:"token"`
	Sequence   int    `json:"sequence"`
	Phase      string `json:"phase"`
	SHA256     string `json:"sha256"`
	finished   bool
}

const topologySessionDirectory = "/tmp/tc-topology-session"

func startTopologyRadosSession(t *testing.T, ctx context.Context, client testcontainers.Container) *topologySession {
	t.Helper()
	token := strconv.FormatInt(time.Now().UnixNano(), 10)
	data := topologyExecOutput(t, ctx, client, "python3", "-c", topologySessionLaunchScript, topologySessionDirectory, token, topologySessionWorkerScript)
	var launched struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(data, &launched); err != nil || launched.PID <= 1 {
		t.Fatalf("background librados process did not start: %v: %s", err, data)
	}
	session := &topologySession{PID: launched.PID, Token: token}
	t.Cleanup(func() {
		if session.finished {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// Check the per-session token in cmdline before killing an owned PID.
		code, reader, err := client.Exec(cleanupCtx, topologyCommandWithTimeout(10*time.Second, "python3", "-c", `import os, signal, sys
pid, token = int(sys.argv[1]), sys.argv[2].encode()
try:
    command = open('/proc/%d/cmdline' % pid, 'rb').read().split(b'\0')
    if token in command:
        os.kill(pid, signal.SIGKILL)
except ProcessLookupError:
    pass
except FileNotFoundError:
    pass`, strconv.Itoa(session.PID), token), tcexec.Multiplexed())
		if err == nil {
			_, err = io.ReadAll(reader)
		}
		if err != nil || code != 0 {
			t.Errorf("stop retained librados process %d: exit=%d error=%v", session.PID, code, err)
		}
	})
	receipt := session.await(t, ctx, client, 0)
	if receipt.Connection == 0 || receipt.Phase != "ready" {
		t.Fatalf("retained librados ready handshake is incomplete: %+v", receipt)
	}
	session.Connection = receipt.Connection
	t.Logf("retained native librados ready: PID=%d connection=%d token=%s", session.PID, session.Connection, session.Token)
	return session
}

func (session *topologySession) await(t *testing.T, ctx context.Context, client testcontainers.Container, sequence int) topologySession {
	t.Helper()
	data := topologyExecOutput(t, ctx, client, "python3", "-c", topologySessionAwaitScript, topologySessionDirectory, strconv.Itoa(sequence), strconv.Itoa(session.PID))
	var receipt topologySession
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatalf("decode retained librados acknowledgement: %v: %s", err, data)
	}
	payload := make([]byte, 256*256)
	for index := range payload {
		payload[index] = byte(index)
	}
	expectedSHA := fmt.Sprintf("%x", sha256.Sum256(payload))
	if receipt.PID != session.PID || receipt.Token != session.Token || receipt.Sequence != sequence || receipt.SHA256 != expectedSHA || (session.Connection != 0 && receipt.Connection != session.Connection) {
		t.Fatalf("retained connection or bytes changed: expected PID=%d connection=%d sequence=%d sha256=%s; got %+v", session.PID, session.Connection, sequence, expectedSHA, receipt)
	}
	return receipt
}

func (session *topologySession) request(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) topologySession {
	t.Helper()
	sequence := session.Sequence + 1
	data, err := json.Marshal(map[string]any{"sequence": sequence, "phase": phase, "token": session.Token})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyToContainer(ctx, data, fmt.Sprintf("%s/request-%d.json", topologySessionDirectory, sequence), 0o600); err != nil {
		t.Fatal(err)
	}
	receipt := session.await(t, ctx, client, sequence)
	if receipt.Phase != phase {
		t.Fatalf("retained librados acknowledged the wrong phase: %+v", receipt)
	}
	session.Sequence = sequence
	return receipt
}

func (session *topologySession) check(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	receipt := session.request(t, ctx, client, phase)
	t.Logf("same native librados session: PID=%d connection=%d phase=%s sequence=%d sha256=%s", receipt.PID, receipt.Connection, receipt.Phase, receipt.Sequence, receipt.SHA256)
}

func (session *topologySession) finish(t *testing.T, ctx context.Context, client testcontainers.Container) {
	t.Helper()
	session.request(t, ctx, client, "finished")
	session.finished = true
}

func topologyFreshConnection(t *testing.T, ctx context.Context, client testcontainers.Container, denied bool) {
	t.Helper()
	want := "connected"
	if denied {
		want = "denied"
	}
	data := topologyExecOutput(t, ctx, client, "python3", "-c", topologyFreshProbeScript, want)
	var result struct {
		State string `json:"state"`
		PID   int    `json:"pid"`
		Cause string `json:"cause"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.State != want || result.PID <= 1 {
		t.Fatalf("fresh authentication probe did not report %s: %v: %s", want, err, data)
	}
	t.Logf("fresh native librados authentication: PID=%d state=%s cause=%s", result.PID, result.State, result.Cause)
}

const topologySessionLaunchScript = `import json, os, subprocess, sys
directory, token, worker = sys.argv[1:]
os.mkdir(directory, 0o700)
with open(directory + '/worker.log', 'wb') as log:
    process = subprocess.Popen([sys.executable, '-u', '-c', worker, directory, token],
                               stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
print(json.dumps({'pid': process.pid}))
`

const topologySessionWorkerScript = `import hashlib, json, os, rados, sys, time
directory, token = sys.argv[1:]
payload = bytes(range(256)) * 256
pid = os.getpid()
connection = rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin',
                         conf={'client_mount_timeout': '10', 'rados_mon_op_timeout': '10', 'rados_osd_op_timeout': '20'})
connection_id = id(connection)
def publish(filename, result):
    temporary = directory + '/' + filename + '.tmp'
    with open(temporary, 'w') as output:
        json.dump(result, output)
    os.replace(temporary, directory + '/' + filename)
def acknowledge(sequence, phase):
    publish('ack-%d.json' % sequence, {'pid': pid, 'connection': connection_id, 'token': token,
            'sequence': sequence, 'phase': phase, 'sha256': hashlib.sha256(payload).hexdigest()})
try:
    connection.connect()
    with connection.open_ioctx('tc-topology') as io:
        io.write_full('retained', payload)
        assert io.read('retained', len(payload) + 1) == payload
        acknowledge(0, 'ready')
        sequence = 1
        while True:
            path = directory + '/request-%d.json' % sequence
            try:
                with open(path) as request:
                    command = json.load(request)
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.1)
                continue
            assert command['sequence'] == sequence and command['token'] == token
            assert id(connection) == connection_id and os.getpid() == pid
            if command['phase'] == 'finished':
                break
            assert io.read('retained', len(payload) + 1) == payload, 'retained binary payload differs'
            update = payload[::-1] + command['phase'].encode()
            name = 'session-%d' % sequence
            io.write_full(name, update)
            assert io.read(name, len(update) + 1) == update, 'retained-session write/read differs'
            acknowledge(sequence, command['phase'])
            sequence += 1
    connection.shutdown()
    acknowledge(sequence, 'finished')
except BaseException as error:
    publish('error.json', {'pid': pid, 'error': type(error).__name__ + ': ' + str(error)})
    raise
`

const topologySessionAwaitScript = `import json, os, sys, time
directory, sequence, pid = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
deadline = time.monotonic() + 40
while time.monotonic() < deadline:
    path = directory + '/ack-%d.json' % sequence
    if os.path.exists(path):
        print(open(path).read())
        break
    error = directory + '/error.json'
    if os.path.exists(error):
        raise RuntimeError(open(error).read())
    os.kill(pid, 0)
    time.sleep(0.1)
else:
    raise RuntimeError('retained librados process did not acknowledge sequence %d' % sequence)
`

const topologyFreshProbeScript = `import json, subprocess, sys
expected = sys.argv[1]
worker = r'''import json, os, rados
connection = rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin',
                         conf={'client_mount_timeout': '10', 'rados_mon_op_timeout': '10', 'rados_osd_op_timeout': '20'})
print(json.dumps({'state': 'connecting', 'pid': os.getpid()}), flush=True)
try:
    connection.connect()
except rados.Error as error:
    print(json.dumps({'state': 'denied', 'pid': os.getpid(), 'cause': type(error).__name__}), flush=True)
else:
    payload = bytes(range(256)) * 256
    with connection.open_ioctx('tc-topology') as io:
        assert io.read('retained', len(payload) + 1) == payload
    print(json.dumps({'state': 'connected', 'pid': os.getpid(), 'cause': 'authenticated native read'}), flush=True)
    connection.shutdown()
'''
try:
    result = subprocess.run([sys.executable, '-u', '-c', worker], capture_output=True, text=True, timeout=25)
except subprocess.TimeoutExpired as error:
    output = error.stdout.decode() if isinstance(error.stdout, bytes) else error.stdout
    events = [json.loads(line) for line in (output or '').splitlines()]
    assert expected == 'denied' and len(events) == 1 and events[0]['state'] == 'connecting', 'fresh process stalled outside authentication'
    print(json.dumps({'state': 'denied', 'pid': events[0]['pid'], 'cause': 'fresh connect exceeded 25-second watchdog'}))
else:
    assert result.returncode == 0, 'fresh native probe failed: ' + result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(events) == 2 and events[0]['state'] == 'connecting' and events[-1]['state'] == expected, 'fresh native probe reported: ' + result.stdout
    print(json.dumps(events[-1]))
`
