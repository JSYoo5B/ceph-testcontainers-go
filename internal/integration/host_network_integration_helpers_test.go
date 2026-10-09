//go:build all || (integration && hostnetwork)

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func hostNetworkRunPair(t *testing.T, ctx context.Context, image string, imageOpts []testcontainers.ContainerCustomizer) []*ceph.Container {
	t.Helper()
	type result struct {
		index   int
		cluster *ceph.Container
		err     error
	}
	results := make(chan result, 2)
	for i := range 2 {
		go func() {
			opts := append([]testcontainers.ContainerCustomizer(nil), imageOpts...)
			opts = append(opts, ceph.WithHostNetwork(), ceph.WithHostAddress("127.0.0.1"),
				ceph.WithOSDCount(2), ceph.WithStartupTimeout(5*time.Minute))
			cluster, err := ceph.Run(ctx, image, opts...)
			results <- result{index: i, cluster: cluster, err: err}
		}()
	}
	clusters := make([]*ceph.Container, 2)
	var failures []string
	for range 2 {
		r := <-results
		clusters[r.index] = r.cluster
		if r.cluster != nil {
			hostNetworkCleanupCluster(t, r.cluster)
		}
		if r.err != nil {
			failures = append(failures, fmt.Sprintf("cluster %d: %v", r.index, r.err))
		}
	}
	if len(failures) != 0 {
		t.Fatalf("concurrent host network bootstrap failed: %s", strings.Join(failures, "; "))
	}
	return clusters
}

func hostNetworkS3Request(t *testing.T, ctx context.Context, client testcontainers.Container, gateway s3HTTPClient, method, path string, payload []byte, wantStatus int) []byte {
	t.Helper()
	input, err := hostNetworkS3RequestInput(ctx, gateway, method, path, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyToContainer(ctx, input, "/tmp/host-s3-request.json", 0o600); err != nil {
		t.Fatal(err)
	}
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", hostNetworkHTTPScript, "/tmp/host-s3-request.json"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("VM host namespace HTTP probe exited %d: %s", code, output)
	}
	var result struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode VM HTTP response: %v: %s", err, output)
	}
	body, err := base64.StdEncoding.DecodeString(result.Body)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != wantStatus {
		t.Fatalf("VM HTTP %s %s returned %d, want %d: %s", method, path, result.Status, wantStatus, body)
	}
	return body
}

func hostNetworkS3RequestInput(ctx context.Context, gateway s3HTTPClient, method, path string, payload []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, gateway.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	gateway.sign(request, payload, time.Now().UTC())
	return json.Marshal(struct {
		URL     string      `json:"url"`
		Method  string      `json:"method"`
		Headers http.Header `json:"headers"`
		Payload string      `json:"payload"`
	}{request.URL.String(), method, request.Header, base64.StdEncoding.EncodeToString(payload)})
}

// urllib adds an unsigned Content-Type to PUT requests, including an empty
// create-bucket body. Ceph rejects that request because SigV4 requires any
// supplied Content-Type to be signed. Send the Go signer's request directly.
const hostNetworkHTTPScript = `import base64, http.client, json, sys, urllib.parse
with open(sys.argv[1]) as source:
    spec = json.load(source)
payload = base64.b64decode(spec["payload"])
headers = {key: ", ".join(values) for key, values in spec["headers"].items()}
url = urllib.parse.urlsplit(spec["url"])
if url.scheme not in ("http", "https"):
    raise ValueError("unsupported HTTP scheme: " + url.scheme)
connection_type = http.client.HTTPConnection if url.scheme == "http" else http.client.HTTPSConnection
connection = connection_type(url.hostname, url.port, timeout=30)
headers["Host"] = url.netloc
target = url.path or "/"
if url.query:
    target += "?" + url.query
try:
    connection.request(spec["method"], target, body=payload if payload or spec["method"] == "PUT" else None,
                       headers=headers)
    response = connection.getresponse()
    print(json.dumps({"status": response.status, "body": base64.b64encode(response.read()).decode("ascii")}))
finally:
    connection.close()
`

func hostNetworkCleanupCluster(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if t.Failed() {
			ctrs := append([]testcontainers.Container{cluster.Container, cluster.ManagerContainer()}, cluster.ServiceContainers()...)
			ctrs = append(ctrs, osdContainers(cluster)...)
			for _, ctr := range ctrs {
				if ctr == nil {
					continue
				}
				logs, err := ctr.Logs(ctx)
				if err != nil {
					continue
				}
				data, _ := io.ReadAll(logs)
				logs.Close()
				if len(data) > 8000 {
					data = data[len(data)-8000:]
				}
				t.Logf("container %s logs:\n%s", ctr.GetContainerID(), data)
			}
		}
		if err := cluster.Terminate(ctx); err != nil {
			t.Errorf("terminate host network cluster: %v", err)
		}
	})
}

type hostNetworkDaemonEndpoints struct {
	cluster   int
	daemon    string
	addresses []string
}

func hostNetworkAssertDistinctAddresses(t *testing.T, ctx context.Context, clusters []*ceph.Container) []hostNetworkDaemonEndpoints {
	t.Helper()
	seen := make(map[string]int)
	var groups []hostNetworkDaemonEndpoints
	for i, cluster := range clusters {
		for _, daemon := range []string{"mon", "mgr", "osd"} {
			data, err := cluster.Ceph(ctx, daemon, "dump", "--format", "json")
			if err != nil {
				t.Fatal(err)
			}
			var dump any
			if err := json.Unmarshal(data, &dump); err != nil {
				t.Fatal(err)
			}
			endpoints, err := hostNetworkNormalizeEndpoints(dump, cluster.PublicAddress())
			if err != nil {
				t.Fatalf("cluster %d %s advertised endpoints: %v", i, daemon, err)
			}
			if len(endpoints) == 0 {
				t.Fatalf("cluster %d %s dump does not advertise any %s endpoints: %s", i, daemon, cluster.PublicAddress(), data)
			}
			for _, endpoint := range endpoints {
				if owner, exists := seen[endpoint]; exists && owner != i {
					t.Fatalf("clusters %d and %d advertise the same endpoint %s", owner, i, endpoint)
				}
				seen[endpoint] = i
			}
			groups = append(groups, hostNetworkDaemonEndpoints{cluster: i, daemon: daemon, addresses: endpoints})
			t.Logf("cluster %d %s endpoints: %v", i, daemon, endpoints)
		}
	}
	if os.Getenv("CEPH_TEST_HOST_TCP_REQUIRED") == "1" {
		hostNetworkAssertRunnerTCP(t, ctx, groups)
	}
	return groups
}

// Dump fields differ between Ceph versions. Normalize actual IP/port pairs
// across address vectors and legacy fields rather than nonce-bearing strings.
func hostNetworkNormalizeEndpoints(dump any, publicAddress string) ([]string, error) {
	prefix := net.JoinHostPort(publicAddress, "")
	pattern := regexp.MustCompile(`(?:v[12]:)?(` + regexp.QuoteMeta(prefix) + `[0-9]+)(?:/[0-9]+)?`)
	addresses := make(map[string]bool)
	var visit func(any) error
	visit = func(value any) error {
		switch value := value.(type) {
		case string:
			for _, match := range pattern.FindAllStringSubmatch(value, -1) {
				_, port, err := net.SplitHostPort(match[1])
				if err != nil {
					return fmt.Errorf("invalid endpoint %q: %w", match[1], err)
				}
				number, err := strconv.Atoi(port)
				if err != nil || number > 65535 {
					return fmt.Errorf("invalid endpoint port %q", match[1])
				}
				// MGR can retain a legacy :0 sentinel alongside real vectors.
				if number == 0 {
					continue
				}
				addresses[match[1]] = true
			}
		case []any:
			for _, child := range value {
				if err := visit(child); err != nil {
					return err
				}
			}
		case map[string]any:
			for _, child := range value {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(dump); err != nil {
		return nil, err
	}
	var endpoints []string
	for address := range addresses {
		endpoints = append(endpoints, address)
	}
	slices.Sort(endpoints)
	return endpoints, nil
}

// This connects from the Go test runner, using each advertised IP unchanged.
// A successful TCP connection is not CephX authentication or native RADOS I/O.
func hostNetworkAssertRunnerTCP(t *testing.T, ctx context.Context, groups []hostNetworkDaemonEndpoints) {
	t.Helper()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	for _, group := range groups {
		for _, address := range group.addresses {
			connection, err := dialer.DialContext(ctx, "tcp", address)
			if err != nil {
				t.Fatalf("test runner cannot reach cluster %d %s advertised endpoint %s: %v", group.cluster, group.daemon, address, err)
			}
			connection.Close()
		}
		t.Logf("test runner TCP reached cluster %d %s advertised endpoints: %v", group.cluster, group.daemon, group.addresses)
	}
}

func hostNetworkAdvanceTopology(t *testing.T, ctx context.Context, cluster *ceph.Container, clusters []*ceph.Container) {
	t.Helper()
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	original := cluster.OSDs()[0]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	logStatus(t, ctx, cluster, "host service data after OSD addition")
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	logStatus(t, ctx, cluster, "host service data after original OSD removal")
	t.Logf("host service topology 2 -> 3 -> 2: added osd.%d, removed original osd.%d", added.ID, original.ID)
}

func hostNetworkRadosProbe(t *testing.T, ctx context.Context, client testcontainers.Container, fsid string, payload []byte, stage string) {
	t.Helper()
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", hostNetworkRadosScript, fsid, hex.EncodeToString(payload), stage}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("native host namespace RADOS probe exited %d: %s", code, output)
	}
	var result struct {
		FSID    string `json:"fsid"`
		Payload string `json:"payload"`
		Stage   string `json:"stage"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode native RADOS probe: %v: %s", err, output)
	}
	if result.FSID != fsid || result.Payload != hex.EncodeToString(payload) || result.Stage != stage {
		t.Fatalf("native RADOS probe connected to wrong cluster or changed bytes: %+v", result)
	}
}

const hostNetworkRadosScript = `import json
import rados
import sys

fsid, payload_hex, stage = sys.argv[1:]
payload = bytes.fromhex(payload_hex)
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
    "keyring": "/etc/ceph/ceph.client.admin.keyring",
    "rados_mon_op_timeout": "30",
    "rados_osd_op_timeout": "30",
}) as cluster:
    assert cluster.get_fsid() == fsid, "connected to another cluster"
    with cluster.open_ioctx("tc-host-isolation") as ioctx:
        if stage == "seed":
            ioctx.write_full("same-object", payload)
        assert ioctx.read("same-object", len(payload) + 1) == payload, "retained payload changed"
        fresh = payload + stage.encode("ascii")
        ioctx.write_full("new-" + stage, fresh)
        assert ioctx.read("new-" + stage, len(fresh) + 1) == fresh, "new roundtrip changed"
    print(json.dumps({"fsid": cluster.get_fsid(), "payload": payload_hex, "stage": stage}))
`
