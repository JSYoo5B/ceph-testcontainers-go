//go:build integration && hostnetwork

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
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This test exercises native librados in the Docker daemon's host namespace.
// On Docker Desktop that namespace belongs to its Linux VM; successful I/O
// here does not prove that a native macOS process can reach advertised OSDs.
func TestHostNetworkMultiCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	clusters := hostNetworkRunPair(t, ctx, image, imageOpts)

	fsids := make([]string, 2)
	keyrings := make([][]byte, 2)
	clients := make([]testcontainers.Container, 2)
	for i, cluster := range clusters {
		if !cluster.UsesHostNetwork() || cluster.PublicAddress() != "127.0.0.1" {
			t.Fatalf("cluster %d has unexpected networking: host=%t address=%q", i, cluster.UsesHostNetwork(), cluster.PublicAddress())
		}
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fsids[i] = status.FSID
		config, keyring, err := cluster.ConnectionConfig()
		if err != nil {
			t.Fatal(err)
		}
		if len(config) == 0 || len(keyring) == 0 {
			t.Fatalf("cluster %d returned empty connection files", i)
		}
		keyrings[i] = keyring
		cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-host-isolation", "8")
		cephCommand(t, ctx, cluster, "osd", "pool", "set", "tc-host-isolation", "pg_autoscale_mode", "off")
		cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-host-isolation", "rados")
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
			testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})),
		)
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		inspect, err := client.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if inspect.HostConfig.NetworkMode != "host" {
			t.Fatalf("client %d is not in the Docker host network namespace", i)
		}
		clients[i] = client
	}
	if fsids[0] == "" || fsids[1] == "" || fsids[0] == fsids[1] || bytes.Equal(keyrings[0], keyrings[1]) {
		t.Fatal("host network clusters do not have independent FSIDs and credentials")
	}
	hostNetworkAssertDistinctAddresses(t, ctx, clusters)
	t.Logf("concurrent host network clusters: FSIDs %s and %s", fsids[0], fsids[1])

	payloads := [][]byte{
		bytes.Repeat([]byte("host-network-cluster-A\n"), 128),
		bytes.Repeat([]byte("host-network-cluster-B\n"), 128),
	}
	for i := range clients {
		hostNetworkRadosProbe(t, ctx, clients[i], fsids[i], payloads[i], "seed")
	}
	for i := range clients {
		hostNetworkRadosProbe(t, ctx, clients[i], fsids[i], payloads[i], "initial")
	}
	for i, cluster := range clusters {
		advanceServiceTopology(t, ctx, cluster)
		hostNetworkAssertDistinctAddresses(t, ctx, clusters)
		for j := range clients {
			hostNetworkRadosProbe(t, ctx, clients[j], fsids[j], payloads[j], fmt.Sprintf("after-topology-%d", i))
		}
	}
	t.Log("native Python librados: identical pool/object names remained independent after both OSD 2 -> 3 -> 2 replacements; fresh sessions read retained bytes and wrote new objects")
}

func TestHostNetworkRGWEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	clusters := hostNetworkRunPair(t, ctx, image, opts)
	// Occupy the conventional RGW port inside the daemon's host namespace.
	// This also works when port 7480 was already taken by another service.
	holder, err := testcontainers.Run(ctx, image, clusters[0].WithClient(),
		testcontainers.WithEntrypoint("python3"), testcontainers.WithCmd("-u", "-c", `import errno, signal, socket, time
signal.signal(signal.SIGTERM, lambda *_: exit(0))
s = socket.socket()
try:
    s.bind(("127.0.0.1", 7480))
    s.listen()
except OSError as e:
    if e.errno != errno.EADDRINUSE:
        raise
print("PORT_7480_RESERVED", flush=True)
time.sleep(1200)
`), testcontainers.WithWaitStrategy(wait.ForLog("PORT_7480_RESERVED")),
	)
	if holder != nil {
		testcontainers.CleanupContainer(t, holder)
	}
	if err != nil {
		t.Fatal(err)
	}
	const bucket = "/tc-host-rgw"
	gateways := make([]s3HTTPClient, 2)
	clients := make([]testcontainers.Container, 2)
	payloads := [][]byte{[]byte("host RGW cluster A"), []byte("host RGW cluster B")}
	for i, cluster := range clusters {
		rgw, err := cluster.StartRGW(ctx)
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := rgw.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Port() == "" || parsed.Port() == "7480" {
			t.Fatalf("RGW must use an allocated port while 7480 is occupied: %q (%v)", endpoint, err)
		}
		gateways[i] = s3HTTPClient{endpoint: endpoint, accessKey: rgw.AccessKey, secretKey: rgw.SecretKey,
			region: rgw.Region, http: &http.Client{Timeout: 15 * time.Second}}
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
			testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import http.client"})),
		)
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
		hostNetworkS3Request(t, ctx, client, gateways[i], http.MethodPut, bucket, nil, http.StatusOK)
		hostNetworkS3Request(t, ctx, client, gateways[i], http.MethodPut, bucket+"/same-object", payloads[i], http.StatusOK)
	}
	if gateways[0].endpoint == gateways[1].endpoint {
		t.Fatalf("independent RGWs share endpoint %s", gateways[0].endpoint)
	}
	for i, gateway := range gateways {
		actual := hostNetworkS3Request(t, ctx, clients[i], gateway, http.MethodGet, bucket+"/same-object", nil, http.StatusOK)
		if !bytes.Equal(actual, payloads[i]) {
			t.Fatalf("VM host namespace RGW %d returned another cluster's payload", i)
		}
		t.Logf("VM host namespace signed S3 roundtrip: cluster %d endpoint=%s", i, gateway.endpoint)
	}

	// Desktop can provide VM host networking without exposing these ports to
	// the macOS test runner. Keep that result separate from VM-native S3 I/O.
	for i, gateway := range gateways {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway.endpoint+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		probe := &http.Client{Timeout: 3 * time.Second}
		response, err := probe.Do(request)
		if err != nil {
			if os.Getenv("CEPH_TEST_HOST_HTTP_REQUIRED") == "1" {
				t.Fatalf("test runner cannot reach host RGW %d: %v", i, err)
			}
			t.Logf("test runner HTTP unavailable for cluster %d: %v; VM host namespace S3 passed; enable Docker Desktop host networking to validate host exposure", i, err)
			continue
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusForbidden {
			t.Fatalf("test runner reached unexpected HTTP service at %s: status %d", gateway.endpoint, response.StatusCode)
		}
		if actual := gateway.request(t, ctx, http.MethodGet, bucket+"/same-object", nil, http.StatusOK); !bytes.Equal(actual, payloads[i]) {
			t.Fatalf("test runner host RGW %d returned wrong payload", i)
		}
		gateway.request(t, ctx, http.MethodPut, bucket+"/from-test-runner", payloads[i], http.StatusOK)
		if actual := gateway.request(t, ctx, http.MethodGet, bucket+"/from-test-runner", nil, http.StatusOK); !bytes.Equal(actual, payloads[i]) {
			t.Fatalf("test runner host RGW %d changed new payload", i)
		}
		gateway.request(t, ctx, http.MethodDelete, bucket+"/from-test-runner", nil, http.StatusNoContent)
		t.Logf("test runner Go HTTP signed S3 read/write passed: cluster %d endpoint=%s", i, gateway.endpoint)
	}
	for i, gateway := range gateways {
		hostNetworkS3Request(t, ctx, clients[i], gateway, http.MethodDelete, bucket+"/same-object", nil, http.StatusNoContent)
		hostNetworkS3Request(t, ctx, clients[i], gateway, http.MethodDelete, bucket, nil, http.StatusNoContent)
	}
}

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

// Verify the actual Python wire transport without starting a Ceph cluster.
func TestHostNetworkHTTPTransportPreservesSignedRequest(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required to verify the container HTTP probe")
	}
	for _, tc := range []struct {
		name, method, path string
		payload            []byte
		status             int
	}{
		{"empty bucket PUT", http.MethodPut, "/bucket", nil, http.StatusOK},
		{"binary object PUT", http.MethodPut, "/bucket/object", []byte{0, 1, 2, 255}, http.StatusOK},
		{"escaped GET with query", http.MethodGet, "/bucket/object%20name?prefix=a%2Fb&list-type=2", nil, http.StatusForbidden},
		{"object DELETE", http.MethodDelete, "/bucket/object", nil, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type receivedRequest struct {
				method, target, host string
				headers              http.Header
				payload              []byte
				err                  error
			}
			received := make(chan receivedRequest, 1)
			responseBody := []byte{255, 0, 'r', 'g', 'w'}
			if tc.status == http.StatusNoContent {
				responseBody = nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				received <- receivedRequest{r.Method, r.RequestURI, r.Host, r.Header.Clone(), payload, err}
				w.WriteHeader(tc.status)
				_, _ = w.Write(responseBody)
			}))
			defer server.Close()
			gateway := s3HTTPClient{endpoint: server.URL, accessKey: "test-access", secretKey: "test-secret", region: "us-east-1"}
			input, err := hostNetworkS3RequestInput(t.Context(), gateway, tc.method, tc.path, tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			var signed struct {
				URL     string      `json:"url"`
				Headers http.Header `json:"headers"`
			}
			if err := json.Unmarshal(input, &signed); err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(inputPath, input, 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, "-c", hostNetworkHTTPScript, inputPath).CombinedOutput()
			if err != nil {
				t.Fatalf("Python HTTP transport: %v: %s", err, output)
			}
			var result struct {
				Status int    `json:"status"`
				Body   string `json:"body"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("decode Python response: %v: %s", err, output)
			}
			body, err := base64.StdEncoding.DecodeString(result.Body)
			if err != nil || result.Status != tc.status || !bytes.Equal(body, responseBody) {
				t.Fatalf("Python changed HTTP response: status=%d body=%q error=%v", result.Status, body, err)
			}
			actual := <-received
			parsed, err := url.Parse(signed.URL)
			if err != nil {
				t.Fatal(err)
			}
			if actual.err != nil || actual.method != tc.method || actual.target != tc.path || actual.host != parsed.Host || !bytes.Equal(actual.payload, tc.payload) {
				t.Fatalf("Python changed signed request: %+v", actual)
			}
			if _, exists := actual.headers["Content-Type"]; exists {
				t.Fatal("transport injected an unsigned Content-Type header")
			}
			for _, header := range []string{"Authorization", "X-Amz-Date", "X-Amz-Content-Sha256"} {
				if got, want := actual.headers.Get(header), signed.Headers.Get(header); got == "" || got != want {
					t.Errorf("transport changed %s: got %q, want %q", header, got, want)
				}
			}
		})
	}
}

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

func hostNetworkAssertDistinctAddresses(t *testing.T, ctx context.Context, clusters []*ceph.Container) {
	t.Helper()
	seen := make(map[string]int)
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
			// Dump fields differ between Ceph versions. Extract and normalize
			// address vectors rather than comparing nonce-bearing map strings.
			prefix := net.JoinHostPort(cluster.PublicAddress(), "")
			pattern := regexp.MustCompile(`(?:v[12]:)?(` + regexp.QuoteMeta(prefix) + `[0-9]+)(?:/[0-9]+)?`)
			addresses := make(map[string]bool)
			var visit func(any)
			visit = func(value any) {
				switch value := value.(type) {
				case string:
					for _, match := range pattern.FindAllStringSubmatch(value, -1) {
						_, port, err := net.SplitHostPort(match[1])
						if err != nil {
							t.Fatalf("invalid advertised endpoint %q: %v", match[1], err)
						}
						// MGR maps can retain a legacy address with port zero
						// alongside their actual messenger address vectors.
						if port == "0" {
							continue
						}
						addresses[match[1]] = true
					}
				case []any:
					for _, child := range value {
						visit(child)
					}
				case map[string]any:
					for _, child := range value {
						visit(child)
					}
				}
			}
			visit(dump)
			if len(addresses) == 0 {
				t.Fatalf("cluster %d %s dump does not advertise any %s endpoints: %s", i, daemon, cluster.PublicAddress(), data)
			}
			var endpoints []string
			for endpoint := range addresses {
				if owner, exists := seen[endpoint]; exists && owner != i {
					t.Fatalf("clusters %d and %d advertise the same endpoint %s", owner, i, endpoint)
				}
				seen[endpoint] = i
				endpoints = append(endpoints, endpoint)
			}
			slices.Sort(endpoints)
			t.Logf("cluster %d %s endpoints: %v", i, daemon, endpoints)
		}
	}
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
