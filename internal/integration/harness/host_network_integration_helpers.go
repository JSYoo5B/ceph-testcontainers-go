//go:build all || (integration && hostnetwork)

package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

func HostNetworkRunPair(t *testing.T, ctx context.Context, image string, imageOpts []testcontainers.ContainerCustomizer) []*ceph.Container {
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
			opts = append(opts, ceph.WithHostNetwork(), ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithHostAddress("127.0.0.1")),
				ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)), ceph.WithStartupTimeout(5*time.Minute))
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
			HostNetworkCleanupCluster(t, r.cluster)
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

func HostNetworkS3RequestInput(ctx context.Context, gateway S3HTTPClient, method, path string, payload []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, gateway.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	gateway.Sign(request, payload, time.Now().UTC())
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
const HostNetworkHTTPScript = `import base64, http.client, json, sys, urllib.parse
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

func HostNetworkCleanupCluster(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if t.Failed() {
			serviceCtx, cancelServices := context.WithTimeout(context.Background(), 5*time.Second)
			services, inventoryErr := ObserveCluster(cluster).ServiceContainers(serviceCtx)
			cancelServices()
			if inventoryErr != nil {
				t.Logf("service inventory unavailable: %v", inventoryErr)
			}
			ctrs := append([]testcontainers.Container{cluster.Container, clusteraccess.InitialManager(t, cluster)}, services...)
			ctrs = append(ctrs, OSDContainers(t, cluster)...)
			for _, ctr := range ctrs {
				if ctr == nil {
					continue
				}
				logs, err := ctr.Logs(ctx)
				if err != nil {
					continue
				}
				data, _ := io.ReadAll(logs)
				_ = logs.Close()
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
