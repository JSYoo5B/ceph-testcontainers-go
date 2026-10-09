//go:build all || (integration && hostnetwork && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_endpoints_host))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
			ceph.WithIdleEntrypoint(),
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
