//go:build all || (integration && features)

package rgw_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// stableEndpoint is the test-owned TCP forwarder of the stable-endpoint
// recipe in rgw/README.md. It listens on one fixed loopback address and
// connects each new client connection to the backend that the test selected
// when the connection was accepted. It forwards bytes only: TLS runs end to
// end between the client and the native gateway listener, so the client
// verifies the gateway's own certificate. It does not check backend health,
// retry or replay requests, or move an established connection to another
// backend.
type stableEndpoint struct {
	listener net.Listener
	backends []string
	selected atomic.Int32
	accepted []atomic.Int32
	wg       sync.WaitGroup
}

// newStableEndpoint starts a forwarder to backends (host:port), which selects
// the first one.
func newStableEndpoint(t *testing.T, backends ...string) *stableEndpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &stableEndpoint{listener: listener, backends: backends, accepted: make([]atomic.Int32, len(backends))}
	e.wg.Add(1)
	go e.serve()
	t.Cleanup(func() {
		_ = listener.Close()
		e.wg.Wait()
	})
	return e
}

// URL is the one HTTPS endpoint the SUT is given.
func (e *stableEndpoint) URL() string { return "https://" + e.listener.Addr().String() }

// Select routes connections accepted from now on to backend i. Connections
// already forwarded keep their backend.
func (e *stableEndpoint) Select(i int) { e.selected.Store(int32(i)) }

// Connections is the number of client connections forwarded to backend i.
func (e *stableEndpoint) Connections(i int) int { return int(e.accepted[i].Load()) }

func (e *stableEndpoint) serve() {
	defer e.wg.Done()
	for {
		client, err := e.listener.Accept()
		if err != nil {
			return
		}
		index := int(e.selected.Load())
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.forward(client, index)
		}()
	}
}

func (e *stableEndpoint) forward(client net.Conn, index int) {
	defer func() { _ = client.Close() }()
	backend, err := net.DialTimeout("tcp", e.backends[index], 5*time.Second)
	if err != nil {
		return // The client sees its connection closed.
	}
	defer func() { _ = backend.Close() }()
	e.accepted[index].Add(1)
	deadline := time.Now().Add(2 * time.Minute)
	_ = client.SetDeadline(deadline)
	_ = backend.SetDeadline(deadline)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(backend, client)
		_ = backend.(*net.TCPConn).CloseWrite()
	}()
	_, _ = io.Copy(client, backend)
	_ = client.(*net.TCPConn).CloseWrite()
	<-done
}

// testRGWStableEndpoint gives the SUT, the AWS SDK probe, one fixed HTTPS
// URL in front of two gateways of the same zone with the same certificate.
// The SUT writes an object through gateway A; the test then routes new
// connections to gateway B and stops A, and a fresh SUT process reads the
// exact bytes through the unchanged URL. The forwarder's counters show which
// gateway each connection reached. The gateways share the cluster's storage,
// so this is a gateway failover, not independent replication.
func testRGWStableEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	authority := harness.NewRGWTestAuthority(t)
	material := authority.Issue(t)
	gateways := make([]*rgw.Gateway, 2)
	backends := make([]string, 2)
	for i, name := range []string{"stable-a", "stable-b"} {
		gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: name, TLS: material, SkipUserCreation: true})
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := gateway.S3SecureEndpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		gateways[i], backends[i] = gateway, u.Host
	}
	user, err := gateways[0].CreateUser(ctx, rgw.UserConfig{ID: "tc-stable-endpoint"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	stable := newStableEndpoint(t, backends...)
	probe := buildS3Probe(t, ctx, stable.URL(), gateways[0].Region)
	ca := authority.File(t)
	const bucket, object = "tc-stable-endpoint", "data"
	payload := "bytes written through gateway A and read through gateway B"
	run := func(operation, data string) s3ProbeResult {
		t.Helper()
		return probe.exec(t, ctx, access, secret, "-endpoint", stable.URL(), "-region", gateways[0].Region, "-ca-file", ca,
			"-bucket", bucket, "-object", object, "-operation", operation, "-data", data)
	}
	if result := run("create-bucket", ""); result.Outcome != "accepted" {
		t.Fatalf("SDK CreateBucket through A: %+v", result)
	}
	if result := run("put", payload); result.Outcome != "accepted" || result.SHA256 != sha256Hex([]byte(payload)) {
		t.Fatalf("SDK PutObject through A: %+v", result)
	}
	throughA := stable.Connections(0)
	if throughA == 0 || stable.Connections(1) != 0 {
		t.Fatalf("connections before the switch: A %d, B %d", throughA, stable.Connections(1))
	}

	stable.Select(1)
	grace := 3 * time.Second
	if err := gateways[0].Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	state, err := gateways[0].State(ctx)
	if err != nil || state.Running {
		t.Fatalf("gateway A is not stopped: %v", err)
	}
	if result := run("get", ""); result.Outcome != "accepted" || result.SHA256 != sha256Hex([]byte(payload)) {
		t.Fatalf("SDK GetObject through B: %+v", result)
	}
	if stable.Connections(0) != throughA || stable.Connections(1) == 0 {
		t.Fatalf("connections after the switch: A %d (was %d), B %d", stable.Connections(0), throughA, stable.Connections(1))
	}
	t.Logf("one HTTPS URL, TLS end to end to native RGW; SDK wrote through A, A stopped, fresh SDK process read exact bytes through B; forwarded connections A=%d B=%d; no request replay", stable.Connections(0), stable.Connections(1))
}
