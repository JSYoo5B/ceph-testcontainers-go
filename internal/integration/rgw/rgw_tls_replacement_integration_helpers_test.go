//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// testRGWTLSReplacement replaces the certificate of a native TLS gateway
// twice: a renewed leaf of the same CA, then a leaf of another CA. After each
// replacement a fresh handshake at the endpoint read again presents the new
// leaf, a connection established before was closed rather than moved, the
// client that trusts the right CA reads the original bytes, and after the CA
// change a client that trusts only the old CA is refused. A later plain
// Stop and Start of the container is ready with the replaced certificate.
func testRGWTLSReplacement(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}, opts...)...)
	authority := harness.NewRGWTestAuthority(t)
	original := authority.Issue(t)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{Name: "tls-replace", TLS: original, SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-tls-replace"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		t.Fatal(err)
	}
	client := func(endpoint string, roots *x509.CertPool) harness.S3HTTPClient {
		transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
		t.Cleanup(transport.CloseIdleConnections)
		return harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: &http.Client{Transport: transport, Timeout: 15 * time.Second}}
	}
	secureEndpoint := func() string {
		t.Helper()
		endpoint, err := gateway.S3SecureEndpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return endpoint
	}
	endpoint := secureEndpoint()
	const bucket = "/tc-tls-replace"
	payload := bytes.Repeat([]byte("bytes written before the certificate was replaced\n"), 512)
	sut := client(endpoint, authority.Roots)
	sut.Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	sut.Request(t, ctx, http.MethodPut, bucket+"/data", payload, http.StatusOK)
	if !bytes.Equal(rgwServedLeaf(t, endpoint, authority.Roots), rgwLeaf(t, original)) {
		t.Fatal("the gateway does not serve the configured certificate")
	}

	// replace replaces the certificate, which current verifies, with
	// material, which roots verify, and returns the endpoint read again.
	replace := func(material *rgw.TLSConfig, current, roots *x509.CertPool) string {
		t.Helper()
		held, err := rgwDialTLS(t, endpoint, current)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Close() }()
		if err := gateway.ReplaceTLS(ctx, *material); err != nil {
			t.Fatal(err)
		}
		// The gateway restarted before ReplaceTLS returned: the connection
		// that was established before is closed, not served the new leaf.
		if err := held.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var timeout net.Error
		if _, err := held.Read(make([]byte, 1)); err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
			t.Fatalf("a connection established before the replacement stayed open: %v", err)
		}
		next := secureEndpoint()
		if !bytes.Equal(rgwServedLeaf(t, next, roots), rgwLeaf(t, material)) {
			t.Fatal("a fresh handshake does not present the replaced certificate")
		}
		if actual := client(next, roots).Request(t, ctx, http.MethodGet, bucket+"/data", nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatal("the gateway with the replaced certificate serves other bytes")
		}
		return next
	}

	// A renewed leaf of the same CA: the SUT's trust stays valid.
	renewed := authority.Issue(t)
	endpoint = replace(renewed, authority.Roots, authority.Roots)
	if bytes.Equal(rgwLeaf(t, renewed), rgwLeaf(t, original)) {
		t.Fatal("the renewed certificate is the original one")
	}

	// A leaf of another CA: the old trust is refused.
	successor := harness.NewRGWTestAuthority(t)
	moved := successor.Issue(t)
	endpoint = replace(moved, authority.Roots, successor.Roots)
	var unknown x509.UnknownAuthorityError
	if _, err := rgwDialTLS(t, endpoint, authority.Roots); !errors.As(err, &unknown) {
		t.Fatalf("a client that trusts only the previous CA was not refused for its CA: %v", err)
	}

	// Every later start expects the replaced certificate.
	grace := 3 * time.Second
	if err := gateway.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatalf("a restart after the replacement was not ready: %v", err)
	}
	endpoint = secureEndpoint()
	if !bytes.Equal(rgwServedLeaf(t, endpoint, successor.Roots), rgwLeaf(t, moved)) {
		t.Fatal("a later restart serves another certificate")
	}
	if actual := client(endpoint, successor.Roots).Request(t, ctx, http.MethodGet, bucket+"/data", nil, http.StatusOK); !bytes.Equal(actual, payload) {
		t.Fatal("the restarted gateway serves other bytes")
	}
	t.Log("same-CA renewal and CA change through ReplaceTLS: fresh handshakes present each new leaf at the endpoint read again, earlier connections closed, exact bytes kept, previous CA refused, later restart ready with the replaced certificate")
}
