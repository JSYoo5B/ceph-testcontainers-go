//go:build all || (integration && features)

package rgw_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// rgwLeaf returns the DER leaf certificate of material.
func rgwLeaf(t *testing.T, material *rgw.TLSConfig) []byte {
	t.Helper()
	pair, err := tls.X509KeyPair(material.CertificatePEM, material.PrivateKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair.Certificate[0]
}

// rgwDialTLS makes a fresh TLS connection to the host of endpoint that roots
// verify, with host name verification.
func rgwDialTLS(t *testing.T, endpoint string, roots *x509.CertPool) (*tls.Conn, error) {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	conn, err := dialer.Dial("tcp", u.Host)
	if err != nil {
		return nil, err
	}
	return conn.(*tls.Conn), nil
}

// rgwServedLeaf returns the leaf certificate that a fresh handshake with
// endpoint verified through roots.
func rgwServedLeaf(t *testing.T, endpoint string, roots *x509.CertPool) []byte {
	t.Helper()
	conn, err := rgwDialTLS(t, endpoint, roots)
	if err != nil {
		t.Fatalf("fresh TLS handshake with %s: %v", endpoint, err)
	}
	defer func() { _ = conn.Close() }()
	return conn.ConnectionState().PeerCertificates[0].Raw
}
