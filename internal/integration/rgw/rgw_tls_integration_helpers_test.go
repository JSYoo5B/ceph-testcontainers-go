//go:build all || (integration && features)

package rgw_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// rgwTestAuthority is a private CA that issues gateway certificates.
type rgwTestAuthority struct {
	key         *ecdsa.PrivateKey
	certificate *x509.Certificate
	// PEM is the CA certificate, Roots the pool that trusts only it.
	PEM   []byte
	Roots *x509.CertPool
}

func newRGWTestAuthority(t *testing.T) *rgwTestAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: rgwTestSerial(t), Subject: pkix.Name{CommonName: "Testcontainers fixture CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	authority := &rgwTestAuthority{key: key, certificate: certificate, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), Roots: x509.NewCertPool()}
	authority.Roots.AddCert(certificate)
	return authority
}

// issue returns a new leaf certificate for localhost, 127.0.0.1 and ::1 with
// its own key, followed by the CA certificate in the chain.
func (a *rgwTestAuthority) issue(t *testing.T) *rgw.TLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	leaf := &x509.Certificate{SerialNumber: rgwTestSerial(t), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	chain := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), a.PEM...)
	return &rgw.TLSConfig{CertificatePEM: chain, PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
}

// file writes the CA certificate to a file of the test, as the SUT's CA
// configuration.
func (a *rgwTestAuthority) file(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, a.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func rgwTestSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

// rgwTestCertificate returns a gateway certificate of a new CA and the pool
// that trusts that CA only.
func rgwTestCertificate(t *testing.T) (*rgw.TLSConfig, *x509.CertPool) {
	t.Helper()
	authority := newRGWTestAuthority(t)
	return authority.issue(t), authority.Roots
}

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
