package cluster

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func testRGWTLSMaterial(t *testing.T) *RGWTLSConfig {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(42), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return &RGWTLSConfig{CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
}

func TestRGWTLSValidationAndKeyIsolation(t *testing.T) {
	first, second := testRGWTLSMaterial(t), testRGWTLSMaterial(t)
	for _, invalid := range []*RGWTLSConfig{{}, {CertificatePEM: first.CertificatePEM}, {PrivateKeyPEM: first.PrivateKeyPEM}, {CertificatePEM: first.CertificatePEM, PrivateKeyPEM: second.PrivateKeyPEM}} {
		if _, err := normalizeRGWConfig(RGWConfig{TLS: invalid}); err == nil {
			t.Fatal("invalid keypair reached daemon creation")
		}
	}
	config, err := normalizeRGWConfig(RGWConfig{TLS: first})
	if err != nil {
		t.Fatal(err)
	}
	original := slices.Clone(config.TLS.PrivateKeyPEM)
	first.PrivateKeyPEM[0] = '!'
	first.CertificatePEM[0] = '!'
	if !bytes.Equal(config.TLS.PrivateKeyPEM, original) {
		t.Fatal("caller mutation changed normalized TLS key")
	}
	if _, err := tls.X509KeyPair(config.TLS.CertificatePEM, config.TLS.PrivateKeyPEM); err != nil {
		t.Fatal("caller mutation changed certificate", err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, config), "PRIVATE KEY") || strings.Contains(fmt.Sprintf(format, config.TLS), "PRIVATE KEY") {
			t.Fatal("descriptor formatting exposed private key")
		}
		gateway := &RGWContainer{SecretKey: "sensitive-test-s3-secret", config: config}
		if text := fmt.Sprintf(format, gateway); text != "RGW gateway default" {
			t.Fatal("gateway formatting recursed into private descriptors")
		}
	}
	if _, err := (&RGWContainer{}).S3SecureEndpoint(t.Context()); err == nil {
		t.Fatal("HTTP gateway advertised TLS")
	}
}

func TestRGWTLSOptionsKeepHTTPAndNativeListenerPorts(t *testing.T) {
	for _, host := range []bool{false, true} {
		material := testRGWTLSMaterial(t)
		cluster := &Container{settings: options{hostNetwork: host, publicAddress: "127.0.0.1", startupTimeout: time.Second}}
		port, secure := 7480, 7481
		if host {
			port, secure = 41301, 41302
		}
		var req testcontainers.GenericContainerRequest
		for _, customizer := range cluster.namedRGWDaemonOptions(port, secure, RGWConfig{Name: "tls", TLS: material}) {
			if err := customizer.Customize(&req); err != nil {
				t.Fatal(err)
			}
		}
		if req.Env["CEPH_RGW_TLS_PORT"] != fmt.Sprint(secure) || req.Env["CEPH_RGW_PORT"] != fmt.Sprint(port) || req.WaitingFor == nil {
			t.Fatal("listener contract missing")
		}
		if host {
			if len(req.ExposedPorts) != 0 || req.Env["CEPH_RGW_ENDPOINT"] != "127.0.0.1:41301" || req.Env["CEPH_RGW_TLS_ENDPOINT"] != "127.0.0.1:41302" {
				t.Fatal("host TLS listener lost selected port")
			}
		} else if len(req.ExposedPorts) != 2 || !slices.Contains(req.ExposedPorts, "7480/tcp") || !slices.Contains(req.ExposedPorts, "7481/tcp") {
			t.Fatal("bridge listeners were not mapped separately")
		}
		var found bool
		for _, file := range req.Files {
			if file.ContainerFilePath != "/tc/rgw-tls.pem" {
				continue
			}
			found = true
			if file.FileMode != 0o600 {
				t.Fatal("native private key file mode")
			}
			data, err := io.ReadAll(file.Reader)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tls.X509KeyPair(data, data); err != nil {
				t.Fatal("native combined certificate/key file", err)
			}
		}
		if !found {
			t.Fatal("TLS file not supplied to native Beast")
		}
	}
}
