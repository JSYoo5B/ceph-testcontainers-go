//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWNativeTLS(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, _ := newServiceCluster(t, opts...)
			material, roots := rgwTestCertificate(t)
			gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{Name: "tls", TLS: material, SkipUserCreation: true})
			if err != nil {
				t.Fatal(err)
			}
			// Start normalization owns copies; mutable caller bytes cannot change
			// a later placement restart or the descriptor's TLS configuration.
			material.PrivateKeyPEM[0] = '!'
			material.CertificatePEM[0] = '!'
			secure, err := gateway.S3SecureEndpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			plain, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			secureURL, err := url.Parse(secure)
			if err != nil {
				t.Fatal(err)
			}
			plainURL, err := url.Parse(plain)
			if err != nil {
				t.Fatal(err)
			}
			if secureURL.Scheme != "https" || plainURL.Scheme != "http" || secureURL.Port() == "" || secureURL.Port() == plainURL.Port() {
				t.Fatal("TLS and HTTP endpoints share a port")
			}
			user, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-tls-client"})
			if err != nil {
				t.Fatal(err)
			}
			access, secret, err := user.Credentials()
			if err != nil {
				t.Fatal(err)
			}
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
			defer transport.CloseIdleConnections()
			client := s3HTTPClient{endpoint: secure, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Transport: transport, Timeout: 15 * time.Second}}
			const bucket = "/tc-native-tls"
			payload := bytes.Repeat([]byte("TLS native RGW verified certificate and durable S3 bytes\n"), 1024)
			client.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			client.request(t, ctx, http.MethodPut, bucket+"/data", payload, http.StatusOK)
			actual := client.request(t, ctx, http.MethodGet, bucket+"/data", nil, http.StatusOK)
			if !bytes.Equal(actual, payload) {
				t.Fatal("HTTPS native S3 payload differs")
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, secure+bucket+"/data", nil)
			if err != nil {
				t.Fatal(err)
			}
			client.sign(request, nil, time.Now().UTC())
			response, err := client.http.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.TLS == nil || len(response.TLS.VerifiedChains) == 0 || response.TLS.Version < tls.VersionTLS12 {
				t.Fatal("consumer did not verify actual native TLS certificate")
			}
			untrustedTransport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}
			defer untrustedTransport.CloseIdleConnections()
			untrusted := &http.Client{Transport: untrustedTransport, Timeout: 10 * time.Second}
			if response, err := untrusted.Do(request.Clone(ctx)); err == nil {
				response.Body.Close()
				t.Fatal("consumer accepted certificate without fixture CA")
			} else {
				var unknown x509.UnknownAuthorityError
				if !errors.As(err, &unknown) {
					t.Fatalf("untrusted client failed for unrelated reason: %v", err)
				}
			}
			ordinary := client
			ordinary.endpoint = plain
			ordinary.http = &http.Client{Timeout: 15 * time.Second}
			if actual := ordinary.request(t, ctx, http.MethodGet, bucket+"/data", nil, http.StatusOK); !bytes.Equal(actual, payload) {
				t.Fatal("HTTP and native TLS listeners do not share S3 state")
			}
			client.request(t, ctx, http.MethodDelete, bucket+"/data", nil, http.StatusNoContent)
			client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
			if err := gateway.RemoveUser(ctx, user); err != nil {
				t.Fatal(err)
			}
			if err := cluster.RemoveRGW(ctx, gateway.GatewayName); err != nil {
				t.Fatal(err)
			}
			t.Log("native Beast dual listeners with separate mapped/selected ports; trusted CA+SAN+TLS12 verification and exact S3 bytes, unknown CA rejected, HTTP reads same data, owned resource cleanup")
		})
	}
}

func rgwTestCertificate(t *testing.T) (*ceph.RGWTLSConfig, *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(101), Subject: pkix.Name{CommonName: "Testcontainers fixture CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(102), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certificate := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), caPEM...)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("fixture CA not parsed")
	}
	return &ceph.RGWTLSConfig{CertificatePEM: certificate, PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})}, roots
}
