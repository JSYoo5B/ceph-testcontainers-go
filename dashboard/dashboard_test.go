package dashboard

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func testMaterial(t *testing.T) (TLSConfig, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "dashboard.test"}, DNSNames: []string{"dashboard.test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return TLSConfig{CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}, der
}

func TestNormalizeConfig(t *testing.T) {
	material, der := testMaterial(t)
	cfg, leaf, err := normalizeConfig(Config{TLS: material})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.Username, "tc-dashboard-") || cfg.Role != "read-only" || leaf != fingerprint(der) {
		t.Fatalf("defaults %q %q %q", cfg.Username, cfg.Role, leaf)
	}
	other, _ := testMaterial(t)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecTemplate := &x509.Certificate{SerialNumber: big.NewInt(8), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	ecDER, err := x509.CreateCertificate(rand.Reader, ecTemplate, ecTemplate, &ecKey.PublicKey, ecKey)
	if err != nil {
		t.Fatal(err)
	}
	ecKeyDER, err := x509.MarshalPKCS8PrivateKey(ecKey)
	if err != nil {
		t.Fatal(err)
	}
	ecdsaMaterial := TLSConfig{CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ecDER}), PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: ecKeyDER})}
	for name, bad := range map[string]Config{
		"no certificate":    {},
		"ECDSA key":         {TLS: ecdsaMaterial},
		"mismatched key":    {TLS: TLSConfig{CertificatePEM: material.CertificatePEM, PrivateKeyPEM: other.PrivateKeyPEM}},
		"invalid username":  {TLS: material, Username: "Admin user"},
		"invalid role name": {TLS: material, Role: "read only"},
	} {
		if _, _, err := normalizeConfig(bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestStartRefusesNilClusterAndBadConfig(t *testing.T) {
	material, _ := testMaterial(t)
	if d, err := Start(context.Background(), nil, Config{TLS: material}); d != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil cluster: %v %v", d, err)
	}
}

func TestGeneratePassword(t *testing.T) {
	for range 200 {
		password, err := generatePassword()
		if err != nil {
			t.Fatal(err)
		}
		if len(password) != 24 {
			t.Fatalf("length %d", len(password))
		}
		for _, alphabet := range passwordAlphabets {
			if !strings.ContainsAny(password, alphabet) {
				t.Fatalf("%d-character password lacks a class", len(password))
			}
		}
		lower := strings.ToLower(password)
		for _, word := range []string{"osd", "rbd", "nfs", "ceph", "host", "pool", "mon", "mgr"} {
			if strings.Contains(lower, word) {
				t.Fatalf("password contains an excluded word %q", word)
			}
		}
		for i := 1; i < len(password); i++ {
			if d := int(password[i]) - int(password[i-1]); d >= -1 && d <= 1 {
				t.Fatal("password has repeated or sequential neighbours")
			}
		}
	}
}

func TestFormattingHidesSecrets(t *testing.T) {
	material, _ := testMaterial(t)
	d := &Dashboard{&state{username: "tc-dashboard-1", password: "secret-password"}}
	for _, text := range []string{fmt.Sprint(d), fmt.Sprintf("%+v %#v %v", *d, *d, material), fmt.Sprintf("%#v %d", material, material)} {
		if strings.Contains(text, "secret-password") || strings.Contains(text, "PRIVATE KEY") {
			t.Fatalf("formatted secret: %s", text)
		}
	}
}

// summaryBody is GET /api/summary of a 20.2.4 manager, shortened.
const summaryBody = `{"health_status": "HEALTH_WARN", "mgr_id": "a", "mgr_host": "https://172.18.0.3:8443/", "have_mon_connection": true}`

func readyReport(leaf string) probeReport {
	return probeReport{Fingerprint: leaf, Rejected: probeResult{Status: 400, Body: `{"detail": "Invalid credentials", "code": "invalid_credentials", "component": "auth"}`},
		Login: probeResult{Status: 201}, Token: "token", Results: []probeResult{{Path: summaryPath, Status: 200, Body: summaryBody}}}
}

func TestCheckReadiness(t *testing.T) {
	if err := checkReadiness(readyReport("leaf"), "leaf", "a"); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*probeReport){
		"other certificate": func(r *probeReport) { r.Fingerprint = "other" },
		"unreachable": func(r *probeReport) {
			r.Rejected = probeResult{Error: "ConnectionRefusedError: [Errno 111] Connection refused"}
		},
		"standby redirect": func(r *probeReport) { r.Rejected = probeResult{Status: 303, Location: "https://172.18.0.5:8443/"} },
		"wrong password accepted": func(r *probeReport) {
			r.Rejected = probeResult{Status: 201}
		},
		"login refused":   func(r *probeReport) { r.Login, r.Token = probeResult{Status: 400}, "" },
		"no summary":      func(r *probeReport) { r.Results = nil },
		"summary refused": func(r *probeReport) { r.Results[0].Status = 401 },
	}
	for name, change := range cases {
		report := readyReport("leaf")
		change(&report)
		if err := checkReadiness(report, "leaf", "a"); err == nil {
			t.Errorf("%s passed", name)
		}
	}
	if err := checkReadiness(readyReport("leaf"), "leaf", "b"); err == nil || !strings.Contains(err.Error(), "mgr.a") {
		t.Fatalf("summary of another manager: %v", err)
	}
}

func TestDecodeProbeReport(t *testing.T) {
	report, err := decodeProbeReport([]byte(`{"results": [{"status": 200, "location": "", "body": "[]", "path": "/api/rgw/user"}], "fingerprint": "ab", "rejected": {"status": 400}, "login": {"status": 201}, "token": "t"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result, ok := report.result(rgwUserPath); !ok || result.Status != 200 || report.Token != "t" {
		t.Fatalf("decoded %+v", report)
	}
	if _, err := decodeProbeReport([]byte("Traceback")); err == nil {
		t.Fatal("decoded a non-JSON report")
	}
}

// Dashboard answers about its RGW backend observed on 20.2.4.
func TestBackendError(t *testing.T) {
	cases := []struct {
		result probeResult
		want   error
	}{
		{probeResult{Path: rgwUserPath, Status: 200, Body: `["tc-owner"]`}, nil},
		{probeResult{Path: rgwUserPath, Status: 404, Body: `{"detail": "Error connecting to Object Gateway: RGW REST API failed request with status code 403\n(b'{\"Code\":\"SignatureDoesNotMatch\",\"Message\":\"\"}')"}`}, ErrGatewayRefused},
		{probeResult{Path: rgwUserPath, Status: 500, Body: `{"detail": "RGW REST API failed request with status code 403\n(b'{\"Code\":\"AccessDenied\",\"Message\":\"\"}')"}`}, ErrGatewayRefused},
		{probeResult{Path: rgwUserPath, Status: 404, Body: `{"detail": "Error connecting to Object Gateway: RGW REST API failed request with status code 403\n(b'{\"Code\":\"UserSuspended\"}')"}`}, ErrGatewayRefused},
		{probeResult{Path: rgwDaemonPath, Error: "timeout"}, ErrGatewayUnreachable},
		{probeResult{Path: rgwDaemonPath, Status: 404, Body: `{"detail": "Error connecting to Object Gateway: HTTPConnectionPool(host='172.18.0.7', port=7480): Max retries exceeded with url"}`}, ErrGatewayUnreachable},
	}
	for _, c := range cases {
		err := backendError(c.result)
		if (c.want == nil) != (err == nil) || (c.want != nil && !errors.Is(err, c.want)) {
			t.Errorf("%d %q: %v, want %v", c.result.Status, c.result.Body, err, c.want)
		}
	}
	if err := backendError(probeResult{Path: rgwUserPath, Status: 500, Body: `{"detail": "something else"}`}); err == nil || errors.Is(err, ErrGatewayRefused) || errors.Is(err, ErrGatewayUnreachable) {
		t.Fatalf("unclassified failure: %v", err)
	}
}

// multiclusterValue is the module's record after a login of tc-reader on
// 20.2.4, with the token shortened.
const multiclusterRecord = `{"current_url": null, "current_user": "tc-reader", "hub_url": null, "config": {"a39027e6-f17c-4ec4-8d64-af54b9f25258": [{"name": "a39027e6-f17c-4ec4-8d64-af54b9f25258", "url": null, "cluster_alias": "local-cluster", "user": "tc-reader", "token": "eyJ"}]}}`

func TestOnlyUser(t *testing.T) {
	if !onlyUser(multiclusterRecord, "tc-reader") {
		t.Fatal("the owned account's record was not recognised")
	}
	if onlyUser(multiclusterRecord, "tc-other") || onlyUser(strings.Replace(multiclusterRecord, `"user": "tc-reader"`, `"user": "sut"`, 1), "tc-reader") || onlyUser("not json", "tc-reader") {
		t.Fatal("a record of another account was claimed")
	}
}
