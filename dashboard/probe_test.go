package dashboard

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// probeListener is a Dashboard stand-in that presents installed for the
// first switchAfter TLS handshakes and other after them, and records every
// request it receives.
type probeListener struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []probeSeen
}

// probeSeen is one request a probeListener received.
type probeSeen struct {
	method, path, body, authorization string
}

const (
	probePassword = "probe-secret"
	probeToken    = "probe-token"
)

func newProbeListener(t *testing.T, installed, other tls.Certificate, switchAfter int64) *probeListener {
	t.Helper()
	l := &probeListener{}
	var handshakes atomic.Int64
	l.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		l.mu.Lock()
		l.requests = append(l.requests, probeSeen{method: r.Method, path: r.URL.Path, body: string(body), authorization: r.Header.Get("Authorization")})
		l.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/auth":
			var login struct {
				Password string `json:"password"`
			}
			if json.Unmarshal(body, &login) != nil || login.Password != probePassword {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token": "` + probeToken + `"}`))
		case r.Method == http.MethodGet && r.URL.Path == summaryPath && r.Header.Get("Authorization") == "Bearer "+probeToken:
			_, _ = w.Write([]byte(`{"mgr_id": "a"}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	// The probe connects to an address without SNI, so the certificate is
	// chosen per handshake by the configuration, not by GetCertificate.
	l.server.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		presented := other
		if handshakes.Add(1) <= switchAfter {
			presented = installed
		}
		return &tls.Config{Certificates: []tls.Certificate{presented}}, nil
	}}
	l.server.StartTLS()
	t.Cleanup(l.server.Close)
	return l
}

func (l *probeListener) seen() []probeSeen {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]probeSeen(nil), l.requests...)
}

// probeCertificate returns a self-signed key pair and the fingerprint of its
// certificate.
func probeCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	material, der := testMaterial(t)
	pair, err := tls.X509KeyPair(material.CertificatePEM, material.PrivateKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, fingerprint(der)
}

// runLocalProbe runs probeScript on this host, as the control container runs
// it, against url and accepts only the certificate with fingerprint want.
func runLocalProbe(t *testing.T, url, want string) probeReport {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local probe regressions require installed python3; native Dashboard qualification covers the runtime")
	}
	request, err := newProbeRequest(url, want, "probe", probePassword, []string{summaryPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-I", "-c", probeScript)
	cmd.Stdin = bytes.NewReader(request)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe: %v: %s", err, stderr.String())
	}
	report, err := decodeProbeReport(out)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return report
}

func TestProbeLogsInOnTheInstalledCertificate(t *testing.T) {
	installed, want := probeCertificate(t)
	other, _ := probeCertificate(t)
	l := newProbeListener(t, installed, other, 1<<30)
	report := runLocalProbe(t, l.server.URL, want)
	if err := checkReadiness(report, "a"); err != nil {
		t.Fatalf("readiness on the installed certificate: %v (%+v)", err, report)
	}
	var bearer bool
	for _, seen := range l.seen() {
		bearer = bearer || seen.path == summaryPath && seen.authorization == "Bearer "+probeToken
	}
	if !bearer {
		t.Fatalf("the listener received no bearer request: %+v", l.seen())
	}
}

func TestProbeSendsNoCredentialsToAnotherCertificate(t *testing.T) {
	_, want := probeCertificate(t)
	other, otherPrint := probeCertificate(t)
	l := newProbeListener(t, other, other, 1<<30)
	report := runLocalProbe(t, l.server.URL, want)
	if seen := l.seen(); len(seen) != 0 {
		t.Fatalf("a listener with another certificate received %d requests: %+v", len(seen), seen)
	}
	if report.Mismatch != otherPrint || report.Token != "" {
		t.Fatalf("report %+v", report)
	}
	if err := checkReadiness(report, "a"); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("readiness on another certificate: %v", err)
	}
}

func TestProbeSendsNoTokenAfterTheCertificateChanges(t *testing.T) {
	installed, want := probeCertificate(t)
	other, otherPrint := probeCertificate(t)
	// The wrong-password and login connections present the installed
	// certificate; the bearer request's connection presents another one.
	l := newProbeListener(t, installed, other, 2)
	report := runLocalProbe(t, l.server.URL, want)
	seen := l.seen()
	if len(seen) != 2 || seen[0].path != "/api/auth" || seen[1].path != "/api/auth" {
		t.Fatalf("the listener received %+v, want only the two logins", seen)
	}
	for _, s := range seen {
		if s.authorization != "" {
			t.Fatalf("the listener received a bearer header after its certificate changed: %+v", s)
		}
	}
	if report.Mismatch != otherPrint {
		t.Fatalf("report %+v", report)
	}
	if err := checkReadiness(report, "a"); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("readiness after the certificate changed: %v", err)
	}
}

func TestProbeSendsNoPasswordAfterTheCertificateChanges(t *testing.T) {
	installed, want := probeCertificate(t)
	other, _ := probeCertificate(t)
	// Only the wrong-password connection presents the installed certificate.
	l := newProbeListener(t, installed, other, 1)
	report := runLocalProbe(t, l.server.URL, want)
	seen := l.seen()
	if len(seen) != 1 {
		t.Fatalf("the listener received %+v, want only the wrong-password login", seen)
	}
	if err := checkReadiness(report, "a"); err == nil || report.Token != "" {
		t.Fatalf("readiness after the certificate changed: %v (%+v)", err, report)
	}
}
