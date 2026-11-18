package dashboard

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

// probeAuthLifetime models request accounting, not Ceph account lockout.
// Its barrier holds the unauthenticated exchange before any successful login
// can conceal a preceding invalid-password attempt.
type probeAuthLifetime struct {
	server  *httptest.Server
	arrived chan struct{}
	release chan struct{}
	invalid atomic.Int64
	logins  atomic.Int64
}

func newProbeAuthLifetime(t *testing.T) (*probeAuthLifetime, string) {
	t.Helper()
	cert, want := probeCertificate(t)
	m := &probeAuthLifetime{arrived: make(chan struct{}, 16), release: make(chan struct{})}
	m.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/auth" {
			var credentials struct {
				Username string `json:"username"`
				Password string `json:"password"`
			}
			if json.NewDecoder(r.Body).Decode(&credentials) == nil && credentials.Username == "probe" && credentials.Password == probePassword {
				m.logins.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = fmt.Fprintf(w, `{"token":%q}`, probeToken)
				return
			}
			m.invalid.Add(1)
		} else if r.Method == http.MethodGet && r.URL.Path == summaryPath && r.Header.Get("Authorization") == "Bearer "+probeToken {
			_, _ = w.Write([]byte(`{"mgr_id":"a"}`))
			return
		}
		m.arrived <- struct{}{}
		select {
		case <-m.release:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	m.server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	m.server.StartTLS()
	t.Cleanup(m.server.Close)
	return m, want
}

func authLifetimeProbe(ctx context.Context, python, url, want string) (probeReport, error) {
	request, err := newProbeRequest(url, want, "probe", probePassword, []string{summaryPath})
	if err != nil {
		return probeReport{}, err
	}
	cmd := exec.CommandContext(ctx, python, "-I", "-c", probeScript)
	cmd.Stdin = bytes.NewReader(request)
	output, err := cmd.Output()
	if err != nil {
		return probeReport{}, err
	}
	return decodeProbeReport(output)
}

func authLifetimePython(t *testing.T) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("probe lifecycle model requires python3")
	}
	return python
}

func TestProbeOverlappingAuthenticationDoesNotSubmitInvalidPasswords(t *testing.T) {
	python := authLifetimePython(t)
	m, want := newProbeAuthLifetime(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	const count = 4
	results := make(chan error, count)
	for range count {
		go func() {
			report, err := authLifetimeProbe(ctx, python, m.server.URL, want)
			if err == nil {
				err = checkReadiness(report, "a")
			}
			results <- err
		}()
	}
	for range count {
		select {
		case <-m.arrived:
		case <-ctx.Done():
			t.Fatal("probes did not reach the authentication barrier")
		}
	}
	invalid := m.invalid.Load()
	logins := m.logins.Load()
	close(m.release)
	for range count {
		if err := <-results; err != nil {
			t.Errorf("probe: %v", err)
		}
	}
	if invalid != 0 || m.invalid.Load() != 0 {
		t.Errorf("probe submitted invalid passwords: before release=%d final=%d", invalid, m.invalid.Load())
	}
	if logins != 0 || m.logins.Load() != count {
		t.Errorf("login barrier: before release=%d final=%d", logins, m.logins.Load())
	}
}

func TestProbeInterruptedAuthenticationDoesNotLeaveInvalidAttempt(t *testing.T) {
	python := authLifetimePython(t)
	m, want := newProbeAuthLifetime(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := authLifetimeProbe(ctx, python, m.server.URL, want); finished <- err }()
	select {
	case <-m.arrived:
	case <-ctx.Done():
		t.Fatal("probe did not reach the authentication barrier")
	}
	cancel()
	if err := <-finished; err == nil {
		t.Error("interrupted probe unexpectedly completed")
	}
	if m.logins.Load() != 0 {
		t.Error("a successful login masked the interrupted exchange")
	}
	invalid := m.invalid.Load()
	close(m.release)
	retryCtx, retryCancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer retryCancel()
	report, err := authLifetimeProbe(retryCtx, python, m.server.URL, want)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkReadiness(report, "a"); err != nil {
		t.Fatal(err)
	}
	if invalid != 0 || m.invalid.Load() != 0 {
		t.Errorf("interruption or retry submitted invalid passwords: interrupted=%d final=%d", invalid, m.invalid.Load())
	}
	if m.logins.Load() != 1 {
		t.Errorf("retry did not complete exactly one successful login: %d", m.logins.Load())
	}
}
