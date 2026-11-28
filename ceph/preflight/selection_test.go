package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

const (
	selectionChildEnv = "CEPH_PREFLIGHT_SELECTION_CHILD"
	// providerHostEnv asks the child to register a runtime provider whose
	// endpoint is its value, so testcontainers selects that endpoint and no
	// other candidate.
	providerHostEnv = "CEPH_PREFLIGHT_PROVIDER_HOST"
	providerName    = "preflight-test"
)

type testProvider struct{ host string }

func (p testProvider) Initialize(context.Context) (testcontainers.RuntimeConfig, error) {
	return testcontainers.RuntimeConfig{DockerHost: p.host}, nil
}

// TestSelectionChild is the child process of the engine selection tests. It
// runs the production Observe, whose engine testcontainers resolves from the
// child's environment, and prints the report and the error's category.
func TestSelectionChild(t *testing.T) {
	if os.Getenv(selectionChildEnv) == "" {
		t.Skip("child process of the engine selection tests")
	}
	if host := os.Getenv(providerHostEnv); host != "" {
		if err := testcontainers.RegisterProvider(providerName, func() testcontainers.RuntimeProvider { return testProvider{host} }); err != nil {
			t.Fatal(err)
		}
	}
	report, err := Observe(t.Context(), Request{Capabilities: both})
	data, marshalErr := json.Marshal(report)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	category := "none"
	switch {
	case errors.Is(err, ceph.ErrEnvironmentUnavailable):
		category = "environment"
	case err != nil:
		category = "other"
	}
	fmt.Printf("REPORT %s\nERROR %s\n", data, category)
	if err != nil {
		fmt.Printf("ERRTEXT %s\n", err)
	}
}

// fakeEngine is an HTTP server on a Unix socket that answers the read-only
// endpoints LevelInspect needs, with infoStatus for /info, and records every
// request.
type fakeEngine struct {
	socket   string
	mu       sync.Mutex
	requests []string
}

func startFakeEngine(t *testing.T, infoStatus int) *fakeEngine {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pf")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	engine := &fakeEngine{socket: filepath.Join(dir, "d.sock")}
	listener, err := net.Listen("unix", engine.socket)
	if err != nil {
		t.Fatal(err)
	}
	version := regexp.MustCompile(`^/v[0-9.]+`)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := version.ReplaceAllString(r.URL.Path, "")
		engine.mu.Lock()
		engine.requests = append(engine.requests, r.Method+" "+path)
		engine.mu.Unlock()
		w.Header().Set("Api-Version", "1.47")
		w.Header().Set("Ostype", "linux")
		w.Header().Set("Content-Type", "application/json")
		switch path {
		case "/_ping":
			_, _ = w.Write([]byte("OK"))
		case "/version":
			_, _ = w.Write([]byte(`{"Version":"99.0.0-selected","ApiVersion":"1.47","MinAPIVersion":"1.24","Os":"linux","Arch":"amd64"}`))
		case "/info":
			if infoStatus != http.StatusOK {
				w.WriteHeader(infoStatus)
				_, _ = w.Write([]byte(`{"message":"daemon failure on secret-host.example.internal"}`))
				return
			}
			_, _ = w.Write([]byte(`{"Name":"secret-host.example.internal","ServerVersion":"99.0.0-selected","OSType":"linux","Architecture":"x86_64","KernelVersion":"6.1.0-selected","OperatingSystem":"Docker Desktop","SecurityOptions":["name=seccomp,profile=builtin","name=rootless"],"DockerRootDir":"/home/secret-user/docker"}`))
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return engine
}

func (e *fakeEngine) recorded() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.requests)
}

// runSelectionChild runs TestSelectionChild with a clean testcontainers
// configuration and the given environment, and returns its report, error
// category and error text.
func runSelectionChild(t *testing.T, extra ...string) (Report, string, string) {
	t.Helper()
	home := t.TempDir()
	env := append([]string{selectionChildEnv + "=1", "HOME=" + home, "USERPROFILE=" + home, "TESTCONTAINERS_RYUK_DISABLED=true"}, extra...)
	for _, name := range []string{"PATH", "TMPDIR", "GOCOVERDIR", "SYSTEMROOT"} {
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSelectionChild$", "-test.v")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	line := func(prefix string) string {
		_, rest, ok := strings.Cut(string(out), "\n"+prefix+" ")
		if !ok {
			return ""
		}
		rest, _, _ = strings.Cut(rest, "\n")
		return rest
	}
	data := line("REPORT")
	if data == "" {
		t.Fatalf("child printed no report:\n%s", out)
	}
	var report Report
	if err := json.Unmarshal([]byte(data), &report); err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if strings.Contains(data, secret) || strings.Contains(line("ERRTEXT"), secret) {
			t.Fatalf("child output carries %q:\n%s", secret, out)
		}
	}
	return report, line("ERROR"), line("ERRTEXT")
}

// The engine that testcontainers selects through DOCKER_HOST is the
// substitute engine. The report must come from it, and an LevelInspect level call
// must send nothing but reads.
func TestInspectUsesTheSelectedEngine(t *testing.T) {
	if os.Getenv(selectionChildEnv) != "" {
		t.Skip("parent test")
	}
	engine := startFakeEngine(t, http.StatusOK)
	report, category, _ := runSelectionChild(t, "DOCKER_HOST=unix://"+engine.socket)
	if category != "none" {
		t.Fatalf("error category %s", category)
	}
	if report.Engine.ServerVersion != "99.0.0-selected" || report.Engine.Transport != "unix" || report.Engine.Desktop == nil || !*report.Engine.Rootless {
		t.Fatalf("report does not describe the selected engine: %+v", report.Engine)
	}
	if report.Verdict(CapabilityEngine) != StateObserved || report.Verdict(CapabilityBlockIODelay) != StateUnknown {
		t.Fatalf("verdicts:\n%s", report.String())
	}
	requests := engine.recorded()
	if !slices.Contains(requests, "GET /info") {
		t.Fatalf("the selected engine was not read: %v", requests)
	}
	for _, request := range requests {
		method, path, _ := strings.Cut(request, " ")
		if method != http.MethodGet && method != http.MethodHead || !slices.Contains([]string{"/_ping", "/info", "/version"}, path) {
			t.Errorf("inspect level sent %s; requests %v", request, requests)
		}
	}
}

// testcontainers reads the engine information while it selects the engine.
// Through a registered runtime provider, which leaves it no other candidate,
// an engine that answers /info with an error is an inconclusive selection
// with a collection error, and an endpoint that nothing listens on is an
// unreachable engine without one.
func TestEngineSelectionKeepsAnsweredFailureUnknown(t *testing.T) {
	if os.Getenv(selectionChildEnv) != "" {
		t.Skip("parent test")
	}
	failing := startFakeEngine(t, http.StatusInternalServerError)
	missing := filepath.Join(filepath.Dir(failing.socket), "missing.sock")
	for _, tc := range []struct {
		name, host, category string
		engine, delay        State
	}{
		{"information error", "unix://" + failing.socket, "environment", StateUnknown, StateUnknown},
		{"nothing listening", "unix://" + missing, "none", StateUnavailable, StateUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, category, text := runSelectionChild(t, providerHostEnv+"="+tc.host, "TESTCONTAINERS_PROVIDER="+providerName)
			if category != tc.category {
				t.Fatalf("error category %s (%s), want %s", category, text, tc.category)
			}
			if report.Verdict(CapabilityEngine) != tc.engine || report.Verdict(CapabilityBlockIODelay) != tc.delay || report.Verdict(CapabilityBlockIOEIO) != tc.delay {
				t.Fatalf("verdicts:\n%s", report.String())
			}
			if strings.Contains(text, failing.socket) || strings.Contains(report.String(), failing.socket) {
				t.Fatalf("the endpoint left Observe: %s", text)
			}
		})
	}
	if !slices.Contains(failing.recorded(), "GET /info") {
		t.Fatalf("the failing engine was not read: %v", failing.recorded())
	}
}
