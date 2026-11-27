package preflight

import (
	"encoding/json"
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
)

const selectionChildEnv = "CEPH_PREFLIGHT_SELECTION_CHILD"

// TestSelectionChild is the child process of TestInspectUsesTheSelectedEngine.
// It runs the production Observe, whose engine testcontainers resolves from
// the child's environment, and prints the report.
func TestSelectionChild(t *testing.T) {
	if os.Getenv(selectionChildEnv) == "" {
		t.Skip("child process of TestInspectUsesTheSelectedEngine")
	}
	report, err := Observe(t.Context(), Request{Capabilities: both})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("REPORT %s\n", data)
}

// The engine that testcontainers selects through DOCKER_HOST is a small
// HTTP server that answers the read-only endpoints Inspect needs and records
// every request. The child process starts with a clean testcontainers
// resolution and configuration, so the report must come from this engine,
// and an Inspect level call must send nothing but reads.
func TestInspectUsesTheSelectedEngine(t *testing.T) {
	if os.Getenv(selectionChildEnv) != "" {
		t.Skip("parent test")
	}
	dir, err := os.MkdirTemp("/tmp", "pf")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "d.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu       sync.Mutex
		requests []string
	)
	version := regexp.MustCompile(`^/v[0-9.]+`)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := version.ReplaceAllString(r.URL.Path, "")
		mu.Lock()
		requests = append(requests, r.Method+" "+path)
		mu.Unlock()
		w.Header().Set("Api-Version", "1.47")
		w.Header().Set("Ostype", "linux")
		w.Header().Set("Content-Type", "application/json")
		switch path {
		case "/_ping":
			_, _ = w.Write([]byte("OK"))
		case "/version":
			_, _ = w.Write([]byte(`{"Version":"99.0.0-selected","ApiVersion":"1.47","MinAPIVersion":"1.24","Os":"linux","Arch":"amd64"}`))
		case "/info":
			_, _ = w.Write([]byte(`{"Name":"secret-host.example.internal","ServerVersion":"99.0.0-selected","OSType":"linux","Architecture":"x86_64","KernelVersion":"6.1.0-selected","OperatingSystem":"Docker Desktop","SecurityOptions":["name=seccomp,profile=builtin","name=rootless"],"DockerRootDir":"/home/secret-user/docker"}`))
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	home := t.TempDir()
	env := []string{selectionChildEnv + "=1", "DOCKER_HOST=unix://" + socket, "HOME=" + home, "USERPROFILE=" + home, "TESTCONTAINERS_RYUK_DISABLED=true"}
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
	_, line, ok := strings.Cut(string(out), "REPORT ")
	if !ok {
		t.Fatalf("child printed no report:\n%s", out)
	}
	line, _, _ = strings.Cut(line, "\n")
	var report Report
	if err := json.Unmarshal([]byte(line), &report); err != nil {
		t.Fatal(err)
	}
	if report.Engine.ServerVersion != "99.0.0-selected" || report.Engine.Transport != "unix" || report.Engine.Desktop == nil || !*report.Engine.Rootless {
		t.Fatalf("report does not describe the selected engine: %+v", report.Engine)
	}
	if report.Verdict(Engine) != Observed || report.Verdict(BlockIODelay) != Unknown {
		t.Fatalf("verdicts:\n%s", report.String())
	}
	if strings.Contains(line, "secret-host") || strings.Contains(line, "secret-user") || strings.Contains(line, dir) {
		t.Fatalf("report carries engine details: %s", line)
	}
	mu.Lock()
	defer mu.Unlock()
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
