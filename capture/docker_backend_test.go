package capture

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// fakeEngine answers the Engine API calls of dockerBackend from fixed
// documents and records each call as "METHOD path?query" without the version
// prefix.
type fakeEngine struct {
	mu     sync.Mutex
	calls  []string
	routes map[string]func(http.ResponseWriter, *http.Request)
}

var engineVersionPrefix = regexp.MustCompile(`^/v[0-9.]+`)

func (e *fakeEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := engineVersionPrefix.ReplaceAllString(r.URL.Path, "")
	key := r.Method + " " + path
	e.mu.Lock()
	e.calls = append(e.calls, key+"?"+r.URL.RawQuery)
	route := e.routes[key]
	e.mu.Unlock()
	if path == "/_ping" {
		w.Header().Set("API-Version", "1.47")
		_, _ = io.WriteString(w, "OK")
		return
	}
	if route == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"no such object"}`)
		return
	}
	route(w, r)
}

// set routes key to handler; a nil handler removes the route.
func (e *fakeEngine) set(key string, handler func(http.ResponseWriter, *http.Request)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if handler == nil {
		delete(e.routes, key)
		return
	}
	e.routes[key] = handler
}

func (e *fakeEngine) json(key string, status int, body string) {
	e.set(key, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
}

func (e *fakeEngine) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = nil
}

func (e *fakeEngine) called(prefix string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, call := range e.calls {
		if strings.HasPrefix(call, prefix) {
			return true
		}
	}
	return false
}

// withFakeEngine points every backend client at a fake Engine API.
func withFakeEngine(t *testing.T) *fakeEngine {
	t.Helper()
	engine := &fakeEngine{routes: map[string]func(http.ResponseWriter, *http.Request){}}
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	original := newDockerClient
	t.Cleanup(func() { newDockerClient = original })
	newDockerClient = func(context.Context) (*testcontainers.DockerClient, error) {
		client, err := mobycl.New(mobycl.WithHost("tcp://"+strings.TrimPrefix(server.URL, "http://")))
		if err != nil {
			return nil, err
		}
		return &testcontainers.DockerClient{Client: client}, nil
	}
	return engine
}

const engineRunningTarget = `{"Id":"target","Image":"sha256:helper","State":{"Running":true,"Paused":false,"Restarting":false,"StartedAt":"2026-10-30T01:02:03.000000004Z"},` +
	`"HostConfig":{"NetworkMode":"bridge"},"NetworkSettings":{"SandboxID":"sandbox","Networks":{"ceph":{"MacAddress":"02:42:ac:11:00:02"}}}}`

func TestDockerBackendTargetReadsGenerationAndEndpointDriver(t *testing.T) {
	engine := withFakeEngine(t)
	engine.json("GET /containers/target/json", http.StatusOK, engineRunningTarget)
	engine.json("GET /networks/ceph", http.StatusOK, `{"Name":"ceph","Driver":"bridge"}`)
	state, err := dockerBackend{}.target(t.Context(), "target", "ceph")
	if err != nil {
		t.Fatal(err)
	}
	want := targetState{running: true, networkMode: "bridge", sandboxID: "sandbox", startedAt: "2026-10-30T01:02:03.000000004Z", driver: "bridge", mac: "02:42:ac:11:00:02"}
	if state != want {
		t.Fatalf("got %+v, want %+v", state, want)
	}

	// A restarting container is not the running generation, and a target off
	// the requested network has no endpoint and needs no network lookup.
	engine.json("GET /containers/target/json", http.StatusOK, strings.Replace(engineRunningTarget, `"Restarting":false`, `"Restarting":true`, 1))
	engine.reset()
	state, err = dockerBackend{}.target(t.Context(), "target", "other")
	if err != nil || state.running || state.mac != "" || state.driver != "" || engine.called("GET /networks/") {
		t.Fatalf("restarting off-network target: %+v %v", state, err)
	}

	engine.json("GET /containers/target/json", http.StatusOK, engineRunningTarget)
	engine.set("GET /networks/ceph", nil)
	if _, err := (dockerBackend{}).target(t.Context(), "target", "ceph"); err == nil || !strings.Contains(err.Error(), "inspect network") {
		t.Fatalf("a failed network lookup was not reported: %v", err)
	}
	if _, err := (dockerBackend{}).target(t.Context(), "removed", "ceph"); !errdefs.IsNotFound(err) {
		t.Fatalf("a removed target is not NotFound: %v", err)
	}
}

func TestDockerBackendHelperStateAndPlatform(t *testing.T) {
	engine := withFakeEngine(t)
	engine.json("GET /containers/helper/json", http.StatusOK, `{"Id":"helper","State":{"Running":false,"Restarting":false,"ExitCode":3,"FinishedAt":"2026-10-30T01:02:03.5+09:00"}}`)
	state, err := dockerBackend{}.state(t.Context(), "helper")
	if err != nil || state.running || state.exitCode != 3 || state.finishedAt.String() != "2026-10-29 16:02:03.5 +0000 UTC" {
		t.Fatalf("exited helper: %+v %v", state, err)
	}
	engine.json("GET /containers/helper/json", http.StatusOK, `{"Id":"helper","State":{"Running":false,"Restarting":true,"ExitCode":0,"FinishedAt":"2026-10-30T01:02:03Z"}}`)
	if state, err := (dockerBackend{}).state(t.Context(), "helper"); err != nil || !state.running || !state.finishedAt.IsZero() {
		t.Fatalf("a restarting helper is still running and has no finish time: %+v %v", state, err)
	}
	engine.json("GET /containers/helper/json", http.StatusOK, `{"Id":"helper"}`)
	if _, err := (dockerBackend{}).state(t.Context(), "helper"); err == nil {
		t.Fatal("a helper without state was accepted")
	}

	engine.json("GET /images/sha256:helper/json", http.StatusOK, `{"Id":"sha256:helper","Os":"linux","Architecture":"arm64","Variant":"v8"}`)
	if platform, err := (dockerBackend{}).platform(t.Context(), "sha256:helper"); err != nil || platform != "linux/arm64/v8" {
		t.Fatalf("platform: %q %v", platform, err)
	}
	engine.json("GET /images/sha256:helper/json", http.StatusOK, `{"Id":"sha256:helper","Os":"linux","Architecture":"amd64"}`)
	if platform, err := (dockerBackend{}).platform(t.Context(), "sha256:helper"); err != nil || platform != "linux/amd64" {
		t.Fatalf("platform without variant: %q %v", platform, err)
	}
}

// engineFrame encodes one stdcopy frame of the Engine's multiplexed log stream.
func engineFrame(stream byte, payload string) []byte {
	header := make([]byte, 8)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
	return append(header, payload...)
}

func TestDockerBackendLogsSplitStreamsAndCapEach(t *testing.T) {
	engine := withFakeEngine(t)
	engine.set("GET /containers/helper/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stdout") != "1" || r.URL.Query().Get("stderr") != "1" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.docker.multiplexed-stream")
		_, _ = w.Write(append(append(engineFrame(1, "stdout-0123456789"), engineFrame(2, "err")...), engineFrame(1, "more")...))
	})
	stdout, stderr, err := dockerBackend{}.logs(t.Context(), "helper", 10)
	if err != nil || string(stdout) != "stdout-012" || string(stderr) != "err" {
		t.Fatalf("logs: %q %q %v", stdout, stderr, err)
	}
}

func encodedPathStat(t *testing.T, mode os.FileMode) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{"name": "capture.pcap", "size": 4, "mode": mode})
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func TestDockerBackendExistsRequiresARegularFile(t *testing.T) {
	engine := withFakeEngine(t)
	for name, tc := range map[string]struct {
		mode os.FileMode
		want bool
	}{"regular": {0o644, true}, "directory": {os.ModeDir | 0o755, false}} {
		t.Run(name, func(t *testing.T) {
			engine.set("HEAD /containers/helper/archive", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Docker-Container-Path-Stat", encodedPathStat(t, tc.mode))
			})
			if exists, err := (dockerBackend{}).exists(t.Context(), "helper", "/capture/capture.pcap"); err != nil || exists != tc.want {
				t.Fatalf("exists: %v %v", exists, err)
			}
		})
	}
	engine.set("HEAD /containers/helper/archive", nil)
	if exists, err := (dockerBackend{}).exists(t.Context(), "helper", "/capture/capture.pcap"); err != nil || exists {
		t.Fatalf("a missing path is not absent: %v %v", exists, err)
	}
	engine.json("HEAD /containers/helper/archive", http.StatusInternalServerError, `{"message":"daemon failure"}`)
	if _, err := (dockerBackend{}).exists(t.Context(), "helper", "/capture/capture.pcap"); err == nil || errdefs.IsNotFound(err) {
		t.Fatalf("a daemon failure was not reported: %v", err)
	}
}

func engineArchive(t *testing.T, typeflag byte, content string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	archive := tar.NewWriter(&buffer)
	header := &tar.Header{Name: "capture.pcap", Typeflag: typeflag, Mode: 0o644}
	if typeflag == tar.TypeReg {
		header.Size = int64(len(content))
	}
	if err := archive.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(archive, content); err != nil && typeflag == tar.TypeReg {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestDockerBackendCopyReturnsOnlyARegularFile(t *testing.T) {
	engine := withFakeEngine(t)
	serve := func(typeflag byte) {
		engine.set("GET /containers/helper/archive", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("path") != "/capture/capture.pcap" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("X-Docker-Container-Path-Stat", encodedPathStat(t, 0o644))
			w.Header().Set("Content-Type", "application/x-tar")
			_, _ = w.Write(engineArchive(t, typeflag, "pcap"))
		})
	}
	serve(tar.TypeReg)
	file, err := dockerBackend{}.copy(t.Context(), "helper", "/capture/capture.pcap")
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(file)
	if err != nil || string(content) != "pcap" {
		t.Fatalf("copy: %q %v", content, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	serve(tar.TypeSymlink)
	if _, err := (dockerBackend{}).copy(t.Context(), "helper", "/capture/capture.pcap"); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("a symlink was copied: %v", err)
	}
	engine.set("GET /containers/helper/archive", nil)
	if _, err := (dockerBackend{}).copy(t.Context(), "helper", "/capture/capture.pcap"); !errdefs.IsNotFound(err) {
		t.Fatalf("a missing file is not NotFound: %v", err)
	}
}

func TestDockerBackendSignalAndRemoveSendTheirOptions(t *testing.T) {
	engine := withFakeEngine(t)
	engine.set("POST /containers/helper/kill", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	engine.set("DELETE /containers/helper", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	if err := (dockerBackend{}).signal(t.Context(), "helper", "SIGINT"); err != nil || !engine.called("POST /containers/helper/kill?signal=SIGINT") {
		t.Fatalf("signal: %v", err)
	}
	if err := (dockerBackend{}).remove(t.Context(), "helper"); err != nil || !engine.called("DELETE /containers/helper?force=1&v=1") {
		t.Fatalf("remove: %v", err)
	}
	if err := (dockerBackend{}).signal(t.Context(), "removed", "SIGINT"); !errdefs.IsNotFound(err) {
		t.Fatalf("signalling a removed helper: %v", err)
	}
}

func TestDockerBackendReportsAnUnavailableClient(t *testing.T) {
	original := newDockerClient
	t.Cleanup(func() { newDockerClient = original })
	unavailable := errors.New("docker unavailable")
	newDockerClient = func(context.Context) (*testcontainers.DockerClient, error) { return nil, unavailable }
	if _, err := (dockerBackend{}).target(t.Context(), "target", "ceph"); !errors.Is(err, unavailable) {
		t.Fatalf("target: %v", err)
	}
	if _, err := (dockerBackend{}).copy(t.Context(), "helper", "/capture/capture.pcap"); !errors.Is(err, unavailable) {
		t.Fatalf("copy: %v", err)
	}
}
