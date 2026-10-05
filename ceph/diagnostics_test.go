package ceph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type diagnosticsTestContainer struct {
	testcontainers.Container
	id   string
	exec func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error)
}

func (c *diagnosticsTestContainer) GetContainerID() string { return c.id }
func (c *diagnosticsTestContainer) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if c.exec != nil {
		return c.exec(ctx, argv, opts...)
	}
	return 0, strings.NewReader(diagnosticsSupervisorResult("native observation", 0, false, false)), nil
}

type diagnosticsTestDocker struct {
	inspect func(context.Context, string, dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error)
	logs    func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error)
	network func(context.Context, string, dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error)
}

func (d diagnosticsTestDocker) ContainerInspect(ctx context.Context, id string, opts dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
	if d.inspect != nil {
		return d.inspect(ctx, id, opts)
	}
	return dockerclient.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id, Config: &dockercontainer.Config{Tty: true}, State: &dockercontainer.State{Running: true}}}, nil
}
func (d diagnosticsTestDocker) ContainerLogs(ctx context.Context, id string, opts dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
	if d.logs != nil {
		return d.logs(ctx, id, opts)
	}
	return io.NopCloser(strings.NewReader("daemon log")), nil
}
func (d diagnosticsTestDocker) NetworkInspect(ctx context.Context, id string, opts dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error) {
	if d.network != nil {
		return d.network(ctx, id, opts)
	}
	return dockerclient.NetworkInspectResult{Network: dockernetwork.Inspect{Network: dockernetwork.Network{ID: id, Name: "public", Driver: "bridge"}}}, nil
}

func diagnosticsSupervisorResult(output string, exit int, timedOut, clipped bool) string {
	data, _ := json.Marshal(map[string]any{"output": base64.StdEncoding.EncodeToString([]byte(output)), "exit_code": exit, "timed_out": timedOut, "truncated": clipped})
	return string(data)
}

func diagnosticsUnitConfig(t *testing.T) DiagnosticsConfig {
	t.Helper()
	config, err := normalizeDiagnosticsConfig(DiagnosticsConfig{Timeout: time.Second, OperationTimeout: 200 * time.Millisecond, MaxOutputBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func diagnosticsUnitReport() *DiagnosticsReport {
	return &DiagnosticsReport{SchemaVersion: 1, StartedAt: time.Now().UTC()}
}

func TestDiagnosticsConfigLimits(t *testing.T) {
	defaults, err := normalizeDiagnosticsConfig(DiagnosticsConfig{})
	if err != nil || defaults.Timeout != time.Minute || defaults.OperationTimeout != 10*time.Second || defaults.MaxOutputBytes != 64<<10 || defaults.LogTail != 1000 || defaults.Concurrency != 4 || defaults.MaxContainers != 128 {
		t.Fatalf("unexpected defaults: %+v, %v", defaults, err)
	}
	cases := []struct {
		name   string
		config DiagnosticsConfig
	}{
		{"negative-total-timeout", DiagnosticsConfig{Timeout: -1}},
		{"excess-total-timeout", DiagnosticsConfig{Timeout: 5*time.Minute + 1}},
		{"negative-probe-timeout", DiagnosticsConfig{OperationTimeout: -1}},
		{"excess-probe-timeout", DiagnosticsConfig{OperationTimeout: time.Minute + 1}},
		{"negative-bytes", DiagnosticsConfig{MaxOutputBytes: -1}},
		{"excess-bytes", DiagnosticsConfig{MaxOutputBytes: 1<<20 + 1}},
		{"negative-tail", DiagnosticsConfig{LogTail: -1}},
		{"excess-tail", DiagnosticsConfig{LogTail: 10001}},
		{"negative-workers", DiagnosticsConfig{Concurrency: -1}},
		{"excess-workers", DiagnosticsConfig{Concurrency: 9}},
		{"negative-containers", DiagnosticsConfig{MaxContainers: -1}},
		{"excess-containers", DiagnosticsConfig{MaxContainers: 129}},
		{"excess-extras", DiagnosticsConfig{AdditionalContainers: make([]DiagnosticsContainer, 65)}},
		{"excess-secret-count", DiagnosticsConfig{RedactValues: make([]string, 129)}},
		{"excess-secret-size", DiagnosticsConfig{RedactValues: []string{strings.Repeat("s", 4097)}}},
		{"long-role", DiagnosticsConfig{AdditionalContainers: []DiagnosticsContainer{{Role: strings.Repeat("r", 65)}}}},
		{"long-name", DiagnosticsConfig{AdditionalContainers: []DiagnosticsContainer{{Name: strings.Repeat("n", 129)}}}},
		{"nul-role", DiagnosticsConfig{AdditionalContainers: []DiagnosticsContainer{{Role: "client\x00"}}}},
		{"newline-name", DiagnosticsConfig{AdditionalContainers: []DiagnosticsContainer{{Name: "client\nother"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeDiagnosticsConfig(tc.config); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	for _, config := range []DiagnosticsConfig{
		{Timeout: 1, OperationTimeout: 1, MaxOutputBytes: 1, LogTail: 1, Concurrency: 1, MaxContainers: 1},
		{Timeout: 5 * time.Minute, OperationTimeout: time.Minute, MaxOutputBytes: 1 << 20, LogTail: 10000, Concurrency: 8, MaxContainers: 128, AdditionalContainers: make([]DiagnosticsContainer, 64), RedactValues: make([]string, 128)},
	} {
		if _, err := normalizeDiagnosticsConfig(config); err != nil {
			t.Fatalf("valid boundary rejected: %v", err)
		}
	}
}

func TestDiagnosticsSnapshotPartialInventory(t *testing.T) {
	primary := &diagnosticsTestContainer{id: "primary"}
	mgr := &diagnosticsTestContainer{id: "manager"}
	service := &diagnosticsTestContainer{id: "partial-gateway"}
	cluster := &Container{
		Container: primary, manager: mgr, controlPlane: primary, closed: true, monitorTerminated: true,
		monitors: map[string]*MonitorContainer{"partial": {}, "uncreated": nil},
		managers: map[string]*ManagerContainer{"a": {Container: mgr}},
		osds:     map[int]*OSDContainer{0: {ID: 0}, 1: nil},
		services: map[string]testcontainers.Container{"rgw:bootstrapping": service, "mds.tenant-0": nil},
		gateways: map[string]*RGWContainer{"secrets": {AccessKey: "access-private", SecretKey: "s3-private"}},
		network:  &testcontainers.DockerNetwork{ID: "public-id", Name: "public"},
		keyring:  []byte("[client.admin]\n key = cephx-private\n"),
	}
	// The generic service owns the gateway before its public descriptor exists.
	config := diagnosticsUnitConfig(t)
	config.RedactValues = []string{"app-private"}
	config.AdditionalContainers = []DiagnosticsContainer{{Container: primary}, {Role: "mirror", Name: "external", Container: &diagnosticsTestContainer{id: "external"}}, {}}
	snapshot, err := cluster.diagnosticsSnapshot(t.Context(), config)
	if err != nil || !snapshot.closed || snapshot.control != primary {
		t.Fatalf("partial snapshot failed: %+v, %v", snapshot, err)
	}
	ids := make(map[string]int)
	missing := 0
	foundGateway := false
	for _, target := range snapshot.containers {
		if target.Container == nil {
			missing++
			continue
		}
		ids[target.Container.GetContainerID()]++
		if target.Container.GetContainerID() == "primary" && (target.Role != "mon" || target.Name != "a") {
			t.Fatalf("additional label replaced the owned MON identity: %+v", target)
		}
		foundGateway = foundGateway || target.Name == "rgw:bootstrapping" && target.Role == "rgw"
	}
	if ids["primary"] != 1 || ids["manager"] != 1 || ids["external"] != 1 || !foundGateway || missing != 4 {
		t.Fatalf("lost partial handles or duplicated native IDs: ids=%v, missing=%d, gateway=%v", ids, missing, foundGateway)
	}
	for _, secret := range []string{"app-private", "access-private", "s3-private", "cephx-private"} {
		if !slices.Contains(snapshot.secrets, secret) {
			t.Fatalf("known secret missing from redaction snapshot: %q", secret)
		}
	}
	if len(snapshot.networks) != 1 || snapshot.networks[0].id != "public-id" {
		t.Fatalf("network-only bootstrap information lost: %v", snapshot.networks)
	}
	config.MaxContainers = 2
	limited, err := cluster.diagnosticsSnapshot(t.Context(), config)
	if err != nil || len(limited.containers) != 2 || limited.omitted != len(snapshot.containers)-2 {
		t.Fatalf("container bound not applied: %+v, %v", limited, err)
	}
	var nilCluster *Container
	if _, err := nilCluster.diagnosticsSnapshot(t.Context(), config); err == nil {
		t.Fatal("nil cluster accepted")
	}
	if empty, err := (&Container{}).diagnosticsSnapshot(t.Context(), diagnosticsUnitConfig(t)); err != nil || len(empty.containers) != 0 {
		t.Fatalf("zero-value partial cluster panicked or fabricated handles: %+v, %v", empty, err)
	}
	t.Run("dedicated-control", func(t *testing.T) {
		var controlQueries atomic.Int32
		primary.exec = func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
			t.Error("native diagnostic query executed on the MON instead of dedicated control")
			return 0, nil, errors.New("wrong control target")
		}
		control := &diagnosticsTestContainer{id: "dedicated-control", exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
			controlQueries.Add(1)
			return 0, strings.NewReader(diagnosticsSupervisorResult("control result", 0, false, false)), nil
		}}
		cluster.controlPlane = control
		config := diagnosticsUnitConfig(t)
		snapshot, err := cluster.diagnosticsSnapshot(t.Context(), config)
		if err != nil || snapshot.control != control {
			t.Fatalf("dedicated CLI target lost: %+v, %v", snapshot, err)
		}
		matches := 0
		for _, target := range snapshot.containers {
			if target.Container == control && target.Role == "control" && target.Name == "cli" {
				matches++
			}
		}
		if matches != 1 {
			t.Fatalf("dedicated control inventory count: %d", matches)
		}
		_, _ = collectDiagnostics(t.Context(), config, snapshot, diagnosticsTestDocker{}, diagnosticsUnitReport())
		if controlQueries.Load() != int32(len(diagnosticCephQueries)) {
			t.Fatalf("dedicated control executed %d queries; expected %d", controlQueries.Load(), len(diagnosticCephQueries))
		}
	})
}

func TestDiagnosticsSnapshotLockDeadline(t *testing.T) {
	for _, lock := range []string{"topology", "control", "config"} {
		t.Run(lock, func(t *testing.T) {
			cluster := &Container{}
			var unlock func()
			switch lock {
			case "topology":
				cluster.mu.Lock()
				unlock = cluster.mu.Unlock
			case "control":
				cluster.controlMu.Lock()
				unlock = cluster.controlMu.Unlock
			case "config":
				cluster.configMu.Lock()
				unlock = cluster.configMu.Unlock
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err := cluster.diagnosticsSnapshot(ctx, diagnosticsUnitConfig(t))
			unlock()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 500*time.Millisecond {
				t.Fatalf("held mutex ignored deadline: %v", err)
			}
			if !cluster.mu.TryLock() {
				t.Fatal("snapshot retained topology mutex after cancellation")
			}
			cluster.mu.Unlock()
		})
	}
	cluster := &Container{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cluster.diagnosticsSnapshot(ctx, diagnosticsUnitConfig(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled snapshot acquired an available lock: %v", err)
	}
}

func TestDiagnosticsTypedNilHandlesRemainInspectable(t *testing.T) {
	var missing *diagnosticsTestContainer
	cluster := &Container{
		Container: missing, controlPlane: missing, manager: missing,
		monitors: map[string]*MonitorContainer{"joining": {Container: missing}},
		osds:     map[int]*OSDContainer{0: {Container: missing, ID: 0}},
		services: map[string]testcontainers.Container{"rgw:partial": missing},
	}
	config := diagnosticsUnitConfig(t)
	config.AdditionalContainers = []DiagnosticsContainer{{Role: "mirror", Name: "failed-mirror", Container: missing}}
	snapshot, err := cluster.diagnosticsSnapshot(t.Context(), config)
	if err != nil || snapshot.control != nil {
		t.Fatalf("typed-nil control was not normalized: %+v, %v", snapshot, err)
	}
	foundExtra, foundOwned := false, false
	for _, target := range snapshot.containers {
		if target.Container != nil {
			t.Fatalf("snapshot retained a typed-nil handle: %+v", target)
		}
		foundExtra = foundExtra || target.Role == "mirror" && target.Name == "failed-mirror"
		foundOwned = foundOwned || target.Role == "rgw" && target.Name == "rgw:partial"
	}
	if !foundExtra || !foundOwned {
		t.Fatalf("missing owned or caller-owned creation attempts were discarded: %+v", snapshot.containers)
	}
	var queries atomic.Int32
	docker := diagnosticsTestDocker{inspect: func(context.Context, string, dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
		queries.Add(1)
		return dockerclient.ContainerInspectResult{}, nil
	}}
	report, err := collectDiagnostics(t.Context(), config, snapshot, docker, diagnosticsUnitReport())
	if err == nil || report.Complete || queries.Load() != 0 {
		t.Fatalf("missing handles claimed success or queried Docker: complete=%v, queries=%d, err=%v", report.Complete, queries.Load(), err)
	}
	for _, artifact := range report.Artifacts {
		if artifact.Error == "" || artifact.Data != "" {
			t.Fatalf("missing handle did not retain an explicit artifact failure: %+v", artifact)
		}
	}
}

func TestDiagnosticsInspectAllowlist(t *testing.T) {
	inspection := dockercontainer.InspectResponse{
		ID: "owned", Name: "node", Image: "image-id", Path: "private-command", Args: []string{"private-argument"},
		Config:     &dockercontainer.Config{Image: "public-image", Env: []string{"SECRET=private-env"}, Cmd: []string{"private-cmd"}, Labels: map[string]string{"private-label": "private-value"}},
		State:      &dockercontainer.State{Running: false, ExitCode: 7, OOMKilled: true, Error: "private-state-error", Health: &dockercontainer.Health{Log: []*dockercontainer.HealthcheckResult{{Output: "private-health-output"}}}},
		HostConfig: &dockercontainer.HostConfig{NetworkMode: "host", Binds: []string{"private-host-path:/data"}},
		Mounts:     []dockercontainer.MountPoint{{Source: "private-mount"}},
	}
	data, err := json.Marshal(diagnosticContainerInspection(inspection))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-") || !strings.Contains(string(data), `"exit_code":7`) || !strings.Contains(string(data), `"oom_killed":true`) || !strings.Contains(string(data), `"network_mode":"host"`) {
		t.Fatalf("inspect leaked excluded fields or lost failure state: %s", data)
	}
	nw := dockernetwork.Inspect{
		Network:    dockernetwork.Network{ID: "network", Name: "public", Driver: "bridge", Options: map[string]string{"private-option": "secret"}, Labels: map[string]string{"private-label": "secret"}, IPAM: dockernetwork.IPAM{Config: []dockernetwork.IPAMConfig{{Subnet: netip.MustParsePrefix("172.30.0.0/24"), Gateway: netip.MustParseAddr("172.30.0.1")}}}},
		Containers: map[string]dockernetwork.EndpointResource{"owned": {Name: "owned-node"}, "external": {Name: "explicit-client"}, "foreign": {Name: "private-foreign-node"}},
	}
	data, err = json.Marshal(diagnosticNetworkInspection(nw, map[string]bool{"owned": true, "external": true}))
	if err != nil || strings.Contains(string(data), "private-") || strings.Contains(string(data), "foreign") || !strings.Contains(string(data), "explicit-client") || !strings.Contains(string(data), "172.30.0.0/24") {
		t.Fatalf("network inspect leaked unselected endpoints or dropped selected subnet: %s, %v", data, err)
	}
}

type diagnosticsCloseReader struct {
	io.Reader
	closed atomic.Int32
}

func (r *diagnosticsCloseReader) Close() error { r.closed.Add(1); return nil }

type diagnosticsByteReader struct{ io.Reader }

func (r diagnosticsByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

func diagnosticsFrame(kind byte, data string) []byte {
	header := make([]byte, 8)
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return append(header, data...)
}

func TestDiagnosticLogsBoundsAndFrames(t *testing.T) {
	hugeHeader := make([]byte, 8)
	hugeHeader[0] = 1
	binary.BigEndian.PutUint32(hugeHeader[4:], ^uint32(0))
	cases := []struct {
		name      string
		tty       bool
		input     []byte
		want      string
		truncated bool
		wantErr   bool
	}{
		{"tty-tail", true, []byte("old-last"), "last", true, false},
		{"exact-bound", true, []byte("last"), "last", false, false},
		{"split-mux-tail", false, append(diagnosticsFrame(1, "old-"), diagnosticsFrame(2, "last")...), "last", true, false},
		{"empty", false, nil, "", false, false},
		{"huge-advertised-frame", false, append(hugeHeader, []byte("x")...), "x", false, true},
		{"short-header", false, []byte{1, 0}, "", false, true},
		{"invalid-frame-kind", false, diagnosticsFrame(3, "daemon-error"), "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := diagnosticsUnitConfig(t)
			config.MaxOutputBytes, config.LogTail = 4, 7
			reader := &diagnosticsCloseReader{Reader: diagnosticsByteReader{bytes.NewReader(tc.input)}}
			docker := diagnosticsTestDocker{
				inspect: func(_ context.Context, _ string, opts dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
					if opts.Size {
						t.Error("expensive container size inspection requested")
					}
					return dockerclient.ContainerInspectResult{Container: dockercontainer.InspectResponse{Config: &dockercontainer.Config{Tty: tc.tty}}}, nil
				},
				logs: func(_ context.Context, id string, opts dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
					if id != "owned" || !opts.ShowStdout || !opts.ShowStderr || opts.Follow || opts.Details || opts.Tail != "7" {
						t.Errorf("logs were not bounded read-only tail requests: %s, %+v", id, opts)
					}
					return reader, nil
				},
			}
			output, clipped, err := diagnosticLogs(t.Context(), docker, "owned", config)
			if output != tc.want || clipped != tc.truncated || (err != nil) != tc.wantErr || reader.closed.Load() == 0 {
				t.Fatalf("unexpected log result: %q, clipped=%v, closed=%d, err=%v", output, clipped, reader.closed.Load(), err)
			}
		})
	}
}

type diagnosticsBlockingReader struct {
	closed chan struct{}
	once   sync.Once
	exited chan struct{}
}

func (r *diagnosticsBlockingReader) Read([]byte) (int, error) {
	<-r.closed
	close(r.exited)
	return 0, io.ErrClosedPipe
}
func (r *diagnosticsBlockingReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func TestDiagnosticLogsCancelClosesBlockedStream(t *testing.T) {
	reader := &diagnosticsBlockingReader{closed: make(chan struct{}), exited: make(chan struct{})}
	docker := diagnosticsTestDocker{logs: func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
		return reader, nil
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, _, err := diagnosticLogs(ctx, docker, "owned", diagnosticsUnitConfig(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stream cancellation cause lost: %v", err)
	}
	select {
	case <-reader.exited:
	default:
		t.Fatal("blocked stream reader did not finish before return")
	}
}

func TestDiagnosticCephCommandProtocol(t *testing.T) {
	cases := []struct {
		name      string
		result    string
		code      int
		want      string
		truncated bool
		wantErr   bool
		deadline  bool
	}{
		{"success", diagnosticsSupervisorResult("abc", 0, false, false), 0, "abc", false, false, false},
		{"native-failure-retains-output", diagnosticsSupervisorResult("abc", 2, false, false), 0, "abc", false, true, false},
		{"timeout-retains-output", diagnosticsSupervisorResult("abc", -9, true, false), 0, "abc", false, true, true},
		{"native-clipped", diagnosticsSupervisorResult("abc", 0, false, true), 0, "abc", true, false, false},
		{"oversize-output", diagnosticsSupervisorResult("abcdef", 0, false, false), 0, "abcd", true, false, false},
		{"missing-exit", `{"output":"YWJj"}`, 0, "", false, true, false},
		{"null-exit", `{"output":"YWJj","exit_code":null}`, 0, "", false, true, false},
		{"missing-output", `{"exit_code":0,"timed_out":false,"truncated":false}`, 0, "", false, true, false},
		{"null-output", `{"output":null,"exit_code":0,"timed_out":false,"truncated":false}`, 0, "", false, true, false},
		{"missing-timeout", `{"output":"YWJj","exit_code":0,"truncated":false}`, 0, "", false, true, false},
		{"null-timeout", `{"output":"YWJj","exit_code":0,"timed_out":null,"truncated":false}`, 0, "", false, true, false},
		{"missing-truncated", `{"output":"YWJj","exit_code":0,"timed_out":false}`, 0, "", false, true, false},
		{"null-truncated", `{"output":"YWJj","exit_code":0,"timed_out":false,"truncated":null}`, 0, "", false, true, false},
		{"invalid-json", `{"output":`, 0, "", false, true, false},
		{"invalid-base64", `{"output":"???","exit_code":0,"timed_out":false,"truncated":false}`, 0, "", false, true, false},
		{"supervisor-failure", "private-supervisor-stderr", 9, "", false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			control := &diagnosticsTestContainer{exec: func(_ context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
				if len(argv) != 11 || !slices.Equal(argv[:2], []string{"python3", "-c"}) || argv[2] != diagnosticCommandScript || argv[3] != "4" || !slices.Equal(argv[5:], []string{"ceph", "--connect-timeout", "2", "status", "--format", "json"}) || len(opts) != 1 {
					t.Fatalf("supervisor did not receive the bounded fixed argv: %v", argv)
				}
				seconds, err := strconv.ParseFloat(argv[4], 64)
				if err != nil || seconds <= 0 || seconds > 0.75 {
					t.Fatalf("supervisor deadline outside caller budget: %q, %v", argv[4], err)
				}
				return tc.code, strings.NewReader(tc.result), nil
			}}
			output, clipped, err := diagnosticCephCommand(ctx, control, []string{"status", "--format", "json"}, 4)
			if output != tc.want || clipped != tc.truncated || (err != nil) != tc.wantErr || tc.deadline && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected supervisor result: %q, clipped=%v, err=%v", output, clipped, err)
			}
			if err != nil && strings.Contains(err.Error(), "private-supervisor-stderr") {
				t.Fatal("supervisor stderr was embedded in an error")
			}
		})
	}
	control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		t.Fatal("Exec called without usable deadline")
		return 0, nil, nil
	}}
	if _, _, err := diagnosticCephCommand(t.Context(), control, nil, 4); err == nil {
		t.Fatal("unbounded command accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := diagnosticCephCommand(ctx, nil, nil, 4); err == nil {
		t.Fatal("missing control handle accepted")
	}
}

func TestDiagnosticCephQueryAllowlist(t *testing.T) {
	want := map[string][]string{
		"version": {"--version"}, "status": {"status", "--format", "json"}, "health": {"health", "detail", "--format", "json"},
		"pgs": {"pg", "dump", "pgs_brief", "--format", "json"}, "quorum": {"quorum_status", "--format", "json"},
		"monmap": {"mon", "dump", "--format", "json"}, "mgrmap": {"mgr", "dump", "--format", "json"},
		"osdmap": {"osd", "dump", "--format", "json"}, "fsmap": {"fs", "dump", "--format", "json"},
	}
	if len(diagnosticCephQueries) != len(want) {
		t.Fatal("fixed read-only query inventory changed")
	}
	for _, query := range diagnosticCephQueries {
		if !slices.Equal(query.args, want[query.name]) {
			t.Fatalf("unexpected Ceph query %q: %v", query.name, query.args)
		}
		delete(want, query.name)
	}
	if len(want) != 0 {
		t.Fatalf("missing queries: %v", want)
	}
}

type diagnosticsPartialReader struct{ delivered bool }

func (r *diagnosticsPartialReader) Read(data []byte) (int, error) {
	if r.delivered {
		return 0, io.ErrUnexpectedEOF
	}
	r.delivered = true
	return copy(data, "partial daemon log"), io.ErrUnexpectedEOF
}

func TestCollectDiagnosticsPreservesPartialFailures(t *testing.T) {
	config := diagnosticsUnitConfig(t)
	control := &diagnosticsTestContainer{id: "owned", exec: func(_ context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
		if argv[8] == "status" {
			return 0, strings.NewReader(diagnosticsSupervisorResult("partial native status", 1, false, false)), nil
		}
		return 0, strings.NewReader(diagnosticsSupervisorResult("native success", 0, false, false)), nil
	}}
	docker := diagnosticsTestDocker{logs: func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
		return io.NopCloser(&diagnosticsPartialReader{}), nil
	}}
	snapshot := diagnosticsSnapshot{containers: []DiagnosticsContainer{{Role: "mon", Name: "a", Container: control}, {Role: "osd", Name: "missing"}}, networks: []diagnosticNetwork{{id: "public", name: "public"}}, control: control, omitted: 1}
	report, err := collectDiagnostics(t.Context(), config, snapshot, docker, diagnosticsUnitReport())
	if err == nil || report.Complete || report.FinishedAt.IsZero() {
		t.Fatalf("partial failure presented as complete: %+v, %v", report, err)
	}
	found := map[string]bool{}
	for _, artifact := range report.Artifacts {
		switch {
		case artifact.Kind == "container-logs" && artifact.Name == "a":
			found["logs"] = artifact.Data == "partial daemon log" && artifact.Error != ""
		case artifact.Kind == "ceph" && artifact.Name == "status":
			found["native"] = artifact.Data == "partial native status" && artifact.Error != ""
		case artifact.Kind == "container-inspect" && artifact.Name == "a":
			found["inspect"] = artifact.Data != "" && artifact.Error == ""
		case artifact.Kind == "network-inspect":
			found["network"] = artifact.Data != "" && artifact.Error == ""
		case artifact.Kind == "container-inspect" && artifact.Name == "missing":
			found["missing"] = artifact.Data == "" && artifact.Error != ""
		case artifact.Kind == "inventory" && artifact.Name == "container-limit":
			found["omitted"] = artifact.Error != ""
		}
	}
	for _, section := range []string{"logs", "native", "inspect", "network", "missing", "omitted"} {
		if !found[section] {
			t.Fatalf("partial report lost %s: %+v", section, report.Artifacts)
		}
	}
}

func TestCollectDiagnosticsCanceledBeforeQueries(t *testing.T) {
	var queries atomic.Int32
	docker := diagnosticsTestDocker{inspect: func(context.Context, string, dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
		queries.Add(1)
		return dockerclient.ContainerInspectResult{}, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	control := &diagnosticsTestContainer{id: "owned", exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		queries.Add(1)
		return 0, nil, nil
	}}
	report, err := collectDiagnostics(ctx, diagnosticsUnitConfig(t), diagnosticsSnapshot{containers: []DiagnosticsContainer{{Container: control}}, control: control}, docker, diagnosticsUnitReport())
	if !errors.Is(err, context.Canceled) || report.Complete || queries.Load() != 0 {
		t.Fatalf("canceled collection queried Docker or claimed completion: queries=%d, complete=%v, err=%v", queries.Load(), report.Complete, err)
	}
}

func TestCollectDiagnosticsRedactsAllArtifactSurfaces(t *testing.T) {
	const secret = "private-application-secret"
	config := diagnosticsUnitConfig(t)
	control := &diagnosticsTestContainer{id: secret, exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		return 0, strings.NewReader(diagnosticsSupervisorResult(`{"token":"opaque-native-token","observation":"`+secret+`"}`, 1, false, false)), nil
	}}
	docker := diagnosticsTestDocker{
		inspect: func(_ context.Context, id string, _ dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
			if id == "logs-node" {
				return dockerclient.ContainerInspectResult{Container: dockercontainer.InspectResponse{Config: &dockercontainer.Config{Tty: true}}}, nil
			}
			return dockerclient.ContainerInspectResult{}, errors.New("inspect " + secret + ": password=opaque-error-password")
		},
		logs: func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
			return io.NopCloser(strings.NewReader("observation " + secret + ` secret_key="opaque-log-secret`)), nil
		},
		network: func(context.Context, string, dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error) {
			return dockerclient.NetworkInspectResult{}, errors.New("network " + secret)
		},
	}
	snapshot := diagnosticsSnapshot{containers: []DiagnosticsContainer{{Role: secret, Name: secret, Container: control}, {Role: "client", Name: "logs-node", Container: &diagnosticsTestContainer{id: "logs-node"}}}, networks: []diagnosticNetwork{{id: "public", name: secret}}, control: control, secrets: []string{secret}}
	report, err := collectDiagnostics(t.Context(), config, snapshot, docker, diagnosticsUnitReport())
	data, marshalErr := json.Marshal(report)
	if err == nil || marshalErr != nil || !strings.Contains(string(data), "REDACTED") {
		t.Fatalf("redacted failure report unavailable: %s, %v, %v", data, err, marshalErr)
	}
	for _, value := range []string{secret, "opaque-native-token", "opaque-error-password", "opaque-log-secret"} {
		if strings.Contains(string(data), value) || strings.Contains(err.Error(), value) {
			t.Fatalf("secret retained in report or aggregate error: %q", value)
		}
	}
	redact := diagnosticsRedactor([]string{secret})
	for _, raw := range []string{`secret_key="incomplete-private-token`, "key = incomplete-private-key", "Bearer private-bearer-token"} {
		result := redact(raw)
		if strings.Contains(result, "incomplete-private") || strings.Contains(result, "private-bearer-token") {
			t.Fatalf("truncated or plain-text credential retained: %q", result)
		}
	}
}

func TestCollectDiagnosticsTruncationAndUTF8Budget(t *testing.T) {
	for _, raw := range []string{"abcdefgh", "\xff"} {
		t.Run(strconv.Quote(raw), func(t *testing.T) {
			config := diagnosticsUnitConfig(t)
			config.MaxOutputBytes = 1
			control := &diagnosticsTestContainer{id: "owned"}
			docker := diagnosticsTestDocker{logs: func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
				return io.NopCloser(strings.NewReader(raw)), nil
			}}
			report, err := collectDiagnostics(t.Context(), config, diagnosticsSnapshot{containers: []DiagnosticsContainer{{Container: control}}, control: control}, docker, diagnosticsUnitReport())
			if err != nil || report.Complete {
				t.Fatalf("truncation should retain partial report without an error: complete=%v, err=%v", report.Complete, err)
			}
			for _, artifact := range report.Artifacts {
				if len(artifact.Data) > config.MaxOutputBytes || !utf8.ValidString(artifact.Data) {
					t.Fatalf("artifact exceeded byte budget or retained invalid UTF8: %+v", artifact)
				}
			}
		})
	}
}

func TestCollectDiagnosticsHostNetworkNeedsNoNetworkInspect(t *testing.T) {
	config := diagnosticsUnitConfig(t)
	control := &diagnosticsTestContainer{id: "owned"}
	docker := diagnosticsTestDocker{network: func(context.Context, string, dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error) {
		t.Error("shared host network was inspected")
		return dockerclient.NetworkInspectResult{}, nil
	}}
	report, err := collectDiagnostics(t.Context(), config, diagnosticsSnapshot{hostNetwork: true, publicAddress: "127.0.0.1", networks: []diagnosticNetwork{{id: "host", name: "host"}}, control: control}, docker, diagnosticsUnitReport())
	if err != nil || !report.Complete {
		t.Fatalf("host network summary unavailable: %+v, %v", report, err)
	}
	for _, artifact := range report.Artifacts {
		if artifact.Kind == "network-inspect" && strings.Contains(artifact.Data, `"shared":true`) && strings.Contains(artifact.Data, "127.0.0.1") {
			return
		}
	}
	t.Fatal("host network summary missing")
}

func TestCollectDiagnosticsProbeDeadlineRetainsStoppedState(t *testing.T) {
	config := diagnosticsUnitConfig(t)
	config.OperationTimeout = 20 * time.Millisecond
	control := &diagnosticsTestContainer{id: "stopped"}
	stream := &diagnosticsBlockingReader{closed: make(chan struct{}), exited: make(chan struct{})}
	docker := diagnosticsTestDocker{
		inspect: func(_ context.Context, id string, opts dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
			if opts.Size {
				t.Error("container inspection requested filesystem size")
			}
			return dockerclient.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id, Config: &dockercontainer.Config{Tty: true}, State: &dockercontainer.State{Status: "exited", ExitCode: 17}}}, nil
		},
		logs: func(context.Context, string, dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
			return stream, nil
		},
		network: func(_ context.Context, _ string, opts dockerclient.NetworkInspectOptions) (dockerclient.NetworkInspectResult, error) {
			if opts.Verbose || opts.Scope != "" {
				t.Error("network inspection expanded beyond the fixed local request")
			}
			return dockerclient.NetworkInspectResult{Network: dockernetwork.Inspect{Network: dockernetwork.Network{ID: "owned-network", Driver: "bridge"}}}, nil
		},
	}
	report, err := collectDiagnostics(t.Context(), config, diagnosticsSnapshot{containers: []DiagnosticsContainer{{Name: "stopped", Role: "osd", Container: control}}, networks: []diagnosticNetwork{{id: "owned-network", name: "public"}}, control: control}, docker, diagnosticsUnitReport())
	if err == nil || report.Complete {
		t.Fatalf("timed-out log stream claimed success: %+v, %v", report, err)
	}
	foundState, foundTimeout, foundNative := false, false, false
	for _, artifact := range report.Artifacts {
		if artifact.Kind == "container-inspect" {
			foundState = artifact.Error == "" && strings.Contains(artifact.Data, `"exit_code":17`) && strings.Contains(artifact.Data, `"status":"exited"`)
		}
		if artifact.Kind == "container-logs" {
			foundTimeout = strings.Contains(artifact.Error, context.DeadlineExceeded.Error())
		}
		if artifact.Kind == "ceph" && artifact.Name == "status" {
			foundNative = artifact.Error == "" && artifact.Data == "native observation"
		}
	}
	if !foundState || !foundTimeout || !foundNative {
		t.Fatalf("one timed-out probe discarded other observations: state=%v timeout=%v native=%v", foundState, foundTimeout, foundNative)
	}
	select {
	case <-stream.exited:
	default:
		t.Fatal("probe deadline returned with a live stream reader")
	}
}

func diagnosticsInstalledPython(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX pipes and process groups are unavailable on Windows")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local Python supervisor regression requires installed python3; native image validation covers the runtime")
	}
	return python
}

type diagnosticsPythonResult struct {
	Output    string `json:"output"`
	ExitCode  int    `json:"exit_code"`
	TimedOut  bool   `json:"timed_out"`
	Truncated bool   `json:"truncated"`
}

func diagnosticsRunPythonSupervisor(t *testing.T, python string, limit int, seconds string, child string, args ...string) diagnosticsPythonResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	argv := []string{"-c", diagnosticCommandScript, strconv.Itoa(limit), seconds, python, "-c", child}
	argv = append(argv, args...)
	command := exec.CommandContext(ctx, python, argv...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("actual supervisor failed: %v; stderr=%s", err, stderr.String())
	}
	if len(output) > limit*2+4096 {
		t.Fatalf("actual supervisor exceeded its protocol envelope: %d bytes", len(output))
	}
	var result diagnosticsPythonResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("actual supervisor returned invalid protocol: %v; output=%q", err, output)
	}
	return result
}

func TestDiagnosticPythonSupervisorNonblockingStderr(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	// Ceph's logger applies O_NONBLOCK to stderr. Sharing that pipe with stdout
	// can make large Python CLI writes fail with EAGAIN and exit 1 or 120.
	const child = `import fcntl,os,select,sys
fcntl.fcntl(2,fcntl.F_SETFL,fcntl.fcntl(2,fcntl.F_GETFL)|os.O_NONBLOCK)
sys.stdout.buffer.write(b'O'*(4*1024*1024)); sys.stdout.buffer.flush()
pending=b'E'*(256*1024)
while pending:
 try:
  count=os.write(2,pending)
  pending=pending[count:]
 except BlockingIOError:
  select.select([],[2],[],0.1)
`
	result := diagnosticsRunPythonSupervisor(t, python, 512, "3", child)
	output, err := base64.StdEncoding.DecodeString(result.Output)
	if err != nil || result.ExitCode != 0 || result.TimedOut || !result.Truncated || string(output) != strings.Repeat("O", 512) {
		t.Fatalf("nonblocking stderr broke stdout or stopped drain at byte cap: result=%+v, decoded=%d bytes, err=%v", result, len(output), err)
	}
	t.Log("installed Python executed supervisor: independent stderr pipe, 4MiB stdout plus 256KiB stderr fully drained after a 512-byte cap")
}

func TestDiagnosticPythonSupervisorKillsOwnedProcessGroup(t *testing.T) {
	python := diagnosticsInstalledPython(t)
	pidPath := filepath.Join(t.TempDir(), "child-pid")
	heartbeatPath := filepath.Join(t.TempDir(), "grandchild-heartbeat")
	// Read only this fixture's recorded group identity, even if a failure occurs
	// before the supervisor returns. Never signal arbitrary host processes.
	t.Cleanup(func() {
		data, err := os.ReadFile(pidPath)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || pid <= 0 {
			t.Error("invalid test-owned process group identity")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, python, "-c", "import os,signal,sys\ntry: os.killpg(int(sys.argv[1]),signal.SIGKILL)\nexcept ProcessLookupError: pass", strconv.Itoa(pid))
		if output, err := command.CombinedOutput(); err != nil {
			t.Errorf("clean test-owned process group: %v; %s", err, output)
		}
	})
	const child = `import os,subprocess,sys,time
with open(sys.argv[1],'w') as identity: identity.write(str(os.getpid()))
grandchild="import sys,time\nwith open(sys.argv[1],'ab',buffering=0) as f:\n while True:\n  f.write(b'x'); time.sleep(0.01)"
subprocess.Popen([sys.executable,'-c',grandchild,sys.argv[2]])
while True:
 print('child-running',flush=True)
 time.sleep(0.01)
`
	result := diagnosticsRunPythonSupervisor(t, python, 64, "0.4", child, pidPath, heartbeatPath)
	if !result.TimedOut || result.ExitCode != -9 || !result.Truncated {
		t.Fatalf("deadline did not SIGKILL the owned child group: %+v", result)
	}
	before, err := os.ReadFile(heartbeatPath)
	if err != nil || len(before) == 0 {
		t.Fatalf("grandchild did not demonstrate activity before deadline: %d bytes, %v", len(before), err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(heartbeatPath)
	if err != nil || len(after) != len(before) {
		t.Fatalf("descendant continued after supervisor returned: before=%d, after=%d, err=%v", len(before), len(after), err)
	}
	t.Log("installed Python executed supervisor: timeout SIGKILLed its child group and stopped the descendant heartbeat before return")
}
