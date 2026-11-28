package diagnostics

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	dockercontainer "github.com/moby/moby/api/types/container"
	dockerclient "github.com/moby/moby/client"
)

// processLogEpoch is the time of the first line of the fake logs.
var processLogEpoch = time.Date(2026, 11, 27, 4, 0, 0, 0, time.UTC)

// fakeLogEntry is one log entry of a fake container, at processLogEpoch
// plus at.
type fakeLogEntry struct {
	at     time.Duration
	stream byte
	text   string
	// partial leaves out the newline, as Docker does for a part of a long
	// line; bare leaves out Docker's timestamp.
	partial, bare bool
}

// cephEntry is a native Ceph log entry at processLogEpoch plus at.
func cephEntry(at time.Duration, level, message string) fakeLogEntry {
	return fakeLogEntry{at: at, stream: 2, text: processLogEpoch.Add(at).Format("2006-01-02T15:04:05.000-0700") + " 7f1b8c0a1640 " + level + " " + message}
}

func startEntry(at time.Duration, process string, pid int) fakeLogEntry {
	return cephEntry(at, " 0", "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo), process "+process+", pid "+strconv.Itoa(pid))
}

// fakeLogContainer is one container of processLogDocker: the start time
// that each inspection reports in turn (the last repeats) and its log.
type fakeLogContainer struct {
	started []time.Duration
	// startedAt, when set, is the start time that Docker reports instead.
	startedAt string
	running   bool
	driver    string
	options   map[string]string
	entries   []fakeLogEntry
	logErr    error
}

// processLogDocker is a Docker engine with fake containers. Logs filters by
// Since and prefixes Docker's timestamps, as the engine does.
type processLogDocker struct {
	diagnosticsTestDocker
	containers map[string]*fakeLogContainer
	inspected  map[string]int
	since      map[string]string
}

func newProcessLogDocker(containers map[string]*fakeLogContainer) *processLogDocker {
	return &processLogDocker{containers: containers, inspected: map[string]int{}, since: map[string]string{}}
}

func (d *processLogDocker) ContainerInspect(_ context.Context, id string, _ dockerclient.ContainerInspectOptions) (dockerclient.ContainerInspectResult, error) {
	ctr, ok := d.containers[id]
	if !ok {
		return dockerclient.ContainerInspectResult{}, errors.New("no such container")
	}
	started := ctr.started[min(d.inspected[id], len(ctr.started)-1)]
	d.inspected[id]++
	startedAt := processLogEpoch.Add(started).Format(time.RFC3339Nano)
	if ctr.startedAt != "" {
		startedAt = ctr.startedAt
	}
	driver := ctr.driver
	if driver == "" {
		driver = "json-file"
	}
	return dockerclient.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id,
		Config:     &dockercontainer.Config{},
		State:      &dockercontainer.State{Running: ctr.running, StartedAt: startedAt},
		HostConfig: &dockercontainer.HostConfig{LogConfig: dockercontainer.LogConfig{Type: driver, Config: ctr.options}}}}, nil
}

func (d *processLogDocker) ContainerLogs(_ context.Context, id string, opts dockerclient.ContainerLogsOptions) (dockerclient.ContainerLogsResult, error) {
	ctr := d.containers[id]
	if ctr.logErr != nil {
		return nil, ctr.logErr
	}
	d.since[id] = opts.Since
	since, err := time.Parse(time.RFC3339Nano, opts.Since)
	if err != nil || !opts.Timestamps || !opts.ShowStdout || !opts.ShowStderr {
		return nil, errors.New("unexpected log options")
	}
	var out bytes.Buffer
	for _, entry := range ctr.entries {
		at := processLogEpoch.Add(entry.at)
		if at.Before(since) {
			continue
		}
		payload := at.Format(time.RFC3339Nano) + " " + entry.text + "\n"
		if entry.bare {
			payload = entry.text + "\n"
		}
		if entry.partial {
			payload = strings.TrimSuffix(payload, "\n")
		}
		header := make([]byte, 8)
		header[0] = entry.stream
		binary.BigEndian.PutUint32(header[4:], uint32(len(payload)))
		out.Write(header)
		out.WriteString(payload)
	}
	return io.NopCloser(&out), nil
}

func logMember(role, name, id string) evidence.Member {
	return evidence.Member{Role: role, Name: name, Container: &diagnosticsTestContainer{id: id}}
}

func noRedaction(text string, _ bool) string { return text }

func lineTexts(lines []LogLine) []string {
	var texts []string
	for _, line := range lines {
		texts = append(texts, line.Text)
	}
	return texts
}

func TestProcessLogKeepsTheLatestProcess(t *testing.T) {
	ctr := &fakeLogContainer{started: []time.Duration{10 * time.Second}, running: true, entries: []fakeLogEntry{
		// The container's earlier run, before its current start time.
		startEntry(time.Second, "ceph-mgr", 7),
		cephEntry(2*time.Second, "-1", "old run"),
		// The current run: a process that respawned.
		startEntry(11*time.Second, "ceph-mgr", 7),
		cephEntry(12*time.Second, " 1", "before the respawn"),
		cephEntry(13*time.Second, " 0", "mgr respawn  e: '/usr/bin/ceph-mgr'"),
		startEntry(14*time.Second, "ceph-mgr", 7),
		{at: 15 * time.Second, stream: 1, text: "plain stdout"},
		cephEntry(16*time.Second, "-1", "after the respawn"),
	}}
	docker := newProcessLogDocker(map[string]*fakeLogContainer{"m": ctr})
	log, err := readProcessLog(t.Context(), docker, logMember("mgr", "a", "m"), LogConfig{MaxBytes: 1 << 20}, noRedaction, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := processLogEpoch.Add(10 * time.Second).Format(time.RFC3339Nano); docker.since["m"] != want {
		t.Fatalf("since = %q, want the container start %q", docker.since["m"], want)
	}
	texts := lineTexts(log.Lines)
	if log.Status != LogComplete || len(texts) != 3 || texts[0] != log.Start.Text || !strings.Contains(texts[0], ", process ceph-mgr, pid 7") ||
		texts[1] != "plain stdout" || !strings.HasSuffix(texts[2], "after the respawn") {
		t.Fatalf("process log = %+v", log)
	}
	if !log.Start.Time.Equal(processLogEpoch.Add(14*time.Second)) || !log.Since.Equal(log.Start.Time) ||
		!log.ContainerStartedAt.Equal(processLogEpoch.Add(10*time.Second)) || !log.Running || log.Role != "mgr" || log.Name != "a" || log.ContainerID != "m" {
		t.Fatalf("process log source = %+v, start %+v", log.LogSource, log.Start)
	}
}

func TestProcessLogStatus(t *testing.T) {
	start := startEntry(11*time.Second, "ceph-mon", 1)
	lines := []fakeLogEntry{start, cephEntry(12*time.Second, " 0", "one"), cephEntry(13*time.Second, " 0", "two")}
	for name, test := range map[string]struct {
		ctr     *fakeLogContainer
		max     int
		status  LogStatus
		lines   int
		started bool
	}{
		"complete":                {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines}, 1 << 20, LogComplete, 3, true},
		"truncated":               {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines}, len(start.text) + 50, LogTruncated, 2, true},
		"bound at the start":      {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines}, 1, LogTruncated, 1, true},
		"only an oversized start": {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines[:1]}, 1, LogTruncated, 1, true},
		"only a start that fits":  {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines[:1]}, len(start.text), LogComplete, 1, true},
		"no start line":           {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines[1:]}, 1 << 20, LogStartNotFound, 2, false},
		"start before the run": {&fakeLogContainer{started: []time.Duration{20 * time.Second},
			entries: append(lines, cephEntry(21*time.Second, " 0", "after"))}, 1 << 20, LogStartNotFound, 1, false},
		"restarted":     {&fakeLogContainer{started: []time.Duration{10 * time.Second, 30 * time.Second}, entries: lines}, 1 << 20, LogChanged, 3, true},
		"stopped":       {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: lines}, 1 << 20, LogComplete, 3, true},
		"cut long line": {&fakeLogContainer{started: []time.Duration{10 * time.Second}, entries: append(lines, cephEntry(14*time.Second, " 0", strings.Repeat("x", evidence.MaxLogLine)))}, 1 << 20, LogTruncated, 4, true},
	} {
		t.Run(name, func(t *testing.T) {
			log, err := readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"x": test.ctr}), logMember("mon", "a", "x"), LogConfig{MaxBytes: test.max}, noRedaction, nil)
			if err != nil {
				t.Fatal(err)
			}
			if log.Status != test.status || len(log.Lines) != test.lines || (log.Start.Text != "") != test.started {
				t.Fatalf("log = %+v", log)
			}
			if !test.started && (!log.Since.Equal(log.ContainerStartedAt) || !log.Start.Time.IsZero()) {
				t.Fatalf("a log without its start line starts at the container start: %+v", log)
			}
		})
	}
}

func TestProcessLogStartsAgainAfterATruncatedProcess(t *testing.T) {
	ctr := &fakeLogContainer{started: []time.Duration{0}, entries: []fakeLogEntry{
		startEntry(time.Second, "ceph-mgr", 7),
		cephEntry(2*time.Second, " 0", strings.Repeat("a", 400)),
		cephEntry(3*time.Second, " 0", strings.Repeat("b", 400)),
		startEntry(4*time.Second, "ceph-mgr", 7),
		cephEntry(5*time.Second, " 0", "small"),
	}}
	log, err := readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"m": ctr}), logMember("mgr", "a", "m"), LogConfig{MaxBytes: 600}, noRedaction, nil)
	if err != nil || log.Status != LogComplete || len(log.Lines) != 2 || !log.Start.Time.Equal(processLogEpoch.Add(4*time.Second)) {
		t.Fatalf("log = %+v, %v", log, err)
	}
	// A later start that alone exceeds the bound truncates the new window.
	ctr.entries = append(ctr.entries[:3:3], startEntry(4*time.Second, "ceph-mgr", 7))
	log, err = readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"m": ctr}), logMember("mgr", "a", "m"), LogConfig{MaxBytes: 100}, noRedaction, nil)
	if err != nil || log.Status != LogTruncated || len(log.Lines) != 1 || !log.Start.Time.Equal(processLogEpoch.Add(4*time.Second)) {
		t.Fatalf("oversized later start = %+v, %v", log, err)
	}
}

// A respawned MGR whose MONs refuse its key exits before it logs its start
// line, as Tentacle 20.2.4 does after "mgr fail" with a replaced key: its
// lines start after the replaced process's last line, never in that process.
func TestProcessLogStartsAtARespawn(t *testing.T) {
	respawn := cephEntry(5*time.Second, " 1", "mgr respawn  exe_path /proc/self/exe")
	entries := []fakeLogEntry{
		startEntry(time.Second, "ceph-mgr", 1),
		cephEntry(2*time.Second, " 1", "mgr handle_mgr_map I was active but no longer am"),
		cephEntry(3*time.Second, " 1", "mgr respawn respawning with exe /usr/bin/ceph-mgr"),
		respawn,
		cephEntry(6*time.Second, "-1", "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied"),
		{at: 6 * time.Second, stream: 2, text: "failed to fetch mon config (--no-mon-config to skip)"},
	}
	log, err := readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"m": {started: []time.Duration{0}, entries: entries}}),
		logMember("mgr", "a", "m"), LogConfig{MaxBytes: 1 << 20}, noRedaction, nil)
	if err != nil {
		t.Fatal(err)
	}
	if log.Status != LogComplete || log.Start.Text != respawn.text || len(log.Lines) != 3 || !strings.HasSuffix(log.Lines[1].Text, "(13) Permission denied") {
		t.Fatalf("respawned process log = %+v", log)
	}
	// The respawned process that logs its start line starts there.
	started := append(entries[:4:4], startEntry(6*time.Second, "ceph-mgr", 1), cephEntry(7*time.Second, " 0", "after"))
	log, err = readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"m": {started: []time.Duration{0}, entries: started}}),
		logMember("mgr", "a", "m"), LogConfig{MaxBytes: 1 << 20}, noRedaction, nil)
	if err != nil || log.Status != LogComplete || !log.Start.Time.Equal(processLogEpoch.Add(6*time.Second)) || len(log.Lines) != 2 {
		t.Fatalf("respawned process with its start line = %+v, %v", log, err)
	}
}

func TestProcessStartLine(t *testing.T) {
	for _, text := range []string{
		"2026-11-27T04:00:00.000+0000 7f1b8c0a1640  0 ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo), process ceph-mgr, pid 7",
		"2026-11-27T04:00:00.000000+0000 ffff9a3e4020  0 ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable), process ceph-osd, pid 1",
	} {
		if !processStartLine.MatchString(text) {
			t.Errorf("not a start line: %s", text)
		}
	}
	for _, text := range []string{
		"2026-11-27T04:27:08.916+0000 786e52f7c640  1 mgr respawn  exe_path /proc/self/exe",
		"2026-11-27T04:27:08.916+0000 786e52f7c640  1 mds.a respawn  exe_path /proc/self/exe",
	} {
		if !processBoundary(text) {
			t.Errorf("not a process boundary: %s", text)
		}
	}
	for _, text := range []string{
		"2026-11-27T04:27:08.916+0000 786e52f7c640  1 mgr respawn respawning with exe /usr/bin/ceph-mgr",
		"2026-11-27T04:27:08.916+0000 786e52f7c640  1 mgr respawn  e: 'ceph-mgr'",
		"mgr respawn  exe_path /proc/self/exe",
		"ceph version 20.2.4 (7f793731) tentacle (stable), process ceph-mgr, pid 7",
		"2026-11-27T04:00:00.000+0000 7f1b8c0a1640  0 log_channel(audit) log [DBG] : ceph version 20.2.4 (7f793731) tentacle (stable), process ceph-mgr, pid 7",
		"2026-11-27T04:00:00.000+0000 7f1b8c0a1640  0 ceph version 20.2.4 (7f793731) tentacle (stable)",
	} {
		if processBoundary(text) {
			t.Errorf("a process boundary: %s", text)
		}
	}
}

func TestProcessLogRedacts(t *testing.T) {
	ctr := &fakeLogContainer{started: []time.Duration{0}, entries: []fakeLogEntry{
		startEntry(time.Second, "ceph-rgw", 3),
		cephEntry(2*time.Second, " 0", "loaded AQBsecretsecretsecretsecretsecret=="),
		{at: 3 * time.Second, stream: 2, text: "before -----BEGIN PRIVATE KEY-----"},
		{at: 4 * time.Second, stream: 2, text: "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQ"},
		{at: 5 * time.Second, stream: 2, text: "-----END PRIVATE KEY----- after"},
	}}
	redact := logRedactor([]string{"AQBsecretsecretsecretsecretsecret=="})
	log, err := readProcessLog(t.Context(), newProcessLogDocker(map[string]*fakeLogContainer{"r": ctr}), logMember("rgw", "rgw", "r"), LogConfig{MaxBytes: 1 << 20}, redact, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(lineTexts(log.Lines), "\n")
	if strings.Contains(joined, "AQBsecret") || strings.Contains(joined, "MIIEvQ") || !strings.Contains(joined, "[REDACTED]") ||
		!strings.Contains(joined, "before "+evidence.PrivateKeyMarker) || !strings.Contains(joined, evidence.PrivateKeyMarker+" after") || len(log.Lines) != 5 {
		t.Fatalf("redacted lines:\n%s", joined)
	}
}

func TestProcessLogReadErrors(t *testing.T) {
	member := logMember("osd", "0", "o")
	if _, err := readProcessLog(t.Context(), newProcessLogDocker(nil), member, LogConfig{MaxBytes: 1}, noRedaction, nil); err == nil {
		t.Fatal("a removed container was read")
	}
	boom := errors.New("log driver none")
	docker := newProcessLogDocker(map[string]*fakeLogContainer{"o": {started: []time.Duration{0}, logErr: boom}})
	if _, err := readProcessLog(t.Context(), docker, member, LogConfig{MaxBytes: 1}, noRedaction, nil); !errors.Is(err, boom) {
		t.Fatalf("log error = %v", err)
	}
	never := newProcessLogDocker(map[string]*fakeLogContainer{"o": {started: []time.Duration{0}, startedAt: "0001-01-01T00:00:00Z"}})
	if _, err := readProcessLog(t.Context(), never, member, LogConfig{MaxBytes: 1}, noRedaction, nil); err == nil || !strings.Contains(err.Error(), "has not started") {
		t.Fatalf("a container that never started = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	docker = newProcessLogDocker(map[string]*fakeLogContainer{"o": {started: []time.Duration{0}, entries: []fakeLogEntry{startEntry(time.Second, "ceph-osd", 1)}}})
	if _, err := readProcessLog(ctx, docker, member, LogConfig{MaxBytes: 1 << 10}, noRedaction, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
}

func TestLogConfigLimits(t *testing.T) {
	if config, err := normalizeLogConfig(LogConfig{}); err != nil || config.MaxBytes != defaultLogBytes {
		t.Fatalf("defaults = %+v, %v", config, err)
	}
	for name, config := range map[string]LogConfig{
		"negative":       {MaxBytes: -1},
		"over the limit": {MaxBytes: maxLogBytes + 1},
		"long value":     {RedactValues: []string{strings.Repeat("x", 4097)}},
		"many values":    {RedactValues: make([]string, 129)},
	} {
		if _, err := normalizeLogConfig(config); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := CollectProcessLog(t.Context(), nil, nil, LogConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil daemon = %v", err)
	}
}

func TestFindLogMember(t *testing.T) {
	containers := []Target{
		{Role: "control", Name: "cli", Container: &diagnosticsTestContainer{id: "c"}},
		{Role: "mgr", Name: "a", Container: &diagnosticsTestContainer{id: "m"}},
		{Role: "mon", Name: "a", Container: &diagnosticsTestContainer{id: "n"}},
		{Role: "extra", Name: "mirror", Container: &diagnosticsTestContainer{id: "e"}},
	}
	if member, err := findLogMember(containers, "m", ""); err != nil || member.Role != "mgr" || member.Name != "a" {
		t.Fatalf("mgr = %+v, %v", member, err)
	}
	if member, err := findLogMember(containers, "m", "mgr"); err != nil || member.Name != "a" {
		t.Fatalf("mgr by role = %+v, %v", member, err)
	}
	for name, test := range map[string]struct{ id, role string }{
		"not tracked":    {"z", ""},
		"no daemon":      {"c", ""},
		"additional":     {"e", ""},
		"another role":   {"n", "mgr"},
		"no container":   {"", ""},
		"control as mgr": {"c", "mgr"},
	} {
		if _, err := findLogMember(containers, test.id, test.role); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
