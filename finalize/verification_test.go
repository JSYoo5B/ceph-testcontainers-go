package finalize

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
	"github.com/containerd/errdefs"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/system"
	mobycl "github.com/moby/moby/client"
)

var verifyStart = time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)

// fakeVerifyContainer is one container as the fake engine sees it.
type fakeVerifyContainer struct {
	startedAt  string
	restarts   int
	removed    bool
	inspectErr error
	driver     string
	maxSize    string
	tty        bool
	// lines are the log lines, each with its engine timestamp.
	lines []fakeLogLine
	// rawLogs replaces the generated log stream.
	rawLogs []byte
	logsErr error
	// crashes are the crash IDs in its crash directory; notDone lists
	// those without a done marker.
	crashes []string
	notDone map[string]bool
	// posted lists crash IDs also present, done, as posted metadata;
	// messages replaces a crash's assert_msg.
	posted   []string
	messages map[string]string
	process  string
	copyErr  error
	noCrash  bool
}

type fakeLogLine struct {
	at   time.Time
	text string
}

type fakeVerifyDocker struct {
	mu         sync.Mutex
	clock      time.Time
	infoErr    error
	containers map[string]*fakeVerifyContainer
	logOptions map[string]mobycl.ContainerLogsOptions
}

func (d *fakeVerifyDocker) Info(ctx context.Context, _ mobycl.InfoOptions) (mobycl.SystemInfoResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.SystemInfoResult{}, err
	}
	if d.infoErr != nil {
		return mobycl.SystemInfoResult{}, d.infoErr
	}
	return mobycl.SystemInfoResult{Info: system.Info{SystemTime: d.clock.Format(time.RFC3339Nano)}}, nil
}

func (d *fakeVerifyDocker) ContainerInspect(ctx context.Context, id string, _ mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.ContainerInspectResult{}, err
	}
	ctr := d.containers[id]
	switch {
	case ctr == nil || ctr.removed:
		return mobycl.ContainerInspectResult{}, fmt.Errorf("no such container %s: %w", id, errdefs.ErrNotFound)
	case ctr.inspectErr != nil:
		return mobycl.ContainerInspectResult{}, ctr.inspectErr
	}
	driver := ctr.driver
	if driver == "" {
		driver = "json-file"
	}
	options := map[string]string{}
	if ctr.maxSize != "" {
		options["max-size"] = ctr.maxSize
	}
	return mobycl.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id, Image: "sha256:image-" + id, RestartCount: ctr.restarts,
		State:      &dockercontainer.State{Status: "running", StartedAt: ctr.startedAt},
		Config:     &dockercontainer.Config{Tty: ctr.tty},
		HostConfig: &dockercontainer.HostConfig{LogConfig: dockercontainer.LogConfig{Type: driver, Config: options}}}}, nil
}

func parseDockerTimestamp(t string) time.Time {
	seconds, nanos, _ := strings.Cut(t, ".")
	s, _ := strconv.ParseInt(seconds, 10, 64)
	n, _ := strconv.ParseInt(nanos, 10, 64)
	return time.Unix(s, n).UTC()
}

func (d *fakeVerifyDocker) ContainerLogs(ctx context.Context, id string, options mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.logOptions[id] = options
	d.mu.Unlock()
	ctr := d.containers[id]
	switch {
	case ctr == nil || ctr.removed:
		return nil, fmt.Errorf("no such container %s: %w", id, errdefs.ErrNotFound)
	case ctr.logsErr != nil:
		return nil, ctr.logsErr
	case !options.Timestamps || !options.ShowStderr || !options.ShowStdout:
		return nil, errors.New("unexpected log options")
	}
	if ctr.rawLogs != nil {
		return io.NopCloser(bytes.NewReader(ctr.rawLogs)), nil
	}
	since, until := parseDockerTimestamp(options.Since), time.Time{}
	if options.Until != "" {
		until = parseDockerTimestamp(options.Until)
	}
	var stream []byte
	for _, line := range ctr.lines {
		if line.at.Before(since) || (!until.IsZero() && line.at.After(until)) {
			continue
		}
		data := line.at.Format(time.RFC3339Nano) + " " + line.text + "\n"
		if ctr.tty {
			stream = append(stream, data...)
		} else {
			stream = append(stream, diagnosticsFrame(2, data)...)
		}
	}
	return io.NopCloser(bytes.NewReader(stream)), nil
}

func (d *fakeVerifyDocker) CopyFromContainer(ctx context.Context, id string, options mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.CopyFromContainerResult{}, err
	}
	ctr := d.containers[id]
	switch {
	case options.SourcePath != ceph.CrashDir:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("unexpected path %s", options.SourcePath)
	case ctr == nil || ctr.removed || ctr.noCrash:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("could not find %s: %w", options.SourcePath, errdefs.ErrNotFound)
	case ctr.copyErr != nil:
		return mobycl.CopyFromContainerResult{}, ctr.copyErr
	}
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	write := func(name, body string, kind byte) {
		header := &tar.Header{Name: name, Mode: 0o644, Typeflag: kind, Size: int64(len(body)), ModTime: time.Unix(1, 0)}
		if kind == tar.TypeDir {
			header.Mode = 0o755
		}
		_ = writer.WriteHeader(header)
		_, _ = writer.Write([]byte(body))
	}
	write("crash/", "", tar.TypeDir)
	meta := func(crash string) string {
		message := "fixture assertion " + crash
		if ctr.messages[crash] != "" {
			message = ctr.messages[crash]
		}
		data, _ := json.Marshal(map[string]any{"crash_id": crash, "timestamp": verifyStart.Add(time.Minute).Format(time.RFC3339Nano),
			"process_name": ctr.process, "entity_name": "entity-of-" + id,
			"assert_condition": "r == 0", "assert_msg": message, "backtrace": []string{"frame-a", "frame-b"}})
		return string(data)
	}
	for _, crash := range ctr.crashes {
		write("crash/"+crash+"/", "", tar.TypeDir)
		write("crash/"+crash+"/meta", meta(crash), tar.TypeReg)
		if !ctr.notDone[crash] {
			write("crash/"+crash+"/done", "", tar.TypeReg)
		}
	}
	if len(ctr.posted) > 0 {
		write("crash/posted/", "", tar.TypeDir)
	}
	for _, crash := range ctr.posted {
		write("crash/posted/"+crash+"/", "", tar.TypeDir)
		write("crash/posted/"+crash+"/meta", meta(crash), tar.TypeReg)
		write("crash/posted/"+crash+"/done", "", tar.TypeReg)
	}
	_ = writer.Close()
	return mobycl.CopyFromContainerResult{Content: io.NopCloser(&buffer)}, nil
}

// verifyFixture is a cluster of a MON, two OSDs, the separate control
// container and the toolkit client, where osd.0 already holds one crash.
type verifyFixture struct {
	docker   *fakeVerifyDocker
	snapshot clusterSnapshot
	config   VerificationConfig
}

func newVerifyFixture() *verifyFixture {
	started := verifyStart.Add(-10 * time.Minute).Format(time.RFC3339Nano)
	docker := &fakeVerifyDocker{clock: verifyStart, logOptions: map[string]mobycl.ContainerLogsOptions{}, containers: map[string]*fakeVerifyContainer{
		"mon":     {startedAt: started, process: "ceph-mon"},
		"osd0":    {startedAt: started, process: "ceph-osd", crashes: []string{"2026-10-23T00:55:00Z_pre"}},
		"osd1":    {startedAt: started, process: "ceph-osd"},
		"cli":     {startedAt: started, noCrash: true, driver: "local"},
		"toolkit": {startedAt: started, noCrash: true, driver: "local"},
	}}
	for _, ctr := range docker.containers {
		ctr.lines = []fakeLogLine{
			{verifyStart.Add(-5 * time.Minute), daemonLogLine("cluster", "WRN", "Health check failed: OSD count 0 < osd_pool_default_size 1 (TOO_FEW_OSDS)")},
			{verifyStart.Add(time.Minute), daemonLogLine("cluster", "INF", "osd.0 boot")},
		}
	}
	snapshot := clusterSnapshot{
		containers: []diagnostics.Target{
			crashContainer("client", "toolkit", "toolkit"), crashContainer("control", "cli", "cli"),
			crashContainer("mon", "a", "mon"), crashContainer("osd", "0", "osd0"), crashContainer("osd", "1", "osd1"),
		},
		controlPlane: crashTarget{id: "cli"},
		secrets:      []string{"AQBverifysecret=="},
	}
	return &verifyFixture{docker: docker, snapshot: snapshot}
}

// daemonLogLine is a cluster log entry as a daemon logs it (LogChannel::do_log
// through the Log of the pinned Ceph source).
func daemonLogLine(channel, priority, message string) string {
	return "2026-10-24T01:02:03.123+0000 7f3a1c0e5640  0 log_channel(" + channel + ") log [" + priority + "] : " + message
}

func (fx *verifyFixture) line(id string, after time.Duration, text string) {
	ctr := fx.docker.containers[id]
	ctr.lines = append(ctr.lines, fakeLogLine{verifyStart.Add(after), text})
}

// activate activates a verification over the fixture and moves the
// engine's clock ten minutes on, for the evaluation.
func (fx *verifyFixture) activate(t *testing.T) *Verification {
	t.Helper()
	v, err := newVerification(fx.config)
	if err != nil {
		t.Fatal(err)
	}
	fx.wire(v)
	if err := v.activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	fx.docker.clock = verifyStart.Add(10 * time.Minute)
	return v
}

func (fx *verifyFixture) wire(v *Verification) {
	v.cluster, v.images = "cluster-id", ceph.ImageSet{Essential: "example.invalid/ceph:test"}
	v.snapshot = func(context.Context) (clusterSnapshot, error) { return fx.snapshot, nil }
	v.docker = func(context.Context) (verificationDocker, func(), error) { return fx.docker, func() {}, nil }
}

func (fx *verifyFixture) evaluate(t *testing.T, v *Verification) *VerificationReport {
	t.Helper()
	report, err := v.Evaluate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func gapReasons(check VerificationCheck) string {
	var reasons []string
	for _, gap := range check.Gaps {
		reasons = append(reasons, gap.Source+": "+gap.Reason)
	}
	return strings.Join(reasons, "\n")
}

// An ordinary run is clean: the bootstrap's warning before the activation
// and the pre-existing crash are outside the window, the control container
// and the toolkit are not sources, and the window is the engine's.
func TestVerificationOrdinaryRunIsClean(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationClean || report.Logs.Outcome != VerificationClean || report.Crashes.Outcome != VerificationClean {
		t.Fatalf("outcome %s: logs %+v crashes %+v", report.Outcome, report.Logs, report.Crashes)
	}
	if !report.Window.StartedAt.Equal(verifyStart) || !report.Window.EndedAt.Equal(verifyStart.Add(10*time.Minute)) || report.Window.Phase != "test" {
		t.Fatalf("window %+v", report.Window)
	}
	var sources []string
	for _, source := range report.Sources {
		sources = append(sources, source.Source)
	}
	if !slices.Equal(sources, []string{"mon.a", "osd.0", "osd.1"}) {
		t.Fatalf("sources %v", sources)
	}
	if got := report.Sources[1].BaselineCrashes; !slices.Equal(got, []string{"2026-10-23T00:55:00Z_pre"}) || report.Sources[1].ImageID != "sha256:image-osd0" || report.Sources[1].LogBytes == 0 {
		t.Fatalf("osd.0 source %+v", report.Sources[1])
	}
	options := fx.docker.logOptions["osd0"]
	if options.Since != dockerTimestamp(verifyStart) || options.Until != dockerTimestamp(verifyStart.Add(10*time.Minute)) {
		t.Fatalf("log window %+v", options)
	}
	if _, ok := fx.docker.logOptions["cli"]; ok {
		t.Fatal("the control container was read as a source")
	}
	if len(report.Notes) == 0 || report.Images.Essential == "" || report.Cluster != "cluster-id" {
		t.Fatalf("report %+v", report)
	}
}

// A crash is new by its identity: the crash that osd.0 held at activation
// stays out, a new one in the same directory is unexpected, and only an
// expectation that matches it permits it.
func TestVerificationNewCrashesByIdentity(t *testing.T) {
	newCrash := "2026-10-23T01:01:00Z_new"
	t.Run("unexpected", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, newCrash)
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationUnexpected || report.Crashes.Unexpected != 1 || report.Crashes.Findings[0].CrashID != newCrash ||
			report.Crashes.Findings[0].Source != "osd.0" || !strings.Contains(report.Crashes.Findings[0].Text, "fixture assertion") {
			t.Fatalf("crashes %+v", report.Crashes)
		}
		if report.Logs.Outcome != VerificationClean {
			t.Fatalf("a crash changed the log check: %+v", report.Logs)
		}
	})
	t.Run("pre-existing crash in a new directory listing", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		// The same crash ID is still there, now listed after another file.
		fx.docker.containers["osd0"].crashes = []string{"2026-10-23T00:55:00Z_pre"}
		if report := fx.evaluate(t, v); report.Outcome != VerificationClean {
			t.Fatalf("pre-existing crash counted: %+v", report.Crashes)
		}
	})
	t.Run("permitted narrowly", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Crashes = []CrashExpectation{{Name: "osd0-assert", Daemon: "osd.0", Process: "ceph-osd", Signature: `^r == 0$`}}
		v := fx.activate(t)
		fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, newCrash)
		fx.docker.containers["osd1"].crashes = []string{"2026-10-23T01:02:00Z_other"}
		report := fx.evaluate(t, v)
		if report.Crashes.Permitted != 1 || report.Crashes.Unexpected != 1 || report.Outcome != VerificationUnexpected {
			t.Fatalf("crashes %+v", report.Crashes)
		}
		// Unexpected findings come first; the permitted one names its rule.
		if report.Crashes.Findings[0].Source != "osd.1" || report.Crashes.Findings[0].Expectation != "" ||
			report.Crashes.Findings[1].Expectation != "osd0-assert" {
			t.Fatalf("findings %+v", report.Crashes.Findings)
		}
	})
	t.Run("log expectation does not waive a crash", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Logs = []LogExpectation{{Name: "anything", Pattern: `.*`}}
		v := fx.activate(t)
		fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, newCrash)
		if report := fx.evaluate(t, v); report.Crashes.Unexpected != 1 || report.Outcome != VerificationUnexpected {
			t.Fatalf("a log expectation permitted a crash: %+v", report.Crashes)
		}
	})
}

// The collector of a crash's metadata redacts the configured values and the
// cluster's own secrets before a finding keeps the text.
func TestVerificationRedactsCrashMetadata(t *testing.T) {
	newCrash := "2026-10-23T01:01:00Z_secret"
	fx := newVerifyFixture()
	fx.config.RedactValues = []string{"opaque-caller-value-41"}
	v := fx.activate(t)
	osd := fx.docker.containers["osd0"]
	osd.crashes = append(osd.crashes, newCrash)
	osd.messages = map[string]string{newCrash: "failed with opaque-caller-value-41 and key AQBverifysecret=="}
	report := fx.evaluate(t, v)
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if report.Crashes.Unexpected != 1 || !strings.Contains(report.Crashes.Findings[0].Text, "failed with") {
		t.Fatalf("crashes %+v", report.Crashes)
	}
	for _, secret := range []string{"opaque-caller-value-41", "AQBverifysecret=="} {
		if strings.Contains(string(data), secret) || strings.Contains(report.summary(), secret) {
			t.Errorf("the report keeps %q", secret)
		}
	}
}

// Report redaction masks crash IDs but does not merge them: a new crash
// whose masked ID equals a baseline crash's is still new.
func TestVerificationCrashIdentityIgnoresRedaction(t *testing.T) {
	old := "2026-10-23T00:55:00.000000Z_11111111-1111-4111-8111-111111111111"
	added := "2026-10-23T01:01:00.000000Z_22222222-2222-4222-8222-222222222222"
	for _, masked := range []bool{false, true} {
		fx := newVerifyFixture()
		fx.docker.containers["osd1"].crashes = []string{old}
		if masked {
			fx.config.RedactValues = []string{old, added}
		}
		v := fx.activate(t)
		fx.docker.containers["osd1"].crashes = []string{old, added}
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationUnexpected || report.Crashes.Unexpected != 1 {
			t.Fatalf("masked %t: crashes %+v", masked, report.Crashes)
		}
		data, _ := json.Marshal(report)
		if masked && (strings.Contains(string(data), old) || strings.Contains(string(data), added)) {
			t.Fatalf("a masked crash ID is in the report")
		}
	}
}

// A crash seen as unposted and as posted metadata is one crash.
func TestVerificationCountsDistinctCrashes(t *testing.T) {
	newCrash := "2026-10-23T01:01:00Z_twice"
	seenTwice := func(fx *verifyFixture) {
		osd := fx.docker.containers["osd0"]
		osd.crashes = append(osd.crashes, newCrash)
		osd.posted = []string{newCrash}
		// The unposted copy has no done marker; the posted one does.
		osd.notDone = map[string]bool{newCrash: true}
	}
	t.Run("two required", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Crashes = []CrashExpectation{{Name: "two", Daemon: "osd.0", Required: true, MinCount: 2}}
		v := fx.activate(t)
		seenTwice(fx)
		report := fx.evaluate(t, v)
		if report.Outcome == VerificationClean || report.Crashes.Permitted != 1 || report.Crashes.Expectations[0].Satisfied {
			t.Fatalf("one crash satisfied two: %+v", report.Crashes)
		}
	})
	t.Run("one permitted", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Crashes = []CrashExpectation{{Name: "one", Daemon: "osd.0", MaxCount: 1}}
		v := fx.activate(t)
		seenTwice(fx)
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationClean || report.Crashes.Permitted != 1 || report.Crashes.Unexpected != 0 {
			t.Fatalf("crashes %+v\n%s", report.Crashes, gapReasons(report.Crashes))
		}
	})
}

// Log events are classified lines inside the window. A narrow expectation
// permits its line only, MaxCount limits it, and an ordinary INFO line is no
// event.
func TestVerificationLogExpectations(t *testing.T) {
	signal := "*** Caught signal (Segmentation fault) **"
	warning := daemonLogLine("cluster", "WRN", "Health check failed: 1 osds down (OSD_DOWN)")
	t.Run("unexpected warning", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, warning)
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationUnexpected || report.Logs.Unexpected != 1 || report.Logs.Findings[0].Event != string(LogClusterWarning) ||
			report.Logs.Findings[0].Time != verifyStart.Add(2*time.Minute).Format(time.RFC3339Nano) {
			t.Fatalf("logs %+v", report.Logs)
		}
		if report.Crashes.Outcome != VerificationClean {
			t.Fatalf("a log line changed the crash check: %+v", report.Crashes)
		}
	})
	t.Run("narrow permission keeps another event", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Logs = []LogExpectation{{Name: "osd0-signal", Daemon: "osd.0", Event: LogFatalSignal, Pattern: `Caught signal \(Segmentation fault\)`, MaxCount: 1}}
		v := fx.activate(t)
		fx.line("osd0", 2*time.Minute, signal)
		fx.line("osd1", 2*time.Minute, signal)
		fx.line("osd0", 3*time.Minute, signal)
		fx.line("osd0", 4*time.Minute, "FAILED ceph_assert(r == 0)")
		report := fx.evaluate(t, v)
		if report.Logs.Permitted != 1 || report.Logs.Unexpected != 3 || report.Outcome != VerificationUnexpected {
			t.Fatalf("logs %+v", report.Logs)
		}
		events := map[string]int{}
		for _, finding := range report.Logs.Findings {
			if finding.Expectation == "" {
				events[finding.Source+" "+finding.Event]++
			}
		}
		if events["osd.1 fatal-signal"] != 1 || events["osd.0 fatal-signal"] != 1 || events["osd.0 assertion"] != 1 {
			t.Fatalf("unexpected findings %v", events)
		}
	})
	t.Run("permitted and required are satisfied", func(t *testing.T) {
		fx := newVerifyFixture()
		fx.config.Logs = []LogExpectation{
			{Name: "osd-down", Daemon: "mon.a", Event: LogClusterWarning, Pattern: `\(OSD_DOWN\)`, Required: true},
			{Name: "osd0-signal", Daemon: "osd.0", Pattern: `Caught signal`},
		}
		v := fx.activate(t)
		fx.line("mon", 2*time.Minute, warning)
		report := fx.evaluate(t, v)
		if report.Outcome != VerificationClean || report.Logs.Permitted != 1 || !report.Logs.Expectations[0].Satisfied || report.Logs.Expectations[0].Matched != 1 ||
			!report.Logs.Expectations[1].Satisfied || report.Logs.Findings[0].Expectation != "osd-down" {
			t.Fatalf("logs %+v", report.Logs)
		}
	})
	t.Run("lines are redacted and bounded", func(t *testing.T) {
		fx := newVerifyFixture()
		v := fx.activate(t)
		fx.line("osd1", 2*time.Minute, daemonLogLine("cluster", "ERR", "key AQBverifysecret== "+strings.Repeat("x", 4000)))
		report := fx.evaluate(t, v)
		finding := report.Logs.Findings[0]
		if strings.Contains(finding.Text, "AQBverifysecret") || len(finding.Text) > maxFindingText || !finding.Truncated || finding.Event != string(LogClusterError) {
			t.Fatalf("finding %+v", finding)
		}
	})
}

// A required expectation that nothing matched fails, as does one matched
// fewer than MinCount times; an unmatched permission does not.
// Expectations match the whole redacted line, not the bounded text a
// finding keeps.
func TestVerificationMatchesBeyondTheFindingText(t *testing.T) {
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "marker", Daemon: "mon.a", Required: true, Pattern: `late-marker-7f3a`}}
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, daemonLogLine("cluster", "WRN", strings.Repeat("x", 1100)+" late-marker-7f3a"))
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationClean || report.Logs.Permitted != 1 || !report.Logs.Expectations[0].Satisfied ||
		!report.Logs.Findings[0].Truncated || len(report.Logs.Findings[0].Text) > maxFindingText {
		t.Fatalf("logs %+v", report.Logs)
	}
}

// A line longer than the line bound leaves its rest unread: a gap, also when
// its kept prefix is no event.
func TestVerificationCutLineIsAGap(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	fx.line("mon", 2*time.Minute, strings.Repeat("x", maxVerificationLine+16)+" log_channel(cluster) log [ERR] : lost")
	report := fx.evaluate(t, v)
	if report.Logs.Outcome != VerificationIncomplete || !strings.Contains(gapReasons(report.Logs), "longer than") {
		t.Fatalf("logs %s: %s", report.Logs.Outcome, gapReasons(report.Logs))
	}
}

// Waiting for another evaluation is bounded by the evaluation's context.
func TestVerificationEvaluationWaitIsBounded(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	v.evaluating <- struct{}{} // another evaluation holds the verification
	defer func() { <-v.evaluating }()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := v.Evaluate(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled evaluation waited for another one")
	}
}

func TestVerificationRequiredExpectations(t *testing.T) {
	fx := newVerifyFixture()
	fx.config.Logs = []LogExpectation{{Name: "may-warn", Pattern: `OSD_DOWN`}}
	fx.config.Crashes = []CrashExpectation{{Name: "osd0-crash", Daemon: "osd.0", Required: true, MinCount: 2}}
	v := fx.activate(t)
	fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, "2026-10-23T01:01:00Z_new")
	report := fx.evaluate(t, v)
	if report.Logs.Outcome != VerificationClean || !report.Logs.Expectations[0].Satisfied {
		t.Fatalf("an unmatched permission failed: %+v", report.Logs)
	}
	result := report.Crashes.Expectations[0]
	if report.Crashes.Outcome != VerificationUnexpected || report.Outcome != VerificationUnexpected || result.Satisfied || result.Matched != 1 ||
		result.MinCount != 2 || report.Crashes.Unexpected != 0 || report.Crashes.Permitted != 1 {
		t.Fatalf("crashes %+v", report.Crashes)
	}
	if summary := report.summary(); !strings.Contains(summary, "required but missing: osd0-crash matched 1 of 2") {
		t.Fatalf("summary %s", summary)
	}
}

// Every observation that cannot be completed makes the verification
// incomplete, never clean. The control case shows that the fixture is clean
// without the mutation.
func TestVerificationGapsAreIncomplete(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*verifyFixture)
		logs   string
		crash  string
	}{
		{name: "control"},
		{name: "removed container", mutate: func(fx *verifyFixture) { fx.docker.containers["osd1"].removed = true },
			logs: "osd.1: the container was removed", crash: "osd.1: the container was removed"},
		{name: "restarted container", mutate: func(fx *verifyFixture) { fx.docker.containers["osd1"].restarts = 1 },
			logs: "osd.1: restarted since the activation", crash: "osd.1: restarted since the activation"},
		{name: "inaccessible container", mutate: func(fx *verifyFixture) { fx.docker.containers["mon"].inspectErr = errors.New("permission denied") },
			logs: "mon.a: permission denied", crash: "mon.a: permission denied"},
		{name: "rotating log driver", mutate: func(fx *verifyFixture) { fx.docker.containers["mon"].maxSize = "10m" },
			logs: "mon.a: the json-file driver rotates at max-size 10m"},
		{name: "unsupported log driver", mutate: func(fx *verifyFixture) { fx.docker.containers["mon"].driver = "local" },
			logs: `mon.a: log driver "local" is not covered`},
		{name: "log over the limit", mutate: func(fx *verifyFixture) {
			fx.line("osd0", 2*time.Minute, strings.Repeat("y", 2048))
		}, logs: "osd.0: the log stream of the window exceeds 1024 bytes"},
		{name: "malformed log frame", mutate: func(fx *verifyFixture) { fx.docker.containers["osd0"].rawLogs = []byte{9, 0, 0, 0, 0, 0, 0, 1, 'x'} },
			logs: "osd.0: invalid Docker log frame"},
		{name: "cut log frame", mutate: func(fx *verifyFixture) { fx.docker.containers["osd0"].rawLogs = diagnosticsFrame(2, "x")[:6] },
			logs: "osd.0: the log stream ends inside a frame"},
		{name: "line without timestamp", mutate: func(fx *verifyFixture) {
			fx.docker.containers["osd0"].rawLogs = diagnosticsFrame(2, "no timestamp here\n")
		}, logs: "osd.0: 1 log lines have no valid timestamp"},
		{name: "log stream error", mutate: func(fx *verifyFixture) { fx.docker.containers["osd0"].logsErr = errors.New("log driver failed") },
			logs: "osd.0: log driver failed"},
		{name: "unknown end of window", mutate: func(fx *verifyFixture) { fx.docker.infoErr = errors.New("engine busy") },
			logs: ": the end of the window is unknown", crash: ": the end of the window is unknown"},
		{name: "clock that did not advance", mutate: func(fx *verifyFixture) { fx.docker.clock = verifyStart },
			logs: "not after the activation's", crash: "not after the activation's"},
		{name: "unreadable crash directory", mutate: func(fx *verifyFixture) { fx.docker.containers["osd1"].copyErr = errors.New("archive refused") },
			crash: "osd.1: crash directory failed: archive refused"},
		{name: "missing crash directory", mutate: func(fx *verifyFixture) { fx.docker.containers["osd1"].noCrash = true },
			crash: "osd.1: the container has no crash directory"},
		{name: "crash without done marker", mutate: func(fx *verifyFixture) {
			ctr := fx.docker.containers["osd1"]
			ctr.crashes, ctr.notDone = []string{"2026-10-23T01:03:00Z_partial"}, map[string]bool{"2026-10-23T01:03:00Z_partial": true}
			fx.config.Crashes = []CrashExpectation{{Name: "osd1", Daemon: "osd.1"}}
		}, crash: "osd.1: crash 2026-10-23T01:03:00Z_partial has no done marker"},
		{name: "container added after the activation", mutate: func(fx *verifyFixture) {
			fx.docker.containers["osd2"] = &fakeVerifyContainer{startedAt: verifyStart.Format(time.RFC3339Nano)}
			fx.snapshot.containers = append(fx.snapshot.containers, crashContainer("osd", "2", "osd2"))
		}, logs: "osd.2: added after the activation", crash: "osd.2: added after the activation"},
		{name: "containers over the limit", mutate: func(fx *verifyFixture) { fx.snapshot.omitted = 3 },
			logs: "3 containers exceed the container limit", crash: "3 containers exceed the container limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.config.MaxLogBytes = 1024
			v := fx.activate(t)
			if tc.mutate != nil {
				tc.mutate(fx)
			}
			if tc.name == "crash without done marker" {
				// The expectation was added after the activation.
				var err error
				_, v.logs, v.crashes, err = normalizeVerificationConfig(fx.config)
				if err != nil {
					t.Fatal(err)
				}
			}
			report := fx.evaluate(t, v)
			want := VerificationIncomplete
			if tc.logs == "" && tc.crash == "" {
				want = VerificationClean
			}
			if report.Outcome != want {
				t.Fatalf("outcome %s, want %s; logs:\n%s\ncrashes:\n%s", report.Outcome, want, gapReasons(report.Logs), gapReasons(report.Crashes))
			}
			for _, check := range []struct {
				name, want string
				check      VerificationCheck
			}{{"logs", tc.logs, report.Logs}, {"crashes", tc.crash, report.Crashes}} {
				reasons := gapReasons(check.check)
				switch {
				case check.want == "" && check.check.Outcome != VerificationClean:
					t.Errorf("%s: %s with gaps:\n%s", check.name, check.check.Outcome, reasons)
				case check.want != "" && (check.check.Outcome != VerificationIncomplete || !strings.Contains(reasons, check.want)):
					t.Errorf("%s: %s, gaps:\n%s\nwant %q", check.name, check.check.Outcome, reasons, check.want)
				}
			}
		})
	}
}

// An unexpected event stays unexpected when a gap also exists; the gap stays
// in the report.
func TestVerificationUnexpectedOutranksGaps(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	fx.line("osd0", 2*time.Minute, "FAILED ceph_assert(false)")
	fx.docker.containers["osd1"].removed = true
	report := fx.evaluate(t, v)
	if report.Outcome != VerificationUnexpected || report.Logs.Outcome != VerificationUnexpected || len(report.Logs.Gaps) == 0 ||
		report.Crashes.Outcome != VerificationIncomplete {
		t.Fatalf("report %s, logs %+v, crashes %+v", report.Outcome, report.Logs, report.Crashes)
	}
}

// Gaps at activation persist into every evaluation, and an unread baseline
// crash directory never turns its records into new crashes.
func TestVerificationActivationGaps(t *testing.T) {
	fx := newVerifyFixture()
	fx.docker.containers["osd0"].copyErr = errors.New("archive refused")
	fx.docker.containers["osd1"].inspectErr = errors.New("permission denied")
	v := fx.activate(t)
	fx.docker.containers["osd0"].copyErr = nil
	fx.docker.containers["osd1"].inspectErr = nil
	report := fx.evaluate(t, v)
	if report.Crashes.Outcome != VerificationIncomplete || report.Crashes.Unexpected != 0 ||
		!strings.Contains(gapReasons(report.Crashes), "osd.0: at activation: crash directory failed") ||
		!strings.Contains(gapReasons(report.Crashes), "osd.0: 1 crash records whose presence at activation is unknown") ||
		!strings.Contains(gapReasons(report.Logs), "osd.1: at activation: permission denied") || report.Logs.Outcome != VerificationIncomplete {
		t.Fatalf("logs:\n%s\ncrashes:\n%s", gapReasons(report.Logs), gapReasons(report.Crashes))
	}
}

// A canceled evaluation is incomplete, and a failed activation clock read
// fails the activation.
func TestVerificationDeadlinesAndActivationErrors(t *testing.T) {
	fx := newVerifyFixture()
	v := fx.activate(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := v.Evaluate(ctx)
	if err != nil || report.Outcome != VerificationIncomplete || report.Logs.Outcome != VerificationIncomplete || report.Crashes.Outcome != VerificationIncomplete {
		t.Fatalf("canceled evaluation %v: %+v", err, report)
	}

	fx = newVerifyFixture()
	fx.docker.infoErr = errors.New("engine down")
	v, err = newVerification(fx.config)
	if err != nil {
		t.Fatal(err)
	}
	fx.wire(v)
	if err := v.activate(t.Context()); err == nil || !strings.Contains(err.Error(), "engine down") {
		t.Fatalf("activation without the engine's clock: %v", err)
	}
	if _, err := v.Evaluate(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("evaluation without a baseline: %v", err)
	}
	var nilVerification *Verification
	if _, err := nilVerification.Evaluate(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil verification: %v", err)
	}
}

// Findings keep the unexpected events when the limit cuts the list.
func TestVerificationFindingLimitKeepsUnexpected(t *testing.T) {
	fx := newVerifyFixture()
	fx.config.MaxFindings = 2
	fx.config.Logs = []LogExpectation{{Name: "warnings", Daemon: "mon.a", Pattern: `WRN`}}
	v := fx.activate(t)
	for i := range 3 {
		fx.line("mon", time.Duration(i+2)*time.Minute, daemonLogLine("cluster", "WRN", "permitted"))
	}
	fx.line("osd1", 6*time.Minute, daemonLogLine("cluster", "ERR", "unexpected"))
	report := fx.evaluate(t, v)
	if report.Logs.Unexpected != 1 || report.Logs.Permitted != 3 || report.Logs.OmittedFindings != 2 || len(report.Logs.Findings) != 2 ||
		report.Logs.Findings[0].Expectation != "" || report.Logs.Findings[0].Source != "osd.1" {
		t.Fatalf("logs %+v", report.Logs)
	}
}

func TestVerificationConfigValidation(t *testing.T) {
	valid := VerificationConfig{
		Logs:    []LogExpectation{{Name: "l", Pattern: "x", Event: LogClusterWarning, Required: true, MinCount: 2, MaxCount: 3}},
		Crashes: []CrashExpectation{{Name: "c", Process: "ceph-osd"}},
	}
	config, logs, crashes, err := normalizeVerificationConfig(valid)
	if err != nil || config.Name != "post-test" || config.Timeout != 2*time.Minute || config.MaxLogBytes != 32<<20 || config.MaxFindings != 256 ||
		len(logs) != 1 || len(crashes) != 1 || logs[0].minCount != 2 {
		t.Fatalf("valid configuration: %v %+v", err, config)
	}
	if _, logs, _, _ := normalizeVerificationConfig(VerificationConfig{Logs: []LogExpectation{{Name: "r", Pattern: "x", Required: true}}}); logs[0].minCount != 1 {
		t.Fatalf("a required expectation without MinCount needs %d events", logs[0].minCount)
	}
	for name, mutate := range map[string]func(*VerificationConfig){
		"two-line name":            func(c *VerificationConfig) { c.Name = "a\nb" },
		"negative timeout":         func(c *VerificationConfig) { c.Timeout = -time.Second },
		"long timeout":             func(c *VerificationConfig) { c.Timeout = 6 * time.Minute },
		"long operation timeout":   func(c *VerificationConfig) { c.OperationTimeout = 2 * time.Minute },
		"huge log limit":           func(c *VerificationConfig) { c.MaxLogBytes = 1 << 30 },
		"huge finding limit":       func(c *VerificationConfig) { c.MaxFindings = 5000 },
		"log without name":         func(c *VerificationConfig) { c.Logs[0].Name = "" },
		"log without pattern":      func(c *VerificationConfig) { c.Logs[0].Pattern = "" },
		"log with invalid pattern": func(c *VerificationConfig) { c.Logs[0].Pattern = "(" },
		"log with unknown event":   func(c *VerificationConfig) { c.Logs[0].Event = "info" },
		"min over max":             func(c *VerificationConfig) { c.Logs[0].MinCount = 4 },
		"negative count":           func(c *VerificationConfig) { c.Logs[0].MaxCount = -1 },
		"min without required":     func(c *VerificationConfig) { c.Logs[0].Required = false },
		"duplicate names":          func(c *VerificationConfig) { c.Crashes[0].Name = "l" },
		"crash without criteria":   func(c *VerificationConfig) { c.Crashes[0].Process = "" },
		"crash invalid signature":  func(c *VerificationConfig) { c.Crashes[0].Signature = "[" },
		"crash daemon newline":     func(c *VerificationConfig) { c.Crashes[0].Daemon = "osd.0\n" },
		"too many redactions":      func(c *VerificationConfig) { c.RedactValues = make([]string, 129) },
		"invalid container": func(c *VerificationConfig) {
			c.AdditionalContainers = []diagnostics.Target{{Role: "x\ny"}}
		},
	} {
		config := valid
		config.Logs, config.Crashes = slices.Clone(valid.Logs), slices.Clone(valid.Crashes)
		mutate(&config)
		if _, _, _, err := normalizeVerificationConfig(config); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

// The refusal texts are part of the contract: each names the field at fault.
func TestVerificationConfigRefusalMessages(t *testing.T) {
	valid := VerificationConfig{
		Logs:    []LogExpectation{{Name: "l", Pattern: "x", Event: LogClusterWarning, Required: true, MinCount: 2, MaxCount: 3}},
		Crashes: []CrashExpectation{{Name: "c", Process: "ceph-osd"}},
	}
	for want, mutate := range map[string]func(*VerificationConfig){
		"verification: name must be one line of at most 128 bytes":                             func(c *VerificationConfig) { c.Name = "a\nb" },
		"verification: timeouts out of range":                                                  func(c *VerificationConfig) { c.Timeout = -time.Second },
		"verification: limits out of range":                                                    func(c *VerificationConfig) { c.MaxFindings = 5000 },
		"verification: more than 256 expectations of one kind":                                 func(c *VerificationConfig) { c.Logs = make([]LogExpectation, 257) },
		"verification: too many additional containers or redaction values":                     func(c *VerificationConfig) { c.RedactValues = make([]string, 129) },
		"verification: invalid container description":                                          func(c *VerificationConfig) { c.AdditionalContainers = []diagnostics.Target{{Role: "x\ny"}} },
		"verification: redaction value exceeds 4096 bytes":                                     func(c *VerificationConfig) { c.RedactValues = []string{strings.Repeat("x", 4097)} },
		"verification: log expectation name must be one non-empty line of at most 128 bytes":   func(c *VerificationConfig) { c.Logs[0].Name = "" },
		"verification: crash expectation name must be one non-empty line of at most 128 bytes": func(c *VerificationConfig) { c.Crashes[0].Name = "" },
		`verification: expectation name "l" is used twice`:                                     func(c *VerificationConfig) { c.Crashes[0].Name = "l" },
		`verification: expectation "l" has a negative count`:                                   func(c *VerificationConfig) { c.Logs[0].MaxCount = -1 },
		`verification: expectation "l" sets MinCount without Required`:                         func(c *VerificationConfig) { c.Logs[0].Required = false },
		`verification: expectation "l" has an invalid criterion`:                               func(c *VerificationConfig) { c.Logs[0].Daemon = "osd\n" },
		`verification: expectation "l" requires more events than it permits`:                   func(c *VerificationConfig) { c.Logs[0].MinCount = 4 },
		"verification: expectation \"l\": error parsing regexp: missing closing ): `(`":        func(c *VerificationConfig) { c.Logs[0].Pattern = "(" },
		`verification: log expectation "l" has no pattern`:                                     func(c *VerificationConfig) { c.Logs[0].Pattern = "" },
		`verification: log expectation "l" has unknown event "info"`:                           func(c *VerificationConfig) { c.Logs[0].Event = "info" },
		`verification: crash expectation "c" has an invalid process`:                           func(c *VerificationConfig) { c.Crashes[0].Process = "a\nb" },
		`verification: crash expectation "c" needs a daemon, a process or a signature`:         func(c *VerificationConfig) { c.Crashes[0].Process = "" },
	} {
		config := valid
		config.Logs, config.Crashes = slices.Clone(valid.Logs), slices.Clone(valid.Crashes)
		mutate(&config)
		_, _, _, err := normalizeVerificationConfig(config)
		if err == nil || !strings.HasSuffix(err.Error(), want) || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("want %q, got %v", want, err)
		}
	}
}

// verifyFinalization is a finalization fixture whose Verify activates over
// a verifyFixture.
func verifyFinalization(t *testing.T, config Config) (*finalizationFixture, *verifyFixture) {
	t.Helper()
	fx := newFinalizationFixture(t, config)
	vfx := newVerifyFixture()
	fx.activate = func(ctx context.Context, config VerificationConfig) (*Verification, error) {
		v, err := newVerification(config)
		if err != nil {
			return nil, err
		}
		vfx.wire(v)
		if err := v.activate(ctx); err != nil {
			return nil, err
		}
		vfx.docker.clock = verifyStart.Add(10 * time.Minute)
		return v, nil
	}
	return fx, vfx
}

// An unexpected crash fails the test through the verification's check,
// which triggers the archive before the cleanups; the report is archived
// and every cleanup and Terminate still run.
func TestFinalizationVerificationFailureArchivesBeforeCleanup(t *testing.T) {
	fx, vfx := verifyFinalization(t, Config{Archive: true})
	if _, err := fx.Verify(t.Context(), VerificationConfig{}); err != nil {
		t.Fatal(err)
	}
	fx.Cleanup("client", func(context.Context) error {
		fx.event("client cleanup")
		return nil
	})
	vfx.docker.containers["osd1"].crashes = []string{"2026-10-23T01:04:00Z_new"}
	fx.run()
	if !fx.tb.Failed() || len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "check verification post-test: post-test verification post-test: unexpected") {
		t.Fatalf("errors %v", fx.tb.errors)
	}
	if want := []string{"diagnostics", "crash", "client cleanup", "terminate"}; !slices.Equal(sortArchive(fx.events), want) {
		t.Fatalf("events %v, want %v", fx.events, want)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives %v", archives)
	}
	manifest := readManifest(t, archives[0])
	if !manifest.Trigger.CheckFailed || manifest.Phase != "before-cleanup" || !slices.ContainsFunc(manifest.Files, func(file finalizationFile) bool {
		return file.Name == "verification-1-post-test.json" && file.Error == ""
	}) {
		t.Fatalf("manifest %+v", manifest)
	}
	data, err := os.ReadFile(filepath.Join(archives[0], "verification-1-post-test.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report VerificationReport
	if err := json.Unmarshal(data, &report); err != nil || report.Outcome != VerificationUnexpected || report.Crashes.Findings[0].Source != "osd.1" {
		t.Fatalf("archived report %v: %s", err, data)
	}
}

// sortArchive puts the two concurrent collectors in a fixed order.
func sortArchive(events []string) []string {
	events = slices.Clone(events)
	if len(events) > 1 && events[0] == "crash" && events[1] == "diagnostics" {
		events[0], events[1] = events[1], events[0]
	}
	return events
}

// A clean verification passes without an archive; an incomplete one fails
// the test like an unexpected one.
func TestFinalizationVerificationOutcomes(t *testing.T) {
	t.Run("clean", func(t *testing.T) {
		fx, _ := verifyFinalization(t, Config{Archive: true})
		if _, err := fx.Verify(t.Context(), VerificationConfig{}); err != nil {
			t.Fatal(err)
		}
		fx.run()
		if fx.tb.Failed() || len(fx.archives(t)) != 0 || !slices.Equal(fx.events, []string{"terminate"}) ||
			!strings.Contains(strings.Join(fx.tb.logs, "\n"), "ceph post-test verification post-test: clean") {
			t.Fatalf("errors %v, events %v, logs %v", fx.tb.errors, fx.events, fx.tb.logs)
		}
	})
	t.Run("incomplete", func(t *testing.T) {
		fx, vfx := verifyFinalization(t, Config{})
		if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "logs only"}); err != nil {
			t.Fatal(err)
		}
		vfx.docker.containers["mon"].removed = true
		fx.run()
		if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "post-test verification logs only: incomplete") ||
			!slices.Equal(fx.events, []string{"terminate"}) {
			t.Fatalf("errors %v, events %v", fx.tb.errors, fx.events)
		}
	})
}

func TestFinalizationVerifyRefusals(t *testing.T) {
	fx, _ := verifyFinalization(t, Config{})
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "one"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "bad", Logs: []LogExpectation{{Name: "x"}}}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("invalid configuration: %v", err)
	}
	// The failed activation released its name.
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "bad"}); err != nil {
		t.Fatalf("name of a failed activation: %v", err)
	}
	fx.run()
	if _, err := fx.Verify(t.Context(), VerificationConfig{Name: "late"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("after finalization started: %v", err)
	}
	var none *Finalization
	if _, err := none.Verify(t.Context(), VerificationConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil finalization: %v", err)
	}
	if _, err := (&Finalization{}).Verify(t.Context(), VerificationConfig{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("finalization without a cluster: %v", err)
	}
}

func TestVerificationSourceNames(t *testing.T) {
	for _, tc := range [][3]string{{"osd", "0", "osd.0"}, {"mds", "mds.a", "mds.a"}, {"rgw", "rgw", "rgw"}, {"rgw", "rgw:zone", "rgw:zone"}, {"extra", "mirror", "extra.mirror"}} {
		if got := verificationSourceName(tc[0], tc[1]); got != tc[2] {
			t.Errorf("%s %s = %s, want %s", tc[0], tc[1], got, tc[2])
		}
	}
}
