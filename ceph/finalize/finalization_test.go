package finalize

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidencehook"
)

// finalizationTB records what a finalization reports to its test. Only the
// methods that a finalization calls are implemented.
type finalizationTB struct {
	testing.TB
	name     string
	mu       sync.Mutex
	failed   bool
	errors   []string
	logs     []string
	cleanups []func()
}

func (tb *finalizationTB) Name() string { return tb.name }
func (tb *finalizationTB) Failed() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.failed
}
func (tb *finalizationTB) Errorf(format string, args ...any) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.failed = true
	tb.errors = append(tb.errors, fmt.Sprintf(format, args...))
}

// FailNow ends the calling goroutine as testing.T's does.
func (tb *finalizationTB) FailNow() {
	tb.mu.Lock()
	tb.failed = true
	tb.mu.Unlock()
	runtime.Goexit()
}
func (tb *finalizationTB) Fatalf(format string, args ...any) {
	tb.Errorf(format, args...)
	tb.FailNow()
}
func (tb *finalizationTB) Logf(format string, args ...any) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.logs = append(tb.logs, fmt.Sprintf(format, args...))
}
func (tb *finalizationTB) Cleanup(cleanup func()) { tb.cleanups = append(tb.cleanups, cleanup) }

// finalizationFixture is a Finalization over fake collectors that record
// the order of every phase.
type finalizationFixture struct {
	*Finalization
	tb     *finalizationTB
	mu     sync.Mutex
	events []string
	root   string
}

func (fx *finalizationFixture) event(name string) {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	fx.events = append(fx.events, name)
}

func newFinalizationFixture(t *testing.T, config Config) *finalizationFixture {
	t.Helper()
	fx := &finalizationFixture{tb: &finalizationTB{name: "TestWorkload/case_1"}, root: t.TempDir()}
	if config.ArchiveDirectory == "" {
		config.ArchiveDirectory = fx.root
	}
	f, err := newFinalization(fx.tb, config)
	if err != nil {
		t.Fatal(err)
	}
	f.cluster = "0123456789abcdef0123"
	f.images = ceph.ImageSet{Essential: "example.invalid/ceph:test"}
	f.collectDiagnostics = func(ctx context.Context, _ diagnostics.Config) (*diagnostics.Report, error) {
		fx.event("diagnostics")
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &diagnostics.Report{SchemaVersion: 1, Complete: true, Artifacts: []diagnostics.Artifact{{Kind: "ceph", Name: "status", Data: "{}"}}}, nil
	}
	f.collectCrash = func(ctx context.Context, _ diagnostics.CrashMetadataConfig) (*diagnostics.CrashMetadataReport, error) {
		fx.event("crash")
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &diagnostics.CrashMetadataReport{SchemaVersion: 1, Complete: true, Containers: []diagnostics.CrashMetadataContainer{
			{Role: "osd", Name: "0", Outcome: diagnostics.CrashDirectoryEmpty, Records: []diagnostics.CrashRecord{}},
			{Role: "mon", Name: "a", Outcome: diagnostics.CrashRecordsFound, Records: []diagnostics.CrashRecord{{CrashID: "c1"}}},
		}}, nil
	}
	f.terminate = func(context.Context) error {
		fx.event("terminate")
		return nil
	}
	fx.Finalization = f
	return fx
}

// run registers the finalization as TerminateOnCleanup does and runs the
// test's cleanups.
func (fx *finalizationFixture) run() {
	fx.tb.Cleanup(fx.finalize)
	for i := len(fx.tb.cleanups) - 1; i >= 0; i-- {
		fx.tb.cleanups[i]()
	}
}

func (fx *finalizationFixture) archives(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(fx.root, "*", "ceph-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func readManifest(t *testing.T, directory string) finalizationManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest finalizationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestFinalizationPassingRunCollectsNothing(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	fx.Check("verify", func(context.Context) error { fx.event("check"); return nil })
	fx.Cleanup("client-a", func(context.Context) error { fx.event("cleanup-a"); return nil })
	fx.Cleanup("client-b", func(context.Context) error { fx.event("cleanup-b"); return nil })
	fx.run()
	if want := []string{"check", "cleanup-b", "cleanup-a", "terminate"}; !slices.Equal(fx.events, want) {
		t.Fatalf("events = %v, want %v", fx.events, want)
	}
	if fx.tb.failed || len(fx.archives(t)) != 0 {
		t.Fatalf("a passing run failed (%v) or wrote an archive: %v", fx.tb.errors, fx.archives(t))
	}
	if entries, _ := os.ReadDir(fx.root); len(entries) != 0 {
		t.Fatalf("a passing run created %v", entries)
	}
}

func TestFinalizationArchivesAssertionFailureBeforeCleanupAndTermination(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true, Scenario: "short/basic"})
	fx.Check("verify", func(context.Context) error { fx.event("check"); return nil })
	fx.Cleanup("client", func(context.Context) error { fx.event("cleanup"); return nil })
	fx.terminate = func(context.Context) error { fx.event("terminate"); return errors.New("network still in use") }
	fx.tb.failed = true // the test body's assertion
	fx.run()
	events := slices.Clone(fx.events)
	slices.Sort(events[1:3]) // the collectors run concurrently
	if want := []string{"check", "crash", "diagnostics", "cleanup", "terminate"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", fx.events, want)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	for _, name := range []string{"diagnostics.json", "crash-metadata.json", "manifest.json"} {
		if _, err := os.Stat(filepath.Join(archives[0], name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(archives[0], ".manifest.json.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary manifest left behind: %v", err)
	}
	m := readManifest(t, archives[0])
	if m.Test != "TestWorkload/case_1" || m.Scenario != "short/basic" || m.Phase != "before-cleanup" || !m.Final ||
		!m.Trigger.TestFailed || m.Trigger.CheckFailed || m.Trigger.CleanupFailed || !m.Archive.Complete || !m.Archive.Retained ||
		m.Images.Essential != "example.invalid/ceph:test" || m.Execution.ID == "" || m.Archive.ID != filepath.Base(archives[0]) {
		t.Fatalf("manifest = %+v", m)
	}
	if m.Termination == nil || m.Termination.Error != "network still in use" || len(m.Cleanups) != 1 || len(m.Checks) != 1 {
		t.Fatalf("late outcomes = %+v %+v", m.Termination, m.Cleanups)
	}
	if m.Collectors[1].CrashOutcomes[diagnostics.CrashDirectoryEmpty] != 1 || m.Collectors[1].CrashOutcomes[diagnostics.CrashRecordsFound] != 1 {
		t.Fatalf("crash outcomes = %v", m.Collectors[1].CrashOutcomes)
	}
	if !strings.Contains(filepath.Dir(archives[0]), "TestWorkload_case_1") {
		t.Fatalf("archive %s is not under the test's directory", archives[0])
	}
	if len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "terminate the cluster") {
		t.Fatalf("errors = %v", fx.tb.errors)
	}
	data, _ := os.ReadFile(filepath.Join(archives[0], "manifest.json"))
	if strings.Contains(string(data), fx.root) {
		t.Fatal("the manifest records the host path of the archive")
	}
}

func TestFinalizationArchivesLaterCheckFailure(t *testing.T) {
	for _, reported := range []string{"returned", "reported", "panicked"} {
		t.Run(reported, func(t *testing.T) {
			fx := newFinalizationFixture(t, Config{Archive: true})
			fx.Check("first", func(context.Context) error { fx.event("first"); return nil })
			fx.Check("crash-check", func(context.Context) error {
				fx.event("crash-check")
				switch reported {
				case "returned":
					return errors.New("unexpected crash osd.1")
				case "reported":
					fx.tb.Errorf("unexpected crash osd.1")
				default:
					panic("unexpected crash osd.1")
				}
				return nil
			})
			fx.run()
			if fx.events[0] != "first" || fx.events[1] != "crash-check" || fx.events[len(fx.events)-1] != "terminate" || len(fx.events) != 5 {
				t.Fatalf("events = %v", fx.events)
			}
			archives := fx.archives(t)
			if len(archives) != 1 {
				t.Fatalf("archives = %v", archives)
			}
			m := readManifest(t, archives[0])
			if m.Trigger.TestFailed || m.Phase != "before-cleanup" {
				t.Fatalf("trigger = %+v phase %s", m.Trigger, m.Phase)
			}
			if returned := reported != "reported"; m.Trigger.CheckFailed != returned || (m.Checks[1].Error != "") != returned {
				t.Fatalf("checks = %+v trigger %+v", m.Checks, m.Trigger)
			}
			// A report through the test is recorded apart from an error.
			if through := reported == "reported"; m.Trigger.CheckReportedFailure != through ||
				m.Checks[0].FailedTest || m.Checks[1].FailedTest != through {
				t.Fatalf("checks = %+v trigger %+v", m.Checks, m.Trigger)
			}
		})
	}
}

func TestFinalizationUsesFreshBoundedContexts(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true, CheckTimeout: time.Minute, ArchiveTimeout: 2 * time.Minute, CleanupTimeout: 3 * time.Minute})
	// The test's own context has ended before cleanup; every phase gets a
	// fresh one with its own bound.
	bounds := map[string]time.Duration{}
	record := func(name string, ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || ctx.Err() != nil {
			return fmt.Errorf("%s context has no deadline or ended", name)
		}
		fx.mu.Lock()
		bounds[name] = time.Until(deadline)
		fx.mu.Unlock()
		return nil
	}
	fx.Check("check", func(ctx context.Context) error { fx.tb.Errorf("failed"); return record("check", ctx) })
	collect := fx.collectDiagnostics
	fx.collectDiagnostics = func(ctx context.Context, config diagnostics.Config) (*diagnostics.Report, error) {
		if err := record("archive", ctx); err != nil {
			return nil, err
		}
		return collect(ctx, config)
	}
	fx.Cleanup("cleanup", func(ctx context.Context) error { return record("cleanup", ctx) })
	fx.terminate = func(ctx context.Context) error { return record("terminate", ctx) }
	fx.run()
	for name, limit := range map[string]time.Duration{"check": time.Minute, "archive": 2 * time.Minute, "cleanup": 3 * time.Minute, "terminate": 3 * time.Minute} {
		if bounds[name] <= 0 || bounds[name] > limit {
			t.Errorf("%s bound = %v, want within %v", name, bounds[name], limit)
		}
	}
	if len(fx.tb.errors) != 1 {
		t.Fatalf("errors = %v", fx.tb.errors)
	}
}

func TestFinalizationKeepsPartialOutputOfFailedCollectors(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	fx.collectDiagnostics = func(context.Context, diagnostics.Config) (*diagnostics.Report, error) {
		return &diagnostics.Report{SchemaVersion: 1, Artifacts: []diagnostics.Artifact{{Kind: "ceph", Name: "status", Error: "timed out"}}},
			errors.New("ceph/status: timed out")
	}
	fx.collectCrash = func(context.Context, diagnostics.CrashMetadataConfig) (*diagnostics.CrashMetadataReport, error) {
		panic("Docker client vanished")
	}
	fx.tb.failed = true
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	if _, err := os.Stat(filepath.Join(archives[0], "diagnostics.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(archives[0], "crash-metadata.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a crash report was written without one: %v", err)
	}
	m := readManifest(t, archives[0])
	diagnostics, crash := m.Collectors[0], m.Collectors[1]
	if !diagnostics.Reported || diagnostics.Complete || diagnostics.Error != "ceph/status: timed out" ||
		crash.Reported || !strings.Contains(crash.Error, "Docker client vanished") || m.Archive.Complete || len(m.Files) != 1 {
		t.Fatalf("manifest = %+v", m)
	}
	if m.Termination == nil || m.Termination.Error != "" {
		t.Fatalf("termination = %+v", m.Termination)
	}
	if len(fx.tb.errors) != 0 {
		t.Fatalf("collection errors failed the test again: %v", fx.tb.errors)
	}
}

func TestFinalizationStorageErrorsDoNotBlockCleanup(t *testing.T) {
	t.Run("one file", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		write := fx.writeFile
		fx.writeFile = func(path string, data []byte) error {
			if filepath.Base(path) == "diagnostics.json" {
				return errors.New("no space left on device")
			}
			return write(path, data)
		}
		fx.tb.failed = true
		fx.run()
		archives := fx.archives(t)
		if len(archives) != 1 {
			t.Fatalf("archives = %v", archives)
		}
		m := readManifest(t, archives[0])
		if len(m.Files) != 2 || m.Files[0].Error != "no space left on device" || m.Files[1].Error != "" || m.Archive.Complete {
			t.Fatalf("files = %+v", m.Files)
		}
		if _, err := os.Stat(filepath.Join(archives[0], "crash-metadata.json")); err != nil {
			t.Fatal(err)
		}
		if fx.events[len(fx.events)-1] != "terminate" {
			t.Fatalf("events = %v", fx.events)
		}
	})
	t.Run("directory", func(t *testing.T) {
		blocked := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(blocked, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		fx := newFinalizationFixture(t, Config{Archive: true, ArchiveDirectory: blocked})
		fx.Cleanup("client", func(context.Context) error { fx.event("cleanup"); return nil })
		fx.tb.failed = true
		fx.run()
		if want := []string{"cleanup", "terminate"}; !slices.Equal(fx.events, want) {
			t.Fatalf("events = %v, want %v", fx.events, want)
		}
		if len(fx.tb.logs) != 1 || !strings.Contains(fx.tb.logs[0], "nothing was written") {
			t.Fatalf("logs = %v", fx.tb.logs)
		}
	})
	t.Run("manifest", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		write := fx.writeFile
		fx.writeFile = func(path string, data []byte) error {
			if strings.Contains(path, "manifest") {
				return errors.New("read-only file system")
			}
			return write(path, data)
		}
		fx.tb.failed = true
		fx.run()
		if fx.events[len(fx.events)-1] != "terminate" || len(fx.archives(t)) != 1 {
			t.Fatalf("events = %v", fx.events)
		}
		logs := strings.Join(fx.tb.logs, "\n")
		if !strings.Contains(logs, "write manifest.json: read-only file system") || !strings.Contains(logs, "rewrite the manifest") {
			t.Fatalf("logs = %v", fx.tb.logs)
		}
	})
}

func TestFinalizationArchivesCleanupFailureBeforeTermination(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	fx.Cleanup("restore-network", func(context.Context) error { fx.event("cleanup"); return errors.New("endpoint not restored") })
	fx.run()
	if fx.events[0] != "cleanup" || fx.events[len(fx.events)-1] != "terminate" || len(fx.events) != 4 {
		t.Fatalf("events = %v", fx.events)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	m := readManifest(t, archives[0])
	if m.Phase != "after-cleanup" || !m.Trigger.CleanupFailed || m.Trigger.TestFailed || m.Cleanups[0].Error != "endpoint not restored" {
		t.Fatalf("manifest = %+v", m)
	}
}

func TestFinalizationRecordsCleanupsThatFailThroughTheTest(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	fx.Cleanup("remove-client", func(context.Context) error {
		fx.event("cleanup")
		fx.tb.Errorf("client not removed")
		return nil
	})
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	m := readManifest(t, archives[0])
	if m.Phase != "after-cleanup" || m.Trigger.CleanupFailed || !m.Trigger.CleanupReportedFailure ||
		m.Cleanups[0].Error != "" || !m.Cleanups[0].FailedTest || !m.Final {
		t.Fatalf("manifest = %+v", m)
	}
}

// A check or cleanup that calls t.Fatal ends its goroutine through
// runtime.Goexit, which recover does not intercept.
func TestFinalizationSurvivesFatalChecksAndCleanups(t *testing.T) {
	t.Run("check", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.Cleanup("remove-client", func(context.Context) error { fx.event("cleanup"); return nil })
		fx.Check("fatal", func(context.Context) error {
			fx.event("check")
			fx.tb.Fatalf("unexpected crash osd.1")
			return nil
		})
		fx.Check("later", func(context.Context) error { fx.event("later"); return nil })
		fx.run()
		// The archive still sees the client, and nothing is skipped.
		if want := []string{"check", "later", "diagnostics", "crash", "cleanup", "terminate"}; !sameEvents(fx.events, want) {
			t.Fatalf("events = %v, want %v", fx.events, want)
		}
		m := readManifest(t, fx.archives(t)[0])
		if m.Phase != "before-cleanup" || !m.Trigger.CheckFailed || !m.Trigger.CheckReportedFailure ||
			!strings.Contains(m.Checks[0].Error, "without returning") || !m.Checks[0].FailedTest || len(m.Checks) != 2 || !m.Final {
			t.Fatalf("manifest = %+v", m)
		}
	})
	t.Run("cleanup", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.Cleanup("first-registered", func(context.Context) error { fx.event("first-registered"); return nil })
		fx.Cleanup("fatal", func(context.Context) error {
			fx.event("fatal")
			fx.tb.Fatalf("endpoint not restored")
			return nil
		})
		fx.run()
		if want := []string{"fatal", "first-registered", "diagnostics", "crash", "terminate"}; !sameEvents(fx.events, want) {
			t.Fatalf("events = %v, want %v", fx.events, want)
		}
		m := readManifest(t, fx.archives(t)[0])
		if m.Phase != "after-cleanup" || !m.Trigger.CleanupFailed || !m.Trigger.CleanupReportedFailure ||
			len(m.Cleanups) != 2 || !strings.Contains(m.Cleanups[0].Error, "without returning") || !m.Final {
			t.Fatalf("manifest = %+v", m)
		}
	})
}

// sameEvents compares events, in which the two collectors may come in
// either order.
func sameEvents(got, want []string) bool {
	normalize := func(events []string) []string {
		events = slices.Clone(events)
		for i := 0; i+1 < len(events); i++ {
			if events[i] == "crash" && events[i+1] == "diagnostics" {
				events[i], events[i+1] = events[i+1], events[i]
			}
		}
		return events
	}
	return slices.Equal(normalize(got), normalize(want))
}

func TestFinalizationKeepsWorkflowRevisionApartFromSource(t *testing.T) {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				t.Skip("this test binary carries a VCS revision")
			}
		}
	}
	workflow := strings.Repeat("a", 40)
	t.Setenv("GITHUB_SHA", workflow)
	fx := newFinalizationFixture(t, Config{})
	m := fx.newManifest()
	if m.Execution.WorkflowSHA != workflow || m.Execution.Revision != "" || !slices.Contains(m.Missing, "source revision") {
		t.Fatalf("execution = %+v missing %v", m.Execution, m.Missing)
	}
}

func TestFinalizationArtifactIdentities(t *testing.T) {
	root := t.TempDir()
	var directories []string
	for range 3 {
		fx := newFinalizationFixture(t, Config{Archive: true, ArchiveDirectory: root})
		fx.root = root
		fx.tb.failed = true
		fx.run()
		directories = fx.archives(t)
	}
	if len(directories) != 3 {
		t.Fatalf("repeated executions share archives: %v", directories)
	}
	ids := map[string]bool{}
	for _, directory := range directories {
		ids[readManifest(t, directory).Execution.ID] = true
	}
	if len(ids) != 3 {
		t.Fatalf("execution IDs = %v", ids)
	}

	// Parallel subtests have other names; a colliding ID is replaced.
	fx := newFinalizationFixture(t, Config{Archive: true, ArchiveDirectory: root})
	fx.root = root
	ids = map[string]bool{}
	for _, directory := range directories {
		ids[readManifest(t, directory).Execution.ID] = true
	}
	taken := readManifest(t, directories[0]).Execution.ID
	next := []string{taken, "fresh0000000000"}
	fx.newID = func() string { id := next[0]; next = next[1:]; return id }
	fx.tb.failed = true
	fx.run()
	after := fx.archives(t)
	if len(after) != 4 || readManifest(t, directories[0]).Final != true {
		t.Fatalf("a colliding execution ID overwrote an archive: %v", after)
	}
	if !slices.ContainsFunc(after, func(directory string) bool { return strings.HasSuffix(directory, "-fresh0000000000") }) {
		t.Fatalf("archives = %v", after)
	}

	parallel := newFinalizationFixture(t, Config{Archive: true, ArchiveDirectory: root})
	parallel.root = root
	parallel.tb.name = "TestWorkload/case_2"
	parallel.tb.failed = true
	parallel.run()
	if got := parallel.archives(t); len(got) != 5 {
		t.Fatalf("archives = %v", got)
	}
}

func TestFinalizationDefaultDirectoryReportsRetention(t *testing.T) {
	for _, retained := range []bool{true, false} {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.config.ArchiveDirectory = ""
		artifacts := t.TempDir()
		fx.artifactDir = func() (string, bool) { return artifacts, retained }
		fx.tb.failed = true
		fx.run()
		archives, _ := filepath.Glob(filepath.Join(artifacts, "ceph-*"))
		if len(archives) != 1 {
			t.Fatalf("archives = %v", archives)
		}
		if m := readManifest(t, archives[0]); m.Archive.Retained != retained {
			t.Fatalf("retained = %v, want %v", m.Archive.Retained, retained)
		}
		if logs := strings.Join(fx.tb.logs, "\n"); strings.Contains(logs, "-artifacts") == retained {
			t.Fatalf("logs = %v", fx.tb.logs)
		}
	}
}

func TestFinalizationNegativeControls(t *testing.T) {
	t.Run("archive not selected", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{})
		fx.tb.failed = true
		fx.run()
		if want := []string{"terminate"}; !slices.Equal(fx.events, want) || len(fx.archives(t)) != 0 {
			t.Fatalf("events = %v", fx.events)
		}
	})
	t.Run("late registration", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.Check("late", func(context.Context) error {
			fx.Cleanup("too late", func(context.Context) error { fx.event("too late"); return nil })
			return nil
		})
		fx.run()
		if slices.Contains(fx.events, "too late") || len(fx.tb.errors) != 1 || !strings.Contains(fx.tb.errors[0], "after finalization started") {
			t.Fatalf("events = %v errors = %v", fx.events, fx.tb.errors)
		}
	})
	t.Run("nil function", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{})
		fx.Check("missing", nil)
		if len(fx.tb.errors) != 1 {
			t.Fatalf("errors = %v", fx.tb.errors)
		}
	})
	t.Run("redaction", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true, Diagnostics: diagnostics.Config{RedactValues: []string{"s3cr3t"}}})
		fx.Check("leaky", func(context.Context) error { return errors.New("token s3cr3t rejected") })
		fx.run()
		data, err := os.ReadFile(filepath.Join(fx.archives(t)[0], "manifest.json"))
		if err != nil || strings.Contains(string(data), "s3cr3t") || strings.Contains(strings.Join(fx.tb.errors, ""), "s3cr3t") {
			t.Fatalf("secret kept: %s %v", data, fx.tb.errors)
		}
	})
	t.Run("invalid configuration", func(t *testing.T) {
		for _, config := range []Config{
			{ArchiveTimeout: -time.Second},
			{CleanupTimeout: 11 * time.Minute},
			{Scenario: "two\nlines"},
			{ArchiveDirectory: "bad\x00dir"},
			{Diagnostics: diagnostics.Config{LogTail: -1}},
			{CrashMetadata: diagnostics.CrashMetadataConfig{MaxRecords: -1}},
		} {
			if _, err := newFinalization(&finalizationTB{name: "T"}, config); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Errorf("%+v: err = %v", config, err)
			}
		}
		if _, err := TerminateOnCleanup(&finalizationTB{name: "T"}, nil, Config{}); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("nil cluster: err = %v", err)
		}
		if _, err := TerminateOnCleanup(nil, &ceph.Container{}, Config{}); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("nil test: err = %v", err)
		}
	})
}

func TestTerminateOnCleanupRegistersOneCleanup(t *testing.T) {
	tb := &finalizationTB{name: "TestRegistered"}
	cluster := &ceph.Container{}
	f, err := TerminateOnCleanup(tb, cluster, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tb.cleanups) != 1 || f.terminate == nil || f.collectDiagnostics == nil || f.collectCrash == nil || f.activate == nil || f.cluster != evidencehook.Owner(cluster) {
		t.Fatalf("registration = %d cleanups, %+v", len(tb.cleanups), f)
	}
}

func TestArtifactName(t *testing.T) {
	long := strings.Repeat("a", 200)
	for name, want := range map[string]string{
		"TestX/sub case": "TestX_sub_case",
		"../..":          "_",
		"":               "unnamed",
	} {
		if got := artifactName(name); got != want {
			t.Errorf("artifactName(%q) = %q, want %q", name, got, want)
		}
	}
	if got := artifactName(long); len(got) != 96 || got == artifactName(long+"b") {
		t.Errorf("long names = %q", got)
	}
}

func TestFinalizationDefaultDirectoryIsTheTestsArtifactDir(t *testing.T) {
	f, err := newFinalization(t, Config{Archive: true})
	if err != nil {
		t.Fatal(err)
	}
	directory, retained := f.artifactDir()
	if directory != t.ArtifactDir() {
		t.Fatalf("directory = %s, want the test's ArtifactDir", directory)
	}
	if want := flag.Lookup("test.artifacts").Value.String() == "true"; retained != want {
		t.Fatalf("retained = %v, want %v", retained, want)
	}
}
