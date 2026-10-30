package ceph

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// FinalizationConfig selects what the cleanup that TerminateOnCleanup
// registers does before it terminates the cluster. Zero durations select two
// minutes for the checks together, three minutes for the failure archive, and
// three minutes for the cleanups together and again for Terminate; each is at
// most ten minutes.
type FinalizationConfig struct {
	// Archive enables the failure archive: when the test has failed, a
	// check failed or a finalization cleanup failed, the diagnostics, crash
	// metadata and a manifest are written before the cluster is terminated.
	// A passing run collects nothing.
	Archive bool
	// ArchiveDirectory is the root under which each archive gets its own
	// directory, named after the test, the cluster and a random execution
	// ID. Empty selects the test's ArtifactDir, which go test keeps only
	// with its -artifacts flag; otherwise it is removed after the test.
	ArchiveDirectory string
	// Scenario labels the manifest, for example with a batch or profile name.
	Scenario string
	// CheckTimeout bounds all checks together (default 2 minutes),
	// ArchiveTimeout the whole archive with both collectors and every write
	// (default 3 minutes), and CleanupTimeout the cleanups together and
	// Terminate separately (default 3 minutes). Each is at most 10 minutes;
	// invalid fields are refused before anything is registered.
	CheckTimeout   time.Duration
	ArchiveTimeout time.Duration
	CleanupTimeout time.Duration
	// Diagnostics and CrashMetadata configure the two collectors; select
	// caller-owned clients or mirror daemons with their AdditionalContainers.
	Diagnostics   DiagnosticsConfig
	CrashMetadata CrashMetadataConfig
}

// Finalization is the single cleanup path that TerminateOnCleanup registers.
// It runs, in this order: the checks in registration order, the failure
// archive, the cleanups in reverse registration order, then Terminate, so
// that a check registered after the test body still decides whether to
// archive and that a client removed by a cleanup is still there to inspect.
// Captures registered with Capture are settled after the checks and before the
// archive. The archive is written when the test has failed by then, so a
// failure reported by a check or a capture settlement triggers it too, and a
// cleanup that fails when nothing had failed before triggers an archive of
// what still runs, labelled after-cleanup. Test cleanups registered after
// TerminateOnCleanup run before all of this, as Go runs cleanups last-in
// first-out, so route the removal of clients the archive should see through
// Cleanup. A check fails the test by returning an error, by panicking or
// through t. Adding a step after the finalization has started fails the test.
type Finalization struct {
	t      testing.TB
	config FinalizationConfig

	mu       sync.Mutex
	started  bool
	checks   []finalizationStep
	cleanups []finalizationStep
	// captures are the registered packet captures, and captureReports the
	// reports of those that were settled, for the archive.
	captures       []finalizationCapture
	captureReports []finalizationCaptureReport
	// verificationNames are the active verifications' names, and
	// verificationReports the reports of their checks, for the archive.
	verificationNames   map[string]bool
	verificationReports []*VerificationReport
	// debugWindows are the registered debug level handles in registration
	// order; a nil entry is a registration without a handle.
	debugWindows []DebugLevelReceipter

	// The cluster's operations, replaced in tests.
	cluster            string
	images             ImageSet
	collectDiagnostics func(context.Context, DiagnosticsConfig) (*DiagnosticsReport, error)
	collectCrash       func(context.Context, CrashMetadataConfig) (*CrashMetadataReport, error)
	terminate          func(context.Context) error
	activate           func(context.Context, VerificationConfig) (*Verification, error)
	writeFile          func(string, []byte) error
	artifactDir        func() (string, bool)
	now                func() time.Time
	newID              func() string
}

type finalizationStep struct {
	name string
	run  func(context.Context) error
}

const (
	defaultCheckTimeout   = 2 * time.Minute
	defaultArchiveTimeout = 3 * time.Minute
	defaultCleanupTimeout = 3 * time.Minute
	maxFinalizationBound  = 10 * time.Minute
)

// TerminateOnCleanup registers one t.Cleanup that finalizes and then
// terminates the cluster, and returns the Finalization to add checks and
// cleanups to. Register it right after Run instead of another cleanup that
// terminates the cluster, and route the removal of caller-owned clients that
// the archive should still see through Finalization.Cleanup: test cleanups
// registered later run before it.
//
// Every phase uses a fresh context bounded by its own timeout, because the
// test's context has ended by then. Check, collection and storage errors
// never skip the cleanups or Terminate. A check's error and Terminate's error
// fail the test; collection and storage errors are logged and recorded in the
// manifest. Nothing runs after a forced process exit or a lost runner.
//
//purpose:observation,lifetime
func (c *Container) TerminateOnCleanup(t testing.TB, config FinalizationConfig) (*Finalization, error) {
	if c == nil || t == nil {
		return nil, invalid(errors.New("TerminateOnCleanup needs a cluster and a test"))
	}
	f, err := newFinalization(t, config)
	if err != nil {
		return nil, err
	}
	f.cluster, f.images = c.startup.ownerID(), c.staticFacts().Images
	f.collectDiagnostics, f.collectCrash = c.CollectDiagnostics, c.CollectCrashMetadata
	f.terminate = func(ctx context.Context) error { return c.Terminate(ctx) }
	f.activate = c.startVerification
	t.Cleanup(f.finalize)
	return f, nil
}

func newFinalization(t testing.TB, config FinalizationConfig) (*Finalization, error) {
	for _, bound := range []*time.Duration{&config.CheckTimeout, &config.ArchiveTimeout, &config.CleanupTimeout} {
		if *bound < 0 || *bound > maxFinalizationBound {
			return nil, invalid(errors.New("finalization timeouts must be between zero and ten minutes"))
		}
	}
	if config.CheckTimeout == 0 {
		config.CheckTimeout = defaultCheckTimeout
	}
	if config.ArchiveTimeout == 0 {
		config.ArchiveTimeout = defaultArchiveTimeout
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultCleanupTimeout
	}
	if len(config.Scenario) > 256 || strings.ContainsAny(config.Scenario, "\x00\r\n") {
		return nil, invalid(errors.New("finalization scenario must be one line of at most 256 bytes"))
	}
	if strings.ContainsRune(config.ArchiveDirectory, 0) {
		return nil, invalid(errors.New("finalization archive directory contains a NUL byte"))
	}
	if _, err := normalizeDiagnosticsConfig(config.Diagnostics); err != nil {
		return nil, err
	}
	if _, err := normalizeCrashMetadataConfig(config.CrashMetadata); err != nil {
		return nil, err
	}
	f := &Finalization{t: t, config: config, now: func() time.Time { return time.Now().UTC() }, newID: newExecutionID}
	f.writeFile = func(path string, data []byte) error { return os.WriteFile(path, data, 0o644) }
	f.artifactDir = func() (string, bool) {
		retained := false
		if value := flag.Lookup("test.artifacts"); value != nil {
			retained = value.Value.String() == "true"
		}
		return t.ArtifactDir(), retained
	}
	return f, nil
}

// Check adds a post-test check. Checks run before the archive decision, in
// registration order, while the cluster and every client still run. A check
// that returns an error, or panics, fails the test and triggers the archive;
// a check may also report through the test itself, including with t.Fatal,
// which ends only the check. Adding a check after
// finalization has started fails the test instead.
//
//purpose:observation
func (f *Finalization) Check(name string, check func(context.Context) error) {
	f.add(&f.checks, "Check", name, check)
}

// Cleanup adds a cleanup that runs after the archive and before Terminate,
// in reverse registration order, such as the removal of a caller-owned
// client. Its error, or panic, fails the test; when nothing has failed
// before, it also triggers an archive of what still runs.
//
//purpose:observation,lifetime
func (f *Finalization) Cleanup(name string, cleanup func(context.Context) error) {
	f.add(&f.cleanups, "Cleanup", name, cleanup)
}

func (f *Finalization) add(steps *[]finalizationStep, kind, name string, run func(context.Context) error) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if run == nil {
		f.t.Errorf("ceph finalization: %s %q has no function", kind, name)
		return
	}
	if f.started {
		f.t.Errorf("ceph finalization: %s %q was added after finalization started", kind, name)
		return
	}
	*steps = append(*steps, finalizationStep{name: name, run: run})
}

// finalizationRun is the state of one finalization, which the manifest
// reports.
type finalizationRun struct {
	manifest finalizationManifest
	// directory is where the archive is written; empty until it exists.
	directory string
	// archiveAttempted keeps a second trigger from retrying a failed archive
	// directory, whose creation may have ended the cleanup's goroutine.
	archiveAttempted bool
}

func (f *Finalization) finalize() {
	f.mu.Lock()
	f.started = true
	checks, cleanups, captures := f.checks, f.cleanups, f.captures
	f.mu.Unlock()
	run := &finalizationRun{manifest: f.newManifest()}
	run.manifest.Trigger.TestFailed = f.t.Failed()
	// Deferred, so that the cleanups and Terminate still run when the
	// archive directory's creation ends this goroutine through t.Fatal.
	defer f.finish(run, cleanups)
	ctx, cancel := context.WithTimeout(context.Background(), f.config.CheckTimeout)
	for _, check := range checks {
		outcome := f.runStep(ctx, check)
		if outcome.Error != "" {
			run.manifest.Trigger.CheckFailed = true
			f.t.Errorf("ceph finalization check %s: %s", check.name, outcome.Error)
		}
		run.manifest.Trigger.CheckReportedFailure = run.manifest.Trigger.CheckReportedFailure || outcome.FailedTest
		run.manifest.Checks = append(run.manifest.Checks, outcome)
	}
	cancel()
	f.settleCaptures(run, captures)
	if f.config.Archive && f.t.Failed() {
		f.archive(run, "before-cleanup")
	}
}

func (f *Finalization) finish(run *finalizationRun, cleanups []finalizationStep) {
	defer f.terminateCluster(run)
	ctx, cancel := context.WithTimeout(context.Background(), f.config.CleanupTimeout)
	defer cancel()
	for i := len(cleanups) - 1; i >= 0; i-- {
		outcome := f.runStep(ctx, cleanups[i])
		if outcome.Error != "" {
			run.manifest.Trigger.CleanupFailed = true
			f.t.Errorf("ceph finalization cleanup %s: %s", cleanups[i].name, outcome.Error)
		}
		run.manifest.Trigger.CleanupReportedFailure = run.manifest.Trigger.CleanupReportedFailure || outcome.FailedTest
		run.manifest.Cleanups = append(run.manifest.Cleanups, outcome)
	}
	if f.config.Archive {
		f.snapshotDebugWindows(&run.manifest, "after-cleanup", true)
	}
	if f.config.Archive && !run.archiveAttempted && f.t.Failed() {
		f.archive(run, "after-cleanup")
	}
}

func (f *Finalization) terminateCluster(run *finalizationRun) {
	ctx, cancel := context.WithTimeout(context.Background(), f.config.CleanupTimeout)
	defer cancel()
	outcome := f.runStep(ctx, finalizationStep{name: "terminate", run: f.terminate})
	run.manifest.Termination = &outcome
	if outcome.Error != "" {
		f.t.Errorf("ceph finalization: terminate the cluster: %s", outcome.Error)
	}
	if run.directory == "" {
		return
	}
	// The late outcomes complete the manifest that the archive wrote while
	// the cluster still ran; the snapshot itself is not taken again.
	run.manifest.Final = true
	if err := f.writeManifest(run); err != nil {
		f.t.Logf("ceph failure archive: rewrite the manifest with the cleanup outcomes: %v", err)
	}
}

// runStep runs one check, cleanup or Terminate on its own goroutine and
// records its outcome. A panic, and a t.FailNow, t.Fatal, t.SkipNow or
// runtime.Goexit that ends the step's goroutine instead of returning, become
// the step's error, so that neither can skip the later phases. A step that
// turns a passing test into a failed one through the test is recorded as
// such, apart from its error; a test that had already failed cannot
// attribute a later report to the step.
func (f *Finalization) runStep(ctx context.Context, step finalizationStep) (outcome finalizationOutcome) {
	outcome = finalizationOutcome{Name: boundedName(step.name), StartedAt: f.now()}
	failedBefore := f.t.Failed()
	var err error
	done := make(chan struct{})
	go func() {
		returned := false
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %v", recovered)
			} else if !returned {
				err = errors.New("ended its goroutine without returning (t.FailNow, t.Fatal, t.SkipNow or runtime.Goexit)")
			}
		}()
		err = step.run(ctx)
		returned = true
	}()
	<-done
	outcome.FinishedAt = f.now()
	if err != nil {
		outcome.Error = f.redact(err.Error())
	}
	outcome.FailedTest = !failedBefore && f.t.Failed()
	return outcome
}

// redact masks the configured secrets and the recognized secret patterns of
// CollectDiagnostics in an error that the manifest keeps.
func (f *Finalization) redact(message string) string {
	message, _ = diagnosticBoundText(f.redactor()(message), 64<<10)
	if message == "" {
		message = "?"
	}
	return message
}

// redactor masks the configured secrets and the recognized secret patterns.
func (f *Finalization) redactor() func(string) string {
	values := append(append([]string{}, f.config.Diagnostics.RedactValues...), f.config.CrashMetadata.RedactValues...)
	return diagnosticsRedactor(values)
}

func boundedName(name string) string {
	name, _ = diagnosticBoundText(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '_'
		}
		return r
	}, name), 128)
	return name
}

// archive collects both reports independently, within one bound, and
// writes each one that it got, then the manifest.
func (f *Finalization) archive(run *finalizationRun, phase string) {
	run.archiveAttempted = true
	m := &run.manifest
	m.Phase = phase
	f.snapshotDebugWindows(m, phase, false)
	m.Archive.StartedAt = f.now()
	m.Archive.Timeout = f.config.ArchiveTimeout.String()
	directory, retained, err := f.archiveDirectory(run)
	m.Archive.Retained = retained
	if err != nil {
		m.Archive.Error = f.redact(err.Error())
		f.t.Logf("ceph failure archive: no directory, nothing was written: %v", err)
		return
	}
	run.directory = directory
	m.Archive.ID = filepath.Base(directory)
	ctx, cancel := context.WithTimeout(context.Background(), f.config.ArchiveTimeout)
	defer cancel()
	var diagnostics *DiagnosticsReport
	var crash *CrashMetadataReport
	var collectors sync.WaitGroup
	m.Collectors = []finalizationCollector{{Name: "diagnostics", File: "diagnostics.json"}, {Name: "crash-metadata", File: "crash-metadata.json"}}
	collectors.Go(func() {
		m.Collectors[0].StartedAt = f.now()
		var collectErr error
		diagnostics, collectErr = collectSafely(func() (*DiagnosticsReport, error) {
			return f.collectDiagnostics(ctx, f.config.Diagnostics)
		})
		m.Collectors[0].FinishedAt = f.now()
		m.Collectors[0].record(diagnostics != nil, diagnostics != nil && diagnostics.Complete, collectErr, f.redact)
	})
	collectors.Go(func() {
		m.Collectors[1].StartedAt = f.now()
		var collectErr error
		crash, collectErr = collectSafely(func() (*CrashMetadataReport, error) {
			return f.collectCrash(ctx, f.config.CrashMetadata)
		})
		m.Collectors[1].FinishedAt = f.now()
		m.Collectors[1].record(crash != nil, crash != nil && crash.Complete, collectErr, f.redact)
		if crash != nil {
			m.Collectors[1].CrashOutcomes = map[CrashOutcome]int{}
			for _, container := range crash.Containers {
				m.Collectors[1].CrashOutcomes[container.Outcome]++
			}
		}
	})
	collectors.Wait()
	if diagnostics != nil {
		m.Archive.ClusterClosed = diagnostics.Closed
		m.Files = append(m.Files, f.writeReport(directory, "diagnostics.json", diagnostics))
	}
	if crash != nil {
		m.Archive.ClusterClosed = m.Archive.ClusterClosed || crash.Closed
		m.Files = append(m.Files, f.writeReport(directory, "crash-metadata.json", crash))
	}
	f.mu.Lock()
	verifications := slices.Clone(f.verificationReports)
	f.mu.Unlock()
	for i, report := range verifications {
		m.Files = append(m.Files, f.writeReport(directory, fmt.Sprintf("verification-%d-%s.json", i+1, artifactName(report.Name)), report))
	}
	f.mu.Lock()
	captureReports := slices.Clone(f.captureReports)
	f.mu.Unlock()
	for _, report := range captureReports {
		m.Files = append(m.Files, f.writeReport(directory, captureReportFile(report.sequence, report.name), report.report))
	}
	f.archiveCaptureFiles(ctx, directory, captureReports, m)
	m.Archive.FinishedAt = f.now()
	m.Archive.Complete = true
	for _, collector := range m.Collectors {
		m.Archive.Complete = m.Archive.Complete && collector.Complete && collector.Error == ""
	}
	for _, file := range m.Files {
		m.Archive.Complete = m.Archive.Complete && file.Error == ""
	}
	manifestErr := f.writeManifest(run)
	summary := []string{}
	for _, collector := range m.Collectors {
		state := "complete"
		switch {
		case !collector.Reported:
			state = "no report"
		case !collector.Complete:
			state = "incomplete"
		}
		if collector.Error != "" {
			state += ": " + collector.Error
		}
		summary = append(summary, collector.Name+" "+state)
	}
	for _, file := range m.Files {
		if file.Error != "" {
			summary = append(summary, "write "+file.Name+": "+file.Error)
		}
	}
	if manifestErr != nil {
		summary = append(summary, "write manifest.json: "+manifestErr.Error())
	}
	where := directory
	if !retained {
		where += " (a temporary directory that is removed after the test; run go test with -artifacts to keep it)"
	}
	f.t.Logf("ceph failure archive (%s) in %s: %s", phase, where, strings.Join(summary, "; "))
}

func collectSafely[R any](collect func() (*R, error)) (report *R, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, fmt.Errorf("panic: %v", recovered))
		}
	}()
	return collect()
}

func (f *Finalization) writeReport(directory, name string, report any) finalizationFile {
	file := finalizationFile{Name: name}
	data, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = f.writeFile(filepath.Join(directory, name), append(data, '\n'))
	}
	if err != nil {
		file.Error = f.redact(err.Error())
		return file
	}
	file.Bytes = len(data) + 1
	return file
}

// writeManifest replaces manifest.json through a rename, so that a reader
// sees either the earlier manifest or the complete new one.
func (f *Finalization) writeManifest(run *finalizationRun) error {
	data, err := json.MarshalIndent(run.manifest, "", "  ")
	if err != nil {
		return err
	}
	temporary := filepath.Join(run.directory, ".manifest.json.tmp")
	if err := f.writeFile(temporary, append(data, '\n')); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(run.directory, "manifest.json"))
}

// archiveDirectory creates a new directory for this archive: under
// ArchiveDirectory, in a directory named after the test, or in the test's
// artifact directory, which is already unique to this test execution. The
// random execution ID keeps parallel and repeated executions apart.
func (f *Finalization) archiveDirectory(run *finalizationRun) (string, bool, error) {
	parent, retained := f.config.ArchiveDirectory, true
	if parent == "" {
		parent, retained = f.artifactDir()
	} else {
		parent = filepath.Join(parent, artifactName(f.t.Name()))
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", retained, err
	}
	cluster := f.cluster
	if len(cluster) > 12 {
		cluster = cluster[:12]
	}
	var err error
	for range 4 {
		directory := filepath.Join(parent, "ceph-"+artifactName(cluster)+"-"+run.manifest.Execution.ID)
		if err = os.Mkdir(directory, 0o755); err == nil {
			return directory, retained, nil
		}
		if !errors.Is(err, os.ErrExist) {
			break
		}
		run.manifest.Execution.ID = f.newID()
	}
	return "", retained, err
}

// artifactName keeps letters, digits, dot, dash and underscore, and shortens
// a long name with a hash of the whole name.
func artifactName(name string) string {
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, name)
	clean = strings.Trim(clean, ".")
	if clean == "" {
		clean = "unnamed"
	}
	if len(clean) > 96 {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(name))
		clean = fmt.Sprintf("%s-%016x", clean[:79], hash.Sum64())
	}
	return clean
}

// finalizationManifest is manifest.json. Missing provenance stays listed in
// Missing instead of being guessed.
type finalizationManifest struct {
	SchemaVersion int                   `json:"schema_version"`
	Test          string                `json:"test"`
	Scenario      string                `json:"scenario,omitempty"`
	Cluster       string                `json:"cluster"`
	Execution     finalizationExecution `json:"execution"`
	// Images are the cluster's role images; a daemon started with DaemonOptions.WithImage
	// is not reflected, the diagnostics archive records each container's image.
	Images       ImageSet                     `json:"images"`
	Missing      []string                     `json:"missing_provenance"`
	Phase        string                       `json:"phase,omitempty"`
	Trigger      finalizationTrigger          `json:"trigger"`
	Checks       []finalizationOutcome        `json:"checks"`
	Captures     []finalizationCaptureOutcome `json:"captures,omitempty"`
	Archive      finalizationArchive          `json:"archive"`
	Collectors   []finalizationCollector      `json:"collectors"`
	Files        []finalizationFile           `json:"files"`
	CaptureFiles []finalizationCaptureFile    `json:"capture_files,omitempty"`
	Cleanups     []finalizationOutcome        `json:"cleanups"`
	// DebugWindows are the registered debug level windows, one per
	// registration, each with an archive-phase and a final receipt copy.
	DebugWindows        []finalizationDebugWindow `json:"debug_windows"`
	DebugWindowsOmitted int                       `json:"debug_windows_omitted,omitempty"`
	Termination         *finalizationOutcome      `json:"termination,omitempty"`
	// Final means that the cleanups and Terminate have finished and their
	// outcomes are recorded; the reports remain the earlier snapshot.
	Final bool     `json:"final"`
	Notes []string `json:"notes"`
}

type finalizationExecution struct {
	ID         string    `json:"id"`
	StartedAt  time.Time `json:"started_at"`
	GoVersion  string    `json:"go_version"`
	Platform   string    `json:"platform"`
	Revision   string    `json:"revision,omitempty"`
	Modified   *bool     `json:"modified,omitempty"`
	Repository string    `json:"repository,omitempty"`
	// WorkflowSHA is GITHUB_SHA, the revision of the workflow's event. It is
	// not the revision that was checked out and built: a comment-triggered
	// run of a pull request checks out another revision. Only Revision
	// identifies the source.
	WorkflowSHA string `json:"workflow_sha,omitempty"`
	Workflow    string `json:"workflow,omitempty"`
	Job         string `json:"job,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	RunAttempt  string `json:"run_attempt,omitempty"`
}

type finalizationTrigger struct {
	// TestFailed is the test's state before the checks ran.
	TestFailed bool `json:"test_failed"`
	// CheckFailed and CleanupFailed mean that a step returned an error,
	// panicked or ended its goroutine.
	CheckFailed   bool `json:"check_failed"`
	CleanupFailed bool `json:"cleanup_failed"`
	// CheckReportedFailure and CleanupReportedFailure mean that a step
	// turned the passing test into a failed one through the test itself.
	CheckReportedFailure bool `json:"check_reported_failure"`
	// CaptureFailed means that a capture's settlement returned an error,
	// panicked or ended its goroutine.
	CaptureFailed          bool `json:"capture_failed,omitempty"`
	CleanupReportedFailure bool `json:"cleanup_reported_failure"`
}

type finalizationOutcome struct {
	Name       string    `json:"name"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Error      string    `json:"error,omitempty"`
	// FailedTest means that the test was passing before the step and had
	// failed after it.
	FailedTest bool `json:"failed_test"`
}

type finalizationArchive struct {
	// ID is the archive directory's name; the manifest records no host path.
	ID            string    `json:"id,omitempty"`
	Retained      bool      `json:"retained"`
	Timeout       string    `json:"timeout,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	ClusterClosed bool      `json:"cluster_closed"`
	Complete      bool      `json:"complete"`
	Error         string    `json:"error,omitempty"`
}

type finalizationCollector struct {
	Name          string               `json:"name"`
	File          string               `json:"file"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    time.Time            `json:"finished_at"`
	Reported      bool                 `json:"reported"`
	Complete      bool                 `json:"complete"`
	Error         string               `json:"error,omitempty"`
	CrashOutcomes map[CrashOutcome]int `json:"crash_outcomes,omitempty"`
}

func (c *finalizationCollector) record(reported, complete bool, err error, redact func(string) string) {
	c.Reported, c.Complete = reported, complete
	if err != nil {
		c.Error = redact(err.Error())
	}
}

type finalizationFile struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
	Error string `json:"error,omitempty"`
}

var finalizationNotes = []string{
	"The reports are a snapshot taken during finalization: observations are not atomic and log tails are not continuous history.",
	"No crash records is not proof that a daemon never crashed; a removed container takes its records along.",
	"Cleanup and termination outcomes were recorded after the snapshot; nothing was collected after termination.",
	"Nothing is archived after a forced process exit or a lost runner.",
}

func (f *Finalization) newManifest() finalizationManifest {
	m := finalizationManifest{SchemaVersion: 1, Test: f.t.Name(), Scenario: f.config.Scenario, Cluster: f.cluster,
		Images: f.images, Checks: []finalizationOutcome{}, Collectors: []finalizationCollector{},
		Files: []finalizationFile{}, Cleanups: []finalizationOutcome{}, DebugWindows: []finalizationDebugWindow{}, Missing: []string{},
		Notes: finalizationNotes}
	m.Execution = finalizationExecution{ID: f.newID(), StartedAt: f.now(), GoVersion: runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				m.Execution.Revision = setting.Value
			case "vcs.modified":
				modified := setting.Value == "true"
				m.Execution.Modified = &modified
			}
		}
	}
	// Only these public identifiers of a GitHub Actions run are read; no
	// other environment variable is.
	for name, field := range map[string]*string{"GITHUB_REPOSITORY": &m.Execution.Repository, "GITHUB_SHA": &m.Execution.WorkflowSHA,
		"GITHUB_WORKFLOW": &m.Execution.Workflow, "GITHUB_JOB": &m.Execution.Job, "GITHUB_RUN_ID": &m.Execution.RunID,
		"GITHUB_RUN_ATTEMPT": &m.Execution.RunAttempt} {
		*field = boundedName(os.Getenv(name))
	}
	// The workflow's revision does not stand in for the source revision.
	if m.Execution.Revision == "" {
		m.Missing = append(m.Missing, "source revision")
	}
	if m.Execution.RunID == "" {
		m.Missing = append(m.Missing, "CI run identity")
	}
	if m.Scenario == "" {
		m.Missing = append(m.Missing, "scenario")
	}
	// Image IDs are in the diagnostics' container-inspect artifacts; the
	// bug compatibility selection is a build property that the library
	// does not see.
	m.Missing = append(m.Missing, "bug compatibility selection")
	return m
}

func newExecutionID() string {
	var id [8]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}
