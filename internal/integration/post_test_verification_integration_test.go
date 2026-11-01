//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestPostTestVerification crashes the MGR and an OSD for real after the
// activation. It shows that a permitted crash and log line do not hide the
// other daemon's, that a required but absent crash fails, that crashes
// present at a later activation are not new, that a removed container is a
// gap, and that the failed verification archives before Terminate. The
// finalization runs through a recorder of the test, so that the expected
// verification failures are asserted instead of failing this test.
func TestPostTestVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2).WithPoolDefaults(1, 1)))...)
	recorder := &finalizationRecorder{TB: t}
	var finalization *finalize.Finalization
	archives := t.TempDir()
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(recorder, cluster, finalize.Config{Archive: true, ArchiveDirectory: archives, Scenario: "post-test-verification"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
		// Whatever ends the test body, the recorded finalization runs.
		t.Cleanup(recorder.finalize)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	policy, err := finalization.Verify(ctx, finalize.VerificationConfig{
		Name:        "crash-policy",
		MaxFindings: 4096,
		Logs: []finalize.LogExpectation{
			{Name: "osd0-signal", Required: true, Daemon: "osd.0", Event: finalize.LogFatalSignal, Pattern: `Caught signal \(Segmentation fault\)`},
		},
		Crashes: []finalize.CrashExpectation{
			{Name: "osd0-crash", Required: true, MaxCount: 1, Daemon: "osd.0", Process: "ceph-osd"},
			{Name: "osd1-crash", Required: true, Daemon: "osd.1", Process: "ceph-osd"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Before any crash, the crash check finds no new crash in any source.
	report, err := policy.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logVerification(t, "before the crashes", report)
	if report.Crashes.Unexpected != 0 || report.Crashes.Permitted != 0 || len(report.Crashes.Gaps) != 0 || len(report.Sources) != 4 {
		t.Fatalf("crash check before the crashes: %+v; sources %+v", report.Crashes, report.Sources)
	}
	for _, source := range report.Sources {
		if len(source.BaselineCrashes) != 0 || source.ContainerID == "" || source.StartedAt == "" {
			t.Fatalf("source at activation: %+v", source)
		}
	}
	// osd1-crash is required but never happens.
	if result := expectation(t, report.Crashes, "osd1-crash"); result.Satisfied || report.Crashes.Outcome != finalize.VerificationUnexpected {
		t.Fatalf("an absent required crash passed: %+v", report.Crashes)
	}

	crashDaemon(t, ctx, clusteraccess.InitialManager(t, cluster))
	crashDaemon(t, ctx, clusteraccess.OSDs(t, cluster)[0].Container)

	report, err = policy.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logVerification(t, "after the crashes", report)
	osd0 := expectation(t, report.Crashes, "osd0-crash")
	if report.Outcome != finalize.VerificationUnexpected || report.Crashes.Permitted != 1 || report.Crashes.Unexpected != 1 || !osd0.Satisfied || osd0.Matched != 1 {
		t.Fatalf("crash check after the crashes: %+v", report.Crashes)
	}
	for _, finding := range report.Crashes.Findings {
		switch {
		case finding.Source == "osd.0" && finding.Expectation == "osd0-crash" && finding.CrashID != "":
		case finding.Source == "mgr.a" && finding.Expectation == "" && finding.CrashID != "":
		default:
			t.Fatalf("crash finding %+v", finding)
		}
	}
	// The narrow log expectation permits osd.0's signal; the MGR's signal
	// stays unexpected. A log driver that verification does not cover is a
	// gap instead, which must not pass either.
	logGaps := map[string]bool{}
	for _, gap := range report.Logs.Gaps {
		logGaps[gap.Source] = true
	}
	if logGaps["osd.0"] || logGaps["mgr.a"] {
		if report.Logs.Outcome == finalize.VerificationClean {
			t.Fatalf("log gaps passed: %+v", report.Logs.Gaps)
		}
		t.Logf("the log sources are not covered here: %+v", report.Logs.Gaps)
	} else {
		signal := expectation(t, report.Logs, "osd0-signal")
		if !signal.Satisfied || report.Logs.Outcome != finalize.VerificationUnexpected || !slices.ContainsFunc(report.Logs.Findings, func(finding finalize.VerificationFinding) bool {
			return finding.Source == "mgr.a" && finding.Event == string(finalize.LogFatalSignal) && finding.Expectation == ""
		}) {
			t.Fatalf("log check after the crashes: %+v", report.Logs)
		}
	}

	// A later activation records both crashes in its baseline: they are not
	// new to it.
	later, err := finalization.Verify(ctx, finalize.VerificationConfig{Name: "after-crashes"})
	if err != nil {
		t.Fatal(err)
	}
	report, err = later.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logVerification(t, "later activation", report)
	baseline := map[string]int{}
	for _, source := range report.Sources {
		baseline[source.Source] = len(source.BaselineCrashes)
	}
	if report.Crashes.Outcome != finalize.VerificationClean || report.Crashes.Unexpected != 0 || baseline["mgr.a"] != 1 || baseline["osd.0"] != 1 || baseline["osd.1"] != 0 {
		t.Fatalf("crash check of the later activation: %+v; baseline %v", report.Crashes, baseline)
	}

	// An OSD container removed behind the cluster's back is a gap of both
	// checks.
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ContainerRemove(ctx, clusteraccess.OSDs(t, cluster)[1].GetContainerID(), mobycl.ContainerRemoveOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	report, err = later.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logVerification(t, "after the removal", report)
	for _, check := range []finalize.VerificationCheck{report.Logs, report.Crashes} {
		if check.Outcome == finalize.VerificationClean || !slices.ContainsFunc(check.Gaps, func(gap finalize.VerificationGap) bool {
			return gap.Source == "osd.1" && strings.Contains(gap.Reason, "removed")
		}) {
			t.Fatalf("a removed container passed: %+v", check)
		}
	}

	// The finalization fails both verifications, archives them before the
	// cleanups and still terminates the cluster.
	recorder.finalize()
	failures := recorder.failures()
	// The later verification's crash check is incomplete; its log check may
	// also hold the MON's OSD_DOWN warnings for the removal, which nothing
	// permits.
	for _, want := range []string{"verification crash-policy: post-test verification crash-policy: unexpected", "crashes incomplete (0 unexpected, 0 permitted, 1 gaps"} {
		if !slices.ContainsFunc(failures, func(failure string) bool { return strings.Contains(failure, want) }) {
			t.Fatalf("finalization failures %q lack %q", failures, want)
		}
	}
	directories, err := filepath.Glob(filepath.Join(archives, "*", "ceph-*"))
	if err != nil || len(directories) != 1 {
		t.Fatalf("archives %v %v", directories, err)
	}
	data, err := os.ReadFile(filepath.Join(directories[0], "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Phase   string `json:"phase"`
		Trigger struct {
			CheckFailed bool `json:"check_failed"`
		} `json:"trigger"`
		Files []struct {
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"files"`
		Termination *struct {
			Error string `json:"error"`
		} `json:"termination"`
		Final bool `json:"final"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, file := range manifest.Files {
		if file.Error == "" {
			files = append(files, file.Name)
		}
	}
	if manifest.Phase != "before-cleanup" || !manifest.Trigger.CheckFailed || !manifest.Final || manifest.Termination == nil || manifest.Termination.Error != "" ||
		!slices.Contains(files, "verification-1-crash-policy.json") || !slices.Contains(files, "verification-2-after-crashes.json") {
		t.Fatalf("manifest %s", data)
	}
	data, err = os.ReadFile(filepath.Join(directories[0], "verification-1-crash-policy.json"))
	if err != nil {
		t.Fatal(err)
	}
	var archived finalize.VerificationReport
	if err := json.Unmarshal(data, &archived); err != nil || archived.Outcome != finalize.VerificationUnexpected || archived.Crashes.Unexpected != 1 {
		t.Fatalf("archived report %v: %s", err, data)
	}
	t.Logf("finalization: %d failures recorded, archive files %v, termination recorded without error", len(failures), files)
}

func expectation(t *testing.T, check finalize.VerificationCheck, name string) finalize.ExpectationResult {
	t.Helper()
	for _, result := range check.Expectations {
		if result.Name == name {
			return result
		}
	}
	t.Fatalf("no expectation %s in %+v", name, check.Expectations)
	return finalize.ExpectationResult{}
}

// logVerification records the evidence of one evaluation.
func logVerification(t *testing.T, label string, report *finalize.VerificationReport) {
	t.Helper()
	var sources []string
	for _, source := range report.Sources {
		sources = append(sources, fmt.Sprintf("%s(%s,%s,%d bytes,%d baseline crashes)", source.Source, source.State, source.LogDriver, source.LogBytes, len(source.BaselineCrashes)))
	}
	t.Logf("%s: %s, window %s to %s; sources %s", label, report.Outcome, report.Window.StartedAt.Format(time.RFC3339Nano), report.Window.EndedAt.Format(time.RFC3339Nano), strings.Join(sources, " "))
	for _, check := range []struct {
		name  string
		check finalize.VerificationCheck
	}{{"logs", report.Logs}, {"crashes", report.Crashes}} {
		t.Logf("%s %s: %d unexpected, %d permitted, %d gaps, expectations %+v", check.name, check.check.Outcome, check.check.Unexpected, check.check.Permitted, len(check.check.Gaps), check.check.Expectations)
		for i, finding := range check.check.Findings {
			if i == 8 {
				t.Logf("  ... %d more findings", len(check.check.Findings)-i)
				break
			}
			t.Logf("  %s %s %s [%s] %.160s", finding.Time, finding.Source, finding.Event, finding.Expectation, finding.Text)
		}
		for _, gap := range check.check.Gaps {
			t.Logf("  gap %s: %s", gap.Source, gap.Reason)
		}
	}
}

// finalizationRecorder is the test for a finalization whose failures the
// test expects: it records them instead of failing the test, and runs the
// finalization once, when the test calls finalize or at the latest in the
// test's cleanup.
type finalizationRecorder struct {
	testing.TB
	mu      sync.Mutex
	failed  bool
	errors  []string
	cleanup func()
	once    sync.Once
}

func (r *finalizationRecorder) Cleanup(cleanup func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanup = cleanup
}

func (r *finalizationRecorder) finalize() {
	r.once.Do(func() {
		r.mu.Lock()
		cleanup := r.cleanup
		r.mu.Unlock()
		if cleanup != nil {
			cleanup()
		}
	})
}

func (r *finalizationRecorder) Failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed
}

func (r *finalizationRecorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	r.failed = true
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	r.TB.Logf("recorded finalization failure: "+format, args...)
}

func (r *finalizationRecorder) Error(args ...any) { r.Errorf("%s", fmt.Sprint(args...)) }
func (r *finalizationRecorder) Fail()             { r.Errorf("failed") }
func (r *finalizationRecorder) FailNow() {
	r.Fail()
	runtime.Goexit()
}
func (r *finalizationRecorder) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}
func (r *finalizationRecorder) Fatal(args ...any) {
	r.Error(args...)
	runtime.Goexit()
}

func (r *finalizationRecorder) failures() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errors)
}
