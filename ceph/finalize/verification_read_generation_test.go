package finalize

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	mobycl "github.com/moby/moby/client"
)

// generationBoundaryDocker changes one fake generation at an evidence boundary,
// including before the crash collector's own inspection sees a stable new one.
// It proves orchestration behavior, not native restart timing.
type generationBoundaryDocker struct {
	*fakeVerifyDocker
	boundary  string
	inspected bool
	logsRead  bool
	fired     bool
}

func (d *generationBoundaryDocker) restart() {
	d.fired = true
	c := d.containers["osd0"]
	c.startedAt = verifyStart.Add(2 * time.Minute).Format(time.RFC3339Nano)
	c.restarts++
}
func (d *generationBoundaryDocker) ContainerInspect(ctx context.Context, id string, opts mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if id == "osd0" && !d.fired {
		if d.boundary == "baseline-crash" && d.inspected || d.boundary == "after-log-before-confirmation" && d.logsRead {
			d.restart()
		}
		d.inspected = true
	}
	return d.fakeVerifyDocker.ContainerInspect(ctx, id, opts)
}
func (d *generationBoundaryDocker) ContainerLogs(ctx context.Context, id string, opts mobycl.ContainerLogsOptions) (mobycl.ContainerLogsResult, error) {
	if id == "osd0" && d.boundary == "before-log" && !d.fired {
		d.restart()
	}
	r, err := d.fakeVerifyDocker.ContainerLogs(ctx, id, opts)
	if err != nil {
		return r, err
	}
	return &generationReadBoundary{ReadCloser: r, after: func() {
		d.logsRead = true
		if d.boundary == "during-log" && !d.fired {
			d.restart()
		}
	}}, nil
}

// generationReadBoundary releases its transition once the complete stream was
// consumed, before the verifier can inspect the source again.
type generationReadBoundary struct {
	io.ReadCloser
	after func()
}

func (r *generationReadBoundary) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF && r.after != nil {
		r.after()
		r.after = nil
	}
	return n, err
}

func TestVerificationGenerationReadBoundaries(t *testing.T) {
	for _, boundary := range []string{"unchanged", "before-evaluation", "baseline-crash", "before-log", "during-log", "after-log-before-confirmation"} {
		t.Run(boundary, func(t *testing.T) {
			fx := newVerifyFixture()
			fx.snapshot.containers = []diagnostics.Target{crashContainer("osd", "0", "osd0")}
			fx.config = VerificationConfig{ArchiveLogs: true, Logs: []LogExpectation{{Name: "warning", Event: LogClusterWarning, Pattern: "boundary-positive", Required: true}}, Crashes: []CrashExpectation{{Name: "crash", Process: "ceph-osd", Required: true}}}
			v, err := newVerification(fx.config)
			if err != nil {
				t.Fatal(err)
			}
			fx.wire(v)
			d := &generationBoundaryDocker{fakeVerifyDocker: fx.docker}
			if boundary == "baseline-crash" {
				d.boundary = boundary
			}
			v.docker = func(context.Context) (verificationDocker, func(), error) { return d, func() {}, nil }
			if err := v.activate(t.Context()); err != nil {
				t.Fatal(err)
			}
			d.boundary = boundary
			if boundary == "before-evaluation" {
				d.restart()
			}
			fx.docker.clock = verifyStart.Add(10 * time.Minute)
			fx.line("osd0", time.Minute, daemonLogLine("cluster", "WRN", "boundary-positive"))
			fx.docker.containers["osd0"].crashes = append(fx.docker.containers["osd0"].crashes, "boundary-new-crash")
			report := fx.evaluate(t, v)
			if boundary == "unchanged" {
				if report.Outcome != VerificationClean {
					t.Fatalf("unchanged control: %+v", report)
				}
				return
			}
			if !d.fired {
				t.Fatal("read boundary was not reached")
			}
			if report.Outcome == VerificationClean || len(report.Crashes.Gaps) == 0 {
				t.Fatalf("generation transition reported complete crash coverage: %+v", report)
			}
			if boundary != "after-log-before-confirmation" && len(report.Logs.Gaps) == 0 {
				t.Fatalf("generation transition reported complete log coverage: %+v", report)
			}
			if boundary == "baseline-crash" && v.baseline.targets[0].crashes != nil {
				t.Fatal("later generation's crash list became the activation baseline")
			}
			if boundary != "baseline-crash" && len(report.Crashes.Findings) == 0 {
				t.Fatal("positive crash observation was discarded instead of retaining it with a gap")
			}
			if boundary == "before-log" || boundary == "during-log" {
				if len(report.Logs.Findings) == 0 || !strings.Contains(report.Logs.Findings[0].Text, "boundary-positive") {
					t.Fatal("positive log observation was discarded")
				}
			}
		})
	}
}

// The collector starts only after the new generation is stable. Its own
// inspection therefore cannot detect the difference from activation for us.
func TestVerificationStableNewGenerationBeforeCrashCollection(t *testing.T) {
	fx := newVerifyFixture()
	fx.snapshot.containers = []diagnostics.Target{crashContainer("osd", "0", "osd0")}
	fx.config.Crashes = []CrashExpectation{{Name: "crash", Process: "ceph-osd", Required: true}}
	v := fx.activate(t)
	o := &verificationObservation{current: &fx.snapshot}
	w := verificationWindow{config: v.config, baseline: v.baseline, docker: fx.docker, o: o, redact: evidence.Redactor(v.baseline.secrets)}
	w.close(t.Context())
	generation := w.observeSource(t.Context(), v.baseline.targets[0])
	if generation == nil || len(o.logGaps) != 0 {
		t.Fatal("stable log control failed")
	}
	ctr := fx.docker.containers["osd0"]
	ctr.startedAt = verifyStart.Add(2 * time.Minute).Format(time.RFC3339Nano)
	ctr.restarts++
	ctr.crashes = append(ctr.crashes, "after-log-new-crash")
	w.observeCrashes(t.Context(), []*verificationGeneration{generation})
	report := v.evaluate(o)
	if report.Logs.Outcome != VerificationClean || report.Crashes.Outcome == VerificationClean || len(report.Crashes.Gaps) == 0 || len(report.Crashes.Findings) != 1 {
		t.Fatalf("stable newer crash read claimed full activation coverage or lost positive evidence: %+v", report)
	}
}
