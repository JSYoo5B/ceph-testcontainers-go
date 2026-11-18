package diagnostics

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	mobycl "github.com/moby/moby/client"
)

// completionCrashDocker completes an otherwise successful operation only after
// the test has ended its context. No Docker engine or background reader is used.
type completionCrashDocker struct {
	*fakeCrashDocker
	finish    func(context.Context)
	reinspect bool
}

func (d *completionCrashDocker) ContainerInspect(ctx context.Context, id string, opts mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	result, err := d.fakeCrashDocker.ContainerInspect(ctx, id, opts)
	if d.reinspect && d.containers[id].inspections == 2 {
		d.finish(ctx)
	}
	return result, err
}

func (d *completionCrashDocker) CopyFromContainer(ctx context.Context, id string, opts mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	result, err := d.fakeCrashDocker.CopyFromContainer(ctx, id, opts)
	if err == nil {
		result.Content = &completionCrashReader{ReadCloser: result.Content, remaining: len(d.containers[id].archive), finish: func() { d.finish(ctx) }}
	}
	return result, err
}

type completionCrashReader struct {
	io.ReadCloser
	remaining int
	finish    func()
}

func (r *completionCrashReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.remaining -= n
	if r.remaining == 0 {
		r.finish()
	}
	return n, err
}

func TestCrashMetadataLateCompletion(t *testing.T) {
	for _, reinspect := range []bool{false, true} {
		for _, mode := range []string{"parent-cancel", "operation-timeout", "total-timeout"} {
			name := "archive"
			if reinspect {
				name = "reinspect"
			}
			name += "/" + mode
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if mode == "total-timeout" {
					var end context.CancelFunc
					ctx, end = context.WithTimeout(ctx, 100*time.Millisecond)
					defer end()
				}
				config, err := normalizeCrashMetadataConfig(CrashMetadataConfig{})
				if err != nil {
					t.Fatal(err)
				}
				finish := func(context.Context) { cancel() }
				wantErr := context.Canceled
				if mode != "parent-cancel" {
					if mode == "operation-timeout" {
						config.OperationTimeout = 100 * time.Millisecond
					}
					finish = func(opCtx context.Context) { <-opCtx.Done() }
					wantErr = context.DeadlineExceeded
				}
				docker := &completionCrashDocker{fakeCrashDocker: &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
					"mgr": {noCrashDir: reinspect, archive: crashArchive(t, crashEntry{name: "crash/r/meta", body: `{"crash_id":"r"}`})},
				}}, finish: finish, reinspect: reinspect}
				report, err := collectCrashMetadata(ctx, config, crashSnapshot(crashContainer("mgr", "a", "mgr")), docker, &CrashMetadataReport{})
				if !errors.Is(err, wantErr) || report.Complete {
					t.Fatalf("late completion: report=%+v error=%v", report, err)
				}
				wantOutcome := CrashArchiveIncomplete
				if reinspect {
					wantOutcome = CrashReadFailed
				}
				if report.Containers[0].Outcome != wantOutcome {
					t.Fatalf("outcome=%s", report.Containers[0].Outcome)
				}
				if !reinspect && len(report.Containers[0].Records) != 1 {
					t.Fatal("lost accepted record")
				}
				if mode == "operation-timeout" && ctx.Err() != nil {
					t.Fatal("operation timeout ended parent context")
				}
			})
		}
	}
}

func TestCrashMetadataCanceledEmptyCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := collectCrashes(t, ctx, CrashMetadataConfig{}, &fakeCrashDocker{}, crashSnapshot())
	if !errors.Is(err, context.Canceled) || report.Complete {
		t.Fatalf("report=%+v error=%v", report, err)
	}
}

// Unavailable and rejected entries affect coverage without making transport fail.
func TestCrashMetadataIncompleteWithoutError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		container *fakeCrashContainer
		outcome   CrashOutcome
	}{
		{"unavailable", &fakeCrashContainer{removed: true}, CrashContainerUnavailable},
		{"all rejected", &fakeCrashContainer{archive: crashArchive(t, crashEntry{name: "crash/r/meta", body: "invalid"})}, CrashEntriesRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, err := collectCrashes(t, t.Context(), CrashMetadataConfig{}, &fakeCrashDocker{containers: map[string]*fakeCrashContainer{"mgr": tc.container}}, crashSnapshot(crashContainer("mgr", "a", "mgr")))
			if err != nil || report.Complete || report.Containers[0].Outcome != tc.outcome {
				t.Fatalf("report=%+v error=%v", report, err)
			}
		})
	}
}
