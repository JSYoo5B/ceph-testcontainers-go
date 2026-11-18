package finalize

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

func TestFinalizationStepBudgetWaitsForReturnAndRecordsExpiry(t *testing.T) {
	for _, cooperative := range []bool{true, false} {
		name := "nil after expiry"
		if cooperative {
			name = "cooperative error"
		}
		t.Run(name, func(t *testing.T) {
			fx := newFinalizationFixture(t, Config{})
			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan finalizationOutcome, 1)
			go func() {
				done <- fx.runStep(ctx, finalizationStep{name: "controlled", run: func(ctx context.Context) error {
					close(entered)
					<-release
					if cooperative {
						return ctx.Err()
					}
					return nil
				}})
			}()
			<-entered
			<-ctx.Done()
			select {
			case outcome := <-done:
				close(release)
				t.Fatalf("callback abandoned before release: %+v", outcome)
			default:
			}
			close(release)
			outcome := <-done
			if !strings.Contains(outcome.Error, context.DeadlineExceeded.Error()) {
				t.Fatalf("late completion lost expiry: %+v", outcome)
			}
		})
	}
	fx := newFinalizationFixture(t, Config{})
	if got := fx.runStep(t.Context(), finalizationStep{name: "normal", run: func(context.Context) error { return nil }}); got.Error != "" {
		t.Fatalf("normal completion: %+v", got)
	}
}

func TestFinalizationArchiveBudgetIncludesLateWrites(t *testing.T) {
	for _, filename := range []string{"diagnostics.json", ".manifest.json.tmp"} {
		t.Run(filename, func(t *testing.T) {
			fx := newFinalizationFixture(t, Config{Archive: true, ArchiveTimeout: 10 * time.Millisecond})
			var archiveCtx context.Context
			collect := fx.collectDiagnostics
			fx.collectDiagnostics = func(ctx context.Context, config diagnostics.Config) (*diagnostics.Report, error) {
				archiveCtx = ctx
				return collect(ctx, config)
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			write := fx.writeFile
			delayed := false
			fx.writeFile = func(path string, data []byte) error {
				if filepath.Base(path) == filename && !delayed {
					delayed = true
					close(entered)
					<-release
				}
				return write(path, data)
			}
			run := &finalizationRun{manifest: fx.newManifest()}
			go func() { defer close(done); fx.archive(run, "before-cleanup") }()
			<-entered
			<-archiveCtx.Done()
			select {
			case <-done:
				close(release)
				t.Fatal("archive abandoned the active writer")
			default:
			}
			close(release)
			<-done
			m := readManifest(t, run.directory)
			if m.Archive.Complete || !strings.Contains(m.Archive.Error, context.DeadlineExceeded.Error()) || len(m.Files) != 2 {
				t.Fatalf("late write lost partial evidence or expiry: %+v", m)
			}
		})
	}
}

func TestFinalizationArchiveBudgetIncludesPreparation(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true, ArchiveTimeout: time.Millisecond})
	fx.clusterSecrets = func(ctx context.Context) []string {
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Errorf("lookup: %v", ctx.Err())
		}
		return nil
	}
	run := &finalizationRun{manifest: fx.newManifest()}
	fx.archive(run, "before-cleanup")
	m := readManifest(t, run.directory)
	if m.Archive.Complete || !strings.Contains(m.Archive.Error, context.DeadlineExceeded.Error()) {
		t.Fatalf("preparation escaped archive budget: %+v", m.Archive)
	}
}
