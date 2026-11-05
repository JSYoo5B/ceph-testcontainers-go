package consumerevidence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

// FinishBounds bounds the end of a fixture: the diagnostics collection, the
// saving of its report and Terminate, each with a fresh context of its own.
type FinishBounds struct {
	Collect, Save, Terminate time.Duration
	// Diagnostics configures the collection; its own Timeout should stay
	// below Collect.
	Diagnostics diagnostics.Config
}

// Finish ends a cluster that a fixture process owns without a testing.TB, the
// way finalize.TerminateOnCleanup ends one for a test. With a failure signal
// (a failure the driving test reported, a driver that went away, an
// interrupt), it collects one bounded diagnostics report and saves it, even a
// partial one returned with an error; then it terminates the cluster. A
// passing run collects nothing. The returned error joins the failure signal
// with any collection, saving and Terminate error, so neither a complete
// report nor a clean Terminate hides the failure, and a failed Terminate fails
// a passing run.
func Finish(cluster *ceph.Container, signal error, save func(context.Context, *diagnostics.Report) error, bounds FinishBounds) error {
	return finish(signal, func(ctx context.Context) (*diagnostics.Report, error) {
		return diagnostics.Collect(ctx, cluster, bounds.Diagnostics)
	}, save, func(ctx context.Context) error { return cluster.Terminate(ctx) }, bounds)
}

// finish runs the steps of Finish in order, each from a fresh bounded
// context.
func finish(signal error, collect func(context.Context) (*diagnostics.Report, error), save func(context.Context, *diagnostics.Report) error,
	terminate func(context.Context) error, bounds FinishBounds) error {
	errs := []error{signal}
	if signal != nil {
		ctx, cancel := context.WithTimeout(context.Background(), bounds.Collect)
		report, err := collect(ctx)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("diagnostics: %w", err))
		}
		if report != nil {
			ctx, cancel := context.WithTimeout(context.Background(), bounds.Save)
			if err := save(ctx, report); err != nil {
				errs = append(errs, fmt.Errorf("save diagnostics: %w", err))
			}
			cancel()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), bounds.Terminate)
	defer cancel()
	if err := terminate(ctx); err != nil {
		errs = append(errs, fmt.Errorf("terminate: %w", err))
	}
	return errors.Join(errs...)
}
