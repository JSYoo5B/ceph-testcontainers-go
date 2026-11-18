package harness

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// A diagnostic read that Docker stalls returns only when its context
// expires. The release that follows must still get a live context with its
// whole budget.
func TestReleaseAfterDiagnosticsGivesReleaseAFreshContext(t *testing.T) {
	const releaseTimeout = time.Minute
	stalledRead := func(ctx context.Context) { <-ctx.Done() }
	var releaseErr error
	var releaseBudget time.Duration
	released := false
	releaseAfterDiagnostics(true, 20*time.Millisecond, releaseTimeout, stalledRead, func(ctx context.Context) {
		released = true
		releaseErr = ctx.Err()
		if deadline, ok := ctx.Deadline(); ok {
			releaseBudget = time.Until(deadline)
		}
	})
	if !released {
		t.Fatal("release did not run after the stalled diagnostics")
	}
	if releaseErr != nil {
		t.Fatalf("release got a context the stalled diagnostics expired: %v", releaseErr)
	}
	if releaseBudget < releaseTimeout-10*time.Second {
		t.Fatalf("release got %s of its %s budget", releaseBudget, releaseTimeout)
	}
}

func TestReleaseAfterDiagnosticsSkipsDiagnosticsOfAPassingTest(t *testing.T) {
	diagnosed, released := false, false
	releaseAfterDiagnostics(false, time.Minute, time.Minute,
		func(context.Context) { diagnosed = true },
		func(context.Context) { released = true })
	if diagnosed || !released {
		t.Fatalf("passing test: diagnosed=%t released=%t, want only the release", diagnosed, released)
	}
}

// Fatal uses Goexit; its unwinding must still release the owned fixture.
func TestReleaseAfterDiagnosticsSurvivesDiagnosticGoexit(t *testing.T) {
	done := make(chan struct{})
	released := make(chan error, 1)
	go func() {
		defer close(done)
		releaseAfterDiagnostics(true, time.Minute, time.Minute,
			func(context.Context) { runtime.Goexit() },
			func(ctx context.Context) { released <- ctx.Err() })
	}()
	<-done
	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("release context: %v", err)
		}
	default:
		t.Fatal("diagnostic Goexit skipped release")
	}
}

// A held topology lock is modeled without Docker or a leaked goroutine. The
// read may return only through the provided deadline while the lock is held.
func TestDiagnosticInventoryContentionKeepsReleaseBudget(t *testing.T) {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	defer func() { <-lock }()
	read := func(ctx context.Context) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			return 0, errors.New("inventory lost diagnostic deadline")
		}
		select {
		case lock <- struct{}{}:
			<-lock
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	var diagnosticErr, releaseErr error
	var releaseDeadline bool
	releaseAfterDiagnostics(true, 20*time.Millisecond, time.Minute, func(ctx context.Context) {
		result := diagnosticRead(t, ctx, "contended topology", read)
		diagnosticErr = ctx.Err()
		if result != 0 {
			t.Error("contended inventory unexpectedly acquired lock")
		}
	}, func(ctx context.Context) {
		releaseErr = ctx.Err()
		_, releaseDeadline = ctx.Deadline()
	})
	if !errors.Is(diagnosticErr, context.DeadlineExceeded) || releaseErr != nil || !releaseDeadline {
		t.Fatalf("diagnostic=%v release=%v bounded=%t", diagnosticErr, releaseErr, releaseDeadline)
	}
}

// canceledCleanupTB models testing cancelling Context before Cleanup runs.
//
//testdouble:behavioral-model
type canceledCleanupTB struct {
	testing.TB
	ctx context.Context
}

func (tb canceledCleanupTB) Context() context.Context { return tb.ctx }
func (tb canceledCleanupTB) Failed() bool             { return true }

func TestReleaseAfterDiagnosticsDetachesCanceledTestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tb := canceledCleanupTB{TB: t, ctx: ctx}
	calls := 0
	check := func(ctx context.Context) {
		calls++
		if ctx.Err() != nil {
			t.Errorf("cleanup inherited test cancellation: %v", ctx.Err())
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("cleanup context has no deadline")
		}
	}
	ReleaseAfterDiagnostics(tb, time.Minute, check, check)
	if calls != 2 {
		t.Fatalf("cleanup callbacks=%d, want 2", calls)
	}
}
