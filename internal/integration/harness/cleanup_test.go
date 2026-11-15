package harness

import (
	"context"
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
