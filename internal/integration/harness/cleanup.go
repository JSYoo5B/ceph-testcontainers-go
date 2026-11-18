package harness

import (
	"context"
	"testing"
	"time"
)

// cleanupDiagnosticsTimeout bounds all failure diagnostics of one cleanup.
const cleanupDiagnosticsTimeout = time.Minute

// ReleaseAfterDiagnostics is the body of a fixture cleanup. When t failed it
// runs diagnose (state, logs and native reads of the failed fixture) under a
// bounded context of its own, and then release (Terminate, Remove, Restore)
// under a fresh context of releaseTimeout. A diagnostic read that Docker
// stalls until its deadline therefore cannot hand release an expired context
// and leave the fixture behind. release also runs if diagnose calls Fatal or
// Goexit, and reports its own errors.
func ReleaseAfterDiagnostics(t testing.TB, releaseTimeout time.Duration, diagnose, release func(context.Context)) {
	t.Helper()
	releaseAfterDiagnostics(t.Failed(), cleanupDiagnosticsTimeout, releaseTimeout, diagnose, release)
}

func releaseAfterDiagnostics(failed bool, diagnosticsTimeout, releaseTimeout time.Duration, diagnose, release func(context.Context)) {
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		release(ctx)
	}()
	if failed && diagnose != nil {
		ctx, cancel := context.WithTimeout(context.Background(), diagnosticsTimeout)
		defer cancel()
		diagnose(ctx)
	}
}
