package mirror

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
)

// waitCephFSRemovalCompletion is the polling loop shared by the directory
// release and peer drain waits. The status type, its completion flag and the
// per-removal observation stay with the caller: complete reports the flag of a
// status and clearComplete resets it on every non-success return.
//
// An observation that was not admitted (it never reached the native query,
// for example because a lock wait expired) only fills the report until the
// first admitted one arrives; a later admitted report replaces it even if it
// looks initial. The last native query cause is retained across attempts
// (multicluster.RetainWaitQueryCause), a context error is joined after it, and
// a permanent guard error stops the wait at once without that join. The
// returned report is the last one with its completion flag cleared except when
// the final observation is both error-free and complete.
func waitCephFSRemovalCompletion[S any](ctx context.Context, interval time.Duration, observe func(context.Context) (S, bool, error), complete func(S) bool, clearComplete func(*S)) (S, error) {
	var last S
	var lastError error
	var haveObservation bool
	for {
		if err := ctx.Err(); err != nil {
			clearComplete(&last)
			return last, errors.Join(lastError, err)
		}
		current, admitted, err := observe(ctx)
		if admitted || !haveObservation {
			last = current
		}
		haveObservation = haveObservation || admitted
		if err != nil {
			lastError = multicluster.RetainWaitQueryCause(ctx, lastError, err)
		}
		if canceled := ctx.Err(); canceled != nil {
			clearComplete(&last)
			return last, errors.Join(lastError, canceled)
		}
		if errors.Is(err, errCephFSObservationGuard) {
			clearComplete(&last)
			return last, err
		}
		if err == nil && complete(last) {
			return last, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			clearComplete(&last)
			return last, errors.Join(lastError, ctx.Err())
		case <-timer.C:
		}
	}
}
