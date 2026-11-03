package multicluster

import (
	"context"
	"errors"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

// LockObservation acquires mutex unless ctx ends first. Observation waits
// include time queued behind a topology mutation or another observer. No
// goroutine remains waiting after cancellation.
func LockObservation(ctx context.Context, mutex *sync.Mutex) error {
	return clusterutil.LockMutex(ctx, mutex)
}

// RetainWaitQueryCause handles a caller-context-only failure, which has no new independent native query cause.
// Retain an earlier exposed cause without changing inner-attempt timeouts,
// mixed errors, permanent guards, or newer successful observations.
func RetainWaitQueryCause(ctx context.Context, previous, current error) error {
	if ctx.Err() != nil && previous != nil && waitContextOnlyError(current) {
		return errors.Join(previous, current)
	}
	return current
}

func waitContextOnlyError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			found = true
			if !waitContextOnlyError(child) {
				return false
			}
		}
		return found
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok && wrapped.Unwrap() != nil {
		return waitContextOnlyError(wrapped.Unwrap())
	}
	return err == context.Canceled || err == context.DeadlineExceeded
}
