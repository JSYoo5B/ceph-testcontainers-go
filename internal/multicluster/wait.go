package multicluster

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LockObservation acquires mutex unless ctx ends first. Observation waits
// include time queued behind a topology mutation or another observer. TryLock
// preserves the shared mutex without an abandoned lock goroutine.
func LockObservation(ctx context.Context, mutex *sync.Mutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mutex.TryLock() {
			if err := ctx.Err(); err != nil {
				mutex.Unlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
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
