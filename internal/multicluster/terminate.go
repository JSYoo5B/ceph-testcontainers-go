package multicluster

import (
	"context"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

// TerminateOnce removes ctr under mu, remembering success in *done so fixture
// cleanup does not run container hooks twice. A missing container counts as
// removed, a nil ctr or an already removed one is a no-op, and a failed removal
// leaves *done false so it can be retried. The lock wait is bounded by ctx. The
// caller keeps the mutex and flag in its own daemon type, so its other
// observers continue to read the same state under the same lock.
func TerminateOnce(ctx context.Context, mu *sync.Mutex, done *bool, ctr testcontainers.Container, opts ...testcontainers.TerminateOption) error {
	if err := LockObservation(ctx, mu); err != nil {
		return err
	}
	defer mu.Unlock()
	if *done || ctr == nil {
		return nil
	}
	if err := IgnoreMissing(ctr.Terminate(ctx, opts...)); err != nil {
		return err
	}
	*done = true
	return nil
}

// TerminateFixture closes a mirror fixture under mu: it marks *closed, runs the
// owned cleanup actions and, only when they all succeed, calls forget so the
// caller drops its daemon membership and initial-daemon handle. A cleanup error
// leaves the membership in place so the call can be retried; *closed stays set
// either way, so later admission sees a closed fixture. The lock wait is
// bounded by ctx. The caller keeps the mutex, flag and fields in its own
// fixture type, so its other observers read the same state under the same lock.
func TerminateFixture(ctx context.Context, mu *sync.Mutex, closed *bool, owned *Resources, forget func(), opts ...testcontainers.TerminateOption) error {
	if err := LockObservation(ctx, mu); err != nil {
		return err
	}
	defer mu.Unlock()
	*closed = true
	if err := owned.Terminate(ctx, opts...); err != nil {
		return err
	}
	forget()
	return nil
}
