package multicluster

import (
	"context"
	"errors"
)

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
	return EveryBranch(err, func(leaf error) bool {
		return leaf == context.Canceled || leaf == context.DeadlineExceeded
	})
}
