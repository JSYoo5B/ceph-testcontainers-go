package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
)

// retainedCancelFault forwards the real runner unchanged. Only after the runner
// returns a container without an error does it cancel the caller's context and
// hand the unchanged result back, so the cancellation lands after creation and
// readiness. It never replaces an actual runtime failure, withholds a reply,
// retries a creation or changes the request's customizers.
type retainedCancelFault struct {
	next     func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
	cancel   context.CancelFunc
	calls    int
	canceled int
	observed testcontainers.Container
}

func (f *retainedCancelFault) run(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	f.calls++
	ctr, err := f.next(ctx, image, opts...)
	if err != nil || ctr == nil {
		return ctr, err
	}
	f.observed = ctr
	f.canceled++
	f.cancel()
	return ctr, nil
}
