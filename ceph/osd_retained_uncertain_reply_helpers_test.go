package ceph

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
)

// retainedReplyFault forwards the real runner unchanged, then withholds only a
// successful reply. It never replaces an actual runtime failure with its
// injected error, retries a creation or changes the request's customizers.
type retainedReplyFault struct {
	next     func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
	lost     error
	calls    int
	observed testcontainers.Container
}

func (f *retainedReplyFault) run(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	f.calls++
	ctr, err := f.next(ctx, image, opts...)
	if err != nil || ctr == nil {
		return ctr, err
	}
	f.observed = ctr
	return nil, f.lost
}
