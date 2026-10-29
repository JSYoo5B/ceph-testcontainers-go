package rbd

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for RBD clients. It takes the same
// arguments as ceph.Run: the ceph.RunOptions, in which the options of other
// service packages go through WithOptions, and trailing container customizers. When no WithPools option is given,
// it creates and initializes a replicated pool named rbd.
// Bootstrap options that leave the cluster without OSDs for the pool, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, config ceph.RunOptions, customizers ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, config.WithOptions(defaultPool()), customizers...)
	if c == nil {
		return nil, err
	}
	return &Container{Container: c}, err
}

// Container is a cluster started by Run. It embeds *ceph.Container, so every
// cluster method is available on it; pass its Container field to functions
// that take a *ceph.Container, such as those of this and the other service
// packages.
type Container struct {
	*ceph.Container
}
