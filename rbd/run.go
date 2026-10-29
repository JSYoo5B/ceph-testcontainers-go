package rbd

import (
	"context"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Run starts a disposable cluster for RBD clients. It takes the same
// ceph.RunOptions values as ceph.Run, composed the same way; pass the options of
// other service packages through RunOptions.WithCustomizers. When no WithPools option is given,
// it creates and initializes a replicated pool named rbd.
// Bootstrap options that leave the cluster without OSDs for the pool, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, configs ...ceph.RunOptions) (*Container, error) {
	c, err := ceph.Run(ctx, append(slices.Clone(configs), ceph.RunOptions{}.WithCustomizers(defaultPool()))...)
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
