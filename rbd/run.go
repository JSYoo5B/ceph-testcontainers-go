package rbd

import (
	"context"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for RBD clients. It accepts every ceph.Run
// option and the options of other service packages. When no WithPools option is given,
// it creates and initializes a replicated pool named rbd.
// Bootstrap options that leave the cluster without OSDs for the pool, such as
// ceph.ClusterConfig.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
//
// Run returns after the pools exist and their RBD metadata is initialized.
// PG peering may still be finishing, so call WaitForClean before the first
// client I/O.
//
//purpose:construction
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, img, append(slices.Clone(opts), defaultPool())...)
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
