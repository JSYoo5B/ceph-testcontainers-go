package rbd

import (
	"context"
	"slices"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for RBD clients. It accepts every ceph.Run
// option and the options of other service packages. When no WithPools option is given,
// it creates and initializes a replicated pool named rbd.
// Bootstrap options that leave the cluster without OSDs for the pool, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, error) {
	return ceph.Run(ctx, img, append(slices.Clone(opts), cluster.DefaultRBDPool())...)
}
