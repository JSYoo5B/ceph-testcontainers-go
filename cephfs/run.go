package cephfs

import (
	"context"
	"slices"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for CephFS clients. It accepts every ceph.Run
// option and the options of other service packages. When no WithFilesystems option
// is given, it creates the default filesystem tc-cephfs with one active MDS.
// Bootstrap options that leave the cluster without OSDs or managers for the filesystem, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, img, append(slices.Clone(opts), cluster.DefaultCephFS())...)
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
