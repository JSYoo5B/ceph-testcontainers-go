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
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*ceph.Container, error) {
	return ceph.Run(ctx, img, append(slices.Clone(opts), cluster.DefaultCephFS())...)
}
