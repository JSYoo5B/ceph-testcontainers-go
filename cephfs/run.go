package cephfs

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for CephFS clients. It takes the same
// arguments as ceph.Run: the ceph.RunOptions, in which the options of other
// service packages go through WithOptions, and trailing container customizers. When no WithFilesystems option
// is given, it creates the default filesystem tc-cephfs with one active MDS.
// Bootstrap options that leave the cluster without OSDs or managers for the filesystem, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, config ceph.RunOptions, customizers ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, config.WithOptions(defaultFilesystem()), customizers...)
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

// Filesystems returns the cluster's filesystems sorted by name: the package
// function Filesystems applied to the embedded cluster.
func (c *Container) Filesystems() []*Filesystem {
	if c == nil {
		return nil
	}
	return Filesystems(c.Container)
}
