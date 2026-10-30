package rgw

import (
	"context"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for S3 and Swift clients. It accepts every ceph.Run
// option and the options of other service packages. When no WithGateways option is
// given, it starts the default gateway and creates its test user.
// Bootstrap options that leave the cluster without OSDs or managers for the gateway, such as
// ceph.ClusterConfig.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
//
//purpose:construction
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, img, append(slices.Clone(opts), defaultGateway())...)
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
