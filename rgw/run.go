package rgw

import (
	"context"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Run starts a disposable cluster for S3 and Swift clients. It takes the same
// arguments as ceph.Run: the ceph.RunOptions, in which the options of other
// service packages go through WithOptions, and trailing container customizers. When no WithGateways option is
// given, it starts the default gateway and creates its test user.
// Bootstrap options that leave the cluster without OSDs or managers for the gateway, such as
// ceph.WithNoInitialOSDs, are rejected; use ceph.Run for those stages.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, config ceph.RunOptions, customizers ...testcontainers.ContainerCustomizer) (*Container, error) {
	c, err := ceph.Run(ctx, config.WithOptions(defaultGateway()), customizers...)
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

// Gateways returns the cluster's gateways sorted by name: the package
// function Gateways applied to the embedded cluster. A nil receiver returns an
// error.
func (c *Container) Gateways(ctx context.Context) ([]*Gateway, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return Gateways(ctx, c.Container)
}

// S3Endpoint returns the S3 endpoint of the cluster's only gateway, such as
// the default gateway that Run starts when no WithGateways option is given.
// It fails when the cluster has several gateways or none; choose one from
// Gateways then.
func (c *Container) S3Endpoint(ctx context.Context) (string, error) {
	if c == nil {
		return "", errNilCluster
	}
	gateways, err := Gateways(ctx, c.Container)
	if err != nil {
		return "", err
	}
	if len(gateways) != 1 {
		return "", fmt.Errorf("cluster has %d RGW gateways; choose one from Gateways", len(gateways))
	}
	return gateways[0].S3Endpoint(ctx)
}
