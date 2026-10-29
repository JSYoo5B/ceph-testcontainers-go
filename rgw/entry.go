package rgw

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/namedregistry"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
	"github.com/testcontainers/testcontainers-go"
)

var errNilCluster = errors.New("ceph cluster is unavailable")

// Start starts an independently owned, named gateway on an existing
// cluster. A zero config starts the default gateway and creates its test user.
// Customizers are applied last; preserve the required command, network and
// listener settings. A non-nil result returned with an error remains owned by
// the cluster for cleanup.
func Start(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Gateway, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return startGateway(ctx, c, config, opts...)
}

// Remove removes this owned gateway container. Zone configuration, S3 users,
// buckets and data remain available through other gateways or a replacement.
// Multisite period endpoints are not changed automatically. The last gateway
// can be removed because RGW is optional. Failed cleanup remains tracked.
// A gateway that Remove already removed, or that is not the one the cluster
// owns for its name, returns an error.
func (g *Gateway) Remove(ctx context.Context) error {
	if g == nil || g.owner == nil {
		return errNilCluster
	}
	return removeGateway(ctx, g.owner, g)
}

// Gateways returns sorted owned gateway descriptors, including partial
// startup attempts, while bounding the wait for fixture topology by ctx.
func Gateways(ctx context.Context, c *ceph.Container) ([]*Gateway, error) {
	if c == nil {
		return nil, errNilCluster
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return nil, err
	}
	defer servicehook.Unlock(c)
	return gateways(c).Sorted(), nil
}

// WithGateways starts named gateways during Run. No arguments starts one
// default gateway. Repeated options replace the list. All gateways share this
// cluster's backing zone; multisite scope is configured separately by
// multisite.Run and multisite.RunTopology.
func WithGateways(configs ...Config) ceph.Option {
	if len(configs) == 0 {
		configs = []Config{{}}
	}
	return servicehook.WithBootstrap(servicekind.RGW, false, func() (servicehook.Bootstrap, error) { return newRGWBootstrap(configs), nil })
}

// WithImage selects the image for every gateway of the cluster, those
// started by Run with WithGateways as well as those started later with Start.
// If omitted, gateways use the image passed to Run. It selects the rgw role of
// the cluster, so Images().RGW, which the multisite setup clients use, reports
// it too. It replaces the rgw role of a WithImages set whichever option comes
// first. Use matching Ceph
// versions. Multisite gateways are not affected: multisite.Run and multisite.RunTopology
// take their gateway image as an argument.
func WithImage(image string) ceph.Option { return servicehook.WithServiceImage("rgw", image) }

// defaultGateway selects one default gateway when no gateway option was given.
func defaultGateway() ceph.Option {
	return servicehook.WithBootstrap(servicekind.RGW, true, func() (servicehook.Bootstrap, error) { return newRGWBootstrap([]Config{{}}), nil })
}

// gateways returns the cluster's gateway registry, creating it on first use.
// The caller holds the cluster mutex.
func gateways(c *ceph.Container) *namedregistry.Registry[*Gateway] {
	return describeGateways(namedregistry.From[*Gateway](func(kind string, create func() any) any { return servicehook.Extension(c, kind, create) }, servicekind.RGW))
}

// describeGateways tells the cluster how to read gateway credentials and
// config targets out of the registry, whichever side created it first.
func describeGateways(r *namedregistry.Registry[*Gateway]) *namedregistry.Registry[*Gateway] {
	return r.Describe(
		func(g *Gateway) []string {
			if g == nil {
				return nil
			}
			return []string{g.AccessKey, g.SecretKey}
		},
		func(name string, g *Gateway) (string, testcontainers.Container) {
			if g == nil || g.Container == nil {
				return "", nil
			}
			return "rgw." + name, g.Container
		})
}
