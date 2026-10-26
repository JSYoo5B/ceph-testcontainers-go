package rgw

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
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
	return gateways(c).sorted(), nil
}

// WithGateways starts named gateways during Run. No arguments starts one
// default gateway. Repeated options replace the list. All gateways share this
// cluster's backing zone; multisite scope is configured separately by
// RunMultisite and RunTopology.
func WithGateways(configs ...Config) ceph.Option {
	if len(configs) == 0 {
		configs = []Config{{}}
	}
	return servicehook.WithBootstrap(rgwBootstrapKind, false, func() (servicehook.Bootstrap, error) { return newRGWBootstrap(configs), nil })
}

// WithImage selects the image for every gateway of the cluster, those
// started by Run with WithGateways as well as those started later with Start.
// If omitted, gateways use the image passed to Run. Use matching Ceph
// versions. Multisite gateways are not affected: RunMultisite and RunTopology
// take their gateway image as an argument.
func WithImage(image string) ceph.Option { return servicehook.WithServiceImage("rgw", image) }

// defaultGateway selects one default gateway when no gateway option was given.
func defaultGateway() ceph.Option {
	return servicehook.WithBootstrap(rgwBootstrapKind, true, func() (servicehook.Bootstrap, error) { return newRGWBootstrap([]Config{{}}), nil })
}

// gatewayRegistry is the cluster's per-cluster RGW state. The cluster mutex
// guards it.
type gatewayRegistry struct {
	byName map[string]*Gateway
}

// gateways returns the cluster's gateway registry, creating it on first use.
// The caller holds the cluster mutex.
func gateways(c *ceph.Container) *gatewayRegistry {
	return gatewaysFrom(func(kind string, create func() any) any { return servicehook.Extension(c, kind, create) })
}

// gatewaysFrom is gateways for a caller that reaches the cluster's service
// state through something other than the cluster, such as a tenant port.
func gatewaysFrom(extension func(kind string, create func() any) any) *gatewayRegistry {
	return extension(rgwBootstrapKind, func() any {
		return &gatewayRegistry{byName: make(map[string]*Gateway)}
	}).(*gatewayRegistry)
}

func (r *gatewayRegistry) sorted() []*Gateway {
	result := slices.Collect(maps.Values(r.byName))
	slices.SortFunc(result, func(a, b *Gateway) int { return strings.Compare(a.GatewayName, b.GatewayName) })
	return result
}

// Secrets lists the gateways' S3 credentials for diagnostics to redact.
func (r *gatewayRegistry) Secrets() []string {
	var secrets []string
	for _, gateway := range r.byName {
		if gateway != nil {
			secrets = append(secrets, gateway.AccessKey, gateway.SecretKey)
		}
	}
	return secrets
}

// ConfigTargets lists gateway containers that read the cluster's ceph.conf.
func (r *gatewayRegistry) ConfigTargets() map[string]testcontainers.Container {
	targets := make(map[string]testcontainers.Container)
	for name, gateway := range r.byName {
		if gateway != nil && gateway.Container != nil {
			targets["rgw."+name] = gateway.Container
		}
	}
	return targets
}
