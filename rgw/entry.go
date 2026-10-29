package rgw

import (
	"context"
	"errors"
	"maps"
	"slices"

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
	return gateways(c).Sorted(), nil
}

// WithGateways starts named gateways during Run. No arguments starts one
// default gateway. Repeated options replace the list. All gateways share this
// cluster's backing zone; multisite scope is configured separately by
// multisite.Run.
func WithGateways(configs ...Config) ceph.Option {
	if len(configs) == 0 {
		configs = []Config{{}}
	}
	return servicehook.WithBootstrap(bootstrapKind, false, func() (servicehook.Bootstrap, error) { return newRGWBootstrap(configs), nil })
}

// defaultGateway selects one default gateway when no gateway option was given.
func defaultGateway() ceph.Option {
	return servicehook.WithBootstrap(bootstrapKind, true, func() (servicehook.Bootstrap, error) { return newRGWBootstrap([]Config{{}}), nil })
}

// gatewayRegistry holds the gateways this cluster owns. The cluster mutex
// guards byName; the diagnostics and configuration readers use the same state.
type gatewayRegistry struct {
	byName map[string]*Gateway
}

// gateways returns the cluster's owned gateways, creating their state on first use.
// The caller holds the cluster mutex.
func gateways(c *ceph.Container) *gatewayRegistry {
	return servicehook.Extension(c, bootstrapKind, func() any {
		return &gatewayRegistry{byName: make(map[string]*Gateway)}
	}).(*gatewayRegistry)
}

// Sorted returns gateways in name order, including partial startup attempts.
func (r *gatewayRegistry) Sorted() []*Gateway {
	result := make([]*Gateway, 0, len(r.byName))
	for _, name := range slices.Sorted(maps.Keys(r.byName)) {
		result = append(result, r.byName[name])
	}
	return result
}

// Secrets lists owned gateway credentials for diagnostic redaction.
func (r *gatewayRegistry) Secrets() []string {
	var result []string
	for _, g := range r.byName {
		if g != nil {
			result = append(result, g.AccessKey, g.SecretKey)
		}
	}
	return result
}

// ConfigTargets lists owned gateway containers that read ceph.conf.
func (r *gatewayRegistry) ConfigTargets() map[string]testcontainers.Container {
	targets := make(map[string]testcontainers.Container)
	for name, g := range r.byName {
		if g != nil && g.Container != nil {
			targets["rgw."+name] = g.Container
		}
	}
	return targets
}
