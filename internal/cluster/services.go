package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// The public cephfs, rgw and rbd packages expose these service entry points.
// They are functions rather than Container methods so that the base cluster
// type carries only cluster-wide operations.

var errNilCluster = errors.New("ceph cluster is unavailable")

// StartCephFS creates one filesystem with dedicated pools and its
// ActiveMDS+StandbyMDS daemons on an existing cluster. A zero config selects
// the default filesystem tc-cephfs with one active MDS named a. Every pool and
// OSD placement is validated before mutations. Existing filesystems are
// rejected instead of modified. Creating a second filesystem enables Ceph's
// global enable_multiple flag; the existing filesystem and its configuration
// are preserved.
//
// Customizers apply to each MDS and must preserve its identity, config, keyring,
// entrypoint and networking. A non-nil result with an error identifies attempted
// resources; the cluster owns cleanup, including partial daemon startup.
func StartCephFS(ctx context.Context, c *Container, config CephFSConfig, opts ...testcontainers.ContainerCustomizer) (*CephFSContainer, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.startCephFS(ctx, config, opts...)
}

// Filesystems returns the cluster's owned filesystem descriptors sorted by
// filesystem name, including a descriptor whose setup partially failed. The
// cluster owns their MDS containers.
func Filesystems(c *Container) []*CephFSContainer {
	if c == nil {
		return nil
	}
	return c.filesystemList()
}

// StartRGW starts an independently owned, named gateway on an existing
// cluster. A zero config starts the default gateway and creates its test user.
// Customizers are applied last; preserve the required command, network and
// listener settings. A non-nil result returned with an error remains owned by
// the cluster for cleanup.
func StartRGW(ctx context.Context, c *Container, config RGWConfig, opts ...testcontainers.ContainerCustomizer) (*RGWContainer, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.startRGW(ctx, config, opts...)
}

// RemoveCephFS deletes an owned filesystem: it fails and removes it in the
// FSMap, terminates its MDS daemons, deletes their Cephx keys and removes the
// metadata, default data and AdditionalDataPools that its configuration
// created, together with their CRUSH rules and EC profiles. Pools attached
// later with AddDataPool are detached but kept; remove them with RemovePool.
// File data in the removed pools is destroyed. Stop clients, CephFS mirrors
// and subvolume operations on the filesystem first; their handles fail
// afterwards. A retry after an error continues with whatever remains.
func RemoveCephFS(ctx context.Context, c *Container, name string) error {
	if c == nil {
		return errNilCluster
	}
	return c.removeCephFS(ctx, name)
}

// RemoveRGW removes one owned gateway container. Zone configuration, S3 users,
// buckets and data remain available through other gateways or a replacement.
// Multisite period endpoints are not changed automatically. The last gateway
// can be removed because RGW is optional. Failed cleanup remains tracked.
func RemoveRGW(ctx context.Context, c *Container, name string) error {
	if c == nil {
		return errNilCluster
	}
	return c.removeRGW(ctx, name)
}

// Gateways returns the cluster's named gateway descriptors sorted by gateway
// name, including partially initialized gateways. The cluster owns their
// containers.
func Gateways(c *Container) []*RGWContainer {
	if c == nil {
		return nil
	}
	return c.gatewayList()
}

// GatewaysContext returns sorted owned gateway descriptors, including partial
// startup attempts, while bounding the wait for fixture topology by ctx.
func GatewaysContext(ctx context.Context, c *Container) ([]*RGWContainer, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.gatewaysContext(ctx)
}

// InitRBDPool initializes an existing replicated pool for RBD metadata. Pools
// registered to other applications are rejected; no force option is used. An
// existing RBD initialization is safe to repeat. EC pools may supply image data
// but cannot hold RBD metadata. Create the pool first, then wait for clean PGs
// before client I/O. Partial initialization never deletes pool data.
func InitRBDPool(ctx context.Context, c *Container, pool string) error {
	if c == nil {
		return errNilCluster
	}
	return c.initRBDPool(ctx, pool)
}

// CreateRBDNamespace creates a fresh image namespace in an initialized,
// replicated RBD pool. Existing names are rejected without changing their data.
// A non-nil descriptor returned with an error identifies an attempted creation,
// but cannot authorize removal unless creation was confirmed. Inspect native
// state or dispose of the cluster after an uncertain creation failure.
func CreateRBDNamespace(ctx context.Context, c *Container, pool, name string) (*RBDNamespace, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.createRBDNamespace(ctx, pool, name)
}

// ListRBDNamespaces returns the pool's native namespace names in sorted order,
// including names created outside this fixture. The unnamed default namespace
// is omitted by Ceph. Listing does not grant ownership for removal.
func ListRBDNamespaces(ctx context.Context, c *Container, pool string) ([]string, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.namespaceList(ctx, pool)
}

// RemoveRBDNamespace removes a confirmed namespace owned by this cluster. Ceph
// refuses nonempty namespaces, including images in trash. No image purge or
// force option is used. Successful removal is idempotent; failed CLI removal
// can be retried with the same descriptor and a new context. Replacing its
// underlying pool is rejected. Do not delete and recreate the same named
// namespace through another client while its descriptor is in use: native RBD
// namespaces have no separate generation identity to distinguish replacements.
func RemoveRBDNamespace(ctx context.Context, c *Container, ns *RBDNamespace) error {
	if c == nil {
		return errNilCluster
	}
	return c.removeRBDNamespace(ctx, ns)
}

// WithRBDPools creates replicated pools for RBD images during Run and
// initializes their RBD metadata. Application defaults to rbd and must not be
// another application; erasure-coded data pools are created with WithPools and
// referenced as an image's data pool. Repeated options replace the list.
func WithRBDPools(configs ...PoolConfig) Option {
	return func(o *options) error {
		pools := make([]PoolConfig, len(configs))
		for i, config := range configs {
			if config.Application != "" && config.Application != "rbd" {
				return fmt.Errorf("RBD pool %q cannot use application %q", config.Name, config.Application)
			}
			if config.ErasureCode != nil {
				return fmt.Errorf("RBD pool %q must be replicated; create EC data pools with WithPools", config.Name)
			}
			config = clonePoolConfig(config)
			config.Application = "rbd"
			pools[i] = config
		}
		o.rbdPools = pools
		return nil
	}
}

// DefaultCephFS selects the default filesystem when no filesystem option was
// given. The cephfs package appends it to the options of its Run.
func DefaultCephFS() Option {
	return func(o *options) error {
		if len(o.filesystems) == 0 {
			o.filesystems = []CephFSConfig{{}}
		}
		return nil
	}
}

// DefaultRGW selects one default gateway when no gateway option was given.
func DefaultRGW() Option {
	return func(o *options) error {
		if len(o.gateways) == 0 {
			o.gateways = []RGWConfig{{}}
		}
		return nil
	}
}

// DefaultRBDPool selects one initialized RBD pool named rbd when no RBD pool
// option was given.
func DefaultRBDPool() Option {
	return func(o *options) error {
		if len(o.rbdPools) == 0 {
			o.rbdPools = []PoolConfig{{Name: "rbd", Application: "rbd"}}
		}
		return nil
	}
}

// RBD pools join the ordinary initial pool list before validation, so every
// placement and bootstrap rule applies to them; their names are kept for the
// rbd pool init that follows pool creation.
func mergeRBDPools(settings *options) {
	for _, config := range settings.rbdPools {
		settings.pools = append(settings.pools, config)
		settings.rbdPoolNames = append(settings.rbdPoolNames, config.Name)
	}
	settings.rbdPools = nil
	settings.rbdPoolNames = slices.Clip(settings.rbdPoolNames)
}
