package cluster

import (
	"context"
	"errors"

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

// DefaultCephFS selects the default filesystem when no filesystem option was
// given. The cephfs package appends it to the options of its Run.
func DefaultCephFS() Option {
	return withServiceBootstrap(cephFSBootstrapKind, true, func() (serviceBootstrap, error) { return newCephFSBootstrap([]CephFSConfig{{}}), nil })
}

// DefaultRGW selects one default gateway when no gateway option was given.
func DefaultRGW() Option {
	return withServiceBootstrap(rgwBootstrapKind, true, func() (serviceBootstrap, error) { return newRGWBootstrap([]RGWConfig{{}}), nil })
}
