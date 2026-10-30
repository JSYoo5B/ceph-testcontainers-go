package rbd

import (
	"context"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// bootstrapKind identifies this service in Run provisioning and owned state.
const bootstrapKind = "rbd"

// poolsBootstrap creates RBD pools with the initial pools and initializes
// their RBD metadata once they exist.
type poolsBootstrap struct{ pools []ceph.PoolConfig }

func newPoolsBootstrap(configs []ceph.PoolConfig) (*poolsBootstrap, error) {
	pools := make([]ceph.PoolConfig, len(configs))
	for i, config := range configs {
		if config.Application != "" && config.Application != "rbd" {
			return nil, fmt.Errorf("RBD pool %q cannot use application %q", config.Name, config.Application)
		}
		if config.ErasureCode != nil {
			return nil, fmt.Errorf("RBD pool %q must be replicated; create EC data pools with WithPools", config.Name)
		}
		config = clonePool(config)
		config.Application = "rbd"
		pools[i] = config
	}
	return &poolsBootstrap{pools: pools}, nil
}

func (b *poolsBootstrap) Kind() string                    { return bootstrapKind }
func (b *poolsBootstrap) Order() int                      { return 1 }
func (b *poolsBootstrap) InitialPools() []ceph.PoolConfig { return b.pools }
func (b *poolsBootstrap) NeedsManagers() bool             { return false }
func (b *poolsBootstrap) MGRModules() []string            { return nil }

func (b *poolsBootstrap) Prepare(func(ceph.PoolConfig) ceph.PoolConfig, func(ceph.PoolConfig) error) error {
	return nil
}

func (b *poolsBootstrap) Start(ctx context.Context, c *ceph.Container) error {
	for _, pool := range b.pools {
		if err := initPool(ctx, c, pool.Name); err != nil {
			return fmt.Errorf("initialize RBD pool %q: %w", pool.Name, err)
		}
	}
	return nil
}

// WithPools creates replicated pools for RBD images during Run and
// initializes their RBD metadata. Application defaults to rbd and must not be
// another application; erasure-coded data pools are created with WithPools and
// referenced as an image's data pool. Repeated options replace the list, and
// passing the option replaces the default rbd pool of Run. The other
// ceph.PoolConfig fields keep their ceph meaning: a zero PGNum selects 8 PGs
// with the autoscaler off, zero Replicas and MinSize select the cluster
// defaults (2/1, or 1/1 with one OSD), and the placement fields are
// validated against the OSDs like those of any other pool.
//
//purpose:construction
func WithPools(configs ...ceph.PoolConfig) ceph.Option {
	return servicehook.WithBootstrap(bootstrapKind, false, func() (servicehook.Bootstrap, error) { return newPoolsBootstrap(configs) })
}

// defaultPool selects one initialized RBD pool named rbd when no RBD pool
// option was given.
func defaultPool() ceph.Option {
	return servicehook.WithBootstrap(bootstrapKind, true, func() (servicehook.Bootstrap, error) {
		return newPoolsBootstrap([]ceph.PoolConfig{{Name: "rbd", Application: "rbd"}})
	})
}

func clonePool(config ceph.PoolConfig) ceph.PoolConfig {
	if config.ErasureCode != nil {
		ec := *config.ErasureCode
		config.ErasureCode = &ec
	}
	return config
}
