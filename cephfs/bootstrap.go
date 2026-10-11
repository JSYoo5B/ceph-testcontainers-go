package cephfs

import (
	"context"
	"fmt"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

const cephFSBootstrapKind = "cephfs"

// cephFSBootstrap validates initial filesystems with the initial composition
// and creates them, with their MDS topologies, once the pools exist.
type cephFSBootstrap struct{ configs []Config }

func newCephFSBootstrap(configs []Config) *cephFSBootstrap {
	b := &cephFSBootstrap{configs: make([]Config, len(configs))}
	for i, config := range configs {
		b.configs[i] = cloneCephFSConfig(config)
	}
	return b
}

func (b *cephFSBootstrap) Kind() string                    { return cephFSBootstrapKind }
func (b *cephFSBootstrap) Order() int                      { return 2 }
func (b *cephFSBootstrap) InitialPools() []ceph.PoolConfig { return nil }
func (b *cephFSBootstrap) NeedsManagers() bool             { return true }
func (b *cephFSBootstrap) MGRModules() []string            { return []string{"volumes"} }

func (b *cephFSBootstrap) Prepare(resolve func(ceph.PoolConfig) ceph.PoolConfig, validate func(ceph.PoolConfig) error) error {
	names := make(map[string]bool)
	for i, config := range b.configs {
		config, err := normalizeCephFSConfig(resolveFilesystemPools(resolve, config))
		if err != nil {
			return err
		}
		if names[config.Name] {
			return fmt.Errorf("initial filesystem %q is duplicated", config.Name)
		}
		names[config.Name] = true
		for _, pool := range cephFSConfiguredPools(config) {
			if err := validate(pool); err != nil {
				return err
			}
		}
		if config.NoInitialMDS {
			// Preserve the omitted startup-count request for the runtime constructor;
			// validation above used the ordinary prospective max_mds of one.
			config.ActiveMDS = 0
		}
		b.configs[i] = config
	}
	return nil
}

func (b *cephFSBootstrap) Start(ctx context.Context, c *ceph.Container) error {
	for _, config := range b.configs {
		if _, err := startFilesystem(ctx, c, config); err != nil {
			return err
		}
	}
	return nil
}
