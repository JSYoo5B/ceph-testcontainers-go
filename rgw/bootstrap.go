package rgw

import (
	"context"
	"fmt"
	"slices"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

const rgwBootstrapKind = "rgw"

// rgwBootstrap validates initial gateway names and starts the gateways once
// the initial pools exist.
type rgwBootstrap struct{ configs []Config }

func newRGWBootstrap(configs []Config) *rgwBootstrap {
	return &rgwBootstrap{configs: slices.Clone(configs)}
}

func (b *rgwBootstrap) Kind() string                    { return rgwBootstrapKind }
func (b *rgwBootstrap) Order() int                      { return 3 }
func (b *rgwBootstrap) InitialPools() []ceph.PoolConfig { return nil }
func (b *rgwBootstrap) NeedsManagers() bool             { return true }
func (b *rgwBootstrap) MGRModules() []string            { return nil }

func (b *rgwBootstrap) Prepare(func(ceph.PoolConfig) ceph.PoolConfig, func(ceph.PoolConfig) error) error {
	names := make(map[string]bool)
	for i, config := range b.configs {
		config, err := normalizeRGWConfig(config)
		if err != nil {
			return err
		}
		if names[config.Name] {
			return fmt.Errorf("initial RGW gateway %q is duplicated", config.Name)
		}
		names[config.Name] = true
		b.configs[i] = config
	}
	return nil
}

func (b *rgwBootstrap) Start(ctx context.Context, c *ceph.Container) error {
	for _, config := range b.configs {
		if _, err := startGateway(ctx, c, config); err != nil {
			return err
		}
	}
	return nil
}
