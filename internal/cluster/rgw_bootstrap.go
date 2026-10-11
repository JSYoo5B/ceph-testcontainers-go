package cluster

import (
	"context"
	"fmt"
	"slices"
)

const rgwBootstrapKind = "rgw"

// rgwBootstrap validates initial gateway names and starts the gateways once
// the initial pools exist.
type rgwBootstrap struct{ configs []RGWConfig }

func newRGWBootstrap(configs []RGWConfig) *rgwBootstrap {
	return &rgwBootstrap{configs: slices.Clone(configs)}
}

func (b *rgwBootstrap) Kind() string               { return rgwBootstrapKind }
func (b *rgwBootstrap) Order() int                 { return 3 }
func (b *rgwBootstrap) InitialPools() []PoolConfig { return nil }
func (b *rgwBootstrap) NeedsManagers() bool        { return true }
func (b *rgwBootstrap) MGRModules() []string       { return nil }

func (b *rgwBootstrap) Prepare(func(PoolConfig) PoolConfig, func(PoolConfig) error) error {
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

func (b *rgwBootstrap) Start(ctx context.Context, c *Container) error {
	for _, config := range b.configs {
		if _, err := c.startRGW(ctx, config); err != nil {
			return err
		}
	}
	return nil
}
