package cluster

import (
	"context"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// serviceHooks gives the rbd, cephfs and rgw packages the parts of a cluster
// that are not public API. Its methods are exported only so that
// internal/servicehook can call them through an interface; the type itself is
// unexported and never appears in the public packages.
type serviceHooks struct{}

func init() { hookreg.Value = serviceHooks{} }

// Lock and Unlock hold the cluster mutex that serializes fixture mutations.
func (serviceHooks) Lock(c *Container)   { c.mu.Lock() }
func (serviceHooks) Unlock(c *Container) { c.mu.Unlock() }

// StartupTimeout bounds one fixture operation.
func (serviceHooks) StartupTimeout(c *Container) time.Duration { return c.settings.startupTimeout }

// CLI returns the container that runs Ceph CLI commands, or nil.
func (serviceHooks) CLI(c *Container) testcontainers.Container { return c.cliContainer() }

// Closed reports a terminated cluster.
func (serviceHooks) Closed(c *Container) bool { return c.closed }

// Command runs a CLI command in ctr and returns its standard output.
func (serviceHooks) Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	return command(ctx, ctr, args...)
}

// ServiceBootstrap returns the Run option that records a service bootstrap.
func (serviceHooks) ServiceBootstrap(kind string, fallback bool, build func() (serviceBootstrap, error)) Option {
	return withServiceBootstrap(kind, fallback, build)
}

// TestCluster builds a cluster value for the service packages' unit tests.
func (serviceHooks) TestCluster(control testcontainers.Container, startupTimeout time.Duration, closed bool) *Container {
	return &Container{Container: control, settings: options{startupTimeout: startupTimeout}, closed: closed}
}

// TestApply applies options to empty settings and returns the service
// bootstraps they record, for the service packages' unit tests.
func (serviceHooks) TestApply(opts ...Option) ([]serviceBootstrap, error) {
	var settings options
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	return settings.services, nil
}

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list.
func (serviceHooks) TestPrepare(osds int, noInitialOSDs bool, pools []PoolConfig, opts ...Option) ([]PoolConfig, error) {
	settings := options{osds: osds, noInitialOSDs: noInitialOSDs, pools: pools}
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return nil, err
	}
	return settings.pools, nil
}
