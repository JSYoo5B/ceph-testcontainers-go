// Package servicehook gives the rbd, cephfs and rgw packages the parts of a
// ceph.Container that are not public API, such as its mutex and Run-time
// provisioning. The cluster implementation registers them in
// internal/hookreg.
package servicehook

import (
	"context"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// Bootstrap is the provisioning a service adds to Run: pools created and
// validated with the initial pools, validation of its own configuration, and
// a start step after the initial pools exist. Bootstraps of one Kind replace
// each other; Order sorts the start steps (RBD pools, filesystems, gateways).
// resolve applies the cluster's pool defaults and validate checks a pool
// against the initial layout and the names already in use.
type Bootstrap = interface {
	Kind() string
	Order() int
	InitialPools() []ceph.PoolConfig
	NeedsManagers() bool
	MGRModules() []string
	Prepare(resolve func(ceph.PoolConfig) ceph.PoolConfig, validate func(ceph.PoolConfig) error) error
	Start(ctx context.Context, c *ceph.Container) error
}

type hooks interface {
	Lock(c *ceph.Container)
	Unlock(c *ceph.Container)
	StartupTimeout(c *ceph.Container) time.Duration
	CLI(c *ceph.Container) testcontainers.Container
	Closed(c *ceph.Container) bool
	Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error)
	ServiceBootstrap(kind string, fallback bool, build func() (Bootstrap, error)) ceph.Option
	TestCluster(control testcontainers.Container, startupTimeout time.Duration, closed bool) *ceph.Container
	TestApply(opts ...ceph.Option) ([]Bootstrap, error)
	TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, error)
}

func get() hooks {
	h, ok := hookreg.Value.(hooks)
	if !ok {
		panic("servicehook: the cluster implementation did not register its hooks")
	}
	return h
}

// Lock holds the cluster mutex that serializes fixture mutations.
func Lock(c *ceph.Container) { get().Lock(c) }

// Unlock releases the cluster mutex.
func Unlock(c *ceph.Container) { get().Unlock(c) }

// StartupTimeout bounds one fixture operation.
func StartupTimeout(c *ceph.Container) time.Duration { return get().StartupTimeout(c) }

// CLI returns the container that runs Ceph CLI commands, or nil.
func CLI(c *ceph.Container) testcontainers.Container { return get().CLI(c) }

// Closed reports a terminated cluster.
func Closed(c *ceph.Container) bool { return get().Closed(c) }

// Command runs a CLI command in ctr and returns its standard output.
func Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	return get().Command(ctx, ctr, args...)
}

// WithBootstrap returns the Run option that records the bootstrap build
// returns. A fallback applies only when no bootstrap of its kind was given.
func WithBootstrap(kind string, fallback bool, build func() (Bootstrap, error)) ceph.Option {
	return get().ServiceBootstrap(kind, fallback, build)
}

// TestCluster builds a cluster value for unit tests: control runs the CLI.
func TestCluster(control testcontainers.Container, startupTimeout time.Duration, closed bool) *ceph.Container {
	return get().TestCluster(control, startupTimeout, closed)
}

// TestApply applies options to empty Run settings and returns the bootstraps
// they record.
func TestApply(opts ...ceph.Option) ([]Bootstrap, error) { return get().TestApply(opts...) }

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list.
func TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, error) {
	return get().TestPrepare(osds, noInitialOSDs, pools, opts...)
}
