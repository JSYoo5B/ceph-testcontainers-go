// Package servicehook gives the rbd, cephfs and rgw packages the parts of a
// ceph.Container that are not public API, such as its mutex and Run-time
// provisioning. The cluster implementation registers them in
// internal/hookreg.
package servicehook

import (
	"context"
	"sync"
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
	TryLock(c *ceph.Container) bool
	StartupTimeout(c *ceph.Container) time.Duration
	CLI(c *ceph.Container) testcontainers.Container
	Closed(c *ceph.Container) bool
	Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error)
	ServiceBootstrap(kind string, fallback bool, build func() (Bootstrap, error)) ceph.Option
	LockTopology(ctx context.Context, c *ceph.Container) error
	Service(c *ceph.Container, name string) testcontainers.Container
	SetService(c *ceph.Container, name string, ctr testcontainers.Container)
	DeleteService(c *ceph.Container, name string)
	Services(c *ceph.Container) map[string]testcontainers.Container
	PoolStates(ctx context.Context, c *ceph.Container) ([]ceph.PoolState, error)
	StartService(ctx context.Context, c *ceph.Container, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
	ReserveHostPorts(ctx context.Context, c *ceph.Container, count int) ([]int, func(context.Context) error, error)
	HostPortAttempts() int
	IsPortConflict(ctx context.Context, ctr testcontainers.Container) bool
	OnlyMissingHostResource(err error) bool
	DiagnosticHandle(ctr testcontainers.Container) testcontainers.Container
	ScriptFile(name string) testcontainers.ContainerFile
	ServiceImage(c *ceph.Container, role string) string
	WithServiceImage(role, image string) ceph.Option
	Extension(c *ceph.Container, kind string) any
	SetExtension(c *ceph.Container, kind string, value any)
	TestCluster(control testcontainers.Container, config TestConfig) *ceph.Container
	TestSetClosed(c *ceph.Container, closed bool)
	TestControlMutex(c *ceph.Container) *sync.RWMutex
	TestMutex(c *ceph.Container) *sync.Mutex
	TestPortLeases(c *ceph.Container) int
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

// TryLock holds the cluster mutex if it is free.
func TryLock(c *ceph.Container) bool { return get().TryLock(c) }

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

// LockTopology holds the cluster mutex unless ctx ends first.
func LockTopology(ctx context.Context, c *ceph.Container) error { return get().LockTopology(ctx, c) }

// Service returns the named service container the cluster owns, or nil. The
// caller holds the cluster mutex.
func Service(c *ceph.Container, name string) testcontainers.Container { return get().Service(c, name) }

// SetService records a service container the cluster owns and terminates.
// The caller holds the cluster mutex.
func SetService(c *ceph.Container, name string, ctr testcontainers.Container) {
	get().SetService(c, name, ctr)
}

// DeleteService forgets a service container. The caller holds the mutex.
func DeleteService(c *ceph.Container, name string) { get().DeleteService(c, name) }

// Services returns a copy of the service containers. The caller holds the
// cluster mutex.
func Services(c *ceph.Container) map[string]testcontainers.Container { return get().Services(c) }

// PoolStates reads the native pool map. The caller holds the cluster mutex.
func PoolStates(ctx context.Context, c *ceph.Container) ([]ceph.PoolState, error) {
	return get().PoolStates(ctx, c)
}

// StartService starts a service container on the cluster's networks and
// records it for termination. It takes the cluster mutex itself.
func StartService(ctx context.Context, c *ceph.Container, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	return get().StartService(ctx, c, name, image, opts...)
}

// ReserveHostPorts reserves count host ports that the cluster tracks until
// release is called or the cluster terminates.
func ReserveHostPorts(ctx context.Context, c *ceph.Container, count int) ([]int, func(context.Context) error, error) {
	return get().ReserveHostPorts(ctx, c, count)
}

// HostPortAttempts bounds retries after a host port conflict.
func HostPortAttempts() int { return get().HostPortAttempts() }

// IsPortConflict reports a container that failed because its port was taken.
func IsPortConflict(ctx context.Context, ctr testcontainers.Container) bool {
	return get().IsPortConflict(ctx, ctr)
}

// OnlyMissingHostResource reports an error that only says a container or
// network is already gone.
func OnlyMissingHostResource(err error) bool { return get().OnlyMissingHostResource(err) }

// DiagnosticHandle returns ctr, or nil when it is a typed nil.
func DiagnosticHandle(ctr testcontainers.Container) testcontainers.Container {
	return get().DiagnosticHandle(ctr)
}

// ScriptFile returns an embedded bootstrap script as a container file.
func ScriptFile(name string) testcontainers.ContainerFile { return get().ScriptFile(name) }

// ServiceImage returns the image selected for a service role: rgw or mds.
func ServiceImage(c *ceph.Container, role string) string { return get().ServiceImage(c, role) }

// WithServiceImage selects the image for a service role: rgw or mds.
func WithServiceImage(role, image string) ceph.Option { return get().WithServiceImage(role, image) }

// Extension returns the per-cluster state a service package stored under
// kind, or nil. The caller holds the cluster mutex. The cluster reads
// Secrets() []string, ConfigTargets() map[string]testcontainers.Container
// and RequiresMGRModule(string) bool from such state when it implements them.
func Extension(c *ceph.Container, kind string) any { return get().Extension(c, kind) }

// SetExtension stores per-cluster state for a service package. The caller
// holds the cluster mutex.
func SetExtension(c *ceph.Container, kind string, value any) { get().SetExtension(c, kind, value) }

// TestConfig selects the settings of a unit-test cluster value.
type TestConfig = struct {
	StartupTimeout time.Duration
	Closed         bool
	HostNetwork    bool
	PublicAddress  string
	NetworkName    string
}

// TestSetClosed marks a unit-test cluster value terminated or open.
func TestSetClosed(c *ceph.Container, closed bool) { get().TestSetClosed(c, closed) }

// TestControlMutex returns the gate that serializes native control commands,
// for unit tests that hold it.
func TestControlMutex(c *ceph.Container) *sync.RWMutex { return get().TestControlMutex(c) }

// TestMutex returns the cluster mutex, for unit tests that hold it.
func TestMutex(c *ceph.Container) *sync.Mutex { return get().TestMutex(c) }

// TestPortLeases counts the host port leases the cluster tracks.
func TestPortLeases(c *ceph.Container) int { return get().TestPortLeases(c) }

// TestCluster builds a cluster value for unit tests: control runs the CLI.
func TestCluster(control testcontainers.Container, config TestConfig) *ceph.Container {
	return get().TestCluster(control, config)
}

// TestApply applies options to empty Run settings and returns the bootstraps
// they record.
func TestApply(opts ...ceph.Option) ([]Bootstrap, error) { return get().TestApply(opts...) }

// TestPrepare validates an initial composition of osds OSDs and pools with
// opts applied, as Run does, and returns the resulting initial pool list.
func TestPrepare(osds int, noInitialOSDs bool, pools []ceph.PoolConfig, opts ...ceph.Option) ([]ceph.PoolConfig, error) {
	return get().TestPrepare(osds, noInitialOSDs, pools, opts...)
}
