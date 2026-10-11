package cluster

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
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

// TryLock holds the cluster mutex if it is free.
func (serviceHooks) TryLock(c *Container) bool { return c.mu.TryLock() }

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

// LockTopology holds the cluster mutex unless ctx ends first.
func (serviceHooks) LockTopology(ctx context.Context, c *Container) error { return c.lockTopology(ctx) }

// Service returns the named service container the cluster owns, or nil. The
// caller holds the cluster mutex.
func (serviceHooks) Service(c *Container, name string) testcontainers.Container {
	return c.services[name]
}

// SetService records a service container the cluster owns and terminates.
// The caller holds the cluster mutex.
func (serviceHooks) SetService(c *Container, name string, ctr testcontainers.Container) {
	if c.services == nil {
		c.services = make(map[string]testcontainers.Container)
	}
	c.services[name] = ctr
}

// DeleteService forgets a service container. The caller holds the mutex.
func (serviceHooks) DeleteService(c *Container, name string) { delete(c.services, name) }

// Services returns a copy of the service containers. The caller holds the
// cluster mutex.
func (serviceHooks) Services(c *Container) map[string]testcontainers.Container {
	return maps.Clone(c.services)
}

// PoolStates reads the native pool map. The caller holds the cluster mutex.
func (serviceHooks) PoolStates(ctx context.Context, c *Container) ([]PoolState, error) {
	return c.poolStates(ctx)
}

// StartService starts a service container on the cluster's networks and
// records it for termination. It takes the cluster mutex itself.
func (serviceHooks) StartService(ctx context.Context, c *Container, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	return c.startService(ctx, name, image, opts...)
}

// ReserveHostPorts reserves count host ports that the cluster tracks until
// release is called or the cluster terminates.
func (serviceHooks) ReserveHostPorts(ctx context.Context, c *Container, count int) ([]int, func(context.Context) error, error) {
	lease, err := c.reserveTrackedHostPorts(ctx, count)
	if err != nil {
		return nil, nil, err
	}
	return lease.Ports, lease.Release, nil
}

// HostPortAttempts bounds retries after a host port conflict.
func (serviceHooks) HostPortAttempts() int { return hostPortAttempts }

// IsPortConflict reports a container that failed because its port was taken.
func (serviceHooks) IsPortConflict(ctx context.Context, ctr testcontainers.Container) bool {
	return isPortConflict(ctx, ctr)
}

// OnlyMissingHostResource reports an error that only says a container or
// network is already gone.
func (serviceHooks) OnlyMissingHostResource(err error) bool { return onlyMissingHostResource(err) }

// DiagnosticHandle returns ctr, or nil when it is a typed nil.
func (serviceHooks) DiagnosticHandle(ctr testcontainers.Container) testcontainers.Container {
	return diagnosticContainerHandle(ctr)
}

// ScriptFile returns an embedded bootstrap script as a container file.
func (serviceHooks) ScriptFile(name string) testcontainers.ContainerFile { return scriptFile(name) }

// ServiceImage returns the image selected for a service role.
func (serviceHooks) ServiceImage(c *Container, role string) string {
	switch role {
	case "rgw":
		return c.settings.rgwImage
	case "mds":
		return c.settings.mdsImage
	}
	return c.settings.controlImage
}

// WithServiceImage selects the image for a service role.
func (serviceHooks) WithServiceImage(role, image string) Option {
	return func(o *options) error {
		if strings.TrimSpace(image) == "" {
			return fmt.Errorf("%s image must not be blank", strings.ToUpper(role))
		}
		switch role {
		case "rgw":
			o.rgwImage = image
		case "mds":
			o.mdsImage = image
		default:
			return fmt.Errorf("unknown service role %q", role)
		}
		return nil
	}
}

// Extension returns the per-cluster state a service package stored under
// kind, or nil. The caller holds the cluster mutex.
func (serviceHooks) Extension(c *Container, kind string) any { return c.extensions[kind] }

// SetExtension stores per-cluster state for a service package. The caller
// holds the cluster mutex.
func (serviceHooks) SetExtension(c *Container, kind string, value any) {
	if c.extensions == nil {
		c.extensions = make(map[string]any)
	}
	c.extensions[kind] = value
}

// testClusterConfig selects the settings of a unit-test cluster value.
type testClusterConfig = struct {
	StartupTimeout time.Duration
	Closed         bool
	HostNetwork    bool
	PublicAddress  string
	NetworkName    string
}

// TestCluster builds a cluster value for the service packages' unit tests.
func (serviceHooks) TestCluster(control testcontainers.Container, config testClusterConfig) *Container {
	c := &Container{Container: control, closed: config.Closed, settings: options{
		startupTimeout: config.StartupTimeout, hostNetwork: config.HostNetwork, publicAddress: config.PublicAddress,
	}}
	if config.NetworkName != "" {
		c.network = &testcontainers.DockerNetwork{Name: config.NetworkName}
	}
	return c
}

// TestSetClosed marks a unit-test cluster value terminated or open.
func (serviceHooks) TestSetClosed(c *Container, closed bool) { c.closed = closed }

// TestControlMutex returns the gate that serializes native control commands,
// for unit tests that hold it.
func (serviceHooks) TestControlMutex(c *Container) *sync.RWMutex { return &c.controlMu }

// TestMutex returns the cluster mutex, for unit tests that hold it.
func (serviceHooks) TestMutex(c *Container) *sync.Mutex { return &c.mu }

// TestPortLeases counts the host port leases the cluster tracks.
func (serviceHooks) TestPortLeases(c *Container) int { return len(c.portLeases) }

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
