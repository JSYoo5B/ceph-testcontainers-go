package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

// serviceHooks gives the rbd, cephfs and rgw packages the parts of a cluster
// that are not public API. Its methods are exported only so that
// internal/servicehook can call them through an interface; the type itself is
// unexported and never appears in the public packages.
type serviceHooks struct{}

func init() {
	hookreg.Value = serviceHooks{}
	clusterutil.Quiet = quiet
}

// Lock and Unlock hold the cluster mutex that serializes fixture mutations.
func (serviceHooks) Lock(c *Container)   { c.mu.Lock() }
func (serviceHooks) Unlock(c *Container) { c.mu.Unlock() }

// TryLock holds the cluster mutex if it is free.
func (serviceHooks) TryLock(c *Container) bool { return c.mu.TryLock() }

// StartupTimeout bounds one fixture operation.
func (serviceHooks) StartupTimeout(c *Container) time.Duration { return c.settings.startupTimeout }

// CLI returns the container that runs Ceph CLI commands, or nil.
func (serviceHooks) ClientCommand(ctx context.Context, c *Container, args ...string) ([]byte, error) {
	target, err := c.clientContainerContext(ctx)
	if err != nil {
		return nil, err
	}
	return command(ctx, target, args...)
}

func (serviceHooks) CLI(c *Container) testcontainers.Container { return c.cliContainer() }

// Closed reports a terminated cluster.
func (serviceHooks) Closed(c *Container) bool { return c.closed }

// TenantCluster returns the shared cluster of a live tenant, whose gate
// still refuses the methods a tenant cannot use, or nil.
func (serviceHooks) TenantCluster(t *Tenant) *Container {
	c, err := t.cluster()
	if err != nil {
		return nil
	}
	return c
}

// TenantClient creates client.ID-suffix for a live tenant with caps that the
// service limited to the tenant's resources. The tenant's release deletes it,
// and WithClientIdentity and RunNativeCommand of the tenant accept it.
func (serviceHooks) TenantClient(ctx context.Context, t *Tenant, suffix string, caps ClientCaps) (*ClientConfig, error) {
	return t.serviceClient(ctx, suffix, caps)
}

// AttachTenantScope registers a service resource of a live tenant that its
// release removes with release, before its clients are deleted. release
// returns what remains of the resource; a remainder or an error poisons the
// shared cluster. Attach before creating the resource.
func (serviceHooks) AttachTenantScope(t *Tenant, key string, release func(ctx context.Context) ([]string, error)) error {
	return t.attachScope(key, release)
}

// RegisterSharedService records a service's shared-cluster hooks: prepare
// runs before a shared cluster's baseline, and probe lists the service's
// resources mapped to their owning tenant IDs.
func (serviceHooks) RegisterSharedService(kind string, prepare func(context.Context, *Container) error, probe func(context.Context, *Container) (map[string]string, error)) {
	registerSharedService(sharedService{kind: kind, prepare: prepare, probe: probe})
}

// Command runs a CLI command in ctr and returns its standard output.
func (serviceHooks) Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	return command(ctx, ctr, args...)
}

// RunOwnedShell runs argv in an admitted container of c under the shell
// supervisor and tracks it until it is known to have ended.
func (serviceHooks) RunOwnedShell(ctx context.Context, c *Container, target testcontainers.Container, label string, limit int, argv ...string) (supervised.ShellReport, error) {
	return c.runOwnedShell(ctx, target, label, limit, argv...)
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
	case "client":
		return c.Images().Client
	case "rbd-aux":
		return c.Images().RBDAux
	case "essential":
		return c.Images().Essential
	case "rgw":
		return c.settings.rgwImage
	case "mds":
		return c.settings.mdsImage
	}
	return ""
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
			if o.imageSet != nil {
				o.imageSet.RGW = image
			}
		case "mds":
			o.mdsImage = image
			if o.imageSet != nil {
				o.imageSet.CephFS = image
			}
		default:
			return fmt.Errorf("unknown service role %q", role)
		}
		return nil
	}
}

// Extension returns the per-cluster state a service package keeps under
// kind, storing the result of create on first use. It takes its own lock, so
// callers may or may not hold the cluster mutex.
func (serviceHooks) Extension(c *Container, kind string, create func() any) any {
	c.extensionsMu.Lock()
	defer c.extensionsMu.Unlock()
	if state, ok := c.extensions[kind]; ok {
		return state
	}
	if c.extensions == nil {
		c.extensions = make(map[string]any)
	}
	state := create()
	c.extensions[kind] = state
	return state
}

// ResolvePool applies the cluster's pool defaults: CRUSH root, replicas and
// minimum size.
func (serviceHooks) ResolvePool(c *Container, config PoolConfig) PoolConfig {
	return resolvePoolDefaults(c.settings, config)
}

// NormalizePool validates a pool configuration and fills its defaults.
func (serviceHooks) NormalizePool(config PoolConfig) (PoolConfig, error) {
	return normalizePoolConfig(config)
}

// PoolPlacementDomains counts the owned OSD placement domains that config
// can use. The caller holds the cluster mutex.
func (serviceHooks) PoolPlacementDomains(c *Container, config PoolConfig) int {
	return c.poolPlacementDomains(config)
}

// PoolPolicyReady reports whether the cluster's pool policy accepts changes.
// The caller holds the cluster mutex.
func (serviceHooks) PoolPolicyReady(c *Container) error { return c.poolPolicyReady() }

// RemovePool removes a fixture-created pool and its rule and profile. With
// skipForeign, a pool the fixture did not create is left alone. The caller
// holds the cluster mutex.
func (serviceHooks) RemovePool(ctx context.Context, c *Container, name string, skipForeign bool) error {
	return c.removePool(ctx, name, skipForeign)
}

// PoolMissing reports a RemovePool error for a name with nothing to remove.
func (serviceHooks) PoolMissing(err error) bool { return errors.Is(err, errPoolMissing) }

// CephBefore reports whether the recorded Ceph release is older than major.
func (serviceHooks) CephBefore(c *Container, major int) bool { return c.cephBefore(major) }

// NativeFailure describes a command that exited nonzero.
func (serviceHooks) NativeFailure(program string, code int, stdout, stderr []byte) error {
	return nativeFailure(program, code, stdout, stderr)
}

// ClassifyExec classifies a failed Docker exec of a native command.
func (serviceHooks) ClassifyExec(ctx context.Context, err error) error {
	return classifyDispatched(ctx, err)
}

// AuthCommand runs a Cephx command with the admin identity and reports a
// failure by operation without the native output.
func (serviceHooks) AuthCommand(ctx context.Context, c *Container, operation string, args ...string) ([]byte, error) {
	return c.clientAuthCommand(ctx, operation, args...)
}

// ClusterIdentity returns the FSID of the bootstrap configuration and whether
// the configuration and admin keyring were confirmed. The error is only ctx's.
func (serviceHooks) ClusterIdentity(ctx context.Context, c *Container) (string, bool, error) {
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return "", false, err
	}
	fsid, err := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	return fsid, err == nil && confirmed, nil
}

// ClientConfirmed reports a client identity that c created and confirmed.
func (serviceHooks) ClientConfirmed(client *ClientConfig, c *Container) bool {
	return client.owner == c && client.created && client.ready
}

// ClientKeyring returns the keyring retrieved when the client was created.
func (serviceHooks) ClientKeyring(client *ClientConfig) []byte { return client.keyring }

// ClientRevoked reports a client identity known to be deleted.
func (serviceHooks) ClientRevoked(client *ClientConfig) bool { return client.revoked }

// SetClientRevoked records whether a service deleted the client identity.
// The caller holds the cluster mutex.
func (serviceHooks) SetClientRevoked(client *ClientConfig, revoked bool) {
	client.revoked = revoked
	if revoked {
		client.owner.setClientKey(client.name, "")
	}
}

// SettleClient settles the client's identity and refuses a descriptor that is
// not its current one. A descriptor of an identity that the service deleted
// is only settled, for the service's own reconciliation. The caller holds the
// cluster mutex.
func (serviceHooks) SettleClient(ctx context.Context, c *Container, client *ClientConfig) error {
	if err := c.settleClient(ctx, client.name); err != nil {
		return err
	}
	if client.revoked {
		return nil
	}
	return c.checkCurrentClient(client)
}

// MonitorAddresses returns the bootstrap addresses of the monitors in quorum.
func (serviceHooks) MonitorAddresses(status QuorumStatus) (string, error) {
	return monitorBootstrapAddresses(status)
}

// testClusterConfig selects the settings of a unit-test cluster value.
type testClusterConfig = struct {
	StartupTimeout time.Duration
	Closed         bool
	HostNetwork    bool
	PublicAddress  string
	NetworkName    string
	Shared         bool
	// OSDs adds owned OSD descriptors 0..OSDs-1 whose container is control
	// and whose CRUSH location is OSDPlacement.
	OSDs         int
	OSDPlacement OSDConfig
	// MemStore selects the OSDMemStore object store instead of BlueStore.
	MemStore bool
}

// TestCluster builds a cluster value for the service packages' unit tests.
func (serviceHooks) TestCluster(control testcontainers.Container, config testClusterConfig) *Container {
	c := &Container{Container: control, closed: config.Closed, settings: options{
		startupTimeout: config.StartupTimeout, hostNetwork: config.HostNetwork, publicAddress: config.PublicAddress,
		controlImage: "unit-all", osdImage: "unit-all", mdsImage: "unit-all", rgwImage: "unit-all",
	}}
	if config.MemStore {
		c.settings.osdObjectStore = OSDMemStore
	}
	if config.Shared {
		c.sharing.Store(int32(sharedWithTenants))
	}
	if config.NetworkName != "" {
		c.network = &testcontainers.DockerNetwork{Name: config.NetworkName}
	}
	if config.OSDs > 0 {
		c.osds = make(map[int]*OSDContainer)
		for id := range config.OSDs {
			c.osds[id] = &OSDContainer{ID: id, Container: control, placement: config.OSDPlacement}
		}
	}
	return c
}

// testState points at a cluster's internals for the service packages' unit
// tests. PortLeases is a count taken when TestState is called.
type testState = struct {
	Mutex          *sync.Mutex
	ControlMutex   *sync.RWMutex
	ConfigMutex    *sync.RWMutex
	Closed         *bool
	Config         *[]byte
	Keyring        *[]byte
	CephVersion    *string
	ControlPlane   *testcontainers.Container
	OSDs           *map[int]*OSDContainer
	InitialOSDs    *int
	PoolReplicas   *int
	PoolMinSize    *int
	StartupTimeout *time.Duration
	MDSImage       *string
	RGWImage       *string
	PortLeases     int
	NetworkRemoved *bool
}

// TestState exposes a cluster's internals to unit tests.
func (serviceHooks) TestState(c *Container) testState {
	return testState{
		Mutex: &c.mu, ControlMutex: &c.controlMu, ConfigMutex: &c.configMu, Closed: &c.closed,
		Config: &c.config, Keyring: &c.keyring, CephVersion: &c.cephVersion, ControlPlane: &c.controlPlane,
		OSDs: &c.osds, InitialOSDs: &c.settings.osds, PoolReplicas: &c.settings.poolReplicas,
		PoolMinSize: &c.settings.poolMinSize, StartupTimeout: &c.settings.startupTimeout,
		MDSImage: &c.settings.mdsImage, RGWImage: &c.settings.rgwImage, PortLeases: len(c.portLeases), NetworkRemoved: &c.networkRemoved,
	}
}

// TestClientState returns a client identity's owner and lifecycle flags.
func (serviceHooks) TestClientState(client *ClientConfig) (owner *Container, created, ready, revoked bool) {
	return client.owner, client.created, client.ready, client.revoked
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
// opts applied, as Run does, and returns the resulting initial pool list and
// the prepared service bootstraps.
func (serviceHooks) TestPrepare(osds int, noInitialOSDs bool, pools []PoolConfig, opts ...Option) ([]PoolConfig, []serviceBootstrap, error) {
	settings := options{osds: osds, noInitialOSDs: noInitialOSDs, pools: pools}
	for _, opt := range opts {
		if err := opt(&settings); err != nil {
			return nil, nil, err
		}
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return nil, nil, err
	}
	return settings.pools, settings.services, nil
}
