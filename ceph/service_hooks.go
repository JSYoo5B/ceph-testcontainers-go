package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
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
// still refuses the methods a tenant cannot use, or nil. Only the native
// gate and release tests use it; the service adapters use the tenant port.
func (serviceHooks) TenantCluster(t *Tenant) *Container {
	c, err := t.cluster()
	if err != nil {
		return nil
	}
	return c.raw()
}

// container returns the gated shared cluster of a live tenant for the
// Observer, which stays in package ceph and reads through the cluster's own
// gate and locks. It is the only non-test path from a Tenant to a *Container.
func (t *Tenant) container() (*Container, error) {
	ops, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return ops.raw(), nil
}

// tenantPort is what the service adapters may do for one tenant: check that
// it is live, create its scoped clients, attach scopes that its release
// removes, and run bounded CLI commands in its cluster. It does not return the
// cluster. internal/servicehook names the same method set as an alias, so the
// adapters depend on this port and not on *Tenant.
type tenantPort = interface {
	ID() string
	// Live fails once the tenant is released or its cluster terminated.
	Live() error
	StartupTimeout() time.Duration
	// Command runs a CLI command in the tenant's cluster. It does not check
	// liveness: a scope's release runs it after the tenant stopped
	// admitting.
	Command(ctx context.Context, args ...string) ([]byte, error)
	// CreateClient creates client.ID-suffix with caps that the service
	// limited to the tenant's resources. The tenant's release deletes it.
	CreateClient(ctx context.Context, suffix string, caps ClientCaps) (*ClientConfig, error)
	// AttachScope registers a service resource that the tenant's release
	// removes, in reverse order and before its clients are deleted.
	AttachScope(key string, release func(ctx context.Context) ([]string, error)) error
	WithClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption
	// WithTopology runs fn while it holds the cluster mutex, the one that
	// serializes fixture mutations, and returns fn's error. fn must not call
	// WithTopology again.
	WithTopology(fn func() error) error
	// Extension returns the service state that the cluster keeps for kind,
	// created by create on first use. fn of WithTopology is the only caller
	// that may use it, because the state is guarded by the cluster mutex.
	Extension(kind string, create func() any) any
}

// tenantLease is the tenantPort of one tenant. It holds the cluster that the
// tenant had when the port was taken, the same one that the commands of an
// adapter used before the port existed.
type tenantLease struct {
	t *Tenant
	c clusterOps
}

func (l tenantLease) ID() string { return l.t.id }

func (l tenantLease) Live() error { return l.t.check() }

func (l tenantLease) StartupTimeout() time.Duration { return l.c.startupTimeout() }

func (l tenantLease) Command(ctx context.Context, args ...string) ([]byte, error) {
	return l.c.command(ctx, args...)
}

func (l tenantLease) CreateClient(ctx context.Context, suffix string, caps ClientCaps) (*ClientConfig, error) {
	return l.t.serviceClient(ctx, suffix, caps)
}

func (l tenantLease) AttachScope(key string, release func(ctx context.Context) ([]string, error)) error {
	return l.t.attachScope(key, release)
}

func (l tenantLease) WithClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return l.t.WithClientIdentity(client, opts...)
}

func (l tenantLease) WithTopology(fn func() error) error {
	return l.c.withTopology(fn)
}

func (l tenantLease) Extension(kind string, create func() any) any {
	return l.c.extension(kind, create)
}

// TenantPort returns the port of a live tenant, or the error that says it is
// released.
func (serviceHooks) TenantPort(t *Tenant) (tenantPort, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return tenantLease{t: t, c: c}, nil
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

// ServiceImage returns the image selected for an image role: client, rbd-aux,
// essential, rgw or cephfs.
func (serviceHooks) ServiceImage(c *Container, role string) string {
	switch role {
	case "client":
		return c.staticFacts().Images.Client
	case "rbd-aux":
		return c.staticFacts().Images.RBDAux
	case "essential":
		return c.staticFacts().Images.Essential
	case "rgw":
		return c.staticFacts().Images.RGW
	case "cephfs":
		return c.staticFacts().Images.CephFS
	}
	return ""
}

// WithServiceImage selects the image of the rgw or cephfs role. It replaces
// that role of any ImageSet, whatever the option order.
func (serviceHooks) WithServiceImage(role, image string) Option {
	return func(o *options) error {
		if strings.TrimSpace(image) == "" {
			return fmt.Errorf("%s image must not be blank", strings.ToUpper(role))
		}
		switch role {
		case "rgw":
			o.rgwOverride = image
		case "cephfs":
			o.cephfsOverride = image
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
	return c.removePool(ctx, name, skipForeign, nil)
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
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return "", false, err
	}
	return identity.fsid, identity.err == nil && identity.confirmed, nil
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
		client.owner.authRecords.confirmKey(client.name, "")
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
