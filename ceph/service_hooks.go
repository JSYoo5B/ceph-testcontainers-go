package ceph

import (
	"context"
	"errors"
	"fmt"
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

func (serviceHooks) CLI(c *Container) testcontainers.Container { return c.conn().cliContainer() }

// Closed reports a terminated cluster.
func (serviceHooks) Closed(c *Container) bool { return c.closed }

// Command runs a CLI command in ctr and returns its standard output.
func (serviceHooks) Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	return command(ctx, ctr, args...)
}

// RunOwnedShell runs argv in target, a container of c that the caller
// admitted while it holds the cluster mutex, under the shell supervisor with
// limit seconds (0 for none). The invocation is tracked like a
// RunNativeCommand's: when ctx ends first, the command is stopped before
// RunOwnedShell returns, and a stop that cannot be confirmed returns a
// NativeCommandUnstoppedError, whose Stop and Terminate retry it. Terminate
// cancels a running call. label names the command in errors.
func (serviceHooks) RunOwnedShell(ctx context.Context, c *Container, target testcontainers.Container, label string, limit int, argv ...string) (supervised.ShellReport, error) {
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	inv := &nativeInvocation{owner: c, target: target, label: label, cancel: cancel, stopCommand: supervised.CancelShell, done: make(chan struct{})}
	// An owned shell has no target instance, which no fence can name, so the
	// fence check of admit never refuses it.
	if c.nativeCmds.admit(inv, nil) == nativeRefusedClosed {
		return supervised.ShellReport{}, fmt.Errorf("%s: %w", label, errClusterTerminated)
	}
	report, err := supervised.RunShell(runCtx, target, label, limit, argv...)
	unstopped, _ := supervised.Unconfirmed(err)
	c.nativeCmds.finish(inv, unstopped, unstopped != "")
	close(inv.done)
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(runCtx), errNativeTerminated) {
		err = fmt.Errorf("%s: %w: %w", label, errNativeTerminated, err)
	}
	err = safeNativeError(err)
	if unstopped != "" {
		return supervised.ShellReport{}, &NativeCommandUnstoppedError{invocation: inv, err: err}
	}
	return report, err
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
	return c.roster.service(name)
}

// SetService records a service container the cluster owns and terminates.
// The caller holds the cluster mutex.
func (serviceHooks) SetService(c *Container, name string, ctr testcontainers.Container) {
	c.roster.setService(name, ctr)
}

// DeleteService forgets a service container. The caller holds the mutex.
func (serviceHooks) DeleteService(c *Container, name string) { c.roster.dropService(name) }

// Services returns a copy of the service containers. The caller holds the
// cluster mutex.
func (serviceHooks) Services(c *Container) map[string]testcontainers.Container {
	return c.roster.serviceCopy()
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

// Extension returns the per-cluster state a service package keeps under
// kind, storing the result of create on first use. It takes its own lock, so
// callers may or may not hold the cluster mutex.
func (serviceHooks) Extension(c *Container, kind string, create func() any) any {
	return c.extensions.getOrCreate(kind, create)
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

// Invalid classifies a refused request from a service package.
func (serviceHooks) Invalid(err error) error { return newInvalid(err) }
