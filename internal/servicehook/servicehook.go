// Package servicehook gives the rbd, cephfs and rgw packages the parts of a
// ceph.Container that are not public API, such as its mutex and Run-time
// provisioning. Package ceph registers them in internal/hookreg when it is
// initialized.
package servicehook

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
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
	Invalid(error) error
	Lock(c *ceph.Container)
	Unlock(c *ceph.Container)
	TryLock(c *ceph.Container) bool
	StartupTimeout(c *ceph.Container) time.Duration
	ClientCommand(ctx context.Context, c *ceph.Container, args ...string) ([]byte, error)
	CLI(c *ceph.Container) testcontainers.Container
	Closed(c *ceph.Container) bool
	Command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error)
	RunOwnedShell(ctx context.Context, c *ceph.Container, target testcontainers.Container, label string, limit int, argv ...string) (supervised.ShellReport, error)
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
	Extension(c *ceph.Container, kind string, create func() any) any
	ResolvePool(c *ceph.Container, config ceph.PoolConfig) ceph.PoolConfig
	NormalizePool(config ceph.PoolConfig) (ceph.PoolConfig, error)
	PoolPlacementDomains(c *ceph.Container, config ceph.PoolConfig) int
	PoolPolicyReady(c *ceph.Container) error
	RemovePool(ctx context.Context, c *ceph.Container, name string, skipForeign bool) error
	PoolMissing(err error) bool
	CephBefore(c *ceph.Container, major int) bool
	NativeFailure(program string, code int, stdout, stderr []byte) error
	ClassifyExec(ctx context.Context, err error) error
	AuthCommand(ctx context.Context, c *ceph.Container, operation string, args ...string) ([]byte, error)
	ClusterIdentity(ctx context.Context, c *ceph.Container) (string, bool, error)
	MonitorAddresses(status ceph.QuorumStatus) (string, error)
	ClientConfirmed(client *ceph.ClientConfig, c *ceph.Container) bool
	ClientKeyring(client *ceph.ClientConfig) []byte
	ClientRevoked(client *ceph.ClientConfig) bool
	SetClientRevoked(client *ceph.ClientConfig, revoked bool)
	SettleClient(ctx context.Context, c *ceph.Container, client *ceph.ClientConfig) error
}

func get() hooks {
	h, ok := hookreg.Value.(hooks)
	if !ok {
		panic("servicehook: package ceph did not register its hooks")
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

// RunOwnedShell runs argv in target, a container of c that the caller holds
// the cluster mutex for, under the shell supervisor with limit seconds (0
// for none), and tracks it until it is known to have ended: when ctx ends
// first the command is stopped before RunOwnedShell returns, and a stop that
// cannot be confirmed returns a *ceph.NativeCommandUnstoppedError.
func RunOwnedShell(ctx context.Context, c *ceph.Container, target testcontainers.Container, label string, limit int, argv ...string) (supervised.ShellReport, error) {
	return get().RunOwnedShell(ctx, c, target, label, limit, argv...)
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

// ServiceImage returns the image selected for an image role: rgw or cephfs.
func ServiceImage(c *ceph.Container, role string) string { return get().ServiceImage(c, role) }

// Extension returns the per-cluster state a service package keeps under
// kind, storing the result of create on first use. It takes its own lock, so
// callers may or may not hold the cluster mutex.
func Extension(c *ceph.Container, kind string, create func() any) any {
	return get().Extension(c, kind, create)
}

// ResolvePool applies the cluster's pool defaults: CRUSH root, replicas and
// minimum size.
func ResolvePool(c *ceph.Container, config ceph.PoolConfig) ceph.PoolConfig {
	return get().ResolvePool(c, config)
}

// NormalizePool validates a pool configuration and fills its defaults.
func NormalizePool(config ceph.PoolConfig) (ceph.PoolConfig, error) {
	return get().NormalizePool(config)
}

// PoolPlacementDomains counts the owned OSD placement domains that config
// can use. The caller holds the cluster mutex.
func PoolPlacementDomains(c *ceph.Container, config ceph.PoolConfig) int {
	return get().PoolPlacementDomains(c, config)
}

// PoolPolicyReady reports whether the cluster's pool policy accepts changes.
// The caller holds the cluster mutex.
func PoolPolicyReady(c *ceph.Container) error { return get().PoolPolicyReady(c) }

// RemovePool removes a fixture-created pool and its rule and profile. With
// skipForeign, a pool the fixture did not create is left alone. The caller
// holds the cluster mutex.
func RemovePool(ctx context.Context, c *ceph.Container, name string, skipForeign bool) error {
	return get().RemovePool(ctx, c, name, skipForeign)
}

// PoolMissing reports a RemovePool error for a name with nothing to remove.
func PoolMissing(err error) bool { return get().PoolMissing(err) }

// CephBefore reports whether the recorded Ceph release is older than major.
func CephBefore(c *ceph.Container, major int) bool { return get().CephBefore(c, major) }

// NativeFailure describes a command that exited nonzero, as the module's own
// commands do: a *ceph.NativeFailure read from its error output, or from its
// standard output when stdout is not nil and it wrote no error output.
func NativeFailure(program string, code int, stdout, stderr []byte) error {
	return get().NativeFailure(program, code, stdout, stderr)
}

// ClassifyExec classifies a failed Docker exec of a native command as the
// module's own commands do: matching ctx's error when ctx ended, unclassified
// for a container that is gone or stopped, since the command may have run,
// and the engine's failure otherwise.
func ClassifyExec(ctx context.Context, err error) error { return get().ClassifyExec(ctx, err) }

// AuthCommand runs a Cephx command with the admin identity and reports a
// failure by operation without the native output.
func AuthCommand(ctx context.Context, c *ceph.Container, operation string, args ...string) ([]byte, error) {
	return get().AuthCommand(ctx, c, operation, args...)
}

// ClusterIdentity returns the FSID of the bootstrap configuration and whether
// the configuration and admin keyring were confirmed. The error is only ctx's.
func ClusterIdentity(ctx context.Context, c *ceph.Container) (string, bool, error) {
	return get().ClusterIdentity(ctx, c)
}

// ClientConfirmed reports a client identity that c created and confirmed.
func ClientConfirmed(client *ceph.ClientConfig, c *ceph.Container) bool {
	return get().ClientConfirmed(client, c)
}

// ClientKeyring returns the keyring retrieved when the client was created.
func ClientKeyring(client *ceph.ClientConfig) []byte { return get().ClientKeyring(client) }

// ClientRevoked reports a client identity known to be deleted.
func ClientRevoked(client *ceph.ClientConfig) bool { return get().ClientRevoked(client) }

// SetClientRevoked records whether a service deleted the client identity.
// The caller holds the cluster mutex.
func SetClientRevoked(client *ceph.ClientConfig, revoked bool) {
	get().SetClientRevoked(client, revoked)
}

// SettleClient makes sure that no earlier auth command of the module for the
// client's identity can still change it, and refuses a descriptor whose key
// was replaced or whose identity has a change that is not resolved. The
// caller holds the cluster mutex.
func SettleClient(ctx context.Context, c *ceph.Container, client *ceph.ClientConfig) error {
	return get().SettleClient(ctx, c, client)
}

// MonitorAddresses returns the bootstrap addresses of the monitors in quorum.
func MonitorAddresses(status ceph.QuorumStatus) (string, error) {
	return get().MonitorAddresses(status)
}

// ClientCommand executes feature tools in the selected toolkit runtime.
func ClientCommand(ctx context.Context, c *ceph.Container, args ...string) ([]byte, error) {
	return get().ClientCommand(ctx, c, args...)
}

// Invalid classifies a refused service request without losing its cause.
func Invalid(err error) error { return get().Invalid(err) }
