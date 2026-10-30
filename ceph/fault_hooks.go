package ceph

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/testcontainers/testcontainers-go"
)

// faultHooks gives internal/faulthook the fault registry and the private
// cluster helpers that fault operations need. Its methods are exported only
// so that internal/faulthook can call them through an interface; the type
// itself is unexported and never appears in a public package.
type faultHooks struct{}

func init() {
	hookreg.Fault = faultHooks{}
}

// faultKindNames are the kinds that internal/faulthook names. The phase,
// overlap refusal and Terminate message of each stay fixed in faultKinds.
var faultKindNames = map[string]faultKind{
	"NetworkDelay":        faultNetworkDelay,
	"NetworkInterruption": faultNetworkInterruption,
	"ContainerPause":      faultContainerPause,
	"Blocklist":           faultBlocklist,
	"FullRatios":          faultFullRatios,
}

var errFaultToken = errors.New("fault registry token is not one that a cluster issued")

// Reserve reserves key for a fault of kind. The caller holds the cluster
// mutex and has refused a closed cluster. A refusal may return the previous
// handle, which still needs its undo.
func (faultHooks) Reserve(ctx context.Context, c *Container, kind, key string) (any, any, error) {
	k, ok := faultKindNames[kind]
	if !ok {
		return nil, nil, invalidRequest("unknown fault kind %q", kind)
	}
	token, previous, err := c.faults.reserve(ctx, k, key)
	if err != nil {
		return nil, previous, err
	}
	return token, nil, nil
}

// Settle records handle for a reservation, or ends it when handle is nil,
// and reports whether Terminate has started. The caller holds the mutex.
func (faultHooks) Settle(c *Container, token any, network string, handle any, undo func(context.Context) error, complete func(context.Context) (bool, error)) (bool, error) {
	entry, ok := token.(*faultEntry)
	if !ok || entry == nil {
		return false, errFaultToken
	}
	c.faults.settle(entry, network, handle, undo, complete)
	return c.closed, nil
}

// Release forgets the fault of token while its key still maps to it. The
// caller holds the mutex.
func (faultHooks) Release(c *Container, token any) bool {
	entry, _ := token.(*faultEntry)
	return c.faults.release(entry)
}

// Tracks reports whether token is still the fault of its key. The caller
// holds the mutex.
func (faultHooks) Tracks(c *Container, token any) bool {
	entry, _ := token.(*faultEntry)
	return c.faults.tracks(entry)
}

// WithTopology runs fn with the cluster mutex held, unless ctx ends first.
func (faultHooks) WithTopology(ctx context.Context, c *Container, fn func() error) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	return fn()
}

// WithTopologyNoContext runs fn with the cluster mutex held, waiting for it
// without a context as override.AddBlocklist and lease releases do.
func (faultHooks) WithTopologyNoContext(c *Container, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn()
}

// Closed reports a terminated cluster. The caller holds the mutex.
func (faultHooks) Closed(c *Container) bool { return c.closed }

// Owns reports a container of the cluster's own daemons. The caller holds
// the mutex.
func (faultHooks) Owns(c *Container, id string) bool { return c.ownsNativeTarget(id) }

// OwnedLabel labels a helper container as owned by the cluster, so that
// Terminate removes it when it leaked.
func (faultHooks) OwnedLabel(c *Container) testcontainers.CustomizeRequestOption {
	return c.startup.ownedLabel()
}

// Control returns the container that runs the control CLI.
func (faultHooks) Control(ctx context.Context, c *Container) (testcontainers.Container, error) {
	return c.conn().controlContainer(ctx)
}

// CLI runs a Ceph CLI command with the admin identity.
func (faultHooks) CLI(ctx context.Context, c *Container, args ...string) ([]byte, error) {
	return c.conn().ceph(ctx, args...)
}

// ClusterIdentity returns the bootstrap FSID and whether the configuration
// and admin keyring were confirmed. The error is only ctx's.
func (faultHooks) ClusterIdentity(ctx context.Context, c *Container) (string, bool, error) {
	return serviceHooks{}.ClusterIdentity(ctx, c)
}

// PoolPolicyReady refuses a terminated cluster or one without a control
// container. The caller holds the mutex.
func (faultHooks) PoolPolicyReady(c *Container) error { return c.poolPolicyReady() }

// PoolState reads one pool. The caller holds the mutex.
func (faultHooks) PoolState(ctx context.Context, c *Container, name string) (PoolState, error) {
	return poolCatalog{c.conn()}.state(ctx, name)
}

// CheckPoolID refuses a pool that was replaced since expected was read.
func (faultHooks) CheckPoolID(ctx context.Context, c *Container, expected PoolState) error {
	return poolCatalog{c.conn()}.checkID(ctx, expected)
}

// OwnedOSD reports an OSD that the cluster owns and has not purged. The
// caller holds the mutex.
func (faultHooks) OwnedOSD(c *Container, id int) bool {
	osd := c.osdHandles.get(id)
	return osd != nil && !osd.purged
}

// BlueStore reports BlueStore OSDs.
func (faultHooks) BlueStore(c *Container) bool { return c.settings.objectStore() == OSDBlueStore }

// StartupTimeout bounds one fixture operation.
func (faultHooks) StartupTimeout(c *Container) time.Duration { return c.settings.startupTimeout }

// Terminated returns the error of a fault that Terminate overtook and the
// error of an operation on a terminated cluster.
func (faultHooks) Terminated() (native, cluster error) {
	return errNativeTerminated, errClusterTerminated
}

// ClassifyDocker classifies a failed Docker call.
func (faultHooks) ClassifyDocker(ctx context.Context, err error) error {
	return classifyDocker(ctx, err)
}

// NativeFailure describes a native command that exited nonzero.
func (faultHooks) NativeFailure(program string, code int, stdout, stderr []byte) error {
	return nativeFailure(program, code, stdout, stderr)
}

// OnlyMissingHostResource reports an error that only says a container or
// network is already gone.
func (faultHooks) OnlyMissingHostResource(err error) bool { return onlyMissingHostResource(err) }
