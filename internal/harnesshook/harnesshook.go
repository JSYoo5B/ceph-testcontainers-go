// Package harnesshook is the typed internal contract between package ceph and
// the packages that compose it: the rbd, cephfs and rgw packages and their
// mirror packages (the error contract) and ceph/harness (the whole contract).
// Package ceph registers the implementation in internal/hookreg when it is
// initialized, so a caller imports ceph and never ceph's private names, and
// ceph imports none of its callers.
//
// The contract has three parts: the error construction and classification
// that the harness shares with the core instead of copying
// (docs/design/shared-harness-boundary.md, "Error handling"), the start of a
// cluster as a Backend, and the tenant Observer constructor. It adds no public
// API.
//
// Every method signature of the registered core uses only types that package
// ceph can spell, because ceph must not import this package: a type declared
// here cannot appear in a method that ceph implements. Backend and Ops are
// therefore interfaces over ceph, standard library and internal leaf types,
// which ceph satisfies structurally, and Mode is converted to int32 at the
// call.
package harnesshook

import (
	"context"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/sharedinvariants"
)

// Mode is how a SharedCluster lends a cluster. The values are those of the
// core's sharing gate.
type Mode int32

const (
	// NotShared is a cluster of its own test.
	NotShared Mode = iota
	// SharedWithTenants refuses every method that tenants cannot use.
	SharedWithTenants
	// SharedExclusively lends the cluster to one exclusive lease.
	SharedExclusively
)

// Prepare readies a cluster that runs a service before its baseline is taken.
type Prepare = func(ctx context.Context, c *ceph.Container) error

// Probe lists a service's resources, each mapped to its owning tenant ID.
type Probe = func(ctx context.Context, c *ceph.Container) (map[string]string, error)

// Backend is one started shared cluster. It carries no lock, field or
// executor of the cluster; the core implements it with unexported paths that
// the shared gate does not refuse.
type Backend interface {
	CreatePool(ctx context.Context, name string) error
	PrepareServices(ctx context.Context, prepare []Prepare) error
	Ready(ctx context.Context) error
	Invariants(ctx context.Context, pool string) (sharedinvariants.Invariants, error)
	Resources(ctx context.Context, probe []Probe) (map[string]string, error)
	DaemonsRunning(ctx context.Context) error
	CreateClient(ctx context.Context, entity string, caps ceph.ClientCaps) (client *ceph.ClientConfig, created bool, err error)
	DeleteClient(ctx context.Context, client *ceph.ClientConfig) error
	PurgeNamespace(ctx context.Context, pool, namespace string) error
	// SetSharing sets the gate; the argument is a Mode.
	SetSharing(mode int32)
	Terminate(ctx context.Context) error
	// Ops returns a value that satisfies the Ops interface.
	Ops() any
}

// Ops is what a Tenant and its tenant port may do in the cluster of a
// Backend. It exposes no lock, field or raw cluster: WithTopology runs a
// callback under the topology mutex and Extension is the service-state slot.
type Ops interface {
	StartupTimeout() time.Duration
	// Command runs a CLI command in the cluster without a liveness check.
	Command(ctx context.Context, args ...string) ([]byte, error)
	WithTopology(fn func() error) error
	Extension(kind string, create func() any) any
	RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
	WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
}

type core interface {
	Invalid(err error) error
	InvalidRequest(format string, args ...any) error
	Classify(kind, err error) error
	ClassifyDocker(ctx context.Context, err error) error
	// Start starts a cluster. On failure it still returns the partial
	// backend, which the caller owns and terminates.
	Start(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (any, error)
	// NewTenantObserver builds the tenant form of an Observer: target
	// resolves the leased cluster and refuses an expired lease, live
	// re-checks the same lease.
	NewTenantObserver(target func() (*ceph.Container, error), live func() error) *ceph.Observer
	// Lend returns the gated cluster of a Backend. Only the exclusive lease
	// and the tenant Observer call it, and the test-only cluster lookup.
	Lend(backend any) *ceph.Container
}

func get() core {
	c, ok := hookreg.Harness.(core)
	if !ok {
		panic("harnesshook: package ceph did not register its harness hooks")
	}
	return c
}

// Invalid marks err as a request that the fixture refused before any change,
// with ceph.ErrInvalidRequest. The result is an *ceph.OperationError whose
// cause is err itself, also when err is nil.
func Invalid(err error) error { return get().Invalid(err) }

// InvalidRequest is Invalid with a formatted cause.
func InvalidRequest(format string, args ...any) error {
	return get().InvalidRequest(format, args...)
}

// Classify wraps a non-nil err in an OperationError of kind.
func Classify(kind, err error) error { return get().Classify(kind, err) }

// ClassifyDocker classifies a failed Docker call as the core does.
func ClassifyDocker(ctx context.Context, err error) error { return get().ClassifyDocker(ctx, err) }

// Start starts a cluster and returns it as a Backend. A failed start returns
// the partial Backend, if any, together with the error.
func Start(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (Backend, error) {
	value, err := get().Start(ctx, image, opts)
	if value == nil {
		return nil, err
	}
	backend, ok := value.(Backend)
	if !ok {
		panic("harnesshook: package ceph started a backend that does not satisfy Backend")
	}
	return backend, err
}

// NewTenantObserver builds the tenant form of an Observer.
func NewTenantObserver(target func() (*ceph.Container, error), live func() error) *ceph.Observer {
	return get().NewTenantObserver(target, live)
}

// Lend returns the gated cluster of backend, or nil for a model that has none.
func Lend(backend Backend) *ceph.Container { return get().Lend(backend) }

// AsOps asserts that value, as Backend.Ops returns it, is an Ops. A nil value
// is a model without a cluster and gives nil.
func AsOps(value any) Ops {
	if value == nil {
		return nil
	}
	ops, ok := value.(Ops)
	if !ok {
		panic("harnesshook: package ceph returned operations that do not satisfy Ops")
	}
	return ops
}
