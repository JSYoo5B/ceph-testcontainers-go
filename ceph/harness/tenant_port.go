package harness

import (
	"context"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// tenantPort is what the service harnesses may do for one tenant: check that
// it is live, create its scoped clients, attach scopes that its release
// removes, and run bounded CLI commands in its cluster. It does not return the
// cluster. internal/servicehook names the same method set as an alias, so the
// service harnesses depend on this port and not on *Tenant.
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
	CreateClient(ctx context.Context, suffix string, caps ceph.ClientCaps) (*ceph.ClientConfig, error)
	// AttachScope registers a service resource that the tenant's release
	// removes, in reverse order and before its clients are deleted.
	AttachScope(key string, release func(ctx context.Context) ([]string, error)) error
	// RequireService fails with ErrInvalidRequest when the shared cluster
	// was not given the support of kind, so that a service Lease fails
	// before it creates anything that no probe would attribute.
	RequireService(kind string) error
	WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
	// WithTopology runs fn while it holds the cluster mutex, the one that
	// serializes fixture mutations, and returns fn's error. fn must not call
	// WithTopology again.
	WithTopology(fn func() error) error
	// Extension returns the service state that the cluster keeps for kind,
	// created by create on first use. fn of WithTopology is the only caller
	// that may use it, because the state is guarded by the cluster mutex.
	Extension(kind string, create func() any) any
}

// tenantLease is the tenantPort of one tenant. It holds the cluster operations
// that the tenant had when the port was taken, the same ones that the commands
// of an adapter used before the port existed.
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

func (l tenantLease) CreateClient(ctx context.Context, suffix string, caps ceph.ClientCaps) (*ceph.ClientConfig, error) {
	return l.t.serviceClient(ctx, suffix, caps)
}

func (l tenantLease) AttachScope(key string, release func(ctx context.Context) ([]string, error)) error {
	return l.t.attachScope(key, release)
}

func (l tenantLease) RequireService(kind string) error { return l.t.requireService(kind) }

func (l tenantLease) WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption {
	return l.t.WithClientIdentity(client, opts...)
}

func (l tenantLease) WithTopology(fn func() error) error { return l.c.withTopology(fn) }

func (l tenantLease) Extension(kind string, create func() any) any {
	return l.c.extension(kind, create)
}

// tenantHooks is the harness side of the internal seams of internal/servicehook
// and internal/testhelper. Its methods are exported only so that those
// packages can call them through interfaces; the type is unexported and
// appears in no public package. It is not service registration: it holds no
// service state, and each consumer asserts only the method it uses.
type tenantHooks struct{}

func init() {
	hookreg.HarnessTenant = tenantHooks{}
}

// TenantPort returns the port of a live tenant, or the error that says it is
// released.
func (tenantHooks) TenantPort(t *Tenant) (tenantPort, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return tenantLease{t: t, c: c}, nil
}

// NewServiceSupport builds the opaque support of one service from the
// callbacks that the service harness supplies.
func (tenantHooks) NewServiceSupport(kind string, prepare func(context.Context, *ceph.Container) error, probe func(context.Context, *ceph.Container) (map[string]string, error)) ServiceSupport {
	return ServiceSupport{service: sharedService{kind: kind, prepare: prepare, probe: probe}}
}

// TenantCluster returns the shared cluster of a live tenant, whose gate still
// refuses the methods a tenant cannot use, or nil. Only the unit tests and the
// native gate and release tests use it; the service harnesses use the tenant
// port.
func (tenantHooks) TenantCluster(t *Tenant) *ceph.Container {
	c, err := t.cluster()
	if err != nil {
		return nil
	}
	return c.raw()
}
