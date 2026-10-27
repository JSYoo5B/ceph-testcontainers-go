package ceph

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

// Tenant is one lease of a SharedCluster: the RADOS namespace ID of
// SharedPool and Cephx clients that can reach nothing else. Its release
// removes the clients and the namespace's objects. A Tenant never exposes
// the cluster's *Container; its methods fail once it is released.
type Tenant struct {
	owner         *SharedCluster
	inst          *sharedInstance
	id            string
	keepOnFailure bool
	// holder names the test that holds the lease.
	holder string
	// released is guarded by owner.mu.
	released bool

	mu      sync.Mutex
	client  *ClientConfig
	clients []*ClientConfig
	// scopes are the service resources attached to the tenant, released in
	// reverse order; releasing is set once the release has taken them.
	scopes    []tenantScope
	releasing bool
}

// tenantScope is a service resource of a tenant, such as an RBD namespace,
// a CephFS subvolume group or an RGW tenant. release removes it and lists
// what remains of it; an error is a removal or check that could not run.
type tenantScope struct {
	key     string
	release func(ctx context.Context) ([]string, error)
}

// TenantAccess selects what a client created by Tenant.CreateClient can do
// in its tenant's namespace.
type TenantAccess int

const (
	// TenantReadOnly reads objects of the tenant's namespace.
	TenantReadOnly TenantAccess = iota + 1
	// TenantReadWrite reads and writes them.
	TenantReadWrite
)

func (access TenantAccess) osdCap() (string, bool) {
	switch access {
	case TenantReadOnly:
		return "allow r", true
	case TenantReadWrite:
		return "allow rw", true
	}
	return "", false
}

// ID returns the tenant's identifier: "t" and 8 hex digits, valid as a RADOS
// or RBD namespace, a CephFS subvolume group, an RGW tenant and part of a
// client name.
func (t *Tenant) ID() string { return t.id }

// Pool returns the pool that holds the tenant's namespace, SharedPool.
func (t *Tenant) Pool() string { return SharedPool }

// Namespace returns the tenant's RADOS namespace, its ID.
func (t *Tenant) Namespace() string { return t.id }

// Client returns the tenant's client, client.ID, with mon 'allow r' and osd
// 'allow rw pool=SharedPool namespace=ID'.
func (t *Tenant) Client() *ClientConfig {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.client
}

// CreateClient creates client.ID-name with mon 'allow r' and the access to
// the tenant's namespace only. The release deletes it.
func (t *Tenant) CreateClient(ctx context.Context, name string, access TenantAccess) (*ClientConfig, error) {
	name = strings.TrimPrefix(name, "client.")
	if !clientIDPattern.MatchString(name) {
		return nil, invalidRequest("tenant client name must use letters, digits, dots, underscores or hyphens")
	}
	return t.newClient(ctx, t.id+"-"+name, access)
}

func (t *Tenant) newClient(ctx context.Context, id string, access TenantAccess) (*ClientConfig, error) {
	osd, ok := access.osdCap()
	if !ok {
		return nil, invalidRequest("unknown tenant access %d", access)
	}
	return t.createClient(ctx, id, ClientCaps{Mon: "allow r", OSD: fmt.Sprintf("%s pool=%s namespace=%s", osd, SharedPool, t.id)})
}

// serviceClient creates client.ID-suffix with caps that a service package
// limited to the tenant's resources. The release deletes it.
func (t *Tenant) serviceClient(ctx context.Context, suffix string, caps ClientCaps) (*ClientConfig, error) {
	if !clientIDPattern.MatchString(t.id + "-" + suffix) {
		return nil, invalidRequest("tenant client name must use letters, digits, dots, underscores or hyphens")
	}
	return t.createClient(ctx, t.id+"-"+suffix, caps)
}

func (t *Tenant) createClient(ctx context.Context, id string, caps ClientCaps) (*ClientConfig, error) {
	if err := t.check(); err != nil {
		return nil, err
	}
	client, err := t.inst.backend.createClient(ctx, "client."+id, caps)
	if client != nil && client.created {
		t.mu.Lock()
		t.clients = append(t.clients, client)
		t.mu.Unlock()
	}
	return client, err
}

// WithClient attaches a container to the cluster with the tenant's client:
// ceph.conf, which names it, and its keyring, and no admin keyring. The
// container is labeled as the tenant's, so RunNativeCommand accepts it. The
// caller terminates it.
func (t *Tenant) WithClient(opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return t.WithClientIdentity(t.Client(), opts...)
}

// WithClientIdentity attaches a container like WithClient with client, the
// tenant's own client or one its CreateClient created.
func (t *Tenant) WithClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c, err := t.cluster()
		if err != nil {
			return err
		}
		if !t.owns(client) {
			return invalidRequest("client is not one of tenant %s", t.id)
		}
		if err := c.withClientIdentity(client, opts...)(req); err != nil {
			return err
		}
		return testcontainers.WithLabels(map[string]string{sharedTenantLabel: t.id})(req)
	}
}

func (t *Tenant) owns(client *ClientConfig) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, owned := range t.clients {
		if owned == client {
			return true
		}
	}
	return false
}

// RunNativeCommand runs command as Container.RunNativeCommand does, only in
// a container that the tenant's WithClient or WithClientIdentity attached.
func (t *Tenant) RunNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error) {
	c, err := t.cluster()
	if err != nil {
		return NativeCommandResult{}, err
	}
	if target == nil || target.GetContainerID() == "" {
		return NativeCommandResult{}, invalidRequest("tenant native command needs a started container")
	}
	info, err := target.Inspect(ctx)
	if err != nil {
		return NativeCommandResult{}, fmt.Errorf("inspect the tenant's container: %w", classifyDocker(ctx, err))
	}
	if info.Config == nil || info.Config.Labels[sharedTenantLabel] != t.id {
		return NativeCommandResult{}, invalidRequest("tenant %s runs native commands only in containers that its WithClient attached", t.id)
	}
	return c.runNativeCommand(ctx, target, command)
}

// Status returns the cluster's status; see Container.Status.
func (t *Tenant) Status(ctx context.Context) (Status, error) {
	c, err := t.cluster()
	if err != nil {
		return Status{}, err
	}
	return c.status(ctx)
}

// HealthDetails returns the cluster's health; see Container.HealthDetails.
func (t *Tenant) HealthDetails(ctx context.Context) (HealthSnapshot, error) {
	c, err := t.cluster()
	if err != nil {
		return HealthSnapshot{}, err
	}
	return c.healthDetails(ctx)
}

// Pools lists the cluster's pools; see Container.Pools.
func (t *Tenant) Pools(ctx context.Context) ([]PoolState, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return c.pools(ctx)
}

// PoolStatus returns one pool's state by its exact native name; see
// Pool.Status.
func (t *Tenant) PoolStatus(ctx context.Context, name string) (PoolState, error) {
	c, err := t.cluster()
	if err != nil {
		return PoolState{}, err
	}
	return c.poolStatus(ctx, name)
}

// PoolUsage returns one pool's usage by its exact native name; see
// Pool.Usage. Usage of
// SharedPool includes the other tenants' objects.
func (t *Tenant) PoolUsage(ctx context.Context, name string) (PoolUsageSnapshot, error) {
	c, err := t.cluster()
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	return c.poolUsage(ctx, name)
}

// ObjectWatchers lists an object's watchers; see Container.ObjectWatchers.
func (t *Tenant) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return c.objectWatchers(ctx, pool, namespace, object)
}

// OSDFlags lists the global OSD flags; see Container.OSDFlags.
func (t *Tenant) OSDFlags(ctx context.Context) ([]OSDFlag, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return c.osdFlags(ctx)
}

// BlocklistEntries lists the blocklist; see Container.BlocklistEntries.
func (t *Tenant) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c, err := t.cluster()
	if err != nil {
		return nil, err
	}
	return c.blocklistEntries(ctx)
}

func (t *Tenant) check() error {
	t.owner.mu.Lock()
	gone := t.released || t.inst.terminated
	t.owner.mu.Unlock()
	t.mu.Lock()
	gone = gone || t.releasing
	t.mu.Unlock()
	if gone {
		return invalidRequest("tenant %s is released", t.id)
	}
	return nil
}

// attachScope registers a service resource that the tenant's release
// removes. A service attaches it before it creates the resource, so that the
// release also removes a partial creation. Each key is attached once.
func (t *Tenant) attachScope(key string, release func(ctx context.Context) ([]string, error)) error {
	if err := t.check(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.releasing {
		return invalidRequest("tenant %s is released", t.id)
	}
	for _, scope := range t.scopes {
		if scope.key == key {
			return invalidRequest("tenant %s already holds %s", t.id, key)
		}
	}
	t.scopes = append(t.scopes, tenantScope{key: key, release: release})
	return nil
}

// takeScopes ends attachments and returns the scopes to release.
func (t *Tenant) takeScopes() []tenantScope {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.releasing = true
	return slices.Clone(t.scopes)
}

func (t *Tenant) cluster() (clusterOps, error) {
	if err := t.check(); err != nil {
		return nil, err
	}
	c := t.inst.backend.ops()
	if c == nil {
		return nil, invalidRequest("tenant %s has no container", t.id)
	}
	return c, nil
}
