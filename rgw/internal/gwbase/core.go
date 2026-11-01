package gwbase

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// Scope is the configured realm, zonegroup and zone of a gateway. Empty
// fields select the native defaults.
type Scope struct{ Realm, Zonegroup, Zone string }

// Core identifies one gateway to every operation that runs through it: the
// owning cluster, the gateway name, its configured scope, its listener and
// its container. It is fixed at construction.
type Core struct {
	cluster       *ceph.Container
	name          string
	scope         Scope
	port          int
	publicAddress string
	// container is the address of the gateway's exported Container field,
	// which callers may replace; operations read it when they run.
	container *testcontainers.Container
	// gateways reads the cluster's registered gateway cores in name order.
	gateways func() []*Core
}

// Params are the construction arguments of a Core. Package rgw passes the
// registry state the foundation needs instead of the foundation keeping its
// own copy.
type Params struct {
	// Cluster is the cluster that owns the gateway.
	Cluster *ceph.Container
	// Name is the normalized gateway name.
	Name string
	// Scope is the configured realm, zonegroup and zone.
	Scope Scope
	// Port is the gateway's HTTP port and PublicAddress the address a host
	// network gateway binds; both identify its listener in the service map.
	Port          int
	PublicAddress string
	// Container is the address of the gateway's exported Container field.
	Container *testcontainers.Container
	// Gateways reads the cluster's registered gateway cores in name order. It
	// runs with the cluster mutex held and takes no lock.
	Gateways func() []*Core
}

// NewCore returns the Core of one gateway. It does not register it.
func NewCore(p Params) *Core {
	return &Core{cluster: p.Cluster, name: p.Name, scope: p.Scope, port: p.Port, publicAddress: p.PublicAddress,
		container: p.Container, gateways: p.Gateways}
}

// Handle binds a gateway descriptor to its Core. It has only unexported
// members, so embedding it promotes nothing into a public API.
type Handle struct{ c *Core }

// NewHandle returns the handle of c.
func NewHandle(c *Core) Handle { return Handle{c: c} }

func (h Handle) core() *Core { return h.c }

// Owner is a type that embeds a Handle. It is sealed: only types that embed
// Handle satisfy it.
type Owner interface{ core() *Core }

// Of returns the Core of o; nil for a nil o or a descriptor that package rgw
// did not construct.
func Of(o Owner) *Core {
	if isNil(o) {
		return nil
	}
	return o.core()
}

// Origin binds a resource handle that a feature package creates, such as a
// placement, to the core of the gateway that created it. It has only
// unexported members, so embedding it promotes nothing into a public API.
type Origin struct{ c *Core }

// NewOrigin returns the origin of a handle that c created.
func NewOrigin(c *Core) Origin { return Origin{c: c} }

func (o Origin) origin() *Core { return o.c }

// OriginOwner is a type that embeds an Origin. It is sealed: only types that
// embed Origin satisfy it.
type OriginOwner interface{ origin() *Core }

// OriginOf returns the core of the gateway that created o; nil for a nil o or
// a handle that its feature package did not create.
func OriginOf(o OriginOwner) *Core {
	if isNil(o) {
		return nil
	}
	return o.origin()
}

// isNil reports whether v is nil or a nil pointer, whose promoted handle
// method would dereference it.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

// ServiceName is the cluster service name of the gateway named name.
func ServiceName(name string) string {
	if name == "default" {
		return "rgw"
	}
	return "rgw:" + name
}

// Cluster returns the owning cluster; nil for a nil Core.
func (c *Core) Cluster() *ceph.Container {
	if c == nil {
		return nil
	}
	return c.cluster
}

// Name returns the gateway name; empty for a nil Core.
func (c *Core) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// Scope returns the configured scope; zero for a nil Core.
func (c *Core) Scope() Scope {
	if c == nil {
		return Scope{}
	}
	return c.scope
}

// Container returns the gateway's current container; nil for a nil Core.
func (c *Core) Container() testcontainers.Container {
	if c == nil || c.container == nil {
		return nil
	}
	return *c.container
}

// SameCluster reports whether c and other are gateways of one cluster.
func (c *Core) SameCluster(other *Core) bool {
	return c != nil && other != nil && c.cluster != nil && c.cluster == other.cluster
}

// ServesScopeOf reports whether c belongs to the cluster of origin and serves
// its scope, so that it can run the operations of a handle that origin
// created.
func (c *Core) ServesScopeOf(origin *Core) bool {
	return c.SameCluster(origin) && c.scope == origin.scope
}

// ValidateAdmin refuses a gateway of a terminated cluster, or one that is no
// longer the cluster's service of its name. The caller holds the cluster
// mutex.
func (c *Core) ValidateAdmin() error {
	if servicehook.Closed(c.cluster) {
		return servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	service := servicehook.Service(c.cluster, ServiceName(c.name))
	ctr := c.Container()
	// DiagnosticHandle also refuses a typed nil, whose ID lookup would panic.
	if servicehook.DiagnosticHandle(ctr) == nil || servicehook.DiagnosticHandle(service) == nil || service.GetContainerID() != ctr.GetContainerID() {
		return servicehook.Invalid(errors.New("RGW gateway is no longer owned by this cluster"))
	}
	return nil
}

// AdminContext bounds a typed admin operation by the cluster's startup
// timeout. A handle calls it before it has resolved a gateway, so it takes the
// cluster. Gateway.Admin does not: its caller's ctx is the native command's
// budget.
func AdminContext(ctx context.Context, cluster *ceph.Container) (context.Context, context.CancelFunc) {
	if servicehook.StartupTimeout(cluster) > 0 {
		return context.WithTimeout(ctx, servicehook.StartupTimeout(cluster))
	}
	return ctx, func() {}
}

// ScopeGateway returns the first registered gateway, in name order, that
// serves the scope of c, still admits admin commands and whose container
// accepts Exec. A handle created through c runs its operations through it, so
// they keep working after c's gateway is removed if another one serves it. A
// stopped, paused or restarting gateway, or one whose startup failed after its
// container was published, stays registered, so it is skipped here instead of
// being chosen and failing the operation; the selection runs before any
// command, so no mutation is replayed on another gateway. The caller holds the
// owning cluster's mutex.
func (c *Core) ScopeGateway(ctx context.Context) (*Core, error) {
	if servicehook.Closed(c.cluster) {
		return nil, servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	ctx, cancel := AdminContext(ctx, c.cluster)
	defer cancel()
	var registered []*Core
	if c.gateways != nil {
		registered = c.gateways()
	}
	var stopped []string
	var inspectErr error
	for _, g := range registered {
		if g == nil || g.scope != c.scope || g.ValidateAdmin() != nil {
			continue
		}
		state, err := g.Container().State(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			inspectErr = errors.Join(inspectErr, fmt.Errorf("inspect RGW gateway %s: %w", g.name, err))
			continue
		}
		// Docker keeps Running true while a container is paused or restarting,
		// but refuses Exec in both states.
		if state != nil && state.Running && !state.Paused && !state.Restarting {
			return g, nil
		}
		stopped = append(stopped, g.name)
	}
	if len(stopped) > 0 || inspectErr != nil {
		return nil, errors.Join(fmt.Errorf("no RGW gateway that accepts commands serves this handle's scope (stopped, paused or restarting: %s)", strings.Join(stopped, ", ")), inspectErr)
	}
	return nil, servicehook.Invalid(errors.New("no registered RGW gateway serves this handle's scope"))
}
