package gwbase

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// LockCluster takes the topology mutex of cluster unless ctx ends first. It
// is how every RGW operation except Gateway.Admin takes the mutex, either
// directly or through Admit and AdmitCreation; Gateway.Admin holds it for its
// admission only. Release it with servicehook.Unlock.
func LockCluster(ctx context.Context, cluster *ceph.Container) error {
	return servicehook.LockTopology(ctx, cluster)
}

// Admission is an admitted typed operation: the gateway that runs its native
// commands and the context they run under, whose admin budget (AdminContext)
// starts once the topology mutex is held and the gateway is admitted, so a
// wait for the mutex never consumes it. Done ends the budget and, for an
// admission that took the mutex, releases it.
type Admission struct {
	Gateway *Core
	Ctx     context.Context
	cancel  context.CancelFunc
	unlock  func()
}

// Done ends the admission.
func (a *Admission) Done() {
	a.cancel()
	if a.unlock != nil {
		a.unlock()
	}
}

// Admit admits a typed operation on a handle that origin created: it takes
// the topology mutex of origin's cluster, selects the gateway that serves
// origin's scope (ScopeGateway) and starts the admin budget. On an error it
// holds nothing.
func Admit(ctx context.Context, origin *Core) (*Admission, error) {
	return lockAndAdmit(ctx, origin, origin.AdmitLocked)
}

// AdmitCreation admits a typed operation that creates a resource through the
// gateway of c: it takes the topology mutex of c's cluster, validates c
// (ValidateAdmin) and starts the admin budget. On an error it holds nothing.
func AdmitCreation(ctx context.Context, c *Core) (*Admission, error) {
	return lockAndAdmit(ctx, c, c.AdmitCreationLocked)
}

func lockAndAdmit(ctx context.Context, c *Core, admit func(context.Context) (*Admission, error)) (*Admission, error) {
	if c.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	if err := LockCluster(ctx, c.Cluster()); err != nil {
		return nil, err
	}
	a, err := admit(ctx)
	if err != nil {
		servicehook.Unlock(c.Cluster())
		return nil, err
	}
	a.unlock = func() { servicehook.Unlock(c.Cluster()) }
	return a, nil
}

// AdmitLocked is Admit for a caller that already holds the topology mutex,
// for example because it settled earlier work under it first. Done does not
// release the mutex.
func (c *Core) AdmitLocked(ctx context.Context) (*Admission, error) {
	g, err := c.ScopeGateway(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := AdminContext(ctx, c.cluster)
	return &Admission{Gateway: g, Ctx: ctx, cancel: cancel}, nil
}

// AdmitCreationLocked is AdmitCreation for a caller that already holds the
// topology mutex. Done does not release the mutex.
func (c *Core) AdmitCreationLocked(ctx context.Context) (*Admission, error) {
	if err := c.ValidateAdmin(); err != nil {
		return nil, err
	}
	ctx, cancel := AdminContext(ctx, c.cluster)
	return &Admission{Gateway: c, Ctx: ctx, cancel: cancel}, nil
}

// RequireScope is the admission of an operation on a resource that was
// created in native scope want: it reads the gateway's current runtime scope
// and refuses, with changed as the error, a gateway that no longer runs in
// want. The caller holds the owning cluster's mutex.
func (g *Core) RequireScope(ctx context.Context, want NativeScope, changed string) (NativeScope, error) {
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return NativeScope{}, err
	}
	if scope != want {
		return NativeScope{}, errors.New(changed)
	}
	return scope, nil
}
