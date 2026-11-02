package ceph

import "context"

// WaitForClean waits until a manager is available, the native OSD count
// equals the owned OSD count with every OSD up and in, at least one PG exists
// and every PG is exactly active+clean. HEALTH_WARN alone does not block it.
// PG states with extra flags, such as active+clean+scrubbing, do not count. It
// never succeeds while an owned OSD is intentionally stopped or out; use
// WaitForPGClean then. The cluster's startup timeout also bounds the wait.
//
// Call it after creating pools and after topology changes, before client I/O.
// OSDs registered outside the fixture make the count differ, a pool whose
// replica count exceeds the OSD count never becomes clean, and after
// Pool.SetPGCount or Pool.SetPlacement it can pass before the new PG set is
// reported (see Pool.WaitForPGCount and Pool.PGs).
//
//purpose:observation
func (o *Observer) WaitForClean(ctx context.Context) error {
	c, err := o.resolve()
	if err != nil {
		return err
	}
	return c.waitForClean(ctx)
}

// WaitForPGClean waits for every existing PG to be exactly active+clean. Unlike
// WaitForClean it allows intentionally out/down OSDs, and does not assert any
// OSD topology. At least one PG and an available manager are required. This is
// a recovery barrier, not a promise of cluster health or daemon availability.
//
//purpose:observation
func (o *Observer) WaitForPGClean(ctx context.Context) error {
	c, err := o.resolve()
	if err != nil {
		return err
	}
	return c.waitForPGClean(ctx)
}

// WaitForQuorum waits for a majority of the current monmap, including fixtures
// with a stopped monitor. It does not require HEALTH_OK or every MON to run.
//
//purpose:observation
func (o *Observer) WaitForQuorum(ctx context.Context) error {
	c, err := o.resolve()
	if err != nil {
		return err
	}
	return c.waitForQuorum(ctx)
}

// WaitMGRModuleReady waits for enabled/can-run metadata and successful native
// read-only commands. Supported modules are rbd_support (task and snapshot
// schedule lists) and volumes (volume list). Other modules need an application
// specific command probe. Each poll releases the cluster lock; cancellation and
// termination stop the wait. Success does not claim worker tasks have completed.
//
//purpose:observation
func (o *Observer) WaitMGRModuleReady(ctx context.Context, name string) error {
	c, err := o.resolve()
	if err != nil {
		return err
	}
	return c.waitMGRModuleReady(ctx, name)
}
