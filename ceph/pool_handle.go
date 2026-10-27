package ceph

import (
	"context"
	"errors"
	"fmt"
)

// errStalePool marks an operation through a Pool handle that no longer
// identifies a native pool: the handle removed it, or the name belongs to
// another pool now.
var errStalePool = errors.New("pool handle is stale")

// Pool returns a handle for the existing pool with the exact native name.
// Use it for a pool that CreatePool did not create, such as an initial pool
// from WithPools or a default pool of a service. It fails when the pool does
// not exist. The handle identifies the pool by its native ID, so it does not
// act on a later pool that reuses the name.
//
// The returned Pool describes what the cluster reports: Name, PGNum,
// Replicas (of a replicated pool), MinSize and ErasureCodeProfile. The other
// fields, CRUSHRule included, are zero; PoolState has the full native state.
func (c *Container) Pool(ctx context.Context, name string) (*Pool, error) {
	if err := validateExistingPoolName(name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	state, err := c.poolState(ctx, name)
	if err != nil {
		return nil, err
	}
	return c.poolOf(state), nil
}

// pools lists the native pools as handles, as Container.Pool returns one. It
// implements Observer.Pools.
func (c *Container) pools(ctx context.Context) ([]*Pool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	states, err := c.poolStates(ctx)
	if err != nil {
		return nil, err
	}
	pools := make([]*Pool, len(states))
	for i, state := range states {
		pools[i] = c.poolOf(state)
	}
	return pools, nil
}

// poolOf returns a handle that identifies the native pool that state
// describes, by name and native ID.
func (c *Container) poolOf(state PoolState) *Pool {
	config := PoolConfig{Name: state.Name, PGNum: state.PGNum, MinSize: state.MinSize}
	if state.Type == "replicated" {
		config.Replicas = state.Size
	}
	return &Pool{
		PoolConfig:         config,
		ErasureCodeProfile: state.ErasureCodeProfile,
		owner:              c,
		ident:              &poolIdentity{bound: true, id: state.ID},
	}
}

// cluster admits op for the shared-cluster policy and returns the cluster
// that the handle belongs to.
func (p *Pool) cluster(op string) (*Container, error) {
	if p == nil || p.owner == nil || p.ident == nil {
		return nil, invalidRequest("%s: the pool does not come from a cluster; obtain it from Container.CreatePool or Container.Pool", op)
	}
	if p.lease != nil {
		if err := p.lease.tenant.check(); err != nil {
			return nil, err
		}
		if err := p.owner.admitHandleAs(op, sharedWithTenants); err != nil {
			return nil, err
		}
		return p.owner, nil
	}
	if err := p.owner.admitHandle(op); err != nil {
		return nil, err
	}
	return p.owner, nil
}

// matchLocked checks, with the cluster's c.mu held, that the handle still
// identifies the native pool with id. A negative id checks only that the
// handle did not remove the pool. The first match of a handle without a
// known ID, which a CreatePool that failed returns, binds it to id. A nil
// handle matches every pool.
func (p *Pool) matchLocked(name string, id int64) error {
	if p == nil {
		return nil
	}
	if p.ident.removed {
		return invalid(fmt.Errorf("%w: pool %q was removed through it", errStalePool, name))
	}
	switch {
	case id < 0:
	case !p.ident.bound:
		p.ident.bound, p.ident.id = true, id
	case p.ident.id != id:
		return invalid(fmt.Errorf("%w: pool %q was replaced by another pool (native ID %d, this handle identifies %d)", errStalePool, name, id, p.ident.id))
	}
	return nil
}

// handlePoolState reads the pool called name and requires it to be the pool
// that handle identifies. The caller holds c.mu.
func (c *Container) handlePoolState(ctx context.Context, name string, handle *Pool) (PoolState, error) {
	if err := handle.matchLocked(name, -1); err != nil {
		return PoolState{}, err
	}
	state, err := c.poolState(ctx, name)
	if err != nil {
		return PoolState{}, err
	}
	if err := handle.matchLocked(name, state.ID); err != nil {
		return PoolState{}, err
	}
	return state, nil
}

// Status returns the native state of this pool. It fails when the pool does
// not exist or the name belongs to another pool now.
func (p *Pool) Status(ctx context.Context) (PoolState, error) {
	c, err := p.cluster("Pool.Status")
	if err != nil {
		return PoolState{}, err
	}
	return c.poolStatus(ctx, p.Name, p)
}

// Usage reports the pool's stored bytes and objects with the original
// bootstrap FSID and the exact native pool ID/name before and after the
// observation. A pool without a statistics row is unavailable, not empty.
// All failures, including cancellation after a query, return a zero snapshot.
// This method makes no writes, retries or background workers. External pool or
// control configuration writers must not race it; the native reads are not an
// atomic transaction. Poll with a caller deadline for eventual assertions.
func (p *Pool) Usage(ctx context.Context) (PoolUsageSnapshot, error) {
	c, err := p.cluster("Pool.Usage")
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	return c.poolUsage(ctx, p.Name, p)
}

// PGs observes the PGs that Ceph reports for this pool; PoolPGSnapshot
// describes the evidence. All failures, including post-query cancellation,
// return a zero snapshot and static errors that omit native output. There
// are no writes, retries or workers. External native/configuration writers
// must not race these separate reads. Compare original fixture identities
// and data when asserting movement; these reported PGs are not current
// mappings or read-after-write evidence.
func (p *Pool) PGs(ctx context.Context) (PoolPGSnapshot, error) {
	c, err := p.cluster("Pool.PGs")
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	return c.poolPGs(ctx, p.Name, p)
}

// SetPGCount requests a new PG count for this pool with autoscaling off or
// warn. It records both native pg_num and pgp_num targets; the active manager
// then splits or merges PGs in steps, and merges wait for clean PGs. Success
// means the targets were accepted, not that PGs have changed. Use
// WaitForPGCount for the count and Container.WaitForClean for the resulting
// movement. Without an active manager the targets remain pending. The native
// pool ID is checked before each mutation; on partial failure inspect Status.
// Native per-OSD PG limits can still reject the request.
func (p *Pool) SetPGCount(ctx context.Context, pgNum int) error {
	c, err := p.cluster("Pool.SetPGCount")
	if err != nil {
		return err
	}
	return c.setPoolPGCount(ctx, p.Name, p, pgNum)
}

// WaitForPGCount waits until the native pool reports pgNum as its current,
// pending and target PG and PGP counts and the manager reports exactly pgNum
// PGs for it. Until then Container.WaitForClean can pass on the previous PG
// set. It fails if the name is bound to another pool than this handle
// identifies. The count does not imply clean PGs, completed backfill or data
// visibility; call WaitForClean afterwards or use PGs. Cluster startup
// timeout applies.
func (p *Pool) WaitForPGCount(ctx context.Context, pgNum int) error {
	c, err := p.cluster("Pool.WaitForPGCount")
	if err != nil {
		return err
	}
	return c.waitForPoolPGCount(ctx, p.Name, p, pgNum)
}

// SetPlacement moves a replicated pool to a simple osd/host/rack CRUSH rule.
// The size and min_size are unchanged and the new placement needs enough owned
// eligible domains in the native CRUSH map. It reuses the pool's CreatePool
// rule or an earlier rule from this method when its native steps match;
// otherwise it creates tc-<pool>-placement-<hash>. Rules are not deleted, so
// a pool can move back. Success means the pool references the rule; data
// moves asynchronously. Until PG reports reflect the new map, WaitForClean
// can pass on the previous placement, so poll PGs for the expected up/acting
// sets. Holding both nobackfill and norecover with OverrideOSDFlag keeps the
// move incomplete; nobackfill alone does not stop log-based recovery of small
// PGs. EC rules are bound to their profile and are refused. External
// pool/CRUSH edits must not race this call.
func (p *Pool) SetPlacement(ctx context.Context, placement PoolPlacement) error {
	c, err := p.cluster("Pool.SetPlacement")
	if err != nil {
		return err
	}
	return c.setPoolPlacement(ctx, p.Name, p, placement)
}

// SetQuota replaces both limits, including zero values that remove a limit.
// Commands are sequential and may partially succeed; inspect Status after an
// error. Existing data is never removed. External pool edits must not race
// this operation; native pool IDs are checked before each mutation.
func (p *Pool) SetQuota(ctx context.Context, quota PoolQuota) error {
	c, err := p.cluster("Pool.SetQuota")
	if err != nil {
		return err
	}
	return c.setPoolQuota(ctx, p.Name, p, quota)
}

// SetReplication changes a replicated pool's size/min_size without changing
// data, PG count or placement. It requires enough owned placement domains in
// the native CRUSH rule. EC shard counts cannot be changed by this method.
// WaitForClean waits for resulting recovery. On partial failure, inspect
// Status; no rollback or data deletion is attempted. External pool/CRUSH
// edits must not race this call.
func (p *Pool) SetReplication(ctx context.Context, replicas, minSize int) error {
	c, err := p.cluster("Pool.SetReplication")
	if err != nil {
		return err
	}
	return c.setPoolReplication(ctx, p.Name, p, replicas, minSize)
}

// Remove deletes a pool created by CreatePool or WithPools, together with
// the CRUSH rule and erasure-code profile created for it, so a long-lived
// cluster can give each test its own pool without reaching PG limits. Pools
// that Ceph or a service created (.mgr, RGW zone pools, CephFS pools) and
// pools still used by a CephFS filesystem are refused. Every object in the
// pool is destroyed, including RBD images and namespaces; stop clients and
// mirroring that use it first. mon_allow_pool_delete is enabled in the
// central database only while the pool is removed and then restored; a local
// config file that sets it to false blocks removal. Ownership is recognized by
// the pool's tc-<name>-replicated or tc-<name>-ec CRUSH rule, so a pool that
// SetPlacement moved to another rule is refused until it moves back;
// placement rules are not removed. A retry after an error finishes removing
// whichever owned resources remain. External edits to the pool must not race
// this call.
//
// After a complete removal the handle is stale and every operation through
// it returns an error, so a later pool that reuses the name is not affected.
func (p *Pool) Remove(ctx context.Context) error {
	c, err := p.cluster("Pool.Remove")
	if err != nil {
		return err
	}
	return c.removePoolByHandle(ctx, p.Name, p)
}
