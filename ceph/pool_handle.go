package ceph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
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
//
//purpose:observation
func (c *Container) Pool(ctx context.Context, name string) (*Pool, error) {
	if err := validateExistingPoolName(name); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	state, err := poolCatalog{c.conn()}.state(ctx, name)
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
	states, err := poolCatalog{c.conn()}.states(ctx)
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

// cluster returns the cluster that owns this handle.
func (p *Pool) cluster(op string) (*Container, error) {
	if p == nil || p.owner == nil || p.ident == nil {
		return nil, invalidRequest("%s: the pool does not come from a cluster; obtain it from Container.CreatePool or Container.Pool", op)
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

// handleState reads the pool called name and requires it to be the pool
// that handle identifies. The caller holds c.mu.
func (pc poolCatalog) handleState(ctx context.Context, name string, handle *Pool) (PoolState, error) {
	if err := handle.matchLocked(name, -1); err != nil {
		return PoolState{}, err
	}
	state, err := pc.state(ctx, name)
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
//
//purpose:observation
func (p *Pool) Status(ctx context.Context) (PoolState, error) {
	c, err := p.cluster("Pool.Status")
	if err != nil {
		return PoolState{}, err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return PoolState{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return PoolState{}, err
	}
	return poolCatalog{c.conn()}.handleState(ctx, name, p)
}

// Usage reports the pool's stored bytes and objects with the original
// bootstrap FSID and the exact native pool ID/name before and after the
// observation. A pool without a statistics row is unavailable, not empty.
// All failures, including cancellation after a query, return a zero snapshot.
// This method makes no writes, retries or background workers. External pool or
// control configuration writers must not race it; the native reads are not an
// atomic transaction. Poll with a caller deadline for eventual assertions.
//
// A pool without a statistics row is an error, not an empty result, and
// AllocatedBytes need not equal StoredBytes times the replica count.
//
//purpose:observation
func (p *Pool) Usage(ctx context.Context) (PoolUsageSnapshot, error) {
	c, err := p.cluster("Pool.Usage")
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	// It reads ceph df detail through the fixture's control CLI. It checks
	// the original bootstrap FSID and exact native pool ID/name before and after
	// the observation. A pool without a statistics row is unavailable, not empty.
	// All failures, including cancellation after a query, return a zero snapshot.
	// This method makes no writes, retries or background workers. External pool or
	// control configuration writers must not race it; the native reads are not an
	// atomic transaction. Poll with a caller deadline for eventual assertions.
	name, handle := p.Name, p
	if err := ctx.Err(); err != nil {
		return PoolUsageSnapshot{}, err
	}
	if c == nil {
		return PoolUsageSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := validateExistingPoolName(name); err != nil {
		return PoolUsageSnapshot{}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return PoolUsageSnapshot{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return PoolUsageSnapshot{}, errClusterTerminated
	}
	if err := handle.matchLocked(name, -1); err != nil {
		return PoolUsageSnapshot{}, err
	}
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	if control == nil {
		return PoolUsageSnapshot{}, errors.New("ceph control container is unavailable")
	}
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	fsid, identityErr := identity.fsid, identity.err
	if identityErr != nil || !identity.confirmed {
		return PoolUsageSnapshot{}, errors.New("ceph original bootstrap identity is unavailable")
	}
	query := func(args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		if err = errors.Join(err, ctx.Err()); err != nil {
			return nil, fmt.Errorf("observe pool usage: %w", err)
		}
		return data, nil
	}
	checkFSID := func() error {
		data, err := query("fsid")
		if err != nil {
			return err
		}
		native := strings.TrimSpace(string(data))
		parsed, err := uuid.Parse(native)
		if err != nil || parsed == uuid.Nil || parsed.String() != native || native != fsid {
			return errors.New("native pool usage FSID differs from the original cluster")
		}
		return ctx.Err()
	}
	readID := func() (int64, error) {
		data, err := query("osd", "pool", "ls", "detail", "--format", "json")
		if err != nil {
			return 0, err
		}
		id, err := poolUsageID(data, name)
		return id, errors.Join(err, ctx.Err())
	}
	if err := checkFSID(); err != nil {
		return PoolUsageSnapshot{}, err
	}
	id, err := readID()
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	if err := handle.matchLocked(name, id); err != nil {
		return PoolUsageSnapshot{}, err
	}
	data, err := query("df", "detail", "--format", "json")
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	usage, err := decodePoolUsage(data, fsid, name, id)
	if err = errors.Join(err, ctx.Err()); err != nil {
		return PoolUsageSnapshot{}, err
	}
	afterID, err := readID()
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	if afterID != id {
		return PoolUsageSnapshot{}, fmt.Errorf("pool %q was replaced during usage observation", name)
	}
	if err := checkFSID(); err != nil {
		return PoolUsageSnapshot{}, err
	}
	return usage, nil
}

// PGs observes the PGs that Ceph reports for this pool; PoolPGSnapshot
// describes the evidence. All failures, including post-query cancellation,
// return a zero snapshot and static errors that omit native output. There
// are no writes, retries or workers. External native/configuration writers
// must not race these separate reads. Compare original fixture identities
// and data when asserting movement; these reported PGs are not current
// mappings or read-after-write evidence.
//
// It works before any OSD exists.
//
//purpose:observation
func (p *Pool) PGs(ctx context.Context) (PoolPGSnapshot, error) {
	c, err := p.cluster("Pool.PGs")
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	// It observes one existing pool through one captured control CLI handle.
	// It checks the original bootstrap FSID and the exact pool name/ID before and
	// after pg ls-by-pool, using five CLI reads and no per-PG mapping fanout. It
	// works before any OSD is started when a native pool has been prepared through
	// Ceph. Successful empty/unready/unknown reports are retained as observations;
	// no clean, health, owned-OSD membership or expected PG-count predicate applies.
	//
	// All failures, including post-query cancellation, return a zero snapshot and
	// static errors that omit native output. There are no writes, retries or
	// workers. External native/configuration writers must not race these separate
	// reads. Compare original fixture identities and data when asserting movement;
	// these reported PGs are not current mappings or read-after-write evidence.
	name, handle := p.Name, p
	if err := ctx.Err(); err != nil {
		return PoolPGSnapshot{}, err
	}
	if c == nil {
		return PoolPGSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := validateExistingPoolName(name); err != nil {
		return PoolPGSnapshot{}, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return PoolPGSnapshot{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return PoolPGSnapshot{}, clientOperationError(ctx, "ceph cluster is terminated", nil)
	}
	if err := handle.matchLocked(name, -1); err != nil {
		return PoolPGSnapshot{}, err
	}
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	if control == nil {
		return PoolPGSnapshot{}, clientOperationError(ctx, "ceph control container is unavailable", nil)
	}
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	fsid, identityErr := identity.fsid, identity.err
	if identityErr != nil || !identity.confirmed {
		return PoolPGSnapshot{}, clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identityErr)
	}
	query := func(args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		if err != nil || ctx.Err() != nil {
			return nil, clientOperationError(ctx, "observe pool PGs: native query failed", err)
		}
		return data, nil
	}
	checkFSID := func() error {
		data, err := query("fsid")
		if err != nil {
			return err
		}
		native := strings.TrimSpace(string(data))
		parsed, err := uuid.Parse(native)
		if err != nil || parsed == uuid.Nil || parsed.String() != native || native != fsid {
			return clientOperationError(ctx, "native pool PG FSID differs from the original cluster", err)
		}
		return ctx.Err()
	}
	readPool := func() (PoolState, uint32, error) {
		data, err := query("osd", "dump", "--format", "json")
		if err != nil {
			return PoolState{}, 0, err
		}
		pool, epoch, err := decodePoolPGOSDMap(data, fsid, name)
		if err != nil || ctx.Err() != nil {
			return PoolState{}, 0, clientOperationError(ctx, "decode native pool PG identity failed", err)
		}
		return pool, epoch, nil
	}
	if err := checkFSID(); err != nil {
		return PoolPGSnapshot{}, err
	}
	before, beforeEpoch, err := readPool()
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	if err := handle.matchLocked(name, before.ID); err != nil {
		return PoolPGSnapshot{}, err
	}
	data, err := query("pg", "ls-by-pool", name, "--format", "json")
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	ready, pgs, err := decodePoolPGReports(data, before.ID)
	if err != nil || ctx.Err() != nil {
		return PoolPGSnapshot{}, clientOperationError(ctx, "decode native pool PG reports failed", err)
	}
	after, afterEpoch, err := readPool()
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	if after.ID != before.ID {
		return PoolPGSnapshot{}, clientOperationError(ctx, "native pool was replaced during PG observation", nil)
	}
	if err := checkFSID(); err != nil {
		return PoolPGSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return PoolPGSnapshot{}, err
	}
	return PoolPGSnapshot{FSID: fsid, PoolBefore: before, PoolAfter: after,
		OSDMapEpochBefore: beforeEpoch, OSDMapEpochAfter: afterEpoch, PGReady: ready, PGs: pgs}, nil
}

// SetPGCount requests a new PG count for this pool with autoscaling off or
// warn. It records both native pg_num and pgp_num targets; the active manager
// then splits or merges PGs in steps, and merges wait for clean PGs. Success
// means the targets were accepted, not that PGs have changed. Use
// WaitForPGCount for the count and Container.WaitForClean for the resulting
// movement. Without an active manager the targets remain pending. The native
// pool ID is checked before each mutation; on partial failure inspect Status.
// Native per-OSD PG limits can still reject the request.
//
//purpose:construction,condition
func (p *Pool) SetPGCount(ctx context.Context, pgNum int) error {
	c, err := p.cluster("Pool.SetPGCount")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	if err := validatePoolPGCount(pgNum); err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	pool, err := poolCatalog{c.conn()}.handleState(ctx, name, p)
	if err != nil {
		return err
	}
	if pool.AutoscaleMode == "on" {
		return fmt.Errorf("pool %q has PG autoscaling on; it would replace the requested PG count", name)
	}
	// pg_num first: native pgp_num targets cannot exceed the pg_num target.
	for _, setting := range []string{"pg_num", "pgp_num"} {
		if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
			return err
		}
		if _, err := c.conn().ceph(ctx, "osd", "pool", "set", name, setting, strconv.Itoa(pgNum)); err != nil {
			return fmt.Errorf("set pool %q %s target (earlier settings may have changed): %w", name, setting, err)
		}
	}
	return nil
}

// WaitForPGCount waits until the native pool reports pgNum as its current,
// pending and target PG and PGP counts and the manager reports exactly pgNum
// PGs for it. Until then Container.WaitForClean can pass on the previous PG
// set. It fails if the name is bound to another pool than this handle
// identifies. The count does not imply clean PGs, completed backfill or data
// visibility; call WaitForClean afterwards or use PGs. Cluster startup
// timeout applies.
//
//purpose:observation
func (p *Pool) WaitForPGCount(ctx context.Context, pgNum int) error {
	c, err := p.cluster("Pool.WaitForPGCount")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	if err := validatePoolPGCount(pgNum); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	poolID := int64(-1)
	for {
		if err := c.lockTopology(ctx); err != nil {
			return errors.Join(err, last)
		}
		if err := c.waitPolicyReady(ctx); err != nil {
			c.mu.Unlock()
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), last, err)
			}
			return err
		}
		pool, err := poolCatalog{c.conn()}.handleState(ctx, name, p)
		if p != nil && err != nil && errors.Is(err, errStalePool) {
			c.mu.Unlock()
			return err
		}
		replaced := err == nil && poolID >= 0 && pool.ID != poolID
		reported := 0
		if err == nil && !replaced && poolPGCountReached(pool, pgNum) {
			reported, err = poolCatalog{c.conn()}.reportedPGs(ctx, pool)
		}
		c.mu.Unlock()
		if replaced {
			return fmt.Errorf("pool %q was replaced while waiting for its PG count", name)
		}
		if err == nil {
			poolID = pool.ID
			switch {
			case pool.PGNumTarget == 0 || pool.PGPlacementNumTarget == 0 || pool.PGNumPending == 0:
				err = fmt.Errorf("pool %q does not report native PG targets", name)
			case !poolPGCountReached(pool, pgNum):
				err = fmt.Errorf("pool %q PG count is %d/%d (pgp %d/%d, pending %d); waiting for %d",
					name, pool.PGNum, pool.PGNumTarget, pool.PGPlacementNum, pool.PGPlacementNumTarget, pool.PGNumPending, pgNum)
			case reported != pgNum:
				err = fmt.Errorf("pool %q reports %d PGs; waiting for %d", name, reported, pgNum)
			default:
				return nil
			}
		}
		// Keep the last observation made before the deadline; a canceled
		// command error would hide why the count was not reached.
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		last = err
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

// SetPlacement moves a replicated pool to a simple osd/host/rack CRUSH rule.
// The size and min_size are unchanged and the new placement needs enough owned
// eligible domains in the native CRUSH map. It reuses the pool's CreatePool
// rule or an earlier rule from this method when its native steps match;
// otherwise it creates tc-<pool>-placement-<hash>. Rules are not deleted, so
// a pool can move back. Success means the pool references the rule; data
// moves asynchronously. Until PG reports reflect the new map, WaitForClean
// can pass on the previous placement, so poll PGs for the expected up/acting
// sets. Holding both nobackfill and norecover with override.SetOSDFlag keeps the
// move incomplete; nobackfill alone does not stop log-based recovery of small
// PGs. EC rules are bound to their profile and are refused. External
// pool/CRUSH edits must not race this call.
//
//purpose:construction,condition
func (p *Pool) SetPlacement(ctx context.Context, placement PoolPlacement) error {
	c, err := p.cluster("Pool.SetPlacement")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	desired, err := normalizePoolPlacement(name, placement)
	if err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	pool, err := poolCatalog{c.conn()}.handleState(ctx, name, p)
	if err != nil {
		return err
	}
	if pool.Type != "replicated" {
		return errors.New("placement changes require a replicated pool")
	}
	if current, err := (poolCatalog{c.conn()}).rulePlacement(ctx, pool.CRUSHRule); err == nil && samePoolPlacement(current, desired) {
		return nil
	}
	domains, err := c.nativePoolDomains(ctx, desired)
	if err != nil {
		return err
	}
	if domains < pool.Size {
		return fmt.Errorf("pool %q needs %d owned %s domains; native CRUSH placement has %d eligible owned domains", name, pool.Size, desired.FailureDomain, domains)
	}
	rule, exists, err := poolCatalog{c.conn()}.placementRule(ctx, name, desired)
	if err != nil {
		return err
	}
	if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
		return err
	}
	if !exists {
		if _, err := c.conn().ceph(ctx, replicatedCRUSHRuleCommand(desired, rule)...); err != nil {
			return fmt.Errorf("create placement rule for pool %q: %w", name, err)
		}
	}
	if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
		return err
	}
	if _, err := c.conn().ceph(ctx, "osd", "pool", "set", name, "crush_rule", rule); err != nil {
		return fmt.Errorf("set pool %q CRUSH rule %q: %w", name, rule, err)
	}
	return nil
}

// SetQuota replaces both limits, including zero values that remove a limit.
// Commands are sequential and may partially succeed; inspect Status after an
// error. Existing data is never removed. External pool edits must not race
// this operation; native pool IDs are checked before each mutation.
//
// Quotas are enforced from asynchronously reported PG statistics, so writes
// can overshoot briefly; when a quota is reached Ceph marks the pool full and
// clients see writes blocked or EDQUOT, depending on the client.
//
//purpose:construction,condition
func (p *Pool) SetQuota(ctx context.Context, quota PoolQuota) error {
	c, err := p.cluster("Pool.SetQuota")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	pool, err := poolCatalog{c.conn()}.handleState(ctx, name, p)
	if err != nil {
		return err
	}
	for _, setting := range []struct {
		name             string
		current, desired uint64
	}{
		{"max_objects", pool.Quota.MaxObjects, quota.MaxObjects},
		{"max_bytes", pool.Quota.MaxBytes, quota.MaxBytes},
	} {
		if setting.current == setting.desired {
			continue
		}
		if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
			return err
		}
		if _, err := c.conn().ceph(ctx, "osd", "pool", "set-quota", name, setting.name, strconv.FormatUint(setting.desired, 10)); err != nil {
			return fmt.Errorf("set pool %q %s quota (earlier settings may have changed): %w", name, setting.name, err)
		}
	}
	return nil
}

// SetReplication changes a replicated pool's size/min_size without changing
// data, PG count or placement. It requires enough owned placement domains in
// the native CRUSH rule. EC shard counts cannot be changed by this method.
// WaitForClean waits for resulting recovery. On partial failure, inspect
// Status; no rollback or data deletion is attempted. External pool/CRUSH
// edits must not race this call.
//
//purpose:construction,condition
func (p *Pool) SetReplication(ctx context.Context, replicas, minSize int) error {
	c, err := p.cluster("Pool.SetReplication")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	if replicas < 1 || replicas > 10 || minSize < 1 || minSize > replicas {
		return errors.New("pool replicas must be 1..10 and min size must be 1..replicas")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	pool, err := poolCatalog{c.conn()}.handleState(ctx, name, p)
	if err != nil {
		return err
	}
	if pool.Type != "replicated" {
		return errors.New("replication policy requires a replicated pool")
	}
	placement, err := poolCatalog{c.conn()}.rulePlacement(ctx, pool.CRUSHRule)
	if err != nil {
		return err
	}
	domains, err := c.nativePoolDomains(ctx, placement)
	if err != nil {
		return err
	}
	if domains < replicas {
		return fmt.Errorf("pool %q needs %d owned %s domains; native CRUSH rule has %d eligible owned domains", name, replicas, placement.FailureDomain, domains)
	}
	// Lower min_size before lowering size; raise size before raising min_size.
	settings := []struct {
		name             string
		current, desired int
	}{
		{"size", pool.Size, replicas}, {"min_size", pool.MinSize, minSize},
	}
	if minSize < pool.MinSize {
		settings[0], settings[1] = settings[1], settings[0]
	}
	for _, setting := range settings {
		if setting.current == setting.desired {
			continue
		}
		if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
			return err
		}
		args := []string{"osd", "pool", "set", name, setting.name, strconv.Itoa(setting.desired)}
		if setting.name == "size" && setting.desired == 1 {
			args = append(args, "--yes-i-really-mean-it")
		}
		if _, err := c.conn().ceph(ctx, args...); err != nil {
			return fmt.Errorf("set pool %q %s (earlier settings may have changed): %w", name, setting.name, err)
		}
	}
	return nil
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
//
//purpose:lifetime
func (p *Pool) Remove(ctx context.Context) error {
	c, err := p.cluster("Pool.Remove")
	if err != nil {
		return err
	}
	name := p.Name
	if err := validateExistingPoolName(name); err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := p.matchLocked(name, -1); err != nil {
		return err
	}
	if err := c.removePool(ctx, name, false, p); err != nil {
		return err
	}
	p.ident.removed = true
	return nil
}
