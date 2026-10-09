package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// PoolPlacement selects the CRUSH hierarchy of a replicated pool after creation.
// Its fields have PoolConfig's meaning and defaults: empty FailureDomain uses
// osd, empty CRUSHRoot uses default and empty DeviceClass leaves class unset.
type PoolPlacement struct {
	FailureDomain string
	CRUSHRoot     string
	DeviceClass   string
}

// SetPoolPGCount requests a new PG count for an existing pool with autoscaling
// off or warn. It records both native pg_num and pgp_num targets; the active
// manager then splits or merges PGs in steps, and merges wait for clean PGs.
// Success means the targets were accepted, not that PGs have changed. Use
// WaitForPoolPGCount for the count and WaitForClean for the resulting movement.
// Without an active manager the targets remain pending. The native pool ID is
// checked before each mutation; on partial failure inspect PoolStatus. Native
// per-OSD PG limits can still reject the request.
func (c *Container) SetPoolPGCount(ctx context.Context, name string, pgNum int) error {
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
	pool, err := c.poolState(ctx, name)
	if err != nil {
		return err
	}
	if pool.AutoscaleMode == "on" {
		return fmt.Errorf("pool %q has PG autoscaling on; it would replace the requested PG count", name)
	}
	// pg_num first: native pgp_num targets cannot exceed the pg_num target.
	for _, setting := range []string{"pg_num", "pgp_num"} {
		if err := c.checkPoolID(ctx, pool); err != nil {
			return err
		}
		if _, err := c.Ceph(ctx, "osd", "pool", "set", name, setting, strconv.Itoa(pgNum)); err != nil {
			return fmt.Errorf("set pool %q %s target (earlier settings may have changed): %w", name, setting, err)
		}
	}
	return nil
}

// WaitForPoolPGCount waits until the native pool reports pgNum as its current,
// pending and target PG and PGP counts and the manager reports exactly pgNum
// PGs for it. Until then WaitForClean can pass on the previous PG set. It
// identifies the pool by the native ID first observed and fails if the name is
// later bound to another pool. The count does not imply clean PGs, completed
// backfill or data visibility; call WaitForClean afterwards or use PoolPGs.
// Cluster startup timeout applies.
func (c *Container) WaitForPoolPGCount(ctx context.Context, name string, pgNum int) error {
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
		pool, err := c.poolState(ctx, name)
		replaced := err == nil && poolID >= 0 && pool.ID != poolID
		reported := 0
		if err == nil && !replaced && poolPGCountReached(pool, pgNum) {
			reported, err = c.reportedPoolPGs(ctx, pool)
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

// The OSDMap can reach a count before the manager reports the new PGs. Until
// the reported set matches too, WaitForClean could still pass on the old set.
func (c *Container) reportedPoolPGs(ctx context.Context, pool PoolState) (int, error) {
	data, err := c.Ceph(ctx, "pg", "ls-by-pool", pool.Name, "--format", "json")
	if err != nil {
		return 0, err
	}
	_, pgs, err := decodePoolPGReports(data, pool.ID)
	if err != nil {
		return 0, errors.New("decode native pool PG reports")
	}
	return len(pgs), nil
}

func poolPGCountReached(pool PoolState, pgNum int) bool {
	for _, value := range []int{pool.PGNum, pool.PGNumTarget, pool.PGNumPending, pool.PGPlacementNum, pool.PGPlacementNumTarget} {
		if value != pgNum {
			return false
		}
	}
	return true
}

// The native default mon_max_pool_pg_num is 65536; larger values need a
// configuration change this fixture does not make implicitly.
func validatePoolPGCount(pgNum int) error {
	if pgNum < 1 || pgNum > 65536 {
		return errors.New("pool PG count must be between 1 and 65536")
	}
	return nil
}

// SetPoolPlacement moves a replicated pool to a simple osd/host/rack CRUSH rule.
// The size and min_size are unchanged and the new placement needs enough owned
// eligible domains in the native CRUSH map. It reuses the pool's CreatePool rule
// or an earlier rule from this method when its native steps match; otherwise it
// creates tc-<pool>-placement-<hash>. Rules are not deleted, so a pool can move
// back. Success means the pool references the rule; data moves asynchronously.
// Until PG reports reflect the new map, WaitForClean can pass on the previous
// placement, so poll PoolPGs for the expected up/acting sets. Holding both
// nobackfill and norecover with TemporaryOSDFlag keeps the move incomplete;
// nobackfill alone does not stop log-based recovery of small PGs. EC rules are
// bound to their profile and are refused. External pool/CRUSH edits must not
// race this call.
func (c *Container) SetPoolPlacement(ctx context.Context, name string, placement PoolPlacement) error {
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
	pool, err := c.poolState(ctx, name)
	if err != nil {
		return err
	}
	if pool.Type != "replicated" {
		return errors.New("placement changes require a replicated pool")
	}
	if current, err := c.poolRulePlacement(ctx, pool.CRUSHRule); err == nil && samePoolPlacement(current, desired) {
		return nil
	}
	domains, err := c.nativePoolDomains(ctx, desired)
	if err != nil {
		return err
	}
	if domains < pool.Size {
		return fmt.Errorf("pool %q needs %d owned %s domains; native CRUSH placement has %d eligible owned domains", name, pool.Size, desired.FailureDomain, domains)
	}
	rule, exists, err := c.poolPlacementRule(ctx, name, desired)
	if err != nil {
		return err
	}
	if err := c.checkPoolID(ctx, pool); err != nil {
		return err
	}
	if !exists {
		if _, err := c.Ceph(ctx, replicatedCRUSHRuleCommand(desired, rule)...); err != nil {
			return fmt.Errorf("create placement rule for pool %q: %w", name, err)
		}
	}
	if err := c.checkPoolID(ctx, pool); err != nil {
		return err
	}
	if _, err := c.Ceph(ctx, "osd", "pool", "set", name, "crush_rule", rule); err != nil {
		return fmt.Errorf("set pool %q CRUSH rule %q: %w", name, rule, err)
	}
	return nil
}

func normalizePoolPlacement(name string, placement PoolPlacement) (PoolConfig, error) {
	// Existing native names, such as .mgr, are validated separately; only the
	// placement fields use PoolConfig's creation-time rules here.
	config := PoolConfig{Name: "placement", FailureDomain: placement.FailureDomain, CRUSHRoot: placement.CRUSHRoot, DeviceClass: placement.DeviceClass}
	config, err := normalizePoolConfig(config)
	if err != nil {
		return PoolConfig{}, fmt.Errorf("configure pool placement: %w", err)
	}
	config.Name = name
	return config, nil
}

func samePoolPlacement(a, b PoolConfig) bool {
	return a.FailureDomain == b.FailureDomain && a.CRUSHRoot == b.CRUSHRoot && a.DeviceClass == b.DeviceClass
}

// Only fixture-named rules are reused. A hashed name always ends with eight
// hex digits, so it cannot equal CreatePool's -replicated or -ec rule names.
func poolPlacementRuleName(pool string, placement PoolConfig) string {
	sum := sha256.Sum256([]byte(placement.CRUSHRoot + "\x00" + placement.FailureDomain + "\x00" + placement.DeviceClass))
	return "tc-" + pool + "-placement-" + hex.EncodeToString(sum[:4])
}

func (c *Container) poolPlacementRule(ctx context.Context, pool string, desired PoolConfig) (string, bool, error) {
	data, err := c.Ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
	if err != nil {
		return "", false, err
	}
	var rules []struct {
		ID   *int   `json:"rule_id"`
		Name string `json:"rule_name"`
	}
	if err := json.Unmarshal(data, &rules); err != nil || rules == nil {
		return "", false, errors.New("decode native CRUSH rules")
	}
	ids := make(map[string]int, len(rules))
	for _, rule := range rules {
		if rule.ID == nil || rule.Name == "" {
			return "", false, errors.New("incomplete native CRUSH rule")
		}
		ids[rule.Name] = *rule.ID
	}
	hashed := poolPlacementRuleName(pool, desired)
	for _, name := range []string{"tc-" + pool + "-replicated", hashed} {
		id, exists := ids[name]
		if !exists {
			continue
		}
		current, err := c.poolRulePlacement(ctx, id)
		if err == nil && samePoolPlacement(current, desired) {
			return name, true, nil
		}
		if name == hashed {
			return "", false, fmt.Errorf("native CRUSH rule %q exists with a different placement", name)
		}
	}
	return hashed, false, nil
}
