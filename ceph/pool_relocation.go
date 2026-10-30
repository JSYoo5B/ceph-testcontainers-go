package ceph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// PoolPlacement selects the CRUSH hierarchy of a replicated pool after creation.
// Its fields have PoolConfig's meaning and defaults: empty FailureDomain uses
// osd, empty CRUSHRoot uses default and empty DeviceClass leaves class unset.
type PoolPlacement struct {
	FailureDomain string
	CRUSHRoot     string
	DeviceClass   string
}

// The OSDMap can reach a count before the manager reports the new PGs. Until
// the reported set matches too, WaitForClean could still pass on the old set.
func (c *Container) reportedPoolPGs(ctx context.Context, pool PoolState) (int, error) {
	data, err := c.cephCLI(ctx, "pg", "ls-by-pool", pool.Name, "--format", "json")
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
		return invalid(errors.New("pool PG count must be between 1 and 65536"))
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
	data, err := c.cephCLI(ctx, "osd", "crush", "rule", "dump", "--format", "json")
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
