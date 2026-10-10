package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

var allowPoolDeleteSetting = ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete"}

// RemovePool deletes a pool created by CreatePool or WithPools, together with
// the CRUSH rule and erasure-code profile created for it, so a long-lived
// cluster can give each test its own pool without reaching PG limits. Pools
// that Ceph or a service created (.mgr, RGW zone pools, CephFS pools) and
// pools still used by a CephFS filesystem are refused. Every object in the
// pool is destroyed, including RBD images and namespaces; stop clients and
// mirroring that use it first. mon_allow_pool_delete is enabled in the
// central database only while the pool is removed and then restored; a local
// config file that sets it to false blocks removal. A retry after an error
// finishes removing whichever owned resources remain. External edits to the
// pool must not race this call.
func (c *Container) RemovePool(ctx context.Context, name string) error {
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
	rules, err := c.crushRuleNames(ctx)
	if err != nil {
		return err
	}
	pools, err := c.poolStates(ctx)
	if err != nil {
		return err
	}
	replicated, erasure := "tc-"+name+"-replicated", "tc-"+name+"-ec"
	ruleNames := slices.Collect(maps.Values(rules))
	profiles, err := c.erasureCodeProfiles(ctx)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(pools, func(pool PoolState) bool { return pool.Name == name })
	if index < 0 && !slices.Contains(ruleNames, replicated) && !slices.Contains(ruleNames, erasure) && !slices.Contains(profiles, erasure) {
		return fmt.Errorf("pool %q does not exist", name)
	}
	if index >= 0 {
		pool := pools[index]
		rule := rules[pool.CRUSHRule]
		if rule != replicated && rule != erasure {
			return fmt.Errorf("pool %q was not created by CreatePool or WithPools (CRUSH rule %q); refusing removal", name, rule)
		}
		if err := c.refusePoolInUse(ctx, name); err != nil {
			return err
		}
		if err := c.deletePool(ctx, pool); err != nil {
			return err
		}
	}
	// Fixture rules and profiles are named after their pool. A retry after
	// pool deletion finds them without the pool.
	if slices.Contains(ruleNames, replicated) {
		if _, err := c.Ceph(ctx, "osd", "crush", "rule", "rm", replicated); err != nil {
			return fmt.Errorf("remove CRUSH rule %q of removed pool %q: %w", replicated, name, err)
		}
	}
	if slices.Contains(ruleNames, erasure) {
		if _, err := c.Ceph(ctx, "osd", "crush", "rule", "rm", erasure); err != nil {
			return fmt.Errorf("remove CRUSH rule %q of removed pool %q: %w", erasure, name, err)
		}
	}
	if slices.Contains(profiles, erasure) {
		if _, err := c.Ceph(ctx, "osd", "erasure-code-profile", "rm", erasure); err != nil {
			return fmt.Errorf("remove erasure-code profile %q of removed pool %q: %w", erasure, name, err)
		}
	}
	return nil
}

// deletePool is called with c.mu held. It enables pool deletion in the
// central database only for this command and restores the previous entry
// even when deletion fails.
func (c *Container) deletePool(ctx context.Context, pool PoolState) (err error) {
	if override := c.configOverrides[configOverrideKey(allowPoolDeleteSetting)]; override != nil && !override.state.restored {
		return errors.New("restore the TemporaryConfig override of mon_allow_pool_delete before RemovePool")
	}
	entries, err := c.configuration(ctx)
	if err != nil {
		return err
	}
	previous := findConfigEntry(entries, allowPoolDeleteSetting)
	if previous == nil || previous.Value != "true" {
		_, setErr := c.configCommand(ctx, "set", configWho(allowPoolDeleteSetting), allowPoolDeleteSetting.Name, "true")
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			var restoreErr error
			if previous == nil {
				_, restoreErr = c.configCommand(restoreCtx, "rm", configWho(allowPoolDeleteSetting), allowPoolDeleteSetting.Name)
			} else {
				_, restoreErr = c.configCommand(restoreCtx, "set", configWho(allowPoolDeleteSetting), allowPoolDeleteSetting.Name, previous.Value)
			}
			if restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore mon_allow_pool_delete: %w", restoreErr))
			}
		}()
		if setErr != nil {
			return fmt.Errorf("allow pool deletion: %w", setErr)
		}
	}
	if err := c.checkPoolID(ctx, pool); err != nil {
		return err
	}
	// Monitors apply a committed config change asynchronously; retry the
	// permission refusal briefly instead of failing on that window.
	for attempt := 0; ; attempt++ {
		_, err = c.Ceph(ctx, "osd", "pool", "rm", pool.Name, pool.Name, "--yes-i-really-really-mean-it")
		if err == nil || !strings.Contains(err.Error(), "mon_allow_pool_delete") || attempt == 10 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err != nil {
		return fmt.Errorf("remove pool %q: %w", pool.Name, err)
	}
	return nil
}

func (c *Container) refusePoolInUse(ctx context.Context, name string) error {
	data, err := c.Ceph(ctx, "fs", "ls", "--format", "json")
	if err != nil {
		return err
	}
	var filesystems []struct {
		Name         string   `json:"name"`
		MetadataPool string   `json:"metadata_pool"`
		DataPools    []string `json:"data_pools"`
	}
	if err := json.Unmarshal(data, &filesystems); err != nil {
		return fmt.Errorf("decode CephFS filesystems: %w", err)
	}
	for _, fs := range filesystems {
		if fs.MetadataPool == name || slices.Contains(fs.DataPools, name) {
			return fmt.Errorf("pool %q is used by CephFS filesystem %q; refusing removal", name, fs.Name)
		}
	}
	return nil
}

func (c *Container) crushRuleNames(ctx context.Context) (map[int]string, error) {
	data, err := c.Ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var rules []struct {
		ID   int    `json:"rule_id"`
		Name string `json:"rule_name"`
	}
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("decode CRUSH rules: %w", err)
	}
	names := make(map[int]string, len(rules))
	for _, rule := range rules {
		names[rule.ID] = rule.Name
	}
	return names, nil
}

func (c *Container) erasureCodeProfiles(ctx context.Context) ([]string, error) {
	data, err := c.Ceph(ctx, "osd", "erasure-code-profile", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	var profiles []string
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, fmt.Errorf("decode erasure-code profiles: %w", err)
	}
	return profiles, nil
}
