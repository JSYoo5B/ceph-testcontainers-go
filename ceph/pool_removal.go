package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
)

var allowPoolDeleteSetting = ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete"}

// errPoolMissing marks a name with neither a pool nor fixture leftovers.
var errPoolMissing = errors.New("pool does not exist")

// removePool is called with c.mu held. With skipForeign, a pool that the
// fixture did not create is left alone instead of being reported.
func (c *Container) removePool(ctx context.Context, name string, skipForeign bool, handle *Pool) error {
	if err := c.restorePoolDeletePermission(ctx); err != nil {
		return err
	}
	rules, err := poolCatalog{c.conn()}.crushRuleNames(ctx)
	if err != nil {
		return err
	}
	pools, err := poolCatalog{c.conn()}.states(ctx)
	if err != nil {
		return err
	}
	replicated, erasure := "tc-"+name+"-replicated", "tc-"+name+"-ec"
	if skipForeign {
		if handle.CRUSHRule == erasure {
			replicated = ""
		} else {
			erasure = ""
		}
	}
	ruleNames := slices.Collect(maps.Values(rules))
	profiles, err := poolCatalog{c.conn()}.erasureCodeProfiles(ctx)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(pools, func(pool PoolState) bool { return pool.Name == name })
	if index < 0 && !slices.Contains(ruleNames, replicated) && !slices.Contains(ruleNames, erasure) && !slices.Contains(profiles, erasure) {
		return fmt.Errorf("pool %q: %w", name, errPoolMissing)
	}
	if index >= 0 {
		pool := pools[index]
		if skipForeign && !handle.ident.bound {
			return invalid(fmt.Errorf("pool %q has an unconfirmed native ID after partial creation; refusing removal", name))
		}
		if err := handle.matchLocked(name, pool.ID); err != nil {
			return err
		}
		rule := rules[pool.CRUSHRule]
		if rule == "" || (rule != replicated && rule != erasure) {
			if skipForeign {
				return nil
			}
			return invalid(fmt.Errorf("pool %q was not created by CreatePool or WithPools (CRUSH rule %q); refusing removal", name, rule))
		}
		if err := (poolCatalog{c.conn()}).refuseInUse(ctx, name); err != nil {
			return err
		}
		if err := c.deletePool(ctx, pool); err != nil {
			return err
		}
	}
	// Fixture rules and profiles are named after their pool. A retry after
	// pool deletion finds them without the pool.
	if slices.Contains(ruleNames, replicated) {
		if _, err := c.conn().ceph(ctx, "osd", "crush", "rule", "rm", replicated); err != nil {
			return fmt.Errorf("remove CRUSH rule %q of removed pool %q: %w", replicated, name, err)
		}
	}
	if slices.Contains(ruleNames, erasure) {
		if _, err := c.conn().ceph(ctx, "osd", "crush", "rule", "rm", erasure); err != nil {
			return fmt.Errorf("remove CRUSH rule %q of removed pool %q: %w", erasure, name, err)
		}
	}
	if slices.Contains(profiles, erasure) {
		if _, err := c.conn().ceph(ctx, "osd", "erasure-code-profile", "rm", erasure); err != nil {
			return fmt.Errorf("remove erasure-code profile %q of removed pool %q: %w", erasure, name, err)
		}
	}
	return nil
}

// poolDeleteOverride returns the reservation for mon_allow_pool_delete.
// The caller holds c.mu.
func (r *faultRegistry) poolDeleteOverride() *faultEntry {
	return r.entries[faultID{kind: faultConfigOverride, key: configOverrideKey(allowPoolDeleteSetting)}]
}

// poolDeletePermission retains the prior entry and an unfinished write while
// the cluster holds the temporary configuration override.
type poolDeletePermission struct {
	previous *ConfigEntry
	command  mutation.Fence
}

// restorePoolDeletePermission settles an earlier write and restores the exact
// previous value or absence. The caller holds c.mu.
func (c *Container) restorePoolDeletePermission(ctx context.Context) error {
	token := c.faults.poolDeleteOverride()
	if token == nil {
		return nil
	}
	permission, ok := token.handle.(*poolDeletePermission)
	if !ok {
		return nil
	}
	if err := permission.restore(ctx, c); err != nil {
		return fmt.Errorf("restore mon_allow_pool_delete: %w", err)
	}
	c.faults.release(token)
	return nil
}

func (p *poolDeletePermission) restore(ctx context.Context, c *Container) error {
	if err := p.command.Wait(ctx); err != nil {
		return err
	}
	setting := allowPoolDeleteSetting
	read := func() (*ConfigEntry, error) {
		entries, err := readConfiguration(ctx, c.conn().ceph)
		return findConfigEntry(entries, setting), err
	}
	matchesPrevious := func(current *ConfigEntry) bool {
		if p.previous == nil {
			return current == nil
		}
		return current != nil && current.Value == p.previous.Value
	}
	current, err := read()
	if err != nil || matchesPrevious(current) {
		return err
	}
	if current == nil || current.Value != "true" {
		return errors.New("configuration entry changed outside the temporary pool removal override")
	}
	args := []string{"config", "rm", configWho(setting), setting.Name}
	if p.previous != nil {
		// The saved value was already stored. Preserve its exact spelling.
		args = []string{"config", "set", configWho(setting), setting.Name, p.previous.Value, "--force"}
	}
	if err := p.command.Record(c.overrideCommand(ctx, args...)); err != nil {
		return err
	}
	current, err = read()
	if err == nil && !matchesPrevious(current) {
		return errors.New("configuration restoration readback differs from previous entry")
	}
	return err
}

// deletePool is called with c.mu held (the caller holds it). It enables pool deletion in the
// central database only for this command and restores the previous entry
// even when deletion fails.
func (c *Container) deletePool(ctx context.Context, pool PoolState) (err error) {
	if c.faults.poolDeleteOverride() != nil {
		return errors.New("restore the override.SetConfig override of mon_allow_pool_delete before Pool.Remove")
	}
	entries, err := readConfiguration(ctx, c.conn().ceph)
	if err != nil {
		return err
	}
	previous := findConfigEntry(entries, allowPoolDeleteSetting)
	if previous == nil || previous.Value != "true" {
		setting := allowPoolDeleteSetting
		token, _, reserveErr := c.faults.reserve(ctx, faultConfigOverride, configOverrideKey(setting))
		if reserveErr != nil {
			return reserveErr
		}
		permission := &poolDeletePermission{previous: previous}
		c.faults.settle(token, "", permission, nil, nil)
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if restoreErr := c.restorePoolDeletePermission(restoreCtx); restoreErr != nil {
				err = errors.Join(err, restoreErr)
			}
		}()
		if setErr := permission.command.Record(c.overrideCommand(ctx, "config", "set", configWho(setting), setting.Name, "true")); setErr != nil {
			return fmt.Errorf("allow pool deletion: %w", setErr)
		}
	}
	if err := (poolCatalog{c.conn()}).checkID(ctx, pool); err != nil {
		return err
	}
	// Monitors apply a committed config change asynchronously; retry the
	// permission refusal briefly instead of failing on that window.
	for attempt := 0; ; attempt++ {
		_, err = c.conn().ceph(ctx, "osd", "pool", "rm", pool.Name, pool.Name, "--yes-i-really-really-mean-it")
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

func (pc poolCatalog) refuseInUse(ctx context.Context, name string) error {
	data, err := pc.conn.ceph(ctx, "fs", "ls", "--format", "json")
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
			return invalid(fmt.Errorf("pool %q is used by CephFS filesystem %q; refusing removal", name, fs.Name))
		}
	}
	return nil
}

func (pc poolCatalog) crushRuleNames(ctx context.Context) (map[int]string, error) {
	data, err := pc.conn.ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
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

func (pc poolCatalog) erasureCodeProfiles(ctx context.Context) ([]string, error) {
	data, err := pc.conn.ceph(ctx, "osd", "erasure-code-profile", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	var profiles []string
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, fmt.Errorf("decode erasure-code profiles: %w", err)
	}
	return profiles, nil
}
