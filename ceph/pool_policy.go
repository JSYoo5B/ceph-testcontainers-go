package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// PoolQuota limits logical bytes and object count. Zero removes that limit.
// Enforcement depends on asynchronously reported PG statistics, so the quota
// is not a strict per-write accounting boundary.
type PoolQuota struct {
	MaxBytes   uint64
	MaxObjects uint64
}

// PoolState is an authoritative snapshot of a native pool, including pools
// created by services or through Ceph. Size is replica count or EC shard count.
// It does not change when a policy is subsequently updated. PGNum is the
// current count; the target and pending fields show a split or merge still in
// progress and are zero when the native map omits them.
type PoolState struct {
	ID                   int64
	Name                 string
	Type                 string
	Size, MinSize, PGNum int
	PGNumTarget          int
	PGNumPending         int
	PGPlacementNum       int
	PGPlacementNumTarget int
	CRUSHRule            int
	AutoscaleMode        string
	ErasureCodeProfile   string
	Flags                string
	Quota                PoolQuota
}

// Pools lists native pool policies. Creation descriptors returned by CreatePool
// describe requested settings; this method reports the current cluster state.
func (c *Container) Pools(ctx context.Context) ([]PoolState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.poolStates(ctx)
}

// PoolStatus looks up a pool by its exact native name.
func (c *Container) PoolStatus(ctx context.Context, name string) (PoolState, error) {
	if err := validateExistingPoolName(name); err != nil {
		return PoolState{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return PoolState{}, err
	}
	return c.poolState(ctx, name)
}

// SetPoolQuota replaces both limits, including zero values that remove a limit.
// Commands are sequential and may partially succeed; inspect PoolStatus after
// an error. Existing data is never removed. External pool edits must not race
// this operation; native pool IDs are checked before each mutation.
func (c *Container) SetPoolQuota(ctx context.Context, name string, quota PoolQuota) error {
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
	pool, err := c.poolState(ctx, name)
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
		if err := c.checkPoolID(ctx, pool); err != nil {
			return err
		}
		if _, err := c.Ceph(ctx, "osd", "pool", "set-quota", name, setting.name, strconv.FormatUint(setting.desired, 10)); err != nil {
			return fmt.Errorf("set pool %q %s quota (earlier settings may have changed): %w", name, setting.name, err)
		}
	}
	return nil
}

// SetPoolReplication changes a replicated pool's size/min_size without changing
// data, PG count or placement. It requires enough owned placement domains in the
// native CRUSH rule. EC shard counts cannot be changed by this method. WaitForClean
// waits for resulting recovery. On partial failure, inspect PoolStatus; no rollback
// or data deletion is attempted. External pool/CRUSH edits must not race this call.
func (c *Container) SetPoolReplication(ctx context.Context, name string, replicas, minSize int) error {
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
	pool, err := c.poolState(ctx, name)
	if err != nil {
		return err
	}
	if pool.Type != "replicated" {
		return errors.New("replication policy requires a replicated pool")
	}
	placement, err := c.poolRulePlacement(ctx, pool.CRUSHRule)
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
		if err := c.checkPoolID(ctx, pool); err != nil {
			return err
		}
		args := []string{"osd", "pool", "set", name, setting.name, strconv.Itoa(setting.desired)}
		if setting.name == "size" && setting.desired == 1 {
			args = append(args, "--yes-i-really-mean-it")
		}
		if _, err := c.Ceph(ctx, args...); err != nil {
			return fmt.Errorf("set pool %q %s (earlier settings may have changed): %w", name, setting.name, err)
		}
	}
	return nil
}

var existingPoolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.][A-Za-z0-9_.-]{0,127}$`)

func validateExistingPoolName(name string) error {
	if !existingPoolNamePattern.MatchString(name) {
		return errors.New("invalid native pool name")
	}
	return nil
}

func (c *Container) poolPolicyReady() error {
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if c.cliContainer() == nil {
		return errors.New("ceph control container is unavailable")
	}
	return nil
}

func (c *Container) poolStates(ctx context.Context) ([]PoolState, error) {
	data, err := c.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return nil, err
	}
	var native []struct {
		ID                 *int64 `json:"pool_id"`
		Name               string `json:"pool_name"`
		Type               int    `json:"type"`
		Size               int    `json:"size"`
		MinSize            int    `json:"min_size"`
		PGNum              int    `json:"pg_num"`
		PGNumTarget        int    `json:"pg_num_target"`
		PGNumPending       int    `json:"pg_num_pending"`
		PGPlacementNum     int    `json:"pg_placement_num"`
		PGPlacementTarget  int    `json:"pg_placement_num_target"`
		CRUSHRule          int    `json:"crush_rule"`
		AutoscaleMode      string `json:"pg_autoscale_mode"`
		ErasureCodeProfile string `json:"erasure_code_profile"`
		Flags              string `json:"flags_names"`
		MaxBytes           uint64 `json:"quota_max_bytes"`
		MaxObjects         uint64 `json:"quota_max_objects"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native == nil {
		return nil, errors.New("decode native pool policies")
	}
	states := make([]PoolState, 0, len(native))
	seenIDs := make(map[int64]bool)
	seenNames := make(map[string]bool)
	for _, pool := range native {
		kind := "replicated"
		if pool.Type == 3 {
			kind = "erasure"
		} else if pool.Type != 1 {
			return nil, fmt.Errorf("unsupported native pool type %d", pool.Type)
		}
		if pool.Name == "" || pool.ID == nil || *pool.ID < 0 || pool.Size < 1 || pool.MinSize < 1 || pool.PGNum < 1 || seenIDs[*pool.ID] || seenNames[pool.Name] {
			return nil, errors.New("incomplete native pool policy")
		}
		seenIDs[*pool.ID], seenNames[pool.Name] = true, true
		states = append(states, PoolState{ID: *pool.ID, Name: pool.Name, Type: kind, Size: pool.Size, MinSize: pool.MinSize, PGNum: pool.PGNum, PGNumTarget: pool.PGNumTarget, PGNumPending: pool.PGNumPending, PGPlacementNum: pool.PGPlacementNum, PGPlacementNumTarget: pool.PGPlacementTarget, CRUSHRule: pool.CRUSHRule, AutoscaleMode: pool.AutoscaleMode, ErasureCodeProfile: pool.ErasureCodeProfile, Flags: pool.Flags, Quota: PoolQuota{MaxBytes: pool.MaxBytes, MaxObjects: pool.MaxObjects}})
	}
	return states, nil
}

func (c *Container) poolState(ctx context.Context, name string) (PoolState, error) {
	states, err := c.poolStates(ctx)
	if err != nil {
		return PoolState{}, err
	}
	for _, pool := range states {
		if pool.Name == name {
			return pool, nil
		}
	}
	return PoolState{}, fmt.Errorf("pool %q does not exist", name)
}

func (c *Container) checkPoolID(ctx context.Context, expected PoolState) error {
	pool, err := c.poolState(ctx, expected.Name)
	if err != nil {
		return err
	}
	if pool.ID != expected.ID {
		return fmt.Errorf("pool %q was replaced; refusing policy mutation", expected.Name)
	}
	return nil
}

func (c *Container) poolRulePlacement(ctx context.Context, id int) (PoolConfig, error) {
	data, err := c.Ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
	if err != nil {
		return PoolConfig{}, err
	}
	var rules []struct {
		ID    int `json:"rule_id"`
		Steps []struct {
			Op       string `json:"op"`
			ItemName string `json:"item_name"`
			Type     string `json:"type"`
			Num      *int   `json:"num"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(data, &rules); err != nil || rules == nil {
		return PoolConfig{}, errors.New("decode native CRUSH rules")
	}
	for _, rule := range rules {
		if rule.ID != id {
			continue
		}
		// Only the simple take/choose/emit rule generated by this fixture is
		// understood. Reject custom algorithms rather than miscount domains.
		if len(rule.Steps) != 3 || rule.Steps[0].Op != "take" || rule.Steps[2].Op != "emit" {
			break
		}
		choose := rule.Steps[1]
		if choose.Num == nil || *choose.Num != 0 {
			break
		}
		if choose.Op != "chooseleaf_firstn" && choose.Op != "choose_firstn" {
			break
		}
		if choose.Type != "osd" && choose.Type != "host" && choose.Type != "rack" {
			break
		}
		if choose.Op == "choose_firstn" && choose.Type != "osd" {
			break
		}
		root, class, _ := strings.Cut(rule.Steps[0].ItemName, "~")
		if root == "" {
			break
		}
		return PoolConfig{CRUSHRoot: root, DeviceClass: class, FailureDomain: choose.Type}, nil
	}
	return PoolConfig{}, fmt.Errorf("native CRUSH rule %d is not a supported simple osd/host/rack rule", id)
}

// Count native buckets rather than creation-time placement descriptions. A
// completed external move, class or weight edit is reflected in this snapshot.
func (c *Container) nativePoolDomains(ctx context.Context, placement PoolConfig) (int, error) {
	data, err := c.Ceph(ctx, "osd", "crush", "dump", "--format", "json")
	if err != nil {
		return 0, err
	}
	type bucket struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Type  string `json:"type_name"`
		Items []struct {
			ID     int     `json:"id"`
			Weight float64 `json:"weight"`
		} `json:"items"`
	}
	var dump struct {
		Devices []struct {
			ID    int    `json:"id"`
			Class string `json:"class"`
		} `json:"devices"`
		Buckets []bucket `json:"buckets"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Devices == nil || dump.Buckets == nil {
		return 0, errors.New("decode native CRUSH hierarchy")
	}
	buckets := make(map[int]bucket)
	classes := make(map[int]string)
	var root *bucket
	for _, node := range dump.Buckets {
		if node.ID >= 0 {
			return 0, errors.New("invalid native CRUSH bucket ID")
		}
		buckets[node.ID] = node
		if node.Name == placement.CRUSHRoot && node.Type == "root" {
			copy := node
			root = &copy
		}
	}
	if root == nil {
		return 0, errors.New("native CRUSH root is missing")
	}
	for _, device := range dump.Devices {
		classes[device.ID] = device.Class
	}
	domains := make(map[int]struct{})
	visiting := make(map[int]bool)
	var visit func(int, int, bool) error
	visit = func(id, domain int, hasDomain bool) error {
		if id >= 0 {
			owned := c.osds[id]
			class, exists := classes[id]
			if owned == nil || owned.purged || !exists || (placement.DeviceClass != "" && class != placement.DeviceClass) {
				return nil
			}
			if placement.FailureDomain == "osd" {
				domain, hasDomain = id, true
			}
			if hasDomain {
				domains[domain] = struct{}{}
			}
			return nil
		}
		node, exists := buckets[id]
		if !exists || visiting[id] {
			return errors.New("invalid native CRUSH hierarchy reference")
		}
		visiting[id] = true
		defer delete(visiting, id)
		if node.Type == placement.FailureDomain {
			domain, hasDomain = id, true
		}
		for _, item := range node.Items {
			if item.Weight > 0 {
				if err := visit(item.ID, domain, hasDomain); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(root.ID, 0, false); err != nil {
		return 0, err
	}
	return len(domains), nil
}
