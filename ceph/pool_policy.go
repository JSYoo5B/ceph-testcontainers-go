package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
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

var existingPoolNamePattern = naming.ExistingPool

func validateExistingPoolName(name string) error {
	if !existingPoolNamePattern.MatchString(name) {
		return invalid(errors.New("invalid native pool name"))
	}
	return nil
}

func (c *Container) poolPolicyReady() error {
	if c.closed {
		return errClusterTerminated
	}
	if c.conn().cliContainer() == nil {
		return errors.New("ceph control container is unavailable")
	}
	return nil
}

func (c *Container) poolStates(ctx context.Context) ([]PoolState, error) {
	data, err := c.conn().ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodePoolStates(data)
}

// decodePoolStates decodes the output of osd pool ls detail.
func decodePoolStates(data []byte) ([]PoolState, error) {
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
	data, err := c.conn().ceph(ctx, "osd", "crush", "rule", "dump", "--format", "json")
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
// The caller holds c.mu, which guards the OSD registry it reads.
func (c *Container) nativePoolDomains(ctx context.Context, placement PoolConfig) (int, error) {
	data, err := c.conn().ceph(ctx, "osd", "crush", "dump", "--format", "json")
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
			owned := c.osdHandles.get(id)
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
