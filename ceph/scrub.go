package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// InconsistentObject is one object reported by the last deep scrub of a PG.
// Errors are object-level native error names and UnionShardErrors combine the
// per-shard names, such as read_error or data_digest_mismatch.
type InconsistentObject struct {
	Name, Namespace  string
	Errors           []string
	UnionShardErrors []string
	Shards           []InconsistentShard
}

// InconsistentShard retains one native shard report. Shard is -1 for
// replicated pools.
type InconsistentShard struct {
	OSD, Shard int
	Primary    bool
	Errors     []string
}

var (
	pgIDPattern      = regexp.MustCompile(`^[0-9]+\.[0-9a-f]+$`)
	scrubStampLayout = "2006-01-02T15:04:05.999999-0700"
)

func validatePGID(pgid string) error {
	if !pgIDPattern.MatchString(pgid) {
		return errors.New("invalid native PG ID")
	}
	return nil
}

// RADOS accepts arbitrary object names, but the fixture passes them as CLI
// arguments; reject names that the CLI would parse as options or truncate.
func validateObjectName(name string) error {
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "-") || strings.ContainsRune(name, 0) {
		return errors.New("object name must be non-empty, at most 4096 bytes, without NUL and not start with a dash")
	}
	return nil
}

// InjectObjectDataError makes one replica of an object on an owned OSD return
// a read error to scrub and recovery, through the native injectdataerr debug
// command. The OSD must be in the object's current acting set of a replicated
// pool. The command needs bluestore_debug_inject_read_err, which this method
// enables on that daemon only through its runtime configuration; it is not
// stored centrally and lasts until the OSD restarts. The setting has no effect
// on objects without an injected error.
//
// The injection stays in OSD memory until the replica is rewritten, such as by
// RepairPG, or the OSD restarts. Client reads served by a healthy primary are
// unaffected. Use DeepScrubPG to report the damage and PGInconsistencies,
// HealthDetails (OSD_SCRUB_ERRORS, PG_DAMAGED) or PoolPGs to observe it.
func (c *Container) InjectObjectDataError(ctx context.Context, pool, object string, osdID int) error {
	if err := validateExistingPoolName(pool); err != nil {
		return err
	}
	if err := validateObjectName(object); err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if osd := c.osds[osdID]; osd == nil || osd.purged {
		return fmt.Errorf("OSD %d is not owned by this cluster", osdID)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	state, err := c.poolState(ctx, pool)
	if err != nil {
		return err
	}
	if state.Type != "replicated" {
		return errors.New("data error injection supports replicated pools")
	}
	data, err := c.Ceph(ctx, "osd", "map", pool, object, "--format", "json")
	if err != nil {
		return err
	}
	var mapping struct {
		PoolID *int64 `json:"pool_id"`
		PGID   string `json:"pgid"`
		Acting []int  `json:"acting"`
	}
	if err := json.Unmarshal(data, &mapping); err != nil || mapping.PoolID == nil || *mapping.PoolID != state.ID || validatePGID(mapping.PGID) != nil {
		return errors.New("decode native object mapping")
	}
	if !slices.Contains(mapping.Acting, osdID) {
		return fmt.Errorf("OSD %d is not in the acting set %v of PG %s", osdID, mapping.Acting, mapping.PGID)
	}
	daemon := "osd." + strconv.Itoa(osdID)
	if _, err := c.Ceph(ctx, "tell", daemon, "config", "set", "bluestore_debug_inject_read_err", "true"); err != nil {
		return fmt.Errorf("enable data error injection on %s: %w", daemon, err)
	}
	if err := c.checkPoolID(ctx, state); err != nil {
		return err
	}
	if _, err := c.Ceph(ctx, "tell", daemon, "injectdataerr", pool, object); err != nil {
		return fmt.Errorf("inject data error on %s: %w", daemon, err)
	}
	return nil
}

// DeepScrubPG requests a native deep scrub of one PG and waits until the PG
// reports a deep-scrub stamp newer than before the request. Inconsistencies
// are reported, not repaired. The noscrub and nodeep-scrub flags or an
// unavailable primary can keep the wait from finishing before its deadline.
func (c *Container) DeepScrubPG(ctx context.Context, pgid string) error {
	_, err := c.scrubPG(ctx, pgid, "deep-scrub")
	return err
}

// RepairPG requests a native repair of one PG and waits until the PG reports a
// newer deep-scrub stamp and is no longer inconsistent. Repair rewrites bad
// replicas from an authoritative copy; it cannot recover an object without
// one, and the wait then ends with the PG still inconsistent.
func (c *Container) RepairPG(ctx context.Context, pgid string) error {
	state, err := c.scrubPG(ctx, pgid, "repair")
	if err == nil && strings.Contains(state, "inconsistent") {
		return fmt.Errorf("PG %s is still %s after repair", pgid, state)
	}
	return err
}

func (c *Container) scrubPG(ctx context.Context, pgid, operation string) (string, error) {
	if err := validatePGID(pgid); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return "", err
	}
	before, _, err := c.pgScrubReport(ctx, pgid)
	if err == nil {
		_, err = c.Ceph(ctx, "pg", operation, pgid)
	}
	c.mu.Unlock()
	if err != nil {
		return "", err
	}
	var state string
	err = c.poll(ctx, func() (bool, error) {
		if err := c.lockTopology(ctx); err != nil {
			return false, err
		}
		defer c.mu.Unlock()
		stamp, current, err := c.pgScrubReport(ctx, pgid)
		if err != nil {
			return false, err
		}
		state = current
		// Repair reports its deep scrub stamp before clearing the state.
		return stamp.After(before) && !strings.Contains(current, "repair") && !strings.Contains(current, "scrubbing"), nil
	})
	return state, err
}

// Caller holds c.mu. The PG's pool is resolved by its native ID each time.
func (c *Container) pgScrubReport(ctx context.Context, pgid string) (time.Time, string, error) {
	if err := c.poolPolicyReady(); err != nil {
		return time.Time{}, "", err
	}
	poolID, _, _ := strings.Cut(pgid, ".")
	pools, err := c.poolStates(ctx)
	if err != nil {
		return time.Time{}, "", err
	}
	name := ""
	for _, pool := range pools {
		if strconv.FormatInt(pool.ID, 10) == poolID {
			name = pool.Name
		}
	}
	if name == "" {
		return time.Time{}, "", fmt.Errorf("pool of PG %s does not exist", pgid)
	}
	data, err := c.Ceph(ctx, "pg", "ls-by-pool", name, "--format", "json")
	if err != nil {
		return time.Time{}, "", err
	}
	var native struct {
		PGs []struct {
			PGID  string `json:"pgid"`
			State string `json:"state"`
			Stamp string `json:"last_deep_scrub_stamp"`
		} `json:"pg_stats"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return time.Time{}, "", errors.New("decode native PG scrub reports")
	}
	for _, pg := range native.PGs {
		if pg.PGID != pgid {
			continue
		}
		stamp, err := time.Parse(scrubStampLayout, pg.Stamp)
		if err != nil || pg.State == "" {
			return time.Time{}, "", fmt.Errorf("PG %s has no valid native deep-scrub report", pgid)
		}
		return stamp, pg.State, nil
	}
	return time.Time{}, "", fmt.Errorf("PG %s is not reported", pgid)
}

// PGInconsistencies lists the objects that the last deep scrub of a PG found
// inconsistent. An empty result means that scrub found none; it is not a new
// scrub. A PG whose results are no longer available returns an error.
func (c *Container) PGInconsistencies(ctx context.Context, pgid string) ([]InconsistentObject, error) {
	if err := validatePGID(pgid); err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return nil, err
	}
	data, err := command(ctx, control, "rados", "list-inconsistent-obj", pgid, "--format=json")
	if err != nil {
		return nil, err
	}
	return decodeInconsistentObjects(data)
}

func decodeInconsistentObjects(data []byte) ([]InconsistentObject, error) {
	var native struct {
		Inconsistents *[]struct {
			Object struct {
				Name      *string `json:"name"`
				Namespace string  `json:"nspace"`
			} `json:"object"`
			Errors      []string `json:"errors"`
			UnionErrors []string `json:"union_shard_errors"`
			Shards      []struct {
				OSD     *int     `json:"osd"`
				Shard   *int     `json:"shard"`
				Primary bool     `json:"primary"`
				Errors  []string `json:"errors"`
			} `json:"shards"`
		} `json:"inconsistents"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Inconsistents == nil {
		return nil, errors.New("decode native inconsistent objects")
	}
	result := make([]InconsistentObject, 0, len(*native.Inconsistents))
	for _, item := range *native.Inconsistents {
		if item.Object.Name == nil {
			return nil, errors.New("native inconsistent object has no name")
		}
		object := InconsistentObject{Name: *item.Object.Name, Namespace: item.Object.Namespace,
			Errors: slices.Clone(item.Errors), UnionShardErrors: slices.Clone(item.UnionErrors)}
		for _, shard := range item.Shards {
			if shard.OSD == nil {
				return nil, errors.New("native inconsistent shard has no OSD")
			}
			number := -1
			if shard.Shard != nil {
				number = *shard.Shard
			}
			object.Shards = append(object.Shards, InconsistentShard{OSD: *shard.OSD, Shard: number, Primary: shard.Primary, Errors: slices.Clone(shard.Errors)})
		}
		result = append(result, object)
	}
	return result, nil
}
