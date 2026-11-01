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

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
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
		return invalid(errors.New("invalid native PG ID"))
	}
	return nil
}

// PGID identifies one placement group as "<pool>.<hex>": the decimal native
// pool ID, a dot and the lowercase hexadecimal PG number, for example "1.0" or
// "3.1f". Pool.PGs reports PGs with this type, so a reported PG passes straight
// to DeepScrubPG, RepairPG and PGInconsistencies. An untyped string constant
// converts to PGID without validation, so these methods check the grammar
// again and refuse an invalid value; ParsePGID checks it up front.
type PGID string

// ParsePGID returns s as a PGID when it matches the grammar
// "<pool>.<hex>", which is exactly what DeepScrubPG, RepairPG and
// PGInconsistencies accept. Otherwise it returns an error that matches
// ErrInvalidRequest and the zero PGID.
//
//purpose:observation
func ParsePGID(s string) (PGID, error) {
	if err := validatePGID(s); err != nil {
		return "", err
	}
	return PGID(s), nil
}

// String returns the native PG ID.
func (p PGID) String() string { return string(p) }

// DeepScrubPG requests a native deep scrub of one PG and waits until the PG
// reports a deep-scrub stamp newer than before the request. Inconsistencies
// are reported, not repaired. The noscrub and nodeep-scrub flags or an
// unavailable primary can keep the wait from finishing before its deadline.
//
// fault.InjectObjectDataError creates a damaged replica to observe with
// DeepScrubPG, Observer.PGInconsistencies and RepairPG; ceph osd map <pool>
// <object> --format json reports an object's PG and acting set.
//
//purpose:condition,observation
func (c *Container) DeepScrubPG(ctx context.Context, pgid PGID) error {
	_, err := c.scrubPG(ctx, pgid, "deep-scrub")
	return err
}

// RepairPG requests a native repair of one PG and waits until the PG reports a
// newer deep-scrub stamp and has finished repairing; it returns an error if the
// PG is still inconsistent then. Repair rewrites bad
// replicas from an authoritative copy; it cannot recover an object without
// one, and the wait then ends with the PG still inconsistent.
//
//purpose:condition,observation
func (c *Container) RepairPG(ctx context.Context, pgid PGID) error {
	state, err := c.scrubPG(ctx, pgid, "repair")
	if err == nil && strings.Contains(state, "inconsistent") {
		return fmt.Errorf("PG %s is still %s after repair", pgid, state)
	}
	return err
}

func (c *Container) scrubPG(ctx context.Context, pgid PGID, operation string) (string, error) {
	if err := validatePGID(string(pgid)); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return "", err
	}
	before, _, err := c.pgScrubReport(ctx, pgid)
	if err == nil {
		_, err = c.conn().ceph(ctx, "pg", operation, string(pgid))
	}
	c.mu.Unlock()
	if err != nil {
		return "", err
	}
	var state string
	err = clusterutil.Poll(ctx, func() (bool, error) {
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
func (c *Container) pgScrubReport(ctx context.Context, pgid PGID) (time.Time, string, error) {
	if err := c.poolPolicyReady(); err != nil {
		return time.Time{}, "", err
	}
	poolID, _, _ := strings.Cut(string(pgid), ".")
	pools, err := poolCatalog{c.conn()}.states(ctx)
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
	data, err := c.conn().ceph(ctx, "pg", "ls-by-pool", name, "--format", "json")
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
		if PGID(pg.PGID) != pgid {
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
