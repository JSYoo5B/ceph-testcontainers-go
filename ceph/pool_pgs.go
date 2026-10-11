package ceph

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// PoolPGSnapshot contains one pool's reported PGs, bracketed by two native
// OSDMap observations. PoolBefore and PoolAfter reuse PoolState policy fields;
// their names and IDs agree, but policies and OSDMap epochs may differ.
// These reads are not an atomic placement snapshot. PGReady is the native MGR
// flag: false can accompany reports, and true can still accompany unknown PGs.
type PoolPGSnapshot struct {
	FSID                                string
	PoolBefore, PoolAfter               PoolState
	OSDMapEpochBefore, OSDMapEpochAfter uint32
	PGReady                             bool
	PGs                                 []PGState
}

// PGState retains native reported placement and statistics. Up and Acting
// preserve order, empty vectors and repeated missing EC shard slots
// (CRUSH_ITEM_NONE, 2147483647). Primaries are explicit reported values, not
// inferred from the vectors; -1 means no primary. Epochs can be zero or older
// than the OSDMap brackets, and State retains unknown or future native states.
// A reported OSD may have been removed or be outside fixture ownership.
type PGState struct {
	PGID, State                                 string
	Up, Acting                                  []int
	UpPrimary, ActingPrimary                    int
	ReportedEpoch, MappingEpoch, LastEpochClean uint32
	ReportedSequence                            uint64
	StatsInvalid                                bool
	Stats                                       PGStats
}

// PGStats contains signed native PG counters. Bytes counts logical payload,
// not physical disk allocation; ObjectCopies is separate from logical Objects.
// Reports arrive asynchronously, including when StatsInvalid is false, so
// these counters are not an I/O visibility or redistribution barrier.
type PGStats struct {
	Objects, Bytes, ObjectCopies                      int64
	ObjectsDegraded, ObjectsMisplaced, ObjectsUnfound int64
}

// PoolPGs observes one existing pool through one captured control CLI handle.
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
func (c *Container) PoolPGs(ctx context.Context, name string) (PoolPGSnapshot, error) {
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
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return PoolPGSnapshot{}, err
	}
	if control == nil {
		return PoolPGSnapshot{}, clientOperationError(ctx, "ceph control container is unavailable", nil)
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return PoolPGSnapshot{}, err
	}
	fsid, identityErr := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if identityErr != nil || !confirmed {
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

func decodePoolPGOSDMap(data []byte, fsid, name string) (PoolState, uint32, error) {
	var native struct {
		FSID  *string `json:"fsid"`
		Epoch *uint32 `json:"epoch"`
		Pools *[]struct {
			ID                 *int64  `json:"pool"`
			Name               *string `json:"pool_name"`
			Type               *uint8  `json:"type"`
			Size               *uint8  `json:"size"`
			MinSize            *uint8  `json:"min_size"`
			PGNum              *uint32 `json:"pg_num"`
			PGNumTarget        *uint32 `json:"pg_num_target"`
			PGNumPending       *uint32 `json:"pg_num_pending"`
			PGPlacementNum     *uint32 `json:"pg_placement_num"`
			PGPlacementTarget  *uint32 `json:"pg_placement_num_target"`
			CRUSHRule          *uint8  `json:"crush_rule"`
			AutoscaleMode      *string `json:"pg_autoscale_mode"`
			ErasureCodeProfile *string `json:"erasure_code_profile"`
			Flags              *string `json:"flags_names"`
			MaxBytes           *uint64 `json:"quota_max_bytes"`
			MaxObjects         *uint64 `json:"quota_max_objects"`
		} `json:"pools"`
	}
	failure := func() (PoolState, uint32, error) {
		return PoolState{}, 0, errors.New("incomplete or invalid native pool PG identity")
	}
	// The existing decoder bounds size/depth, rejects duplicate and aliased
	// known keys (including Unicode folds), and permits unrelated future fields.
	if err := healthDetailsJSON(data, &native); err != nil || native.FSID == nil || *native.FSID != fsid || native.Epoch == nil || native.Pools == nil {
		return failure()
	}
	seenIDs, seenNames := map[int64]bool{}, map[string]bool{}
	var result PoolState
	found := false
	for _, pool := range *native.Pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == nil || *pool.Name == "" || seenIDs[*pool.ID] || seenNames[*pool.Name] {
			return failure()
		}
		seenIDs[*pool.ID], seenNames[*pool.Name] = true, true
		if *pool.Name != name {
			continue
		}
		if pool.Type == nil || pool.Size == nil || *pool.Size == 0 || pool.MinSize == nil || *pool.MinSize == 0 || pool.PGNum == nil || int(*pool.PGNum) < 1 || pool.CRUSHRule == nil || pool.AutoscaleMode == nil || pool.ErasureCodeProfile == nil || pool.Flags == nil || pool.MaxBytes == nil || pool.MaxObjects == nil {
			return failure()
		}
		kind := "replicated"
		if *pool.Type == 3 {
			kind = "erasure"
		} else if *pool.Type != 1 {
			return failure()
		}
		result = PoolState{ID: *pool.ID, Name: *pool.Name, Type: kind, Size: int(*pool.Size), MinSize: int(*pool.MinSize), PGNum: int(*pool.PGNum),
			CRUSHRule: int(*pool.CRUSHRule), AutoscaleMode: *pool.AutoscaleMode, ErasureCodeProfile: *pool.ErasureCodeProfile,
			Flags: *pool.Flags, Quota: PoolQuota{MaxBytes: *pool.MaxBytes, MaxObjects: *pool.MaxObjects}}
		// Progress fields are optional; an omitted value remains zero.
		for _, field := range []struct {
			value  *uint32
			target *int
		}{{pool.PGNumTarget, &result.PGNumTarget}, {pool.PGNumPending, &result.PGNumPending}, {pool.PGPlacementNum, &result.PGPlacementNum}, {pool.PGPlacementTarget, &result.PGPlacementNumTarget}} {
			if field.value != nil {
				*field.target = int(*field.value)
			}
		}
		found = true
	}
	if !found {
		return failure()
	}
	return result, *native.Epoch, nil
}

func decodePoolPGReports(data []byte, poolID int64) (bool, []PGState, error) {
	var native struct {
		Ready *bool `json:"pg_ready"`
		PGs   *[]struct {
			PGID             *string  `json:"pgid"`
			State            *string  `json:"state"`
			Up               *[]int32 `json:"up"`
			Acting           *[]int32 `json:"acting"`
			UpPrimary        *int32   `json:"up_primary"`
			ActingPrimary    *int32   `json:"acting_primary"`
			ReportedEpoch    *uint32  `json:"reported_epoch"`
			ReportedSequence *uint64  `json:"reported_seq"`
			MappingEpoch     *uint32  `json:"mapping_epoch"`
			LastEpochClean   *uint32  `json:"last_epoch_clean"`
			StatsInvalid     *bool    `json:"stats_invalid"`
			Stats            *struct {
				Objects      *int64 `json:"num_objects"`
				Bytes        *int64 `json:"num_bytes"`
				ObjectCopies *int64 `json:"num_object_copies"`
				Degraded     *int64 `json:"num_objects_degraded"`
				Misplaced    *int64 `json:"num_objects_misplaced"`
				Unfound      *int64 `json:"num_objects_unfound"`
			} `json:"stat_sum"`
		} `json:"pg_stats"`
	}
	failure := func() (bool, []PGState, error) {
		return false, nil, errors.New("incomplete or invalid native pool PG reports")
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.Ready == nil {
		return failure()
	}
	// A successful native query omits pg_stats when the filtered PG set is
	// empty. The strict decoder still rejects an explicit null for this key.
	if native.PGs == nil {
		return *native.Ready, []PGState{}, nil
	}
	result := make([]PGState, 0, len(*native.PGs))
	seen := make(map[string]bool, len(*native.PGs))
	for _, pg := range *native.PGs {
		if pg.PGID == nil || !poolPGIdentity(*pg.PGID, poolID) || seen[*pg.PGID] || pg.State == nil || *pg.State == "" || pg.Up == nil || pg.Acting == nil || pg.UpPrimary == nil || pg.ActingPrimary == nil || pg.ReportedEpoch == nil || pg.ReportedSequence == nil || pg.MappingEpoch == nil || pg.LastEpochClean == nil || pg.StatsInvalid == nil || pg.Stats == nil {
			return failure()
		}
		s := pg.Stats
		if s.Objects == nil || s.Bytes == nil || s.ObjectCopies == nil || s.Degraded == nil || s.Misplaced == nil || s.Unfound == nil {
			return failure()
		}
		seen[*pg.PGID] = true
		up, acting := make([]int, len(*pg.Up)), make([]int, len(*pg.Acting))
		for i, id := range *pg.Up {
			up[i] = int(id)
		}
		for i, id := range *pg.Acting {
			acting[i] = int(id)
		}
		result = append(result, PGState{PGID: *pg.PGID, State: *pg.State, Up: up, Acting: acting,
			UpPrimary: int(*pg.UpPrimary), ActingPrimary: int(*pg.ActingPrimary), ReportedEpoch: *pg.ReportedEpoch,
			ReportedSequence: *pg.ReportedSequence, MappingEpoch: *pg.MappingEpoch, LastEpochClean: *pg.LastEpochClean, StatsInvalid: *pg.StatsInvalid,
			Stats: PGStats{Objects: *s.Objects, Bytes: *s.Bytes, ObjectCopies: *s.ObjectCopies,
				ObjectsDegraded: *s.Degraded, ObjectsMisplaced: *s.Misplaced, ObjectsUnfound: *s.Unfound}})
	}
	return *native.Ready, result, nil
}

func poolPGIdentity(value string, poolID int64) bool {
	pool, seed, found := strings.Cut(value, ".")
	if !found || poolID < 0 || pool != strconv.FormatInt(poolID, 10) {
		return false
	}
	ps, err := strconv.ParseUint(seed, 16, 32)
	return err == nil && seed == strconv.FormatUint(ps, 16)
}
