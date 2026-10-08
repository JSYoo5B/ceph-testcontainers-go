package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/google/uuid"
)

// PoolUsageSnapshot is one monitor-reported pool usage observation. StoredBytes
// estimates logical user data (DATA + OMAP); AllocatedBytes includes replication
// or EC and allocation overhead, but not the BlueStore database. All sizes are
// bytes, Objects is a logical count, and UsedRatio is a fraction in [0, 1].
// MaxAvailableBytes estimates writable logical capacity. PG reports arrive
// asynchronously: this is neither a read-after-write barrier nor a quota check.
type PoolUsageSnapshot struct {
	FSID, Name         string
	ID                 int64
	StoredBytes        uint64
	StoredDataBytes    uint64
	StoredOMAPBytes    uint64
	AllocatedBytes     uint64
	AllocatedDataBytes uint64
	AllocatedOMAPBytes uint64
	Objects            uint64
	MaxAvailableBytes  uint64
	UsedRatio          float64
}

// PoolUsage reads ceph df detail through the fixture's control CLI. It checks
// the original bootstrap FSID and exact native pool ID/name before and after
// the observation. A pool without a statistics row is unavailable, not empty.
// All failures, including cancellation after a query, return a zero snapshot.
// This method makes no writes, retries or background workers. External pool or
// control configuration writers must not race it; the native reads are not an
// atomic transaction. Poll with a caller deadline for eventual assertions.
func (c *Container) PoolUsage(ctx context.Context, name string) (PoolUsageSnapshot, error) {
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
		return PoolUsageSnapshot{}, errors.New("ceph cluster is terminated")
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return PoolUsageSnapshot{}, err
	}
	if control == nil {
		return PoolUsageSnapshot{}, errors.New("ceph control container is unavailable")
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return PoolUsageSnapshot{}, err
	}
	fsid, identityErr := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if identityErr != nil || !confirmed {
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

func poolUsageID(data []byte, name string) (int64, error) {
	var native []struct {
		ID   *int64 `json:"pool_id"`
		Name string `json:"pool_name"`
	}
	if err := poolUsageJSON(data, &native); err != nil || native == nil {
		return 0, errors.New("decode native pool usage identities")
	}
	seenIDs, seenNames := map[int64]bool{}, map[string]bool{}
	var id int64
	found := false
	for _, pool := range native {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == "" || seenIDs[*pool.ID] || seenNames[pool.Name] {
			return 0, errors.New("incomplete or ambiguous native pool usage identities")
		}
		seenIDs[*pool.ID], seenNames[pool.Name] = true, true
		if pool.Name == name {
			id, found = *pool.ID, true
		}
	}
	if !found {
		return 0, fmt.Errorf("pool %q does not exist", name)
	}
	return id, nil
}

func decodePoolUsage(data []byte, fsid, name string, id int64) (PoolUsageSnapshot, error) {
	var native struct {
		Pools []struct {
			ID    *int64 `json:"id"`
			Name  string `json:"name"`
			Stats *struct {
				Stored     *uint64  `json:"stored"`
				StoredData *uint64  `json:"stored_data"`
				StoredOMAP *uint64  `json:"stored_omap"`
				Allocated  *uint64  `json:"bytes_used"`
				Data       *uint64  `json:"data_bytes_used"`
				OMAP       *uint64  `json:"omap_bytes_used"`
				Objects    *uint64  `json:"objects"`
				Available  *uint64  `json:"max_avail"`
				Ratio      *float64 `json:"percent_used"`
			} `json:"stats"`
		} `json:"pools"`
	}
	if err := poolUsageJSON(data, &native); err != nil || native.Pools == nil {
		return PoolUsageSnapshot{}, errors.New("decode native pool usage")
	}
	seenIDs, seenNames := map[int64]bool{}, map[string]bool{}
	var result PoolUsageSnapshot
	found := false
	for _, pool := range native.Pools {
		if pool.ID == nil || *pool.ID < 0 || pool.Name == "" || seenIDs[*pool.ID] || seenNames[pool.Name] {
			return PoolUsageSnapshot{}, errors.New("incomplete or ambiguous native pool usage identity")
		}
		seenIDs[*pool.ID], seenNames[pool.Name] = true, true
		if pool.Name != name && *pool.ID != id {
			continue
		}
		if pool.Name != name || *pool.ID != id {
			return PoolUsageSnapshot{}, fmt.Errorf("pool %q usage identity changed", name)
		}
		s := pool.Stats
		if s == nil || s.Stored == nil || s.StoredData == nil || s.StoredOMAP == nil || s.Allocated == nil || s.Data == nil || s.OMAP == nil || s.Objects == nil || s.Available == nil || s.Ratio == nil || math.IsNaN(*s.Ratio) || math.IsInf(*s.Ratio, 0) || *s.Ratio < 0 || *s.Ratio > 1 {
			return PoolUsageSnapshot{}, errors.New("incomplete or invalid native pool usage statistics")
		}
		if *s.StoredData > math.MaxUint64-*s.StoredOMAP || *s.Stored != *s.StoredData+*s.StoredOMAP || *s.Data > math.MaxUint64-*s.OMAP || *s.Allocated != *s.Data+*s.OMAP {
			return PoolUsageSnapshot{}, errors.New("inconsistent native pool usage totals")
		}
		result = PoolUsageSnapshot{FSID: fsid, Name: name, ID: id, StoredBytes: *s.Stored, StoredDataBytes: *s.StoredData, StoredOMAPBytes: *s.StoredOMAP, AllocatedBytes: *s.Allocated, AllocatedDataBytes: *s.Data, AllocatedOMAPBytes: *s.OMAP, Objects: *s.Objects, MaxAvailableBytes: *s.Available, UsedRatio: *s.Ratio}
		found = true
	}
	if !found {
		return PoolUsageSnapshot{}, fmt.Errorf("pool %q usage statistics are not reported", name)
	}
	return result, nil
}

// Reject duplicate fields and trailing documents; neither may silently change
// an assertion's native identity or numeric value. Unknown fields are allowed.
func poolUsageJSON(data []byte, value any) error {
	if len(data) == 0 || len(data) > 4<<20 {
		return errors.New("invalid pool usage JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate pool usage field")
				}
				seen[name] = true
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid pool usage collection")
		}
		_, err = decoder.Token()
		return err
	}
	if walk() != nil {
		return errors.New("invalid pool usage JSON")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("invalid pool usage JSON")
	}
	if json.Unmarshal(data, value) != nil {
		return errors.New("invalid pool usage JSON")
	}
	return nil
}
