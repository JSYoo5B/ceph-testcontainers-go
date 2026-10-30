package ceph

import (
	"errors"
	"fmt"
	"math"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
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

// Reject duplicate fields, aliases of known fields, nulls of known fields and
// trailing documents; none may silently change an assertion's native identity
// or numeric value. Unrelated unknown fields are allowed.
func poolUsageJSON(data []byte, value any) error {
	if strictjson.Decode(data, value, strictjson.Options{MaxBytes: 4 << 20, RejectNull: true}) != nil {
		return errors.New("invalid pool usage JSON")
	}
	return nil
}
