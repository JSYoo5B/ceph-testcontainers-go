package harness

import (
	"encoding/json"
	"errors"
	"fmt"
)

// RGWShardLag is one data log shard on which a destination zone's data sync
// marker for a source zone differs from the source zone's current data log
// marker.
type RGWShardLag struct {
	Shard int
	// Source is the source zone's data log marker and Destination the
	// destination's sync marker for the shard; Status is the destination's
	// sync state of the shard.
	Source, Destination, Status string
}

// RGWDataSyncLag compares a source zone's `radosgw-admin datalog status`
// with a destination zone's `radosgw-admin data sync status --source-zone`
// for it, shard by shard. Shards whose source marker is empty have no
// changes and are left out. Both outputs must cover the same shards once
// each.
func RGWDataSyncLag(datalog, syncStatus []byte) ([]RGWShardLag, error) {
	var source []struct {
		Marker *string `json:"marker"`
	}
	if err := json.Unmarshal(datalog, &source); err != nil {
		return nil, fmt.Errorf("datalog status: %w", err)
	}
	var destination struct {
		SyncStatus struct {
			Info struct {
				NumShards *int `json:"num_shards"`
			} `json:"info"`
			Markers []struct {
				Key *int `json:"key"`
				Val *struct {
					Status string `json:"status"`
					Marker string `json:"marker"`
				} `json:"val"`
			} `json:"markers"`
		} `json:"sync_status"`
	}
	if err := json.Unmarshal(syncStatus, &destination); err != nil {
		return nil, fmt.Errorf("data sync status: %w", err)
	}
	info, markers := destination.SyncStatus.Info, destination.SyncStatus.Markers
	if info.NumShards == nil || *info.NumShards != len(source) || len(markers) != len(source) {
		return nil, fmt.Errorf("data sync status covers %d of %v shards, datalog status %d", len(markers), info.NumShards, len(source))
	}
	seen := make([]bool, len(source))
	var lag []RGWShardLag
	for _, m := range markers {
		if m.Key == nil || *m.Key < 0 || *m.Key >= len(source) || seen[*m.Key] || m.Val == nil {
			return nil, errors.New("data sync status has a missing, duplicated or invalid shard")
		}
		seen[*m.Key] = true
		want := source[*m.Key].Marker
		if want == nil {
			return nil, fmt.Errorf("datalog status shard %d has no marker", *m.Key)
		}
		if *want != "" && *want != m.Val.Marker {
			lag = append(lag, RGWShardLag{Shard: *m.Key, Source: *want, Destination: m.Val.Marker, Status: m.Val.Status})
		}
	}
	return lag, nil
}
