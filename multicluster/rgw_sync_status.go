package multicluster

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

// RGWMetadataSyncStatus includes only native state/identity/shard counts. It
// omits metadata keys, markers and credentials from native diagnostic output.
type RGWMetadataSyncStatus struct {
	State, PeriodID           string
	RealmEpoch                uint64
	Shards, IncrementalShards int
	CaughtUp                  bool
}

// RGWDataSyncStatus summarizes the native comparison with one source's log.
// CaughtUp requires a running sync, complete incremental shards, no recovering
// shards and the native remote-log comparison. Disabled pipes do not count as
// caught up merely because an object's absence is expected.
type RGWDataSyncStatus struct {
	SourceZone, SourceZoneID              string
	Syncing, CaughtUp                     bool
	Shards, IncrementalShards, FullShards int
	BehindShards, RecoveringShards        int
}

type RGWSyncStatus struct {
	RealmID, PeriodID, Zone, ZoneID, ZonegroupID string
	RealmEpoch                                   uint64
	MetadataMaster                               bool
	Metadata                                     RGWMetadataSyncStatus
	DataSources                                  []RGWDataSyncStatus
}

// SyncStatus observes native metadata/data readiness in an attached owned zone.
// Native human sync status is required for remote-log comparison even when the
// CLI is passed --format json. The structured metadata state separately guards
// period/realm epoch and every shard. This is a point-in-time observation;
// applications should quiesce writes and still verify their own S3 payloads.
func (f *RGWMultisite) SyncStatus(ctx context.Context, zoneName string) (RGWSyncStatus, error) {
	if f == nil {
		return RGWSyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return RGWSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	return f.syncStatus(ctx, zoneName)
}

// WaitSyncReady waits up to four minutes, bounded by the caller's context, for
// metadata and the explicitly requested source-zone data streams to catch up.
// No sources means metadata-only readiness. Native sync leases survive abrupt
// gateway restarts until expiration, so this method does not change lease TTLs,
// unlock objects or restart daemons to shorten normal recovery.
func (f *RGWMultisite) WaitSyncReady(ctx context.Context, zoneName string, sourceZones ...string) (RGWSyncStatus, error) {
	if f == nil {
		return RGWSyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	sourceZones = slices.Clone(sourceZones)
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return RGWSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	if _, err := f.syncStatusZones(zoneName, sourceZones); err != nil {
		return RGWSyncStatus{}, err
	}
	var last RGWSyncStatus
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 30*time.Second)
		last, lastErr = f.syncStatus(attemptCtx, zoneName)
		attemptCancel()
		if err := ctx.Err(); err != nil {
			return last, fmt.Errorf("wait RGW zone %s sync readiness: %w", zoneName, err)
		}
		if lastErr == nil && syncStatusReady(last, sourceZones) {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait RGW zone %s sync readiness: %w; metadata_ready=%t; last_error=%v", zoneName, ctx.Err(), last.Metadata.CaughtUp, lastErr)
		case <-time.After(time.Second):
		}
	}
}

func (f *RGWMultisite) syncStatusZones(name string, sources []string) (*rgwZoneState, error) {
	if f.closed {
		return nil, errors.New("RGW multisite is terminated")
	}
	var target *rgwZoneState
	states := f.zoneStates()
	for _, zone := range states {
		if zone.Name == name && zone.client != nil {
			target = zone
		}
	}
	if target == nil {
		return nil, errors.New("RGW sync status requires an attached owned zone")
	}
	seen := map[string]bool{}
	for _, source := range sources {
		if source == name || seen[source] || !slices.ContainsFunc(states, func(z *rgwZoneState) bool { return z.Name == source && z.Zonegroup == target.Zonegroup }) {
			return nil, errors.New("RGW readiness data sources must be unique other owned zones in the same zonegroup")
		}
		seen[source] = true
	}
	return target, nil
}

func (f *RGWMultisite) syncStatus(ctx context.Context, zoneName string) (RGWSyncStatus, error) {
	zone, err := f.syncStatusZones(zoneName, nil)
	if err != nil {
		return RGWSyncStatus{}, err
	}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return RGWSyncStatus{}, err
	}
	id, epoch, err := syncCommittedPeriodIdentity(current)
	if err != nil {
		return RGWSyncStatus{}, err
	}
	result := RGWSyncStatus{RealmID: f.RealmID, PeriodID: id, RealmEpoch: epoch, Zone: zone.Name, ZoneID: zone.ID, ZonegroupID: f.groupIDs[zone.Zonegroup], MetadataMaster: master.ID == zone.ID}
	data, err := f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "period", "get")
	if err != nil {
		return result, err
	}
	local, err := syncJSON(data)
	if err != nil {
		return result, err
	}
	if local["id"] != id || fmt.Sprint(local["realm_epoch"]) != strconv.FormatUint(epoch, 10) || !syncPeriodPolicyEqual(local, current) {
		return result, errors.New("RGW observed zone has not imported the current committed period")
	}
	if !result.MetadataMaster {
		data, err = f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "metadata", "sync", "status")
		if err != nil {
			return result, err
		}
		result.Metadata, err = decodeSyncMetadata(data)
		if err != nil {
			return result, err
		}
	}
	data, err = f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "sync", "status")
	if err != nil {
		return result, err
	}
	text, err := parseSyncStatusText(data, result, f.zoneStates())
	if err != nil {
		return result, err
	}
	if result.MetadataMaster {
		result.Metadata = RGWMetadataSyncStatus{State: "master", PeriodID: id, RealmEpoch: epoch, CaughtUp: text.metadataMaster}
	} else {
		result.Metadata.CaughtUp = result.Metadata.State == "sync" && result.Metadata.PeriodID == id && result.Metadata.RealmEpoch == epoch && result.Metadata.Shards > 0 && result.Metadata.IncrementalShards == result.Metadata.Shards && text.metadataCaughtUp
	}
	result.DataSources = text.data
	return result, nil
}

func syncCommittedPeriodIdentity(current map[string]any) (string, uint64, error) {
	id, _ := current["id"].(string)
	epoch, err := strconv.ParseUint(fmt.Sprint(current["realm_epoch"]), 10, 64)
	if err != nil || id == "" || strings.HasSuffix(id, ":staging") || epoch == 0 {
		return "", 0, errors.New("RGW committed period identity is malformed")
	}
	return id, epoch, nil
}

func syncStatusReady(status RGWSyncStatus, sources []string) bool {
	if !status.Metadata.CaughtUp {
		return false
	}
	for _, source := range sources {
		if !slices.ContainsFunc(status.DataSources, func(s RGWDataSyncStatus) bool { return s.SourceZone == source && s.CaughtUp }) {
			return false
		}
	}
	return true
}

func decodeSyncMetadata(data []byte) (RGWMetadataSyncStatus, error) {
	var native struct {
		SyncStatus struct {
			Info struct {
				Status     string `json:"status"`
				NumShards  int    `json:"num_shards"`
				Period     string `json:"period"`
				RealmEpoch uint64 `json:"realm_epoch"`
			} `json:"info"`
			Markers []struct {
				Key int `json:"key"`
				Val struct {
					State int `json:"state"`
				} `json:"val"`
			} `json:"markers"`
		} `json:"sync_status"`
	}
	if json.Unmarshal(data, &native) != nil || native.SyncStatus.Info.Status == "" || native.SyncStatus.Info.NumShards < 0 {
		return RGWMetadataSyncStatus{}, errors.New("decode RGW native metadata sync status")
	}
	info := native.SyncStatus.Info
	result := RGWMetadataSyncStatus{State: info.Status, PeriodID: info.Period, RealmEpoch: info.RealmEpoch, Shards: info.NumShards}
	seen := map[int]bool{}
	for _, marker := range native.SyncStatus.Markers {
		if marker.Key < 0 || marker.Key >= result.Shards || seen[marker.Key] {
			return result, errors.New("RGW metadata sync has duplicated or invalid shard identities")
		}
		seen[marker.Key] = true
		if marker.Val.State == 1 {
			result.IncrementalShards++
		}
	}
	return result, nil
}

type rgwSyncText struct {
	metadataMaster, metadataCaughtUp bool
	data                             []RGWDataSyncStatus
}

var syncShardCount = regexp.MustCompile(`^(full|incremental) sync: ([0-9]+)/([0-9]+) shards$`)
var syncBehindCount = regexp.MustCompile(`^data is behind on ([0-9]+) shards$`)
var syncRecoveryCount = regexp.MustCompile(`^([0-9]+) shards are recovering$`)

func parseSyncStatusText(data []byte, expected RGWSyncStatus, zones []*rgwZoneState) (rgwSyncText, error) {
	var result rgwSyncText
	identities := map[string]string{"realm": expected.RealmID, "zonegroup": expected.ZonegroupID, "zone": expected.ZoneID}
	seen := map[string]bool{}
	section := ""
	var current *RGWDataSyncStatus
	metadataFailed := false
	dataFailed := map[string]bool{}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "zonegroup features ") {
			continue
		}
		fields := strings.Fields(line)
		if want, ok := identities[fields[0]]; ok {
			if len(fields) < 2 || fields[1] != want || seen[fields[0]] {
				return result, errors.New("RGW sync status native realm/zonegroup/zone identity differs")
			}
			seen[fields[0]] = true
			continue
		}
		if strings.HasPrefix(line, "metadata sync ") {
			section = "metadata"
			line = strings.TrimSpace(strings.TrimPrefix(line, "metadata sync "))
		} else if strings.HasPrefix(line, "data sync ") {
			section = "data"
			line = strings.TrimSpace(strings.TrimPrefix(line, "data sync "))
		}
		if section == "metadata" {
			if line == "no sync (zone is master)" {
				result.metadataMaster = true
			}
			if line == "metadata is caught up with master" {
				result.metadataCaughtUp = true
			}
			if strings.Contains(line, "failed") || strings.Contains(line, "different period") || strings.Contains(line, "ERROR") || strings.Contains(line, "behind on") {
				metadataFailed = true
			}
			continue
		}
		if section != "data" {
			continue
		}
		if strings.HasPrefix(line, "source: ") {
			id := strings.Fields(strings.TrimPrefix(line, "source: "))[0]
			var source *rgwZoneState
			for _, z := range zones {
				if z.ID == id && z.ID != expected.ZoneID && z.Zonegroup == syncExpectedGroup(expected, zones) {
					source = z
				}
			}
			if source == nil || slices.ContainsFunc(result.data, func(s RGWDataSyncStatus) bool { return s.SourceZoneID == id }) {
				return result, errors.New("RGW sync status has unexpected or duplicate source zone")
			}
			result.data = append(result.data, RGWDataSyncStatus{SourceZone: source.Name, SourceZoneID: id})
			current = &result.data[len(result.data)-1]
			continue
		}
		if current == nil {
			continue
		}
		if line == "syncing" {
			current.Syncing = true
		}
		if line == "data is caught up with source" {
			current.CaughtUp = true
		}
		if strings.Contains(line, "failed") || strings.Contains(line, "ERROR") || strings.Contains(line, "not syncing") {
			dataFailed[current.SourceZoneID] = true
		}
		if match := syncShardCount.FindStringSubmatch(line); match != nil {
			count, _ := strconv.Atoi(match[2])
			total, _ := strconv.Atoi(match[3])
			if current.Shards != 0 && current.Shards != total {
				dataFailed[current.SourceZoneID] = true
			}
			current.Shards = total
			if match[1] == "full" {
				current.FullShards = count
			} else {
				current.IncrementalShards = count
			}
		}
		if match := syncBehindCount.FindStringSubmatch(line); match != nil {
			current.BehindShards, _ = strconv.Atoi(match[1])
		}
		if match := syncRecoveryCount.FindStringSubmatch(line); match != nil {
			current.RecoveringShards, _ = strconv.Atoi(match[1])
		}
	}
	if len(seen) != len(identities) {
		return result, errors.New("RGW native sync status is missing scoped identity headers")
	}
	result.metadataCaughtUp = result.metadataCaughtUp && !metadataFailed
	result.metadataMaster = result.metadataMaster && !metadataFailed
	for i := range result.data {
		s := &result.data[i]
		s.CaughtUp = s.CaughtUp && s.Syncing && s.Shards > 0 && s.IncrementalShards == s.Shards && s.FullShards == 0 && s.BehindShards == 0 && s.RecoveringShards == 0 && !dataFailed[s.SourceZoneID]
	}
	return result, nil
}

func syncExpectedGroup(status RGWSyncStatus, zones []*rgwZoneState) string {
	for _, z := range zones {
		if z.ID == status.ZoneID {
			return z.Zonegroup
		}
	}
	return ""
}
