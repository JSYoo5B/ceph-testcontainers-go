package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// RGWSyncBucketIdentity fixes a bucket namespace and native instance. Recreating
// its name cannot silently change the bucket selected by an owned pipe.
type RGWSyncBucketIdentity struct{ Name, Tenant, ID string }

// RGWBucketSyncStatus observes the bucket tuple captured by an owned pipe.
// Native RGW combines comparisons from matching pipes; this does not attest a
// particular pipe's activation/filter/user mode. State is sanitized: incremental,
// init, full, stopped, error, or
// disabled_or_no_sources. Native markers and error text are never returned.
type RGWBucketSyncStatus struct {
	RealmID, PeriodID, ZonegroupID         string
	SourceZone, SourceZoneID, Zone, ZoneID string
	SourceBucket, DestinationBucket        RGWSyncBucketIdentity
	RealmEpoch                             uint64
	State                                  string
	Shards, BehindShards                   int
	CaughtUp                               bool
}

type rgwSyncBucketCheckpoint struct {
	group                    *RGWSyncGroup
	source, destination      *rgwZoneState
	sourceBucket, destBucket RGWSyncBucketIdentity
}

// BucketSyncStatus reads native per-bucket remote-log comparison for an owned
// unchanged group/pipe and two explicit owned zones. Wildcard bucket selectors
// require a bucket-scoped group. Concrete bucket instance IDs remain fixed.
// A caught-up checkpoint does not prove filter semantics or copied payloads;
// applications should quiesce writes and verify their S3 bytes separately.
func (f *RGWMultisite) BucketSyncStatus(ctx context.Context, g *RGWSyncGroup, pipeID, sourceZone, destinationZone string) (RGWBucketSyncStatus, error) {
	if f == nil {
		return RGWBucketSyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return RGWBucketSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	checkpoint, err := f.syncBucketCheckpoint(ctx, g, pipeID, sourceZone, destinationZone)
	if err != nil {
		return RGWBucketSyncStatus{}, err
	}
	return f.syncBucketStatus(ctx, checkpoint)
}

// WaitBucketSyncReady waits up to four minutes, bounded by the caller context,
// for the captured pipe/bucket checkpoint. Disabled, full-sync, missing sources
// and native per-source errors never satisfy readiness even when CLI exit is 0.
// It changes no sync state, leases, permissions or object data.
func (f *RGWMultisite) WaitBucketSyncReady(ctx context.Context, g *RGWSyncGroup, pipeID, sourceZone, destinationZone string) (RGWBucketSyncStatus, error) {
	if f == nil {
		return RGWBucketSyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return RGWBucketSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	// Resolve once: the checkpoint keeps value copies of the selected identities.
	checkpoint, err := f.syncBucketCheckpoint(ctx, g, pipeID, sourceZone, destinationZone)
	if err != nil {
		return RGWBucketSyncStatus{}, err
	}
	var last RGWBucketSyncStatus
	var lastErr error
	for {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		last, lastErr = f.syncBucketStatus(attempt, checkpoint)
		stop()
		if err := ctx.Err(); err != nil {
			return last, fmt.Errorf("wait RGW bucket sync checkpoint: %w", err)
		}
		if lastErr == nil && last.CaughtUp {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait RGW bucket sync checkpoint: %w; state=%s; behind_shards=%d; last_error=%v", ctx.Err(), last.State, last.BehindShards, lastErr)
		case <-time.After(time.Second):
		}
	}
}

// Observation waits include time queued behind a topology mutation/observer.
// TryLock preserves the shared topology mutex without an abandoned lock goroutine.
func lockRGWSyncObservation(ctx context.Context, mutex *sync.Mutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mutex.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *RGWMultisite) syncBucketCheckpoint(ctx context.Context, g *RGWSyncGroup, pipeID, sourceZone, destinationZone string) (*rgwSyncBucketCheckpoint, error) {
	if err := f.validateSyncGroup(g, false); err != nil {
		return nil, err
	}
	if !validRGWSyncID(pipeID) || sourceZone == destinationZone || g.state.pending != nil {
		return nil, errors.New("RGW bucket checkpoint requires a confirmed pipe, distinct zones and no pending mutation")
	}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return nil, err
	}
	if _, _, err := syncCommittedPeriodIdentity(current); err != nil {
		return nil, err
	}
	policy, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return nil, errors.New("RGW owned sync policy changed before bucket observation")
	}
	if err := checkPublishedSyncBucketGroup(current, g); err != nil {
		return nil, err
	}
	pipe := syncNativePipe(g.state.group, pipeID)
	if pipe == nil {
		return nil, errors.New("RGW bucket checkpoint pipe is absent from the owned group")
	}
	checkpoint := &rgwSyncBucketCheckpoint{group: g}
	for _, zone := range f.zoneStates() {
		if zone.Zonegroup == g.scope.Zonegroup && zone.client != nil {
			if zone.Name == sourceZone {
				checkpoint.source = zone
			}
			if zone.Name == destinationZone {
				checkpoint.destination = zone
			}
		}
	}
	if checkpoint.source == nil || checkpoint.destination == nil {
		return nil, errors.New("RGW bucket checkpoint requires attached owned zones in the policy group")
	}
	for _, side := range []struct {
		key    string
		zone   *rgwZoneState
		bucket *RGWSyncBucketIdentity
	}{{"source", checkpoint.source, &checkpoint.sourceBucket}, {"dest", checkpoint.destination, &checkpoint.destBucket}} {
		entity, _ := pipe[side.key].(map[string]any)
		zones, _ := entity["zones"].([]any)
		if !slices.Contains(zones, any("*")) && !slices.Contains(zones, any(side.zone.ID)) {
			return nil, errors.New("RGW bucket checkpoint zone is outside the selected pipe")
		}
		key, _ := entity["bucket"].(string)
		*side.bucket, err = syncPipeBucketIdentity(g, key)
		if err != nil {
			return nil, err
		}
		if err := f.checkSyncBucketInstance(ctx, master, g, *side.bucket); err != nil {
			return nil, err
		}
	}
	return checkpoint, nil
}

func checkPublishedSyncBucketGroup(current map[string]any, g *RGWSyncGroup) error {
	if g.scope.Bucket == "" {
		published, err := syncPeriodWithGroup(current, g.groupID, g.ID(), g.state.group)
		if err != nil || !syncPeriodPolicyEqual(current, published) {
			return errors.New("RGW bucket checkpoint pipe has not been published in the current period")
		}
	}
	return nil
}

func syncPipeBucketIdentity(g *RGWSyncGroup, key string) (RGWSyncBucketIdentity, error) {
	if key == "*" {
		if g.scope.Bucket == "" || g.bucketID == "" {
			return RGWSyncBucketIdentity{}, errors.New("RGW wildcard checkpoint requires a bucket-scoped owned group")
		}
		return RGWSyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}, nil
	}
	name, id, found := strings.Cut(key, ":")
	if !found || id == "" || strings.ContainsAny(id, "/:$") {
		return RGWSyncBucketIdentity{}, errors.New("RGW native checkpoint bucket selector lacks an exact instance")
	}
	identity := RGWSyncBucketIdentity{Name: name, ID: id}
	if tenant, bucket, found := strings.Cut(name, "/"); found {
		identity.Tenant, identity.Name = tenant, bucket
	}
	if !validRGWSyncID(identity.Name) || identity.Name == "*" || (identity.Tenant != "" && !validRGWSyncID(identity.Tenant)) {
		return RGWSyncBucketIdentity{}, errors.New("RGW native checkpoint bucket namespace is malformed")
	}
	return identity, nil
}

func (f *RGWMultisite) checkSyncBucketInstance(ctx context.Context, zone *rgwZoneState, g *RGWSyncGroup, bucket RGWSyncBucketIdentity) error {
	selected := &RGWSyncGroup{scope: RGWSyncPolicyScope{Zonegroup: g.scope.Zonegroup, Bucket: bucket.Name, Tenant: bucket.Tenant}, groupID: g.groupID}
	id, err := f.syncBucketID(ctx, zone, selected)
	if err != nil {
		return err
	}
	if id != bucket.ID {
		return errors.New("RGW checkpoint bucket was recreated; refusing another native instance")
	}
	return nil
}

func (f *RGWMultisite) syncBucketStatus(ctx context.Context, checkpoint *rgwSyncBucketCheckpoint) (RGWBucketSyncStatus, error) {
	g := checkpoint.group
	result := RGWBucketSyncStatus{RealmID: g.realmID, ZonegroupID: g.groupID, SourceZone: checkpoint.source.Name, SourceZoneID: checkpoint.source.ID, Zone: checkpoint.destination.Name, ZoneID: checkpoint.destination.ID, SourceBucket: checkpoint.sourceBucket, DestinationBucket: checkpoint.destBucket}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return result, err
	}
	result.PeriodID, result.RealmEpoch, err = syncCommittedPeriodIdentity(current)
	if err != nil {
		return result, err
	}
	if err := checkPublishedSyncBucketGroup(current, g); err != nil {
		return result, err
	}
	policy, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return result, errors.New("RGW owned checkpoint policy was externally changed")
	}
	for _, side := range []struct {
		zone   *rgwZoneState
		bucket RGWSyncBucketIdentity
	}{{checkpoint.source, checkpoint.sourceBucket}, {checkpoint.destination, checkpoint.destBucket}} {
		if err := f.checkSyncBucketInstance(ctx, side.zone, g, side.bucket); err != nil {
			return result, err
		}
	}
	for _, zone := range []*rgwZoneState{checkpoint.source, checkpoint.destination} {
		data, err := f.syncPolicyAdmin(ctx, zone, g.scope.Zonegroup, "period", "get")
		if err != nil {
			return result, err
		}
		local, err := syncJSON(data)
		if err != nil {
			return result, err
		}
		if !reflect.DeepEqual(local, current) {
			return result, errors.New("RGW bucket checkpoint endpoint has not imported the exact committed period")
		}
	}
	args := []string{"bucket", "sync", "status", "--bucket", syncScopedBucketName(RGWSyncPolicyScope{Bucket: checkpoint.destBucket.Name, Tenant: checkpoint.destBucket.Tenant}), "--bucket-id", checkpoint.destBucket.ID, "--source-zone-id", checkpoint.source.ID, "--source-bucket", checkpoint.sourceBucket.Name, "--source-bucket-id", checkpoint.sourceBucket.ID}
	if checkpoint.sourceBucket.Tenant != "" {
		args = append(args, "--source-tenant", checkpoint.sourceBucket.Tenant)
	}
	data, err := f.syncPolicyAdmin(ctx, checkpoint.destination, g.scope.Zonegroup, args...)
	if err != nil {
		return result, err
	}
	return decodeBucketSyncStatus(data, result)
}

func decodeBucketSyncStatus(data []byte, result RGWBucketSyncStatus) (RGWBucketSyncStatus, error) {
	var native struct {
		Realm, Zonegroup, Zone, Bucket string
		BucketInstanceID               string `json:"bucket_instance_id"`
		Error                          string
		Sources                        []struct {
			SourceZone     string `json:"source_zone"`
			SourceName     string `json:"source_name"`
			SourceBucket   string `json:"source_bucket"`
			SourceBucketID string `json:"source_bucket_id"`
			Status, Error  string
			TotalShards    *int `json:"total_shards"`
			BehindShards   *[]struct {
				ShardID int `json:"shard_id"`
			} `json:"behind_shards"`
		}
	}
	if json.Unmarshal(data, &native) != nil || native.Realm != result.RealmID || native.Zonegroup != result.ZonegroupID || native.Zone != result.ZoneID || native.Bucket != result.DestinationBucket.Name || native.BucketInstanceID != result.DestinationBucket.ID {
		return result, errors.New("RGW native bucket checkpoint identity is malformed or differs")
	}
	if native.Error != "" {
		result.State = "error"
		if strings.HasPrefix(native.Error, "Sync is disabled for bucket ") {
			result.State = "disabled_or_no_sources"
		}
		return result, nil
	}
	if len(native.Sources) == 0 {
		result.State = "disabled_or_no_sources"
		return result, nil
	}
	// Native get_all_sources reports every matching pipe, even when several
	// pipes share the same source/destination bucket checkpoint. Require all
	// comparisons to match the fixed source and conservatively union any lag.
	result.State = "incremental"
	allReady := true
	behind := map[int]bool{}
	for _, source := range native.Sources {
		if source.SourceZone != result.SourceZoneID || source.SourceName != result.SourceZone {
			return result, errors.New("RGW native bucket checkpoint source zone differs")
		}
		if source.Error != "" {
			result.State, allReady = "error", false
			continue
		}
		if source.SourceBucket != result.SourceBucket.Name || source.SourceBucketID != result.SourceBucket.ID {
			return result, errors.New("RGW native bucket checkpoint source instance differs")
		}
		if source.Status != "" {
			state := ""
			for _, candidate := range []string{"init", "stopped", "full sync"} {
				if strings.HasPrefix(source.Status, candidate+":") {
					state = strings.TrimSuffix(candidate, " sync")
				}
			}
			if state == "" {
				return result, errors.New("RGW native bucket checkpoint state is unrecognized")
			}
			if result.State == "incremental" {
				result.State = state
			}
			allReady = false
			continue
		}
		if source.TotalShards == nil || *source.TotalShards <= 0 || source.BehindShards == nil {
			return result, errors.New("RGW native bucket checkpoint shard comparison is incomplete")
		}
		if result.Shards != 0 && result.Shards != *source.TotalShards {
			return result, errors.New("RGW native bucket checkpoint shard count changed between pipe comparisons")
		}
		result.Shards = *source.TotalShards
		seen := map[int]bool{}
		for _, shard := range *source.BehindShards {
			if shard.ShardID < 0 || seen[shard.ShardID] {
				return result, errors.New("RGW native bucket checkpoint behind shard IDs are malformed")
			}
			seen[shard.ShardID], behind[shard.ShardID] = true, true
		}
	}
	result.BehindShards = len(behind)
	result.CaughtUp = allReady && result.Shards > 0 && result.BehindShards == 0
	return result, nil
}
