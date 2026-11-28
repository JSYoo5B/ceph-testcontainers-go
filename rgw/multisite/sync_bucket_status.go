package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// SyncBucketIdentity fixes a bucket namespace and native instance. Recreating
// its name cannot silently change the bucket selected by an owned pipe.
type SyncBucketIdentity struct{ Name, Tenant, ID string }

// BucketSyncStatus observes the bucket tuple captured by an owned pipe.
// Native RGW combines comparisons from matching pipes; this does not attest a
// particular pipe's activation/filter/user mode. State is sanitized: incremental,
// init, full, stopped, error, or
// disabled_or_no_sources. Native markers and error text are never returned.
type BucketSyncStatus struct {
	periodEpoch                            uint64
	RealmID, PeriodID, ZonegroupID         string
	SourceZone, SourceZoneID, Zone, ZoneID string
	SourceBucket, DestinationBucket        SyncBucketIdentity
	RealmEpoch                             uint64
	State                                  string
	Shards, BehindShards                   int
	CaughtUp                               bool
}

// errSyncBucketRecreated reports a checkpoint bucket name that now resolves
// to another native instance.
var errSyncBucketRecreated = errors.New("RGW checkpoint bucket was recreated; refusing another native instance")

// errSyncPolicyChanged reports a checkpoint group whose native policy differs
// from the one the fixture confirmed.
var errSyncPolicyChanged = errors.New("RGW owned checkpoint policy was externally changed")

type rgwSyncBucketCheckpoint struct {
	group                    *SyncGroup
	source, destination      *rgwZoneState
	sourceBucket, destBucket SyncBucketIdentity
}

// BucketSyncStatus reads native per-bucket remote-log comparison for an owned
// unchanged group/pipe and two explicit owned zones. Wildcard bucket selectors
// require a bucket-scoped group. Concrete bucket instance IDs remain fixed.
// A caught-up checkpoint does not prove filter semantics or copied payloads;
// applications should quiesce writes and verify their S3 bytes separately.
//
// On Ceph 19 radosgw-admin ignores --format json for bucket sync status and
// prints text; this method parses it into the same structure and reports
// unknown lines as errors.
//
//purpose:observation
func (g *SyncGroup) BucketSyncStatus(ctx context.Context, pipeID, sourceZone, destinationZone string) (BucketSyncStatus, error) {
	f, err := g.fixture()
	if err != nil {
		return BucketSyncStatus{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return BucketSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	checkpoint, err := g.bucketCheckpoint(ctx, pipeID, sourceZone, destinationZone)
	if err != nil {
		return BucketSyncStatus{}, err
	}
	return f.syncBucketStatus(ctx, checkpoint)
}

// WaitBucketSyncReady waits up to four minutes, bounded by the caller context,
// for the captured pipe/bucket checkpoint. Disabled, full-sync, missing sources
// and native per-source errors never satisfy readiness even when CLI exit is 0.
// It changes no sync state, leases, permissions or object data. When the wait
// ends without readiness, the returned status is the last one that was read,
// so its lag survives a later attempt that failed or was canceled.
//
//purpose:observation
func (g *SyncGroup) WaitBucketSyncReady(ctx context.Context, pipeID, sourceZone, destinationZone string) (BucketSyncStatus, error) {
	f, err := g.fixture()
	if err != nil {
		return BucketSyncStatus{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return BucketSyncStatus{}, err
	}
	// Resolve once: the checkpoint keeps value copies of the selected identities.
	checkpoint, err := g.bucketCheckpoint(ctx, pipeID, sourceZone, destinationZone)
	var policy map[string]any
	if err == nil {
		policy = syncJSONClone(g.state.group)
	}
	f.topologyMu.Unlock()
	if err != nil {
		return BucketSyncStatus{}, err
	}
	// last is the most recent status that was read; an attempt that fails
	// afterwards, a canceled one included, does not replace it, so the
	// result of an ended wait reports the lag that was last observed.
	var last BucketSyncStatus
	var lastErr error
	observed := false
	for {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		if err := multicluster.LockObservation(attempt, &f.topologyMu); err != nil {
			stop()
			return last, fmt.Errorf("wait RGW bucket sync checkpoint: %w", err)
		}
		if err := f.checkBucketWaitOwnership(g, checkpoint, policy); err != nil {
			f.topologyMu.Unlock()
			stop()
			return last, err
		}
		status, err := f.syncBucketStatus(attempt, checkpoint)
		f.topologyMu.Unlock()
		stop()
		lastErr = err
		if err == nil || !observed {
			last = status
			observed = observed || err == nil
		}
		if lastErr == nil && last.CaughtUp && ctx.Err() == nil {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait RGW bucket sync checkpoint: %w; state=%s; behind_shards=%d; last_error=%v", ctx.Err(), last.State, last.BehindShards, lastErr)
		case <-time.After(time.Second):
		}
	}
}

// checkBucketWaitOwnership revalidates the captured handles after the wait
// releases topologyMu between samples.
func (f *Fixture) checkBucketWaitOwnership(g *SyncGroup, checkpoint *rgwSyncBucketCheckpoint, policy map[string]any) error {
	if err := f.validateSyncGroup(g, false); err != nil {
		return err
	}
	if g.state.pending != nil || !reflect.DeepEqual(g.state.group, policy) {
		return errors.New("RGW owned sync group changed during bucket readiness wait")
	}
	for _, captured := range []*rgwZoneState{checkpoint.source, checkpoint.destination} {
		attached := false
		for _, current := range f.zoneStates() {
			if current.Name == captured.Name && current.ID == captured.ID && current.client != nil && current.client == captured.client {
				attached = true
				break
			}
		}
		if !attached {
			return errors.New("RGW bucket checkpoint zone is no longer attached and owned")
		}
	}
	return nil
}

func (g *SyncGroup) bucketCheckpoint(ctx context.Context, pipeID, sourceZone, destinationZone string) (*rgwSyncBucketCheckpoint, error) {
	f := g.owner
	if err := f.validateSyncGroup(g, false); err != nil {
		return nil, err
	}
	if !validRGWSyncID(pipeID) || sourceZone == destinationZone || g.state.pending != nil {
		return nil, servicehook.Invalid(errors.New("RGW bucket checkpoint requires a confirmed pipe, distinct zones and no pending mutation"))
	}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return nil, err
	}
	if _, _, err := syncCommittedPeriodIdentity(current); err != nil {
		return nil, err
	}
	policy, _, err := g.policyDocument(ctx, master)
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
		bucket *SyncBucketIdentity
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
		if err := g.checkBucketInstance(ctx, master, *side.bucket); err != nil {
			return nil, err
		}
	}
	return checkpoint, nil
}

func checkPublishedSyncBucketGroup(current map[string]any, g *SyncGroup) error {
	if g.scope.Bucket == "" {
		published, err := syncPeriodWithGroup(current, g.groupID, g.ID(), g.state.group)
		if err != nil || !syncPeriodPolicyEqual(current, published) {
			return errors.New("RGW bucket checkpoint pipe has not been published in the current period")
		}
	}
	return nil
}

func syncPipeBucketIdentity(g *SyncGroup, key string) (SyncBucketIdentity, error) {
	if key == "*" {
		if g.scope.Bucket == "" || g.bucketID == "" {
			return SyncBucketIdentity{}, errors.New("RGW wildcard checkpoint requires a bucket-scoped owned group")
		}
		return SyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}, nil
	}
	name, id, found := strings.Cut(key, ":")
	if !found || id == "" || strings.ContainsAny(id, "/:$") {
		return SyncBucketIdentity{}, errors.New("RGW native checkpoint bucket selector lacks an exact instance")
	}
	identity := SyncBucketIdentity{Name: name, ID: id}
	if tenant, bucket, found := strings.Cut(name, "/"); found {
		identity.Tenant, identity.Name = tenant, bucket
	}
	if !validRGWSyncID(identity.Name) || identity.Name == "*" || (identity.Tenant != "" && !validRGWSyncID(identity.Tenant)) {
		return SyncBucketIdentity{}, errors.New("RGW native checkpoint bucket namespace is malformed")
	}
	return identity, nil
}

func (g *SyncGroup) checkBucketInstance(ctx context.Context, zone *rgwZoneState, bucket SyncBucketIdentity) error {
	selected := &SyncGroup{owner: g.owner, scope: SyncPolicyScope{Zonegroup: g.scope.Zonegroup, Bucket: bucket.Name, Tenant: bucket.Tenant}, groupID: g.groupID}
	id, err := selected.bucketInstanceID(ctx, zone)
	if err != nil {
		return err
	}
	if id != bucket.ID {
		return errSyncBucketRecreated
	}
	return nil
}

func (f *Fixture) syncBucketStatus(ctx context.Context, checkpoint *rgwSyncBucketCheckpoint) (BucketSyncStatus, error) {
	g := checkpoint.group
	result := BucketSyncStatus{RealmID: g.realmID, ZonegroupID: g.groupID, SourceZone: checkpoint.source.Name, SourceZoneID: checkpoint.source.ID, Zone: checkpoint.destination.Name, ZoneID: checkpoint.destination.ID, SourceBucket: checkpoint.sourceBucket, DestinationBucket: checkpoint.destBucket}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return result, err
	}
	result.PeriodID, result.RealmEpoch, err = syncCommittedPeriodIdentity(current)
	if err != nil {
		return result, err
	}
	result.periodEpoch, err = syncCommittedPeriodEpoch(current)
	if err != nil {
		return result, err
	}
	if err := checkPublishedSyncBucketGroup(current, g); err != nil {
		return result, err
	}
	policy, _, err := g.policyDocument(ctx, master)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return result, errSyncPolicyChanged
	}
	for _, side := range []struct {
		zone   *rgwZoneState
		bucket SyncBucketIdentity
	}{{checkpoint.source, checkpoint.sourceBucket}, {checkpoint.destination, checkpoint.destBucket}} {
		if err := g.checkBucketInstance(ctx, side.zone, side.bucket); err != nil {
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
	// Bucket sync commands use the legacy source_zone field populated by
	// --source-zone (name or ID), not sync-policy's --source-zone-id option.
	args := []string{"bucket", "sync", "status", "--bucket", syncScopedBucketName(SyncPolicyScope{Bucket: checkpoint.destBucket.Name, Tenant: checkpoint.destBucket.Tenant}), "--bucket-id", checkpoint.destBucket.ID, "--source-zone", checkpoint.source.ID, "--source-bucket", checkpoint.sourceBucket.Name, "--source-bucket-id", checkpoint.sourceBucket.ID}
	if checkpoint.sourceBucket.Tenant != "" {
		args = append(args, "--source-tenant", checkpoint.sourceBucket.Tenant)
	}
	data, err := f.syncPolicyAdmin(ctx, checkpoint.destination, g.scope.Zonegroup, args...)
	if err != nil {
		return result, err
	}
	if checkpoint.destination.cluster != nil && multicluster.CephBefore(checkpoint.destination.cluster, 20) {
		if data, err = legacyBucketSyncStatusJSON(data, result); err != nil {
			return result, err
		}
	}
	return decodeBucketSyncStatus(data, result)
}

// bucketSyncStatusDocument is the JSON document that Ceph 20 and later print
// for bucket sync status, and that legacyBucketSyncStatusJSON rebuilds from
// Ceph 19's text. decodeBucketSyncStatus reads it by exact member name.
type bucketSyncStatusDocument struct {
	Realm            string             `json:"realm"`
	Zonegroup        string             `json:"zonegroup"`
	Zone             string             `json:"zone"`
	Bucket           string             `json:"bucket"`
	BucketInstanceID string             `json:"bucket_instance_id"`
	Error            string             `json:"error,omitempty"`
	Sources          []bucketSyncSource `json:"sources"`
}

// bucketSyncSource is one source comparison of the document.
type bucketSyncSource struct {
	SourceZone     string         `json:"source_zone"`
	SourceName     string         `json:"source_name"`
	SourceBucket   string         `json:"source_bucket"`
	SourceBucketID string         `json:"source_bucket_id"`
	Status         string         `json:"status,omitempty"`
	Error          string         `json:"error,omitempty"`
	TotalShards    *int           `json:"total_shards,omitempty"`
	BehindShards   *[]behindShard `json:"behind_shards,omitempty"`
}

type behindShard struct {
	ShardID int `json:"shard_id"`
}

var errLegacyBucketSyncText = errors.New("RGW Ceph 19 bucket sync status text is malformed")

// legacyBucketSyncStatusJSON converts Ceph 19's bucket sync status, which is
// text even with --format json, into the JSON document later releases print.
// Bucket lines read tenant:name[instance]); the tenants must match result.
func legacyBucketSyncStatusJSON(data []byte, result BucketSyncStatus) ([]byte, error) {
	var native bucketSyncStatusDocument
	// behind is the count of the last "bucket is behind on" line, until the
	// shard list that it counts.
	behind := -1
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		var current *bucketSyncSource
		if len(native.Sources) > 0 {
			current = &native.Sources[len(native.Sources)-1]
		}
		label, value, _ := strings.Cut(line, " ")
		id, _, _ := strings.Cut(value, " ")
		var err error
		switch {
		case line == "" || current == nil && label == "current":
		case current == nil && label == "realm":
			native.Realm = id
		case current == nil && label == "zonegroup":
			native.Zonegroup = id
		case current == nil && label == "zone":
			native.Zone = id
		case current == nil && label == "bucket":
			native.Bucket, native.BucketInstanceID, err = legacyBucketKey(value, result.DestinationBucket.Tenant)
		case strings.HasPrefix(line, "Sync is disabled for bucket "):
			native.Error = line
		case strings.HasPrefix(line, "source zone "):
			zone, name, _ := strings.Cut(strings.TrimPrefix(line, "source zone "), " ")
			native.Sources = append(native.Sources, bucketSyncSource{SourceZone: zone, SourceName: strings.TrimSuffix(strings.TrimPrefix(name, "("), ")")})
		case current == nil:
			return nil, errLegacyBucketSyncText
		default:
			err = current.line(line, result.SourceBucket.Tenant, &behind)
		}
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(native)
}

// line reads one line of the source's section. behind carries the count of a
// "bucket is behind on" line to the shard list that follows it.
func (current *bucketSyncSource) line(line, tenant string, behind *int) error {
	var err error
	switch {
	case strings.HasPrefix(line, "source bucket "):
		current.SourceBucket, current.SourceBucketID, err = legacyBucketKey(strings.TrimPrefix(line, "source bucket "), tenant)
	case line == "does not sync from zone":
		current.Error = line
	case strings.HasPrefix(line, "init:"), strings.HasPrefix(line, "stopped:"), strings.HasPrefix(line, "full sync:"):
		current.Status = line
	case strings.HasPrefix(line, "incremental sync on "):
		count, found := strings.CutSuffix(strings.TrimPrefix(line, "incremental sync on "), " shards")
		shards, convErr := strconv.Atoi(count)
		if !found || convErr != nil {
			return errLegacyBucketSyncText
		}
		current.TotalShards = &shards
	case line == "bucket is caught up with source":
		current.BehindShards = &[]behindShard{}
	case strings.HasPrefix(line, "bucket is behind on "):
		count, found := strings.CutSuffix(strings.TrimPrefix(line, "bucket is behind on "), " shards")
		if *behind, err = strconv.Atoi(count); !found || err != nil {
			return errLegacyBucketSyncText
		}
	case strings.HasPrefix(line, "behind shards: ["):
		shards, err := behindShards(line, *behind)
		if err != nil {
			return err
		}
		current.BehindShards, *behind = &shards, -1
	default:
		return errLegacyBucketSyncText
	}
	return err
}

// legacyBucketKey splits a tenant:name[instance]) bucket key whose tenant
// must be tenant.
func legacyBucketKey(value, tenant string) (string, string, error) {
	key, ok := strings.CutSuffix(value, "])")
	owner, rest, found := strings.Cut(key, ":")
	name, id, bracket := strings.Cut(rest, "[")
	if !ok || !found || !bracket || owner != tenant || name == "" || id == "" {
		return "", "", errLegacyBucketSyncText
	}
	return name, id, nil
}

// behindShards reads a "behind shards: [...]" list, which must name
// exactly the behind count of the line before it. The count also covers
// shards without a remote marker, which the list omits; never report fewer
// shards than the count.
func behindShards(line string, behind int) ([]behindShard, error) {
	list, found := strings.CutSuffix(strings.TrimPrefix(line, "behind shards: ["), "]")
	if !found {
		return nil, errLegacyBucketSyncText
	}
	shards := []behindShard{}
	for _, item := range strings.Split(list, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		shard, err := strconv.Atoi(item)
		if err != nil {
			return nil, errLegacyBucketSyncText
		}
		shards = append(shards, behindShard{shard})
	}
	if behind <= 0 || len(shards) != behind {
		return nil, errors.New("RGW Ceph 19 bucket sync status lists an incomplete behind shard set")
	}
	return shards, nil
}

func decodeBucketSyncStatus(data []byte, result BucketSyncStatus) (BucketSyncStatus, error) {
	// The identity members are matched exactly: the document may repeat no
	// member and spell none in another case.
	var native bucketSyncStatusDocument
	if strictjson.Decode(data, &native, strictjson.Options{}) != nil || native.Realm != result.RealmID || native.Zonegroup != result.ZonegroupID || native.Zone != result.ZoneID || native.Bucket != result.DestinationBucket.Name || native.BucketInstanceID != result.DestinationBucket.ID {
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
		ready, err := mergeBucketSyncSource(&result, source, behind)
		if err != nil {
			return result, err
		}
		allReady = allReady && ready
	}
	result.BehindShards = len(behind)
	result.CaughtUp = allReady && result.Shards > 0 && result.BehindShards == 0
	return result, nil
}

// mergeBucketSyncSource folds one source comparison into result and the
// behind shard union. ready is false for a source in error or not yet in
// incremental sync; the first such state found replaces "incremental".
func mergeBucketSyncSource(result *BucketSyncStatus, source bucketSyncSource, behind map[int]bool) (bool, error) {
	if source.SourceZone != result.SourceZoneID || source.SourceName != result.SourceZone {
		return false, errors.New("RGW native bucket checkpoint source zone differs")
	}
	if source.Error != "" {
		result.State = "error"
		return false, nil
	}
	if source.SourceBucket != result.SourceBucket.Name || source.SourceBucketID != result.SourceBucket.ID {
		return false, errors.New("RGW native bucket checkpoint source instance differs")
	}
	if source.Status != "" {
		state := ""
		for _, candidate := range []string{"init", "stopped", "full sync"} {
			if strings.HasPrefix(source.Status, candidate+":") {
				state = strings.TrimSuffix(candidate, " sync")
			}
		}
		if state == "" {
			return false, errors.New("RGW native bucket checkpoint state is unrecognized")
		}
		if result.State == "incremental" {
			result.State = state
		}
		return false, nil
	}
	if source.TotalShards == nil || *source.TotalShards <= 0 || source.BehindShards == nil {
		return false, errors.New("RGW native bucket checkpoint shard comparison is incomplete")
	}
	if result.Shards != 0 && result.Shards != *source.TotalShards {
		return false, errors.New("RGW native bucket checkpoint shard count changed between pipe comparisons")
	}
	result.Shards = *source.TotalShards
	seen := map[int]bool{}
	for _, shard := range *source.BehindShards {
		if shard.ShardID < 0 || seen[shard.ShardID] {
			return false, errors.New("RGW native bucket checkpoint behind shard IDs are malformed")
		}
		seen[shard.ShardID], behind[shard.ShardID] = true, true
	}
	return true, nil
}
