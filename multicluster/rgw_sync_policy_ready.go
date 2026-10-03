package multicluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"
)

// RGWBucketSyncPolicyStatus describes the captured master's configuration and
// its import into one selected destination. Buckets contains copied, distinct
// native instances required by pipes selecting that destination. It contains
// no policy maps, credentials, object names or sync markers.
type RGWBucketSyncPolicyStatus struct {
	RealmID, PeriodID, ZonegroupID, GroupID, Zone, ZoneID string
	RealmEpoch                                            uint64
	Bucket                                                RGWSyncBucketIdentity
	Buckets                                               []RGWSyncBucketIdentity
	PeriodImported, PolicyImported, BucketsImported       bool
	Imported                                              bool
}

// WaitBucketSyncPolicyReady waits up to four minutes, bounded by ctx including
// mutex queue time, for an unchanged owned bucket group's metadata to import
// into an explicit attached destination selected by its pipes. It checks the
// exact committed period, source-scope group snapshot, and all referenced
// source/destination bucket instances needed by that destination. Missing or
// older local metadata waits; recreated instances and changed master policy
// are refused. Allowed/forbidden policies can also be observed as imported.
//
// This read-only prerequisite does not attest effective discovery hints, data
// checkpoints, payloads or permissions, or replay objects written before the
// policy was imported. It does not pull periods, restart gateways or mutate
// replication state. Use it after creating pipes and before selected writes.
func (f *RGWMultisite) WaitBucketSyncPolicyReady(ctx context.Context, g *RGWSyncGroup, destinationZone string) (RGWBucketSyncPolicyStatus, error) {
	if f == nil {
		return RGWBucketSyncPolicyStatus{}, errors.New("RGW multisite fixture is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &f.topologyMu); err != nil {
		return RGWBucketSyncPolicyStatus{}, err
	}
	defer f.topologyMu.Unlock()
	if err := f.validateSyncGroup(g, false); err != nil {
		return RGWBucketSyncPolicyStatus{}, err
	}
	if g.scope.Bucket == "" || g.bucketID == "" || g.state.pending != nil {
		return RGWBucketSyncPolicyStatus{}, errors.New("RGW policy import requires an owned bucket-scoped group without pending mutations")
	}
	var destination *rgwZoneState
	for _, zone := range f.zoneStates() {
		if zone.Name == destinationZone && zone.Zonegroup == g.scope.Zonegroup && zone.ID != "" && zone.client != nil {
			destination = zone
		}
	}
	if destination == nil {
		return RGWBucketSyncPolicyStatus{}, errors.New("RGW policy import requires an explicit attached destination in the owned zonegroup")
	}
	buckets, err := syncPolicyReadyBuckets(g, destination.ID)
	if err != nil {
		return RGWBucketSyncPolicyStatus{}, err
	}
	status := RGWBucketSyncPolicyStatus{RealmID: g.realmID, ZonegroupID: g.groupID, GroupID: g.ID(), Zone: destination.Name, ZoneID: destination.ID, Bucket: RGWSyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}, Buckets: slices.Clone(buckets)}
	attempt, stop := context.WithTimeout(ctx, 30*time.Second)
	master, period, err := f.syncPolicyMaster(attempt)
	if err == nil {
		status.PeriodID, status.RealmEpoch, err = syncCommittedPeriodIdentity(period)
	}
	if err == nil {
		err = f.checkSyncPolicyReadyMaster(attempt, g, period, buckets)
	}
	stop()
	if err != nil {
		return status, err
	}
	period = syncJSONClone(period)
	wantGroup := syncJSONClone(g.state.group)
	for {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		last, fatal, readErr := f.syncPolicyReadyStatus(attempt, g, master, destination, period, wantGroup, status)
		stop()
		if err := ctx.Err(); err != nil {
			return last, fmt.Errorf("wait RGW bucket policy import: %w", err)
		}
		if fatal {
			return last, readErr
		}
		if readErr == nil && last.Imported {
			return last, nil
		}
		status = last
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait RGW bucket policy import: %w; period_imported=%t policy_imported=%t buckets_imported=%t", ctx.Err(), last.PeriodImported, last.PolicyImported, last.BucketsImported)
		case <-time.After(time.Second):
		}
	}
}

func syncPolicyReadyBuckets(g *RGWSyncGroup, destinationID string) ([]RGWSyncBucketIdentity, error) {
	scope := RGWSyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}
	buckets := []RGWSyncBucketIdentity{scope}
	seen := map[RGWSyncBucketIdentity]bool{scope: true}
	names := map[string]string{scope.Tenant + "/" + scope.Name: scope.ID}
	pipes, ok := g.state.group["pipes"].([]any)
	if !ok {
		return nil, errors.New("RGW owned policy pipes are malformed")
	}
	selected := false
	for _, value := range pipes {
		pipe, _ := value.(map[string]any)
		dest, _ := pipe["dest"].(map[string]any)
		zones, _ := dest["zones"].([]any)
		if !slices.Contains(zones, any("*")) && !slices.Contains(zones, any(destinationID)) {
			continue
		}
		selected = true
		for _, side := range []string{"source", "dest"} {
			entity, _ := pipe[side].(map[string]any)
			key, _ := entity["bucket"].(string)
			bucket, err := syncPipeBucketIdentity(g, key)
			if err != nil {
				return nil, err
			}
			name := bucket.Tenant + "/" + bucket.Name
			if old, exists := names[name]; exists && old != bucket.ID {
				return nil, errors.New("RGW owned policy selects conflicting bucket instances")
			}
			names[name] = bucket.ID
			if !seen[bucket] {
				seen[bucket] = true
				buckets = append(buckets, bucket)
			}
		}
	}
	if !selected {
		return nil, errors.New("RGW destination is outside the owned policy pipes")
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Tenant != buckets[j].Tenant {
			return buckets[i].Tenant < buckets[j].Tenant
		}
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (f *RGWMultisite) checkSyncPolicyReadyMaster(ctx context.Context, g *RGWSyncGroup, period map[string]any, buckets []RGWSyncBucketIdentity) error {
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, period) {
		return errors.New("RGW master committed period changed during policy import observation")
	}
	policy, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return errors.New("RGW owned master bucket policy changed during import observation")
	}
	for _, bucket := range buckets {
		if err := f.checkSyncBucketInstance(ctx, master, g, bucket); err != nil {
			return err
		}
	}
	return nil
}

func (f *RGWMultisite) syncPolicyReadyStatus(ctx context.Context, g *RGWSyncGroup, master, destination *rgwZoneState, period, wantGroup map[string]any, result RGWBucketSyncPolicyStatus) (RGWBucketSyncPolicyStatus, bool, error) {
	result.Buckets = slices.Clone(result.Buckets)
	result.PeriodImported, result.PolicyImported, result.BucketsImported, result.Imported = false, false, false, false
	if err := f.checkSyncPolicyReadyMaster(ctx, g, period, result.Buckets); err != nil {
		return result, true, err
	}
	// Read the attached metadata master's own pointer as well, so an older
	// first-zone import cannot substitute for the actual current master period.
	data, err := f.syncPolicyAdmin(ctx, master, master.Zonegroup, "period", "get")
	if err != nil {
		return result, false, err
	}
	current, err := syncJSON(data)
	if err != nil || !reflect.DeepEqual(current, period) {
		return result, true, errors.New("RGW metadata master no longer has the captured committed period")
	}
	data, err = f.syncPolicyAdmin(ctx, destination, g.scope.Zonegroup, "period", "get")
	if err != nil {
		return result, false, err
	}
	local, err := syncJSON(data)
	if err != nil {
		return result, true, err
	}
	if _, _, err := syncCommittedPeriodIdentity(local); err != nil || local["realm_id"] != g.realmID {
		return result, true, errors.New("RGW destination period identity is malformed or belongs to another realm")
	}
	if !reflect.DeepEqual(local, period) {
		return result, false, nil
	}
	result.PeriodImported = true
	found, fatal, err := f.syncPolicyReadyBucket(ctx, destination, g, result.Bucket)
	if err != nil || !found {
		return result, fatal, err
	}
	key := syncScopedBucketName(g.scope) + ":" + g.bucketID
	data, err = f.syncPolicyAdmin(ctx, destination, g.scope.Zonegroup, "metadata", "get", "bucket.instance:"+key)
	if err != nil {
		return result, false, err
	}
	metadata, err := syncJSON(data)
	if err != nil {
		return result, true, err
	}
	body, _ := metadata["data"].(map[string]any)
	info, _ := body["bucket_info"].(map[string]any)
	bucket, _ := info["bucket"].(map[string]any)
	if metadata["key"] != "bucket.instance:"+key || info == nil || bucket["name"] != g.scope.Bucket || bucket["tenant"] != g.scope.Tenant || bucket["bucket_id"] != g.bucketID || info["zonegroup"] != g.groupID {
		return result, true, errors.New("RGW destination policy metadata has a different bucket instance or zonegroup")
	}
	policy, err := syncPolicyFromDocument(info)
	if err != nil {
		return result, true, err
	}
	result.PolicyImported = reflect.DeepEqual(syncNativeGroup(policy, g.ID()), wantGroup)
	for _, bucket := range result.Buckets {
		found, fatal, err := f.syncPolicyReadyBucket(ctx, destination, g, bucket)
		if err != nil || !found {
			return result, fatal, err
		}
	}
	result.BucketsImported = true
	if err := ctx.Err(); err != nil {
		return result, false, err
	}
	result.Imported = result.PeriodImported && result.PolicyImported && result.BucketsImported
	return result, false, nil
}

func (f *RGWMultisite) syncPolicyReadyBucket(ctx context.Context, zone *rgwZoneState, g *RGWSyncGroup, bucket RGWSyncBucketIdentity) (bool, bool, error) {
	data, err := f.syncPolicyAdmin(ctx, zone, g.scope.Zonegroup, "bucket", "stats", "--bucket", syncScopedBucketName(RGWSyncPolicyScope{Bucket: bucket.Name, Tenant: bucket.Tenant}))
	if err != nil {
		return false, false, err
	}
	native, err := syncJSON(data)
	if err != nil {
		return false, true, err
	}
	id, _ := native["id"].(string)
	if id == "" {
		return false, true, errors.New("RGW destination bucket statistics lack a native instance identity")
	}
	if native["bucket"] != bucket.Name || native["tenant"] != bucket.Tenant || native["zonegroup"] != g.groupID {
		return false, true, errors.New("RGW destination bucket namespace or zonegroup identity differs")
	}
	if id != bucket.ID {
		return false, true, errors.New("RGW destination bucket was recreated; refusing another native instance")
	}
	return true, false, nil
}
