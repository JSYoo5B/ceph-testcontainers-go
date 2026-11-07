package multisite

import (
	"context"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"reflect"
	"slices"
	"sort"
	"time"
)

// BucketSyncPolicyStatus describes the captured master's configuration and
// its import into one selected destination. Buckets contains copied, distinct
// native instances required by pipes selecting that destination. It contains
// no policy maps, credentials, object names or sync markers.
type BucketSyncPolicyStatus struct {
	RealmID, PeriodID, ZonegroupID, GroupID, Zone, ZoneID string
	RealmEpoch                                            uint64
	Bucket                                                SyncBucketIdentity
	Buckets                                               []SyncBucketIdentity
	PeriodImported, PolicyImported, BucketsImported       bool
	Imported                                              bool
}

// WaitBucketPolicyReady waits up to four minutes, bounded by ctx including
// mutex queue time, for an unchanged owned bucket group's metadata to import
// into an explicit attached destination selected by its pipes. It checks the
// exact committed period, source-scope group snapshot, and all referenced
// source/destination bucket instances needed by that destination. Admission
// reads the master once and returns any error immediately. After admission,
// missing or older local metadata and transient read failures are retried. Recreated instances and changed
// master policy are refused. Allowed/forbidden policies can also be observed
// as imported. WatchBucketPolicy reports the stages of the same read.
//
// This read-only prerequisite does not attest effective discovery hints, data
// checkpoints, payloads or permissions, or replay objects written before the
// policy was imported. It does not pull periods, restart gateways or mutate
// replication state. Use it after creating pipes and before selected writes.
//
// Zonegroup groups, removed handles, pending mutations and zones outside the
// pipes' destinations are refused.
//
//purpose:observation
func (g *SyncGroup) WaitBucketPolicyReady(ctx context.Context, destinationZone string) (BucketSyncPolicyStatus, error) {
	f, err := g.fixture()
	if err != nil {
		return BucketSyncPolicyStatus{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return BucketSyncPolicyStatus{}, err
	}
	defer f.topologyMu.Unlock()
	attempt, stop := context.WithTimeout(ctx, 30*time.Second)
	target, status, err := g.admitPolicyImport(attempt, destinationZone)
	stop()
	if err != nil {
		return status, err
	}
	for {
		attempt, stop := context.WithTimeout(ctx, 30*time.Second)
		last, fatal, readErr := target.sample(attempt)
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
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("wait RGW bucket policy import: %w; period_imported=%t policy_imported=%t buckets_imported=%t", ctx.Err(), last.PeriodImported, last.PolicyImported, last.BucketsImported)
		case <-time.After(time.Second):
		}
	}
}

// policyImportTarget is what one observation of a policy import keeps from
// its admission: the handle, the master's committed period and the group
// snapshot captured then, and the identities every result carries.
type policyImportTarget struct {
	group  *SyncGroup
	period map[string]any
	want   map[string]any
	status BucketSyncPolicyStatus
}

// admitPolicyImport validates the handle and the destination and captures
// the master's committed period, the group snapshot and the referenced bucket
// instances. The caller holds topologyMu. On an error the status carries the
// identities resolved so far.
func (g *SyncGroup) admitPolicyImport(ctx context.Context, destinationZone string) (*policyImportTarget, BucketSyncPolicyStatus, error) {
	f := g.owner
	if err := f.validateSyncGroup(g, false); err != nil {
		return nil, BucketSyncPolicyStatus{}, err
	}
	if g.scope.Bucket == "" || g.bucketID == "" || g.state.pending != nil {
		return nil, BucketSyncPolicyStatus{}, errors.New("RGW policy import requires an owned bucket-scoped group without pending mutations")
	}
	destination := f.policyImportDestination(g, destinationZone)
	if destination == nil {
		return nil, BucketSyncPolicyStatus{}, errors.New("RGW policy import requires an explicit attached destination in the owned zonegroup")
	}
	buckets, err := syncPolicyReadyBuckets(g, destination.ID)
	if err != nil {
		return nil, BucketSyncPolicyStatus{}, err
	}
	status := BucketSyncPolicyStatus{RealmID: g.realmID, ZonegroupID: g.groupID, GroupID: g.ID(), Zone: destination.Name, ZoneID: destination.ID, Bucket: SyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}, Buckets: slices.Clone(buckets)}
	_, period, err := f.syncPolicyMaster(ctx)
	if err == nil {
		status.PeriodID, status.RealmEpoch, err = syncCommittedPeriodIdentity(period)
	}
	if err == nil {
		_, _, err = g.checkPolicyReadyMaster(ctx, period, buckets)
	}
	if err != nil {
		return nil, status, err
	}
	return &policyImportTarget{group: g, period: syncJSONClone(period), want: syncJSONClone(g.state.group), status: status}, status, nil
}

// policyImportDestination is the attached owned zone of g's zonegroup named
// name, or nil.
func (f *Fixture) policyImportDestination(g *SyncGroup, name string) *rgwZoneState {
	for _, zone := range f.zoneStates() {
		if zone.Name == name && zone.Zonegroup == g.scope.Zonegroup && zone.ID != "" && zone.client != nil {
			return zone
		}
	}
	return nil
}

// sample makes one read of the import with topologyMu held. It first checks
// that the handle, its group snapshot and the destination are still the
// admitted ones, because a watch releases the lock between reads. A true
// fatal result means that no later read can succeed.
func (t *policyImportTarget) sample(ctx context.Context) (BucketSyncPolicyStatus, bool, error) {
	g := t.group
	f := g.owner
	status := t.status
	status.Buckets = slices.Clone(status.Buckets)
	if err := f.validateSyncGroup(g, false); err != nil {
		return status, true, err
	}
	if g.state.pending != nil || !reflect.DeepEqual(g.state.group, t.want) {
		return status, true, errors.New("RGW owned sync group changed since the policy import observation began")
	}
	destination := f.policyImportDestination(g, t.status.Zone)
	if destination == nil || destination.ID != t.status.ZoneID {
		return status, true, errors.New("RGW policy import destination is no longer the attached owned zone")
	}
	return g.policyReadyStatus(ctx, destination, t.period, t.want, status)
}

func syncPolicyReadyBuckets(g *SyncGroup, destinationID string) ([]SyncBucketIdentity, error) {
	scope := SyncBucketIdentity{Name: g.scope.Bucket, Tenant: g.scope.Tenant, ID: g.bucketID}
	buckets := []SyncBucketIdentity{scope}
	seen := map[SyncBucketIdentity]bool{scope: true}
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

// checkPolicyReadyMaster checks that the master still has the captured
// committed period, the owned group and the referenced bucket instances, and
// returns the master. A failed read is not fatal; a different period, group or
// recreated bucket is.
func (g *SyncGroup) checkPolicyReadyMaster(ctx context.Context, period map[string]any, buckets []SyncBucketIdentity) (*rgwZoneState, bool, error) {
	f := g.owner
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return nil, false, err
	}
	if !reflect.DeepEqual(current, period) {
		return nil, true, errors.New("RGW master committed period changed during policy import observation")
	}
	// The instances come first: the policy read also refuses a recreated
	// bucket, but without telling it apart from a failed read.
	for _, bucket := range buckets {
		if err := g.checkBucketInstance(ctx, master, bucket); err != nil {
			return nil, errors.Is(err, errSyncBucketRecreated), err
		}
	}
	policy, _, err := g.policyDocument(ctx, master)
	if err != nil {
		return nil, false, err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return nil, true, errors.New("RGW owned master bucket policy changed during import observation")
	}
	return master, false, nil
}

func (g *SyncGroup) policyReadyStatus(ctx context.Context, destination *rgwZoneState, period, wantGroup map[string]any, result BucketSyncPolicyStatus) (BucketSyncPolicyStatus, bool, error) {
	f := g.owner
	result.Buckets = slices.Clone(result.Buckets)
	result.PeriodImported, result.PolicyImported, result.BucketsImported, result.Imported = false, false, false, false
	master, fatal, err := g.checkPolicyReadyMaster(ctx, period, result.Buckets)
	if err != nil {
		return result, fatal, err
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
	found, fatal, err := g.policyReadyBucket(ctx, destination, result.Bucket)
	if err != nil || !found {
		return result, fatal, err
	}
	key := syncScopedBucketName(g.scope) + ":" + g.bucketID
	data, err = f.syncPolicyAdmin(ctx, destination, g.scope.Zonegroup, "metadata", "get", "bucket.instance:"+key)
	if err != nil {
		return result, false, err
	}
	instance, matched, err := decodeBucketInstance(data, key, g)
	if err == nil && !matched {
		err = errors.New("RGW destination policy metadata has a different bucket instance or zonegroup")
	}
	if err != nil {
		return result, true, err
	}
	result.PolicyImported = reflect.DeepEqual(syncNativeGroup(instance.policy, g.ID()), wantGroup)
	for _, bucket := range result.Buckets {
		found, fatal, err := g.policyReadyBucket(ctx, destination, bucket)
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

func (g *SyncGroup) policyReadyBucket(ctx context.Context, zone *rgwZoneState, bucket SyncBucketIdentity) (bool, bool, error) {
	f := g.owner
	data, err := f.syncPolicyAdmin(ctx, zone, g.scope.Zonegroup, "bucket", "stats", "--bucket", syncScopedBucketName(SyncPolicyScope{Bucket: bucket.Name, Tenant: bucket.Tenant}))
	if err != nil {
		return false, false, err
	}
	native, err := syncJSON(data)
	if err != nil {
		return false, true, err
	}
	id := nativeDoc(native).member("id").value
	if id == "" {
		return false, true, errors.New("RGW destination bucket statistics lack a native instance identity")
	}
	if stats := nativeDoc(native); !stats.member("bucket").is(bucket.Name) || !stats.member("tenant").is(bucket.Tenant) || !stats.member("zonegroup").is(g.groupID) {
		return false, true, errors.New("RGW destination bucket namespace or zonegroup identity differs")
	}
	if id != bucket.ID {
		return false, true, errors.New("RGW destination bucket was recreated; refusing another native instance")
	}
	return true, false, nil
}
