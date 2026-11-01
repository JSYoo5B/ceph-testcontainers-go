package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
)

// SyncBucketSelector identifies a preexisting bucket. Its current native
// instance ID is captured and stored in the pipe, preventing a recreated name
// from silently becoming a new replication source/destination. Nil selectors
// retain Ceph's current-bucket wildcard behavior. Same-zone replication is not
// supported by native RGW multisite.
type SyncBucketSelector struct{ Name, Tenant string }

// SyncObjectTag matches an exact key/value. Multiple pairs are ORed and
// repeated keys with different values are permitted. Native CLI comma parsing
// cannot represent comma-containing keys/values; '=' is only allowed in values.
//
// Key is 1 to 128 characters and Value up to 256.
type SyncObjectTag struct{ Key, Value string }

// pipeConfig builds the native pipe-create command and the pipe document
// that its readback must equal. Every concrete bucket, principal and storage
// class is confirmed against the native master before anything is returned.
func (g *SyncGroup) pipeConfig(ctx context.Context, master *rgwZoneState, config SyncPipeConfig, source, dest []string) ([]string, map[string]any, error) {
	args := []string{"sync", "group", "pipe", "create", "--group-id", g.ID(), "--pipe-id", config.ID, "--source-zone-ids", strings.Join(source, ","), "--dest-zone-ids", strings.Join(dest, ",")}
	filterArgs, filter, err := syncPipeFilter(config)
	if err != nil {
		return nil, nil, err
	}
	args = append(args, filterArgs...)
	src := map[string]any{"bucket": "*", "zones": syncStrings(source)}
	dst := map[string]any{"bucket": "*", "zones": syncStrings(dest)}
	bucketArgs, sourceInfo, destinationInfo, err := g.pipeBuckets(ctx, master, config, src, dst)
	if err != nil {
		return nil, nil, err
	}
	args = append(args, bucketArgs...)
	destParams := map[string]any{}
	params := map[string]any{"source": map[string]any{"filter": filter}, "dest": destParams, "priority": json.Number(strconv.FormatInt(int64(config.Priority), 10)), "mode": "system"}
	// Ceph 19 stores system-mode pipes with an explicit empty user.
	if master != nil && master.cluster != nil && multicluster.CephBefore(master.cluster, 20) {
		params["user"] = ""
	}
	if config.Priority != 0 {
		args = append(args, "--priority", strconv.FormatInt(int64(config.Priority), 10))
	}
	principalArgs, err := g.pipePrincipals(ctx, config, sourceInfo, destinationInfo, params, destParams)
	if err != nil {
		return nil, nil, err
	}
	args = append(args, principalArgs...)
	if config.DestinationStorageClass != "" {
		if err := g.destinationClass(ctx, config, destinationInfo); err != nil {
			return nil, nil, err
		}
		destParams["storage_class"] = config.DestinationStorageClass
		args = append(args, "--storage-class", config.DestinationStorageClass)
	} else if len(config.DestinationPlacements) > 0 {
		return nil, nil, multisiteInvalid(errors.New("RGW destination placements require a requested storage class"))
	}
	return args, map[string]any{"id": config.ID, "source": src, "dest": dst, "params": params}, nil
}

// syncPipeFilter returns the prefix and tag arguments of a pipe and the
// native source filter they produce.
func syncPipeFilter(config SyncPipeConfig) ([]string, map[string]any, error) {
	var args []string
	filter := map[string]any{"tags": []any{}}
	if config.Prefix != "" {
		filter["prefix"] = config.Prefix
		args = append(args, "--prefix", config.Prefix)
	}
	tags, err := normalizeSyncTags(config.Tags)
	if err != nil {
		return nil, nil, err
	}
	if len(tags) > 0 {
		encoded := make([]string, len(tags))
		native := make([]any, len(tags))
		for i, tag := range tags {
			encoded[i] = tag.Key + "=" + tag.Value
			native[i] = map[string]any{"key": tag.Key, "value": tag.Value}
		}
		filter["tags"] = native
		args = append(args, "--tags-add", strings.Join(encoded, ","))
	}
	return args, filter, nil
}

// pipeBuckets pins each selected bucket to its current native instance
// in src and dst and returns the bucket arguments with the native info of
// both sides. Without a destination selector, a bucket-scoped group's own
// bucket is the destination.
func (g *SyncGroup) pipeBuckets(ctx context.Context, master *rgwZoneState, config SyncPipeConfig, src, dst map[string]any) ([]string, *rgwBucketInstance, *rgwBucketInstance, error) {
	var args []string
	var sourceInfo, destinationInfo *rgwBucketInstance
	for _, side := range []struct {
		selector *SyncBucketSelector
		native   map[string]any
		flag     string
		info     **rgwBucketInstance
	}{{config.SourceBucket, src, "--source", &sourceInfo}, {config.DestinationBucket, dst, "--dest", &destinationInfo}} {
		if side.selector == nil {
			continue
		}
		id, info, err := g.selectedBucket(ctx, master, *side.selector)
		if err != nil {
			return nil, nil, nil, err
		}
		key := side.selector.Name + ":" + id
		if side.selector.Tenant != "" {
			key = side.selector.Tenant + "/" + key
		}
		side.native["bucket"] = key
		args = append(args, side.flag+"-bucket", side.selector.Name, side.flag+"-bucket-id", id)
		if side.selector.Tenant != "" {
			args = append(args, side.flag+"-tenant", side.selector.Tenant)
		}
		*side.info = info
	}
	if destinationInfo == nil && g.scope.Bucket != "" {
		var err error
		if _, destinationInfo, err = g.selectedBucket(ctx, master, SyncBucketSelector{Name: g.scope.Bucket, Tenant: g.scope.Tenant}); err != nil {
			return nil, nil, nil, err
		}
	}
	return args, sourceInfo, destinationInfo, nil
}

// pipePrincipals confirms the destination owner and the user-mode
// principal of a pipe, records them in params and destParams and returns
// their arguments.
func (g *SyncGroup) pipePrincipals(ctx context.Context, config SyncPipeConfig, sourceInfo, destinationInfo *rgwBucketInstance, params, destParams map[string]any) ([]string, error) {
	f := g.owner
	var args []string
	if config.DestinationOwner != nil {
		info, err := g.ownedPrincipal(ctx, config.DestinationOwner)
		if err != nil {
			return nil, err
		}
		if info.AccountID != "" || (info.Type != "" && info.Type != "rgw") {
			return nil, multisiteInvalid(errors.New("RGW owner translation requires a confirmed ordinary user; account owner translation is unproven"))
		}
		if err := checkSyncPrincipalTenants(g.scope, config, info.Tenant); err != nil {
			return nil, err
		}
		if destinationInfo == nil {
			return nil, multisiteInvalid(errors.New("RGW owner translation requires a concrete destination bucket"))
		}
		if config.User != nil && !destinationInfo.Owner.is(info.ID) {
			return nil, multisiteInvalid(errors.New("RGW user-mode owner translation must select the destination bucket owner"))
		}
		destParams["acl_translation"] = map[string]any{"owner": info.ID}
		args = append(args, "--dest-owner", info.ID)
	}
	if config.User == nil {
		return args, nil
	}
	info, err := g.ownedPrincipal(ctx, config.User)
	if err != nil {
		return nil, err
	}
	if err := checkSyncUserModePrincipal(g.scope, config, info, sourceInfo, destinationInfo); err != nil {
		return nil, err
	}
	if zone := f.zoneBefore(20); info.AccountID != "" && zone != nil {
		return nil, accountPipeUnsupported(zone)
	}
	if destinationInfo == nil {
		return nil, multisiteInvalid(errors.New("RGW user mode requires a concrete destination bucket"))
	}
	params["mode"], params["user"] = "user", info.ID
	return append(args, "--mode", "user", "--uid", info.ID), nil
}

func checkSyncPrincipalTenants(scope SyncPolicyScope, config SyncPipeConfig, tenant string) error {
	if scope.Bucket == "" && (config.SourceBucket == nil || config.DestinationBucket == nil) {
		return multisiteInvalid(errors.New("RGW sync principal requires concrete bucket namespaces"))
	}
	sourceTenant, destTenant := scope.Tenant, scope.Tenant
	if config.SourceBucket != nil {
		sourceTenant = config.SourceBucket.Tenant
	}
	if config.DestinationBucket != nil {
		destTenant = config.DestinationBucket.Tenant
	}
	if sourceTenant != tenant || destTenant != tenant || (scope.Bucket != "" && scope.Tenant != tenant) {
		return multisiteInvalid(errors.New("RGW sync principal and all selected bucket tenants must match; cross-tenant translation is unproven"))
	}
	return nil
}

func normalizeSyncTags(tags []SyncObjectTag) ([]SyncObjectTag, error) {
	result := slices.Clone(tags)
	for _, tag := range result {
		if tag.Key == "" || utf8.RuneCountInString(tag.Key) > 128 || utf8.RuneCountInString(tag.Value) > 256 || strings.ContainsAny(tag.Key, ",=") || strings.Contains(tag.Value, ",") || strings.IndexFunc(tag.Key+tag.Value, unicode.IsControl) != -1 {
			return nil, multisiteInvalid(errors.New("RGW sync tags require exact key/value pairs without native CLI comma delimiters"))
		}
	}
	slices.SortFunc(result, func(a, b SyncObjectTag) int {
		if n := strings.Compare(a.Key, b.Key); n != 0 {
			return n
		}
		return strings.Compare(a.Value, b.Value)
	})
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, multisiteInvalid(errors.New("RGW sync tag pairs must be unique"))
		}
	}
	return result, nil
}

func (g *SyncGroup) selectedBucket(ctx context.Context, master *rgwZoneState, selector SyncBucketSelector) (string, *rgwBucketInstance, error) {
	f := g.owner
	if !validRGWSyncID(selector.Name) || selector.Name == "*" || (selector.Tenant != "" && !validRGWSyncID(selector.Tenant)) {
		return "", nil, multisiteInvalid(errors.New("RGW sync bucket selector requires an existing concrete bucket and valid tenant"))
	}
	selected := &SyncGroup{owner: f, scope: SyncPolicyScope{Zonegroup: g.scope.Zonegroup, Bucket: selector.Name, Tenant: selector.Tenant}, realmID: g.realmID, groupID: g.groupID}
	id, err := selected.bucketInstanceID(ctx, master)
	if err != nil {
		return "", nil, err
	}
	selected.bucketID = id
	_, info, err := selected.policyDocument(ctx, master)
	return id, info, err
}

func (g *SyncGroup) ownedPrincipal(ctx context.Context, user *rgw.User) (rgw.UserInfo, error) {
	f := g.owner
	// Info enforces private creation/key identity and actual gateway scope.
	// A replicated user handle retains its creating cluster's ownership, so
	// it must be served by an attached owned gateway rather than adopting a
	// remote UID.
	unconfirmed := multisiteInvalid(errors.New("RGW sync principal was not confirmed as created through an attached owned gateway"))
	if !slices.ContainsFunc(f.zoneStates(), func(zone *rgwZoneState) bool {
		return zone.Zonegroup == g.scope.Zonegroup && zone.Gateway != nil && gwbase.UserOf(user).ServedBy(gwbase.Of(zone.Gateway))
	}) {
		return rgw.UserInfo{}, unconfirmed
	}
	info, err := user.Info(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return rgw.UserInfo{}, ctx.Err()
		}
		return rgw.UserInfo{}, unconfirmed
	}
	if info.ID != user.ID() || info.Namespace != "" || info.Admin || info.System {
		return rgw.UserInfo{}, multisiteInvalid(errors.New("RGW sync principal requires a confirmed local user without global admin or system permissions"))
	}
	return info, nil
}

func checkSyncUserModePrincipal(scope SyncPolicyScope, config SyncPipeConfig, info rgw.UserInfo, source, destination *rgwBucketInstance) error {
	if err := checkSyncPrincipalTenants(scope, config, info.Tenant); err != nil {
		return err
	}
	if info.AccountID == "" {
		if info.Type != "" && info.Type != "rgw" {
			return multisiteInvalid(errors.New("RGW sync user-mode principal is not an ordinary owned user"))
		}
		return nil
	}
	// Native --uid is the canonical root user's ID, never the account ID.
	// User.Info has already checked its private creation account and lifetime.
	if info.Type != "root" || config.DestinationOwner != nil || config.SourceBucket == nil || config.DestinationBucket == nil || source == nil || destination == nil || !source.Owner.is(info.AccountID) || !destination.Owner.is(info.AccountID) {
		return multisiteInvalid(errors.New("RGW account user mode requires its confirmed root, concrete buckets owned by the same account and no owner translation; IAM or cross-account modes are unproven"))
	}
	return nil
}

func (g *SyncGroup) destinationClass(ctx context.Context, config SyncPipeConfig, info *rgwBucketInstance) error {
	f := g.owner
	if info == nil || len(config.DestinationPlacements) != len(config.DestinationZones) || slices.Contains(config.DestinationZones, "*") {
		return multisiteInvalid(errors.New("RGW storage-class translation requires a concrete bucket and confirmed placements for concrete destination zones"))
	}
	target := strings.SplitN(info.PlacementRule.value, "/", 2)[0]
	if target == "" {
		return multisiteInvalid(errors.New("RGW destination bucket native placement is missing"))
	}
	for _, name := range config.DestinationZones {
		var zone *rgwZoneState
		for _, candidate := range f.zoneStates() {
			if candidate.Name == name && candidate.Zonegroup == g.scope.Zonegroup {
				zone = candidate
			}
		}
		if zone == nil || zone.Gateway == nil {
			return multisiteInvalid(errors.New("RGW destination placement zone is not attached and owned"))
		}
		owned := config.DestinationPlacements[name]
		// The placement's pools and resources live in the cluster of the
		// gateway that created it. Matching native realm, zonegroup and zone
		// IDs do not show this: a period can be imported into another storage
		// cluster.
		if !gwbase.Of(zone.Gateway).SameCluster(gwbase.OriginOf(owned)) {
			return multisiteInvalid(errors.New("RGW destination placement is not owned by the destination zone's cluster"))
		}
		state, err := owned.Status(ctx)
		if err != nil {
			return err
		}
		if !state.Confirmed || state.RealmID != g.realmID || state.ZonegroupID != g.groupID || state.ZoneID != zone.ID || state.Name != target || !slices.ContainsFunc(state.StorageClasses, func(c placement.StorageClass) bool { return c.Name == config.DestinationStorageClass }) {
			return multisiteInvalid(errors.New("RGW destination class does not match its confirmed native placement/bucket scope"))
		}
		_, period, err := f.syncPolicyMaster(ctx)
		if err != nil {
			return err
		}
		var committed map[string]any
		for _, group := range syncPeriodGroups(period) {
			if group["id"] != g.groupID {
				continue
			}
			targets, _ := group["placement_targets"].([]any)
			for _, value := range targets {
				candidate, _ := value.(map[string]any)
				if candidate["name"] == target {
					committed = candidate
				}
			}
		}
		classes, _ := committed["storage_classes"].([]any)
		if !slices.Contains(classes, any(config.DestinationStorageClass)) {
			return multisiteInvalid(errors.New("RGW destination class has not been published in the current committed period"))
		}
	}
	return nil
}

// accountPipeUnsupported refuses a user-mode pipe of an account principal
// for zone, which runs Ceph 19. That is a documented limit of the release: no
// tool reveals it.
func accountPipeUnsupported(zone *rgwZoneState) error {
	facts, err := ceph.FactsOf(zone.cluster)
	if err != nil {
		return err
	}
	return &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: "zone " + zone.Name, Version: facts.CephVersion,
		Capability: "RGW account principals in user-mode sync pipes",
		Err:        errors.New("RGW user-mode sync with an account principal needs Ceph 20 or later; Ceph 19 resolves no source for such a pipe")}
}

// zoneBefore returns an owned zone whose cluster runs a release older than
// major, or nil. Any such zone may resolve the pipe, so one is enough to
// refuse.
func (f *Fixture) zoneBefore(major int) *rgwZoneState {
	states := f.zoneStates()
	index := slices.IndexFunc(states, func(zone *rgwZoneState) bool {
		return zone.cluster != nil && multicluster.CephBefore(zone.cluster, major)
	})
	if index < 0 {
		return nil
	}
	return states[index]
}
