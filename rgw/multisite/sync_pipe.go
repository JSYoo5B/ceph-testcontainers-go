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
type SyncObjectTag struct{ Key, Value string }

func (f *Fixture) syncPipeConfig(ctx context.Context, master *rgwZoneState, g *SyncGroup, config SyncPipeConfig, source, dest []string) ([]string, map[string]any, error) {
	args := []string{"sync", "group", "pipe", "create", "--group-id", g.ID(), "--pipe-id", config.ID, "--source-zone-ids", strings.Join(source, ","), "--dest-zone-ids", strings.Join(dest, ",")}
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
	src := map[string]any{"bucket": "*", "zones": syncStrings(source)}
	dst := map[string]any{"bucket": "*", "zones": syncStrings(dest)}
	var sourceInfo, destinationInfo map[string]any
	for _, side := range []struct {
		selector *SyncBucketSelector
		native   map[string]any
		flag     string
	}{{config.SourceBucket, src, "--source"}, {config.DestinationBucket, dst, "--dest"}} {
		if side.selector == nil {
			continue
		}
		id, info, err := f.syncSelectedBucket(ctx, master, g, *side.selector)
		if err != nil {
			return nil, nil, err
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
		if side.flag == "--source" {
			sourceInfo = info
		} else {
			destinationInfo = info
		}
	}
	if destinationInfo == nil && g.scope.Bucket != "" {
		_, destinationInfo, err = f.syncSelectedBucket(ctx, master, g, SyncBucketSelector{Name: g.scope.Bucket, Tenant: g.scope.Tenant})
		if err != nil {
			return nil, nil, err
		}
	}
	destParams := map[string]any{}
	params := map[string]any{"source": map[string]any{"filter": filter}, "dest": destParams, "priority": json.Number(strconv.FormatInt(int64(config.Priority), 10)), "mode": "system"}
	// Ceph 19 stores system-mode pipes with an explicit empty user.
	if master != nil && master.cluster != nil && multicluster.CephBefore(master.cluster, 20) {
		params["user"] = ""
	}
	if config.Priority != 0 {
		args = append(args, "--priority", strconv.FormatInt(int64(config.Priority), 10))
	}
	if config.DestinationOwner != nil {
		info, err := f.syncOwnedPrincipal(ctx, g, config.DestinationOwner)
		if err != nil {
			return nil, nil, err
		}
		if info.AccountID != "" || (info.Type != "" && info.Type != "rgw") {
			return nil, nil, multisiteInvalid(errors.New("RGW owner translation requires a confirmed ordinary user; account owner translation is unproven"))
		}
		if err := checkSyncPrincipalTenants(g.scope, config, info.Tenant); err != nil {
			return nil, nil, err
		}
		if destinationInfo == nil {
			return nil, nil, multisiteInvalid(errors.New("RGW owner translation requires a concrete destination bucket"))
		}
		if config.User != nil && destinationInfo["owner"] != info.ID {
			return nil, nil, multisiteInvalid(errors.New("RGW user-mode owner translation must select the destination bucket owner"))
		}
		destParams["acl_translation"] = map[string]any{"owner": info.ID}
		args = append(args, "--dest-owner", info.ID)
	}
	if config.User != nil {
		info, err := f.syncOwnedPrincipal(ctx, g, config.User)
		if err != nil {
			return nil, nil, err
		}
		if err := checkSyncUserModePrincipal(g.scope, config, info, sourceInfo, destinationInfo); err != nil {
			return nil, nil, err
		}
		if zone := f.zoneBefore(20); info.AccountID != "" && zone != nil {
			return nil, nil, accountPipeUnsupported(zone)
		}
		if destinationInfo == nil {
			return nil, nil, multisiteInvalid(errors.New("RGW user mode requires a concrete destination bucket"))
		}
		params["mode"], params["user"] = "user", info.ID
		args = append(args, "--mode", "user", "--uid", info.ID)
	}
	if config.DestinationStorageClass != "" {
		if err := f.syncDestinationClass(ctx, g, config, destinationInfo); err != nil {
			return nil, nil, err
		}
		destParams["storage_class"] = config.DestinationStorageClass
		args = append(args, "--storage-class", config.DestinationStorageClass)
	} else if len(config.DestinationPlacements) > 0 {
		return nil, nil, multisiteInvalid(errors.New("RGW destination placements require a requested storage class"))
	}
	return args, map[string]any{"id": config.ID, "source": src, "dest": dst, "params": params}, nil
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

func (f *Fixture) syncSelectedBucket(ctx context.Context, master *rgwZoneState, g *SyncGroup, selector SyncBucketSelector) (string, map[string]any, error) {
	if !validRGWSyncID(selector.Name) || selector.Name == "*" || (selector.Tenant != "" && !validRGWSyncID(selector.Tenant)) {
		return "", nil, multisiteInvalid(errors.New("RGW sync bucket selector requires an existing concrete bucket and valid tenant"))
	}
	selected := &SyncGroup{owner: f, scope: SyncPolicyScope{Zonegroup: g.scope.Zonegroup, Bucket: selector.Name, Tenant: selector.Tenant}, realmID: g.realmID, groupID: g.groupID}
	id, err := f.syncBucketID(ctx, master, selected)
	if err != nil {
		return "", nil, err
	}
	selected.bucketID = id
	_, info, err := f.syncPolicyDocument(ctx, master, selected)
	return id, info, err
}

func (f *Fixture) syncOwnedPrincipal(ctx context.Context, g *SyncGroup, user *rgw.User) (rgw.UserInfo, error) {
	// Info enforces private creation/key identity and actual gateway scope.
	// A replicated user handle retains its creating cluster's ownership, so
	// it must be served by an attached owned gateway rather than adopting a
	// remote UID.
	unconfirmed := multisiteInvalid(errors.New("RGW sync principal was not confirmed as created through an attached owned gateway"))
	if !slices.ContainsFunc(f.zoneStates(), func(zone *rgwZoneState) bool {
		return zone.Zonegroup == g.scope.Zonegroup && zone.Gateway != nil && user.ServedBy(zone.Gateway)
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

func checkSyncUserModePrincipal(scope SyncPolicyScope, config SyncPipeConfig, info rgw.UserInfo, source, destination map[string]any) error {
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
	if info.Type != "root" || config.DestinationOwner != nil || config.SourceBucket == nil || config.DestinationBucket == nil || source == nil || destination == nil || source["owner"] != info.AccountID || destination["owner"] != info.AccountID {
		return multisiteInvalid(errors.New("RGW account user mode requires its confirmed root, concrete buckets owned by the same account and no owner translation; IAM or cross-account modes are unproven"))
	}
	return nil
}

func (f *Fixture) syncDestinationClass(ctx context.Context, g *SyncGroup, config SyncPipeConfig, info map[string]any) error {
	if info == nil || len(config.DestinationPlacements) != len(config.DestinationZones) || slices.Contains(config.DestinationZones, "*") {
		return multisiteInvalid(errors.New("RGW storage-class translation requires a concrete bucket and confirmed placements for concrete destination zones"))
	}
	rule, _ := info["placement_rule"].(string)
	target := strings.SplitN(rule, "/", 2)[0]
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
		state, err := (config.DestinationPlacements[name]).Status(ctx)
		if err != nil {
			return err
		}
		if !state.Confirmed || state.RealmID != g.realmID || state.ZonegroupID != g.groupID || state.ZoneID != zone.ID || state.Name != target || !slices.ContainsFunc(state.StorageClasses, func(c rgw.StorageClassConfig) bool { return c.Name == config.DestinationStorageClass }) {
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
