package multicluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

// RGWSyncPolicyScope selects an owned zonegroup, or an existing bucket within
// that group. An empty Zonegroup selects the original source's group. Bucket
// policy fixes the native bucket instance ID and rejects a recreated name.
type RGWSyncPolicyScope struct{ Zonegroup, Bucket, Tenant string }

// RGWSyncGroupStatus separates permission from activation. Allowed permits a
// bucket policy to enable its subset; Enabled activates the group's pipes;
// Forbidden takes precedence over other matching enabled groups.
type RGWSyncGroupStatus string

const (
	RGWSyncAllowed   RGWSyncGroupStatus = "allowed"
	RGWSyncEnabled   RGWSyncGroupStatus = "enabled"
	RGWSyncForbidden RGWSyncGroupStatus = "forbidden"
)

type RGWSyncGroupConfig struct {
	ID     string
	Status RGWSyncGroupStatus
}

// RGWSyncFlowConfig selects either a named symmetrical Zones set, or a
// directional SourceZone/DestinationZone pair. Directional flows have no
// native ID: leave ID and Zones empty. Zone names must be attached and owned
// by this fixture in the selected group. Bucket policies may only narrow the
// flow permitted by their parent zonegroup.
type RGWSyncFlowConfig struct {
	ID                          string
	Zones                       []string
	SourceZone, DestinationZone string
}

// RGWSyncPipeConfig selects owned zone names or a sole "*" on each side.
// Nil bucket selectors use the current bucket; an empty Prefix selects all keys.
// Source tags are ORed; when a prefix is also supplied both must match. User
// mode uses a confirmed ordinary user to validate source read/destination write
// permissions. Principal and selected bucket tenants must match. Account and
// cross-tenant translation remain unproven and are refused by this helper.
// This helper never creates S3 buckets or grants access.
type RGWSyncPipeConfig struct {
	ID                              string
	SourceZones, DestinationZones   []string
	Prefix                          string
	SourceBucket, DestinationBucket *RGWSyncBucketSelector
	Tags                            []RGWSyncObjectTag
	Priority                        int32
	DestinationOwner, User          *ceph.RGWUser
	DestinationStorageClass         string
	// Storage-class translation requires a concrete destination bucket and a
	// confirmed native placement for each concrete DestinationZones entry.
	// The caller must publish/import/reload those placements before traffic.
	DestinationPlacements map[string]*ceph.RGWPlacement
}

// RGWSyncGroup is a fresh group created by this fixture. Copies share native
// readback and lifecycle state. IDs on unrelated existing groups are never
// adopted. Native edits must not race these methods.
type RGWSyncGroup struct {
	owner                      *RGWMultisite
	scope                      RGWSyncPolicyScope
	realmID, groupID, bucketID string
	state                      *rgwSyncGroupState
}

type rgwSyncGroupState struct {
	id                 string
	confirmed, removed bool
	group              map[string]any
	period             map[string]any
	pending            *rgwSyncPending
}

type rgwSyncPending struct {
	beforePolicy, beforeGroup map[string]any
	args                      []string
	check                     func(map[string]any) error
}

func (g *RGWSyncGroup) ID() string {
	if g == nil || g.state == nil {
		return ""
	}
	return g.state.id
}
func (g RGWSyncGroup) String() string   { return "RGW sync group " + g.ID() }
func (g RGWSyncGroup) GoString() string { return g.String() }

func validRGWSyncID(id string) bool {
	return id != "" && len(id) <= 128 && !strings.HasPrefix(id, "-") && strings.IndexFunc(id, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r) || strings.ContainsRune(",/:$", r)
	}) == -1
}

func validRGWSyncStatus(status RGWSyncGroupStatus) bool {
	return status == RGWSyncAllowed || status == RGWSyncEnabled || status == RGWSyncForbidden
}

// CreateSyncGroup stores a fresh empty group on the current owned metadata
// master. Zonegroup changes require ApplySyncGroup; bucket changes are dynamic
// native metadata updates and need no period commit. A partial unconfirmed
// handle cannot remove uncertain native configuration. Use Admin inspection or
// terminate the disposable cluster after an ambiguous creation failure.
func (f *RGWMultisite) CreateSyncGroup(ctx context.Context, scope RGWSyncPolicyScope, config RGWSyncGroupConfig) (*RGWSyncGroup, error) {
	if config.ID == "" {
		config.ID = "tc-sync-" + uuid.NewString()
	}
	if !validRGWSyncID(config.ID) || !validRGWSyncStatus(config.Status) || (scope.Tenant != "" && scope.Bucket == "") || (scope.Bucket != "" && !validRGWSyncID(scope.Bucket)) || (scope.Tenant != "" && !validRGWSyncID(scope.Tenant)) {
		return nil, errors.New("invalid RGW sync group, status or bucket scope")
	}
	if f == nil {
		return nil, errors.New("RGW multisite fixture is required")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	master, period, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return nil, err
	}
	if scope.Zonegroup == "" {
		scope.Zonegroup = f.zoneGroupName(f.config.SourceZone)
	}
	gid := f.groupIDs[scope.Zonegroup]
	if gid == "" {
		return nil, errors.New("RGW sync policy requires an attached owned zonegroup")
	}
	g := &RGWSyncGroup{owner: f, scope: scope, realmID: f.RealmID, groupID: gid, state: &rgwSyncGroupState{id: config.ID, period: period}}
	if scope.Bucket != "" {
		g.bucketID, err = f.syncBucketID(ctx, master, g)
		if err != nil {
			return nil, err
		}
	}
	before, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return nil, err
	}
	if syncNativeGroup(before, config.ID) != nil {
		return nil, errors.New("RGW sync group ID already exists; refusing adoption")
	}
	if _, err := f.syncGroupCommand(ctx, master, g, "sync", "group", "create", "--group-id", config.ID, "--status", string(config.Status)); err != nil {
		return g, err
	}
	after, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return g, err
	}
	created := syncNativeGroup(after, config.ID)
	if created == nil || created["status"] != string(config.Status) || !syncGroupEmpty(created) || !reflect.DeepEqual(before, syncPolicyReplace(after, config.ID, nil)) {
		return g, errors.New("RGW fresh sync group readback or unrelated policy differs")
	}
	g.state.group, g.state.confirmed = created, true
	return g, nil
}

// CreateSyncFlow adds a fresh flow to an owned unchanged group. Native
// directional identity is the exact source/destination pair, not flow-id.
func (f *RGWMultisite) CreateSyncFlow(ctx context.Context, g *RGWSyncGroup, config RGWSyncFlowConfig) error {
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		args, flow, key, err := f.syncFlowConfig(g, config)
		if err != nil {
			return nil, nil, err
		}
		if syncFlowPresent(before, key, flow) {
			return nil, nil, errors.New("RGW sync flow already exists")
		}
		return append([]string{"sync", "group", "flow", "create", "--group-id", g.ID()}, args...), func(after map[string]any) error {
			if !syncFlowMatches(after, key, flow) || !reflect.DeepEqual(before, syncGroupWithoutFlow(after, key, flow)) {
				return errors.New("RGW sync flow readback or unrelated group policy differs")
			}
			return nil
		}, nil
	})
}

// RemoveSyncFlow removes only a flow present in the owned group's exact native
// snapshot. Symmetrical removal deletes the entire named set; directional
// removal selects the native pair. It never changes another group's policy.
func (f *RGWMultisite) RemoveSyncFlow(ctx context.Context, g *RGWSyncGroup, config RGWSyncFlowConfig) error {
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		args, flow, key, err := f.syncFlowConfig(g, config)
		if err != nil {
			return nil, nil, err
		}
		if !syncFlowPresent(before, key, flow) {
			return nil, nil, errors.New("RGW sync flow is absent from the owned group")
		}
		if key == "symmetrical" {
			args = []string{"--flow-type", "symmetrical", "--flow-id", config.ID}
		}
		want := syncGroupWithoutFlow(before, key, flow)
		return append([]string{"sync", "group", "flow", "remove", "--group-id", g.ID()}, args...), func(after map[string]any) error {
			if !reflect.DeepEqual(want, after) {
				return errors.New("RGW sync flow removal changed unrelated group policy")
			}
			return nil
		}, nil
	})
}

// CreateSyncPipe adds a fresh identity-checked pipe. Zonegroup policy
// forms an upper bound for a bucket policy; a stored pipe alone does not prove
// effective replication. Verify positive/negative writes through the S3 client.
func (f *RGWMultisite) CreateSyncPipe(ctx context.Context, g *RGWSyncGroup, config RGWSyncPipeConfig) error {
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		if !validRGWSyncID(config.ID) || strings.IndexFunc(config.Prefix, unicode.IsControl) != -1 {
			return nil, nil, errors.New("invalid RGW sync pipe ID or prefix")
		}
		source, err := f.syncZoneIDs(g, config.SourceZones, true)
		if err != nil {
			return nil, nil, err
		}
		dest, err := f.syncZoneIDs(g, config.DestinationZones, true)
		if err != nil {
			return nil, nil, err
		}
		if syncNativePipe(before, config.ID) != nil {
			return nil, nil, errors.New("RGW sync pipe ID already exists")
		}
		args, expected, err := f.syncPipeConfig(opCtx, master, g, config, source, dest)
		if err != nil {
			return nil, nil, err
		}
		return args, func(after map[string]any) error {
			pipe := syncNativePipe(after, config.ID)
			if !reflect.DeepEqual(pipe, expected) || !reflect.DeepEqual(before, syncGroupWithoutPipe(after, config.ID)) {
				return errors.New("RGW sync pipe readback or unrelated group policy differs")
			}
			return nil
		}, nil
	})
}

// SetSyncPipePrefix changes only the selected owned pipe's source prefix.
// Empty prefix removes the filter. It affects future replication selection;
// historical backfill and deletion of previously copied objects are not promised.
func (f *RGWMultisite) SetSyncPipePrefix(ctx context.Context, g *RGWSyncGroup, id, prefix string) error {
	if !validRGWSyncID(id) || strings.IndexFunc(prefix, unicode.IsControl) != -1 {
		return errors.New("invalid RGW sync pipe ID or prefix")
	}
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		want := syncJSONClone(before)
		pipe := syncNativePipe(want, id)
		if pipe == nil {
			return nil, nil, errors.New("RGW sync pipe is absent from owned group")
		}
		params, _ := pipe["params"].(map[string]any)
		source, _ := params["source"].(map[string]any)
		filter, _ := source["filter"].(map[string]any)
		if filter == nil {
			return nil, nil, errors.New("RGW native pipe source filter is missing")
		}
		args := []string{"sync", "group", "pipe", "modify", "--group-id", g.ID(), "--pipe-id", id}
		if prefix == "" {
			delete(filter, "prefix")
			args = append(args, "--prefix-rm")
		} else {
			filter["prefix"] = prefix
			args = append(args, "--prefix", prefix)
		}
		return args, func(after map[string]any) error {
			if !reflect.DeepEqual(want, after) {
				return errors.New("RGW prefix update changed unrelated pipe policy")
			}
			return nil
		}, nil
	})
}

func (f *RGWMultisite) RemoveSyncPipe(ctx context.Context, g *RGWSyncGroup, id string) error {
	if !validRGWSyncID(id) {
		return errors.New("invalid RGW sync pipe ID")
	}
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		if syncNativePipe(before, id) == nil {
			return nil, nil, errors.New("RGW sync pipe is absent from owned group")
		}
		want := syncGroupWithoutPipe(before, id)
		return []string{"sync", "group", "pipe", "remove", "--group-id", g.ID(), "--pipe-id", id}, func(after map[string]any) error {
			if !reflect.DeepEqual(want, after) {
				return errors.New("RGW pipe removal changed unrelated policy")
			}
			return nil
		}, nil
	})
}

// SetSyncGroupStatus changes an owned group's allowed/enabled/forbidden state.
func (f *RGWMultisite) SetSyncGroupStatus(ctx context.Context, g *RGWSyncGroup, status RGWSyncGroupStatus) error {
	if !validRGWSyncStatus(status) {
		return errors.New("invalid RGW sync group status")
	}
	return f.changeSyncGroup(ctx, g, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		want := syncJSONClone(before)
		want["status"] = string(status)
		return []string{"sync", "group", "modify", "--group-id", g.ID(), "--status", string(status)}, func(after map[string]any) error {
			if !reflect.DeepEqual(want, after) {
				return errors.New("RGW group status update changed unrelated policy")
			}
			return nil
		}, nil
	})
}

// RemoveSyncGroup removes only this confirmed group's unchanged native policy.
// Bucket/object data remains. Zonegroup removal needs ApplySyncGroup afterwards
// to publish it; removal is idempotent across handle copies.
func (f *RGWMultisite) RemoveSyncGroup(ctx context.Context, g *RGWSyncGroup) error {
	if f == nil {
		return errors.New("RGW multisite fixture is required")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	if err := f.validateSyncGroup(g, true); err != nil {
		return err
	}
	if g.state.removed {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	master, _, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	if err := f.finishPendingSyncGroup(ctx, master, g); err != nil {
		return err
	}
	before, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return err
	}
	actual := syncNativeGroup(before, g.ID())
	if actual == nil {
		g.state.group = nil
		g.state.removed = true
		return nil
	}
	if !reflect.DeepEqual(actual, g.state.group) {
		return errors.New("RGW owned sync group was externally changed; refusing removal")
	}
	if _, err := f.syncGroupCommand(ctx, master, g, "sync", "group", "remove", "--group-id", g.ID()); err != nil {
		return err
	}
	after, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(syncPolicyReplace(before, g.ID(), nil), after) {
		return errors.New("RGW sync group removal readback differs")
	}
	g.state.group = nil
	g.state.removed = true
	return nil
}

func (f *RGWMultisite) validateSyncGroup(g *RGWSyncGroup, allowRemoved bool) error {
	if f.closed {
		return errors.New("RGW multisite is terminated")
	}
	if g == nil || g.owner != f || g.state == nil || !g.state.confirmed || (!allowRemoved && g.state.removed) {
		return errors.New("RGW sync group must be active and confirmed as created by this fixture")
	}
	if g.realmID != f.RealmID || g.groupID != f.groupIDs[g.scope.Zonegroup] {
		return errors.New("RGW sync group realm/zonegroup identity changed")
	}
	return nil
}

type rgwSyncMutation func(context.Context, *rgwZoneState, map[string]any) ([]string, func(map[string]any) error, error)

func (f *RGWMultisite) changeSyncGroup(ctx context.Context, g *RGWSyncGroup, change rgwSyncMutation) error {
	if f == nil {
		return errors.New("RGW multisite fixture is required")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	if err := f.validateSyncGroup(g, false); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	master, _, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	if pending := g.state.pending; pending != nil {
		requested, _, requestErr := change(ctx, master, syncJSONClone(pending.beforeGroup))
		same := requestErr == nil && slices.Equal(requested, pending.args)
		completed, err := f.reconcileSyncGroup(ctx, master, g)
		if err != nil {
			return err
		}
		if completed && same {
			return nil
		}
		if !completed {
			if !same {
				return errors.New("RGW sync group has an unconfirmed pending mutation; retry the same request")
			}
			return f.executePendingSyncGroup(ctx, master, g)
		}
	}
	before, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return err
	}
	actual := syncNativeGroup(before, g.ID())
	if !reflect.DeepEqual(actual, g.state.group) {
		return errors.New("RGW owned sync group was externally changed")
	}
	args, check, err := change(ctx, master, actual)
	if err != nil {
		return err
	}
	// Repeated status/prefix assignments can already satisfy their exact
	// intended readback. Do not create an unresolved pending no-op.
	if check(actual) == nil {
		return nil
	}
	g.state.pending = &rgwSyncPending{beforePolicy: syncJSONClone(before), beforeGroup: syncJSONClone(actual), args: slices.Clone(args), check: check}
	return f.executePendingSyncGroup(ctx, master, g)
}

func (f *RGWMultisite) executePendingSyncGroup(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup) error {
	_, commandErr := f.syncGroupCommand(ctx, master, g, g.state.pending.args...)
	readCtx := ctx
	cancel := func() {}
	if commandErr != nil || ctx.Err() != nil {
		readCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	}
	completed, readErr := f.reconcileSyncGroup(readCtx, master, g)
	cancel()
	if completed && readErr == nil {
		return nil
	}
	if readErr != nil && commandErr == nil {
		checkCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		completed, readErr = f.reconcileSyncGroup(checkCtx, master, g)
		stop()
		if completed && readErr == nil {
			return nil
		}
	}
	if commandErr == nil && readErr == nil {
		commandErr = errors.New("RGW native mutation is not yet confirmed; retry the same request")
	}
	return errors.Join(commandErr, readErr)
}

func (f *RGWMultisite) reconcileSyncGroup(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup) (bool, error) {
	pending := g.state.pending
	if pending == nil {
		return true, nil
	}
	actual, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return false, err
	}
	if reflect.DeepEqual(actual, pending.beforePolicy) {
		if group := syncNativeGroup(actual, g.ID()); group != nil && pending.check(group) == nil {
			g.state.group, g.state.pending = group, nil
			return true, nil
		}
		return false, nil
	}
	group := syncNativeGroup(actual, g.ID())
	if group == nil || !reflect.DeepEqual(syncPolicyReplace(actual, g.ID(), nil), syncPolicyReplace(pending.beforePolicy, g.ID(), nil)) {
		return false, errors.New("RGW pending mutation readback contains unrelated sync policy changes")
	}
	if err := pending.check(group); err != nil {
		return false, errors.New("RGW pending mutation differs from its exact owned intent")
	}
	g.state.group, g.state.pending = group, nil
	return true, nil
}

func (f *RGWMultisite) finishPendingSyncGroup(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup) error {
	completed, err := f.reconcileSyncGroup(ctx, master, g)
	if err != nil {
		return err
	}
	if !completed {
		return errors.New("RGW sync group has an unconfirmed pending mutation; retry the same request")
	}
	return nil
}

func (f *RGWMultisite) syncPolicyMaster(ctx context.Context) (*rgwZoneState, map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if f.closed {
		return nil, nil, errors.New("RGW multisite is terminated")
	}
	states := f.zoneStates()
	if len(states) == 0 || states[0].client == nil {
		return nil, nil, errors.New("RGW fixture has no initialized owned zones")
	}
	data, err := f.syncPolicyAdmin(ctx, states[0], states[0].Zonegroup, "period", "get")
	if err != nil {
		return nil, nil, err
	}
	period, err := decodeRGWTopologyPeriod(data)
	if err != nil {
		return nil, nil, err
	}
	master := rgwPeriodMaster(period, states)
	if master == nil {
		return nil, nil, errors.New("RGW metadata master is not attached and owned")
	}
	if err := f.checkZonePeriod(period, master, states); err != nil {
		return nil, nil, err
	}
	native, err := syncJSON(data)
	return master, native, err
}

func (f *RGWMultisite) syncPolicyAdmin(ctx context.Context, zone *rgwZoneState, group string, args ...string) ([]byte, error) {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--realm-id", f.RealmID, "--zonegroup-id", f.groupIDs[group], "--zone-id", zone.ID, "--format", "json"}
	status := (len(args) >= 2 && args[0] == "sync" && args[1] == "status") || (len(args) >= 3 && args[0] == "bucket" && args[1] == "sync" && args[2] == "status")
	data, err := syncNativeExecMode(ctx, zone.client, status, append(argv, args...)...)
	return data, err
}

func syncNativeExec(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	return syncNativeExecMode(ctx, ctr, false, args...)
}
func syncNativeExecMode(ctx context.Context, ctr testcontainers.Container, status bool, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctr == nil {
		return nil, errors.New("RGW native CLI container is unavailable")
	}
	code, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("RGW native command failed; output is redacted")
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, errors.New("RGW native command stream failed; output is redacted")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("RGW native command exited %d; output is redacted", code)
	}
	if status {
		warnings := strings.ToLower(stderr.String())
		if strings.Contains(warnings, "error") || strings.Contains(warnings, "failed") {
			return nil, errors.New("RGW native sync comparison reported an error; output is redacted")
		}
	}
	return stdout.Bytes(), nil
}

func (f *RGWMultisite) syncGroupCommand(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup, args ...string) ([]byte, error) {
	if g.scope.Bucket != "" {
		args = append(args, "--bucket", syncScopedBucketName(g.scope), "--bucket-id", g.bucketID)
	}
	return f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, args...)
}

func (f *RGWMultisite) syncBucketID(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup) (string, error) {
	args := []string{"bucket", "stats", "--bucket", syncScopedBucketName(g.scope)}
	data, err := f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, args...)
	if err != nil {
		return "", err
	}
	var bucket struct{ ID, Bucket, Tenant, Zonegroup string }
	if json.Unmarshal(data, &bucket) != nil || bucket.ID == "" || bucket.Bucket != g.scope.Bucket || bucket.Tenant != g.scope.Tenant || bucket.Zonegroup != g.groupID {
		return "", errors.New("RGW bucket native instance or zonegroup identity differs")
	}
	return bucket.ID, nil
}

// The native bucket CLI accepts tenant/name. Its global --tenant option requires
// a user ID, which bucket statistics and system-mode sync mutations do not need.
// Decoded native metadata is still checked against the separate private fields.
func syncScopedBucketName(scope RGWSyncPolicyScope) string {
	if scope.Tenant != "" {
		return scope.Tenant + "/" + scope.Bucket
	}
	return scope.Bucket
}

func (f *RGWMultisite) syncPolicyDocument(ctx context.Context, master *rgwZoneState, g *RGWSyncGroup) (map[string]any, map[string]any, error) {
	data, err := f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, "zonegroup", "get")
	if err != nil {
		return nil, nil, err
	}
	group, err := syncJSON(data)
	if err != nil {
		return nil, nil, err
	}
	if group["id"] != g.groupID || group["realm_id"] != g.realmID {
		return nil, nil, errors.New("RGW sync policy native zonegroup identity changed")
	}
	if g.scope.Bucket == "" {
		policy, err := syncPolicyFromDocument(group)
		return policy, group, err
	}
	id, err := f.syncBucketID(ctx, master, g)
	if err != nil {
		return nil, nil, err
	}
	if id != g.bucketID {
		return nil, nil, errors.New("RGW bucket name was recreated; refusing sync policy operation")
	}
	key := g.scope.Bucket + ":" + g.bucketID
	if g.scope.Tenant != "" {
		key = g.scope.Tenant + "/" + key
	}
	data, err = f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, "metadata", "get", "bucket.instance:"+key)
	if err != nil {
		return nil, nil, err
	}
	metadata, err := syncJSON(data)
	if err != nil {
		return nil, nil, err
	}
	body, _ := metadata["data"].(map[string]any)
	info, _ := body["bucket_info"].(map[string]any)
	bucket, _ := info["bucket"].(map[string]any)
	if metadata["key"] != "bucket.instance:"+key || info == nil || bucket["name"] != g.scope.Bucket || bucket["tenant"] != g.scope.Tenant || bucket["bucket_id"] != g.bucketID || info["zonegroup"] != g.groupID {
		return nil, nil, errors.New("RGW bucket metadata instance identity differs")
	}
	policy, err := syncPolicyFromDocument(info)
	return policy, info, err
}

func (f *RGWMultisite) syncZoneIDs(g *RGWSyncGroup, names []string, wildcard bool) ([]string, error) {
	if len(names) == 0 {
		return nil, errors.New("RGW sync zone list must not be empty")
	}
	if wildcard && len(names) == 1 && names[0] == "*" {
		return []string{"*"}, nil
	}
	ids := make([]string, 0, len(names))
	for _, name := range names {
		var found *rgwZoneState
		for _, zone := range f.zoneStates() {
			if zone.Name == name && zone.Zonegroup == g.scope.Zonegroup {
				found = zone
			}
		}
		if found == nil || found.ID == "" || slices.Contains(ids, found.ID) {
			return nil, errors.New("RGW sync zones must be unique attached owned zones in this zonegroup")
		}
		ids = append(ids, found.ID)
	}
	slices.Sort(ids)
	return ids, nil
}

func (f *RGWMultisite) syncFlowConfig(g *RGWSyncGroup, c RGWSyncFlowConfig) ([]string, map[string]any, string, error) {
	if len(c.Zones) > 0 {
		if !validRGWSyncID(c.ID) || c.SourceZone != "" || c.DestinationZone != "" || len(c.Zones) < 2 {
			return nil, nil, "", errors.New("symmetrical RGW flow requires its ID and at least two zones")
		}
		ids, err := f.syncZoneIDs(g, c.Zones, false)
		if err != nil {
			return nil, nil, "", err
		}
		return []string{"--flow-type", "symmetrical", "--flow-id", c.ID, "--zone-ids", strings.Join(ids, ",")}, map[string]any{"id": c.ID, "zones": syncStrings(ids)}, "symmetrical", nil
	}
	if c.ID != "" || c.SourceZone == "" || c.SourceZone == c.DestinationZone {
		return nil, nil, "", errors.New("directional RGW flow requires distinct owned source/destination zones and no ID")
	}
	if _, err := f.syncZoneIDs(g, []string{c.SourceZone, c.DestinationZone}, false); err != nil {
		return nil, nil, "", err
	}
	var source, dest string
	for _, z := range f.zoneStates() {
		if z.Name == c.SourceZone {
			source = z.ID
		}
		if z.Name == c.DestinationZone {
			dest = z.ID
		}
	}
	return []string{"--flow-type", "directional", "--flow-id", source + "-to-" + dest, "--source-zone-id", source, "--dest-zone-id", dest}, map[string]any{"source_zone": source, "dest_zone": dest}, "directional", nil
}

func syncJSON(data []byte) (map[string]any, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || value == nil {
		return nil, errors.New("decode RGW native sync configuration")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("RGW native sync configuration has trailing output")
	}
	return value, nil
}
func syncJSONClone(value map[string]any) map[string]any {
	data, _ := json.Marshal(value)
	copy, _ := syncJSON(data)
	return copy
}
func syncStrings(values []string) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}
func syncPolicyFromDocument(doc map[string]any) (map[string]any, error) {
	if doc["sync_policy"] == nil {
		return map[string]any{"groups": []any{}}, nil
	}
	policy, ok := doc["sync_policy"].(map[string]any)
	if !ok {
		return nil, errors.New("RGW native sync policy is malformed")
	}
	groups, ok := policy["groups"].([]any)
	if !ok {
		return nil, errors.New("RGW native sync groups are malformed")
	}
	seen := map[string]bool{}
	for _, v := range groups {
		group, ok := v.(map[string]any)
		id, _ := group["id"].(string)
		if !ok || id == "" || seen[id] {
			return nil, errors.New("RGW native sync group identity is malformed or duplicated")
		}
		seen[id] = true
	}
	return policy, nil
}
func syncNativeGroup(policy map[string]any, id string) map[string]any {
	groups, _ := policy["groups"].([]any)
	for _, v := range groups {
		g, _ := v.(map[string]any)
		if g["id"] == id {
			return g
		}
	}
	return nil
}
func syncPolicyReplace(policy map[string]any, id string, group map[string]any) map[string]any {
	copy := syncJSONClone(policy)
	groups, _ := copy["groups"].([]any)
	groups = slices.DeleteFunc(groups, func(v any) bool { g, _ := v.(map[string]any); return g["id"] == id })
	if group != nil {
		groups = append(groups, syncJSONClone(group))
	}
	slices.SortFunc(groups, func(a, b any) int {
		left, _ := a.(map[string]any)
		right, _ := b.(map[string]any)
		return strings.Compare(fmt.Sprint(left["id"]), fmt.Sprint(right["id"]))
	})
	copy["groups"] = groups
	return copy
}
func syncGroupEmpty(g map[string]any) bool {
	flows, _ := g["data_flow"].(map[string]any)
	pipes, _ := g["pipes"].([]any)
	return flows != nil && len(flows) == 0 && pipes != nil && len(pipes) == 0
}
func syncNativePipe(g map[string]any, id string) map[string]any {
	pipes, _ := g["pipes"].([]any)
	for _, v := range pipes {
		p, _ := v.(map[string]any)
		if p["id"] == id {
			return p
		}
	}
	return nil
}
func syncGroupWithoutPipe(g map[string]any, id string) map[string]any {
	copy := syncJSONClone(g)
	pipes, _ := copy["pipes"].([]any)
	copy["pipes"] = slices.DeleteFunc(pipes, func(v any) bool { p, _ := v.(map[string]any); return p["id"] == id })
	return copy
}
func syncFlowPresent(g map[string]any, key string, flow map[string]any) bool {
	data, _ := g["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	return slices.ContainsFunc(flows, func(v any) bool {
		f, _ := v.(map[string]any)
		if key == "symmetrical" {
			return f["id"] == flow["id"]
		}
		return reflect.DeepEqual(f, flow)
	})
}
func syncFlowMatches(g map[string]any, key string, flow map[string]any) bool {
	data, _ := g["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	return slices.ContainsFunc(flows, func(v any) bool { return reflect.DeepEqual(v, flow) })
}
func syncGroupWithoutFlow(g map[string]any, key string, flow map[string]any) map[string]any {
	copy := syncJSONClone(g)
	data, _ := copy["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	flows = slices.DeleteFunc(flows, func(v any) bool {
		f, _ := v.(map[string]any)
		if key == "symmetrical" {
			return f["id"] == flow["id"]
		}
		return reflect.DeepEqual(f, flow)
	})
	if len(flows) == 0 {
		delete(data, key)
	} else {
		data[key] = flows
	}
	return copy
}
