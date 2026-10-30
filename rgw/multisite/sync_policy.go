package multisite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

// SyncPolicyScope selects an owned zonegroup, or an existing bucket within
// that group. An empty Zonegroup selects the original source's group. Bucket
// policy fixes the native bucket instance ID and rejects a recreated name.
type SyncPolicyScope struct{ Zonegroup, Bucket, Tenant string }

// SyncGroupStatus separates permission from activation. Allowed permits a
// bucket policy to enable its subset; Enabled activates the group's pipes;
// Forbidden takes precedence over other matching enabled groups.
type SyncGroupStatus string

const (
	SyncAllowed   SyncGroupStatus = "allowed"
	SyncEnabled   SyncGroupStatus = "enabled"
	SyncForbidden SyncGroupStatus = "forbidden"
)

// SyncGroupConfig names a new sync group and selects its initial status.
type SyncGroupConfig struct {
	ID     string
	Status SyncGroupStatus
}

// SyncFlowConfig selects either a named symmetrical Zones set, or a
// directional SourceZone/DestinationZone pair. Directional flows have no
// native ID: leave ID and Zones empty. Zone names must be attached and owned
// by this fixture in the selected group. Bucket policies may only narrow the
// flow permitted by their parent zonegroup.
type SyncFlowConfig struct {
	ID                          string
	Zones                       []string
	SourceZone, DestinationZone string
}

// SyncPipeConfig selects owned zone names or a sole "*" on each side.
// Nil bucket selectors use the current bucket; an empty Prefix selects all keys.
// Source tags are ORed; when a prefix is also supplied both must match. User
// mode uses a confirmed ordinary user, or a confirmed account root with concrete
// source/destination buckets owned by that same account and no DestinationOwner.
// Principal and selected bucket tenants must match. IAM nonroot, cross-account
// and cross-tenant principal translation remain unproven and are refused.
// This helper never creates S3 buckets or grants access.
type SyncPipeConfig struct {
	ID                              string
	SourceZones, DestinationZones   []string
	Prefix                          string
	SourceBucket, DestinationBucket *SyncBucketSelector
	Tags                            []SyncObjectTag
	Priority                        int32
	DestinationOwner, User          *rgw.User
	DestinationStorageClass         string
	// Storage-class translation requires a concrete destination bucket and a
	// confirmed native placement for each concrete DestinationZones entry.
	// The caller must publish/import/reload those placements before traffic.
	DestinationPlacements map[string]*rgw.Placement
}

// SyncGroup is a fresh group created by this fixture. Copies share native
// readback and lifecycle state. IDs on unrelated existing groups are never
// adopted. Native edits must not race these methods.
type SyncGroup struct {
	owner                      *Fixture
	scope                      SyncPolicyScope
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

// ID returns the native sync group ID, or "" for a nil handle.
func (g *SyncGroup) ID() string {
	if g == nil || g.state == nil {
		return ""
	}
	return g.state.id
}

// String names the sync group.
func (g SyncGroup) String() string { return "RGW sync group " + g.ID() }

// GoString formats the sync group like String.
func (g SyncGroup) GoString() string { return g.String() }

func validRGWSyncID(id string) bool {
	return id != "" && len(id) <= 128 && !strings.HasPrefix(id, "-") && strings.IndexFunc(id, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r) || strings.ContainsRune(",/:$", r)
	}) == -1
}

func validRGWSyncStatus(status SyncGroupStatus) bool {
	return status == SyncAllowed || status == SyncEnabled || status == SyncForbidden
}

// CreateSyncGroup stores a fresh empty group on the current owned metadata
// master. Zonegroup changes require SyncGroup.Apply; bucket changes are dynamic
// native metadata updates and need no period commit. A partial unconfirmed
// handle cannot remove uncertain native configuration. Use Admin inspection or
// terminate the disposable cluster after an ambiguous creation failure.
func (f *Fixture) CreateSyncGroup(ctx context.Context, scope SyncPolicyScope, config SyncGroupConfig) (*SyncGroup, error) {
	if config.ID == "" {
		config.ID = "tc-sync-" + uuid.NewString()
	}
	if !validRGWSyncID(config.ID) || !validRGWSyncStatus(config.Status) || (scope.Tenant != "" && scope.Bucket == "") || (scope.Bucket != "" && !validRGWSyncID(scope.Bucket)) || (scope.Tenant != "" && !validRGWSyncID(scope.Tenant)) {
		return nil, multisiteInvalid(errors.New("invalid RGW sync group, status or bucket scope"))
	}
	if f == nil {
		return nil, multisiteInvalid(errors.New("RGW multisite fixture is required"))
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return nil, err
	}
	defer f.topologyMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	master, period, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return nil, err
	}
	if scope.Zonegroup == "" {
		scope.Zonegroup = f.realm.zoneGroupName(f.realm.config.SourceZone)
	}
	gid := f.topology.groupIDs[scope.Zonegroup]
	if gid == "" {
		return nil, multisiteInvalid(errors.New("RGW sync policy requires an attached owned zonegroup"))
	}
	g := &SyncGroup{owner: f, scope: scope, realmID: f.RealmID, groupID: gid, state: &rgwSyncGroupState{id: config.ID, period: period}}
	if scope.Bucket != "" {
		g.bucketID, err = g.bucketInstanceID(ctx, master)
		if err != nil {
			return nil, err
		}
	}
	before, _, err := g.policyDocument(ctx, master)
	if err != nil {
		return nil, err
	}
	if syncNativeGroup(before, config.ID) != nil {
		return nil, multisiteInvalid(errors.New("RGW sync group ID already exists; refusing adoption"))
	}
	if _, err := g.command(ctx, master, "sync", "group", "create", "--group-id", config.ID, "--status", string(config.Status)); err != nil {
		return g, err
	}
	after, _, err := g.policyDocument(ctx, master)
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

// CreateFlow adds a fresh flow to an owned unchanged group. Native
// directional identity is the exact source/destination pair, not flow-id.
func (g *SyncGroup) CreateFlow(ctx context.Context, config SyncFlowConfig) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		args, flow, key, err := g.flowConfig(config)
		if err != nil {
			return nil, nil, err
		}
		if syncFlowPresent(before, key, flow) {
			return nil, nil, multisiteInvalid(errors.New("RGW sync flow already exists"))
		}
		return append([]string{"sync", "group", "flow", "create", "--group-id", g.ID()}, args...), func(after map[string]any) error {
			if !syncFlowMatches(after, key, flow) || !reflect.DeepEqual(before, syncGroupWithoutFlow(after, key, flow)) {
				return errors.New("RGW sync flow readback or unrelated group policy differs")
			}
			return nil
		}, nil
	})
}

// RemoveFlow removes only a flow present in the owned group's exact native
// snapshot. Symmetrical removal deletes the entire named set; directional
// removal selects the native pair. It never changes another group's policy.
func (g *SyncGroup) RemoveFlow(ctx context.Context, config SyncFlowConfig) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		args, flow, key, err := g.flowConfig(config)
		if err != nil {
			return nil, nil, err
		}
		if !syncFlowPresent(before, key, flow) {
			return nil, nil, multisiteInvalid(errors.New("RGW sync flow is absent from the owned group"))
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

// CreatePipe adds a fresh identity-checked pipe. Zonegroup policy
// forms an upper bound for a bucket policy; a stored pipe alone does not prove
// effective replication. Verify positive/negative writes through the S3 client.
func (g *SyncGroup) CreatePipe(ctx context.Context, config SyncPipeConfig) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		if !validRGWSyncID(config.ID) || strings.IndexFunc(config.Prefix, unicode.IsControl) != -1 {
			return nil, nil, multisiteInvalid(errors.New("invalid RGW sync pipe ID or prefix"))
		}
		source, err := g.zoneIDs(config.SourceZones, true)
		if err != nil {
			return nil, nil, err
		}
		dest, err := g.zoneIDs(config.DestinationZones, true)
		if err != nil {
			return nil, nil, err
		}
		if syncNativePipe(before, config.ID) != nil {
			return nil, nil, multisiteInvalid(errors.New("RGW sync pipe ID already exists"))
		}
		args, expected, err := g.pipeConfig(opCtx, master, config, source, dest)
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

// SetPipePrefix changes only the selected owned pipe's source prefix.
// Empty prefix removes the filter. It affects future replication selection;
// historical backfill and deletion of previously copied objects are not promised.
// For a bucket-scoped group, wait for WaitBucketPolicyReady at the intended
// destination before writing objects that depend on the new filter. This method
// confirms the master's stored policy, not its asynchronous import into peers.
func (g *SyncGroup) SetPipePrefix(ctx context.Context, id, prefix string) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	if !validRGWSyncID(id) || strings.IndexFunc(prefix, unicode.IsControl) != -1 {
		return multisiteInvalid(errors.New("invalid RGW sync pipe ID or prefix"))
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		want := syncJSONClone(before)
		pipe := syncNativePipe(want, id)
		if pipe == nil {
			return nil, nil, multisiteInvalid(errors.New("RGW sync pipe is absent from owned group"))
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

// RemovePipe removes one pipe from an owned group. The pipe must exist in
// the group's native policy, and the removal must leave the rest of the group
// unchanged. Objects that were already replicated stay. Like the other policy
// changes, it confirms the master's stored policy, not its asynchronous
// import into peers.
func (g *SyncGroup) RemovePipe(ctx context.Context, id string) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	if !validRGWSyncID(id) {
		return errors.New("invalid RGW sync pipe ID")
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
		if syncNativePipe(before, id) == nil {
			return nil, nil, multisiteInvalid(errors.New("RGW sync pipe is absent from owned group"))
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

// SetStatus changes an owned group's allowed/enabled/forbidden state.
func (g *SyncGroup) SetStatus(ctx context.Context, status SyncGroupStatus) error {
	_, err := g.fixture()
	if err != nil {
		return err
	}
	if !validRGWSyncStatus(status) {
		return errors.New("invalid RGW sync group status")
	}
	return g.change(ctx, func(opCtx context.Context, master *rgwZoneState, before map[string]any) ([]string, func(map[string]any) error, error) {
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

// Remove removes only this confirmed group's unchanged native policy.
// Bucket/object data remains. Zonegroup removal needs Apply afterwards
// to publish it; removal is idempotent across handle copies.
func (g *SyncGroup) Remove(ctx context.Context) error {
	f, err := g.fixture()
	if err != nil {
		return err
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return err
	}
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
	if err := g.finishPending(ctx, master); err != nil {
		return err
	}
	before, _, err := g.policyDocument(ctx, master)
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
		return multisiteInvalid(errors.New("RGW owned sync group was externally changed; refusing removal"))
	}
	if _, err := g.command(ctx, master, "sync", "group", "remove", "--group-id", g.ID()); err != nil {
		return err
	}
	after, _, err := g.policyDocument(ctx, master)
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

func (f *Fixture) validateSyncGroup(g *SyncGroup, allowRemoved bool) error {
	if f.closed {
		return multisiteInvalid(errors.New("RGW multisite is terminated"))
	}
	if g == nil || g.owner != f || g.state == nil || !g.state.confirmed || (!allowRemoved && g.state.removed) {
		return multisiteInvalid(errors.New("RGW sync group must be active and confirmed as created by this fixture"))
	}
	if g.realmID != f.RealmID || g.groupID != f.topology.groupIDs[g.scope.Zonegroup] {
		return errors.New("RGW sync group realm/zonegroup identity changed")
	}
	return nil
}

type rgwSyncMutation func(context.Context, *rgwZoneState, map[string]any) ([]string, func(map[string]any) error, error)

func (g *SyncGroup) change(ctx context.Context, change rgwSyncMutation) error {
	f := g.owner
	if f == nil {
		return multisiteInvalid(errors.New("RGW multisite fixture is required"))
	}
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return err
	}
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
		completed, err := g.reconcile(ctx, master)
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
			return g.executePending(ctx, master)
		}
	}
	before, _, err := g.policyDocument(ctx, master)
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
	return g.executePending(ctx, master)
}

func (g *SyncGroup) executePending(ctx context.Context, master *rgwZoneState) error {
	_, commandErr := g.command(ctx, master, g.state.pending.args...)
	readCtx := ctx
	cancel := func() {}
	if commandErr != nil || ctx.Err() != nil {
		readCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	}
	completed, readErr := g.reconcile(readCtx, master)
	cancel()
	if completed && readErr == nil {
		return nil
	}
	if readErr != nil && commandErr == nil {
		checkCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		completed, readErr = g.reconcile(checkCtx, master)
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

func (g *SyncGroup) reconcile(ctx context.Context, master *rgwZoneState) (bool, error) {
	pending := g.state.pending
	if pending == nil {
		return true, nil
	}
	actual, _, err := g.policyDocument(ctx, master)
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

func (g *SyncGroup) finishPending(ctx context.Context, master *rgwZoneState) error {
	completed, err := g.reconcile(ctx, master)
	if err != nil {
		return err
	}
	if !completed {
		return errors.New("RGW sync group has an unconfirmed pending mutation; retry the same request")
	}
	return nil
}

func (f *Fixture) syncPolicyMaster(ctx context.Context) (*rgwZoneState, map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if f.closed {
		return nil, nil, multisiteInvalid(errors.New("RGW multisite is terminated"))
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

func (f *Fixture) syncPolicyAdmin(ctx context.Context, zone *rgwZoneState, group string, args ...string) ([]byte, error) {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--realm-id", f.RealmID, "--zonegroup-id", f.topology.groupIDs[group], "--zone-id", zone.ID, "--format", "json"}
	status := (len(args) >= 2 && args[0] == "sync" && args[1] == "status") || (len(args) >= 3 && args[0] == "bucket" && args[1] == "sync" && args[2] == "status")
	data, err := syncNativeExecMode(ctx, zone.client, status, append(argv, args...)...)
	return data, err
}

func syncNativeExecMode(ctx context.Context, ctr testcontainers.Container, status bool, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctr == nil {
		return nil, multisiteInvalid(errors.New("RGW native CLI container is unavailable"))
	}
	code, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, clusterutil.Quiet(errors.New("RGW native command failed; output is redacted"), servicehook.ClassifyExec(ctx, err))
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, clusterutil.Quiet(errors.New("RGW native command stream failed; output is redacted"), servicehook.ClassifyExec(ctx, err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, clusterutil.Quiet(fmt.Errorf("RGW native command exited %d; output is redacted", code),
			servicehook.NativeFailure(args[0], code, nil, stderr.Bytes()))
	}
	if status {
		warnings := strings.ToLower(stderr.String())
		if strings.Contains(warnings, "error") || strings.Contains(warnings, "failed") {
			return nil, errors.New("RGW native sync comparison reported an error; output is redacted")
		}
	}
	return stdout.Bytes(), nil
}

func (g *SyncGroup) command(ctx context.Context, master *rgwZoneState, args ...string) ([]byte, error) {
	f := g.owner
	if g.scope.Bucket != "" {
		args = append(args, "--bucket", syncScopedBucketName(g.scope), "--bucket-id", g.bucketID)
	}
	return f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, args...)
}

func (g *SyncGroup) bucketInstanceID(ctx context.Context, master *rgwZoneState) (string, error) {
	f := g.owner
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
func syncScopedBucketName(scope SyncPolicyScope) string {
	if scope.Tenant != "" {
		return scope.Tenant + "/" + scope.Bucket
	}
	return scope.Bucket
}

// policyDocument reads the sync policy of g's zonegroup or, for a
// bucket-scoped group, of its exact bucket instance, which it also returns.
func (g *SyncGroup) policyDocument(ctx context.Context, master *rgwZoneState) (map[string]any, *rgwBucketInstance, error) {
	f := g.owner
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
		return policy, nil, err
	}
	id, err := g.bucketInstanceID(ctx, master)
	if err != nil {
		return nil, nil, err
	}
	if id != g.bucketID {
		return nil, nil, errors.New("RGW bucket name was recreated; refusing sync policy operation")
	}
	key := syncScopedBucketName(g.scope) + ":" + g.bucketID
	data, err = f.syncPolicyAdmin(ctx, master, g.scope.Zonegroup, "metadata", "get", "bucket.instance:"+key)
	if err != nil {
		return nil, nil, err
	}
	instance, matched, err := decodeBucketInstance(data, key, g)
	if err != nil {
		return nil, nil, err
	}
	if !matched {
		return nil, nil, errors.New("RGW bucket metadata instance identity differs")
	}
	return instance.policy, instance, nil
}

// rgwBucketInstance is a native `metadata get bucket.instance:<key>`
// document whose identity matched. Owner and PlacementRule are read from its
// bucket_info; policy is that bucket_info's sync policy, kept untyped because
// it is compared with, and written back as, the exact native document.
type rgwBucketInstance struct {
	Owner, PlacementRule nativeString
	policy               map[string]any
}

// decodeBucketInstance decodes the instance document of key. matched is false
// unless the document is that key's, of g's bucket name, tenant and instance
// ID, in g's zonegroup.
func decodeBucketInstance(data []byte, key string, g *SyncGroup) (*rgwBucketInstance, bool, error) {
	metadata, err := syncJSON(data)
	if err != nil {
		return nil, false, err
	}
	// Identity members are read from the decoded document by their exact
	// native key: a typed decode would match keys case-insensitively and keep
	// an earlier value of a repeated key.
	info := nativeDoc(metadata).object("data").object("bucket_info")
	bucket := info.object("bucket")
	if info == nil || !nativeDoc(metadata).member("key").is("bucket.instance:"+key) || !bucket.member("name").is(g.scope.Bucket) ||
		!bucket.member("tenant").is(g.scope.Tenant) || !bucket.member("bucket_id").is(g.bucketID) || !info.member("zonegroup").is(g.groupID) {
		return nil, false, nil
	}
	policy, err := syncPolicyFromDocument(info)
	if err != nil {
		return nil, true, err
	}
	return &rgwBucketInstance{Owner: info.member("owner"), PlacementRule: info.member("placement_rule"), policy: policy}, true, nil
}

func (g *SyncGroup) zoneIDs(names []string, wildcard bool) ([]string, error) {
	f := g.owner
	if len(names) == 0 {
		return nil, multisiteInvalid(errors.New("RGW sync zone list must not be empty"))
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
			return nil, multisiteInvalid(errors.New("RGW sync zones must be unique attached owned zones in this zonegroup"))
		}
		ids = append(ids, found.ID)
	}
	slices.Sort(ids)
	return ids, nil
}

func (g *SyncGroup) flowConfig(c SyncFlowConfig) ([]string, map[string]any, string, error) {
	f := g.owner
	if len(c.Zones) > 0 {
		if !validRGWSyncID(c.ID) || c.SourceZone != "" || c.DestinationZone != "" || len(c.Zones) < 2 {
			return nil, nil, "", multisiteInvalid(errors.New("symmetrical RGW flow requires its ID and at least two zones"))
		}
		ids, err := g.zoneIDs(c.Zones, false)
		if err != nil {
			return nil, nil, "", err
		}
		return []string{"--flow-type", "symmetrical", "--flow-id", c.ID, "--zone-ids", strings.Join(ids, ",")}, map[string]any{"id": c.ID, "zones": syncStrings(ids)}, "symmetrical", nil
	}
	if c.ID != "" || c.SourceZone == "" || c.SourceZone == c.DestinationZone {
		return nil, nil, "", multisiteInvalid(errors.New("directional RGW flow requires distinct owned source/destination zones and no ID"))
	}
	if _, err := g.zoneIDs([]string{c.SourceZone, c.DestinationZone}, false); err != nil {
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
	return nativeDoc(policy).identified("groups", id)
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
	return nativeDoc(g).identified("pipes", id)
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

// fixture returns the fixture that created g, refusing a nil or unowned handle.
func (g *SyncGroup) fixture() (*Fixture, error) {
	if g == nil || g.owner == nil {
		return nil, multisiteInvalid(errors.New("RGW sync group must be active and confirmed as created by this fixture"))
	}
	return g.owner, nil
}
