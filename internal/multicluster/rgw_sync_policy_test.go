package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwSyncCLI struct {
	testcontainers.Container
	group, current, staging                          map[string]any
	bucketID                                         string
	bucketTenant                                     string
	bucketPolicy                                     map[string]any
	calls                                            [][]string
	stagingExit                                      int
	badSymmetry                                      bool
	metadataOutput, humanOutput                      string
	additionalBuckets                                map[string]string
	lostReply, failBeforeMutation                    bool
	readFailuresAfterMutation, remainingReadFailures int
	stderrOutput                                     string
	bucketStatusOutput                               string
	periodByZone                                     map[string]map[string]any
	cancelOnBucketStatus                             context.CancelFunc
}

func syncTestFlag(args []string, name string) string {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}
func syncTestCommand(args []string, words ...string) bool {
	for i := range args {
		if i+len(words) <= len(args) && slices.Equal(args[i:i+len(words)], words) {
			return true
		}
	}
	return false
}
func (c *rgwSyncCLI) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls = append(c.calls, slices.Clone(args))
	if syncTestFlag(args, "--realm-id") != "realm" || syncTestFlag(args, "--zonegroup-id") != "group-id" || !slices.Contains([]string{"source-id", "dest-id"}, syncTestFlag(args, "--zone-id")) {
		return 0, nil, errors.New("secret native scope error")
	}
	var result any
	var raw string
	code := 0
	switch {
	case syncTestCommand(args, "bucket", "sync", "status"):
		raw = c.bucketStatusOutput
		if c.cancelOnBucketStatus != nil {
			c.cancelOnBucketStatus()
		}
	case syncTestCommand(args, "metadata", "sync", "status"):
		raw = c.metadataOutput
	case syncTestCommand(args, "sync", "status"):
		raw = c.humanOutput
	case syncTestCommand(args, "period", "get"):
		if syncTestFlag(args, "--period") != "" {
			if syncTestFlag(args, "--period") != "realm:staging" || syncTestFlag(args, "--epoch") != "1" {
				return 0, nil, errors.New("staging wrong ID")
			}
			code = c.stagingExit
			if c.staging == nil && code == 0 {
				code = 2
			}
			if code == 0 {
				result = c.staging
			}
		} else {
			result = c.current
			if local := c.periodByZone[syncTestFlag(args, "--zone-id")]; local != nil {
				result = local
			}
		}
	case syncTestCommand(args, "period", "update"):
		c.staging = syncJSONClone(c.current)
		c.staging["id"] = "realm:staging"
		epoch, _ := c.current["realm_epoch"].(json.Number).Int64()
		c.staging["realm_epoch"] = json.Number(fmt.Sprint(epoch + 1))
		c.staging["predecessor_uuid"] = c.current["id"]
		c.staging["period_map"].(map[string]any)["zonegroups"] = []any{syncJSONClone(c.group)}
		result = c.staging
	case syncTestCommand(args, "period", "commit"):
		previousRealmEpoch := c.current["realm_epoch"]
		c.current = syncJSONClone(c.staging)
		c.current["id"] = "period-committed"
		c.current["epoch"] = json.Number("2")
		c.current["realm_epoch"] = previousRealmEpoch
		result = c.current
	case syncTestCommand(args, "realm", "pull"):
		result = map[string]any{"id": "realm"}
	case syncTestCommand(args, "zonegroup", "get"):
		if c.remainingReadFailures > 0 {
			c.remainingReadFailures--
			return 0, nil, errors.New("readback lost secret response")
		}
		result = c.group
	case syncTestCommand(args, "bucket", "stats"):
		key := syncTestFlag(args, "--bucket")
		name, tenant := key, ""
		if components := strings.SplitN(key, "/", 2); len(components) == 2 {
			tenant, name = components[0], components[1]
		}
		id := c.bucketID
		if name != "bucket" || tenant != c.bucketTenant {
			id = c.additionalBuckets[key]
		}
		if syncTestFlag(args, "--tenant") != "" {
			return 1, bytes.NewReader(nil), nil // Native global --tenant requires a UID.
		}
		result = map[string]any{"id": id, "bucket": name, "tenant": tenant, "zonegroup": "group-id"}
	case syncTestCommand(args, "metadata", "get"):
		key := args[len(args)-1]
		components := strings.Split(strings.TrimPrefix(key, "bucket.instance:"), ":")
		name, tenant := components[0], ""
		if names := strings.SplitN(name, "/", 2); len(names) == 2 {
			tenant, name = names[0], names[1]
		}
		result = map[string]any{"key": key, "data": map[string]any{"bucket_info": map[string]any{"bucket": map[string]any{"name": name, "tenant": tenant, "bucket_id": components[1]}, "zonegroup": "group-id", "sync_policy": c.bucketPolicy, "owner": "legacy-owner", "placement_rule": "tiered"}}}
	case syncTestCommand(args, "sync", "group"):
		if c.failBeforeMutation {
			return 0, nil, errors.New("native command did not start; secret")
		}
		policy, _ := syncPolicyFromDocument(c.group)
		if syncTestFlag(args, "--bucket") != "" {
			scope := RGWSyncPolicyScope{Bucket: "bucket", Tenant: c.bucketTenant}
			if syncTestFlag(args, "--bucket") != syncScopedBucketName(scope) || syncTestFlag(args, "--bucket-id") != c.bucketID || syncTestFlag(args, "--tenant") != "" {
				return 0, nil, errors.New("bucket wrong ID")
			}
			policy = c.bucketPolicy
		}
		id := syncTestFlag(args, "--group-id")
		group := syncNativeGroup(policy, id)
		switch {
		case syncTestCommand(args, "sync", "group", "create"):
			group = map[string]any{"id": id, "status": syncTestFlag(args, "--status"), "data_flow": map[string]any{}, "pipes": []any{}}
		case syncTestCommand(args, "sync", "group", "modify"):
			group["status"] = syncTestFlag(args, "--status")
		case syncTestCommand(args, "sync", "group", "remove"):
			group = nil
		case syncTestCommand(args, "sync", "group", "flow"):
			key := syncTestFlag(args, "--flow-type")
			flow := map[string]any{"source_zone": syncTestFlag(args, "--source-zone-id"), "dest_zone": syncTestFlag(args, "--dest-zone-id")}
			if key == "symmetrical" {
				flow = map[string]any{"id": syncTestFlag(args, "--flow-id"), "zones": syncStrings(strings.Split(syncTestFlag(args, "--zone-ids"), ","))}
			}
			if syncTestCommand(args, "flow", "remove") {
				group = syncGroupWithoutFlow(group, key, flow)
			} else {
				data := group["data_flow"].(map[string]any)
				flows, _ := data[key].([]any)
				if c.badSymmetry && key == "symmetrical" {
					flow["zones"] = []any{"source-id"}
				}
				data[key] = append(flows, flow)
			}
		case syncTestCommand(args, "sync", "group", "pipe"):
			pipeID := syncTestFlag(args, "--pipe-id")
			if syncTestCommand(args, "pipe", "create") {
				filter := map[string]any{"tags": []any{}}
				if prefix := syncTestFlag(args, "--prefix"); prefix != "" {
					filter["prefix"] = prefix
				}
				if tags := syncTestFlag(args, "--tags-add"); tags != "" {
					var nativeTags []any
					for _, pair := range strings.Split(tags, ",") {
						parts := strings.SplitN(pair, "=", 2)
						nativeTags = append(nativeTags, map[string]any{"key": parts[0], "value": parts[1]})
					}
					filter["tags"] = nativeTags
				}
				pipe := map[string]any{"id": pipeID, "source": map[string]any{"bucket": "*", "zones": syncStrings(strings.Split(syncTestFlag(args, "--source-zone-ids"), ","))}, "dest": map[string]any{"bucket": "*", "zones": syncStrings(strings.Split(syncTestFlag(args, "--dest-zone-ids"), ","))}, "params": map[string]any{"source": map[string]any{"filter": filter}, "dest": map[string]any{}, "priority": json.Number("0"), "mode": "system"}}
				for _, side := range []struct{ flag, key string }{{"--source", "source"}, {"--dest", "dest"}} {
					if name := syncTestFlag(args, side.flag+"-bucket"); name != "" {
						if tenant := syncTestFlag(args, side.flag+"-tenant"); tenant != "" {
							name = tenant + "/" + name
						}
						pipe[side.key].(map[string]any)["bucket"] = name + ":" + syncTestFlag(args, side.flag+"-bucket-id")
					}
				}
				if priority := syncTestFlag(args, "--priority"); priority != "" {
					pipe["params"].(map[string]any)["priority"] = json.Number(priority)
				}
				group["pipes"] = append(group["pipes"].([]any), pipe)
			} else if syncTestCommand(args, "pipe", "modify") {
				filter := syncNativePipe(group, pipeID)["params"].(map[string]any)["source"].(map[string]any)["filter"].(map[string]any)
				if slices.Contains(args, "--prefix-rm") {
					delete(filter, "prefix")
				} else {
					filter["prefix"] = syncTestFlag(args, "--prefix")
				}
			} else {
				group = syncGroupWithoutPipe(group, pipeID)
			}
		}
		policy = syncPolicyReplace(policy, id, group)
		if syncTestFlag(args, "--bucket") != "" {
			c.bucketPolicy = policy
		} else {
			c.group["sync_policy"] = policy
		}
		result = policy
		c.remainingReadFailures = c.readFailuresAfterMutation
		c.readFailuresAfterMutation = 0
		if c.lostReply {
			c.lostReply = false
			return 0, nil, errors.New("lost native mutation response; secret")
		}
	default:
		return 0, nil, errors.New("unrecognized secret command")
	}
	var stream bytes.Buffer
	if result != nil || raw != "" {
		data := []byte(raw)
		if result != nil {
			data, _ = json.Marshal(result)
		}
		header := [8]byte{byte(stdcopy.Stdout)}
		binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
		stream.Write(header[:])
		stream.Write(data)
	}
	if c.stderrOutput != "" && (syncTestCommand(args, "sync", "status") || syncTestCommand(args, "bucket", "sync", "status")) {
		header := [8]byte{byte(stdcopy.Stderr)}
		binary.BigEndian.PutUint32(header[4:], uint32(len(c.stderrOutput)))
		stream.Write(header[:])
		stream.WriteString(c.stderrOutput)
	}
	return code, bytes.NewReader(stream.Bytes()), nil
}

func newRGWSyncTestFixture() (*RGWMultisite, *rgwSyncCLI, *rgwLifecycleContainer, *rgwLifecycleContainer) {
	group := map[string]any{"id": "group-id", "name": "group", "realm_id": "realm", "master_zone": "source-id", "is_master": true, "endpoints": []any{"http://source:7480"}, "zones": []any{map[string]any{"id": "source-id", "name": "source", "endpoints": []any{"http://source:7480"}}, map[string]any{"id": "dest-id", "name": "destination", "endpoints": []any{"http://destination:7480"}}}, "sync_policy": map[string]any{"groups": []any{}}}
	period := map[string]any{"id": "period-original", "epoch": json.Number("1"), "realm_epoch": json.Number("1"), "realm_id": "realm", "master_zone": "source-id", "master_zonegroup": "group-id", "period_map": map[string]any{"id": "period-original", "zonegroups": []any{syncJSONClone(group)}}, "period_config": map[string]any{}}
	cli := &rgwSyncCLI{group: group, current: period, bucketID: "bucket-original", bucketPolicy: map[string]any{"groups": []any{}}}
	source, dest := &rgwLifecycleContainer{running: true}, &rgwLifecycleContainer{running: true}
	f := &RGWMultisite{RealmID: "realm", SourceZoneID: "source-id", DestinationZoneID: "dest-id", config: RGWMultisiteConfig{Zonegroup: "group", SourceZone: "source", DestinationZone: "destination"}, sourceURL: "http://source:7480", destinationURL: "http://destination:7480", sourceClient: cli, destinationClient: cli, groupIDs: map[string]string{"group": "group-id"}, Source: &ceph.RGWContainer{Container: source}, Destination: &ceph.RGWContainer{Container: dest}}
	return f, cli, source, dest
}

func TestRGWOwnedSyncPolicyLifecycle(t *testing.T) {
	f, cli, a, b := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "selective", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	dir := RGWSyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}
	if err := f.CreateSyncFlow(t.Context(), g, dir); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncFlow(t.Context(), g, dir); err == nil {
		t.Fatal("adopted duplicate directional native pair")
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "permit", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := f.ApplySyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	if a.starts != 1 || b.starts != 1 || cli.current["id"] != "period-committed" {
		t.Fatal("publication did not reload owned running gateways")
	}
	if err := f.RemoveSyncPipe(t.Context(), g, "permit"); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveSyncFlow(t.Context(), g, dir); err != nil {
		t.Fatal(err)
	}
	if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncForbidden); err != nil {
		t.Fatal(err)
	}
	copy := *g
	if err := f.RemoveSyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	before := len(cli.calls)
	if err := f.RemoveSyncGroup(t.Context(), &copy); err != nil || len(cli.calls) != before {
		t.Fatal("removal across handle copies was not idempotent")
	}
	if err := f.ApplySyncGroup(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	if syncNativeGroup(syncPeriodGroups(cli.current)[0]["sync_policy"].(map[string]any), g.ID()) != nil {
		t.Fatal("removed group survived committed period")
	}
}

func TestRGWBucketSyncPolicyUsesInstanceAndDynamicUpdates(t *testing.T) {
	f, cli, a, b := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{Bucket: "bucket"}, RGWSyncGroupConfig{ID: "selected", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "prefix", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "published/"}); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"reports/", ""} {
		if err := f.SetSyncPipePrefix(t.Context(), g, "prefix", prefix); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.ApplySyncGroup(t.Context(), g); err != nil || a.starts != 0 || b.starts != 0 {
		t.Fatal("bucket activation committed or restarted gateways")
	}
	cli.bucketID = "bucket-recreated"
	before := syncJSONClone(cli.bucketPolicy)
	if err := f.RemoveSyncGroup(t.Context(), g); err == nil || !reflect.DeepEqual(cli.bucketPolicy, before) {
		t.Fatal("recreated bucket name was changed")
	}
}

func TestRGWSyncPolicyRefusesForeignPendingStagingAndGroupChanges(t *testing.T) {
	for _, kind := range []string{"staging", "stored", "committed", "owned-group", "foreign-owner"} {
		t.Run(kind, func(t *testing.T) {
			f, cli, a, b := newRGWSyncTestFixture()
			g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "owned", Status: RGWSyncAllowed})
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "staging":
				cli.staging = syncJSONClone(cli.current)
				cli.staging["id"] = "realm:staging"
				cli.staging["period_config"].(map[string]any)["foreign"] = "pending"
			case "stored":
				cli.group["api_name"] = "externally-changed"
			case "committed":
				cli.current["period_config"].(map[string]any)["foreign"] = "committed"
			case "owned-group":
				syncNativeGroup(cli.group["sync_policy"].(map[string]any), g.ID())["status"] = "enabled"
			case "foreign-owner":
				g.owner = &RGWMultisite{}
			}
			beforeStage, beforeGroup := syncJSONClone(cli.staging), syncJSONClone(cli.group)
			if err := f.ApplySyncGroup(t.Context(), g); err == nil {
				t.Fatal("foreign policy publication accepted")
			}
			if !reflect.DeepEqual(beforeStage, cli.staging) || !reflect.DeepEqual(beforeGroup, cli.group) || a.stops != 0 || b.stops != 0 {
				t.Fatal("refused publication changed policy or gateways")
			}
			for _, args := range cli.calls {
				if syncTestCommand(args, "period", "update") || syncTestCommand(args, "period", "commit") {
					t.Fatal("period mutation preceded preflight refusal")
				}
			}
		})
	}
}

func TestRGWSymmetricalReadbackMustMatchExactNativeZones(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "symmetric", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	cli.badSymmetry = true
	if err := f.CreateSyncFlow(t.Context(), g, RGWSyncFlowConfig{ID: "pair", Zones: []string{"source", "destination"}}); err == nil {
		t.Fatal("named flow with incorrect zones was confirmed")
	}
}

func TestRGWSyncPolicyCancelledAndUnownedHandlesDoNotMutate(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.CreateSyncGroup(ctx, RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "owned", Status: RGWSyncAllowed}); !errors.Is(err, context.Canceled) || len(cli.calls) != 0 {
		t.Fatal("cancelled create used native CLI")
	}
	if err := f.RemoveSyncGroup(t.Context(), &RGWSyncGroup{}); err == nil || len(cli.calls) != 0 {
		t.Fatal("foreign group used native CLI")
	}
	for _, args := range []RGWSyncGroupConfig{{ID: "--option", Status: RGWSyncAllowed}, {ID: "valid", Status: "unknown"}} {
		if _, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, args); err == nil {
			t.Fatal("invalid policy config accepted")
		}
	}
}

func TestRGWSyncMutationLostReplyConfirmsExactNativeIntent(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "lost-reply", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	cli.lostReply = true
	if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncEnabled); err != nil || g.state.pending != nil || g.state.group["status"] != "enabled" {
		t.Fatalf("lost reply applied mutation was not reconciled: %v", err)
	}
	if err := f.RemoveSyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
}

func TestRGWSyncStatusAndPrefixAssignmentsAreIdempotentNoOps(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "idempotent", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	before := len(cli.calls)
	if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncAllowed); err != nil || g.state.pending != nil {
		t.Fatal("unchanged group status left unresolved intent")
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "group", "modify") {
			t.Fatal("no-op status issued native mutation")
		}
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "same-prefix", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "selected/"}); err != nil {
		t.Fatal(err)
	}
	if err := f.SetSyncPipePrefix(t.Context(), g, "same-prefix", "selected/"); err != nil || g.state.pending != nil {
		t.Fatal("unchanged prefix left unresolved intent")
	}
	if err := f.SetSyncPipePrefix(t.Context(), g, "same-prefix", ""); err != nil {
		t.Fatal(err)
	}
	if err := f.SetSyncPipePrefix(t.Context(), g, "same-prefix", ""); err != nil || g.state.pending != nil {
		t.Fatal("empty-on-empty prefix left unresolved intent")
	}
	if err := f.ApplySyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveSyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
}

func TestRGWSyncMutationRetainsPendingIntentAcrossReadbackOutage(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "outage", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	cli.lostReply, cli.readFailuresAfterMutation = true, 1
	config := RGWSyncPipeConfig{ID: "pending", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "selected/"}
	if err := f.CreateSyncPipe(t.Context(), g, config); err == nil || g.state.pending == nil {
		t.Fatal("ambiguous mutation lost its pending intent")
	}
	before := len(cli.calls)
	if err := f.CreateSyncPipe(t.Context(), g, config); err != nil || g.state.pending != nil || syncNativePipe(g.state.group, "pending") == nil {
		t.Fatalf("same request did not recover applied intent: %v", err)
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "pipe", "create") {
			t.Fatal("reconciled retry issued a duplicate native mutation")
		}
	}
	if err := f.RemoveSyncPipe(t.Context(), g, "pending"); err != nil {
		t.Fatal(err)
	}
}

func TestRGWSyncMutationReadbackOutageCanBeReconciledByRemoval(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "readback-outage", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	cli.readFailuresAfterMutation = 2
	if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncEnabled); err == nil || g.state.pending == nil {
		t.Fatal("readback outage did not retain applied intent")
	}
	if err := f.RemoveSyncGroup(t.Context(), g); err != nil || g.state.pending != nil || !g.state.removed {
		t.Fatalf("removal could not reconcile exact applied intent first: %v", err)
	}
}

func TestRGWSyncPendingMutationRefusesForeignDeltasAndBeforeStatePublication(t *testing.T) {
	for _, foreign := range []bool{true, false} {
		t.Run(map[bool]string{true: "foreign-delta", false: "not-applied"}[foreign], func(t *testing.T) {
			f, cli, a, b := newRGWSyncTestFixture()
			g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "guarded", Status: RGWSyncAllowed})
			if err != nil {
				t.Fatal(err)
			}
			if foreign {
				cli.lostReply, cli.readFailuresAfterMutation = true, 1
			} else {
				cli.failBeforeMutation = true
			}
			if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncEnabled); err == nil || g.state.pending == nil {
				t.Fatal("failure did not retain pending intent")
			}
			if foreign {
				syncNativeGroup(cli.group["sync_policy"].(map[string]any), g.ID())["foreign"] = "edited"
			}
			before := syncJSONClone(cli.group)
			if err := f.ApplySyncGroup(t.Context(), g); err == nil || a.stops != 0 || b.stops != 0 {
				t.Fatal("unresolved/foreign pending mutation was published")
			}
			if err := f.RemoveSyncGroup(t.Context(), g); err == nil || !reflect.DeepEqual(before, cli.group) {
				t.Fatal("unresolved/foreign pending mutation was removed")
			}
			if foreign {
				if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncEnabled); err == nil {
					t.Fatal("foreign delta adopted by retry")
				}
			} else {
				cli.failBeforeMutation = false
				if err := f.SetSyncGroupStatus(t.Context(), g, RGWSyncEnabled); err != nil || g.state.pending != nil {
					t.Fatal("unchanged before-state did not permit same request retry")
				}
			}
		})
	}
}
