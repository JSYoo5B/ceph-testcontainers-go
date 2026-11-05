package multisite

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestRGWPeriodWithGroupReplacesOnlyTheOwnedGroupPolicy(t *testing.T) {
	previous := map[string]any{"id": "p1", "native_extra": "kept", "period_map": map[string]any{"zonegroups": []any{
		map[string]any{"id": "g1", "sync_policy": map[string]any{"groups": []any{map[string]any{"id": "other", "status": "enabled"}}}, "native": true},
		map[string]any{"id": "g2", "sync_policy": map[string]any{"groups": []any{}}},
	}}}
	group := map[string]any{"id": "owned", "status": "allowed"}
	want, err := syncPeriodWithGroup(previous, "g1", "owned", group)
	if err != nil {
		t.Fatal(err)
	}
	groups := syncPeriodGroups(want)
	policy := groups[0]["sync_policy"].(map[string]any)
	if len(policy["groups"].([]any)) != 2 || syncNativeGroup(policy, "owned")["status"] != "allowed" || groups[0]["native"] != true || want["native_extra"] != "kept" {
		t.Fatalf("owned group was not added beside the existing one: %v", want)
	}
	if len(syncPeriodGroups(previous)[0]["sync_policy"].(map[string]any)["groups"].([]any)) != 1 {
		t.Fatal("the captured period was mutated")
	}
	if !reflect.DeepEqual(groups[1], syncPeriodGroups(previous)[1]) {
		t.Fatal("an unrelated zonegroup changed")
	}
	duplicate := syncJSONClone(previous)
	zonegroups := duplicate["period_map"].(map[string]any)["zonegroups"].([]any)
	duplicate["period_map"].(map[string]any)["zonegroups"] = append(zonegroups, syncJSONClone(zonegroups[0].(map[string]any)))
	for name, tc := range map[string]struct {
		period map[string]any
		group  string
		want   string
	}{
		"duplicate zonegroup": {duplicate, "g1", "duplicate zonegroup"},
		"missing zonegroup":   {previous, "g9", "missing from the captured period"},
		"malformed policy": {map[string]any{"period_map": map[string]any{"zonegroups": []any{map[string]any{"id": "g1", "sync_policy": "x"}}}},
			"g1", "malformed"},
	} {
		if _, err := syncPeriodWithGroup(tc.period, tc.group, "owned", group); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", name, err, tc.want)
		}
	}
}

func TestRGWPeriodPolicyIgnoresOnlyPeriodIdentity(t *testing.T) {
	a := map[string]any{"id": "p1", "epoch": 1.0, "realm_epoch": 2.0, "predecessor_uuid": "p0", "realm_id": "r", "period_map": map[string]any{"id": "p1", "zonegroups": []any{}}}
	b := syncJSONClone(a)
	b["id"], b["epoch"], b["realm_epoch"], b["predecessor_uuid"] = "p2", 5.0, 3.0, "p1"
	b["period_map"].(map[string]any)["id"] = "p2"
	if !syncPeriodPolicyEqual(a, b) {
		t.Fatal("periods that differ only in identity compared unequal")
	}
	b["realm_id"] = "other"
	if syncPeriodPolicyEqual(a, b) {
		t.Fatal("a realm change was ignored")
	}
	if a["id"] != "p1" || a["period_map"].(map[string]any)["id"] != "p1" {
		t.Fatal("the comparison mutated its input")
	}
}

func TestRGWSyncPolicyFromDocumentSeparatesMissingNullAndMalformed(t *testing.T) {
	for name, doc := range map[string]map[string]any{"missing": {}, "null": {"sync_policy": nil}} {
		policy, err := syncPolicyFromDocument(doc)
		if err != nil || !reflect.DeepEqual(policy, map[string]any{"groups": []any{}}) {
			t.Errorf("%s policy: %v %v", name, policy, err)
		}
	}
	for name, doc := range map[string]map[string]any{
		"not an object":   {"sync_policy": "x"},
		"no groups":       {"sync_policy": map[string]any{}},
		"null groups":     {"sync_policy": map[string]any{"groups": nil}},
		"group not named": {"sync_policy": map[string]any{"groups": []any{map[string]any{"status": "enabled"}}}},
		"duplicate group": {"sync_policy": map[string]any{"groups": []any{map[string]any{"id": "a"}, map[string]any{"id": "a"}}}},
	} {
		if _, err := syncPolicyFromDocument(doc); err == nil {
			t.Errorf("%s policy was accepted", name)
		}
	}
	native := map[string]any{"sync_policy": map[string]any{"groups": []any{map[string]any{"id": "a"}}, "native": true}}
	if policy, err := syncPolicyFromDocument(native); err != nil || policy["native"] != true {
		t.Fatalf("an unknown policy member was lost: %v %v", policy, err)
	}
}

func TestRGWSyncFreshGroupRequiresTheOnlyChange(t *testing.T) {
	before := map[string]any{"groups": []any{map[string]any{"id": "other", "status": "enabled"}}, "native": "kept"}
	fresh := map[string]any{"id": "owned", "status": "allowed", "data_flow": map[string]any{}, "pipes": []any{}}
	after := syncPolicyReplace(before, "owned", fresh)
	if group, err := syncFreshGroup(before, after, "owned", SyncAllowed); err != nil || !reflect.DeepEqual(group, fresh) {
		t.Fatalf("fresh group: %v %v", group, err)
	}
	foreign := syncPolicyReplace(after, "other", map[string]any{"id": "other", "status": "forbidden"})
	used := syncPolicyReplace(before, "owned", map[string]any{"id": "owned", "status": "allowed", "data_flow": map[string]any{}, "pipes": []any{map[string]any{"id": "p"}}})
	noFlows := syncPolicyReplace(before, "owned", map[string]any{"id": "owned", "status": "allowed", "pipes": []any{}})
	for name, tc := range map[string]struct {
		after  map[string]any
		status SyncGroupStatus
	}{
		"other status": {after, SyncEnabled}, "unrelated change": {foreign, SyncAllowed},
		"not empty": {used, SyncAllowed}, "missing data_flow": {noFlows, SyncAllowed}, "absent": {before, SyncAllowed},
	} {
		if _, err := syncFreshGroup(before, tc.after, "owned", tc.status); err == nil {
			t.Errorf("%s readback was accepted", name)
		}
	}
}

func TestRGWSyncGroupEditsKeepUnknownMembersAndTheInput(t *testing.T) {
	group := map[string]any{"id": "owned", "status": "allowed", "native": "kept", "pipes": []any{
		map[string]any{"id": "p", "params": map[string]any{"source": map[string]any{"filter": map[string]any{"prefix": "old/", "tags": []any{}}}}},
		map[string]any{"id": "bare", "params": map[string]any{"source": map[string]any{}}},
	}}
	original := syncJSONClone(group)
	want, err := syncGroupWithPipePrefix(group, "p", "new/")
	if err != nil || nativeDoc(syncNativePipe(want, "p")).object("params").object("source").object("filter")["prefix"] != "new/" || want["native"] != "kept" {
		t.Fatalf("prefix edit: %v %v", want, err)
	}
	removed, err := syncGroupWithPipePrefix(group, "p", "")
	filter := nativeDoc(syncNativePipe(removed, "p")).object("params").object("source").object("filter")
	if _, ok := filter["prefix"]; err != nil || ok || filter["tags"] == nil {
		t.Fatalf("prefix removal: %v %v", filter, err)
	}
	if _, err := syncGroupWithPipePrefix(group, "absent", "x"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an absent pipe was not a refused request: %v", err)
	}
	if _, err := syncGroupWithPipePrefix(group, "bare", "x"); err == nil || errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a native pipe without a filter was not a native failure: %v", err)
	}
	if status := syncGroupWithStatus(group, SyncEnabled); status["status"] != "enabled" || status["native"] != "kept" {
		t.Fatalf("status edit: %v", status)
	}
	if !reflect.DeepEqual(group, original) {
		t.Fatal("an edit mutated its input")
	}
}

func TestRGWSyncFlowAndPipeReadbacksRequireExactMembers(t *testing.T) {
	before := map[string]any{"id": "owned", "data_flow": map[string]any{}, "pipes": []any{}}
	flow := map[string]any{"source_zone": "a", "dest_zone": "b"}
	after := syncJSONClone(before)
	after["data_flow"] = map[string]any{"directional": []any{flow}}
	if !syncFlowAdded(before, after, "directional", flow) {
		t.Fatal("the added flow was refused")
	}
	extra := syncJSONClone(after)
	extra["data_flow"].(map[string]any)["directional"] = []any{map[string]any{"source_zone": "a", "dest_zone": "b", "native": true}}
	if syncFlowAdded(before, extra, "directional", flow) {
		t.Fatal("a flow with another member matched")
	}
	pipe := map[string]any{"id": "p", "params": map[string]any{"priority": "1"}}
	added := syncJSONClone(before)
	added["pipes"] = []any{pipe}
	if !syncPipeAdded(before, added, "p", pipe) {
		t.Fatal("the added pipe was refused")
	}
	changed := syncJSONClone(added)
	changed["status"] = "enabled"
	if syncPipeAdded(before, changed, "p", pipe) || syncPipeAdded(before, added, "p", map[string]any{"id": "p"}) {
		t.Fatal("an unrelated change or a different pipe matched")
	}
}

func TestRGWSyncPendingOutcomeSeparatesAppliedUnappliedAndForeign(t *testing.T) {
	before := map[string]any{"groups": []any{map[string]any{"id": "other", "status": "enabled"}, map[string]any{"id": "owned", "status": "allowed"}}}
	enabled := func(group map[string]any) error {
		if group["status"] != "enabled" {
			return errors.New("not enabled")
		}
		return nil
	}
	applied := syncPolicyReplace(before, "owned", map[string]any{"id": "owned", "status": "enabled"})
	if group, err := syncPendingOutcome(applied, before, "owned", enabled); err != nil || group["status"] != "enabled" {
		t.Fatalf("applied: %v %v", group, err)
	}
	if group, err := syncPendingOutcome(syncJSONClone(before), before, "owned", enabled); err != nil || group != nil {
		t.Fatalf("unapplied: %v %v", group, err)
	}
	always := func(map[string]any) error { return nil }
	if group, err := syncPendingOutcome(syncJSONClone(before), before, "owned", always); err != nil || group["status"] != "allowed" {
		t.Fatalf("an intent already true in the unchanged policy: %v %v", group, err)
	}
	foreign := syncPolicyReplace(applied, "other", map[string]any{"id": "other", "status": "forbidden"})
	different := syncPolicyReplace(before, "owned", map[string]any{"id": "owned", "status": "forbidden"})
	removed := syncPolicyReplace(before, "owned", nil)
	for name, actual := range map[string]map[string]any{"foreign": foreign, "different": different, "removed": removed} {
		if group, err := syncPendingOutcome(actual, before, "owned", enabled); err == nil || group != nil {
			t.Errorf("%s readback: %v %v", name, group, err)
		}
	}
}

func TestRGWSyncPeriodPolicyOwnedRefusesAThirdPolicy(t *testing.T) {
	previous := map[string]any{"id": "p1", "period_map": map[string]any{"zonegroups": []any{map[string]any{"id": "g1"}}}}
	want, err := syncPeriodWithGroup(previous, "g1", "owned", map[string]any{"id": "owned"})
	if err != nil {
		t.Fatal(err)
	}
	republished := syncJSONClone(want)
	republished["id"] = "p2"
	third, err := syncPeriodWithGroup(previous, "g1", "foreign", map[string]any{"id": "foreign"})
	if err != nil {
		t.Fatal(err)
	}
	if !syncPeriodPolicyOwned(previous, previous, want) || !syncPeriodPolicyOwned(republished, previous, want) || syncPeriodPolicyOwned(third, previous, want) {
		t.Fatal("the owned period policy rule accepted a third policy or refused an owned one")
	}
}
