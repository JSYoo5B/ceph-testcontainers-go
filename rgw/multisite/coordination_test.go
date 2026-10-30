package multisite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestRGWNativeDocReadsOnlyExactMembersOfTheRightType(t *testing.T) {
	doc := nativeDoc{"id": "g1", "count": 3.0, "list": []any{"a", "b"}, "mixed": []any{"a", 1.0}, "none": nil,
		"items": []any{map[string]any{"id": "x"}, "skip", map[string]any{"id": "y", "v": 1.0}}}
	if !doc.member("id").is("g1") || doc.member("ID").set || doc.member("count").set {
		t.Fatal("member matched a case alias or a non-string")
	}
	if v, ok := doc.text("none"); !ok || v != "" {
		t.Fatal("a null optional string was refused")
	}
	if _, ok := doc.text("count"); ok {
		t.Fatal("a numeric member was read as an optional string")
	}
	if v, ok := doc.texts("list"); !ok || !reflect.DeepEqual(v, []string{"a", "b"}) {
		t.Fatalf("string list: %v %v", v, ok)
	}
	if _, ok := doc.texts("mixed"); ok {
		t.Fatal("a list with a non-string element was accepted")
	}
	if v, ok := doc.texts("absent"); !ok || v != nil {
		t.Fatal("an absent optional list was refused")
	}
	if objects := doc.objects("items"); len(objects) != 2 || !objects[1].member("id").is("y") {
		t.Fatalf("objects kept a non-object or lost order: %v", objects)
	}
	if doc.identified("items", "y")["v"] != 1.0 || doc.identified("items", "z") != nil || doc.object("id") != nil {
		t.Fatal("identified or object returned the wrong member")
	}
}

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

func TestRGWStagingPeriodReadDistinguishesMissingFromFailure(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	master := f.zoneStates()[0]
	if period, err := f.syncStagingPeriod(t.Context(), master); err != nil || period != nil {
		t.Fatalf("missing staging object: %v, %v", period, err)
	}
	cli.stagingExit = 1
	if _, err := f.syncStagingPeriod(t.Context(), master); err == nil || !strings.Contains(err.Error(), "redacted") {
		t.Fatalf("a failed staging read was treated as missing: %v", err)
	}
	cli.stagingExit = 0
	cli.staging = map[string]any{"id": "realm:staging", "realm_id": "foreign"}
	if _, err := f.syncStagingPeriod(t.Context(), master); err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("a foreign staging period was accepted: %v", err)
	}
	cli.staging = map[string]any{"id": "realm:staging", "realm_id": "realm", "native": "kept"}
	if period, err := f.syncStagingPeriod(t.Context(), master); err != nil || period["native"] != "kept" {
		t.Fatalf("staging period: %v, %v", period, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := len(cli.calls)
	if _, err := f.syncStagingPeriod(ctx, master); !errors.Is(err, context.Canceled) || len(cli.calls) != before {
		t.Fatalf("a cancelled read reached the CLI: %v", err)
	}
}

func TestRGWPolicyMasterRefusesUnownedOrUnverifiedTopology(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	master, period, err := f.syncPolicyMaster(t.Context())
	if err != nil || master.Name != "source" || period["id"] != "period-original" {
		t.Fatalf("owned master: %v, %v, %v", master, period, err)
	}
	cases := map[string]struct {
		change func(*Fixture)
		want   string
	}{
		"closed":         {func(f *Fixture) { f.closed = true }, "terminated"},
		"no client":      {func(f *Fixture) { f.realm.sourceClient = nil }, "no initialized owned zones"},
		"foreign master": {func(*Fixture) { cli.current["master_zone"] = "foreign-id" }, "not attached and owned"},
		"foreign realm":  {func(*Fixture) { cli.current["master_zone"], cli.current["realm_id"] = "source-id", "other" }, "realm/master identity mismatch"},
	}
	for name, tc := range cases {
		f, cli, _, _ = newRGWSyncTestFixture()
		tc.change(f)
		_, _, err := f.syncPolicyMaster(t.Context())
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", name, err, tc.want)
		}
		if name == "closed" && !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("closed: %v is not an invalid request", err)
		}
	}
}

func TestRGWPublishSyncPeriodRefusesUnrelatedStagingBeforeUpdate(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	master := f.zoneStates()[0]
	current := syncJSONClone(cli.current)
	want, err := syncPeriodWithGroup(current, "group-id", "owned", map[string]any{"id": "owned", "status": "enabled"})
	if err != nil {
		t.Fatal(err)
	}
	cli.staging = syncJSONClone(current)
	cli.staging["id"] = "realm:staging"
	cli.staging["period_config"] = map[string]any{"unrelated": true}
	if _, err := f.publishSyncPeriod(t.Context(), master, current, want); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "unrelated policy") {
		t.Fatalf("unrelated staging: %v", err)
	}
	for _, args := range cli.calls {
		if syncTestCommand(args, "period", "update") || syncTestCommand(args, "period", "commit") {
			t.Fatalf("a refused publication reached %v", args)
		}
	}
}

func TestRGWImportSyncPeriodRequiresTheExactPeriodInEveryZone(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	f.realm.systemAccess, f.realm.systemSecret = "ACCESS", "SECRET-MARKER"
	states := f.zoneStates()
	if err := f.importSyncPeriod(t.Context(), states[0], states, cli.current); err != nil {
		t.Fatal(err)
	}
	pulled := false
	for _, args := range cli.calls {
		if syncTestCommand(args, "realm", "pull") {
			pulled = syncTestFlag(args, "--zone-id") == "dest-id" && syncTestFlag(args, "--url") == "http://source:7480"
		}
	}
	if !pulled {
		t.Fatal("the secondary did not pull from the master's peer endpoint")
	}
	stale := syncJSONClone(cli.current)
	stale["id"] = "period-stale"
	cli.periodByZone = map[string]map[string]any{"dest-id": stale}
	err := f.importSyncPeriod(t.Context(), states[0], states, cli.current)
	if err == nil || !strings.Contains(err.Error(), "has not imported") || strings.Contains(err.Error(), "SECRET-MARKER") {
		t.Fatalf("stale secondary: %v", err)
	}
}
