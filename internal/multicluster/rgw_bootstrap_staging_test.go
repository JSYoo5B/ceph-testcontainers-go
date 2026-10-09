package multicluster

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

func TestRGWFreshBootstrapCanonicalizesRemoteCommitStagingWithoutCommit(t *testing.T) {
	f, cli, a, b := newRGWSyncTestFixture()
	f.bootstrapStagingPending = true
	cli.current["epoch"] = json.Number("4")
	cli.staging = syncJSONClone(cli.current)
	cli.staging["id"] = "realm:staging"
	cli.staging["realm_epoch"], cli.staging["predecessor_uuid"] = json.Number("2"), cli.current["id"]
	group := syncPeriodGroups(cli.staging)[0]
	group["zones"] = group["zones"].([]any)[:1]
	group["zones"].([]any)[0].(map[string]any)["log_data"] = false
	before := syncJSONClone(cli.current)
	if syncPeriodPolicyEqual(cli.staging, before) {
		t.Fatal("regression fixture lacks the native stale source-only staging topology")
	}
	if err := f.canonicalizeBootstrapStaging(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.bootstrapStagingPending || cli.staging["epoch"] != json.Number("4") || !syncPeriodPolicyEqual(cli.staging, before) || !reflect.DeepEqual(cli.current, before) || a.starts != 0 || b.starts != 0 {
		t.Fatal("fresh canonicalization changed current topology or failed to retain both zones")
	}
	updates := 0
	for _, args := range cli.calls {
		if syncTestCommand(args, "period", "commit") || slices.Contains(args, "--commit") {
			t.Fatal("bootstrap staging was unexpectedly committed")
		}
		if syncTestCommand(args, "period", "update") {
			updates++
		}
	}
	if updates != 1 {
		t.Fatal("fresh bootstrap did not perform one source-local staging update")
	}
	beforeCalls := len(cli.calls)
	if err := f.canonicalizeBootstrapStaging(t.Context()); err == nil || len(cli.calls) != beforeCalls {
		t.Fatal("completed bootstrap admitted a later staging overwrite")
	}
	// Foreign manual staging remains protected after returning the fixture.
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "later-owned", Status: RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	cli.staging["period_config"].(map[string]any)["foreign_pending"] = true
	pending := syncJSONClone(cli.staging)
	if err := f.ApplySyncGroup(t.Context(), g); err == nil || !reflect.DeepEqual(cli.staging, pending) {
		t.Fatal("fresh bootstrap weakened later foreign-stage protection")
	}
}

func TestRGWBootstrapStagingPreflightPreservesForeignStoredPolicy(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	f.bootstrapStagingPending = true
	cli.staging = syncJSONClone(cli.current)
	cli.group["foreign_pending"] = true
	before := syncJSONClone(cli.staging)
	if err := f.canonicalizeBootstrapStaging(t.Context()); err == nil || !reflect.DeepEqual(cli.staging, before) {
		t.Fatal("bootstrap canonicalization overwrote unrelated stored policy")
	}
	for _, args := range cli.calls {
		if syncTestCommand(args, "period", "update") {
			t.Fatal("foreign stored configuration reached period update")
		}
	}
}

func TestRGWOuterFreshTopologyCanonicalizesAllOwnedZonesBeforeClosingAdmission(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	// The two-zone constructor has completed; only the still-private outer
	// constructor reopens bootstrap admission while assembling its fresh graph.
	f.bootstrapStagingPending = true
	if err := f.canonicalizeBootstrapStaging(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.additionalZones = map[string]*rgwZoneState{"third": {RGWZone: RGWZone{Name: "third", ID: "third-id", Zonegroup: "group", PeerEndpoint: "http://third:7480"}, client: cli}}
	zones := cli.group["zones"].([]any)
	cli.group["zones"] = append(zones, map[string]any{"id": "third-id", "name": "third", "endpoints": []any{"http://third:7480"}})
	cli.current["period_map"].(map[string]any)["zonegroups"] = []any{syncJSONClone(cli.group)}
	f.bootstrapStagingPending = true
	if err := f.canonicalizeBootstrapStaging(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.bootstrapStagingPending || len(syncPeriodGroups(cli.staging)[0]["zones"].([]any)) != 3 || !syncPeriodPolicyEqual(cli.staging, cli.current) {
		t.Fatal("outer fresh topology left earlier pair-only staging or later overwrite admission")
	}
}
