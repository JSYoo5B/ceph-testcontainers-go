package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
)

// newRGWPlacementFixture returns the gateway core over the placement model,
// which also answers its cluster's control commands.
func newRGWPlacementFixture() (*gwtest.Gateway, *gwtest.PlacementContainer) {
	f := gwtest.NewPlacementContainer()
	return gwtest.NewGateway(f, f), f
}

func testConfig() Config {
	return Config{Name: "tiered", IndexPool: "index", DataExtraPool: "extra", StorageClasses: []StorageClass{{Name: "STANDARD_IA", DataPool: "ec-data"}, {Name: "STANDARD", DataPool: "data"}}}
}

func TestRGWPlacementValidatesNamesClassesAndCopiesConfig(t *testing.T) {
	for _, edit := range []func(*Config){
		func(c *Config) { c.Name = "--foreign" }, func(c *Config) { c.Name = "default-placement" },
		func(c *Config) { c.StorageClasses = nil }, func(c *Config) { c.StorageClasses[0].Name = "standard_ia" },
		func(c *Config) { c.StorageClasses[0].Name = "STANDARD" }, func(c *Config) { c.IndexPool = "--option" },
	} {
		g, f := newRGWPlacementFixture()
		config := testConfig()
		edit(&config)
		if p, err := create(t.Context(), g.Core, config); err == nil || p != nil || len(f.Calls) != 0 {
			t.Fatal("invalid configuration reached native CLI")
		}
	}
	inline := false
	config := testConfig()
	config.InlineData = &inline
	normalized, err := normalizeConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	config.StorageClasses[0].DataPool = "foreign"
	inline = true
	if normalized.StorageClasses[0].Name != "STANDARD" || normalized.StorageClasses[1].DataPool != "ec-data" || *normalized.InlineData {
		t.Fatal("placement configuration aliases caller memory")
	}
}

func TestRGWPlacementUsesRuntimeIDsAndRetainsDefault(t *testing.T) {
	g, f := newRGWPlacementFixture()
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !p.confirmed || p.LocationConstraint != "active-api:tiered" || p.ZoneID != "zone-id" {
		t.Fatalf("unexpected placement descriptor %+v", p)
	}
	state, err := p.Status(t.Context())
	if err != nil || !state.Confirmed || state.DefaultPlacement != "default-placement" || len(state.StorageClasses) != 2 {
		t.Fatalf("placement status: %+v %v", state, err)
	}
	p.LocationConstraint = "caller-mutated:foreign"
	if state, err := p.Status(t.Context()); err != nil || state.LocationConstraint != "active-api:tiered" {
		t.Fatal("native status trusted a mutable descriptor field")
	}
	state.StorageClasses[0].DataPool = "caller-mutated"
	if state, err := p.Status(t.Context()); err != nil || state.StorageClasses[0].DataPool != "data" {
		t.Fatal("native policy snapshot aliases the owned placement configuration")
	}
	state.Confirmed = false
	if current, err := p.Status(t.Context()); err != nil || !current.Confirmed {
		t.Fatal("public status changed private creation confirmation")
	}
	p.confirmed = false
	if partial, err := p.Status(t.Context()); err != nil || partial.Confirmed {
		t.Fatal("complete partial mapping was misreported as confirmed creation")
	}
	p.confirmed = true
	for _, args := range f.Calls {
		if args[0] == "radosgw-admin" && (slices.Contains(args, "--rgw-zone") || slices.Contains(args, "--rgw-zonegroup")) {
			t.Fatal("native default names were used instead of exact runtime IDs")
		}
	}
	if err := apply(t.Context(), g.Core, p); err != nil || f.StopCount != 1 || f.StartCount != 1 {
		t.Fatalf("standalone placement activation did not explicitly restart gateway: %v", err)
	}
	if _, err := create(t.Context(), g.Core, testConfig()); err == nil {
		t.Fatal("existing placement was overwritten")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d", "%t", "%10.3f"} {
		if value := fmt.Sprintf(format, p); strings.Contains(value, "PRIVATE") {
			t.Fatal("placement descriptor exposed native zone secrets")
		}
	}
}

func TestRGWPlacementRejectsECMetadataAndUnrelatedChanges(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.PoolType = 3
	if p, err := create(t.Context(), g.Core, testConfig()); err == nil || p != nil {
		t.Fatal("EC bucket index pool accepted")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "add") {
			t.Fatal("invalid metadata pool mutated RGW")
		}
	}
	g, f = newRGWPlacementFixture()
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	f.Group["default_placement"] = "other-default"
	if err := apply(t.Context(), g.Core, p); err == nil || f.StopCount != 0 {
		t.Fatal("unrelated zonegroup change was activated")
	}
	f.Group["default_placement"] = "default-placement"
	f.ForeignDefault = true
	if err := apply(t.Context(), g.Core, p); err == nil || f.StopCount != 0 {
		t.Fatal("gateway restarted into changed native defaults")
	}
	f.ForeignDefault = false
	f.PoolID = 99
	if _, err := p.Status(t.Context()); err == nil {
		t.Fatal("recreated pool adopted")
	}
}

func TestRGWPlacementPartialFailureNeverRollsBackOrActivates(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.FailClass = "STANDARD_IA"
	p, err := create(t.Context(), g.Core, testConfig())
	if err == nil || p == nil || p.confirmed || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("partial failure was hidden or secret exposed")
	}
	if zonePlacement(f.Zone, "tiered") == nil {
		t.Fatal("successful STANDARD mapping rolled back")
	}
	if err := apply(t.Context(), g.Core, p); err == nil || f.StopCount != 0 {
		t.Fatal("partial configuration activated")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "rm") || slices.Contains(args, "delete") || slices.Contains(args, "commit") {
			t.Fatal("partial error removed resources or published realm")
		}
	}
}

func TestRGWPlacementWaitsForAsynchronousServiceRegistration(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.PendingRegistrations = 1
	if _, err := create(t.Context(), g.Core, testConfig()); err != nil {
		t.Fatal(err)
	}
	g, f = newRGWPlacementFixture()
	f.PendingRegistrations = 1000
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := create(ctx, g.Core, testConfig()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registration wait lost caller cancellation: %v", err)
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "add") {
			t.Fatal("policy was mutated without a native service identity")
		}
	}
}

func TestRGWPlacementPeriodGuardAllowsOnlyOwnedTarget(t *testing.T) {
	previous, _ := gwbase.DecodeObject([]byte(`{"realm_id":"r","master_zone":"z","master_zonegroup":"g","period_config":{"quota":1},"period_map":{"id":"old","zonegroups":[{"id":"g","default_placement":"default-placement","placement_targets":[{"name":"default-placement","storage_classes":["STANDARD"]}],"zones":[{"id":"z","endpoints":["http://original"]}]}]}}`))
	target := map[string]any{"name": "tiered", "storage_classes": []any{"STANDARD", "STANDARD_IA"}}
	next, _ := gwbase.DecodeObject([]byte(`{"realm_id":"r","master_zone":"z","master_zonegroup":"g","period_config":{"quota":1},"period_map":{"id":"staging","zonegroups":[{"id":"g","default_placement":"default-placement","placement_targets":[{"name":"default-placement","storage_classes":["STANDARD"]},{"name":"tiered","storage_classes":["STANDARD","STANDARD_IA"]}],"zones":[{"id":"z","endpoints":["http://original"]}]}]}}`))
	if err := periodChange(previous, next, "g", target); err != nil {
		t.Fatal(err)
	}
	next["master_zone"] = "foreign"
	if err := periodChange(previous, next, "g", target); err == nil {
		t.Fatal("foreign metadata master published")
	}
	next["master_zone"] = "z"
	next["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["zones"] = []any{map[string]any{"id": "z", "endpoints": []any{"http://foreign"}}}
	if err := periodChange(previous, next, "g", target); err == nil {
		t.Fatal("unrelated endpoint edit published")
	}
}

func TestRGWPlacementExistingStagingIsCheckedBeforeOverwrite(t *testing.T) {
	for _, test := range []struct {
		name     string
		edit     func(*gwtest.PlacementContainer)
		wantFail bool
	}{
		{name: "absent"},
		{name: "current policy", edit: func(f *gwtest.PlacementContainer) {
			data, _ := json.Marshal(f.CurrentPeriod)
			f.StagingPeriod, _ = gwbase.DecodeObject(data)
			f.StagingPeriod["id"] = "realm-id:staging"
		}},
		{name: "unrelated pending policy", wantFail: true, edit: func(f *gwtest.PlacementContainer) {
			data, _ := json.Marshal(f.CurrentPeriod)
			f.StagingPeriod, _ = gwbase.DecodeObject(data)
			f.StagingPeriod["id"] = "realm-id:staging"
			f.StagingPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["api_name"] = "foreign-api"
		}},
		{name: "unknown future policy", wantFail: true, edit: func(f *gwtest.PlacementContainer) {
			data, _ := json.Marshal(f.CurrentPeriod)
			f.StagingPeriod, _ = gwbase.DecodeObject(data)
			f.StagingPeriod["id"] = "realm-id:staging"
			f.StagingPeriod["future_policy"] = map[string]any{"setting": "PRIVATE-KEY"}
		}},
		{name: "permission failure", wantFail: true, edit: func(f *gwtest.PlacementContainer) { f.StagingExit = 13 }},
		{name: "wrong realm", wantFail: true, edit: func(f *gwtest.PlacementContainer) {
			data, _ := json.Marshal(f.CurrentPeriod)
			f.StagingPeriod, _ = gwbase.DecodeObject(data)
			f.StagingPeriod["id"] = "realm-id:staging"
			f.StagingPeriod["realm_id"] = "foreign-realm"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			f.Group["realm_id"], f.Zone["realm_id"] = "realm-id", "realm-id"
			data, _ := json.Marshal(f.Group)
			group, _ := gwbase.DecodeObject(data)
			f.CurrentPeriod = map[string]any{"id": "period-id", "epoch": 3, "realm_epoch": 1, "predecessor_uuid": "older", "realm_id": "realm-id", "master_zone": "zone-id", "master_zonegroup": "group-id", "period_config": map[string]any{}, "period_map": map[string]any{"id": "period-id", "zonegroups": []any{group}}, "sync_status": []any{}}
			p, err := create(t.Context(), g.Core, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			if test.edit != nil {
				test.edit(f)
			}
			data, _ = json.Marshal(f.StagingPeriod)
			before, _ := gwbase.DecodeObject(data)
			f.Calls = nil
			err = apply(t.Context(), g.Core, p)
			if (err != nil) != test.wantFail {
				t.Fatalf("unexpected staging guard result: %v", err)
			}
			if err != nil {
				if strings.Contains(err.Error(), "PRIVATE-KEY") {
					t.Fatal("staging guard leaked native policy")
				}
				for _, args := range f.Calls {
					if slices.Contains(args, "update") || slices.Contains(args, "commit") {
						t.Fatal("unrelated staging was overwritten before validation")
					}
				}
				if !reflect.DeepEqual(before, f.StagingPeriod) {
					t.Fatal("refused staging policy changed")
				}
			}
		})
	}
}

func TestRGWPlacementReloadRequiresPublishedTargetAndExactLocalResources(t *testing.T) {
	for _, test := range []struct {
		name     string
		edit     func(*Placement, *gwtest.PlacementContainer)
		wantFail bool
	}{
		{name: "published secondary zone"},
		{name: "unpublished target", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) {
			group := f.CurrentPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)
			group["placement_targets"] = []any{map[string]any{"name": "default-placement", "storage_classes": []any{"STANDARD"}}}
		}},
		{name: "missing zone", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) {
			f.CurrentPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["zones"] = []any{}
		}},
		{name: "staging instead of committed", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) {
			f.CurrentPeriod["id"] = "realm-id:staging"
		}},
		{name: "recreated pool", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) { f.PoolID = 999 }},
		{name: "changed local mapping", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) {
			zonePlacement(f.Zone, p.config.Name)["index_pool"] = "foreign-index"
		}},
		{name: "changed startup scope", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) { f.ForeignDefault = true }},
		{name: "unconfirmed handle", wantFail: true, edit: func(p *Placement, f *gwtest.PlacementContainer) { p.confirmed = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			f.Group["realm_id"], f.Zone["realm_id"] = "realm-id", "realm-id"
			data, _ := json.Marshal(f.Group)
			group, _ := gwbase.DecodeObject(data)
			f.CurrentPeriod = map[string]any{"id": "period-id", "realm_id": "realm-id", "master_zone": "other-zone", "master_zonegroup": "group-id", "period_config": map[string]any{}, "period_map": map[string]any{"id": "period-id", "zonegroups": []any{group}}}
			p, err := create(t.Context(), g.Core, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			data, _ = json.Marshal(f.Group["placement_targets"])
			var published []any
			_ = json.Unmarshal(data, &published)
			group["placement_targets"] = published
			if test.edit != nil {
				test.edit(p, f)
			}
			f.Calls = nil
			err = reload(t.Context(), g.Core, p)
			if (err != nil) != test.wantFail {
				t.Fatalf("unexpected local reload result: %v", err)
			}
			if test.wantFail && (f.StopCount != 0 || f.StartCount != 0) {
				t.Fatal("failed preflight interrupted the gateway")
			}
			if !test.wantFail && (f.StopCount != 1 || f.StartCount != 1) {
				t.Fatal("confirmed secondary did not restart exactly once")
			}
			for _, args := range f.Calls {
				if slices.Contains(args, "update") || slices.Contains(args, "commit") || slices.Contains(args, "pull") {
					t.Fatal("local activation mutated federation topology")
				}
			}
		})
	}
}
