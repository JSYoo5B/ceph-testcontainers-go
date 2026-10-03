package ceph

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
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwPlacementTestContainer struct {
	testcontainers.Container
	calls                 [][]string
	group, zone           map[string]any
	currentPeriod         map[string]any
	stagingPeriod         map[string]any
	stagingExit           int
	poolType, poolID      float64
	failClass             string
	foreignDefault        bool
	pendingRegistrations  int
	stopCount, startCount int
}

func (f *rgwPlacementTestContainer) GetContainerID() string { return "gateway" }
func (f *rgwPlacementTestContainer) Stop(context.Context, *time.Duration) error {
	f.stopCount++
	return nil
}
func (f *rgwPlacementTestContainer) Start(context.Context) error { f.startCount++; return nil }
func (f *rgwPlacementTestContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.calls = append(f.calls, slices.Clone(args))
	var result any
	var raw string
	code := 0
	flag := func(name string) string {
		for i, arg := range args {
			if arg == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	switch {
	case args[0] == "hostname":
		raw = "test-gateway\n"
	case args[0] == "ceph" && slices.Contains(args, "service"):
		if f.pendingRegistrations > 0 {
			f.pendingRegistrations--
			raw = `{"services":{}}`
			break
		}
		result = map[string]any{"services": map[string]any{"rgw": map[string]any{"daemons": map[string]any{"summary": "", "10": map[string]any{"metadata": map[string]any{"hostname": "test-gateway", "pid": "1", "frontend_config#0": "beast port=7480", "zonegroup_id": "group-id", "zonegroup_name": "active-group", "zone_id": "zone-id", "zone_name": "active-zone", "realm_id": f.group["realm_id"], "realm_name": ""}}}}}}
	case args[0] == "ceph" && slices.Contains(args, "pool"):
		result = []map[string]any{
			{"pool": f.poolID, "pool_name": "index", "type": f.poolType, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool": 2, "pool_name": "extra", "type": 1, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool": 3, "pool_name": "data", "type": 1, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool": 4, "pool_name": "ec-data", "type": 3, "size": 3, "min_size": 3, "pg_num": 1},
		}
	case args[0] == "radosgw-admin":
		if flag("--zonegroup-id") != "" && (flag("--zonegroup-id") != "group-id" || flag("--zone-id") != "zone-id") {
			return 0, nil, errors.New("wrong native scope")
		}
		if flag("--zonegroup-id") == "" && f.foreignDefault {
			result = map[string]any{"id": "foreign-default", "realm_id": ""}
			break
		}
		if slices.Contains(args, "period") {
			switch {
			case slices.Contains(args, "get") && flag("--period") != "":
				if flag("--period") != "realm-id:staging" || flag("--epoch") != "1" || flag("--realm-id") != "realm-id" {
					return 0, nil, errors.New("wrong staging scope")
				}
				code = f.stagingExit
				if f.stagingPeriod == nil && code == 0 {
					code = 2
				}
				result = f.stagingPeriod
			case slices.Contains(args, "get"):
				result = f.currentPeriod
			case slices.Contains(args, "update"):
				data, _ := json.Marshal(f.currentPeriod)
				f.stagingPeriod, _ = decodeRGWPlacementObject(data)
				f.stagingPeriod["id"] = "realm-id:staging"
				group := f.stagingPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)
				data, _ = json.Marshal(f.group["placement_targets"])
				var targets []any
				_ = json.Unmarshal(data, &targets)
				group["placement_targets"] = targets
				result = f.stagingPeriod
			case slices.Contains(args, "commit"):
				data, _ := json.Marshal(f.stagingPeriod)
				f.currentPeriod, _ = decodeRGWPlacementObject(data)
				f.currentPeriod["id"] = "period-id"
				result = f.currentPeriod
			default:
				return 0, nil, errors.New("unexpected period command")
			}
			break
		}
		class := flag("--storage-class")
		if class != "" && class == f.failClass {
			raw = "PRIVATE-KEY native failure"
			code = 1
			break
		}
		if slices.Contains(args, "zonegroup") {
			if slices.Contains(args, "add") {
				target := rgwPlacementTarget(f.group, flag("--placement-id"))
				if target == nil {
					target = map[string]any{"name": flag("--placement-id"), "tags": []any{}, "storage_classes": []any{}}
					f.group["placement_targets"] = append(f.group["placement_targets"].([]any), target)
				}
				target["storage_classes"] = append(target["storage_classes"].([]any), class)
			}
			result = f.group
		} else {
			if slices.Contains(args, "add") {
				mapping := rgwZonePlacement(f.zone, flag("--placement-id"))
				if mapping == nil {
					mapping = map[string]any{"index_pool": flag("--index-pool"), "data_extra_pool": flag("--data-extra-pool"), "inline_data": true, "storage_classes": map[string]any{}}
					f.zone["placement_pools"] = append(f.zone["placement_pools"].([]any), map[string]any{"key": flag("--placement-id"), "val": mapping})
				}
				mapping["storage_classes"].(map[string]any)[class] = map[string]any{"data_pool": flag("--data-pool")}
				if slices.Contains(args, "--placement-inline-data=false") {
					mapping["inline_data"] = false
				}
			}
			result = f.zone
		}
	default:
		return 0, nil, fmt.Errorf("unexpected command %v", args)
	}
	if raw == "" && code == 0 {
		data, _ := json.Marshal(result)
		raw = string(data)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(raw)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(raw)
	return code, &stream, nil
}

func newRGWPlacementFixture() (*RGWContainer, *rgwPlacementTestContainer) {
	f := &rgwPlacementTestContainer{poolType: 1, poolID: 1}
	f.group = map[string]any{"id": "group-id", "name": "active-group", "api_name": "active-api", "realm_id": "", "zones": []any{map[string]any{"id": "zone-id"}}, "default_placement": "default-placement", "placement_targets": []any{map[string]any{"name": "default-placement", "storage_classes": []any{"STANDARD"}, "tags": []any{}}}}
	f.zone = map[string]any{"id": "zone-id", "name": "active-zone", "realm_id": "", "system_key": map[string]any{"key": "PRIVATE-KEY"}, "placement_pools": []any{}}
	owner := &Container{Container: f, settings: options{startupTimeout: time.Minute}, services: map[string]testcontainers.Container{"rgw": f}}
	return &RGWContainer{Container: f, owner: owner, port: 7480, config: RGWConfig{Name: "default"}}, f
}

func rgwPlacementTestConfig() RGWPlacementConfig {
	return RGWPlacementConfig{Name: "tiered", IndexPool: "index", DataExtraPool: "extra", StorageClasses: []RGWStorageClassConfig{{Name: "STANDARD_IA", DataPool: "ec-data"}, {Name: "STANDARD", DataPool: "data"}}}
}

func TestRGWPlacementValidatesNamesClassesAndCopiesConfig(t *testing.T) {
	for _, edit := range []func(*RGWPlacementConfig){
		func(c *RGWPlacementConfig) { c.Name = "--foreign" }, func(c *RGWPlacementConfig) { c.Name = "default-placement" },
		func(c *RGWPlacementConfig) { c.StorageClasses = nil }, func(c *RGWPlacementConfig) { c.StorageClasses[0].Name = "standard_ia" },
		func(c *RGWPlacementConfig) { c.StorageClasses[0].Name = "STANDARD" }, func(c *RGWPlacementConfig) { c.IndexPool = "--option" },
	} {
		g, f := newRGWPlacementFixture()
		config := rgwPlacementTestConfig()
		edit(&config)
		if p, err := g.CreatePlacement(t.Context(), config); err == nil || p != nil || len(f.calls) != 0 {
			t.Fatal("invalid configuration reached native CLI")
		}
	}
	inline := false
	config := rgwPlacementTestConfig()
	config.InlineData = &inline
	normalized, err := normalizeRGWPlacementConfig(config)
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
	p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !p.confirmed || p.LocationConstraint != "active-api:tiered" || p.ZoneID != "zone-id" {
		t.Fatalf("unexpected placement descriptor %+v", p)
	}
	state, err := g.PlacementStatus(t.Context(), p)
	if err != nil || state.DefaultPlacement != "default-placement" || len(state.StorageClasses) != 2 {
		t.Fatalf("placement status: %+v %v", state, err)
	}
	p.LocationConstraint = "caller-mutated:foreign"
	if state, err := g.PlacementStatus(t.Context(), p); err != nil || state.LocationConstraint != "active-api:tiered" {
		t.Fatal("native status trusted a mutable descriptor field")
	}
	state.StorageClasses[0].DataPool = "caller-mutated"
	if state, err := g.PlacementStatus(t.Context(), p); err != nil || state.StorageClasses[0].DataPool != "data" {
		t.Fatal("native policy snapshot aliases the owned placement configuration")
	}
	for _, args := range f.calls {
		if args[0] == "radosgw-admin" && (slices.Contains(args, "--rgw-zone") || slices.Contains(args, "--rgw-zonegroup")) {
			t.Fatal("native default names were used instead of exact runtime IDs")
		}
	}
	if err := g.ApplyPlacement(t.Context(), p); err != nil || f.stopCount != 1 || f.startCount != 1 {
		t.Fatalf("standalone placement activation did not explicitly restart gateway: %v", err)
	}
	if _, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig()); err == nil {
		t.Fatal("existing placement was overwritten")
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if value := fmt.Sprintf(format, p); strings.Contains(value, "PRIVATE") {
			t.Fatal("placement descriptor exposed native zone secrets")
		}
	}
}

func TestRGWPlacementRejectsECMetadataAndUnrelatedChanges(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.poolType = 3
	if p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig()); err == nil || p != nil {
		t.Fatal("EC bucket index pool accepted")
	}
	for _, args := range f.calls {
		if slices.Contains(args, "add") {
			t.Fatal("invalid metadata pool mutated RGW")
		}
	}
	g, f = newRGWPlacementFixture()
	p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	f.group["default_placement"] = "other-default"
	if err := g.ApplyPlacement(t.Context(), p); err == nil || f.stopCount != 0 {
		t.Fatal("unrelated zonegroup change was activated")
	}
	f.group["default_placement"] = "default-placement"
	f.foreignDefault = true
	if err := g.ApplyPlacement(t.Context(), p); err == nil || f.stopCount != 0 {
		t.Fatal("gateway restarted into changed native defaults")
	}
	f.foreignDefault = false
	f.poolID = 99
	if _, err := g.PlacementStatus(t.Context(), p); err == nil {
		t.Fatal("recreated pool adopted")
	}
}

func TestRGWPlacementPartialFailureNeverRollsBackOrActivates(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.failClass = "STANDARD_IA"
	p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig())
	if err == nil || p == nil || p.confirmed || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("partial failure was hidden or secret exposed")
	}
	if rgwZonePlacement(f.zone, "tiered") == nil {
		t.Fatal("successful STANDARD mapping rolled back")
	}
	if err := g.ApplyPlacement(t.Context(), p); err == nil || f.stopCount != 0 {
		t.Fatal("partial configuration activated")
	}
	for _, args := range f.calls {
		if slices.Contains(args, "rm") || slices.Contains(args, "delete") || slices.Contains(args, "commit") {
			t.Fatal("partial error removed resources or published realm")
		}
	}
}

func TestRGWPlacementRuntimeScopeRejectsAmbiguity(t *testing.T) {
	valid := `{"services":{"osd":{"daemons":{"summary":""}},"rgw":{"daemons":{"summary":"","1":{"metadata":{"hostname":"fixture","pid":"1","frontend_config#0":"beast port=7480","realm_id":"realm","realm_name":"r","zone_id":"z","zone_name":"zone","zonegroup_id":"g","zonegroup_name":"group"}}}}}}`
	scope, err := decodeRGWPlacementScope([]byte(valid), "fixture", "beast port=7480")
	if err != nil || scope.realmID != "realm" {
		t.Fatal("native scope was not derived")
	}
	for _, data := range []string{strings.Replace(valid, `"zone_id":"z"`, `"zone_id":""`, 1), strings.Replace(valid, `"pid":"1"`, `"pid":"2"`, 1), `{"services":null}`, "not-json"} {
		if _, err := decodeRGWPlacementScope([]byte(data), "fixture", "beast port=7480"); err == nil {
			t.Fatal("missing runtime identity accepted")
		}
	}
	var duplicate map[string]any
	if err := json.Unmarshal([]byte(valid), &duplicate); err != nil {
		t.Fatal(err)
	}
	daemons := duplicate["services"].(map[string]any)["rgw"].(map[string]any)["daemons"].(map[string]any)
	daemons["2"] = map[string]any{"metadata": map[string]string{"hostname": "fixture", "pid": "1", "frontend_config#0": "beast port=7480", "realm_id": "other", "zone_id": "other-z", "zone_name": "other-zone", "zonegroup_id": "other-g", "zonegroup_name": "other-group"}}
	data, _ := json.Marshal(duplicate)
	if _, err := decodeRGWPlacementScope(data, "fixture", "beast port=7480"); err == nil {
		t.Fatal("two different runtime scopes for one hostname were accepted")
	}
	// Host mode shares hostname and PID 1 across gateways. The actual daemon
	// listener must select exactly the requested gateway's native scope.
	first := daemons["1"].(map[string]any)["metadata"].(map[string]any)
	first["frontend_config#0"] = "beast endpoint=127.0.0.1:42001"
	second := daemons["2"].(map[string]any)["metadata"].(map[string]string)
	second["frontend_config#0"] = "beast endpoint=127.0.0.1:42002"
	data, _ = json.Marshal(duplicate)
	scope, err = decodeRGWPlacementScope(data, "fixture", "beast endpoint=127.0.0.1:42001")
	if err != nil || scope.realmID != "realm" || scope.zoneID != "z" {
		t.Fatalf("host listener did not select first native scope: %+v error=%v", scope, err)
	}
	scope, err = decodeRGWPlacementScope(data, "fixture", "beast endpoint=127.0.0.1:42002")
	if err != nil || scope.realmID != "other" || scope.zoneID != "other-z" {
		t.Fatalf("host listener did not select second native scope: %+v error=%v", scope, err)
	}
	daemons["2"] = "malformed daemon"
	data, _ = json.Marshal(duplicate)
	if _, err := decodeRGWPlacementScope(data, "fixture", "beast endpoint=127.0.0.1:42001"); err == nil {
		t.Fatal("malformed native daemon accepted")
	}
}

func TestRGWPlacementWaitsForAsynchronousServiceRegistration(t *testing.T) {
	g, f := newRGWPlacementFixture()
	f.pendingRegistrations = 1
	if _, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig()); err != nil {
		t.Fatal(err)
	}
	g, f = newRGWPlacementFixture()
	f.pendingRegistrations = 1000
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := g.CreatePlacement(ctx, rgwPlacementTestConfig()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("registration wait lost caller cancellation: %v", err)
	}
	for _, args := range f.calls {
		if slices.Contains(args, "add") {
			t.Fatal("policy was mutated without a native service identity")
		}
	}
}

func TestRGWPlacementPeriodGuardAllowsOnlyOwnedTarget(t *testing.T) {
	previous, _ := decodeRGWPlacementObject([]byte(`{"realm_id":"r","master_zone":"z","master_zonegroup":"g","period_config":{"quota":1},"period_map":{"id":"old","zonegroups":[{"id":"g","default_placement":"default-placement","placement_targets":[{"name":"default-placement","storage_classes":["STANDARD"]}],"zones":[{"id":"z","endpoints":["http://original"]}]}]}}`))
	target := map[string]any{"name": "tiered", "storage_classes": []any{"STANDARD", "STANDARD_IA"}}
	next, _ := decodeRGWPlacementObject([]byte(`{"realm_id":"r","master_zone":"z","master_zonegroup":"g","period_config":{"quota":1},"period_map":{"id":"staging","zonegroups":[{"id":"g","default_placement":"default-placement","placement_targets":[{"name":"default-placement","storage_classes":["STANDARD"]},{"name":"tiered","storage_classes":["STANDARD","STANDARD_IA"]}],"zones":[{"id":"z","endpoints":["http://original"]}]}]}}`))
	if err := rgwPlacementPeriodChange(previous, next, "g", target); err != nil {
		t.Fatal(err)
	}
	next["master_zone"] = "foreign"
	if err := rgwPlacementPeriodChange(previous, next, "g", target); err == nil {
		t.Fatal("foreign metadata master published")
	}
	next["master_zone"] = "z"
	next["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["zones"] = []any{map[string]any{"id": "z", "endpoints": []any{"http://foreign"}}}
	if err := rgwPlacementPeriodChange(previous, next, "g", target); err == nil {
		t.Fatal("unrelated endpoint edit published")
	}
}

func TestRGWPlacementExistingStagingIsCheckedBeforeOverwrite(t *testing.T) {
	for _, test := range []struct {
		name     string
		edit     func(*rgwPlacementTestContainer)
		wantFail bool
	}{
		{name: "absent"},
		{name: "current policy", edit: func(f *rgwPlacementTestContainer) {
			data, _ := json.Marshal(f.currentPeriod)
			f.stagingPeriod, _ = decodeRGWPlacementObject(data)
			f.stagingPeriod["id"] = "realm-id:staging"
		}},
		{name: "unrelated pending policy", wantFail: true, edit: func(f *rgwPlacementTestContainer) {
			data, _ := json.Marshal(f.currentPeriod)
			f.stagingPeriod, _ = decodeRGWPlacementObject(data)
			f.stagingPeriod["id"] = "realm-id:staging"
			f.stagingPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["api_name"] = "foreign-api"
		}},
		{name: "unknown future policy", wantFail: true, edit: func(f *rgwPlacementTestContainer) {
			data, _ := json.Marshal(f.currentPeriod)
			f.stagingPeriod, _ = decodeRGWPlacementObject(data)
			f.stagingPeriod["id"] = "realm-id:staging"
			f.stagingPeriod["future_policy"] = map[string]any{"setting": "PRIVATE-KEY"}
		}},
		{name: "permission failure", wantFail: true, edit: func(f *rgwPlacementTestContainer) { f.stagingExit = 13 }},
		{name: "wrong realm", wantFail: true, edit: func(f *rgwPlacementTestContainer) {
			data, _ := json.Marshal(f.currentPeriod)
			f.stagingPeriod, _ = decodeRGWPlacementObject(data)
			f.stagingPeriod["id"] = "realm-id:staging"
			f.stagingPeriod["realm_id"] = "foreign-realm"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			f.group["realm_id"], f.zone["realm_id"] = "realm-id", "realm-id"
			data, _ := json.Marshal(f.group)
			group, _ := decodeRGWPlacementObject(data)
			f.currentPeriod = map[string]any{"id": "period-id", "epoch": 3, "realm_epoch": 1, "predecessor_uuid": "older", "realm_id": "realm-id", "master_zone": "zone-id", "master_zonegroup": "group-id", "period_config": map[string]any{}, "period_map": map[string]any{"id": "period-id", "zonegroups": []any{group}}, "sync_status": []any{}}
			p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig())
			if err != nil {
				t.Fatal(err)
			}
			if test.edit != nil {
				test.edit(f)
			}
			data, _ = json.Marshal(f.stagingPeriod)
			before, _ := decodeRGWPlacementObject(data)
			f.calls = nil
			err = g.ApplyPlacement(t.Context(), p)
			if (err != nil) != test.wantFail {
				t.Fatalf("unexpected staging guard result: %v", err)
			}
			if err != nil {
				if strings.Contains(err.Error(), "PRIVATE-KEY") {
					t.Fatal("staging guard leaked native policy")
				}
				for _, args := range f.calls {
					if slices.Contains(args, "update") || slices.Contains(args, "commit") {
						t.Fatal("unrelated staging was overwritten before validation")
					}
				}
				if !reflect.DeepEqual(before, f.stagingPeriod) {
					t.Fatal("refused staging policy changed")
				}
			}
		})
	}
}

func TestRGWPlacementReloadRequiresPublishedTargetAndExactLocalResources(t *testing.T) {
	for _, test := range []struct {
		name     string
		edit     func(*RGWPlacement, *rgwPlacementTestContainer)
		wantFail bool
	}{
		{name: "published secondary zone"},
		{name: "unpublished target", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) {
			group := f.currentPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)
			group["placement_targets"] = []any{map[string]any{"name": "default-placement", "storage_classes": []any{"STANDARD"}}}
		}},
		{name: "missing zone", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) {
			f.currentPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)["zones"] = []any{}
		}},
		{name: "staging instead of committed", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) {
			f.currentPeriod["id"] = "realm-id:staging"
		}},
		{name: "recreated pool", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) { f.poolID = 999 }},
		{name: "changed local mapping", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) {
			rgwZonePlacement(f.zone, p.config.Name)["index_pool"] = "foreign-index"
		}},
		{name: "changed startup scope", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) { f.foreignDefault = true }},
		{name: "unconfirmed handle", wantFail: true, edit: func(p *RGWPlacement, f *rgwPlacementTestContainer) { p.confirmed = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, f := newRGWPlacementFixture()
			f.group["realm_id"], f.zone["realm_id"] = "realm-id", "realm-id"
			data, _ := json.Marshal(f.group)
			group, _ := decodeRGWPlacementObject(data)
			f.currentPeriod = map[string]any{"id": "period-id", "realm_id": "realm-id", "master_zone": "other-zone", "master_zonegroup": "group-id", "period_config": map[string]any{}, "period_map": map[string]any{"id": "period-id", "zonegroups": []any{group}}}
			p, err := g.CreatePlacement(t.Context(), rgwPlacementTestConfig())
			if err != nil {
				t.Fatal(err)
			}
			data, _ = json.Marshal(f.group["placement_targets"])
			var published []any
			_ = json.Unmarshal(data, &published)
			group["placement_targets"] = published
			if test.edit != nil {
				test.edit(p, f)
			}
			f.calls = nil
			err = g.ReloadPlacement(t.Context(), p)
			if (err != nil) != test.wantFail {
				t.Fatalf("unexpected local reload result: %v", err)
			}
			if test.wantFail && (f.stopCount != 0 || f.startCount != 0) {
				t.Fatal("failed preflight interrupted the gateway")
			}
			if !test.wantFail && (f.stopCount != 1 || f.startCount != 1) {
				t.Fatal("confirmed secondary did not restart exactly once")
			}
			for _, args := range f.calls {
				if slices.Contains(args, "update") || slices.Contains(args, "commit") || slices.Contains(args, "pull") {
					t.Fatal("local activation mutated federation topology")
				}
			}
		})
	}
}
