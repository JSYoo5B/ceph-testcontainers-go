package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestNoInitialManagersRejectsContradictionsBeforeRuntime(t *testing.T) {
	for _, test := range []struct {
		name, want string
		opt        Option
	}{
		{"positive count", "explicit initial manager count", WithManagerCount(1)},
		{"HA count", "explicit initial manager count", WithManagerCount(3)},
		{"zero count", "manager count must be at least 1", WithManagerCount(0)},
		{"negative count", "manager count must be at least 1", WithManagerCount(-1)},
		{"filesystem", "no initial CephFS", WithCephFS()},
		{"gateway", "no initial CephFS", WithRGW()},
	} {
		for _, first := range []bool{false, true} {
			opts := []testcontainers.ContainerCustomizer{test.opt, WithNoInitialManagers()}
			order := "zero-last"
			if first {
				opts, order = []testcontainers.ContainerCustomizer{WithNoInitialManagers(), test.opt}, "zero-first"
			}
			t.Run(test.name+"/"+order, func(t *testing.T) {
				var customizers atomic.Int32
				opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
				// Even a validation regression cannot allocate Docker resources:
				// the canonical canceled fallback is distinguishable from rejection.
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				cluster, err := Run(ctx, DefaultImage, opts...)
				if cluster != nil || err == nil || !strings.Contains(err.Error(), test.want) || errors.Is(err, context.Canceled) || customizers.Load() != 0 {
					t.Fatalf("configuration reached runtime or lost validation: cluster=%v err=%v customizers=%d", cluster, err, customizers.Load())
				}
			})
		}
	}
}

func TestNoInitialManagersPreservesDefaultsAndPositivePoolComposition(t *testing.T) {
	for _, test := range []struct {
		name                 string
		opts                 []Option
		managers, osds, size int
		pools                int
	}{
		{"ordinary default", nil, 1, 2, 2, 0},
		{"ordinary standby", []Option{WithManagerCount(2)}, 2, 2, 2, 0},
		{"ordinary replacement count", []Option{WithManagerCount(3), WithManagerCount(1)}, 1, 2, 2, 0},
		{"zero", []Option{WithNoInitialManagers()}, 0, 2, 2, 0},
		{"zero repeated", []Option{WithNoInitialManagers(), WithNoInitialManagers()}, 0, 2, 2, 0},
		{"zero with pool", []Option{WithNoInitialManagers(), WithPools(PoolConfig{Name: "data"})}, 0, 2, 2, 1},
		{"pool before zero", []Option{WithPools(PoolConfig{Name: "data"}), WithNoInitialManagers()}, 0, 2, 2, 1},
		{"final pool replacement", []Option{WithNoInitialManagers(), WithPools(PoolConfig{Name: "discarded"}), WithPools(PoolConfig{Name: "data"})}, 0, 2, 2, 1},
		{"one storage", []Option{WithNoInitialManagers(), WithOSDCount(1), WithPools(PoolConfig{Name: "data"})}, 0, 1, 1, 1},
		{"selected storage", []Option{WithInitialOSDs(OSDConfig{Root: "storage"}, OSDConfig{Root: "storage"}), WithDefaultCRUSHRoot("storage"), WithNoInitialManagers()}, 0, 2, 2, 0},
		{"combined zero", []Option{WithNoInitialManagers(), WithNoInitialOSDs()}, 0, 0, 2, 0},
		{"combined reverse", []Option{WithNoInitialOSDs(), WithNoInitialManagers()}, 0, 0, 2, 0},
		{"combined final empty pools", []Option{WithPools(PoolConfig{Name: "discarded"}), WithPools(), WithNoInitialManagers(), WithNoInitialOSDs()}, 0, 0, 2, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := options{osds: 2, monitors: 1, managers: 1}
			for _, opt := range test.opts {
				if err := opt(&settings); err != nil {
					t.Fatal(err)
				}
			}
			if !settings.poolDefaultsSet {
				settings.poolReplicas, settings.poolMinSize = min(2, settings.osds), 1
			}
			if err := prepareInitialComposition(&settings); err != nil {
				t.Fatal(err)
			}
			if settings.managers != test.managers || settings.osds != test.osds || settings.poolReplicas != test.size || settings.poolMinSize != 1 || len(settings.pools) != test.pools {
				t.Fatalf("final topology/defaults changed: %+v", settings)
			}
			if test.pools == 1 && (settings.pools[0].Name != "data" || settings.pools[0].Replicas != test.size || settings.pools[0].MinSize != 1) {
				t.Fatalf("initial pool lost final defaults: %+v", settings.pools)
			}
		})
	}
}

func TestNoInitialManagersPreservesPlacementNetworkAndCombinedStorageGuards(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithNoInitialManagers(), WithHostAddress("127.0.0.1")},
		{WithNoInitialManagers(), WithHostNetwork(), WithSeparateClusterNetwork()},
		{WithNoInitialManagers(), WithOSDCount(1), WithPools(PoolConfig{Name: "data", Replicas: 2})},
		{WithNoInitialManagers(), WithInitialOSDs(OSDConfig{Root: "elsewhere"})},
		{WithNoInitialManagers(), WithNoInitialOSDs(), WithPools(PoolConfig{Name: "data"})},
		{WithPools(PoolConfig{Name: "data"}), WithNoInitialOSDs(), WithNoInitialManagers()},
	} {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cluster, err := Run(ctx, DefaultImage, opts...)
		if cluster != nil || err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("zero manager bypassed existing pre-allocation guard: %v", err)
		}
	}
}

func TestNoInitialManagersCanceledRunDoesNotAllocateOrCustomize(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithNoInitialManagers()},
		{WithNoInitialManagers(), WithHostNetwork(), WithPools(PoolConfig{Name: "data"})},
		{WithNoInitialManagers(), WithMonitorCount(3), WithSeparateClusterNetwork()},
		{WithNoInitialManagers(), WithNoInitialOSDs()},
	} {
		var customizers atomic.Int32
		opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cluster, err := Run(ctx, DefaultImage, opts...)
		if cluster != nil || !errors.Is(err, context.Canceled) || customizers.Load() != 0 {
			t.Fatalf("canceled cold constructor reached runtime: cluster=%v err=%v customizers=%d", cluster, err, customizers.Load())
		}
	}
}

func TestNoInitialManagersReadinessUsesQuorumAndOwnedOSDMap(t *testing.T) {
	for _, scenario := range []string{"zero", "positive", "minority", "down", "out", "missing", "different UUID", "invalid map", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control := noInitialManagerFixture()
			cluster.settings.noInitialManagers = true
			good := scenario == "zero" || scenario == "positive"
			if scenario != "zero" {
				cluster.osds[0] = &OSDContainer{ID: 0, nativeUUID: control.osdUUID}
				control.osds = []map[string]any{{"osd": 0, "uuid": control.osdUUID, "up": 1, "in": 1, "weight": 1}}
			}
			switch scenario {
			case "minority":
				control.quorum = `{"quorum_names":["a"],"monmap":{"mons":[{"name":"a"},{"name":"b"},{"name":"c"}]}}`
			case "down":
				control.osds[0]["up"] = 0
			case "out":
				control.osds[0]["in"] = 0
			case "missing":
				control.osds = []map[string]any{}
			case "different UUID":
				control.osds[0]["uuid"] = uuid.NewString()
			case "invalid map":
				delete(control.osds[0], "weight")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			err := cluster.waitForInitialDaemons(ctx)
			wantError := error(context.DeadlineExceeded)
			if scenario == "canceled" {
				wantError = context.Canceled
			}
			if good && err != nil || !good && (err == nil || !errors.Is(err, wantError)) {
				t.Fatalf("unexpected bootstrap readiness: %v calls=%v", err, control.calls)
			}
			if good && !slices.Equal(control.calls, []string{"quorum_status --format json", "osd dump --format json"}) {
				t.Fatal("cold bootstrap queried MGR or skipped native quorum/storage", control.calls)
			}
			if scenario == "canceled" && len(control.calls) != 0 {
				t.Fatal("canceled readiness queried native state")
			}
			if slices.Contains(control.calls, "status --format json") || slices.Contains(control.calls, "mgr dump --format json") {
				t.Fatal("cold readiness waited for MGR-produced state")
			}
		})
	}
}

func TestNoInitialManagersPreservesOrdinaryAvailabilityAndCleanBarriers(t *testing.T) {
	for _, available := range []bool{false, true} {
		cluster, control := noInitialManagerFixture()
		control.available = available
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		err := cluster.waitForInitialDaemons(ctx)
		cancel()
		if available && err != nil || !available && !errors.Is(err, context.DeadlineExceeded) || !slices.Equal(control.calls, []string{"status --format json"}) {
			t.Fatalf("ordinary MGR readiness changed: available=%v err=%v calls=%v", available, err, control.calls)
		}
	}
	for _, wait := range []func(*Container, context.Context) error{(*Container).WaitForClean, (*Container).WaitForPGClean} {
		cluster, control := noInitialManagerFixture()
		cluster.settings.noInitialManagers = true
		cluster.osds[0] = &OSDContainer{ID: 0, nativeUUID: control.osdUUID}
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		err := wait(cluster, ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || !slices.Contains(control.calls, "status --format json") {
			t.Fatalf("absent MGR became PG-clean despite synthetic clean statistics: %v", err)
		}
	}
}

func TestNoInitialManagersFirstAddAdmissionAndIdentityFailures(t *testing.T) {
	cluster, control := noInitialManagerFixture()
	cluster.settings.noInitialManagers = true
	noInitialOSDQueuedCall(t, &cluster.mu, func(ctx context.Context) error { _, err := cluster.AddManager(ctx, "a"); return err })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if mgr, err := cluster.AddManager(ctx, "a"); mgr != nil || !errors.Is(err, context.Canceled) || len(control.calls) != 0 || len(cluster.managers) != 0 || cluster.ManagerContainer() != nil {
		t.Fatal("busy/canceled first manager changed native or owned state", err)
	}
	for _, scenario := range []string{"foreign auth", "foreign map", "auth list error", "auth malformed", "create response loss"} {
		t.Run(scenario, func(t *testing.T) {
			cluster, control := noInitialManagerFixture()
			cluster.settings.noInitialManagers = true
			switch scenario {
			case "foreign auth":
				control.auth = `{"auth_dump":[{"entity":"mgr.a","key":"PRIVATE-FIRST-MGR-KEY"}]}`
			case "foreign map":
				control.mgr = `{"available":true,"active_name":"a","active_gid":99,"standbys":[]}`
			case "auth list error":
				control.fail = "auth ls --format json"
			case "auth malformed":
				control.auth = `{"key":"PRIVATE-FIRST-MGR-KEY"}`
			case "create response loss":
				control.fail = "auth get-or-create mgr.a mon allow profile mgr osd allow * mds allow *"
			}
			mgr, err := cluster.AddManager(t.Context(), "a")
			if mgr != nil || err == nil || strings.Contains(err.Error(), "PRIVATE-FIRST-MGR-KEY") || len(cluster.managers) != 0 || cluster.ManagerContainer() != nil {
				t.Fatalf("first Add identity rejection leaked/adopted state: mgr=%v err=%v", mgr, err)
			}
			for _, call := range control.calls {
				if call != "auth ls --format json" && call != "mgr dump --format json" && (scenario != "create response loss" || call != control.fail) {
					t.Fatal("first Add bypassed preflight or attempted daemon startup", control.calls)
				}
			}
			if scenario == "create response loss" {
				// Auth may have committed despite loss. Existing preflight rejects
				// it on a fresh call; zero mode does not invent adoption or retry.
				control.auth = `{"auth_dump":[{"entity":"mgr.a"}]}`
				control.calls, control.fail = nil, ""
				if mgr, err := cluster.AddManager(t.Context(), "a"); mgr != nil || err == nil || !slices.Equal(control.calls, []string{"auth ls --format json"}) {
					t.Fatal("uncertain first auth was silently adopted on retry", err)
				}
			}
		})
	}
}

func TestNoInitialManagersZeroAndPartialOwnershipCleanup(t *testing.T) {
	for _, partial := range []bool{false, true} {
		cluster, control := noInitialManagerFixture()
		cluster.settings.noInitialManagers = true
		if partial {
			// Existing Add retains a descriptor after successful auth but a failed
			// Docker launch. Whole-cluster cleanup disposes of this owned state.
			cluster.managers["a"] = &ManagerContainer{DaemonName: "a", authOwned: true}
			cluster.managers["b"] = &ManagerContainer{DaemonName: "b", authOwned: true}
		}
		osd := &hostNetworkFixtureContainer{failTerminationOnce: true}
		cluster.osds[0] = &OSDContainer{ID: 0, Container: osd}
		if err := cluster.Terminate(t.Context()); err == nil || !cluster.closed || len(cluster.managers) != 0 || len(cluster.osds) != 1 || control.terminations != 1 {
			t.Fatal("partial storage teardown lost ownership or skipped zero/partial managers", err)
		}
		if err := cluster.Terminate(t.Context()); err != nil || len(cluster.osds) != 0 || osd.terminations != 2 || control.terminations != 1 {
			t.Fatal("cleanup repeated completed MON phase or skipped retained OSD", err)
		}
		if mgr, err := cluster.AddManager(t.Context(), "a"); mgr != nil || err == nil || len(control.calls) != 0 || cluster.ManagerContainer() != nil {
			t.Fatal("terminated cold constructor admitted first manager", err)
		}
	}
}

func noInitialManagerFixture() (*Container, *noInitialManagerControl) {
	control := &noInitialManagerControl{
		osdUUID: uuid.NewString(), osds: []map[string]any{},
		quorum: `{"quorum_names":["a"],"monmap":{"mons":[{"name":"a"}]}}`,
		auth:   `{"auth_dump":[]}`, mgr: `{"available":false,"active_name":"","active_gid":0,"standbys":[]}`,
	}
	cluster := &Container{Container: control, settings: options{startupTimeout: time.Second}, managers: map[string]*ManagerContainer{}, osds: map[int]*OSDContainer{}}
	return cluster, control
}

type noInitialManagerControl struct {
	testcontainers.Container
	quorum, osdUUID, auth, mgr, fail string
	osds                             []map[string]any
	available                        bool
	calls                            []string
	terminations                     int
}

func (c *noInitialManagerControl) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminations++
	return nil
}

func (c *noInitialManagerControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	call := strings.Join(args[3:], " ")
	c.calls = append(c.calls, call)
	if call == c.fail {
		return 1, noInitialOSDStream("PRIVATE-FIRST-MGR-KEY"), nil
	}
	var result string
	switch call {
	case "quorum_status --format json":
		result = c.quorum
	case "osd dump --format json":
		data, _ := json.Marshal(map[string]any{"flags": "", "osds": c.osds})
		result = string(data)
	case "status --format json":
		result = fmt.Sprintf(`{"mgrmap":{"available":%t},"osdmap":{"num_osds":1,"num_up_osds":1,"num_in_osds":1},"pgmap":{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean","count":8}]}}`, c.available)
	case "auth ls --format json":
		result = c.auth
	case "mgr dump --format json":
		result = c.mgr
	default:
		return 1, noInitialOSDStream("unexpected cold-manager test command"), nil
	}
	return 0, noInitialOSDStream(result), nil
}
