//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// testMGRRuntimeRestoration checks that override restorations leave the
// running managers in the original state, not only the stored configuration
// and membership: a module option that was absent is unset again in the
// running module of the active and the standby manager, a daemon option of
// the MGR returns to its original value, and a module membership change is
// followed at once by a working MGR observation. The managers run the
// mgr-modules role image: it loads more modules than essential, so a
// respawn after a membership change takes long enough to matter.
func testMGRRuntimeRestoration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	images := harness.IntegrationImageSet(t)
	images.MGR = harness.MGRModulesImage(t)
	t.Logf("MGR slot image: %s", images.MGR)
	cluster, _ := harness.NewServiceClusterWithOptions(t, "",
		ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)),
		harness.FunctionalOSDConfig(t),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(2)))
	observer := harness.ObserveCluster(cluster)
	const tickOption = "mgr_tick_period"
	stored := func(section, name string) *ceph.ConfigEntry {
		t.Helper()
		entries, err := observer.Configuration(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Section == section && entry.Mask == "" && entry.Name == name {
				copy := entry
				return &copy
			}
		}
		return nil
	}
	// eventually polls read until it returns want, for at most 30 seconds.
	// Configuration updates reach the MGR asynchronously, so the first read
	// after a stored change may still see the previous value.
	eventually := func(what, want string, read func() (string, error)) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := read()
			if err == nil && got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: running MGR reports %q (error %v), want %q", what, got, err, want)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	// balancer status reads the mode option from the running module on every
	// call (show_status in src/pybind/mgr/balancer/module.py).
	balancerMode := func() (string, error) {
		data, err := cluster.Ceph(ctx, "balancer", "status", "--format", "json")
		if err != nil {
			return "", err
		}
		var status struct {
			Mode *string `json:"mode"`
		}
		if err := json.Unmarshal(data, &status); err != nil || status.Mode == nil {
			return "", fmt.Errorf("decode balancer status: %v", err)
		}
		return *status.Mode, nil
	}
	// tell mgr config get reads the active MGR's running daemon value.
	tickPeriod := func() (string, error) {
		data, err := cluster.Ceph(ctx, "tell", "mgr", "config", "get", tickOption, "--format", "json")
		if err != nil {
			return "", err
		}
		var value map[string]string
		if err := json.Unmarshal(data, &value); err != nil {
			return "", fmt.Errorf("decode %s: %v", tickOption, err)
		}
		seconds, err := strconv.ParseFloat(value[tickOption], 64)
		if err != nil {
			return "", fmt.Errorf("decode %s: %v", tickOption, err)
		}
		return strconv.FormatFloat(seconds, 'g', -1, 64), nil
	}
	// activeGID reads the active manager's gid, or zero without an available
	// active manager.
	activeGID := func() (uint64, error) {
		data, err := cluster.Ceph(ctx, "mgr", "dump", "--format", "json")
		if err != nil {
			return 0, err
		}
		var dump struct {
			Available bool   `json:"available"`
			ActiveGID uint64 `json:"active_gid"`
		}
		if err := json.Unmarshal(data, &dump); err != nil {
			return 0, fmt.Errorf("decode mgr dump: %v", err)
		}
		if !dump.Available {
			return 0, nil
		}
		return dump.ActiveGID, nil
	}
	apply := func(setting ceph.ConfigSetting) *override.ConfigHandle {
		t.Helper()
		change, err := override.SetConfig(ctx, cluster, setting)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := change.Restore(cleanupCtx); err != nil {
				t.Errorf("restore %s: %v", setting.Name, err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		return change
	}

	// A module option that was absent. The mode enumeration has no empty
	// value, so the restoration must store it without option validation.
	const modeOption = "mgr/balancer/mode"
	if stored("mgr", modeOption) != nil {
		t.Fatalf("fresh cluster stores %s", modeOption)
	}
	eventually("original balancer mode", "upmap", balancerMode)
	mode := apply(ceph.ConfigSetting{Section: "mgr", Name: modeOption, Value: "none"})
	eventually("overridden balancer mode", "none", balancerMode)
	if err := mode.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if entry := stored("mgr", modeOption); entry != nil {
		t.Fatalf("restored %s is still stored: %+v", modeOption, entry)
	}
	eventually("restored balancer mode", "upmap", balancerMode)
	// The standby manager received the same configuration updates. After a
	// failover it serves the module with the configuration it kept.
	failed, err := activeGID()
	if err != nil || failed == 0 {
		t.Fatalf("active manager before failover: %d %v", failed, err)
	}
	harness.CephCommand(t, ctx, cluster, "mgr", "fail")
	eventually("active manager after failover", "replaced", func() (string, error) {
		gid, err := activeGID()
		if err != nil || gid == 0 || gid == failed {
			return fmt.Sprint(gid), err
		}
		return "replaced", nil
	})
	eventually("restored balancer mode after failover", "upmap", balancerMode)

	// A daemon option of the MGR that was absent. Its removal reaches the
	// running daemon without an empty value.
	if stored("mgr", tickOption) != nil {
		t.Fatalf("fresh cluster stores %s", tickOption)
	}
	original, err := tickPeriod()
	if err != nil {
		t.Fatal(err)
	}
	tick := apply(ceph.ConfigSetting{Section: "mgr", Name: tickOption, Value: "3"})
	eventually("overridden "+tickOption, "3", tickPeriod)
	if err := tick.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if entry := stored("mgr", tickOption); entry != nil {
		t.Fatalf("restored %s is still stored: %+v", tickOption, entry)
	}
	eventually("restored "+tickOption, original, tickPeriod)

	// Every membership change respawns the MGR. Each change must return only
	// once an observation through the respawned active MGR works, so the
	// observation right after it is not retried.
	observeNow := func(phase string, enabled bool) {
		t.Helper()
		started := time.Now()
		modules, err := observer.MGRModules(ctx)
		if err != nil {
			t.Fatalf("%s: MGR observation right after the change failed: %v", phase, err)
		}
		for _, module := range modules {
			if module.Name == "mirroring" {
				if module.Enabled != enabled || !module.Available {
					t.Fatalf("%s: mirroring module is %+v", phase, module)
				}
				t.Logf("%s: observation succeeded in %v", phase, time.Since(started).Round(time.Millisecond))
				return
			}
		}
		t.Fatalf("%s: mirroring module is not listed", phase)
	}
	for round := 1; round <= 2; round++ {
		started := time.Now()
		module, err := override.SetMGRModule(ctx, cluster, "mirroring", true)
		if err != nil {
			_ = module.Restore(context.WithoutCancel(ctx))
			t.Fatalf("round %d: enable mirroring: %v", round, err)
		}
		t.Logf("round %d: enable returned in %v", round, time.Since(started).Round(time.Millisecond))
		observeNow(fmt.Sprintf("round %d enable", round), true)
		started = time.Now()
		if err := module.Restore(ctx); err != nil {
			t.Fatalf("round %d: restore mirroring: %v", round, err)
		}
		t.Logf("round %d: restore returned in %v", round, time.Since(started).Round(time.Millisecond))
		observeNow(fmt.Sprintf("round %d restore", round), false)
	}
}
