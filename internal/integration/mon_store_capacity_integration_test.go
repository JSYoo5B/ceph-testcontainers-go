//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

const monStoreDir = "/var/lib/ceph/mon/ceph-a"

// The MON store row of issue 63. The run first shows where the store lives:
// store.db and the MON data directory are on the container's overlay root,
// the engine's disk that every container shares, so the fixture has no owned
// filesystem under the MON to exhaust and none is filled. What can be tested
// is the MON's space policy: mon_data_avail_warn and mon_data_avail_crit are
// percentages of that filesystem's available space. Raising each above the
// observed share raises MON_DISK_LOW or MON_DISK_CRIT without writing a byte,
// and restoring it clears the check. The MON keeps running in both.
func TestMonStoreCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t)
	raw := scratchContainerExec(t, ctx, cluster, "python3", "-I", "-c", scratchBlockBackingScript, monStoreDir+"/store.db", monStoreDir)
	view, err := parseBlockBacking(raw)
	if err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	domain, err := monStoreDomain(view)
	if err != nil {
		t.Fatal(err)
	}
	var vfs struct{ Total, Avail int64 }
	if err := json.Unmarshal(scratchContainerExec(t, ctx, cluster, "python3", "-I", "-c", logicalStatVFS, monStoreDir), &vfs); err != nil || vfs.Total <= 0 {
		t.Fatalf("MON filesystem %+v: %v", vfs, err)
	}
	avail := int(vfs.Avail * 100 / vfs.Total)
	receipt := map[string]any{"domain": domain, "fstype": *view.Block.Mount.FSType, "mountpoint": *view.Block.Mount.Mountpoint, "avail_percent": avail}
	defer func() {
		data, _ := json.Marshal(receipt)
		t.Logf("MON_STORE_CAPACITY_RECEIPT %s", data)
	}()
	if domain != monStoreEngineDisk {
		t.Fatalf("the MON store is on %s, not the engine disk this run documents", domain)
	}
	if avail >= 99 {
		t.Skipf("the MON filesystem has %d%% available; no threshold above it is left", avail)
	}
	wait := func(code string, present bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			health, err := cluster.Ceph(ctx, "health", "detail", "--format", "json")
			if err != nil {
				t.Fatal(err)
			}
			checks, err := parseHealthChecks(health)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(checks, code) == present {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s present=%t was not reached: %v", code, present, checks)
			}
			time.Sleep(3 * time.Second)
		}
	}
	for _, step := range []struct{ name, code string }{{"mon_data_avail_warn", "MON_DISK_LOW"}, {"mon_data_avail_crit", "MON_DISK_CRIT"}} {
		handle, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mon", Name: step.name, Value: "99"})
		t.Cleanup(func() { _ = handle.Restore(context.Background()) })
		if err != nil {
			t.Fatal(step.name, err)
		}
		wait(step.code, true)
		state, err := cluster.State(ctx)
		if err != nil || state == nil || !state.Running {
			t.Fatalf("the MON is not running under %s: %+v %v", step.name, state, err)
		}
		if err := handle.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		wait(step.code, false)
		receipt[step.name] = map[string]any{"raised": step.code, "cleared": true, "mon_running": true}
		t.Logf("MON_STORE_CAPACITY %s=99 raised=%s avail=%s%% cleared=true", step.name, step.code, strconv.Itoa(avail))
	}
}
