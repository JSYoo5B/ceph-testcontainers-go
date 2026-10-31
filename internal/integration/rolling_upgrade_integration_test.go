//go:build integration && ci_optional && ci_batch_rolling_upgrade

//ci: timeout=40m job-timeout=50 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// The one pair this PoC upgrades: the previous supported release to the
// current one, both from the pinned official role images. Upstream supports
// upgrading Squid to Tentacle; downgrade is not attempted.
const (
	upgradeFrom = "19.2.5"
	upgradeTo   = "20.2.4"
)

// upgradeImages reads the pinned role images of one release.
func upgradeImages(t *testing.T, release string) (essential, client string) {
	t.Helper()
	data, err := os.ReadFile("../../.github/ceph-role-images.json")
	if err != nil {
		t.Fatal(err)
	}
	var pinned struct {
		Images map[string]map[string]map[string]string `json:"images"`
	}
	if err := json.Unmarshal(data, &pinned); err != nil {
		t.Fatal(err)
	}
	roles := pinned.Images[release]["official"]
	if roles["essential"] == "" || roles["client"] == "" {
		t.Fatalf("no pinned official role images for %s", release)
	}
	const repo = "ghcr.io/jsyoo5b/ceph-testcontainers-images@"
	return repo + roles["essential"], repo + roles["client"]
}

// TestRollingUpgradeSquidToTentacle upgrades a running Squid cluster to
// Tentacle one daemon at a time, in the upstream order: MONs, then MGRs, then
// OSDs, then require-osd-release. Every MON keeps its store in a cluster-owned
// volume and is replaced in place: MonitorContainer.Replace starts a Tentacle
// container on the same store and address, so the quorum goes through
// mixed-version states with an unchanged monmap. The MGR holds no store and is
// replaced by membership: a Tentacle manager is added before the Squid one is
// removed. OSDs are upgraded in place: each one is detached and its retained
// store reopened from the Tentacle image, so the Squid-written store is opened
// by the new release. After every step all
// acknowledged writes read back and a new batch is acknowledged, and
// `ceph versions` must show exactly the expected per-role layout.
//
// The fixture's Ceph CLI and client toolkit stay on the Squid images
// throughout; command forms are those of the source release. Health is
// recorded, not required: Tentacle MONs report the AUTH_INSECURE_* checks for
// the keys Squid created, and the fixture rotates none of them.
func TestRollingUpgradeSquidToTentacle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Minute)
	defer cancel()
	from, fromClient := upgradeImages(t, upgradeFrom)
	to, _ := upgradeImages(t, upgradeTo)
	opts := []testcontainers.ContainerCustomizer{
		ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{Essential: from, Client: fromClient})),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3).WithPoolDefaults(3, 2)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume()),
		ceph.WithMonitorStorage(ceph.MonitorStorage{}.WithVolume()),
	}
	cluster, err := ceph.Run(ctx, from, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("rolling upgrade cluster cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	u := &upgradeRun{cluster: cluster}
	defer func() {
		t.Logf("ROLLING_UPGRADE_RECEIPT %s", mustMarshal(map[string]any{"from": upgradeFrom, "to": upgradeTo, "steps": u.steps}))
	}()
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	if facts, err := ceph.FactsOf(cluster); err != nil || facts.CephVersion != upgradeFrom {
		t.Fatalf("cluster release %q, want %s: %v", facts.CephVersion, upgradeFrom, err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: upgradePool, Application: "rados", Replicas: 3, MinSize: 2, PGNum: 16}); err != nil {
		t.Fatal(err)
	}
	u.client = harness.ToolkitContainer(t, cluster)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	layout := func(mon, mgr, osd [2]int) upgradeCheckpoint {
		v := upgradeCheckpoint{}
		for role, counts := range map[string][2]int{"mon": mon, "mgr": mgr, "osd": osd} {
			v[role] = map[string]int{}
			if counts[0] > 0 {
				v[role][upgradeFrom] = counts[0]
			}
			if counts[1] > 0 {
				v[role][upgradeTo] = counts[1]
			}
		}
		return v
	}
	u.step(t, ctx, "source", layout([2]int{3, 0}, [2]int{1, 0}, [2]int{3, 0}))
	if _, err := cluster.Ceph(ctx, "osd", "set", "noout"); err != nil {
		t.Fatal(err)
	}

	// MONs: each one moves to a Tentacle container on the store and address
	// it keeps; the membership never changes.
	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) != 3 {
		t.Fatalf("%d monitors: %v", len(monitors), err)
	}
	for i, old := range monitors {
		replaced, err := old.Replace(ctx, ceph.DaemonOptions{}.WithImage(to))
		if err != nil {
			t.Fatalf("replace mon.%s on Tentacle: %v", old.DaemonName, err)
		}
		if v, err := replaced.Version(ctx); err != nil || v != upgradeTo {
			t.Fatalf("mon.%s reports %q: %v", old.DaemonName, v, err)
		}
		u.step(t, ctx, "mon-replaced-"+old.DaemonName, layout([2]int{2 - i, i + 1}, [2]int{1, 0}, [2]int{3, 0}))
	}
	q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil || len(q.QuorumNames) != 3 {
		t.Fatalf("quorum after the MON upgrade %v: %v", q.QuorumNames, err)
	}
	// With every MON on Tentacle the monmap's minimum release follows.
	if release := u.minMonRelease(t, ctx); release != "tentacle" {
		t.Fatalf("min_mon_release_name %q after the MON upgrade", release)
	}

	// MGRs: add a Tentacle manager, then remove the Squid one.
	managers, err := cluster.Managers(ctx)
	if err != nil || len(managers) != 1 {
		t.Fatalf("%d managers: %v", len(managers), err)
	}
	mgr, err := cluster.AddManager(ctx, "u0", ceph.DaemonOptions{}.WithImage(to))
	if err != nil {
		t.Fatal("add Tentacle mgr:", err)
	}
	if v, err := mgr.Version(ctx); err != nil || v != upgradeTo {
		t.Fatalf("mgr.u0 reports %q: %v", v, err)
	}
	u.step(t, ctx, "mgr-added", layout([2]int{0, 3}, [2]int{1, 1}, [2]int{3, 0}))
	if err := managers[0].Remove(ctx); err != nil {
		t.Fatal("remove Squid mgr:", err)
	}
	u.step(t, ctx, "mgr-removed", layout([2]int{0, 3}, [2]int{0, 1}, [2]int{3, 0}))

	// OSDs: detach each one and reopen its retained store from Tentacle.
	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) != 3 {
		t.Fatalf("%d OSDs: %v", len(osds), err)
	}
	for i, osd := range osds {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatalf("before osd.%d: %v", osd.ID, err)
		}
		detached, err := osd.Detach(ctx)
		if err != nil {
			t.Fatalf("detach osd.%d: %v", osd.ID, err)
		}
		reopened, err := detached.Reopen(ctx, ceph.OSDOptions{}.WithImage(to))
		if err != nil {
			t.Fatalf("reopen osd.%d on Tentacle: %v", osd.ID, err)
		}
		if v, err := reopened.Version(ctx); err != nil || v != upgradeTo {
			t.Fatalf("osd.%d reports %q: %v", osd.ID, v, err)
		}
		u.step(t, ctx, "osd-reopened-"+strconv.Itoa(osd.ID), layout([2]int{0, 3}, [2]int{0, 1}, [2]int{2 - i, i + 1}))
	}

	// Finish: only Tentacle OSDs may join from now on.
	if _, err := cluster.Ceph(ctx, "osd", "require-osd-release", "tentacle"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Ceph(ctx, "osd", "unset", "noout"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	dump, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var osdMap struct {
		RequireOSDRelease string `json:"require_osd_release"`
	}
	if err := json.Unmarshal(dump, &osdMap); err != nil || osdMap.RequireOSDRelease != "tentacle" {
		t.Fatalf("require_osd_release %q: %v", osdMap.RequireOSDRelease, err)
	}
	u.step(t, ctx, "finished", layout([2]int{0, 3}, [2]int{0, 1}, [2]int{0, 3}))
	if facts, err := ceph.FactsOf(cluster); err != nil || facts.CephVersion != upgradeFrom {
		t.Fatalf("the fixture's recorded release changed to %q: %v", facts.CephVersion, err)
	}
}

// minMonRelease is the monmap's min_mon_release_name, polled briefly because
// the MONs commit it after the last one joined on the new release.
func (u *upgradeRun) minMonRelease(t *testing.T, ctx context.Context) string {
	t.Helper()
	var dump struct {
		MinMonRelease string `json:"min_mon_release_name"`
	}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(3 * time.Second) {
		raw, err := u.cluster.Ceph(ctx, "mon", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &dump); err != nil {
			t.Fatal(err)
		}
		if dump.MinMonRelease == "tentacle" || time.Now().After(deadline) {
			return dump.MinMonRelease
		}
	}
}
