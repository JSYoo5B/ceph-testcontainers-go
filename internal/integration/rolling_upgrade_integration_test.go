//go:build integration && ci_optional && ci_batch_rolling_upgrade

//ci: timeout=80m job-timeout=90 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"slices"
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

// upgradeLayout is the checkpoint with mon, mgr and osd counts of the source
// and the destination release, in that order.
func upgradeLayout(mon, mgr, osd [2]int) upgradeCheckpoint {
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

// startUpgradeCluster runs the Squid source cluster of the upgrade scenarios:
// three MONs on store volumes, one MGR and three OSDs on store volumes, a
// pool of three replicas, a toolkit client, and the "source" step. noout is
// set for the upgrade.
func startUpgradeCluster(t *testing.T, ctx context.Context, host bool) *upgradeRun {
	t.Helper()
	from, fromClient := upgradeImages(t, upgradeFrom)
	opts := []testcontainers.ContainerCustomizer{
		ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{Essential: from, Client: fromClient})),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3).WithPoolDefaults(3, 2)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume()),
		ceph.WithMonitorStorage(ceph.MonitorStorage{}.WithVolume()),
	}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
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
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	if facts, err := ceph.FactsOf(cluster); err != nil || facts.CephVersion != upgradeFrom || facts.UsesHostNetwork != host {
		t.Fatalf("cluster release %q host network %v, want %s and %v: %v", facts.CephVersion, facts.UsesHostNetwork, upgradeFrom, host, err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: upgradePool, Application: "rados", Replicas: 3, MinSize: 2, PGNum: 16}); err != nil {
		t.Fatal(err)
	}
	u.client = harness.ToolkitContainer(t, cluster)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	u.step(t, ctx, "source", upgradeLayout([2]int{3, 0}, [2]int{1, 0}, [2]int{3, 0}))
	if _, err := cluster.Ceph(ctx, "osd", "set", "noout"); err != nil {
		t.Fatal(err)
	}
	return u
}

// TestRollingUpgradeSquidToTentacle upgrades a running Squid cluster to
// Tentacle one daemon at a time, in the upstream order: MONs, then MGRs, then
// OSDs, then require-osd-release. Every MON keeps its store in a cluster-owned
// volume and is replaced in place: MonitorContainer.Replace starts a Tentacle
// container on the same store and address, so the quorum goes through
// mixed-version states with an unchanged monmap. On the host network that
// address includes the MON's ports, which the replacement holds from the
// removal of the old container until the start of the new one. The MGR holds
// no store and is replaced by membership: a Tentacle manager is added before
// the Squid one is removed. OSDs are upgraded in place: each one is detached
// and its retained store reopened from the Tentacle image, so the
// Squid-written store is opened by the new release. After every step all
// acknowledged writes read back and a new batch is acknowledged, and
// `ceph versions` must show exactly the expected per-role layout.
//
// The fixture's Ceph CLI and client toolkit stay on the Squid images
// throughout; command forms are those of the source release. Health is
// recorded, not required: Tentacle MONs report the AUTH_INSECURE_* checks for
// the keys Squid created, and the fixture rotates none of them.
func TestRollingUpgradeSquidToTentacle(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		if !t.Run(mode.Name, func(t *testing.T) { rollingUpgrade(t, mode.Host) }) {
			return
		}
	}
}

func rollingUpgrade(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	to, _ := upgradeImages(t, upgradeTo)
	u := startUpgradeCluster(t, ctx, host)
	cluster := u.cluster
	defer func() {
		t.Logf("ROLLING_UPGRADE_RECEIPT %s", mustMarshal(map[string]any{"from": upgradeFrom, "to": upgradeTo, "host": host, "steps": u.steps}))
	}()
	identity := monmapMembers(t, ctx, cluster)

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
		u.step(t, ctx, "mon-replaced-"+old.DaemonName, upgradeLayout([2]int{2 - i, i + 1}, [2]int{1, 0}, [2]int{3, 0}))
	}
	q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil || len(q.QuorumNames) != 3 {
		t.Fatalf("quorum after the MON upgrade %v: %v", q.QuorumNames, err)
	}
	// With every MON on Tentacle the monmap's minimum release follows.
	if release := u.minMonRelease(t, ctx); release != "tentacle" {
		t.Fatalf("min_mon_release_name %q after the MON upgrade", release)
	}
	u.upgradeManager(t, ctx, to, [2]int{0, 3})

	// OSDs: detach each one and reopen its retained store from Tentacle.
	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) != 3 {
		t.Fatalf("%d OSDs: %v", len(osds), err)
	}
	for i, osd := range osds {
		u.reopenOSD(t, ctx, osd, to, upgradeTo)
		u.step(t, ctx, "osd-reopened-"+strconv.Itoa(osd.ID), upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{2 - i, i + 1}))
	}
	u.finishUpgrade(t, ctx, identity)
}

// TestRollingUpgradeResumesAfterInterruptions upgrades the same Squid cluster
// to Tentacle with the upgrade stopped part-way in each daemon phase, and then
// resumed forward to the end. Two kinds of stop are exercised:
//
//   - A step that fails half-done: the next container of a MON, and of an OSD,
//     is started from an image that has no ceph-mon or ceph-osd (the Tentacle
//     client role image). The old container is already gone, so the daemon is
//     down with its store retained. The cluster keeps serving in the mixed
//     state: the MON quorum is the other two, and the OSD's placement groups
//     have the other two replicas. The step is then resumed from the handle the
//     failed call returned, on the Tentacle image, and the store must be the
//     one the daemon had: the monmap members and addresses are unchanged and
//     every acknowledged write
//     reads back.
//   - A daemon of the old release restarted inside the mixed interval: a Squid
//     MON joins a quorum led by Tentacle, and a Squid OSD reopens its store
//     while the MONs and MGR run Tentacle.
//
// Rollback is not exercised: a Tentacle store is never reopened by Squid.
func TestRollingUpgradeResumesAfterInterruptions(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		if !t.Run(mode.Name, func(t *testing.T) { interruptedUpgrade(t, mode.Host) }) {
			return
		}
	}
}

func interruptedUpgrade(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	from, _ := upgradeImages(t, upgradeFrom)
	to, toClient := upgradeImages(t, upgradeTo)
	u := startUpgradeCluster(t, ctx, host)
	cluster := u.cluster
	defer func() {
		t.Logf("INTERRUPTED_UPGRADE_RECEIPT %s", mustMarshal(map[string]any{"from": upgradeFrom, "to": upgradeTo, "host": host, "steps": u.steps}))
	}()
	identity := monmapMembers(t, ctx, cluster)
	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) != 3 {
		t.Fatalf("%d monitors: %v", len(monitors), err)
	}
	mon := map[string]*ceph.MonitorContainer{}
	for _, m := range monitors {
		mon[m.DaemonName] = m
	}
	replace := func(name, image, release string) {
		t.Helper()
		next, err := mon[name].Replace(ctx, ceph.DaemonOptions{}.WithImage(image))
		if err != nil {
			t.Fatalf("replace mon.%s: %v", name, err)
		}
		if v, err := next.Version(ctx); err != nil || v != release {
			t.Fatalf("mon.%s reports %q, want %s: %v", name, v, release, err)
		}
		if got := monmapMembers(t, ctx, cluster); got != identity {
			t.Fatalf("mon.%s replacement changed the monmap:\n%s\n%s", name, identity, got)
		}
		mon[name] = next
	}

	replace("a", to, upgradeTo)
	u.step(t, ctx, "mon-a-tentacle", upgradeLayout([2]int{2, 1}, [2]int{1, 0}, [2]int{3, 0}))
	// A Squid MON restarted while a Tentacle MON is in the quorum.
	replace("c", from, upgradeFrom)
	u.step(t, ctx, "mon-c-restarted-on-squid", upgradeLayout([2]int{2, 1}, [2]int{1, 0}, [2]int{3, 0}))

	// The MON phase stops half-done: mon.b's next container cannot run a MON.
	failed, err := mon["b"].Replace(ctx, ceph.DaemonOptions{}.WithImage(toClient))
	if err == nil {
		t.Fatal("mon.b started from an image without ceph-mon")
	}
	if failed == nil || failed.GetContainerID() == "" || failed.GetContainerID() == mon["b"].GetContainerID() {
		t.Fatalf("the interrupted replacement returned no new handle to resume from: %v", err)
	}
	t.Logf("interrupted mon.b replacement: %v", err)
	mon["b"] = failed
	// The surviving MONs drop mon.b from the quorum at their next election.
	// A read can time out while they elect, on a bridge where mon.b's address
	// no longer answers, so a failed read is retried within the bound.
	var quorum []string
	var quorumErr error
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(3 * time.Second) {
		q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
		quorum, quorumErr = q.QuorumNames, err
		if (err == nil && slices.Equal(quorum, []string{"a", "c"})) || time.Now().After(deadline) {
			break
		}
	}
	if quorumErr != nil || !slices.Equal(quorum, []string{"a", "c"}) {
		t.Fatalf("quorum while mon.b is down %v: %v", quorum, quorumErr)
	}
	if got := monmapMembers(t, ctx, cluster); got != identity {
		t.Fatalf("the interrupted replacement changed the monmap:\n%s\n%s", identity, got)
	}
	u.write(t, ctx, "mon-b-interrupted", nil)

	// Resume forward: mon.b on Tentacle from the handle of the failed step.
	replace("b", to, upgradeTo)
	u.step(t, ctx, "mon-b-resumed", upgradeLayout([2]int{1, 2}, [2]int{1, 0}, [2]int{3, 0}))
	replace("c", to, upgradeTo)
	u.step(t, ctx, "mon-c-tentacle", upgradeLayout([2]int{0, 3}, [2]int{1, 0}, [2]int{3, 0}))
	if release := u.minMonRelease(t, ctx); release != "tentacle" {
		t.Fatalf("min_mon_release_name %q after the MON upgrade", release)
	}
	u.upgradeManager(t, ctx, to, [2]int{0, 3})

	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) != 3 {
		t.Fatalf("%d OSDs: %v", len(osds), err)
	}
	// A Squid OSD reopened under Tentacle MONs and MGR.
	osds[2] = u.reopenOSD(t, ctx, osds[2], from, upgradeFrom)
	u.step(t, ctx, "osd-2-restarted-on-squid", upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{3, 0}))
	osds[0] = u.reopenOSD(t, ctx, osds[0], to, upgradeTo)
	u.step(t, ctx, "osd-0-tentacle", upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{2, 1}))

	// The OSD phase stops half-done: osd.1's next container cannot run an OSD.
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("before osd.1:", err)
	}
	detached, err := osds[1].Detach(ctx)
	if err != nil {
		t.Fatal("detach osd.1:", err)
	}
	broken, err := detached.Reopen(ctx, ceph.OSDOptions{}.WithImage(toClient))
	if err == nil {
		t.Fatal("osd.1 started from an image without ceph-osd")
	}
	if broken == nil || broken.GetContainerID() == "" {
		t.Fatalf("the interrupted reopen returned no handle to resume from: %v", err)
	}
	t.Logf("interrupted osd.1 reopen: %v", err)
	u.write(t, ctx, "osd-1-interrupted", nil)

	// Resume forward: detach the failed container and reopen the store on
	// Tentacle.
	osds[1] = u.reopenOn(t, ctx, broken, to, upgradeTo)
	u.step(t, ctx, "osd-1-resumed", upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{1, 2}))
	osds[2] = u.reopenOSD(t, ctx, osds[2], to, upgradeTo)
	u.step(t, ctx, "osd-2-tentacle", upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{0, 3}))
	u.finishUpgrade(t, ctx, identity)
}

// upgradeManager adds a Tentacle manager and removes the Squid one, with the
// MONs at mon.
func (u *upgradeRun) upgradeManager(t *testing.T, ctx context.Context, to string, mon [2]int) {
	t.Helper()
	managers, err := u.cluster.Managers(ctx)
	if err != nil || len(managers) != 1 {
		t.Fatalf("%d managers: %v", len(managers), err)
	}
	mgr, err := u.cluster.AddManager(ctx, "u0", ceph.DaemonOptions{}.WithImage(to))
	if err != nil {
		t.Fatal("add Tentacle mgr:", err)
	}
	if v, err := mgr.Version(ctx); err != nil || v != upgradeTo {
		t.Fatalf("mgr.u0 reports %q: %v", v, err)
	}
	u.step(t, ctx, "mgr-added", upgradeLayout(mon, [2]int{1, 1}, [2]int{3, 0}))
	if err := managers[0].Remove(ctx); err != nil {
		t.Fatal("remove Squid mgr:", err)
	}
	u.step(t, ctx, "mgr-removed", upgradeLayout(mon, [2]int{0, 1}, [2]int{3, 0}))
}

// reopenOSD detaches osd once the cluster is clean and reopens its retained
// store from image, which must report release.
func (u *upgradeRun) reopenOSD(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, image, release string) *ceph.OSDContainer {
	t.Helper()
	if err := u.cluster.WaitForClean(ctx); err != nil {
		t.Fatalf("before osd.%d: %v", osd.ID, err)
	}
	return u.reopenOn(t, ctx, osd, image, release)
}

// reopenOn detaches osd, which may be down, and reopens its retained store
// from image, which must report release.
func (u *upgradeRun) reopenOn(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, image, release string) *ceph.OSDContainer {
	t.Helper()
	detached, err := osd.Detach(ctx)
	if err != nil {
		t.Fatalf("detach osd.%d: %v", osd.ID, err)
	}
	reopened, err := detached.Reopen(ctx, ceph.OSDOptions{}.WithImage(image))
	if err != nil {
		t.Fatalf("reopen osd.%d on %s: %v", osd.ID, release, err)
	}
	if v, err := reopened.Version(ctx); err != nil || v != release {
		t.Fatalf("osd.%d reports %q, want %s: %v", osd.ID, v, release, err)
	}
	return reopened
}

// finishUpgrade requires Tentacle OSDs from now on, unsets noout, waits for a
// clean cluster and checks the final layout, the unchanged monmap and the
// fixture's recorded release.
func (u *upgradeRun) finishUpgrade(t *testing.T, ctx context.Context, identity string) {
	t.Helper()
	if _, err := u.cluster.Ceph(ctx, "osd", "require-osd-release", "tentacle"); err != nil {
		t.Fatal(err)
	}
	if _, err := u.cluster.Ceph(ctx, "osd", "unset", "noout"); err != nil {
		t.Fatal(err)
	}
	if err := u.cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	dump, err := u.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var osdMap struct {
		RequireOSDRelease string `json:"require_osd_release"`
	}
	if err := json.Unmarshal(dump, &osdMap); err != nil || osdMap.RequireOSDRelease != "tentacle" {
		t.Fatalf("require_osd_release %q: %v", osdMap.RequireOSDRelease, err)
	}
	u.step(t, ctx, "finished", upgradeLayout([2]int{0, 3}, [2]int{0, 1}, [2]int{0, 3}))
	if got := monmapMembers(t, ctx, u.cluster); got != identity {
		t.Fatalf("the upgrade changed the monmap:\n%s\n%s", identity, got)
	}
	if facts, err := ceph.FactsOf(u.cluster); err != nil || facts.CephVersion != upgradeFrom {
		t.Fatalf("the fixture's recorded release changed to %q: %v", facts.CephVersion, err)
	}
}

// monmapMembers is the part of the monmap an upgrade must not change: the FSID
// and every member's name and address vector, in rank order. The epoch is left
// out: the MONs commit a new monmap once all of them run the new release
// (min_mon_release).
func monmapMembers(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	_, members := monmapParts(t, ctx, cluster)
	return members
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
