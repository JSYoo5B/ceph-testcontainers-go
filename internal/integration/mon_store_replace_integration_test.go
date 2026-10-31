//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mon_store_replace))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// monStoreMount is the volume a MON container mounts at its data directory.
func monStoreMount(t *testing.T, ctx context.Context, docker *mobycl.Client, mon *ceph.MonitorContainer) string {
	t.Helper()
	inspect, err := docker.ContainerInspect(ctx, mon.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range inspect.Container.Mounts {
		if m.Destination == "/var/lib/ceph/mon/ceph-"+mon.DaemonName {
			if string(m.Type) != "volume" || m.Name == "" || !m.RW {
				t.Fatalf("mon.%s store mount %+v", mon.DaemonName, m)
			}
			return m.Name
		}
	}
	t.Fatalf("mon.%s has no store mount", mon.DaemonName)
	return ""
}

// TestMonitorStoreReplace replaces every MON of a three-MON cluster, MON a
// included, on its retained store with the same image. Each replacement must
// keep the monmap exactly (epoch, members, address vectors), mount the same
// volume in a new container, rejoin the quorum, and leave every acknowledged
// object readable; new writes are acknowledged after each one. Removing a
// replaced MON then releases its store: MON c added again gets a new volume,
// and the removed MON a is no longer listed. Before that, a
// replacement on a one-MON cluster is refused without touching the MON, and a
// cluster without MON volume storage refuses Replace. On the host network the
// MON address includes its ports, which each replacement must keep.
func TestMonitorStoreReplace(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		if !t.Run(mode.Name, func(t *testing.T) { monitorStoreReplace(t, mode.Host) }) {
			return
		}
	}
}

func monitorStoreReplace(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := harness.IntegrationImages(t)
	run := func(monitors int, volume bool) *ceph.Container {
		t.Helper()
		cfg := append(slices.Clone(opts), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(monitors).WithOSDCount(1).WithPoolDefaults(1, 1)))
		if volume {
			cfg = append(cfg, ceph.WithMonitorStorage(ceph.MonitorStorage{}.WithVolume()))
		}
		if host {
			cfg = append(cfg, ceph.WithHostNetwork())
		}
		cluster, err := ceph.Run(ctx, image, cfg...)
		if cluster != nil {
			t.Cleanup(func() {
				cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
				defer done()
				if err := cluster.Terminate(cleanup); err != nil {
					t.Error("MON store replace cleanup", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		return cluster
	}

	// A single MON cannot be replaced: no quorum would remain.
	single := run(1, true)
	mons, err := single.Monitors(ctx)
	if err != nil || len(mons) != 1 {
		t.Fatalf("%d monitors: %v", len(mons), err)
	}
	before := mons[0].GetContainerID()
	if _, err := mons[0].Replace(ctx); err == nil {
		t.Fatal("the only MON was replaced")
	}
	if state, err := mons[0].State(ctx); err != nil || !state.Running || mons[0].GetContainerID() != before {
		t.Fatalf("a refused replacement touched the MON: %+v %v", state, err)
	}
	if err := single.Terminate(ctx); err != nil {
		t.Fatal(err)
	}

	cluster := run(3, true)
	if _, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: upgradePool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	u := &upgradeRun{cluster: cluster, client: harness.ToolkitContainer(t, cluster)}
	// The replacement keeps every daemon's release: the layout before is the
	// checkpoint of every later step.
	same := upgradeCheckpoint(u.versions(t, ctx))
	if len(same["mon"]) != 1 || len(same["mgr"]) != 1 || len(same["osd"]) != 1 {
		t.Fatalf("inventory before the replacements %v", same)
	}
	u.step(t, ctx, "before", same)
	identity := monmapIdentity(t, ctx, cluster)
	monitors, err := cluster.Monitors(ctx)
	if err != nil || len(monitors) != 3 {
		t.Fatalf("%d monitors: %v", len(monitors), err)
	}
	receipt := []map[string]any{}
	defer func() { t.Logf("MON_STORE_REPLACE_RECEIPT %s", mustMarshal(receipt)) }()
	for _, old := range monitors {
		volume := monStoreMount(t, ctx, docker.Client, old)
		replaced, err := old.Replace(ctx)
		if err != nil {
			t.Fatalf("replace mon.%s: %v", old.DaemonName, err)
		}
		if replaced.GetContainerID() == old.GetContainerID() || replaced.DaemonName != old.DaemonName {
			t.Fatalf("mon.%s was not moved to a new container", old.DaemonName)
		}
		if got := monStoreMount(t, ctx, docker.Client, replaced); got != volume {
			t.Fatalf("mon.%s mounts %s after the replacement, not %s", old.DaemonName, got, volume)
		}
		if _, err := old.Replace(ctx); err == nil {
			t.Fatalf("the stale handle of mon.%s was accepted", old.DaemonName)
		}
		if got := monmapIdentity(t, ctx, cluster); got != identity {
			t.Fatalf("mon.%s replacement changed the monmap:\n%s\n%s", old.DaemonName, identity, got)
		}
		u.step(t, ctx, "replaced-"+old.DaemonName, same)
		receipt = append(receipt, map[string]any{"mon": old.DaemonName, "volume": volume, "old": old.GetContainerID()[:12], "new": replaced.GetContainerID()[:12]})
	}
	q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil || len(q.QuorumNames) != 3 {
		t.Fatalf("quorum %v: %v", q.QuorumNames, err)
	}
	t.Logf("MON_STORE_REPLACE_MONMAP host=%v %s", host, identity)

	// A replaced MON leaves for good with Remove: its store is released, and
	// the same name added again gets a new store.
	current := map[string]*ceph.MonitorContainer{}
	if monitors, err = cluster.Monitors(ctx); err != nil {
		t.Fatal(err)
	}
	for _, mon := range monitors {
		current[mon.DaemonName] = mon
	}
	oldC := monStoreMount(t, ctx, docker.Client, current["c"])
	if err := current["c"].Remove(ctx); err != nil {
		t.Fatal("remove the replaced mon.c:", err)
	}
	readded, err := cluster.AddMonitor(ctx, "c")
	if err != nil {
		t.Fatal("add mon.c again:", err)
	}
	if got := monStoreMount(t, ctx, docker.Client, readded); got == oldC {
		t.Fatalf("the re-added mon.c mounts the removed member's store %s", got)
	}
	if err := current["a"].Remove(ctx); err != nil {
		t.Fatal("remove the replaced mon.a:", err)
	}
	if monitors, err = cluster.Monitors(ctx); err != nil || len(monitors) != 2 || slices.ContainsFunc(monitors, func(m *ceph.MonitorContainer) bool { return m.DaemonName == "a" }) {
		t.Fatalf("monitors after removing the replaced a: %v %v", monitors, err)
	}
	u.step(t, ctx, "removed-and-readded", upgradeCheckpoint{"mon": map[string]int{onlyRelease(same["mon"]): 2}, "mgr": same["mgr"], "osd": same["osd"]})

	// Without the volume the call is refused.
	plain := run(1, false)
	mons, err = plain.Monitors(ctx)
	if err != nil || len(mons) != 1 {
		t.Fatalf("%d monitors: %v", len(mons), err)
	}
	if _, err := mons[0].Replace(ctx); err == nil {
		t.Fatal("Replace without MonitorStorage.WithVolume was accepted")
	}
}

// onlyRelease is the one release of a single-release role.
func onlyRelease(counts map[string]int) string {
	for release := range counts {
		return release
	}
	return ""
}
