//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90

package integration_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// One fresh bridge fixture verifies opt-in storage ownership, real replicated
// bytes and recovery. Sampling observes this run; it is not a disk comparison.
func TestOSDInMemoryStorageTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	engine, err := docker.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || engine.Info.ID == "" {
		t.Fatal("native engine identity unavailable", err)
	}
	beforeVolumes, beforeKeepers := osdMemoryLabelInventory(t, ctx, docker.Client)
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDInMemoryStorage(osdMemoryBytes))
	started := time.Now()
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	owned := map[string]bool{}
	volumeName := ""
	keeperID := ""
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if t.Failed() {
				if keeperID != "" {
					osdMemoryCapacity(t, cleanup, docker.Client, keeperID, "failure")
				}
				for _, osd := range cluster.OSDs() {
					if osd.Container == nil {
						continue
					}
					osdMemoryFailureLogs(t, cleanup, osd.Container)
				}
			}
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("partial memory-storage cluster cleanup failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("memory-storage control bootstrap failed", err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fsid := q.MonMap.FSID
	if id, err := uuid.Parse(fsid); err != nil || id == uuid.Nil || id.String() != fsid {
		t.Fatal("original native FSID unavailable")
	}
	afterVolumes, afterKeepers := osdMemoryLabelInventory(t, ctx, docker.Client)
	if !slices.Equal(beforeVolumes, afterVolumes) || !slices.Equal(beforeKeepers, afterKeepers) || len(cluster.OSDs()) != 0 || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
		t.Fatal("zero-OSD bootstrap allocated memory storage or registered an OSD")
	}
	t.Logf("OSD_MEMORY_LAZY fsid=%s new_volumes=0 new_keepers=0", fsid)
	for range 2 {
		osd, err := cluster.AddOSD(ctx)
		if osd != nil && osd.Container != nil {
			owned[osd.GetContainerID()] = true
		}
		if err != nil {
			t.Fatal("provision initial memory OSD", err)
		}
	}
	initial := cluster.OSDs()
	if len(initial) != 2 {
		t.Fatal("exact two-OSD ownership unavailable")
	}
	volumeName, keeper := osdMemoryBacking(t, ctx, docker.Client, cluster, initial, image)
	keeperID = keeper.ID
	owned[keeper.ID] = true
	for _, ctr := range append([]testcontainers.Container{cluster.Container}, osdContainers(cluster)...) {
		owned[ctr.GetContainerID()] = true
	}
	for _, manager := range cluster.Managers() {
		owned[manager.GetContainerID()] = true
	}
	network, err := docker.Client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
	if err != nil || network.Network.ID == "" {
		t.Fatal("owned bridge identity unavailable", err)
	}
	stats := &osdMemoryStats{docker: docker.Client}
	defer stats.log(t)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "memory-retained", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil || pool == nil {
		t.Fatal("replicated memory pool setup failed", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	poolState, err := cluster.PoolStatus(ctx, pool.Name)
	if err != nil || poolState.ID < 0 || poolState.Size != 2 || poolState.MinSize != 1 {
		t.Fatal("original replicated pool identity unavailable", err)
	}
	identities := osdMemoryIdentities(t, ctx, cluster)
	for _, osd := range initial {
		osdMemoryDirectory(t, ctx, osd, fsid, identities[osd.ID])
	}
	osdMemoryPhase(t, "bootstrap", started)
	started = time.Now()
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "seed")
	osdMemoryCapacity(t, ctx, docker.Client, keeper.ID, "seed")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "seed-128MiB", started)
	started = time.Now()
	noout, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	for _, osd := range initial {
		if err := osd.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
	}
	osdMemoryWaitState(t, ctx, cluster, false)
	for _, osd := range initial {
		raw := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
		if raw.State.Running {
			t.Fatal("original OSD did not stop")
		}
	}
	stoppedKeeper := osdMemoryInspect(t, ctx, docker.Client, keeper.ID)
	if !stoppedKeeper.State.Running || stoppedKeeper.State.StartedAt != keeper.State.StartedAt || !maps.Equal(osdMemoryIdentities(t, ctx, cluster), identities) {
		t.Fatal("stopping every OSD changed its registration or original running keeper")
	}
	osdMemoryVolume(t, ctx, docker.Client, volumeName, keeper.Config.Labels[osdMemoryLabel])
	for _, osd := range initial {
		cid := osd.GetContainerID()
		if err := osd.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if osd.GetContainerID() != cid {
			t.Fatal("restart replaced an OSD container")
		}
	}
	osdMemoryWaitState(t, ctx, cluster, true)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("original memory OSD recovery failed", err)
	}
	if !maps.Equal(osdMemoryIdentities(t, ctx, cluster), identities) {
		t.Fatal("restart replaced an original native OSD UUID")
	}
	for _, osd := range initial {
		osdMemoryDirectory(t, ctx, osd, fsid, identities[osd.ID])
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "both-osds-restarted")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "stop-all-start-same", started)
	started = time.Now()
	added, err := cluster.AddOSD(ctx)
	if added != nil && added.Container != nil {
		owned[added.GetContainerID()] = true
	}
	if err != nil || added == nil {
		t.Fatal("third memory OSD setup failed", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("third OSD recovery failed", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, -1)
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "third-osd-backfill")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "add-third-backfill", started)
	started = time.Now()
	original := initial[0]
	oldCID, oldUUID := original.GetContainerID(), identities[original.ID]
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal("drain original memory OSD failed", err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldCID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("drained original container remains", err)
	}
	osdMemoryExec(t, ctx, added, "test", "!", "-e", "/var/lib/ceph/osd/ceph-"+strconv.Itoa(original.ID))
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("post-drain recovery failed", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, original.ID)
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "original-drained")
	replacement, err := cluster.AddOSD(ctx)
	if replacement != nil && replacement.Container != nil {
		owned[replacement.GetContainerID()] = true
	}
	if err != nil || replacement == nil {
		t.Fatal("fresh replacement memory OSD failed", err)
	}
	current := osdMemoryIdentities(t, ctx, cluster)
	if replacement.GetContainerID() == oldCID || current[replacement.ID] == "" || current[replacement.ID] == oldUUID {
		t.Fatal("replacement reused old container or native UUID")
	}
	osdMemoryDirectory(t, ctx, replacement, fsid, current[replacement.ID])
	osdMemoryBacking(t, ctx, docker.Client, cluster, cluster.OSDs(), image)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("replacement recovery failed", err)
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "fresh-replacement")
	stats.sample(ctx, owned)
	t.Logf("OSD_MEMORY_REPLACEMENT old_id=%d new_id=%d reused_numeric_id=%t old_uuid=%s new_uuid=%s old_cid=%s new_cid=%s old_directory_absent_before_add=true", original.ID, replacement.ID, original.ID == replacement.ID, oldUUID, current[replacement.ID], oldCID, replacement.GetContainerID())
	osdMemoryPhase(t, "drain-and-fresh-replacement", started)
	started = time.Now()
	beforeRefusal := noInitialOSDNativeDump(t, ctx, cluster)
	stopSeconds := 1
	if _, err := docker.Client.ContainerStop(ctx, keeper.ID, mobycl.ContainerStopOptions{Timeout: &stopSeconds}); err != nil {
		t.Fatal("stop original keeper failed", err)
	}
	refusalCtx, refusalCancel := context.WithTimeout(ctx, 30*time.Second)
	refused, refusalErr := cluster.AddOSD(refusalCtx)
	contextErr := refusalCtx.Err()
	refusalCancel()
	if refused != nil || refusalErr == nil || contextErr != nil || errors.Is(refusalErr, context.DeadlineExceeded) || refusalErr.Error() != "OSD memory keeper is not running; stored data may be lost" {
		t.Fatal("lost keeper was not a completed pre-registration refusal", refusalErr, contextErr)
	}
	if !slices.Equal(beforeRefusal, noInitialOSDNativeDump(t, ctx, cluster)) || !maps.Equal(current, osdMemoryIdentities(t, ctx, cluster)) || len(cluster.OSDs()) != 3 {
		t.Fatal("lost-keeper refusal changed native OSD IDs/UUIDs or ownership")
	}
	afterRefusal := osdMemoryInspect(t, ctx, docker.Client, keeper.ID)
	if afterRefusal.State.Running || afterRefusal.State.StartedAt != keeper.State.StartedAt {
		t.Fatal("lost keeper was automatically restarted or replaced")
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "keeper-stopped-refusal")
	t.Logf("OSD_MEMORY_KEEPER_REFUSAL keeper_cid=%s registered_before=3 registered_after=3 automatic_restart=false", keeper.ID)
	osdMemoryPhase(t, "lost-keeper-refusal", started)
	started = time.Now()
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal("whole memory-storage cleanup failed", err)
	}
	terminated = true
	cleanupVerified := true
	for cid := range owned {
		if _, err := docker.Client.ContainerInspect(cleanup, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			cleanupVerified = false
			t.Error("owned memory fixture container remains", cid, err)
		}
	}
	if _, err := docker.Client.VolumeInspect(cleanup, volumeName, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		cleanupVerified = false
		t.Error("owned tmpfs named volume remains", err)
	}
	if _, err := docker.Client.NetworkInspect(cleanup, network.Network.ID, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
		cleanupVerified = false
		t.Error("owned memory fixture network remains", err)
	}
	finalEngine, err := docker.Client.Info(cleanup, mobycl.InfoOptions{})
	if err != nil || finalEngine.Info.ID != engine.Info.ID {
		cleanupVerified = false
		t.Error("cleanup engine identity changed", err)
	}
	if cleanupVerified {
		t.Logf("OSD_MEMORY_CLEANUP engine=%s volume=%s owned_containers=%d owned_networks=1 remaining=0", engine.Info.ID, volumeName, len(owned))
	}
	osdMemoryPhase(t, "terminate", started)
}
