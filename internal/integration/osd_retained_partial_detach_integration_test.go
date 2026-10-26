//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_retained_partial_detach))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDRetainedPartialDetach exercises an actual post-removal store-admission
// failure. Only the ready marker is temporarily renamed; no OSD data or daemon
// identity is changed, and no runtime response is replaced with a fake error.
func TestOSDRetainedPartialDetach(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	original, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldRaw := original.Container
	oldID := oldRaw.GetContainerID()
	oldInspect := osdMemoryInspect(t, ctx, docker.Client, oldID)
	owner := oldInspect.Config.Labels["org.testcontainers.ceph.cluster"]
	if owner == "" {
		t.Fatal("missing cluster ownership")
	}
	// Count actual calls without changing arguments, context, error or result.
	recorder := &partialDetachContainer{Container: oldRaw}
	original.Container = recorder
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "retained-partial", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "seed")
	volume, _ := osdVolumeBacking(t, ctx, docker.Client, cluster)
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	identities := osdMemoryIdentities(t, ctx, cluster)
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: image, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) { h.NetworkMode = "none" },
		Mounts:             testcontainers.ContainerMounts{{Source: testcontainers.DockerVolumeMountSource{Name: volume, VolumeOptions: &mount.VolumeOptions{NoCopy: true, Labels: maps.Clone(testcontainers.GenericLabels())}}, Target: "/observed"}},
	}, Started: true})
	observerRemoved := false
	if observer != nil {
		t.Cleanup(func() {
			if observerRemoved {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := observer.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	store := "/observed/ceph-" + strconv.Itoa(original.ID)
	observerCommands := &partialDetachObserver{container: observer}
	marker := partialDetachReady(t, ctx, observerCommands, store, "inspect")
	markerMayBeHidden := true
	// Register restoration before dispatching the rename. Even a lost observer
	// reply or an assertion failure restores first, before observer/owner cleanup.
	t.Cleanup(func() {
		if !markerMayBeHidden {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if settled := partialDetachReady(t, cleanup, observerCommands, store, "settle"); settled != marker {
			t.Error("cleanup restored a changed ready marker", settled, marker)
		}
	})
	if hidden := partialDetachReady(t, ctx, observerCommands, store, "hide"); hidden != marker {
		t.Fatal("rename receipt differs from original marker")
	}
	before := partialDetachClock(t, ctx, docker.Client)
	detached, err := original.Detach(ctx)
	if err == nil || detached != nil || !strings.Contains(err.Error(), "store lacks a regular ready") {
		t.Fatalf("post-removal marker refusal: detached=%v err=%v", detached != nil, err)
	}
	afterFailure := partialDetachClock(t, ctx, docker.Client)
	if _, err := docker.Client.ContainerInspect(ctx, oldID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("old instance was not removed", err)
	}
	owned := clusteraccess.OSDs(t, cluster)
	if len(owned) != 1 || owned[0] != original || original.GetContainerID() != oldID {
		t.Fatal("partial detach discarded its owned descriptor")
	}
	if recorder.stops.Load() != 1 || recorder.terminations.Load() != 1 {
		t.Fatal("initial native removal call counts differ")
	}
	native := noInitialOSDNativeDump(t, ctx, cluster)
	if len(native) != 1 || native[0].ID != original.ID || native[0].UUID != identities[original.ID] || native[0].Up != 0 || native[0].In != 1 {
		t.Fatal("partial detach changed registration", native)
	}
	if err := original.Remove(ctx); err == nil || !strings.Contains(err.Error(), "being detached") {
		t.Fatal("partial ownership did not refuse purge", err)
	}
	if !slices.Equal(native, noInitialOSDNativeDump(t, ctx, cluster)) {
		t.Fatal("purge refusal changed registration")
	}
	actions := partialDetachEvents(t, ctx, docker.Client, owner, before, afterFailure)
	if len(actions) != 2 || actions["stop/"+oldID] != 1 || actions["destroy/"+oldID] != 1 {
		t.Fatal("failed-detach lifecycle events differ", actions)
	}
	t.Logf("OSD_RETAINED_PARTIAL_FAILURE osd=%d old=%s removed=true detached_handle=false ownership=retained registration=down_in purge=refused stop_calls=%d terminate_calls=%d", original.ID, oldID, recorder.stops.Load(), recorder.terminations.Load())
	restored := partialDetachReady(t, ctx, observerCommands, store, "restore")
	if restored != marker {
		t.Fatalf("ready marker identity or bytes changed: before=%+v after=%+v", marker, restored)
	}
	markerMayBeHidden = false
	beforeRetry := partialDetachClock(t, ctx, docker.Client)
	detached, err = original.Detach(ctx)
	if err != nil {
		t.Fatal("settle known post-removal failure", err)
	}
	afterRetry := partialDetachClock(t, ctx, docker.Client)
	if recorder.stops.Load() != 1 || recorder.terminations.Load() != 1 {
		t.Fatal("settlement replayed a destructive runtime call")
	}
	if actions := partialDetachEvents(t, ctx, docker.Client, owner, beforeRetry, afterRetry); len(actions) != 0 {
		t.Fatal("settlement changed runtime instances", actions)
	}
	if len(clusteraccess.OSDs(t, cluster)) != 0 || detached.ID() != original.ID || detached.UUID() != identities[original.ID] || detached.ClusterFSID() != quorum.MonMap.FSID {
		t.Fatal("settled detached identity differs")
	}
	if !slices.Equal(native, noInitialOSDNativeDump(t, ctx, cluster)) {
		t.Fatal("settlement changed registration")
	}
	t.Logf("OSD_RETAINED_PARTIAL_SETTLEMENT osd=%d detached_handle=true marker_inode_bytes=restored stop_calls=%d terminate_calls=%d lifecycle_events=0", original.ID, recorder.stops.Load(), recorder.terminations.Load())
	if err := observer.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	observerRemoved = true
	beforeReopen := partialDetachClock(t, ctx, docker.Client)
	reopened, err := detached.Reopen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newID := reopened.GetContainerID()
	if reopened == original || newID == oldID || original.GetContainerID() != oldID {
		t.Fatal("replacement generation differs")
	}
	actions = partialDetachEvents(t, ctx, docker.Client, owner, beforeReopen, partialDetachClock(t, ctx, docker.Client))
	if len(actions) != 1 || actions["create/"+newID] != 1 {
		t.Fatal("reopen creation events differ", actions)
	}
	if current := osdMemoryIdentities(t, ctx, cluster); len(current) != 1 || current[original.ID] != identities[original.ID] {
		t.Fatal("reopen registration differs", current)
	}
	if current, _ := osdVolumeBacking(t, ctx, docker.Client, cluster); current != volume {
		t.Fatal("reopened volume differs")
	}
	currentQuorum, err := cluster.QuorumStatus(ctx)
	if err != nil || currentQuorum.MonMap.FSID != quorum.MonMap.FSID {
		t.Fatal("reopened FSID differs", err)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "partial-detach-reopened")
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	terminated = true
	for _, id := range []string{oldID, newID} {
		if _, err := docker.Client.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned OSD instance remains", err)
		}
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("owned volume remains", err)
	}
	t.Logf("OSD_RETAINED_PARTIAL_DETACH osd=%d uuid=%s post_removal_store_refusal=true ownership=retained purge=refused marker_inode_bytes=restored stop_calls=1 terminate_calls=1 retry_lifecycle_events=0 replacement_create_events=1 single_replica=true bytes=1048583 same_fsid=true cleanup=confirmed", original.ID, detached.UUID())
}
