//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_retained_reopen))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDRetainedReopen detaches the only OSD of a single-replica, one-PG
// pool and reopens its retained store in a different container. No other
// replica exists, so the known bytes read afterwards can come only from the
// retained store, not from backfill.
func TestOSDRetainedReopen(t *testing.T) {
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
				t.Error("retained reopen fixture cleanup", err)
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
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "retained-data", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
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
	registered := osdMemoryIdentities(t, ctx, cluster)
	oldContainer := original.GetContainerID()
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}

	detached, err := cluster.DetachOSD(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detached.ID() != original.ID || detached.UUID() != registered[original.ID] || detached.ClusterFSID() != quorum.MonMap.FSID {
		t.Fatalf("detached identity osd.%d %s %s, registered %v fsid %s", detached.ID(), detached.UUID(), detached.ClusterFSID(), registered, quorum.MonMap.FSID)
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldContainer, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("detached container remains", err)
	}
	if len(cluster.OSDs()) != 0 {
		t.Fatal("a detached OSD is still listed")
	}
	native := noInitialOSDNativeDump(t, ctx, cluster)
	if len(native) != 1 || native[0].ID != original.ID || native[0].UUID != detached.UUID() || native[0].Up != 0 || native[0].In != 1 {
		t.Fatalf("detached registration %+v", native)
	}
	if err := cluster.RemoveOSD(ctx, original.ID); err == nil {
		t.Fatal("RemoveOSD purged a detached OSD")
	}

	reopened, err := cluster.ReopenOSD(ctx, detached)
	if err != nil {
		t.Fatal(err)
	}
	if reopened == original || reopened.ID != original.ID || reopened.GetContainerID() == oldContainer || original.GetContainerID() != oldContainer {
		t.Fatalf("reopen handle osd.%d %s, previous %s", reopened.ID, reopened.GetContainerID(), original.GetContainerID())
	}
	if again, err := cluster.ReopenOSD(ctx, detached); err == nil || again != nil {
		t.Fatal("a detached OSD was reopened twice")
	}
	if current := osdMemoryIdentities(t, ctx, cluster); len(current) != 1 || current[original.ID] != registered[original.ID] {
		t.Fatalf("reopened registration %v, detached %v", current, registered)
	}
	if name, _ := osdVolumeBacking(t, ctx, docker.Client, cluster); name != volume {
		t.Fatalf("reopened OSD mounts %s, not the retained volume %s", name, volume)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "reopened")

	newContainer := reopened.GetContainerID()
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	terminated = true
	for _, cid := range []string{oldContainer, newContainer} {
		if _, err := docker.Client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Error("owned OSD container remains", err)
		}
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Error("owned volume remains", err)
	}
	t.Logf("OSD_RETAINED_REOPEN osd=%d uuid=%s same_fsid=true container_changed=true single_replica=true bytes=1048583 remaining=0", original.ID, detached.UUID())
}
