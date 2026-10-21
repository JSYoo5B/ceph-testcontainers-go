//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_volume_storage))))

//ci: timeout=30m job-timeout=40

package integration_test

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// This exercises the owned backend, not a public retained detach/reopen API.
// Recovery deliberately restarts the same Docker containers; replicated data
// is not evidence for recovery into replacement containers without backfill.
func TestOSDVolumeStorageTopology(t *testing.T) {
	for _, mode := range networkModes(t, "bridge", "host") {
		if !t.Run(mode.Name, func(t *testing.T) { osdVolumeTopology(t, mode.Host) }) {
			return
		}
	}
}

func osdVolumeTopology(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	beforeVolumes, beforeHelpers := osdVolumeInventory(t, ctx, docker.Client)
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
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
				t.Error("owned volume fixture cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	afterVolumes, afterHelpers := osdVolumeInventory(t, ctx, docker.Client)
	if !slices.Equal(beforeVolumes, afterVolumes) || !slices.Equal(beforeHelpers, afterHelpers) || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
		t.Fatal("zero-OSD bootstrap allocated storage")
	}
	for range 2 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	name, helper := osdVolumeBacking(t, ctx, docker.Client, cluster)
	volume, err := docker.Client.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owned := []string{cluster.GetContainerID(), helper.ID}
	for _, m := range cluster.Managers() {
		owned = append(owned, m.GetContainerID())
	}
	for _, osd := range cluster.OSDs() {
		owned = append(owned, osd.GetContainerID())
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "volume-data", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "seed")
	original := cluster.OSDs()[0]
	oldUUID := osdMemoryIdentities(t, ctx, cluster)[original.ID]
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, added.GetContainerID())
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, original.GetContainerID(), mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("purged container remains", err)
	}
	osdMemoryExec(t, ctx, added, "test", "!", "-e", "/var/lib/ceph/osd/ceph-"+strconv.Itoa(original.ID))
	replacement, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owned = append(owned, replacement.GetContainerID())
	current := osdMemoryIdentities(t, ctx, cluster)
	if replacement.GetContainerID() == original.GetContainerID() || current[replacement.ID] == oldUUID {
		t.Fatal("fresh OSD reused purged identity")
	}
	osdVolumeBacking(t, ctx, docker.Client, cluster)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "fresh-after-purge")
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	cids := map[int]string{}
	for _, osd := range cluster.OSDs() {
		cids[osd.ID] = osd.GetContainerID()
		if err := osd.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
	}
	osdMemoryWaitState(t, ctx, cluster, false)
	seconds := 1
	if _, err := docker.Client.ContainerStop(ctx, helper.ID, mobycl.ContainerStopOptions{Timeout: &seconds}); err != nil {
		t.Fatal(err)
	}
	for _, cid := range append(slices.Collect(maps.Values(cids)), helper.ID) {
		if osdMemoryInspect(t, ctx, docker.Client, cid).State.Running {
			t.Fatal("volume still has a running owned mount user")
		}
	}
	// All original mount users are stopped. A separately owned read-only observer
	// mounts the ordinary volume without copying the image's directories into it.
	observer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: image, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) { h.NetworkMode = "none" },
		Mounts:             testcontainers.ContainerMounts{{Source: testcontainers.DockerVolumeMountSource{Name: name, VolumeOptions: &mount.VolumeOptions{NoCopy: true}}, Target: "/observed", ReadOnly: true}},
	}, Started: true})
	if observer != nil {
		t.Cleanup(func() {
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
	for id, nativeUUID := range current {
		data := osdMemoryExec(t, ctx, observer, "python3", "-c", `import pathlib,sys
p=pathlib.Path('/observed/ceph-'+sys.argv[1])
assert (p/'ready').is_file()
assert (p/'fsid').read_text().strip()==sys.argv[2]
assert (p/'ceph_fsid').read_text().strip()==sys.argv[3]
assert (p/'block').stat().st_size>0
print('ready')`, strconv.Itoa(id), nativeUUID, q.MonMap.FSID)
		if string(data) != "ready\n" {
			t.Fatal("unmounted directory identity differs")
		}
	}
	for _, osd := range cluster.OSDs() {
		if err := osd.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if osd.GetContainerID() != cids[osd.ID] {
			t.Fatal("restart changed container")
		}
	}
	osdMemoryWaitState(t, ctx, cluster, true)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(current, osdMemoryIdentities(t, ctx, cluster)) {
		t.Fatal("restart changed registration")
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "all-stopped-helper-stopped")
	beforeRefusal := noInitialOSDNativeDump(t, ctx, cluster)
	if refused, err := cluster.AddOSD(ctx); err == nil || refused != nil {
		t.Fatal("stopped helper admitted new storage operation", err)
	}
	if !slices.Equal(beforeRefusal, noInitialOSDNativeDump(t, ctx, cluster)) || osdMemoryInspect(t, ctx, docker.Client, helper.ID).State.Running {
		t.Fatal("helper refusal registered an OSD or restarted helper")
	}
	// The observer intentionally has no cluster ownership label. Docker must
	// refuse non-forced deletion while this independent reference remains.
	if err := cluster.Terminate(ctx); err == nil {
		t.Fatal("busy volume removal reported success")
	}
	still, err := docker.Client.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if err != nil || still.Volume.CreatedAt != volume.Volume.CreatedAt || !maps.Equal(still.Volume.Labels, volume.Volume.Labels) {
		t.Fatal("busy cleanup discarded volume ownership", err)
	}
	if err := observer.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal("volume cleanup retry", err)
	}
	terminated = true
	for _, cid := range owned {
		if _, err := docker.Client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Error("owned container remains", err)
		}
	}
	if _, err := docker.Client.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Error("owned volume remains", err)
	}
	if !host {
		if _, err := docker.Client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Error("owned network remains", err)
		}
	}
	t.Logf("OSD_VOLUME_CLEANUP remaining=0 same_container_restart=true fresh_after_purge=true volume_busy_retry=true volume=%s", name)
}
