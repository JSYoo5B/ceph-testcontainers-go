//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rbd_daemons))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterRBDPeerNetworkInterruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	source, destination, a, b := newMultiClusterPair(t, ceph.WithOSDCount(1), ceph.WithSeparateClusterNetwork())
	const pool = "tc-peer-network"
	const name = "backlog"
	rbdMultiClusterPool(t, ctx, source, a, pool)
	rbdMultiClusterPool(t, ctx, destination, b, pool)
	image := source.ControlImage()
	mirror, err := rbd.RunMirror(ctx, image, rbd.MirrorConfig{Source: source, Destination: destination, Pool: pool})
	if mirror != nil {
		testcontainers.CleanupContainer(t, mirror)
	}
	if err != nil {
		t.Fatal(err)
	}
	daemon := mirror.Daemons()[0]
	rbdDaemonWaitElection(t, ctx, pool, mirror.Daemons())
	status, err := daemon.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	instance := status.PoolReplayers[0].InstanceID
	peer := rbdDaemonPoolIdentity(t, ctx, mirror, pool)
	fsid := func(c *ceph.Container) string {
		status, err := c.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return status.FSID
	}
	sourceID, destinationID := fsid(source), fsid(destination)
	execCommand(t, ctx, a, "rbd", "create", pool+"/"+name, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	initial := rbdMultiClusterPayload(8<<20, 51)
	rbdFanoutWrite(t, ctx, a, sourceID, pool, name, 0, initial)
	if err := mirror.EnableImage(ctx, name); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, b, destinationID, pool, name, initial)
	cut, err := mirror.InterruptPeerLink(ctx, daemon.DaemonName)
	if cut != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := cut.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Error(err)
		}
	})
	inspection, err := docker.ContainerInspect(ctx, daemon.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.Container.State.Running || inspection.Container.NetworkSettings.Networks[source.NetworkName()] != nil || inspection.Container.NetworkSettings.Networks[destination.NetworkName()] == nil || inspection.Container.NetworkSettings.Networks[destination.ClusterNetworkName()] != nil {
		t.Fatal("peer cut stopped process, removed destination endpoint, or attached daemon to OSD backend")
	}
	changed := bytes.Clone(initial)
	patch := rbdMultiClusterPayload(1<<20, 103)
	copy(changed[3<<20:], patch)
	rbdFanoutWrite(t, ctx, a, sourceID, pool, name, 3<<20, patch)
	execCommand(t, ctx, a, "rbd", "mirror", "image", "snapshot", pool+"/"+name)
	for i := 0; i < 3; i++ {
		rbdFanoutRead(t, ctx, b, destinationID, pool, name, initial)
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, b, destinationID, pool, name, changed)
	status, err = daemon.Status(ctx)
	if err != nil || len(status.PoolReplayers) != 1 || status.PoolReplayers[0].InstanceID != instance {
		t.Fatalf("peer restoration restarted/replaced the native receiver: status=%+v error=%v", status, err)
	}
	if got := rbdDaemonPoolIdentity(t, ctx, mirror, pool); got != peer {
		t.Fatal("peer endpoint restoration changed pool/link identity")
	}
	t.Log("separate public/cluster bridges on both clusters: receiver source endpoint interrupted while native process and destination election stayed alive; retained replica read, checkpoint backlog resumed on original IP without daemon restart")
}
