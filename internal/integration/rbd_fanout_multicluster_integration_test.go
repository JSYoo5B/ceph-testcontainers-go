//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

// TestMultiClusterRBDSnapshotFanout composes A -> B and A -> C links. Each
// cluster uses one OSD: this tests independent replication paths and receiver
// recovery, rather than redundancy within a storage cluster. Only rbd-mirror
// copies image data; clients perform native librbd writes and fresh reads.
func TestMultiClusterRBDSnapshotFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	a, b, aClient, bClient := newMultiClusterPair(t, ceph.WithOSDCount(1))
	c, cClient := rbdFanoutThirdCluster(t, ctx, a, b, aClient, bClient)
	clusters := []*ceph.Container{a, b, c}
	clients := []testcontainers.Container{aClient, bClient, cClient}
	var fsids [3]string
	const pool = "tc-rbd-fanout"
	const name = "volume"
	const image = pool + "/" + name
	const size = 8 << 20
	for i, cluster := range clusters {
		status, err := cluster.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fsids[i] = status.FSID
		rbdMultiClusterPool(t, ctx, cluster, clients[i], pool)
	}
	ab := rbdFanoutRunLink(t, ctx, a, b, pool, "b")
	ac := rbdFanoutRunLink(t, ctx, a, c, pool, "c")
	// The receivers register their tx-only entries on A asynchronously. Wait
	// for both before enabling the image, so the initial native mirror snapshot
	// includes both destination peer UUIDs.
	rbdFanoutWaitPeers(t, ctx, aClient, bClient, cClient, pool)
	execCommand(t, ctx, aClient, "rbd", "create", image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	before := rbdMultiClusterPayload(size, 41)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 0, before)
	for _, link := range []*rbd.Mirror{ab, ac} {
		if err := link.EnableImage(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, before)
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, before)
	rbdFanoutAssertImageIdentity(t, ctx, clients, image)

	stopGrace := 2 * time.Second
	if err := ab.Stop(ctx, &stopGrace); err != nil {
		t.Fatal(err)
	}
	if state, err := ab.State(ctx); err != nil || state == nil || state.Running {
		t.Fatalf("B receiver did not stop: state=%+v error=%v", state, err)
	}
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 87)
	copy(after[2<<20:], patch)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 2<<20, patch)
	if _, err := ab.SourceRBD(ctx, "mirror", "image", "snapshot", image); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, after)
	rbdFanoutRead(t, ctx, bClient, fsids[1], pool, name, before)
	t.Log("B receiver is stopped and retains its previous 8 MiB image; C independently received A's new checkpoint")

	if err := ab.Start(ctx); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, after)
	rbdFanoutRead(t, ctx, cClient, fsids[2], pool, name, after)
	final := bytes.Clone(after)
	lastPatch := rbdMultiClusterPayload(256<<10, 119)
	copy(final[6<<20:], lastPatch)
	rbdFanoutWrite(t, ctx, aClient, fsids[0], pool, name, 6<<20, lastPatch)
	if _, err := ac.SourceRBD(ctx, "mirror", "image", "snapshot", image); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, bClient, fsids[1], pool, name, final)
	rbdFanoutWaitBytes(t, ctx, cClient, fsids[2], pool, name, final)
	rbdFanoutRead(t, ctx, aClient, fsids[0], pool, name, final)
	rbdFanoutAssertImageIdentity(t, ctx, clients, image)
	for _, cluster := range clusters {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}

	for _, link := range []*rbd.Mirror{ab, ac} {
		if err := link.Stop(ctx, &stopGrace); err != nil {
			t.Fatal(err)
		}
	}
	stopMultiClusterSource(t, ctx, a)
	rbdFanoutRead(t, ctx, bClient, fsids[1], pool, name, final)
	rbdFanoutRead(t, ctx, cClient, fsids[2], pool, name, final)
	t.Logf("snapshot fanout A -> B/C verified independent FSIDs=%v; exact %d-byte replicas sha256=%x survive stopped source MON/OSD and both stopped receivers", fsids, len(final), sha256.Sum256(final))
}
