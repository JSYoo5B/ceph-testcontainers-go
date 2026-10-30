//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
	"time"
)

// TestMultiClusterRBDPeerLifecycle removes the destination's native peer,
// proves that new checkpoints stop arriving, reboots the relationship and
// requests a full resync before verifying that later checkpoints continue.
func TestMultiClusterRBDPeerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t)
	const pool = "tc-rbd-peer-lifecycle"
	const image = pool + "/volume"
	const size = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := harness.RBDScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := harness.RBDScenarioSeed(t, ctx, sourceClient, image, 53)
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, before)
	oldPeer := harness.RBDScenarioPeer(t, ctx, link, pool)
	harness.RBDScenarioStop(t, ctx, link)
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "remove", pool, oldPeer)
	harness.RBDScenarioRequireNoPeers(t, ctx, link, pool)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(before)
	patch := harness.RBDMultiClusterPayload(1<<20, 191)
	copy(after[5<<20:], patch)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, size, 5<<20, patch)
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	for i := 0; i < 3; i++ {
		harness.VerifyRBDBytes(t, ctx, destinationClient, image, before)
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err := link.Rebootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	newPeer := harness.RBDScenarioPeer(t, ctx, link, pool)
	if newPeer == oldPeer {
		t.Fatal("removed peer UUID was reused rather than creating a new relationship")
	}
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, after)
	latest := bytes.Clone(after)
	lastPatch := harness.RBDMultiClusterPayload(256<<10, 211)
	copy(latest[:len(lastPatch)], lastPatch)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, lastPatch)
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, latest)
	t.Logf("RBD peer lifecycle: removed %s, no peer/no new data while daemon ran, rebootstrap created %s; resync and subsequent checkpoint restored native replication", oldPeer, newPeer)
}
