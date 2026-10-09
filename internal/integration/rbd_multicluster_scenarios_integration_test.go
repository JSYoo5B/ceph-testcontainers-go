//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestMultiClusterRBDPeerLifecycle removes the destination's native peer,
// proves that new checkpoints stop arriving, reboots the relationship and
// requests a full resync before verifying that later checkpoints continue.
func TestMultiClusterRBDPeerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-peer-lifecycle"
	const image = pool + "/volume"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 53)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	oldPeer := rbdScenarioPeer(t, ctx, link, pool)
	rbdScenarioStop(t, ctx, link)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "remove", pool, oldPeer)
	rbdScenarioRequireNoPeers(t, ctx, link, pool)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 191)
	copy(after[5<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 5<<20, patch)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	for i := 0; i < 3; i++ {
		verifyRBDBytes(t, ctx, destinationClient, image, before)
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	if err := link.Rebootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	newPeer := rbdScenarioPeer(t, ctx, link, pool)
	if newPeer == oldPeer {
		t.Fatal("removed peer UUID was reused rather than creating a new relationship")
	}
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	latest := bytes.Clone(after)
	lastPatch := rbdMultiClusterPayload(256<<10, 211)
	copy(latest[:len(lastPatch)], lastPatch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, lastPatch)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, latest)
	t.Logf("RBD peer lifecycle: removed %s, no peer/no new data while daemon ran, rebootstrap created %s; resync and subsequent checkpoint restored native replication", oldPeer, newPeer)
}
