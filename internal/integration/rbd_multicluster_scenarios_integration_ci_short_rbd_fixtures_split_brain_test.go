//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_split_brain))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"
)

// TestMultiClusterRBDSplitBrainResync deliberately forks a disposable volume.
// Recovery explicitly selects A as authoritative and discards B's divergent
// writes; this is not a data merge or a claim of lossless forced failover.
func TestMultiClusterRBDSplitBrainResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-split-brain"
	const image = pool + "/disposable"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 43)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	// A deliberately forced B primary needs a TX-capable peer to create its
	// own divergent mirror checkpoints. No receiver is started on A.
	peer := rbdScenarioPeer(t, ctx, link, pool)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "set", pool, peer, "direction", "rx-tx")
	rbdScenarioStop(t, ctx, link)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "promote", "--force", image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	authoritative := bytes.Clone(before)
	patchA := rbdMultiClusterPayload(1<<20, 137)
	copy(authoritative[1<<20:], patchA)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 1<<20, patchA)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@authoritative-a")
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	discarded := bytes.Clone(before)
	patchB := rbdMultiClusterPayload(1<<20, 163)
	copy(discarded[3<<20:], patchB)
	rbdMultiClusterWriteRange(t, ctx, destinationClient, image, size, 3<<20, patchB)
	execCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@discarded-b")
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "snapshot", image)
	verifyRBDBytes(t, ctx, sourceClient, image, authoritative)
	verifyRBDBytes(t, ctx, destinationClient, image, discarded)

	// A stays primary. Demoting the losing branch permits the restarted B
	// receiver to discover the incompatible histories and report split-brain.
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "demote", image)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	rbdScenarioWaitSplitBrain(t, ctx, link, image)
	rbdScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, authoritative)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image+"@authoritative-a", authoritative)
	rbdScenarioRequireSnapshotAbsent(t, ctx, destinationClient, image, "discarded-b")
	if bytes.Equal(authoritative[3<<20:4<<20], patchB) {
		t.Fatal("fixture cannot distinguish discarded branch from authoritative content")
	}

	// Confirm that recovery clears the fault and mirroring continues, rather
	// than merely producing a one-time copy of the winning branch.
	after := bytes.Clone(authoritative)
	tail := rbdMultiClusterPayload(256<<10, 181)
	copy(after[7<<20:], tail)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 7<<20, tail)
	rbdScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	t.Logf("RBD native split-brain detected and recovered by choosing A + demoting/resyncing B; discarded B branch sha256=%x, winning A branch sha256=%x; subsequent checkpoint replicated", sha256.Sum256(discarded), sha256.Sum256(authoritative))
}
