//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_split_brain))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestMultiClusterRBDSplitBrainResync deliberately forks a disposable volume.
// Recovery explicitly selects A as authoritative and discards B's divergent
// writes; this is not a data merge or a claim of lossless forced failover.
func TestMultiClusterRBDSplitBrainResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t)
	const pool = "tc-rbd-split-brain"
	const image = pool + "/disposable"
	const size = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link := harness.RBDScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, link.SourceRBD, pool, "destination")
	before := harness.RBDScenarioSeed(t, ctx, sourceClient, image, 43)
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, before)
	// A deliberately forced B primary needs a TX-capable peer to create its
	// own divergent mirror checkpoints. No receiver is started on A.
	peer := harness.RBDScenarioPeer(t, ctx, link, pool)
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "pool", "peer", "set", pool, peer, "direction", "rx-tx")
	harness.RBDScenarioStop(t, ctx, link)
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "promote", "--force", image)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	authoritative := bytes.Clone(before)
	patchA := harness.RBDMultiClusterPayload(1<<20, 137)
	copy(authoritative[1<<20:], patchA)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, size, 1<<20, patchA)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@authoritative-a")
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	discarded := bytes.Clone(before)
	patchB := harness.RBDMultiClusterPayload(1<<20, 163)
	copy(discarded[3<<20:], patchB)
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, image, size, 3<<20, patchB)
	harness.ExecCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@discarded-b")
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "snapshot", image)
	harness.VerifyRBDBytes(t, ctx, sourceClient, image, authoritative)
	harness.VerifyRBDBytes(t, ctx, destinationClient, image, discarded)

	// A stays primary. Demoting the losing branch permits the restarted B
	// receiver to discover the incompatible histories and report split-brain.
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "demote", image)
	if err := link.Start(ctx); err != nil {
		t.Fatal(err)
	}
	harness.RBDScenarioWaitSplitBrain(t, ctx, link, image)
	harness.RBDScenarioCommand(t, ctx, link.DestinationRBD, "mirror", "image", "resync", image)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, authoritative)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image+"@authoritative-a", authoritative)
	harness.RBDScenarioRequireSnapshotAbsent(t, ctx, destinationClient, image, "discarded-b")
	if bytes.Equal(authoritative[3<<20:4<<20], patchB) {
		t.Fatal("fixture cannot distinguish discarded branch from authoritative content")
	}

	// Confirm that recovery clears the fault and mirroring continues, rather
	// than merely producing a one-time copy of the winning branch.
	after := bytes.Clone(authoritative)
	tail := harness.RBDMultiClusterPayload(256<<10, 181)
	copy(after[7<<20:], tail)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, size, 7<<20, tail)
	harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "image", "snapshot", image)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, after)
	t.Logf("RBD native split-brain detected and recovered by choosing A + demoting/resyncing B; discarded B branch sha256=%x, winning A branch sha256=%x; subsequent checkpoint replicated", sha256.Sum256(discarded), sha256.Sum256(authoritative))
}
