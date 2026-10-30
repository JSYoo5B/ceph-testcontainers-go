//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_failback))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// TestMultiClusterRBDFailback moves primary ownership A -> B -> A. Both sites
// run native receivers so B's new writes and snapshots return to A before A is
// promoted, and subsequent A writes are mirrored to B again.
func TestMultiClusterRBDFailback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t)
	const pool = "tc-rbd-failback"
	const image = pool + "/volume"
	const size = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := harness.RBDScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := harness.RBDScenarioRunLink(t, ctx, destination, source, pool, "destination", "source")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")
	before := harness.RBDScenarioSeed(t, ctx, sourceClient, image, 31)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@on-a")
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, before)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image+"@on-a", before)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	harness.RBDMultiClusterPromote(t, ctx, destinationClient, image)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, image, false)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	// B is the active site. Pause its receiver without interrupting its native
	// sender: A's receiver must obtain B's writes across the cluster boundary.
	harness.RBDScenarioStop(t, ctx, forward)
	onB := bytes.Clone(before)
	patchB := harness.RBDMultiClusterPayload(1<<20, 101)
	copy(onB[4<<20:], patchB)
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, image, size, 4<<20, patchB)
	harness.ExecCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@on-b")
	harness.RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "snapshot", image)
	harness.RBDScenarioWaitBytes(t, ctx, sourceClient, image, onB)
	harness.RBDScenarioWaitBytes(t, ctx, sourceClient, image+"@on-b", onB)
	harness.VerifyRBDBytes(t, ctx, sourceClient, image+"@on-a", before)
	harness.RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "demote", image)
	harness.RBDMultiClusterPromote(t, ctx, sourceClient, image)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	harness.VerifyRBDBytes(t, ctx, sourceClient, image, onB)

	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(onB)
	patchA := harness.RBDMultiClusterPayload(256<<10, 127)
	copy(after[:len(patchA)], patchA)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, patchA)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", image)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, after)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		harness.VerifyRBDBytes(t, ctx, client, image+"@on-a", before)
		harness.VerifyRBDBytes(t, ctx, client, image+"@on-b", onB)
	}
	t.Logf("RBD planned A -> B -> A failback: B-only writes returned before non-forced A promotion; A resumed replication; both user snapshots retained complete 8 MiB history; final sha256=%x", sha256.Sum256(after))
}
