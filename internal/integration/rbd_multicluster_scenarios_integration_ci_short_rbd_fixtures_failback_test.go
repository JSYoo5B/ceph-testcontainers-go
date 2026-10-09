//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_failback))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-failback"
	const image = pool + "/volume"
	const size = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := rbdScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := rbdScenarioRunLink(t, ctx, destination, source, pool, "destination", "source")
	rbdScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	rbdScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")
	before := rbdScenarioSeed(t, ctx, sourceClient, image, 31)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", image+"@on-a")
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, before)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image+"@on-a", before)
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	rbdMultiClusterPromote(t, ctx, destinationClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, false)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)

	// B is the active site. Pause its receiver without interrupting its native
	// sender: A's receiver must obtain B's writes across the cluster boundary.
	rbdScenarioStop(t, ctx, forward)
	onB := bytes.Clone(before)
	patchB := rbdMultiClusterPayload(1<<20, 101)
	copy(onB[4<<20:], patchB)
	rbdMultiClusterWriteRange(t, ctx, destinationClient, image, size, 4<<20, patchB)
	execCommand(t, ctx, destinationClient, "rbd", "snap", "create", image+"@on-b")
	rbdScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, sourceClient, image, onB)
	rbdScenarioWaitBytes(t, ctx, sourceClient, image+"@on-b", onB)
	verifyRBDBytes(t, ctx, sourceClient, image+"@on-a", before)
	rbdScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "demote", image)
	rbdMultiClusterPromote(t, ctx, sourceClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	verifyRBDBytes(t, ctx, sourceClient, image, onB)

	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after := bytes.Clone(onB)
	patchA := rbdMultiClusterPayload(256<<10, 127)
	copy(after[:len(patchA)], patchA)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, size, 0, patchA)
	rbdScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", image)
	rbdScenarioWaitBytes(t, ctx, destinationClient, image, after)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		verifyRBDBytes(t, ctx, client, image+"@on-a", before)
		verifyRBDBytes(t, ctx, client, image+"@on-b", onB)
	}
	t.Logf("RBD planned A -> B -> A failback: B-only writes returned before non-forced A promotion; A resumed replication; both user snapshots retained complete 8 MiB history; final sha256=%x", sha256.Sum256(after))
}
