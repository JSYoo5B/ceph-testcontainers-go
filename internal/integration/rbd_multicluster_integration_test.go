//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
)

// TestMultiClusterRBDBackup restores CLI-generated full and incremental archives
// into an independent cluster after the source MON/OSDs are stopped.
func TestMultiClusterRBDBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-backup"
	const original = pool + "/original"
	const restored = pool + "/restored"
	const imageSize = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)

	before := rbdMultiClusterPayload(imageSize, 19)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-backup-original", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-backup-original", original,
		"--object-size", "1M", "--image-feature", "layering", "--no-progress")
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@baseline")
	execCommand(t, ctx, sourceClient, "rbd", "image-meta", "set", original, "backup-fixture", "format-2")
	var fullArchive bytes.Buffer
	if err := multicluster.ExportRBDBackup(ctx, sourceClient, original, &fullArchive); err != nil {
		t.Fatal(err)
	}
	full := fullArchive.Bytes()

	// Change exactly one 1 MiB object. This local write fixture uses the public
	// RBD diff stream; Ceph's export-diff command produces the actual backup.
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 61)
	copy(after[2<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, original, imageSize, 2<<20, patch)
	verifyRBDBytes(t, ctx, sourceClient, original, after)
	verifyRBDBytes(t, ctx, sourceClient, original+"@baseline", before)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@next")
	var deltaArchive bytes.Buffer
	if err := multicluster.ExportRBDIncremental(ctx, sourceClient, original+"@next", "baseline", &deltaArchive); err != nil {
		t.Fatal(err)
	}
	delta := deltaArchive.Bytes()
	if len(full) < imageSize || len(delta) == 0 || len(delta) >= len(full)/2 {
		t.Fatalf("unexpected archive sizes: full=%d incremental=%d", len(full), len(delta))
	}
	stopMultiClusterSource(t, ctx, source)

	// Incremental restore must reject an unrelated image without its baseline.
	execCommand(t, ctx, destinationClient, "rbd", "create", pool+"/missing-baseline", "--size", "8M", "--image-feature", "layering")
	if err := multicluster.RestoreRBDIncremental(ctx, destinationClient, pool+"/missing-baseline", bytes.NewReader(delta)); err == nil || !strings.Contains(err.Error(), "baseline") {
		t.Fatalf("missing baseline should reject incremental restore: %v", err)
	}
	execCommand(t, ctx, destinationClient, "rbd", "rm", pool+"/missing-baseline", "--no-progress")
	if err := multicluster.RestoreRBDBackup(ctx, destinationClient, restored, bytes.NewReader(full)); err != nil {
		t.Fatal(err)
	}
	verifyRBDInfo(t, ctx, destinationClient, restored, imageSize)
	verifyRBDBytes(t, ctx, destinationClient, restored, before)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	if got := strings.TrimSpace(string(rbdOutput(t, ctx, destinationClient, "image-meta", "get", restored, "backup-fixture"))); got != "format-2" {
		t.Fatalf("full backup lost image metadata: %q", got)
	}
	if err := multicluster.RestoreRBDIncremental(ctx, destinationClient, restored, bytes.NewReader(delta)); err != nil {
		t.Fatal(err)
	}
	verifyRBDBytes(t, ctx, destinationClient, restored, after)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)

	// Destination-only writes must preserve the restored historical snapshots.
	writable := bytes.Clone(after)
	copy(writable[:256<<10], rbdMultiClusterPayload(256<<10, 97))
	rbdMultiClusterWriteRange(t, ctx, destinationClient, restored, imageSize, 0, writable[:256<<10])
	verifyRBDBytes(t, ctx, destinationClient, restored, writable)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)
	if err := destination.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("RBD intercluster backup: full format-2=%d bytes sha256=%x; incremental=%d bytes sha256=%x; snapshots, metadata, chain rejection and source-independent writable restore verified", len(full), sha256.Sum256(full), len(delta), sha256.Sum256(delta))
}

// TestMultiClusterRBDSnapshotMirror uses a real rbd-mirror daemon and rx-only
// peer. Snapshot mirroring is asynchronous and needs explicit checkpoints.
func TestMultiClusterRBDSnapshotMirror(t *testing.T) {
	testMultiClusterRBDSnapshotMirror(t)
}
