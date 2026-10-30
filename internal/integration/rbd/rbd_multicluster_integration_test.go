//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rbd))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// TestMultiClusterRBDBackup restores CLI-generated full and incremental archives
// into an independent cluster after the source MON/OSDs are stopped.
func TestMultiClusterRBDBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t)
	const pool = "tc-rbd-backup"
	const original = pool + "/original"
	const restored = pool + "/restored"
	const imageSize = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)

	before := harness.RBDMultiClusterPayload(imageSize, 19)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-backup-original", 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-backup-original", original,
		"--object-size", "1M", "--image-feature", "layering", "--no-progress")
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@baseline")
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "image-meta", "set", original, "backup-fixture", "format-2")
	var fullArchive bytes.Buffer
	if err := rbd.ExportBackup(ctx, sourceClient, original, &fullArchive); err != nil {
		t.Fatal(err)
	}
	full := fullArchive.Bytes()

	// Change exactly one 1 MiB object. This local write fixture uses the public
	// RBD diff stream; Ceph's export-diff command produces the actual backup.
	after := bytes.Clone(before)
	patch := harness.RBDMultiClusterPayload(1<<20, 61)
	copy(after[2<<20:], patch)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, original, imageSize, 2<<20, patch)
	harness.VerifyRBDBytes(t, ctx, sourceClient, original, after)
	harness.VerifyRBDBytes(t, ctx, sourceClient, original+"@baseline", before)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@next")
	var deltaArchive bytes.Buffer
	if err := rbd.ExportIncremental(ctx, sourceClient, original+"@next", "baseline", &deltaArchive); err != nil {
		t.Fatal(err)
	}
	delta := deltaArchive.Bytes()
	if len(full) < imageSize || len(delta) == 0 || len(delta) >= len(full)/2 {
		t.Fatalf("unexpected archive sizes: full=%d incremental=%d", len(full), len(delta))
	}
	harness.StopMultiClusterSource(t, ctx, source)

	// Incremental restore must reject an unrelated image without its baseline.
	harness.ExecCommand(t, ctx, destinationClient, "rbd", "create", pool+"/missing-baseline", "--size", "8M", "--image-feature", "layering")
	err := rbd.RestoreIncremental(ctx, destinationClient, pool+"/missing-baseline", bytes.NewReader(delta))
	var refusal *ceph.NativeFailure
	if !errors.As(err, &refusal) || refusal.Program != "rbd" || refusal.Errno != "EINVAL" || !strings.Contains(refusal.Reason, "'baseline' does not exist") {
		t.Fatalf("missing baseline should reject incremental restore: %v", err)
	}
	harness.ExecCommand(t, ctx, destinationClient, "rbd", "rm", pool+"/missing-baseline", "--no-progress")
	if err := rbd.RestoreBackup(ctx, destinationClient, restored, bytes.NewReader(full)); err != nil {
		t.Fatal(err)
	}
	harness.VerifyRBDInfo(t, ctx, destinationClient, restored, imageSize)
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored, before)
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	if got := strings.TrimSpace(string(harness.RBDOutput(t, ctx, destinationClient, "image-meta", "get", restored, "backup-fixture"))); got != "format-2" {
		t.Fatalf("full backup lost image metadata: %q", got)
	}
	if err := rbd.RestoreIncremental(ctx, destinationClient, restored, bytes.NewReader(delta)); err != nil {
		t.Fatal(err)
	}
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored, after)
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)

	// Destination-only writes must preserve the restored historical snapshots.
	writable := bytes.Clone(after)
	copy(writable[:256<<10], harness.RBDMultiClusterPayload(256<<10, 97))
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, restored, imageSize, 0, writable[:256<<10])
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored, writable)
	harness.VerifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)
	if err := destination.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("RBD intercluster backup: full format-2=%d bytes sha256=%x; incremental=%d bytes sha256=%x; snapshots, metadata, chain rejection and source-independent writable restore verified", len(full), sha256.Sum256(full), len(delta), sha256.Sum256(delta))
}

// TestMultiClusterRBDSnapshotMirror uses a real rbd-mirror daemon and rx-only
// peer. Snapshot mirroring is asynchronous and needs explicit checkpoints.
func TestMultiClusterRBDSnapshotMirror(t *testing.T) {
	harness.RunMultiClusterRBDSnapshotMirror(t)
}
