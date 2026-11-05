//go:build all || (integration && multicluster)

package rbd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	rbdmirrorexpect "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror/expect"
	"github.com/testcontainers/testcontainers-go"
)

func runMultiClusterRBDJournalMirrorFailback(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
	const pool = "tc-rbd-journal"
	const name = "volume"
	const image = pool + "/" + name
	const size = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	sourceStatus, err := harness.ObserveCluster(source).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := harness.ObserveCluster(destination).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	forward := harness.RBDJournalRunLink(t, ctx, source, destination, pool, "source", "destination")
	// Journal mode needs exclusive-lock. EnableImage then asks the native CLI
	// to add journaling explicitly to this selected image, leaving pool scope
	// "image" and unrelated volumes unchanged.
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "create", image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	before := harness.RBDMultiClusterPayload(size, 37)
	harness.RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, before)
	if err := forward.EnableImage(ctx, name); err != nil {
		t.Fatal(err)
	}
	initialReplay := harness.RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	harness.RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, before)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)

	first := bytes.Clone(before)
	patch := harness.RBDMultiClusterPayload(1<<20, 83)
	copy(first[3<<20:], patch)
	harness.RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 3<<20, patch)
	harness.RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, first)
	t.Log("journal replay propagated a native ranged write without a manual mirror snapshot")

	// Pause the receiver and prove its existing replica remains unchanged.
	// New source writes are replayed from the journal after the receiver starts.
	rbdScenarioStop(t, ctx, forward)
	paused, err := forward.ImageStatus(ctx, name)
	if err != nil || paused.ReplayReady || paused.GlobalID != initialReplay.GlobalID || paused.DestinationImageID != initialReplay.DestinationImageID {
		t.Fatalf("stopped receiver readiness/identity differs: %+v error=%v", paused, err)
	}
	pausedCtx, pausedCancel := context.WithTimeout(ctx, 3*time.Second)
	_, pausedErr := forward.WaitReplayReady(pausedCtx, name)
	pausedCancel()
	if !errors.Is(pausedErr, context.DeadlineExceeded) {
		t.Fatalf("stopped receiver wait ignored deadline: %v", pausedErr)
	}
	backlog := bytes.Clone(first)
	queued := harness.RBDMultiClusterPayload(256<<10, 109)
	copy(backlog, queued)
	harness.RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, queued)
	harness.RBDJournalRead(t, ctx, destinationClient, destinationStatus.FSID, pool, name, first)
	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := harness.RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	if result, err := (rbdmirrorexpect.Image{Select: initialReplay, Replaying: true, NewInstance: true}).Evaluate(restarted); err != nil || !result.Matched {
		t.Fatalf("receiver restart changed images or retained a stale process instance: %v %v", result, err)
	}
	harness.RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, backlog)

	reverse := harness.RBDJournalRunLink(t, ctx, destination, source, pool, "destination", "source")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")
	// Writers are closed before demotion. Native promotion retries while the
	// journal receiver consumes the demotion marker, and never uses --force.
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	harness.RBDMultiClusterPromote(t, ctx, destinationClient, image)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, false)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, true)
	rbdScenarioStop(t, ctx, forward)
	harness.RBDMirrorReplayReady(t, ctx, reverse, name, rbdmirror.ModeJournal, "", "")
	onB := bytes.Clone(backlog)
	patchB := harness.RBDMultiClusterPayload(1<<20, 137)
	copy(onB[5<<20:], patchB)
	harness.RBDJournalWrite(t, ctx, destinationClient, destinationStatus.FSID, pool, name, 5<<20, patchB)
	harness.RBDJournalWaitBytes(t, ctx, sourceClient, sourceStatus.FSID, pool, name, onB)

	harness.RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "demote", image)
	harness.RBDMultiClusterPromote(t, ctx, sourceClient, image)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)
	harness.RBDJournalRead(t, ctx, sourceClient, sourceStatus.FSID, pool, name, onB)
	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	harness.RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	after := bytes.Clone(onB)
	patchA := harness.RBDMultiClusterPayload(256<<10, 167)
	copy(after[2<<20:], patchA)
	harness.RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 2<<20, patchA)
	harness.RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, after)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)
	t.Logf("native RBD journal: full 8 MiB equality, receiver restart backlog, non-forced A -> B -> A, B-only writes retained, resumed A writes; final sha256=%x; no mirror snapshot commands", sha256.Sum256(after))
}

func rbdJournalAssertPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string, primary bool) {
	t.Helper()
	var info struct {
		Features  []string `json:"features"`
		Mirroring struct {
			Mode, State string
			Primary     bool
			GlobalID    string `json:"global_id"`
		} `json:"mirroring"`
	}
	if err := json.Unmarshal(harness.RBDOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if info.Mirroring.Mode != "journal" || info.Mirroring.State != "enabled" || info.Mirroring.GlobalID == "" || info.Mirroring.Primary != primary || !slices.Contains(info.Features, "journaling") || !slices.Contains(info.Features, "exclusive-lock") {
		t.Fatalf("unexpected journal image mode/features/primary=%t: %+v", primary, info)
	}
}
