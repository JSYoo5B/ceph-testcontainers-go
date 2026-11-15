//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_unlink_trashed_snapshot)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rbd-aux

package rbd_mirror_unlink_trashed_snapshot_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// image is the Ceph image every cluster, client and receiver of the case
// runs: Tentacle 20.2.1, whose unlink request refuses a trashed mirror
// snapshot as not a mirror snapshot. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_unlink_trashed_snapshot_test.image=<image>"
//
// Tentacle 20.2.2 and Squid 19.2.4 skip a trashed snapshot like a missing
// one.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all"

const (
	unlinkPool      = "tc-rbd-unlink"
	unlinkImageName = "primary"
	unlinkImage     = unlinkPool + "/" + unlinkImageName
	unlinkClone     = unlinkPool + "/holder"
	unlinkImageSize = 4 << 20
)

// TestRBDMirrorUnlinkFromTrashedSnapshot reports whether the receiver of a
// snapshot-mode mirror fails when it unlinks itself from a mirror snapshot
// of the primary image that has been moved to the trash namespace.
//
// Given a primary image whose mirror snapshot S1 the receiver has copied and
// still uses as its sync start, a clone of S1 that keeps S1's removal in the
// trash namespace, and S1 listed as trashed, when the next mirror snapshot is
// taken, the receiver copies it and unlinks itself from S1. Then the case is
// REPRODUCED if the unlink request refuses S1 as not a mirror snapshot and
// the replayer fails with EINVAL, and NOT_REPRODUCED if the request skips S1
// as missing or trashed and the replayer goes on. Either way S1 must still be
// trashed afterwards, and a later write must replicate (on a reproducing
// image the replayer is restarted first; the time is reported).
//
// A failure of the given or when steps fails the test; so does an unlink
// that found S1 fully removed, a failure with another errno, or no unlink
// from S1 at all. The result does not.
func TestRBDMirrorUnlinkFromTrashedSnapshot(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorUnlinkFromTrashedSnapshot)
	}
}

func testRBDMirrorUnlinkFromTrashedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("image: %s", image)
	source, sourceClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	destination, destinationClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("ceph: source %s, destination %s", harness.StaticFacts(source).CephVersion, harness.StaticFacts(destination).CephVersion)
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, unlinkPool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, unlinkPool)
	// The receiver names the snapshot it unlinks from, and the unlink
	// request of the remote image, which runs in the receiver on its
	// connection to the source as the bootstrap peer client, names the
	// snapshot and its state.
	harness.CephCommand(t, ctx, destination, "config", "set", "client", "debug_rbd_mirror", "10")
	harness.CephCommand(t, ctx, source, "config", "set", "client.rbd-mirror-peer", "debug_rbd", "15")
	forward := unlinkLink(t, ctx, source, destination)
	harness.RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, unlinkPool, "destination")
	peer := unlinkSourcePeer(t, ctx, forward)

	// Control: an ordinary write and mirror snapshot replicate.
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "create", unlinkImage, "--size", strconv.Itoa(unlinkImageSize>>20)+"M", "--image-feature", "layering,exclusive-lock")
	first := harness.RBDMultiClusterPayload(unlinkImageSize, 11)
	unlinkWrite(t, ctx, sourceClient, first)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", unlinkImage, "snapshot")
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, unlinkImage, first)

	// S1: the next mirror snapshot, copied by the receiver, which keeps its
	// peer linked to it as the start of the next sync.
	second := harness.RBDMultiClusterPayload(unlinkImageSize, 12)
	unlinkWrite(t, ctx, sourceClient, second)
	before := unlinkListing(t, ctx, sourceClient, unlinkImage)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", unlinkImage)
	s1 := unlinkNewMirrorSnapshot(t, ctx, sourceClient, before)
	t.Logf("S1 is primary mirror snapshot %d %s", s1.ID, s1.Name)
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, unlinkImage, second)
	synced, err := harness.Observe(ctx, 2*time.Minute, 20*time.Second, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		code, out, err := harness.RBDMultiClusterExec(probeCtx, destinationClient, "rbd", "snap", "ls", "--all", "--format", "json", unlinkImage)
		if err != nil || code != 0 {
			return string(out), false, nil
		}
		snaps, err := parseUnlinkSnapshots(out)
		if err != nil {
			return string(out), false, err
		}
		if len(snaps) == 0 {
			return string(out), false, nil
		}
		last := snaps[len(snaps)-1].Namespace
		return string(out), last.Type == "mirror" && last.State == "non-primary" && last.PrimarySnapID == s1.ID && last.Complete != nil && *last.Complete, nil
	})
	if err != nil {
		t.Fatalf("destination has no complete copy of S1 (%d) as its newest snapshot: %v; last %s", s1.ID, err, synced.Value)
	}
	t.Logf("destination copied S1: %s", synced.Value)
	linked, ok := findUnlinkSnapshot(unlinkSnapshots(t, ctx, sourceClient, unlinkImage), s1.ID)
	if !ok || linked.Namespace.Type != "mirror" || !slices.Contains(linked.Namespace.MirrorPeerUUIDs, peer) {
		t.Fatalf("S1 (%d) does not keep peer %s linked: %+v", s1.ID, peer, linked)
	}

	// The clone holds S1, so its removal leaves S1 in the trash namespace.
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "clone", "--rbd-default-clone-format", "2", "--snap-id", strconv.FormatUint(s1.ID, 10), unlinkImage, unlinkClone)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "snap", "rm", "--snap-id", strconv.FormatUint(s1.ID, 10), unlinkImage)
	trashed := unlinkListing(t, ctx, sourceClient, unlinkImage)
	if snaps, err := parseUnlinkSnapshots(trashed); err != nil || !trashedMirrorSnapshot(snaps, s1.ID, s1.Name) {
		t.Fatalf("S1 (%d) is not trashed after its removal: %s %v", s1.ID, trashed, err)
	}
	t.Logf("S1 (%d) is trashed: %s", s1.ID, trashed)

	// When: the next mirror snapshot, whose copy ends with the unlink from
	// S1.
	third := harness.RBDMultiClusterPayload(unlinkImageSize, 13)
	unlinkWrite(t, ctx, sourceClient, third)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", unlinkImage)

	daemon := forward.Daemons()[0]
	var symptoms []string
	judged, err := harness.Observe(ctx, 4*time.Minute, 30*time.Second, 2*time.Second, func(probeCtx context.Context) (unlinkOutcome, bool, error) {
		log, err := unlinkReceiverLog(probeCtx, daemon)
		if err != nil {
			return unlinkOutcome{}, false, nil
		}
		got := readUnlinkOutcome(log, s1.ID)
		found, ready, err := judgeUnlink(s1.ID, got)
		symptoms = found
		return got, ready, err
	})
	if err != nil {
		t.Fatalf("receiver's unlink from trashed S1 (%d): %v; lines %q", s1.ID, err, judged.Value.Lines)
	}
	t.Logf("receiver's unlink from S1 (%d) after %s: %q", s1.ID, judged.Elapsed.Round(time.Second), judged.Value.Lines)
	if status, err := forward.ImageStatus(ctx, unlinkImageName); err == nil {
		t.Logf("image status after the unlink: state=%s description=%q", status.State, status.Description)
	} else {
		t.Logf("image status after the unlink unavailable: %v", err)
	}
	after := unlinkListing(t, ctx, sourceClient, unlinkImage)
	if snaps, err := parseUnlinkSnapshots(after); err != nil || !trashedMirrorSnapshot(snaps, s1.ID, s1.Name) {
		t.Fatalf("S1 (%d) is no longer trashed after the unlink: %s %v", s1.ID, after, err)
	}

	// A later write and mirror snapshot replicate; on a reproducing image
	// only after the stopped replayer is started again.
	fourth := harness.RBDMultiClusterPayload(unlinkImageSize, 14)
	unlinkWrite(t, ctx, sourceClient, fourth)
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", unlinkImage)
	started := time.Now()
	if err := unlinkWaitBytes(ctx, destinationClient, fourth); err != nil {
		t.Fatalf("a later write did not replicate: %v", err)
	}
	t.Logf("a later write replicated %s after its snapshot: sha256=%x", time.Since(started).Round(time.Second), sha256.Sum256(fourth))
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rbd_mirror_unlink_trashed_snapshot result=%s image=%s symptoms=%q", result, image, symptoms)
}

// unlinkLink runs the snapshot-mode link from source to destination.
func unlinkLink(t *testing.T, ctx context.Context, source, destination *ceph.Container) *rbdmirror.Container {
	t.Helper()
	link, err := rbdmirror.Run(ctx, image, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: unlinkPool,
		SourceSite: "source", DestinationSite: "destination",
	}))
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && link.Container != nil {
				harness.MultiClusterLogContainer(t, cleanupCtx, link.Container)
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate rbdmirror link: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

// unlinkSourcePeer returns the UUID of the source pool's peer for the
// destination: the peer a primary mirror snapshot links.
func unlinkSourcePeer(t *testing.T, ctx context.Context, link *rbdmirror.Container) string {
	t.Helper()
	var info struct {
		Peers []struct {
			UUID     string `json:"uuid"`
			SiteName string `json:"site_name"`
		} `json:"peers"`
	}
	out := harness.RBDScenarioCommand(t, ctx, link.SourceRBD, "mirror", "pool", "info", unlinkPool, "--format", "json")
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatal(err)
	}
	for _, peer := range info.Peers {
		if peer.SiteName == "destination" && peer.UUID != "" {
			return peer.UUID
		}
	}
	t.Fatalf("source pool has no peer for the destination: %s", out)
	return ""
}

func unlinkWrite(t *testing.T, ctx context.Context, client testcontainers.Container, data []byte) {
	t.Helper()
	harness.RBDMultiClusterWriteRange(t, ctx, client, unlinkImage, unlinkImageSize, 0, data)
	harness.ExecCommand(t, ctx, client, "rm", "-f", "/tmp/rbd-fixture-write.diff")
}

func unlinkListing(t *testing.T, ctx context.Context, client testcontainers.Container, image string) []byte {
	t.Helper()
	code, out, err := harness.RBDMultiClusterExec(ctx, client, "rbd", "snap", "ls", "--all", "--format", "json", image)
	if err != nil || code != 0 {
		t.Fatalf("snap ls %s: exit %d, %v: %s", image, code, err, out)
	}
	return out
}

func unlinkSnapshots(t *testing.T, ctx context.Context, client testcontainers.Container, image string) []unlinkSnapshot {
	t.Helper()
	snaps, err := parseUnlinkSnapshots(unlinkListing(t, ctx, client, image))
	if err != nil {
		t.Fatal(err)
	}
	return snaps
}

// unlinkNewMirrorSnapshot returns the one complete primary mirror snapshot
// that the listing before did not hold.
func unlinkNewMirrorSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, before []byte) unlinkSnapshot {
	t.Helper()
	old, err := parseUnlinkSnapshots(before)
	if err != nil {
		t.Fatal(err)
	}
	var fresh []unlinkSnapshot
	for _, snap := range unlinkSnapshots(t, ctx, client, unlinkImage) {
		if _, ok := findUnlinkSnapshot(old, snap.ID); !ok {
			fresh = append(fresh, snap)
		}
	}
	if len(fresh) != 1 || fresh[0].Namespace.Type != "mirror" || fresh[0].Namespace.State != "primary" || fresh[0].Namespace.Complete == nil || !*fresh[0].Namespace.Complete {
		t.Fatalf("expected one new complete primary mirror snapshot: %+v", fresh)
	}
	return fresh[0]
}

func unlinkReceiverLog(ctx context.Context, daemon *rbdmirror.Daemon) (string, error) {
	logs, err := daemon.Logs(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = logs.Close() }()
	data, err := io.ReadAll(logs)
	return string(data), err
}

// unlinkWaitBytes waits for the destination image to read back as want.
func unlinkWaitBytes(ctx context.Context, client testcontainers.Container, want []byte) error {
	observed, err := harness.Observe(ctx, 4*time.Minute, 30*time.Second, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		_, _, _ = harness.RBDMultiClusterExec(probeCtx, client, "rm", "-f", "/tmp/rbd-unlink-result")
		code, out, err := harness.RBDMultiClusterExec(probeCtx, client, "rbd", "export", unlinkImage, "/tmp/rbd-unlink-result", "--no-progress")
		if err != nil || code != 0 {
			return fmt.Sprintf("export exit %d, %v: %s", code, err, out), false, nil
		}
		reader, err := client.CopyFileFromContainer(probeCtx, "/tmp/rbd-unlink-result")
		if err != nil {
			return err.Error(), false, nil
		}
		defer func() { _ = reader.Close() }()
		data, err := io.ReadAll(reader)
		if err != nil {
			return err.Error(), false, nil
		}
		return fmt.Sprintf("%d bytes sha256=%x", len(data), sha256.Sum256(data)), bytes.Equal(data, want), nil
	})
	if err != nil {
		return fmt.Errorf("%w; last %s", err, observed.Value)
	}
	return nil
}
