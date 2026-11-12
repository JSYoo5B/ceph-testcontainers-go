//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_demote_restart)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rbd-aux

package rbd_mirror_demote_restart_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// rbdDemoteRestartImageSize is rewritten before the demotion, so the
	// demotion snapshot carries the whole image. Through
	// rbdDemoteRestartRate its copy takes about a minute per image, long
	// enough to stop the receiver while the copy is observably incomplete.
	rbdDemoteRestartImageSize = 16 << 20
	rbdDemoteRestartRate      = 2 * fault.MegabitPerSecond
)

// rbdMirrorSnapshot is one entry of rbd snap ls --all with the mirror
// snapshot namespace fields that tell a copied demotion snapshot apart.
type rbdMirrorSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Namespace struct {
		Type              string `json:"type"`
		State             string `json:"state"`
		Complete          *bool  `json:"complete"`
		PrimaryMirrorUUID string `json:"primary_mirror_uuid"`
		PrimarySnapID     uint64 `json:"primary_snap_id"`
		LastCopiedObject  uint64 `json:"last_copied_object_number"`
	} `json:"namespace"`
}

func rbdMirrorSnapshots(ctx context.Context, client testcontainers.Container, image string) ([]rbdMirrorSnapshot, string, error) {
	code, out, err := harness.RBDMultiClusterExec(ctx, client, "rbd", "snap", "ls", "--all", "--format", "json", image)
	if err != nil || code != 0 {
		return nil, string(out), fmt.Errorf("snap ls %s: exit %d, %v: %s", image, code, err, out)
	}
	var snaps []rbdMirrorSnapshot
	if err := json.Unmarshal(out, &snaps); err != nil {
		return nil, string(out), fmt.Errorf("snap ls %s: %w: %s", image, err, out)
	}
	return snaps, string(out), nil
}

// rbdIncompleteDemotion reports whether the newest snapshot of a
// non-primary image is the copy of the primary's demotion snapshot that the
// receiver has created but not completed.
func rbdIncompleteDemotion(snaps []rbdMirrorSnapshot) bool {
	if len(snaps) == 0 {
		return false
	}
	last := snaps[len(snaps)-1].Namespace
	return last.Type == "mirror" && last.State == "demoted" && last.PrimaryMirrorUUID != "" && last.Complete != nil && !*last.Complete
}

// TestRBDMirrorDemotionResumesAfterReceiverRestart kills the receiver while
// it copies a planned demotion snapshot and restarts the same daemon
// container. The rewritten image's demotion snapshot reaches the destination
// through a limited source OSD path, so the copy is observed incomplete before
// and after the SIGKILL. After the restart the same snapshot must complete; a
// receiver that treats the incomplete demotion copy as an orphan never
// resumes it (observed on Squid 19.2.3). A second image, whose demoted
// primary is force-disabled and removed while the receiver is down, must be
// removed on the destination too, because the incomplete copy leaves that
// image non-primary. The destination must then hold the source's exact
// bytes, be promoted without force, and replicate its next write back.
//
// It reports two units. rbd_mirror_demote_restart/demotion-resume is the
// orphan classification of the incomplete copy (Squid 19.2.3): REPRODUCED
// when the copy never completes. rbd_mirror_demote_restart/resumed-bytes is
// REPRODUCED on every qualified image that resumes (19.2.4, 19.2.5, 20.2.4,
// 21.2.0): the resumed copy reports complete but skips the object numbered
// last_copied_object_number at the time of the kill, which keeps the
// previous snapshot's bytes. An ordinary mirror snapshot copy shows the same
// loss, so it is not specific to demotion. The promotion and the reverse
// replication run only after both symptoms are absent.
func TestRBDMirrorDemotionResumesAfterReceiverRestart(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorDemotionResumesAfterReceiverRestart)
	}
}

func testRBDMirrorDemotionResumesAfterReceiverRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	resume := harness.StartReproduction(t, "rbd_mirror_demote_restart/demotion-resume", "resume")
	loss := harness.StartReproduction(t, "rbd_mirror_demote_restart/resumed-bytes", "exact-bytes")
	netem := harness.NetemImage(t)
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "tc-rbd-demote"
	const resumed, removed = pool + "/resumed", pool + "/removed"
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := harness.RBDScenarioRunLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := harness.RBDScenarioRunLink(t, ctx, destination, source, pool, "destination", "source")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")

	before := map[string][]byte{}
	for index, image := range []string{resumed, removed} {
		before[image] = harness.RBDMultiClusterPayload(rbdDemoteRestartImageSize, 51+index)
		if err := sourceClient.CopyToContainer(ctx, before[image], "/tmp/rbd-demote-seed", 0o600); err != nil {
			t.Fatal(err)
		}
		harness.ExecCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-demote-seed", image, "--object-size", "256K", "--image-feature", "layering,exclusive-lock", "--no-progress")
		harness.ExecCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-demote-seed")
		harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", image, "snapshot")
	}
	for _, image := range []string{resumed, removed} {
		harness.RBDScenarioWaitBytes(t, ctx, destinationClient, image, before[image])
	}

	daemon := forward.Daemons()[0]
	inspect, err := daemon.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[harness.StaticFacts(source).NetworkName]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatalf("daemon %s has no endpoint on %s", daemon.DaemonName, harness.StaticFacts(source).NetworkName)
	}
	// Only the source OSD's packets to the forward receiver are limited; the
	// source client and the reverse receiver keep their full-speed paths.
	limit := fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: endpoint.IPAddress, Rate: rbdDemoteRestartRate}
	lease, err := fault.ApplyNetworkProfile(ctx, source, clusteraccess.OSDs(t, source)[0].Container, limit)
	if lease != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := lease.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	after := map[string][]byte{}
	for index, image := range []string{resumed, removed} {
		after[image] = harness.RBDMultiClusterPayload(rbdDemoteRestartImageSize, 71+index)
		harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, rbdDemoteRestartImageSize, 0, after[image])
		harness.ExecCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-fixture-write.diff")
	}
	// No mirror snapshot follows the rewrite: the demotion snapshot alone
	// carries the new bytes.
	for _, image := range []string{resumed, removed} {
		harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	}
	for _, image := range []string{resumed, removed} {
		harness.VerifyRBDBytes(t, ctx, sourceClient, image, after[image])
	}
	for _, image := range []string{resumed, removed} {
		observed, err := harness.Observe(ctx, 2*time.Minute, 20*time.Second, time.Second, func(probeCtx context.Context) (string, bool, error) {
			snaps, raw, err := rbdMirrorSnapshots(probeCtx, destinationClient, image)
			if err != nil {
				return raw, false, nil
			}
			if n := len(snaps); n > 0 && snaps[n-1].Namespace.State == "demoted" && snaps[n-1].Namespace.Complete != nil && *snaps[n-1].Namespace.Complete {
				return raw, false, fmt.Errorf("the demotion snapshot of %s completed under the limit", image)
			}
			return raw, rbdIncompleteDemotion(snaps), nil
		})
		if err != nil {
			resume.Inconclusive(t, "condition not established: no incomplete demotion copy of %s: %v; last %s", image, err, observed.Value)
		}
		t.Logf("destination %s copies the demotion snapshot: %s", image, observed.Value)
	}

	// An abrupt stop of the same receiver container, then the condition
	// again: the demotion copies are still incomplete.
	rbdKillContainer(t, ctx, daemon)
	demotion := map[string]rbdMirrorSnapshot{}
	for _, image := range []string{resumed, removed} {
		snaps, raw, err := rbdMirrorSnapshots(ctx, destinationClient, image)
		if err != nil || !rbdIncompleteDemotion(snaps) {
			resume.Inconclusive(t, "condition not established: after the stop %s holds no incomplete demotion copy: %s %v", image, raw, err)
		}
		demotion[image] = snaps[len(snaps)-1]
		t.Logf("after the stop %s holds demotion copy %d at %s", image, demotion[image].ID, raw)
	}
	// The demoted primary of the second image is removed while the receiver
	// is down; force is needed because the source is no longer primary.
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "disable", "--force", removed)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "rm", "--no-progress", removed)
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := daemon.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// The same demotion snapshot completes, with the source's exact bytes.
	completed, err := harness.Observe(ctx, 5*time.Minute, 20*time.Second, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		snaps, raw, err := rbdMirrorSnapshots(probeCtx, destinationClient, resumed)
		if err != nil {
			return raw, false, nil
		}
		for _, snap := range snaps {
			if snap.ID == demotion[resumed].ID {
				if snap.Namespace.PrimarySnapID != demotion[resumed].Namespace.PrimarySnapID {
					return raw, false, fmt.Errorf("snapshot %d now copies primary snapshot %d", snap.ID, snap.Namespace.PrimarySnapID)
				}
				return raw, snap.Namespace.Complete != nil && *snap.Namespace.Complete, nil
			}
		}
		return raw, false, fmt.Errorf("demotion copy %d is gone", demotion[resumed].ID)
	})
	last := "snapshots " + completed.Value
	if err != nil {
		status, statusErr := forward.ImageStatus(ctx, "resumed")
		last += fmt.Sprintf("; status %+v %v", status, statusErr)
	}
	timedOut := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
	symptom, detail, judged := judgeDemotionResume(err == nil, timedOut, err, demotion[resumed].ID, last)
	switch {
	case judged != nil:
		resume.Inconclusive(t, "%v", judged)
	case symptom:
		resume.Symptom(t, "resume", "%s", detail)
		resume.Conclude()
		// The bytes of a copy that never completed say nothing about the
		// resumed copy.
		if loss.Expected() {
			loss.Inconclusive(t, "condition not established: the demotion copy did not complete after the restart")
		}
		t.Logf("resumed-bytes not evaluated: the demotion copy did not complete after the restart")
		return
	default:
		resume.Absent(t, "resume", "%s", detail)
		resume.Conclude()
	}
	t.Logf("demotion copy %d of %s completed after the restart: %s", demotion[resumed].ID, resumed, completed.Value)

	data := rbdDemoteRestartWaitBytes(t, ctx, destinationClient, resumed, after[resumed])
	symptom, detail, judged = judgeResumedBytes(data, before[resumed], after[resumed], demotion[resumed].Namespace.LastCopiedObject)
	switch {
	case judged != nil:
		loss.Inconclusive(t, "%v", judged)
	case symptom:
		// Promotion and the reverse replication would only carry the
		// wrong bytes back.
		loss.Symptom(t, "exact-bytes", "%s", detail)
		loss.Conclude()
		return
	default:
		loss.Absent(t, "exact-bytes", "%s", detail)
		loss.Conclude()
	}

	gone, err := harness.Observe(ctx, 5*time.Minute, 20*time.Second, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		code, out, err := harness.RBDMultiClusterExec(probeCtx, destinationClient, "rbd", "ls", "--format", "json", pool)
		if err != nil || code != 0 {
			return string(out), false, nil
		}
		var names []string
		if err := json.Unmarshal(out, &names); err != nil {
			return string(out), false, err
		}
		for _, name := range names {
			if pool+"/"+name == removed {
				return string(out), false, nil
			}
		}
		return string(out), true, nil
	})
	if err != nil {
		t.Fatalf("the destination retains %s after its demoted primary was removed: %v; images %s", removed, err, gone.Value)
	}

	// Planned failover completes: non-forced promotion, then the new
	// primary's next write and checkpoint replicate back to the old source.
	harness.RBDMultiClusterPromote(t, ctx, destinationClient, resumed)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, resumed, true)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, resumed, false)
	final := bytes.Clone(after[resumed])
	patch := harness.RBDMultiClusterPayload(1<<20, 89)
	copy(final[2<<20:], patch)
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, resumed, rbdDemoteRestartImageSize, 2<<20, patch)
	harness.RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "snapshot", resumed)
	harness.RBDScenarioWaitBytes(t, ctx, sourceClient, resumed, final)
	t.Logf("demotion copy %d of %s completed after a restart of receiver %s; %s removed with its demoted primary; promoted destination replicated back", demotion[resumed].ID, resumed, daemon.DaemonName, removed)
}

// rbdKillContainer sends SIGKILL to the container's process, so the daemon
// gets no chance to finish or record in-flight work, and waits until the
// container is no longer running.
func rbdKillContainer(t *testing.T, parent context.Context, ctr testcontainers.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ContainerKill(ctx, ctr.GetContainerID(), mobycl.ContainerKillOptions{Signal: "KILL"}); err != nil {
		t.Fatal(err)
	}
	for {
		state, err := ctr.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("killed container %s still running: %v", ctr.GetContainerID(), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// rbdDemoteRestartWaitBytes waits up to four minutes for image to read back
// as after and returns the last export, nil when no export succeeded.
func rbdDemoteRestartWaitBytes(t *testing.T, ctx context.Context, client testcontainers.Container, image string, after []byte) []byte {
	t.Helper()
	read := func(probeCtx context.Context) ([]byte, error) {
		_, _, _ = harness.RBDMultiClusterExec(probeCtx, client, "rm", "-f", "/tmp/rbd-demote-result")
		code, out, err := harness.RBDMultiClusterExec(probeCtx, client, "rbd", "export", image, "/tmp/rbd-demote-result", "--no-progress")
		if err != nil || code != 0 {
			return nil, fmt.Errorf("export exit %d, %v: %s", code, err, out)
		}
		reader, err := client.CopyFileFromContainer(probeCtx, "/tmp/rbd-demote-result")
		if err != nil {
			return nil, err
		}
		defer func() { _ = reader.Close() }()
		return io.ReadAll(reader)
	}
	var last []byte
	_, err := harness.Observe(ctx, 4*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) ([]byte, bool, error) {
		data, err := read(probeCtx)
		if err != nil {
			return nil, false, nil
		}
		last = data
		return data, bytes.Equal(data, after), nil
	})
	if err == nil {
		t.Logf("RBD %s holds the demotion snapshot's bytes: sha256=%x", image, sha256.Sum256(after))
	} else {
		t.Logf("RBD %s does not hold the demotion snapshot's bytes: %v", image, err)
	}
	return last
}
