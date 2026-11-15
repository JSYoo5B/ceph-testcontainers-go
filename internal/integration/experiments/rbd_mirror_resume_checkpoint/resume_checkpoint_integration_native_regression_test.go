//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_resume_checkpoint)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rbd-aux

package rbd_mirror_resume_checkpoint_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// checkpointImageSize is rewritten before every mirror snapshot, so the
	// snapshot carries all 64 objects. Through checkpointRate the copy
	// takes about a minute.
	checkpointImageSize = 16 << 20
	checkpointRate      = 2 * fault.MegabitPerSecond
	// checkpointKillAt is the progress the destination must report before
	// the receiver is killed: far enough into the copy for a positive
	// checkpoint, far from its end.
	checkpointKillAt = 8
	// checkpointAttempts bounds the when step: an attempt whose kill missed
	// the copy of the checkpoint object is reported and repeated with a new
	// snapshot.
	checkpointAttempts = 3
)

// image is the Ceph image every cluster, client and receiver of the case
// runs: Tentacle 20.2.4, which reproduces the defect. A run selects another
// image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_resume_checkpoint_test.image=<image>"
//
// No published release resumes the copy at the checkpoint object.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

// mirrorSnapshot is one entry of rbd snap ls --all with the mirror snapshot
// namespace fields of a copy and its progress.
type mirrorSnapshot struct {
	ID        uint64 `json:"id"`
	Namespace struct {
		Type              string `json:"type"`
		State             string `json:"state"`
		Complete          *bool  `json:"complete"`
		PrimaryMirrorUUID string `json:"primary_mirror_uuid"`
		PrimarySnapID     uint64 `json:"primary_snap_id"`
		LastCopiedObject  uint64 `json:"last_copied_object_number"`
	} `json:"namespace"`
}

func (s mirrorSnapshot) complete() bool {
	return s.Namespace.Complete != nil && *s.Namespace.Complete
}

func mirrorSnapshots(ctx context.Context, client testcontainers.Container, image string) ([]mirrorSnapshot, string, error) {
	code, out, err := harness.RBDMultiClusterExec(ctx, client, "rbd", "snap", "ls", "--all", "--format", "json", image)
	if err != nil || code != 0 {
		return nil, string(out), fmt.Errorf("snap ls %s: exit %d, %v: %s", image, code, err, out)
	}
	var snaps []mirrorSnapshot
	if err := json.Unmarshal(out, &snaps); err != nil {
		return nil, string(out), fmt.Errorf("snap ls %s: %w: %s", image, err, out)
	}
	return snaps, string(out), nil
}

// copyOf returns the destination's copy of primary snapshot primaryID.
func copyOf(snaps []mirrorSnapshot, primaryID uint64) (mirrorSnapshot, bool) {
	for _, snap := range snaps {
		if snap.Namespace.Type == "mirror" && snap.Namespace.State == "non-primary" && snap.Namespace.PrimaryMirrorUUID != "" && snap.Namespace.PrimarySnapID == primaryID {
			return snap, true
		}
	}
	return mirrorSnapshot{}, false
}

// TestRBDMirrorResumedCopyRecopiesCheckpointObject reports whether a
// receiver that is killed during a mirror snapshot copy and started again
// copies the object at its persisted checkpoint.
//
// Given two clusters whose image reads back exactly on the destination, and
// a mirror snapshot of the rewritten image copied through a limited source
// OSD path, when the receiver is killed after it persisted a positive
// checkpoint (last_copied_object_number) while the object at that index
// still held the previous bytes on the destination, and the same receiver
// container is started again, then the result is REPRODUCED if the
// completed copy leaves that object with the previous bytes, and
// NOT_REPRODUCED if the destination holds exactly the snapshot's bytes. The
// checkpoint counts the objects copied, but a restart passes it as the index
// of the last copied object and resumes one object later.
//
// When it is not reproduced, a planned failover must follow: non-forced
// promotion of the destination and replication of its next write back.
//
// A failure of the given or when steps fails the test, the result does not:
// a zero checkpoint, a checkpoint object already copied at the kill (after
// every bounded attempt), or a copy that does not complete after the restart.
func TestRBDMirrorResumedCopyRecopiesCheckpointObject(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorResumedCopyRecopiesCheckpointObject)
	}
}

func testRBDMirrorResumedCopyRecopiesCheckpointObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	t.Logf("image: %s", image)
	source, sourceClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	destination, destinationClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("ceph: source %s, destination %s", harness.StaticFacts(source).CephVersion, harness.StaticFacts(destination).CephVersion)
	const pool = "tc-rbd-checkpoint"
	const mirrored = pool + "/resumed"
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := checkpointLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := checkpointLink(t, ctx, destination, source, pool, "destination", "source")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	harness.RBDScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")

	// Control: the image enabled at full speed reads back exactly.
	before := harness.RBDMultiClusterPayload(checkpointImageSize, 51)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-checkpoint-seed", 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-checkpoint-seed", mirrored, "--object-size", "256K", "--image-feature", "layering,exclusive-lock", "--no-progress")
	harness.ExecCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-checkpoint-seed")
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "enable", mirrored, "snapshot")
	harness.RBDScenarioWaitBytes(t, ctx, destinationClient, mirrored, before)

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
	// clients and the reverse receiver keep their full-speed paths.
	limit := fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: endpoint.IPAddress, Rate: checkpointRate}
	osd := clusteraccess.OSDs(t, source)[0].Container

	var (
		after      []byte
		checkpoint uint64
		resumed    mirrorSnapshot
		final      []byte
		missed     []string
	)
	for attempt := 1; ; attempt++ {
		if attempt > checkpointAttempts {
			t.Fatalf("no attempt interrupted the copy of the checkpoint object: %s", strings.Join(missed, "; "))
		}
		after = harness.RBDMultiClusterPayload(checkpointImageSize, 70+attempt)
		lease, err := fault.ApplyNetworkProfile(ctx, source, osd, limit)
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
		harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, mirrored, checkpointImageSize, 0, after)
		harness.ExecCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-fixture-write.diff")
		harness.VerifyRBDBytes(t, ctx, sourceClient, mirrored, after)
		harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "snapshot", mirrored)
		primaryID := checkpointNewestPrimary(t, ctx, sourceClient, mirrored)

		// The kill follows a persisted checkpoint of at least
		// checkpointKillAt objects whose object the destination has not
		// received yet.
		window := checkpointAwaitWindow(t, ctx, destinationClient, mirrored, primaryID, before, after)
		t.Logf("attempt %d: killing the receiver at %s", attempt, window)
		checkpointKill(t, ctx, daemon)

		// With the receiver down, the persisted checkpoint and the
		// destination's objects are read without a race.
		snaps, raw, err := mirrorSnapshots(ctx, destinationClient, mirrored)
		if err != nil {
			t.Fatal(err)
		}
		snap, ok := copyOf(snaps, primaryID)
		if !ok || snap.complete() {
			t.Fatalf("attempt %d: after the kill the destination holds no incomplete copy of primary snapshot %d: %s", attempt, primaryID, raw)
		}
		checkpoint = snap.Namespace.LastCopiedObject
		atKill := checkpointRead(t, ctx, destinationClient, mirrored)
		missedWindow := checkKillWindow(checkpoint, atKill, before, after)
		t.Logf("attempt %d: copy %d of primary snapshot %d at the kill: last_copied_object_number=%d; %s", attempt, snap.ID, primaryID, checkpoint, raw)

		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if err := daemon.Start(ctx); err != nil {
			t.Fatal(err)
		}
		resumed = checkpointWaitComplete(t, ctx, forward, destinationClient, mirrored, snap)
		final = checkpointRead(t, ctx, destinationClient, mirrored)
		if missedWindow == nil {
			t.Logf("attempt %d: the kill interrupted the copy of object %d, the checkpoint", attempt, checkpoint)
			break
		}
		// A missed window is not a result. Its completed copy must still
		// read back exactly before the next attempt starts from it.
		missed = append(missed, fmt.Sprintf("attempt %d: %v", attempt, missedWindow))
		t.Logf("attempt %d missed the kill window: %v", attempt, missedWindow)
		if !bytes.Equal(final, after) {
			classes, err := objectClasses(final, before, after)
			t.Fatalf("attempt %d missed the kill window and its resumed copy %d does not read back exactly: %v %s", attempt, resumed.ID, err, classSummary(classes))
		}
		before = after
	}

	symptoms, err := judgeResumedCopy(checkpoint, final, before, after)
	if err != nil {
		t.Fatalf("resumed copy %d: %v", resumed.ID, err)
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rbd_mirror_resume_checkpoint result=%s image=%s symptoms=%q", result, image, symptoms)
	if len(symptoms) > 0 {
		t.Logf("promotion and reverse replay not run: the destination keeps a stale object")
		return
	}
	t.Logf("RBD %s holds the resumed snapshot's bytes: sha256=%x", mirrored, sha256.Sum256(after))

	// Planned failover from the exact copy: demotion, non-forced promotion,
	// then the new primary's next write and snapshot replicate back.
	harness.RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", mirrored)
	harness.RBDMultiClusterPromote(t, ctx, destinationClient, mirrored)
	harness.RBDMultiClusterAssertPrimary(t, ctx, destinationClient, mirrored, true)
	harness.RBDMultiClusterAssertPrimary(t, ctx, sourceClient, mirrored, false)
	replayed := bytes.Clone(after)
	patch := harness.RBDMultiClusterPayload(1<<20, 89)
	copy(replayed[2<<20:], patch)
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, mirrored, checkpointImageSize, 2<<20, patch)
	harness.RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "snapshot", mirrored)
	harness.RBDScenarioWaitBytes(t, ctx, sourceClient, mirrored, replayed)
}

// checkpointLink runs one snapshot-mode rbdmirror link of pool from source
// to destination.
func checkpointLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *rbdmirror.Container {
	t.Helper()
	link, err := rbdmirror.Run(ctx, image, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite,
	}))
	if link != nil {
		t.Cleanup(func() {
			// The diagnostics get their own short deadline, so a stalled
			// log read cannot leave the termination an expired context.
			if t.Failed() && link.Container != nil {
				logCtx, cancelLog := context.WithTimeout(context.Background(), 5*time.Second)
				harness.MultiClusterLogContainer(t, logCtx, link.Container)
				cancelLog()
			}
			terminateCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := link.Terminate(terminateCtx); err != nil {
				t.Errorf("terminate rbdmirror link: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

// checkpointNewestPrimary returns the ID of the newest primary mirror
// snapshot of the source image.
func checkpointNewestPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string) uint64 {
	t.Helper()
	snaps, raw, err := mirrorSnapshots(ctx, client, image)
	if err != nil {
		t.Fatal(err)
	}
	for index := len(snaps) - 1; index >= 0; index-- {
		if snaps[index].Namespace.Type == "mirror" && snaps[index].Namespace.State == "primary" {
			return snaps[index].ID
		}
	}
	t.Fatalf("source %s has no primary mirror snapshot: %s", image, raw)
	return 0
}

// checkpointKill sends SIGKILL to the receiver container's process, so the
// daemon cannot finish or record in-flight work, and waits until the
// container is no longer running.
func checkpointKill(t *testing.T, parent context.Context, ctr testcontainers.Container) {
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

// checkpointWaitComplete waits until the restarted receiver completes the
// same copy for the same primary snapshot.
func checkpointWaitComplete(t *testing.T, ctx context.Context, link *rbdmirror.Container, client testcontainers.Container, image string, interrupted mirrorSnapshot) mirrorSnapshot {
	t.Helper()
	var done mirrorSnapshot
	observed, err := harness.Observe(ctx, 5*time.Minute, 20*time.Second, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		snaps, raw, err := mirrorSnapshots(probeCtx, client, image)
		if err != nil {
			return raw, false, nil
		}
		for _, snap := range snaps {
			if snap.ID == interrupted.ID {
				if snap.Namespace.PrimarySnapID != interrupted.Namespace.PrimarySnapID {
					return raw, false, fmt.Errorf("copy %d now copies primary snapshot %d", snap.ID, snap.Namespace.PrimarySnapID)
				}
				done = snap
				return raw, snap.complete(), nil
			}
		}
		return raw, false, fmt.Errorf("copy %d is gone", interrupted.ID)
	})
	if err != nil {
		name := image[strings.LastIndex(image, "/")+1:]
		status, statusErr := link.ImageStatus(ctx, name)
		t.Fatalf("the restarted receiver did not complete copy %d of %s: %v; snapshots %s; status %+v %v", interrupted.ID, image, err, observed.Value, status, statusErr)
	}
	t.Logf("copy %d of %s completed after the restart: %s", interrupted.ID, image, observed.Value)
	return done
}

// checkpointAwaitWindow waits until the destination's copy of primary
// snapshot primaryID persisted a checkpoint of at least checkpointKillAt
// while the destination still holds the old bytes of the object at the
// checkpoint, and describes what it saw. A checkpoint whose object already
// arrived lags the copy (the receiver skips a progress update while another
// is in flight), and the wait goes on. The read after the kill decides
// whether the window held.
func checkpointAwaitWindow(t *testing.T, parent context.Context, client testcontainers.Container, image string, primaryID uint64, before, after []byte) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var lagging []uint64
	var last string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("no checkpoint of at least %d with its object still old in the copy of primary snapshot %d: %v; lagging checkpoints %v; last %s", checkpointKillAt, primaryID, ctx.Err(), lagging, last)
		case <-time.After(500 * time.Millisecond):
		}
		snaps, raw, err := mirrorSnapshots(ctx, client, image)
		if err != nil {
			last = err.Error()
			continue
		}
		snap, ok := copyOf(snaps, primaryID)
		if !ok {
			last = raw
			continue
		}
		if snap.complete() {
			t.Fatalf("copy %d of primary snapshot %d completed under the limit: %s", snap.ID, primaryID, raw)
		}
		checkpoint := snap.Namespace.LastCopiedObject
		last = fmt.Sprintf("last_copied_object_number=%d", checkpoint)
		if checkpoint < checkpointKillAt {
			continue
		}
		data, err := checkpointExport(ctx, client, image)
		if err != nil {
			last = err.Error()
			continue
		}
		if err := checkKillWindow(checkpoint, data, before, after); err != nil {
			if !slices.Contains(lagging, checkpoint) {
				lagging = append(lagging, checkpoint)
			}
			last = err.Error()
			continue
		}
		return fmt.Sprintf("checkpoint %d with object %d old before the kill (lagging checkpoints passed over: %v)", checkpoint, checkpoint, lagging)
	}
}

// checkpointExport exports the destination image's head once.
func checkpointExport(ctx context.Context, client testcontainers.Container, image string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, _ = harness.RBDMultiClusterExec(ctx, client, "rm", "-f", "/tmp/rbd-checkpoint-read")
	code, out, err := harness.RBDMultiClusterExec(ctx, client, "rbd", "export", image, "/tmp/rbd-checkpoint-read", "--no-progress")
	if err != nil || code != 0 {
		return nil, fmt.Errorf("export %s: exit %d, %v: %s", image, code, err, out)
	}
	reader, err := client.CopyFileFromContainer(ctx, "/tmp/rbd-checkpoint-read")
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

// checkpointRead exports the destination image's head, retrying a failed
// export for up to two minutes.
func checkpointRead(t *testing.T, ctx context.Context, client testcontainers.Container, image string) []byte {
	t.Helper()
	var read []byte
	observed, err := harness.Observe(ctx, 2*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		data, err := checkpointExport(probeCtx, client, image)
		if err != nil {
			return err.Error(), false, nil
		}
		read = data
		return "", true, nil
	})
	if err != nil {
		t.Fatalf("export %s: %v; last %s", image, err, observed.Value)
	}
	return read
}
