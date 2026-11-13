//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_demote_restart)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rbd-aux

package rbd_mirror_demote_restart_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
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
	// rbdDemoteRestartImageSize is rewritten before the demotion, so the
	// demotion snapshot carries the whole image. Through
	// rbdDemoteRestartRate its copy takes about a minute per image, long
	// enough to stop the receiver while the copy is observably incomplete.
	rbdDemoteRestartImageSize = 16 << 20
	rbdDemoteRestartRate      = 2 * fault.MegabitPerSecond
)

// image is the Ceph image every cluster, client and receiver of the case
// runs: Squid 19.2.3, which reproduces demotion-resume. A run selects another
// image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_demote_restart_test.image=<image>"
//
// Squid 19.2.4 and Tentacle 20.2.1 fix demotion-resume. resumed-bytes is
// evaluated only on an image that resumes the copy, and every such image
// qualified so far (19.2.4, 19.2.5, 20.2.4, 21.2.0) reproduces it; no image
// reproduces both.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:20c0e5b9d77cf1a45da918d090681744f610977c790f15cb8a6646f35fd8d44c"

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

// TestRBDMirrorDemotionResumesAfterReceiverRestart reports two defects of a
// receiver that is killed while it copies a planned demotion snapshot and is
// started again as the same container.
//
// Given two clusters whose images read back exactly on the destination, and
// a demotion snapshot of each rewritten image copied through a limited source
// OSD path, observed incomplete before and after the SIGKILL of the receiver,
// when the demoted primary of the second image is force-disabled and removed
// while the receiver is down and the receiver is started again, then:
//
//   - demotion-resume is REPRODUCED if the same demotion snapshot does not
//     complete or the removed image's replica is not removed, and
//     NOT_REPRODUCED if both happen. A receiver that treats the incomplete
//     demotion copy as an orphan never resumes it (Squid 19.2.3).
//   - resumed-bytes, evaluated only once the copy completed, is REPRODUCED if
//     the destination does not hold the source's exact bytes, and
//     NOT_REPRODUCED if it does. The resumed copy of 19.2.4, 19.2.5, 20.2.4
//     and 21.2.0 skips the object numbered last_copied_object_number at the
//     kill, which keeps the previous snapshot's bytes.
//
// When neither is reproduced, the destination must also be promoted without
// force and replicate its next write back.
//
// A failure of the given or when steps fails the test; the result does not.
func TestRBDMirrorDemotionResumesAfterReceiverRestart(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorDemotionResumesAfterReceiverRestart)
	}
}

func testRBDMirrorDemotionResumesAfterReceiverRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	t.Logf("image: %s", image)
	source, sourceClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	destination, destinationClient := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("ceph: source %s, destination %s", harness.StaticFacts(source).CephVersion, harness.StaticFacts(destination).CephVersion)
	const pool = "tc-rbd-demote"
	const resumed, removed = pool + "/resumed", pool + "/removed"
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	forward := rbdDemoteRestartLink(t, ctx, source, destination, pool, "source", "destination")
	reverse := rbdDemoteRestartLink(t, ctx, destination, source, pool, "destination", "source")
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
			t.Fatalf("incomplete demotion copy of %s: %v; last %s", image, err, observed.Value)
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
			t.Fatalf("after the stop %s holds no incomplete demotion copy: %s %v", image, raw, err)
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

	// Each symptom observed by the then step, by defect; none means
	// NOT_REPRODUCED.
	var resumeSymptoms, bytesSymptoms []string
	report := func(unit string, symptoms []string) {
		result := "NOT_REPRODUCED"
		if len(symptoms) > 0 {
			result = "REPRODUCED"
		}
		t.Logf("EXPERIMENT_RESULT case=rbd_mirror_demote_restart/%s result=%s image=%s symptoms=%q", unit, result, image, symptoms)
	}

	// demotion-resume: the same demotion snapshot completes, and the replica
	// of the removed primary is removed.
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
	resumedCopy := err == nil
	if resumedCopy {
		t.Logf("demotion copy %d of %s completed after the restart: %s", demotion[resumed].ID, resumed, completed.Value)
	} else {
		status, statusErr := forward.ImageStatus(ctx, "resumed")
		resumeSymptoms = append(resumeSymptoms, fmt.Sprintf("the restarted receiver did not complete demotion copy %d of %s: %v; snapshots %s; status %+v %v", demotion[resumed].ID, resumed, err, completed.Value, status, statusErr))
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
		resumeSymptoms = append(resumeSymptoms, fmt.Sprintf("the destination retains %s after its demoted primary was removed: %v; images %s", removed, err, gone.Value))
	}
	if t.Failed() {
		return
	}
	report("demotion-resume", resumeSymptoms)
	if !resumedCopy {
		t.Logf("resumed-bytes not evaluated: demotion copy %d of %s did not complete", demotion[resumed].ID, resumed)
		return
	}

	// resumed-bytes: the completed copy holds the demotion snapshot's bytes.
	if symptom := rbdDemoteRestartWaitBytes(t, ctx, destinationClient, resumed, before[resumed], after[resumed]); symptom != "" {
		bytesSymptoms = append(bytesSymptoms, symptom)
	}
	if t.Failed() {
		return
	}
	report("resumed-bytes", bytesSymptoms)
	if len(resumeSymptoms)+len(bytesSymptoms) != 0 {
		t.Logf("promotion and reverse replay not run: a defect was reproduced")
		return
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

// rbdDemoteRestartLink runs one snapshot-mode rbdmirror link of image from
// source to destination.
func rbdDemoteRestartLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *rbdmirror.Container {
	t.Helper()
	link, err := rbdmirror.Run(ctx, image, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite,
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

// rbdDemoteRestartWaitBytes waits for image to read back as after. On a
// timeout it returns the symptom, which classifies every 256 KiB object as
// holding the new bytes, the old bytes or neither.
func rbdDemoteRestartWaitBytes(t *testing.T, ctx context.Context, client testcontainers.Container, image string, before, after []byte) string {
	t.Helper()
	const object = 256 << 10
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
	observed, err := harness.Observe(ctx, 4*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) ([]byte, bool, error) {
		data, err := read(probeCtx)
		if err != nil {
			return nil, false, nil
		}
		return data, bytes.Equal(data, after), nil
	})
	if err == nil {
		t.Logf("RBD %s holds the demotion snapshot's bytes: sha256=%x", image, sha256.Sum256(after))
		return ""
	}
	classify := func(data []byte) string {
		if len(data) != len(after) {
			return fmt.Sprintf("%d bytes, want %d", len(data), len(after))
		}
		var fresh, stale, other []int
		for index := 0; index*object < len(after); index++ {
			chunk := data[index*object : (index+1)*object]
			switch {
			case bytes.Equal(chunk, after[index*object:(index+1)*object]):
				fresh = append(fresh, index)
			case bytes.Equal(chunk, before[index*object:(index+1)*object]):
				stale = append(stale, index)
			default:
				other = append(other, index)
			}
		}
		return fmt.Sprintf("objects new=%d old=%v neither=%v", len(fresh), stale, other)
	}
	return fmt.Sprintf("%s does not hold the demotion snapshot's bytes: %v; %s", image, err, classify(observed.Value))
}
