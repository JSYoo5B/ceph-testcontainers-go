//go:build all || (integration && features && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rbd_mirror_path_fault))))

//ci: timeout=30m job-timeout=40 representative=mirroring

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// TestRBDMirrorPeerPathCut cuts one path of an RBD snapshot mirror: the
// source OSD's packets to the destination's rbd-mirror daemon. The daemon
// cannot copy a new mirror snapshot while the source client writes and reads
// through the same OSD; after the cut is restored the snapshot arrives.
func TestRBDMirrorPeerPathCut(t *testing.T) {
	for _, mode := range networkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorPeerPathCut)
	}
}

func testRBDMirrorPeerPathCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	netem := netemImage(t)
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, ceph.WithOSDCount(1))
	const pool = "tc-rbd-path"
	const image = pool + "/cut"
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rbd", PGNum: 1}); err != nil {
			t.Fatal(err)
		}
		if err := rbd.InitPool(ctx, cluster, pool); err != nil {
			t.Fatal(err)
		}
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mirror, err := rbd.RunMirror(ctx, nativeMirrorImage(t, "rbd-aux", source.ControlImage()), rbd.MirrorConfig{Source: source, Destination: destination, Pool: pool})
	if mirror != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := mirror.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	before := rbdMultiClusterPayload(4<<20, 17)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-path-seed", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-path-seed", image, "--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
	if err := mirror.EnableImage(ctx, "cut"); err != nil {
		t.Fatal(err)
	}
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, before)

	daemon := mirror.Daemons()[0]
	inspect, err := daemon.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[source.NetworkName()]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatalf("daemon %s has no endpoint on %s", daemon.DaemonName, source.NetworkName())
	}
	peer := endpoint.IPAddress
	osd := source.OSDs()[0]
	// The status reports the local snapshot a little after its bytes arrive.
	synced, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (rbdMirrorProgress, bool, error) {
		progress, err := rbdMirrorImageProgress(probeCtx, mirror, "cut")
		return progress, err == nil && progress.Remote != 0 && progress.Local == progress.Remote, err
	})
	if err != nil {
		t.Fatalf("the first snapshot is not reported in sync: %+v, %v", synced.Value, err)
	}
	baseline := synced.Value

	// osd.0 drops every packet it sends to the daemon's address on the
	// source network, and only those: the daemon's requests reach the OSD,
	// its replies do not.
	lease, err := fault.ApplyNetworkProfile(ctx, source, osd.Container, fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: peer, Loss: 100})
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
	after := append([]byte{}, before...)
	patch := rbdMultiClusterPayload(1<<20, 99)
	copy(after[2<<20:], patch)
	// The source client writes and snapshots through the same OSD.
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, uint64(len(before)), 2<<20, patch)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	// For 30 s the daemon stays up, but neither sees the new remote snapshot
	// nor copies anything.
	var samples int
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
		progress := rbdMirrorSnapshotProgress(t, ctx, mirror, "cut")
		if progress.State != "up+replaying" || progress.Remote != baseline.Remote || progress.Local != baseline.Local {
			t.Fatalf("the cut daemon advanced: %+v, before %+v", progress, baseline)
		}
		samples++
	}
	verifyRBDBytes(t, ctx, destinationClient, image, before)
	verifyRBDBytes(t, ctx, sourceClient, image, after)
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	restored := time.Now()
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, after)
	took := time.Since(restored)
	reported, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (rbdMirrorProgress, bool, error) {
		progress, err := rbdMirrorImageProgress(probeCtx, mirror, "cut")
		return progress, err == nil && progress.Remote > baseline.Remote && progress.Local == progress.Remote, err
	})
	if err != nil {
		t.Fatalf("the new snapshot is not reported in sync: %+v, %v", reported.Value, err)
	}
	final := reported.Value
	if final.Remote <= baseline.Remote || final.Local != final.Remote || final.LastSnapshotBytes != int64(len(patch)) {
		t.Fatalf("after the restore: %+v, before %+v; want the new snapshot in sync with %d bytes copied", final, baseline, len(patch))
	}
	t.Logf("RBD_MIRROR_PATH_RECEIPT osd.%d dropped %d packets to rbd-mirror %s (%s) on the source network; for %d samples over 30 s the daemon stayed %s at remote/local snapshot %d while the source client wrote and snapshotted through osd.%d; the destination kept the old bytes; %s after the restore the snapshot of %d arrived, %d bytes copied",
		osd.ID, cut.Drops, daemon.DaemonName, peer, samples, final.State, baseline.Remote, osd.ID, took.Round(time.Second), final.Remote, final.LastSnapshotBytes)
}

// rbdMirrorProgress is the snapshot progress that a destination image's
// mirror status description carries.
type rbdMirrorProgress struct {
	State             string
	Remote            int64  `json:"remote_snapshot_timestamp"`
	Local             int64  `json:"local_snapshot_timestamp"`
	LastSnapshotBytes int64  `json:"last_snapshot_bytes"`
	ReplayState       string `json:"replay_state"`
}

// rbdMirrorSnapshotProgress reads the destination's mirror status of image
// and decodes the JSON after "replaying, " in its description.
func rbdMirrorSnapshotProgress(t *testing.T, ctx context.Context, mirror *rbd.Mirror, image string) rbdMirrorProgress {
	t.Helper()
	progress, err := rbdMirrorImageProgress(ctx, mirror, image)
	if err != nil {
		t.Fatal(err)
	}
	return progress
}

// rbdMirrorImageProgress is rbdMirrorSnapshotProgress that returns its error.
func rbdMirrorImageProgress(ctx context.Context, mirror *rbd.Mirror, image string) (rbdMirrorProgress, error) {
	status, err := mirror.ImageStatus(ctx, image)
	if err != nil {
		return rbdMirrorProgress{}, fmt.Errorf("mirror status of %s: %w", image, err)
	}
	_, detail, ok := strings.Cut(status.Description, ", ")
	var progress rbdMirrorProgress
	if !ok || json.Unmarshal([]byte(detail), &progress) != nil {
		return rbdMirrorProgress{}, fmt.Errorf("mirror status of %s has no snapshot progress: %q", image, status.Description)
	}
	progress.State = status.State
	return progress, nil
}
