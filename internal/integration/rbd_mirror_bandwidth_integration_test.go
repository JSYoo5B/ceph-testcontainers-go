//go:build all || (integration && features && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rbd_mirror_bandwidth))))

//ci: timeout=30m job-timeout=40

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

const (
	// rbdMirrorBandwidthImageSize is large enough that a snapshot sync of
	// the whole image through rbdMirrorBandwidthRate takes minutes, longer
	// than two of the daemon's 30-second status updates.
	rbdMirrorBandwidthImageSize = 48 << 20
	rbdMirrorBandwidthRate      = 2 * ceph.MegabitPerSecond
)

// rbdMirrorSyncStatus is the snapshot progress in a destination image's
// mirror status description, with the fields that rbd-mirror adds while it
// syncs a snapshot.
type rbdMirrorSyncStatus struct {
	State              string
	Remote             int64    `json:"remote_snapshot_timestamp"`
	Local              int64    `json:"local_snapshot_timestamp"`
	Syncing            *int64   `json:"syncing_snapshot_timestamp"`
	SyncingPercent     *int64   `json:"syncing_percent"`
	BytesPerSecond     *float64 `json:"bytes_per_second"`
	SecondsUntilSynced *int64   `json:"seconds_until_synced"`
	LastSnapshotBytes  int64    `json:"last_snapshot_bytes"`
	LastSyncSeconds    int64    `json:"last_snapshot_sync_seconds"`
	ReplayState        string   `json:"replay_state"`
	LastUpdate         string
}

func (s rbdMirrorSyncStatus) String() string {
	out := fmt.Sprintf("%s %s remote=%d local=%d", s.State, s.ReplayState, s.Remote, s.Local)
	if s.SyncingPercent != nil {
		out += fmt.Sprintf(" syncing=%d%%", *s.SyncingPercent)
	}
	if s.BytesPerSecond != nil {
		out += fmt.Sprintf(" %.0f B/s", *s.BytesPerSecond)
	}
	if s.SecondsUntilSynced != nil {
		out += fmt.Sprintf(" eta=%ds", *s.SecondsUntilSynced)
	}
	return out + " updated " + s.LastUpdate
}

func rbdMirrorSyncState(ctx context.Context, mirror *rbd.Mirror, image string) (rbdMirrorSyncStatus, error) {
	status, err := mirror.ImageStatus(ctx, image)
	if err != nil {
		return rbdMirrorSyncStatus{}, fmt.Errorf("mirror status of %s: %w", image, err)
	}
	_, detail, ok := strings.Cut(status.Description, ", ")
	var sync rbdMirrorSyncStatus
	if !ok || json.Unmarshal([]byte(detail), &sync) != nil {
		return rbdMirrorSyncStatus{}, fmt.Errorf("mirror status of %s has no snapshot progress: %q", image, status.Description)
	}
	sync.State, sync.LastUpdate = status.State, status.LastUpdate
	return sync, nil
}

// rbdExportSHA256 exports image in the client container and returns the
// SHA-256 and the size of its bytes. It only reads the image.
func rbdExportSHA256(ctx context.Context, client testcontainers.Container, image string) (string, error) {
	code, out, err := rbdMultiClusterExec(ctx, client, "sh", "-c",
		`rm -f /tmp/rbd-bandwidth-export && rbd export --no-progress "$1" /tmp/rbd-bandwidth-export && sha256sum /tmp/rbd-bandwidth-export && rm -f /tmp/rbd-bandwidth-export`, "sh", image)
	if err != nil || code != 0 {
		return "", fmt.Errorf("export %s: exit %d, %v: %s", image, code, err, out)
	}
	sum, _, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if len(sum) != 64 {
		return "", fmt.Errorf("export %s: no SHA-256 in %q", image, out)
	}
	return sum, nil
}

// TestRBDMirrorBandwidthCatchUp limits one path of an RBD snapshot mirror to
// a low bandwidth: the source OSD's packets to the destination's rbd-mirror
// daemon. While the source client rewrites the whole image and creates a
// mirror snapshot through the same OSD at full speed, the daemon's native
// status shows the snapshot syncing slowly but steadily, and the destination
// still holds other bytes. After the limit is restored the destination
// catches up to exactly the source's bytes.
func TestRBDMirrorBandwidthCatchUp(t *testing.T) {
	for _, mode := range networkModes(t, "bridge") {
		t.Run(mode.Name, testRBDMirrorBandwidthCatchUp)
	}
}

func testRBDMirrorBandwidthCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	netem := netemImage(t)
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, ceph.WithOSDCount(1))
	const pool = "tc-rbd-rate"
	const image = pool + "/rate"
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
	mirror, err := rbd.RunMirror(ctx, source.ControlImage(), rbd.MirrorConfig{Source: source, Destination: destination, Pool: pool})
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
	// 256 KiB objects, so that the sync's object progress moves in small
	// steps.
	before := rbdMultiClusterPayload(rbdMirrorBandwidthImageSize, 23)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-rate-seed", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-rate-seed", image, "--object-size", "256K", "--image-feature", "layering,exclusive-lock", "--no-progress")
	execCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-rate-seed")
	if err := mirror.EnableImage(ctx, "rate"); err != nil {
		t.Fatal(err)
	}
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, before)
	synced, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (rbdMirrorSyncStatus, bool, error) {
		status, err := rbdMirrorSyncState(probeCtx, mirror, "rate")
		return status, err == nil && status.Remote != 0 && status.Local == status.Remote, err
	})
	if err != nil {
		t.Fatalf("the first snapshot is not reported in sync: %v, %v", synced.Value, err)
	}
	baseline := synced.Value

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

	// osd.0 sends the daemon's reads of the source image through the limit,
	// and only those: the source client's path through the same OSD stays
	// unlimited.
	limit := ceph.NetworkDelay{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: peer, Rate: rbdMirrorBandwidthRate}
	lease, err := source.TemporaryNetworkDelay(ctx, osd.Container, limit)
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
	applied, err := lease.Status(ctx)
	if err != nil || !applied.Applied || applied.Rate != limit.Rate || lease.Rate != limit.Rate {
		t.Fatalf("status of the rate limit: %+v, lease rate %v, %v", applied, lease.Rate, err)
	}

	// The source client rewrites every byte and creates a mirror snapshot.
	after := rbdMultiClusterPayload(rbdMirrorBandwidthImageSize, 97)
	sum := sha256.Sum256(after)
	afterSHA := hex.EncodeToString(sum[:])
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, uint64(len(before)), 0, after)
	execCommand(t, ctx, sourceClient, "rm", "-f", "/tmp/rbd-fixture-write.diff")
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	snapshotted := time.Now()

	// The unselected path: the source client reads the whole image through
	// the same OSD at full speed.
	start := time.Now()
	controlSHA, err := rbdExportSHA256(ctx, sourceClient, image)
	controlSeconds := time.Since(start).Seconds()
	if err != nil || controlSHA != afterSHA {
		t.Fatalf("the source client's read through the unselected path: %s, %v; want %s", controlSHA, err, afterSHA)
	}
	controlRate := rbdMirrorBandwidthImageSize / controlSeconds
	if controlRate < bandwidthMargin*rateBytes(limit.Rate) {
		t.Fatalf("the source client read %d bytes in %.2fs (%.2f MB/s), not %.0f times the limit %v", rbdMirrorBandwidthImageSize, controlSeconds, controlRate/1e6, bandwidthMargin, limit.Rate)
	}

	// Under the limit the daemon's status shows the new snapshot syncing in
	// at least two updates with a growing, incomplete percentage. Each
	// update's lower bound of the copied bytes stays within the limit since
	// the snapshot.
	type syncSample struct {
		status  rbdMirrorSyncStatus
		elapsed time.Duration
	}
	var samples []syncSample
	progress, err := observe(ctx, 4*time.Minute, 30*time.Second, 5*time.Second, func(probeCtx context.Context) (rbdMirrorSyncStatus, bool, error) {
		status, err := rbdMirrorSyncState(probeCtx, mirror, "rate")
		if err != nil {
			return status, false, err
		}
		elapsed := time.Since(snapshotted)
		switch {
		case status.State != "up+replaying":
			return status, false, fmt.Errorf("the limited daemon is %s", status.State)
		case status.Local != baseline.Local:
			return status, false, fmt.Errorf("the new snapshot completed under the limit after %s: %v", elapsed.Round(time.Second), status)
		case status.Remote == baseline.Remote || status.ReplayState != "syncing":
			// The status update has not reported the new snapshot yet.
			return status, false, nil
		case status.Syncing == nil || *status.Syncing != status.Remote || status.SyncingPercent == nil:
			return status, false, fmt.Errorf("the syncing status lacks the syncing snapshot or its percentage: %v", status)
		}
		percent := *status.SyncingPercent
		if percent < 0 || percent >= 100 {
			return status, false, fmt.Errorf("the syncing status reports %d%%", percent)
		}
		if copied := float64(percent) / 100 * rbdMirrorBandwidthImageSize; copied > bandwidthCeiling*rateBytes(limit.Rate)*elapsed.Seconds() {
			return status, false, fmt.Errorf("%d%% (%.0f bytes) copied %s after the snapshot exceeds the limit %v", percent, copied, elapsed.Round(time.Second), limit.Rate)
		}
		if status.BytesPerSecond != nil && *status.BytesPerSecond > bandwidthCeiling*rateBytes(limit.Rate) {
			return status, false, fmt.Errorf("the daemon reports %.0f B/s through the limit %v", *status.BytesPerSecond, limit.Rate)
		}
		if n := len(samples); n == 0 || status.LastUpdate != samples[n-1].status.LastUpdate {
			if n > 0 && percent < *samples[n-1].status.SyncingPercent {
				return status, false, fmt.Errorf("the syncing percentage went back: %v, then %v", samples[n-1].status, status)
			}
			samples = append(samples, syncSample{status, elapsed})
		}
		first := samples[0].status
		return status, len(samples) >= 2 && percent > *first.SyncingPercent, nil
	})
	if err != nil {
		t.Fatalf("the limited sync: %v, last %v", err, progress.Value)
	}
	last := samples[len(samples)-1]

	// The destination does not hold the source's bytes yet.
	backlogSHA, err := rbdExportSHA256(ctx, destinationClient, image)
	if err != nil {
		t.Fatal(err)
	}
	if backlogSHA == afterSHA {
		t.Fatalf("the destination already holds the new bytes while the status reports %v", last.status)
	}
	limited, err := lease.Status(ctx)
	if err != nil || !limited.Applied || limited.Rate != limit.Rate {
		t.Fatalf("status of the rate limit during the sync: %+v, %v", limited, err)
	}
	sent := limited.Bytes - applied.Bytes
	if floor := uint64(*last.status.SyncingPercent) * rbdMirrorBandwidthImageSize / 100; sent < floor {
		t.Fatalf("netem sent %d bytes to the daemon, less than the %d bytes the status reports copied", sent, floor)
	}
	limitedFor := time.Since(snapshotted)
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored := time.Now()

	// After the restore the destination's bytes match the source's, read by
	// the destination's own client.
	caught, err := observe(ctx, 4*time.Minute, time.Minute, 2*time.Second, func(probeCtx context.Context) (string, bool, error) {
		sum, err := rbdExportSHA256(probeCtx, destinationClient, image)
		if err != nil {
			// The non-primary image can be busy with the sync; a failed
			// export is not evidence either way.
			return err.Error(), false, nil
		}
		return sum, sum == afterSHA, nil
	})
	if err != nil {
		t.Fatalf("the destination did not catch up: %v, last %s", err, caught.Value)
	}
	catchUp := time.Since(restored)
	verifyRBDBytes(t, ctx, destinationClient, image, after)
	reported, err := observe(ctx, 2*time.Minute, 30*time.Second, 2*time.Second, func(probeCtx context.Context) (rbdMirrorSyncStatus, bool, error) {
		status, err := rbdMirrorSyncState(probeCtx, mirror, "rate")
		return status, err == nil && status.Remote == last.status.Remote && status.Local == status.Remote, err
	})
	if err != nil {
		t.Fatalf("the new snapshot is not reported in sync: %v, %v", reported.Value, err)
	}
	final := reported.Value
	reportedAfter := time.Since(restored)
	if final.State != "up+replaying" || final.ReplayState != "idle" || final.LastSnapshotBytes != rbdMirrorBandwidthImageSize {
		t.Fatalf("after the catch-up: %v, %d bytes copied; want idle with %d bytes", final, final.LastSnapshotBytes, rbdMirrorBandwidthImageSize)
	}

	var trace []string
	for _, sample := range samples {
		trace = append(trace, fmt.Sprintf("%d%% at %s", *sample.status.SyncingPercent, sample.elapsed.Round(time.Second)))
	}
	t.Logf("RBD_MIRROR_BANDWIDTH_RECEIPT osd.%d sent rbd-mirror %s (%s) %d bytes in %d packets through %v (drops %d, overlimits %d) on the source network; the source client read %d bytes through the same OSD in %.2fs (%.2f MB/s); for %s the daemon synced snapshot %d (local %d): %s (%v), the destination's SHA-256 %s; %s after the restore the destination's SHA-256 was %s, and %s after it the status reported %v, %d bytes copied in a %ds sync",
		osd.ID, daemon.DaemonName, peer, sent, limited.Packets-applied.Packets, limit.Rate, limited.Drops-applied.Drops, limited.Overlimits-applied.Overlimits,
		rbdMirrorBandwidthImageSize, controlSeconds, controlRate/1e6, limitedFor.Round(time.Second), last.status.Remote, baseline.Local,
		strings.Join(trace, ", "), last.status, backlogSHA, catchUp.Round(time.Second), afterSHA, reportedAfter.Round(time.Second), final, final.LastSnapshotBytes, final.LastSyncSeconds)
}
