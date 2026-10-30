//go:build all || (integration && features && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_snapshot_schedule))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// This recipe exercises public cluster CLI composition instead of duplicating
// the client/admin SDK's schedule methods. No manual checkpoint follows writes.
func TestRBDAutomaticSnapshotSchedule(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
			const pool = "tc-rbd-schedule"
			const image = pool + "/scheduled"
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
			if err := source.WaitMGRModuleReady(ctx, "rbd_support"); err != nil {
				t.Fatal(err)
			}
			runtimeImage := harness.StaticFacts(source).Images.RBDAux
			mirror, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", runtimeImage), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool}))
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
			before := harness.RBDMultiClusterPayload(4<<20, 41)
			if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-schedule-seed", 0o600); err != nil {
				t.Fatal(err)
			}
			harness.ExecCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-schedule-seed", image, "--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
			if err := mirror.EnableImage(ctx, "scheduled"); err != nil {
				t.Fatal(err)
			}
			harness.RBDMultiClusterWaitMirror(t, ctx, destinationClient, image, before)
			maxSnapshot := func() uint64 {
				t.Helper()
				var snapshots []struct {
					ID uint64 `json:"id"`
				}
				if err := json.Unmarshal(harness.RBDOutput(t, ctx, sourceClient, "snap", "ls", image, "--all", "--format", "json"), &snapshots); err != nil || len(snapshots) == 0 {
					t.Fatal("native mirror snapshots missing", err)
				}
				var max uint64
				for _, snapshot := range snapshots {
					if snapshot.ID > max {
						max = snapshot.ID
					}
				}
				return max
			}
			baseline := maxSnapshot()
			after := bytes.Clone(before)
			patch := harness.RBDMultiClusterPayload(1<<20, 103)
			copy(after[1<<20:], patch)
			harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, uint64(len(before)), 1<<20, patch)
			// A pool-wide daily schedule remains after the image-specific one
			// is removed; native inheritance must not be erased by cleanup.
			harness.CephCommand(t, ctx, source, "rbd", "mirror", "snapshot", "schedule", "add", pool+"/", "1d")
			harness.CephCommand(t, ctx, source, "rbd", "mirror", "snapshot", "schedule", "add", image, "1m")
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_, _ = source.Ceph(cleanup, "rbd", "mirror", "snapshot", "schedule", "remove", image, "1m")
				_, _ = source.Ceph(cleanup, "rbd", "mirror", "snapshot", "schedule", "remove", pool+"/", "1d")
			})
			started := time.Now()
			harness.RBDMultiClusterWaitMirror(t, ctx, destinationClient, image, after)
			if current := maxSnapshot(); current <= baseline {
				t.Fatal("changed bytes arrived without a new native scheduled checkpoint")
			}
			harness.CephCommand(t, ctx, source, "rbd", "mirror", "snapshot", "schedule", "remove", image, "1m")
			data, err := source.Ceph(ctx, "rbd", "mirror", "snapshot", "schedule", "list", pool+"/", "--format", "json")
			if err != nil || !bytes.Contains(data, []byte("1d")) || bytes.Contains(data, []byte("1m")) {
				t.Fatalf("schedule cleanup changed inheritance: %s %v", data, err)
			}
			harness.CephCommand(t, ctx, source, "rbd", "mirror", "snapshot", "schedule", "remove", pool+"/", "1d")
			harness.VerifyRBDBytes(t, ctx, sourceClient, image, after)
			harness.VerifyRBDBytes(t, ctx, destinationClient, image, after)
			t.Logf("native rbd_support automatically checkpointed and mirrored 4 MiB changed data after %s; image schedule removed, unrelated pool schedule preserved, both images retained", time.Since(started))
		})
	}
}
