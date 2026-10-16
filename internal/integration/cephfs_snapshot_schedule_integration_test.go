//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_snapshot_schedule))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// A one-minute snapshot schedule on the filesystem's root takes scheduled
// snapshots, its retention prunes all but the newest two, and Restore removes
// the schedule while the snapshots stay. It needs the MGR snap_schedule
// module and, for minutes, its allow_m_granularity setting; the call sets
// neither.
func TestCephFSSnapshotSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	cluster, _ := newServiceClusterRun(t, cephfsRun, image, imageOpts...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	request := cephfs.SnapshotSchedule{Path: "/", Repeat: "1m", Retention: map[string]int{"n": 2}}
	if lease, err := fs.TemporarySnapshotSchedule(ctx, request); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
		t.Fatalf("a schedule without the module: %v, %v", lease, err)
	}
	module, err := cluster.TemporaryMGRModule(ctx, "snap_schedule", true)
	if module != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := module.Restore(cleanup); err != nil {
				t.Errorf("restore snap_schedule: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	// Enabling a module restarts the active MGR; its commands answer again
	// once it is back.
	for {
		status, err := cluster.ManagerStatus(ctx)
		if err == nil && status.Available {
			if _, err := cluster.Ceph(ctx, "fs", "snap-schedule", "list", "/", "--fs", fs.FilesystemName, "--format", "json"); err == nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("snap_schedule did not answer: %v", err)
		case <-time.After(time.Second):
		}
	}
	// Minutes need the module's allow_m_granularity, which the call never
	// sets.
	if lease, err := fs.TemporarySnapshotSchedule(ctx, request); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
		t.Fatalf("a minute schedule without allow_m_granularity: %v, %v", lease, err)
	}
	granularity, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "mgr", Name: "mgr/snap_schedule/allow_m_granularity", Value: "true"})
	if granularity != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := granularity.Restore(cleanup); err != nil {
				t.Errorf("restore allow_m_granularity: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	lease, err := fs.TemporarySnapshotSchedule(ctx, request)
	if lease != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := lease.Restore(cleanup); err != nil {
				t.Errorf("restore the schedule: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if second, err := fs.TemporarySnapshotSchedule(ctx, cephfs.SnapshotSchedule{Path: "/", Repeat: "1h"}); !errors.Is(err, ceph.ErrInvalidRequest) || second != nil {
		t.Fatalf("a second schedule of the same directory: %v, %v", second, err)
	}
	var status cephfs.SnapshotScheduleStatus
	deadline := time.Now().Add(6 * time.Minute)
	for {
		if status, err = lease.Status(ctx); err != nil {
			t.Fatal(err)
		}
		if status.Created >= 3 && status.Pruned >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the schedule took %d and pruned %d snapshots: %s", status.Created, status.Pruned, status.Raw)
		}
		time.Sleep(10 * time.Second)
	}
	snapshots, err := lease.Snapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) == 0 || len(snapshots) > 2 || status.First.IsZero() || !status.Last.After(status.First) {
		t.Fatalf("snapshots %v with status %s", snapshots, status.Raw)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := lease.Snapshots(ctx)
	if err != nil || len(after) == 0 {
		t.Fatalf("snapshots after Restore: %v, %v", after, err)
	}
	// The module prints {} or, after a removal, nothing.
	if out, err := cluster.Ceph(ctx, "fs", "snap-schedule", "list", "/", "--fs", fs.FilesystemName, "--format", "json"); err != nil || (strings.TrimSpace(string(out)) != "{}" && strings.TrimSpace(string(out)) != "") {
		t.Fatalf("schedules after Restore: %s, %v", out, err)
	}
	t.Logf("CEPHFS_SNAP_SCHEDULE %s every 1m keeping 2: created %d, pruned %d, first %s, last %s; snapshots %v; after Restore no schedule and %d snapshots kept",
		status.Path, status.Created, status.Pruned, status.First.Format(time.RFC3339), status.Last.Format(time.RFC3339), snapshots, len(after))
}
