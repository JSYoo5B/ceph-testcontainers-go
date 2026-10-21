//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_snapshot_schedule))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

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
	if lease, err := fs.AddSnapshotSchedule(ctx, request); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
		t.Fatalf("a schedule without the module: %v, %v", lease, err)
	}
	module, err := cluster.OverrideMGRModule(ctx, "snap_schedule", true)
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
	if lease, err := fs.AddSnapshotSchedule(ctx, request); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
		t.Fatalf("a minute schedule without allow_m_granularity: %v, %v", lease, err)
	}
	granularity, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "mgr", Name: "mgr/snap_schedule/allow_m_granularity", Value: "true"})
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
	lease, err := fs.AddSnapshotSchedule(ctx, request)
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
	if second, err := fs.AddSnapshotSchedule(ctx, cephfs.SnapshotSchedule{Path: "/", Repeat: "1h"}); !errors.Is(err, ceph.ErrInvalidRequest) || second != nil {
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
	testSnapshotScheduleOwnership(t, ctx, cluster, fs)
}

// testSnapshotScheduleOwnership changes a lease's schedule and retention as
// another client would, with hourly schedules that start in the future.
func testSnapshotScheduleOwnership(t *testing.T, ctx context.Context, cluster *ceph.Container, fs *cephfs.Filesystem) {
	native := func(args ...string) string {
		t.Helper()
		out, err := cluster.Ceph(ctx, append(args, "--fs", fs.FilesystemName)...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return string(out)
	}
	statusJSON := func() string {
		return strings.TrimSpace(native("fs", "snap-schedule", "status", "/", "--format", "json"))
	}
	lease := func(schedule cephfs.SnapshotSchedule) *cephfs.SnapshotScheduleLease {
		t.Helper()
		lease, err := fs.AddSnapshotSchedule(ctx, schedule)
		if err != nil {
			t.Fatal(err)
		}
		return lease
	}

	// A schedule with the lease's path and repeat that replaced the lease's.
	replaced := lease(cephfs.SnapshotSchedule{Path: "/", Repeat: "1h", Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})
	native("fs", "snap-schedule", "remove", "/", "1h", "2030-01-01T00:00:00")
	native("fs", "snap-schedule", "add", "/", "1h", "2030-02-01T00:00:00")
	if err := replaced.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore of a replaced schedule: %v", err)
	}
	if status := statusJSON(); !strings.Contains(status, `"start": "2030-02-01T00:00:00"`) {
		t.Fatalf("the refused Restore changed the replacement: %s", status)
	}
	native("fs", "snap-schedule", "remove", "/", "1h", "2030-02-01T00:00:00")
	if err := replaced.Restore(ctx); err != nil {
		t.Fatalf("Restore after the replacement was removed: %v", err)
	}

	// A retention rule that is already gone.
	partial := lease(cephfs.SnapshotSchedule{Path: "/", Repeat: "2h", Start: time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"d": 1, "n": 2}})
	native("fs", "snap-schedule", "retention", "remove", "/", "d", "1")
	if err := partial.Restore(ctx); err != nil {
		t.Fatalf("Restore after a rule was removed: %v", err)
	}
	if status := statusJSON(); status != "[]" {
		t.Fatalf("schedules after Restore: %s", status)
	}
	// The module keeps retention per directory, also without schedules: a new
	// schedule shows what is left.
	native("fs", "snap-schedule", "add", "/", "3h", "2030-04-01T00:00:00")
	if status := statusJSON(); !strings.Contains(status, `"retention": {}`) {
		t.Fatalf("retention left after Restore: %s", status)
	}

	// Another repeat's schedule shares the lease's rule: refused, both kept,
	// then restored once that schedule is gone.
	native("fs", "snap-schedule", "remove", "/", "3h")
	shared := lease(cephfs.SnapshotSchedule{Path: "/", Repeat: "5h", Start: time.Date(2030, 5, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"n": 2}})
	native("fs", "snap-schedule", "add", "/", "6h", "2030-06-01T00:00:00")
	if err := shared.Restore(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore beside another repeat's schedule: %v", err)
	}
	if status := statusJSON(); !strings.Contains(status, `"schedule": "5h"`) || !strings.Contains(status, `"schedule": "6h"`) || !strings.Contains(status, `"retention": {"n": 2}`) {
		t.Fatalf("the refused Restore changed the directory: %s", status)
	}
	native("fs", "snap-schedule", "remove", "/", "6h")
	if err := shared.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	// A schedule removed by another client leaves the lease's rule in the
	// directory; Restore still removes it.
	orphan := lease(cephfs.SnapshotSchedule{Path: "/", Repeat: "4h", Start: time.Date(2030, 7, 1, 0, 0, 0, 0, time.UTC), Retention: map[string]int{"n": 2}})
	native("fs", "snap-schedule", "remove", "/", "4h")
	if err := orphan.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	native("fs", "snap-schedule", "add", "/", "3h", "2030-04-01T00:00:00")
	if status := statusJSON(); !strings.Contains(status, `"retention": {}`) {
		t.Fatalf("retention left after the schedule was removed natively: %s", status)
	}

	// An existing schedule, and retention that a new schedule would inherit.
	if refused, err := fs.AddSnapshotSchedule(ctx, cephfs.SnapshotSchedule{Path: "/", Repeat: "4h"}); !errors.Is(err, ceph.ErrInvalidRequest) || refused != nil {
		t.Fatalf("a lease of a directory with a schedule: %v, %v", refused, err)
	}
	native("fs", "snap-schedule", "retention", "add", "/", "n", "3")
	native("fs", "snap-schedule", "remove", "/", "3h")
	inherited, err := fs.AddSnapshotSchedule(ctx, cephfs.SnapshotSchedule{Path: "/", Repeat: "4h"})
	if !errors.Is(err, ceph.ErrInvalidRequest) || inherited == nil {
		t.Fatalf("a lease of a directory with retention left: %v, %v", inherited, err)
	}
	if err := inherited.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	native("fs", "snap-schedule", "add", "/", "3h", "2030-04-01T00:00:00")
	if status := statusJSON(); !strings.Contains(status, `"retention": {"n": 3}`) {
		t.Fatalf("Restore changed the retention it did not add: %s", status)
	}
	native("fs", "snap-schedule", "retention", "remove", "/", "n", "3")
	native("fs", "snap-schedule", "remove", "/", "3h")
	t.Log("CEPHFS_SNAP_SCHEDULE_OWNERSHIP a replaced schedule was refused and kept; a rule removed by another client was skipped; a rule shared with another repeat's schedule was refused, then removed; a rule outliving its schedule was removed; retention left in the directory was refused on add and kept by Restore")
}
