//go:build all || (integration && features)

package cephfs_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// awaitCloneWatchEvent reads events until match accepts one, and fails on a
// closed watch or after limit. Gaps are counted, not fatal.
func awaitCloneWatchEvent(t *testing.T, w *subvolume.CloneWatch, last *uint64, gaps *int, limit time.Duration, what string, match func(subvolume.CloneWatchEvent) bool) subvolume.CloneWatchEvent {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("%s: watch ended: %v", what, w.Stop())
			}
			if event.Sequence != *last+1 {
				t.Fatalf("%s: event %d follows %d", what, event.Sequence, *last)
			}
			*last = event.Sequence
			if event.Kind == ceph.WatchGap {
				*gaps++
				t.Logf("%s: gap at event %d after %v: %v", what, event.Sequence, event.Finished.Sub(event.Started), event.Err)
				continue
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s: %+v", what, event.Sequence, event.Kind, *event.Status)
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

// requireCloneWatchEnded checks that a watch that delivered a terminal status
// closes with no further event and a nil cause.
func requireCloneWatchEnded(t *testing.T, w *subvolume.CloneWatch, last uint64, what string) {
	t.Helper()
	timeout := time.After(time.Minute)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				if err := w.Stop(); err != nil {
					t.Fatalf("%s: the watch ended with %v after its terminal status", what, err)
				}
				return
			}
			t.Fatalf("%s: event %d after the terminal status %d: %+v", what, event.Sequence, last, event)
		case <-timeout:
			t.Fatalf("%s: the watch did not end after its terminal status", what)
		}
	}
}

func cloneWatchTerminal(e subvolume.CloneWatchEvent) bool {
	return e.Status.State == "complete" || e.Status.State == "failed" || e.Status.State == "canceled"
}

// testCephFSCloneWatch watches two owned clones of one snapshot. The first is
// held pending by the native clone delay: its baseline advances the test to
// the cancellation, and the watch must then deliver the native canceled state
// and end by itself. The second runs without a delay and the watch must
// deliver its complete state before Clone.Wait adopts it. The events prove
// only what clone status reported for the captured target incarnation;
// Clone.Status, Clone.Wait, the snapshot's pending-clone protection and the
// client's own reads of the frozen and cloned bytes are the independent
// outcomes.
func testCephFSCloneWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	const filesystem = "clone-watch"
	cluster, client := harness.NewServiceCluster(t,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)),
		cephfs.WithFilesystems(cephfs.Config{Name: filesystem}))
	fs := cephfs.Filesystems(cluster)[0]
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	group, err := subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: "restores"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "source", SizeBytes: 8 << 20, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	sourceInfo, err := subvolume.Info(ctx, fs, source.Name, source.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, "", "seed", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	snapshot, err := source.CreateSnapshot(ctx, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, snapshot.Path, "update-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)

	temporaryConfig := func(name, value string) *override.ConfigHandle {
		t.Helper()
		change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: name, Value: value})
		t.Cleanup(func() {
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer restoreCancel()
			if err := change.Restore(restoreCtx); err != nil {
				t.Errorf("restore clone setting %s: %v", name, err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		return change
	}
	noWait := temporaryConfig("mgr/volumes/snapshot_clone_no_wait", "false")
	delay := temporaryConfig("mgr/volumes/snapshot_clone_delay", "60")
	held, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "held", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}

	// The cost of one read: Status runs the same identity check and status
	// query as a watch read, plus the volumes module check.
	const samples = 5
	began := time.Now()
	for range samples {
		if _, err := held.Status(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d Clone.Status reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	const interval = time.Second
	watchStarted := time.Now()
	w, err := held.Watch(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitCloneWatchEvent(t, w, &last, &gaps, time.Second, "held baseline", func(e subvolume.CloneWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	if baseline.Status.State != "pending" || baseline.Status.SourceSnapshot != snapshot.Name || baseline.Status.SourceSubvolume != source.Name {
		t.Fatalf("held baseline = %+v", *baseline.Status)
	}
	t.Logf("held baseline: %s, read %v", baseline.Status.State, baseline.Finished.Sub(baseline.Started))

	// The baseline advances the test: the watch has seen the clone pending, so
	// the cancellation meets a clone that has not copied, and the snapshot
	// still protects it while the watch runs.
	info, err := subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
	if err != nil || !slices.Contains(info.PendingClones, subvolume.SnapshotPendingClone{Name: held.Name, GroupName: held.GroupName}) {
		t.Fatalf("pending clone not protected by its snapshot: %+v %v", info, err)
	}
	if err := held.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	canceledAt := time.Now()
	canceled := awaitCloneWatchEvent(t, w, &last, &gaps, 2*time.Minute, "held terminal", cloneWatchTerminal)
	if canceled.Status.State != "canceled" || canceled.Kind == ceph.WatchBaseline {
		t.Fatalf("held terminal = %s %+v", canceled.Kind, *canceled.Status)
	}
	t.Logf("held terminal: event %d %s %s errno %d, observed %v after Cancel returned", canceled.Sequence, canceled.Kind, canceled.Status.State, canceled.Status.FailureErrno, canceled.Finished.Sub(canceledAt))
	requireCloneWatchEnded(t, w, last, "held")
	heldElapsed := time.Since(watchStarted)

	if status, err := held.Status(ctx); err != nil || status.State != "canceled" {
		t.Fatalf("canceled clone status = %+v %v", status, err)
	}
	if target, err := held.Wait(ctx); err == nil || target != nil {
		t.Fatal("canceled clone was adopted as a completed subvolume")
	}
	for {
		info, err = subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !info.HasPendingClones && info.OrphanClonesCount == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("canceled clone kept its source protection", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, snapshot.Path, source.Path, "verify-clone", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := held.RemovePartial(ctx); err != nil {
		t.Fatal(err)
	}
	if err := delay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	// Without a delay the clone may already be complete at the baseline, which
	// is then the only event.
	copied, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "copied", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	w, err = copied.Watch(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	last = 0
	complete := awaitCloneWatchEvent(t, w, &last, &gaps, 3*time.Minute, "copied terminal", cloneWatchTerminal)
	if complete.Status.State != "complete" || complete.Status.FailureErrno != 0 {
		t.Fatalf("copied terminal = %+v", *complete.Status)
	}
	t.Logf("copied terminal: event %d %s", complete.Sequence, complete.Kind)
	requireCloneWatchEnded(t, w, last, "copied")
	restored, err := copied.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restoredInfo, err := subvolume.Info(ctx, fs, restored.Name, restored.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "verify-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)

	if err := snapshot.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	for _, volume := range []*subvolume.Subvolume{restored, source} {
		if err := volume.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := group.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("native clone watch: held baseline pending -> canceled -> watch ended; copied -> complete -> watch ended; %d gaps, held watch ran %v at a %v interval", gaps, heldElapsed.Round(time.Second), interval)
}
