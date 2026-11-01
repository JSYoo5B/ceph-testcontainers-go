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
	"github.com/testcontainers/testcontainers-go"
)

func testCephFSCloneCancellationAndPartialCleanup(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const filesystem = "clone-lifecycle"
	partialID := harness.CloneTimelineIdentity{Filesystem: filesystem, Group: "restores", Name: "partial", Incarnation: 1}
	replacementID := harness.CloneTimelineIdentity{Filesystem: filesystem, Group: "restores", Name: "partial", Incarnation: 2}
	faultedID := harness.CloneTimelineIdentity{Filesystem: filesystem, Group: "restores", Name: "faulted", Incarnation: 1}
	timeline := harness.NewCloneTimeline("cephfs-clone-cancellation-and-partial-cleanup",
		harness.CloneTimelineClone{Identity: partialID, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelinePending, harness.CloneTimelineCancelSetup, harness.CloneTimelineTerminal, harness.CloneTimelineSourceRelease, harness.CloneTimelineDataCheck, harness.CloneTimelineCleanup, harness.CloneTimelineDelayRestore}},
		harness.CloneTimelineClone{Identity: replacementID, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelineTerminal, harness.CloneTimelineDataCheck, harness.CloneTimelineFinalDataCheck, harness.CloneTimelineCleanup}},
		harness.CloneTimelineClone{Identity: faultedID, Expect: []string{harness.CloneTimelineSubmit, harness.CloneTimelineCollisionSetup, harness.CloneTimelineDelayRestore, harness.CloneTimelineTerminal, harness.CloneTimelineSourceRelease, harness.CloneTimelineDataCheck, harness.CloneTimelineCleanup}})
	defer timeline.Emit(t)
	options := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)), cephfs.WithFilesystems(cephfs.Config{Name: filesystem})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := harness.NewServiceCluster(t, options...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := cluster.WaitForClean(ctx); err != nil {
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

	// The native delay leaves a deterministic pending window. pause_cloning is
	// unsuitable here: Tentacle's idle worker can consume the pause cancellation
	// event when a new clone wakes its inner condition-variable loop.
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
	delay := temporaryConfig("mgr/volumes/snapshot_clone_delay", "30")
	span := timeline.Begin(partialID, harness.CloneTimelineSubmit, "snapshot_clone_delay=30 snapshot_clone_no_wait=false")
	clone, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "partial", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelinePending, "")
	status, err := clone.Status(ctx)
	if err != nil || status.State != "pending" {
		t.Fatalf("clone did not remain pending during native delay: %+v error=%v", status, err)
	}
	snapshotInfo, err := subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
	if err != nil || !snapshotInfo.HasPendingClones || !slices.Contains(snapshotInfo.PendingClones, subvolume.SnapshotPendingClone{Name: clone.Name, GroupName: clone.GroupName}) {
		t.Fatalf("source snapshot did not protect pending target: %+v error=%v", snapshotInfo, err)
	}
	if err := clone.RemovePartial(ctx); err == nil {
		t.Fatal("fixture removed an active clone")
	}
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "rm", filesystem, clone.Name, "--group_name", clone.GroupName, "--force"); err == nil {
		t.Fatal("native force removal accepted pending clone")
	}
	if err := snapshot.Remove(ctx); err == nil {
		t.Fatal("source snapshot removed while pending clone protected it")
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineCancelSetup, "cancel through a copied descriptor with foreign names")
	copy := *clone
	copy.Name, copy.GroupName, copy.FilesystemName = "foreign", "foreign", "foreign"
	if err := (&copy).Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineTerminal, "")
	status, err = clone.Status(ctx)
	if err != nil || status.State != "canceled" {
		t.Fatalf("native cancellation did not retain canceled target: %+v error=%v", status, err)
	}
	if target, err := clone.Wait(ctx); err == nil || target != nil {
		t.Fatal("canceled clone was adopted as a completed subvolume")
	}
	if err := clone.Cancel(ctx); err != nil {
		t.Fatal("retry did not reconcile canceled target", err)
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineSourceRelease, "")
	snapshotInfo, err = subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
	if err != nil || snapshotInfo.HasPendingClones || snapshotInfo.OrphanClonesCount != 0 {
		t.Fatalf("cancellation did not release source protection: %+v error=%v", snapshotInfo, err)
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineDataCheck, "")
	cephFSSnapshotIO(t, ctx, client, filesystem, snapshot.Path, source.Path, "verify-clone", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineCleanup, "")
	if err := (&copy).RemovePartial(ctx); err != nil {
		t.Fatal(err)
	}
	names, err := subvolume.Names(ctx, fs, group.Name)
	if err != nil || slices.Contains(names, clone.Name) {
		t.Fatalf("partial cleanup did not confirm native logical absence: %v error=%v", names, err)
	}
	span.Ok()
	span = timeline.Begin(partialID, harness.CloneTimelineDelayRestore, "")
	if err := delay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	span.Ok()

	// Reuse the public name after successful cleanup. A copied old descriptor
	// shares its removed state and must never force-remove the new incarnation.
	span = timeline.Begin(replacementID, harness.CloneTimelineSubmit, "settings restored")
	replacement, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: clone.Name, GroupName: clone.GroupName})
	if err != nil {
		t.Fatal(err)
	}
	span.Ok()
	span = timeline.Begin(replacementID, harness.CloneTimelineTerminal, "includes repeated owned cleanup with the old descriptor")
	if err := clone.RemovePartial(ctx); err != nil {
		t.Fatal("repeated owned cleanup did not converge", err)
	}
	restored, err := replacement.Wait(ctx)
	if err != nil {
		t.Fatal("old descriptor damaged replacement clone", err)
	}
	span.Ok()
	span = timeline.Begin(replacementID, harness.CloneTimelineDataCheck, "")
	restoredInfo, err := subvolume.Info(ctx, fs, restored.Name, restored.GroupName)
	if err != nil || restoredInfo.QuotaBytes != 8<<20 || restoredInfo.DataPool != sourceInfo.DataPool || restoredInfo.PoolNamespace != sourceInfo.PoolNamespace {
		t.Fatalf("replacement clone quota/layout changed: %+v error=%v", restoredInfo, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "verify-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "update-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, restored.Path, "verify-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := replacement.RemovePartial(ctx); err == nil {
		t.Fatal("explicit partial cleanup accepted a completed clone")
	}
	span.Ok()
	// Inject a fault only into a new target's contents. The native copy_file
	// opens regular source "data" with O_CREAT|O_TRUNC|O_WRONLY at destination;
	// a pre-existing destination directory produces EISDIR without touching the
	// source snapshot, target UUID, metadata or root inode/birth time.
	failureNoWait := temporaryConfig("mgr/volumes/snapshot_clone_no_wait", "false")
	failureDelay := temporaryConfig("mgr/volumes/snapshot_clone_delay", "60")
	span = timeline.Begin(faultedID, harness.CloneTimelineSubmit, "snapshot_clone_delay=60 snapshot_clone_no_wait=false")
	failed, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "faulted", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	span.Ok()
	span = timeline.Begin(faultedID, harness.CloneTimelineCollisionSetup, "asserts pending metadata, then inserts the target leaf collision")
	cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSCloneTargetCollision,
		filesystem, source.Path, source.Name, snapshot.Name, failed.GroupName, failed.Name)
	span.Ok()
	// The pending worker may already hold its captured delay, so this restore is
	// not the start of a new, shorter worker wait.
	span = timeline.Begin(faultedID, harness.CloneTimelineDelayRestore, "restores settings; the pending worker keeps its captured delay")
	if err := failureDelay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := failureNoWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	span.Ok()
	span = timeline.Begin(faultedID, harness.CloneTimelineTerminal, "")
	if target, err := failed.Wait(ctx); err == nil || target != nil {
		t.Fatal("faulted native clone was adopted as complete")
	}
	status, err = failed.Status(ctx)
	if err != nil || status.State != "failed" || status.FailureErrno != 21 {
		t.Fatalf("target collision did not produce native EISDIR failure: %+v error=%v", status, err)
	}
	if err := failed.Cancel(ctx); err == nil {
		t.Fatal("failed clone was treated as canceled")
	}
	span.Ok()
	span = timeline.Begin(faultedID, harness.CloneTimelineSourceRelease, "polls every 500ms")
	for {
		snapshotInfo, err = subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !snapshotInfo.HasPendingClones && snapshotInfo.OrphanClonesCount == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("failed worker did not release source protection", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	span.Ok()
	span = timeline.Begin(faultedID, harness.CloneTimelineDataCheck, "")
	cephFSSnapshotIO(t, ctx, client, filesystem, snapshot.Path, source.Path, "verify-clone", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	span.Ok()
	span = timeline.Begin(faultedID, harness.CloneTimelineCleanup, "")
	if err := failed.RemovePartial(ctx); err != nil {
		t.Fatal("owned failed target could not be explicitly removed", err)
	}
	names, err = subvolume.Names(ctx, fs, failed.GroupName)
	if err != nil || slices.Contains(names, failed.Name) {
		t.Fatalf("failed target remains after logical removal: %v error=%v", names, err)
	}
	span.Ok()
	span = timeline.Begin(replacementID, harness.CloneTimelineFinalDataCheck, "")
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, restored.Path, "verify-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	span.Ok()
	span = timeline.Begin(replacementID, harness.CloneTimelineCleanup, "")
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
	span.Ok()
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("native canceled/failed clone source protection release, explicit partial cleanup, same-name replacement preservation, frozen snapshot bytes and independent clone I/O passed")
}

const cephFSCloneTargetCollision = `import cephfs, configparser, os, stat, sys, threading, uuid
filesystem, source_path, source_name, snapshot_name, group, name = sys.argv[1:]
deadline = threading.Timer(35, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
    base = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(source_path))), group or '_nogroup', name)
    fd = fs.open(base + '/.meta', os.O_RDONLY)
    try:
        metadata = configparser.ConfigParser()
        metadata.read_string(fs.read(fd, 0, 1048576).decode())
    finally:
        fs.close(fd)
    assert metadata['GLOBAL']['version'] == '2' and metadata['GLOBAL']['type'] == 'clone'
    assert metadata['GLOBAL']['state'] == 'pending', 'native worker escaped fault-injection window'
    assert metadata['source']['volume'] == filesystem
    assert metadata['source']['subvolume'] == source_name and metadata['source']['snapshot'] == snapshot_name
    target = metadata['GLOBAL']['path']
    assert os.path.dirname(target) == base
    uuid.UUID(os.path.basename(target))
    mask = cephfs.CEPH_STATX_INO | cephfs.CEPH_STATX_BTIME | cephfs.CEPH_STATX_MODE
    before = fs.statx(target, mask, cephfs.AT_SYMLINK_NOFOLLOW)
    assert stat.S_ISDIR(before['mode'])
    fs.mkdir(target + '/data', 0o700)
    after = fs.statx(target, mask, cephfs.AT_SYMLINK_NOFOLLOW)
    assert before == after, 'target root identity changed'
    print('owned target leaf collision inserted; source and native root identity retained')
finally:
    fs.shutdown()
    deadline.cancel()
`
