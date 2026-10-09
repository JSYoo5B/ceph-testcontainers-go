//go:build all || (integration && features)

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func testCephFSCloneCancellationAndPartialCleanup(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const filesystem = "clone-lifecycle"
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, options...)
	fs := cluster.Filesystems()[0]
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	group, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: "restores"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "source", SizeBytes: 8 << 20, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	sourceInfo, err := fs.SubvolumeInfo(ctx, source.Name, source.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, "", "seed", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	snapshot, err := fs.CreateSubvolumeSnapshot(ctx, source, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, snapshot.Path, "update-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)

	// The native delay leaves a deterministic pending window. pause_cloning is
	// unsuitable here: Tentacle's idle worker can consume the pause cancellation
	// event when a new clone wakes its inner condition-variable loop.
	temporaryConfig := func(name, value string) *ceph.ConfigOverride {
		t.Helper()
		change, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "mgr", Name: name, Value: value})
		if change != nil {
			t.Cleanup(func() {
				restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer restoreCancel()
				if err := change.Restore(restoreCtx); err != nil {
					t.Errorf("restore clone setting %s: %v", name, err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		return change
	}
	noWait := temporaryConfig("mgr/volumes/snapshot_clone_no_wait", "false")
	delay := temporaryConfig("mgr/volumes/snapshot_clone_delay", "30")
	clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "partial", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	status, err := fs.SubvolumeCloneStatus(ctx, clone)
	if err != nil || status.State != "pending" {
		t.Fatalf("clone did not remain pending during native delay: %+v error=%v", status, err)
	}
	snapshotInfo, err := fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
	if err != nil || !snapshotInfo.HasPendingClones || !slices.Contains(snapshotInfo.PendingClones, ceph.CephFSSnapshotPendingClone{Name: clone.Name, GroupName: clone.GroupName}) {
		t.Fatalf("source snapshot did not protect pending target: %+v error=%v", snapshotInfo, err)
	}
	if err := fs.RemovePartialSubvolumeClone(ctx, clone); err == nil {
		t.Fatal("fixture removed an active clone")
	}
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "rm", filesystem, clone.Name, "--group_name", clone.GroupName, "--force"); err == nil {
		t.Fatal("native force removal accepted pending clone")
	}
	if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err == nil {
		t.Fatal("source snapshot removed while pending clone protected it")
	}
	copy := *clone
	copy.Name, copy.GroupName, copy.FilesystemName = "foreign", "foreign", "foreign"
	if err := fs.CancelSubvolumeClone(ctx, &copy); err != nil {
		t.Fatal(err)
	}
	status, err = fs.SubvolumeCloneStatus(ctx, clone)
	if err != nil || status.State != "canceled" {
		t.Fatalf("native cancellation did not retain canceled target: %+v error=%v", status, err)
	}
	if target, err := fs.WaitForSubvolumeClone(ctx, clone); err == nil || target != nil {
		t.Fatal("canceled clone was adopted as a completed subvolume")
	}
	if err := fs.CancelSubvolumeClone(ctx, clone); err != nil {
		t.Fatal("retry did not reconcile canceled target", err)
	}
	snapshotInfo, err = fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
	if err != nil || snapshotInfo.HasPendingClones || snapshotInfo.OrphanClonesCount != 0 {
		t.Fatalf("cancellation did not release source protection: %+v error=%v", snapshotInfo, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, snapshot.Path, source.Path, "verify-clone", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := fs.RemovePartialSubvolumeClone(ctx, &copy); err != nil {
		t.Fatal(err)
	}
	names, err := fs.Subvolumes(ctx, group.Name)
	if err != nil || slices.Contains(names, clone.Name) {
		t.Fatalf("partial cleanup did not confirm native logical absence: %v error=%v", names, err)
	}
	if err := delay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	// Reuse the public name after successful cleanup. A copied old descriptor
	// shares its removed state and must never force-remove the new incarnation.
	replacement, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: clone.Name, GroupName: clone.GroupName})
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.RemovePartialSubvolumeClone(ctx, clone); err != nil {
		t.Fatal("repeated owned cleanup did not converge", err)
	}
	restored, err := fs.WaitForSubvolumeClone(ctx, replacement)
	if err != nil {
		t.Fatal("old descriptor damaged replacement clone", err)
	}
	restoredInfo, err := fs.SubvolumeInfo(ctx, restored.Name, restored.GroupName)
	if err != nil || restoredInfo.QuotaBytes != 8<<20 || restoredInfo.DataPool != sourceInfo.DataPool || restoredInfo.PoolNamespace != sourceInfo.PoolNamespace {
		t.Fatalf("replacement clone quota/layout changed: %+v error=%v", restoredInfo, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "verify-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "update-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, restored.Path, "verify-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := fs.RemovePartialSubvolumeClone(ctx, replacement); err == nil {
		t.Fatal("explicit partial cleanup accepted a completed clone")
	}
	// Inject a fault only into a new target's contents. The native copy_file
	// opens regular source "data" with O_CREAT|O_TRUNC|O_WRONLY at destination;
	// a pre-existing destination directory produces EISDIR without touching the
	// source snapshot, target UUID, metadata or root inode/birth time.
	failureNoWait := temporaryConfig("mgr/volumes/snapshot_clone_no_wait", "false")
	failureDelay := temporaryConfig("mgr/volumes/snapshot_clone_delay", "60")
	failed, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "faulted", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSCloneTargetCollision,
		filesystem, source.Path, source.Name, snapshot.Name, failed.GroupName, failed.Name)
	if err := failureDelay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := failureNoWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if target, err := fs.WaitForSubvolumeClone(ctx, failed); err == nil || target != nil {
		t.Fatal("faulted native clone was adopted as complete")
	}
	status, err = fs.SubvolumeCloneStatus(ctx, failed)
	if err != nil || status.State != "failed" || status.FailureErrno != 21 {
		t.Fatalf("target collision did not produce native EISDIR failure: %+v error=%v", status, err)
	}
	if err := fs.CancelSubvolumeClone(ctx, failed); err == nil {
		t.Fatal("failed clone was treated as canceled")
	}
	for {
		snapshotInfo, err = fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
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
	cephFSSnapshotIO(t, ctx, client, filesystem, snapshot.Path, source.Path, "verify-clone", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := fs.RemovePartialSubvolumeClone(ctx, failed); err != nil {
		t.Fatal("owned failed target could not be explicitly removed", err)
	}
	names, err = fs.Subvolumes(ctx, failed.GroupName)
	if err != nil || slices.Contains(names, failed.Name) {
		t.Fatalf("failed target remains after logical removal: %v error=%v", names, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, restored.Path, "verify-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, volume := range []*ceph.CephFSSubvolume{restored, source} {
		if err := fs.RemoveSubvolume(ctx, volume); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.RemoveSubvolumeGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
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
