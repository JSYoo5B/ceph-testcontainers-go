//go:build all || (integration && features)

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func testCephFSSubvolumeSnapshotsAndClones(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const filesystem, extraPool = "snapshots", "snapshots-additional"
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem,
		AdditionalDataPools: []ceph.PoolConfig{{Name: extraPool}},
	})}
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
	snapshotInfo, err := fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
	if err != nil || snapshotInfo.HasPendingClones || snapshotInfo.Path != snapshot.Path || snapshotInfo.DataPool != fs.DataPool {
		t.Fatalf("unexpected initial snapshot: %+v error=%v", snapshotInfo, err)
	}
	if duplicate, err := fs.CreateSubvolumeSnapshot(ctx, source, snapshot.Name); err == nil || duplicate != nil {
		t.Fatal("duplicate snapshot accepted")
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, snapshot.Path, "update-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	if err := fs.RemoveSubvolume(ctx, source); err == nil {
		t.Fatal("source subvolume removed while snapshot exists")
	}

	// Delay the native worker so even a small clone exposes source protection
	// and caller timeout behavior. The first job captures this delay before
	// the setting is restored for subsequent jobs.
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
	clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "restored", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	status, err := fs.SubvolumeCloneStatus(ctx, clone)
	if err != nil || status.State != "pending" || status.SourceSnapshot != snapshot.Name || status.SourceSubvolume != source.Name {
		t.Fatalf("native clone did not stay pending during delay: %+v error=%v", status, err)
	}
	snapshotInfo, err = fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
	if err != nil || !snapshotInfo.HasPendingClones || !slices.Contains(snapshotInfo.PendingClones, ceph.CephFSSnapshotPendingClone{Name: clone.Name, GroupName: clone.GroupName}) {
		latest, statusErr := fs.SubvolumeCloneStatus(ctx, clone)
		t.Logf("latest clone status after missing source protection: %+v error=%v", latest, statusErr)
		t.Fatalf("source snapshot did not track pending clone: %+v error=%v", snapshotInfo, err)
	}
	if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err == nil {
		t.Fatal("owned snapshot removal accepted pending clone")
	}
	if _, err := cluster.Ceph(ctx, "fs", "subvolume", "snapshot", "rm", filesystem, source.Name, snapshot.Name); err == nil {
		t.Fatal("native snapshot removal accepted pending clone")
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	if target, err := fs.WaitForSubvolumeClone(waitCtx, clone); err == nil || target != nil {
		waitCancel()
		t.Fatal("delayed clone was adopted or wait ignored caller deadline")
	}
	waitCancel()
	if duplicate, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: clone.Name, GroupName: clone.GroupName}); err == nil || duplicate != nil {
		t.Fatal("duplicate target accepted")
	}
	if err := delay.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noWait.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := fs.WaitForSubvolumeClone(ctx, clone)
	if err != nil {
		t.Fatal(err)
	}
	restoredInfo, err := fs.SubvolumeInfo(ctx, restored.Name, restored.GroupName)
	if err != nil || restoredInfo.Type != "clone" || restoredInfo.State != "complete" || restoredInfo.QuotaBytes != 8<<20 || restoredInfo.DataPool != sourceInfo.DataPool || restoredInfo.PoolNamespace != sourceInfo.PoolNamespace || restoredInfo.Path == source.Path {
		t.Fatalf("clone did not inherit snapshot quota/layout: %+v error=%v", restoredInfo, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "verify-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, restored.Path, source.Path, "update-clone", restoredInfo.DataPool, restoredInfo.PoolNamespace, 8<<20)

	// A pool override selects a different filesystem data pool and native Ceph
	// intentionally clears the inherited pool namespace for the new copy.
	other, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "other-pool", DataPool: extraPool})
	if err != nil {
		t.Fatal(err)
	}
	otherVolume, err := fs.WaitForSubvolumeClone(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	otherInfo, err := fs.SubvolumeInfo(ctx, otherVolume.Name, otherVolume.GroupName)
	if err != nil || otherInfo.QuotaBytes != 8<<20 || otherInfo.DataPool != extraPool || otherInfo.PoolNamespace != "" {
		t.Fatalf("pool override did not apply native quota/namespace semantics: %+v error=%v", otherInfo, err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, otherVolume.Path, source.Path, "verify-clone", extraPool, "", 8<<20)
	snapshotInfo, err = fs.SubvolumeSnapshotInfo(ctx, source.Name, source.GroupName, snapshot.Name)
	if err != nil || snapshotInfo.HasPendingClones {
		t.Fatalf("completed clones still protect source: %+v error=%v", snapshotInfo, err)
	}
	copySnapshot := *snapshot
	if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveSubvolumeSnapshot(ctx, &copySnapshot); err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, filesystem, source.Path, restored.Path, "verify-source", sourceInfo.DataPool, sourceInfo.PoolNamespace, 8<<20)
	cephFSSnapshotIO(t, ctx, client, filesystem, otherVolume.Path, source.Path, "verify-clone", extraPool, "", 8<<20)
	for _, volume := range []*ceph.CephFSSubvolume{restored, otherVolume, source} {
		if err := fs.RemoveSubvolume(ctx, volume); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.RemoveSubvolumeGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.WaitForSubvolumeClone(ctx, clone); err == nil {
		t.Fatal("removed completed target was adopted again")
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("native CephFS snapshot immutability, asynchronous clone/source protection, wait timeout retry, inherited quota/namespace, pool override, independent cloned writes and safe removal passed")
}

func cephFSSnapshotIO(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, root, other, phase, pool, namespace string, quota int64) {
	t.Helper()
	output := cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSSnapshotIOScript, filesystem, root, other, phase, pool, namespace)
	if !strings.Contains(string(output), `"phase"`) {
		t.Fatalf("missing native snapshot proof: %s", output)
	}
	t.Logf("native snapshot/clone session: %s (quota=%d)", strings.TrimSpace(string(output)), quota)
}

const cephFSSnapshotIOScript = `import cephfs, hashlib, json, os, sys, threading
filesystem, root, other, phase, pool, namespace = sys.argv[1:]
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '25')
fs.conf_set('rados_osd_op_timeout', '15')
fs.mount(filesystem_name=filesystem.encode())
original = bytes(range(256)) * 2048
new_source = bytes(value ^ 0x5a for value in range(256)) * 2048
new_clone = bytes(value ^ 0xa5 for value in range(256)) * 2048
def read_at(directory, expected):
    fd = fs.open(directory + '/data', os.O_RDONLY)
    try:
        assert fs.read(fd, 0, len(expected) + 1) == expected, 'checkpoint/copy bytes differ'
    finally:
        fs.close(fd)
def write_at(directory, data, create=False):
    flags = os.O_WRONLY | os.O_TRUNC
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    fd = fs.open(directory + '/data', flags, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data)
        fs.fsync(fd, 0)
    finally:
        fs.close(fd)
assert fs.getxattr(root, 'ceph.dir.layout.pool').decode() == pool
assert fs.getxattr(root, 'ceph.dir.layout.pool_namespace').decode() == namespace
if phase == 'seed':
    write_at(root, original, True)
    read_at(root, original)
elif phase == 'update-source':
    write_at(root, new_source)
    read_at(root, new_source)
    read_at(other, original)
elif phase == 'verify-clone':
    read_at(root, original)
    read_at(other, new_source)
elif phase == 'update-clone':
    write_at(root, new_clone)
    read_at(root, new_clone)
    read_at(other, new_source)
elif phase == 'verify-source':
    read_at(root, new_source)
    read_at(other, new_clone)
else:
    raise AssertionError('unknown phase')
fs.unmount()
fs.shutdown()
deadline.cancel()
print(json.dumps({'phase': phase, 'root': root, 'pool': pool, 'namespace': namespace, 'snapshot_sha256': hashlib.sha256(original).hexdigest()}))
`
