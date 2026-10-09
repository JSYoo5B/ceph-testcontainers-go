//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSSubvolumes(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const filesystem, extraPool = "subvolumes", "subvolumes-additional"
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{
		Name: filesystem, AdditionalDataPools: []ceph.PoolConfig{{Name: extraPool}},
	})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, options...)
	fs := cluster.Filesystems()[0]
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	group, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: "tenant", SizeBytes: 32 << 20, DataPool: extraPool})
	if err != nil {
		t.Fatal(err)
	}
	subvolume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "data", GroupName: group.Name, SizeBytes: 1 << 20, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || info.Path != subvolume.Path || info.DataPool != extraPool || info.PoolNamespace == "" || info.QuotaBytes != 1<<20 || info.State != "complete" {
		t.Fatalf("created subvolume has unexpected native metadata: %+v error=%v", info, err)
	}
	groupInfo, err := fs.SubvolumeGroupInfo(ctx, group.Name)
	if err != nil || groupInfo.DataPool != extraPool || groupInfo.QuotaBytes != 32<<20 || groupInfo.Path != group.Path {
		t.Fatalf("unexpected native group metadata: %+v error=%v", groupInfo, err)
	}
	if duplicate, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: group.Name, SizeBytes: 1, DataPool: fs.DataPool}); err == nil || duplicate != nil {
		t.Fatal("duplicate group accepted and could have changed quota/layout")
	}
	if duplicate, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: subvolume.Name, GroupName: group.Name, SizeBytes: 1, DataPool: fs.DataPool}); err == nil || duplicate != nil {
		t.Fatal("duplicate subvolume accepted and could have changed quota/layout")
	}
	unchanged, err := fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || unchanged.Path != info.Path || unchanged.QuotaBytes != info.QuotaBytes || unchanged.PoolNamespace != info.PoolNamespace || unchanged.DataPool != info.DataPool {
		t.Fatalf("duplicate request altered subvolume: %+v error=%v", unchanged, err)
	}

	// The same name in the default group has a different path, layout and data.
	defaultSubvolume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "data"})
	if err != nil || defaultSubvolume.Path == subvolume.Path {
		t.Fatalf("default group did not isolate same-named subvolume: %+v error=%v", defaultSubvolume, err)
	}
	defaultInfo, err := fs.SubvolumeInfo(ctx, defaultSubvolume.Name, "")
	if err != nil || defaultInfo.DataPool != fs.DataPool || defaultInfo.QuotaBytes != 0 {
		t.Fatalf("default subvolume did not inherit default layout/unlimited quota: %+v error=%v", defaultInfo, err)
	}
	groups, err := fs.SubvolumeGroups(ctx)
	if err != nil || !slices.Equal(groups, []string{"tenant"}) {
		t.Fatalf("unexpected groups: %v error=%v", groups, err)
	}
	for _, groupName := range []string{"", "tenant"} {
		names, err := fs.Subvolumes(ctx, groupName)
		if err != nil || !slices.Equal(names, []string{"data"}) {
			t.Fatalf("unexpected group %q subvolumes: %v error=%v", groupName, names, err)
		}
	}

	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "seed", "tenant", 1<<20)
	cephFSSubvolumeIO(t, ctx, client, filesystem, defaultInfo, "seed", "default", 0)
	objects := cephFSSubvolumeExec(t, ctx, client, "rados", "-p", extraPool, "-N", info.PoolNamespace, "ls")
	if len(strings.TrimSpace(string(objects))) == 0 {
		t.Fatal("namespace-isolated file has no objects in the selected additional data pool")
	}
	// Native libcephfs must reject an oversize write, rather than merely expose
	// the quota xattr. The same payload succeeds in the unlimited neighbor, and
	// existing bytes remain readable in fresh sessions throughout the denial.
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "quota-denied", "tenant", 1<<20)
	cephFSSubvolumeIO(t, ctx, client, filesystem, defaultInfo, "grow", "default", 0)
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "verify", "tenant", 1<<20)
	if err := fs.ResizeSubvolume(ctx, subvolume, 8<<20); err != nil {
		t.Fatal(err)
	}
	// Once the subvolume limit is larger than the write, independently prove
	// that its enclosing group's smaller quota also constrains the native client.
	if err := fs.ResizeSubvolumeGroup(ctx, group, 1<<20); err != nil {
		t.Fatal(err)
	}
	groupInfo, err = fs.SubvolumeGroupInfo(ctx, group.Name)
	if err != nil || groupInfo.QuotaBytes != 1<<20 || groupInfo.Path != group.Path {
		t.Fatalf("group quota did not shrink above existing usage: %+v error=%v", groupInfo, err)
	}
	info, err = fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || info.QuotaBytes != 8<<20 {
		t.Fatalf("subvolume quota did not grow: %+v error=%v", info, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "group-quota-denied", "tenant", 8<<20,
		groupInfo.Path, strconv.FormatInt(groupInfo.QuotaBytes, 10))
	cephFSSubvolumeIO(t, ctx, client, filesystem, defaultInfo, "verify-grown", "default", 0)
	if err := fs.ResizeSubvolumeGroup(ctx, group, 64<<20); err != nil {
		t.Fatal(err)
	}
	info, err = fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || info.QuotaBytes != 8<<20 {
		t.Fatalf("subvolume quota did not grow: %+v error=%v", info, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "grow", "tenant", 8<<20)
	cephFSSubvolumeIO(t, ctx, client, filesystem, defaultInfo, "verify-grown", "default", 0)
	// Quota accounting is asynchronous. Wait for native usage before checking
	// --no_shrink, then prove the rejected limit preserved every existing byte.
	usageCtx, usageCancel := context.WithTimeout(ctx, 30*time.Second)
	defer usageCancel()
	for {
		info, err = fs.SubvolumeInfo(usageCtx, subvolume.Name, group.Name)
		if err == nil && info.BytesUsed >= (256<<10)+(2<<20) {
			break
		}
		select {
		case <-usageCtx.Done():
			t.Fatalf("native subvolume usage did not settle: %+v error=%v", info, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err := fs.ResizeSubvolume(ctx, subvolume, 64); err == nil {
		t.Fatal("subvolume quota shrank below used bytes")
	}
	if err := fs.ResizeSubvolumeGroup(ctx, group, 64); err == nil {
		t.Fatal("group quota shrank below used bytes")
	}
	if err := fs.ResizeSubvolume(ctx, subvolume, 0); err != nil {
		t.Fatal(err)
	}
	if err := fs.ResizeSubvolumeGroup(ctx, group, 0); err != nil {
		t.Fatal(err)
	}
	info, err = fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || info.QuotaBytes != 0 {
		t.Fatalf("subvolume quota did not become unlimited: %+v error=%v", info, err)
	}
	groupInfo, err = fs.SubvolumeGroupInfo(ctx, group.Name)
	if err != nil || groupInfo.QuotaBytes != 0 {
		t.Fatalf("group quota did not become unlimited: %+v error=%v", groupInfo, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "verify-grown", "tenant", 0)
	if err := fs.RemoveSubvolumeGroup(ctx, group); err == nil {
		t.Fatal("nonempty group was removed")
	}
	if err := fs.RemoveSubvolume(ctx, &ceph.CephFSSubvolume{Name: subvolume.Name, GroupName: group.Name, FilesystemName: filesystem, Path: subvolume.Path}); err == nil {
		t.Fatal("unowned public descriptor removed a native subvolume")
	}

	copyBeforeRemoval := *subvolume
	if err := fs.RemoveSubvolume(ctx, subvolume); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveSubvolume(ctx, &copyBeforeRemoval); err != nil {
		t.Fatal(err)
	}
	if err := fs.ResizeSubvolume(ctx, &copyBeforeRemoval, 1<<20); err == nil {
		t.Fatal("copied removed handle accepted resize")
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "absent", "tenant", 0)
	if err := fs.RemoveSubvolume(ctx, defaultSubvolume); err != nil {
		t.Fatal(err)
	}
	copyGroupBeforeRemoval := *group
	if err := fs.RemoveSubvolumeGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	// Recreating a name must not let a stale copied handle affect the new group.
	replacementGroup, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveSubvolumeGroup(ctx, &copyGroupBeforeRemoval); err != nil {
		t.Fatal(err)
	}
	groups, err = fs.SubvolumeGroups(ctx)
	if err != nil || !slices.Equal(groups, []string{"tenant"}) {
		t.Fatal("stale handle affected replacement group")
	}
	if err := fs.RemoveSubvolumeGroup(ctx, replacementGroup); err != nil {
		t.Fatal(err)
	}
	groups, err = fs.SubvolumeGroups(ctx)
	if err != nil || len(groups) != 0 {
		t.Fatalf("named group remained after removal: %v error=%v", groups, err)
	}
	for _, groupName := range []string{""} {
		names, err := fs.Subvolumes(ctx, groupName)
		if err != nil || len(names) != 0 {
			t.Fatalf("subvolume remained after removal: %v error=%v", names, err)
		}
	}
	if status, err := fs.MDSStatus(ctx); err != nil || len(status.Active) != 1 {
		t.Fatalf("subvolume removal damaged filesystem: %+v error=%v", status, err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("native CephFS volumes: existing filesystem/MDS/pools, group/default-group isolation, selected additional pool and RADOS namespace, duplicate refusal, actual subvolume/group EDQUOT write denial with preexisting bytes and independent neighbor retained, quota extension recovered the same payload in a fresh session, unlimited/no-shrink, safe removal and copied-handle replacement protection passed")
}

func cephFSSubvolumeIO(t *testing.T, parent context.Context, client testcontainers.Container, filesystem string, info *ceph.CephFSSubvolumeInfo, phase, token string, quota int64, ancestorQuota ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	args := []string{"python3", "-c", cephFSSubvolumeIOScript, filesystem, info.Path, info.DataPool, info.PoolNamespace, phase, token, strconv.FormatInt(quota, 10)}
	args = append(args, ancestorQuota...)
	code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("fresh native CephFS subvolume %s: exit=%d error=%v output=%s", phase, code, err, data)
	}
	var proof struct {
		Phase       string `json:"phase"`
		Path        string `json:"path"`
		DataPool    string `json:"data_pool"`
		Namespace   string `json:"namespace"`
		Quota       int64  `json:"quota"`
		DenialErrno int    `json:"denial_errno"`
		DenialType  string `json:"denial_type"`
		DeniedBytes int64  `json:"denied_bytes"`
		LimitPath   string `json:"limit_path"`
		LimitQuota  int64  `json:"limit_quota"`
	}
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatalf("invalid native proof: %s error=%v", data, err)
	}
	if proof.Phase != phase || proof.Path != info.Path || proof.DataPool != info.DataPool || proof.Namespace != info.PoolNamespace || proof.Quota != quota {
		t.Fatalf("native subvolume proof differs from requested fixture: %s", data)
	}
	if phase == "quota-denied" || phase == "group-quota-denied" {
		if proof.DenialErrno != 122 || proof.DenialType != "DiskQuotaExceeded" || proof.DeniedBytes != 2<<20 {
			t.Fatalf("expected exact native EDQUOT write/fsync refusal: %s", data)
		}
		if phase == "quota-denied" && (proof.LimitPath != info.Path || proof.LimitQuota != quota) {
			t.Fatalf("subvolume denial did not confirm the limiting quota: %s", data)
		}
		if phase == "group-quota-denied" && (len(ancestorQuota) != 2 || proof.LimitPath != ancestorQuota[0] || strconv.FormatInt(proof.LimitQuota, 10) != ancestorQuota[1] || proof.LimitQuota >= quota) {
			t.Fatalf("group denial did not confirm the independent ancestor limit: %s", data)
		}
	}
	t.Logf("native subvolume session: %s", strings.TrimSpace(string(data)))
}

func cephFSSubvolumeExec(t *testing.T, parent context.Context, client testcontainers.Container, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("native subvolume command %v: exit=%d error=%v output=%s", args, code, err, data)
	}
	return data
}

// Ceph v20.2.4 Client::_write checks is_quota_bytes_exceeded before submitting
// the write and returns EDQUOT; the Python binding maps it to DiskQuotaExceeded.
// ENOSPC would also describe a full OSD pool and is not quota evidence here.
// https://github.com/ceph/ceph/blob/v20.2.4/src/client/Client.cc#L11041-L11045
// https://github.com/ceph/ceph/blob/v20.2.4/src/pybind/cephfs/cephfs.pyx#L208-L223
const cephFSSubvolumeIOScript = `import cephfs, errno, hashlib, json, os, sys, threading
filesystem, root, pool, namespace, phase, token, quota = sys.argv[1:8]
ancestor = sys.argv[8:]
assert len(ancestor) in (0, 2), 'invalid ancestor quota arguments'
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '25')
fs.conf_set('rados_osd_op_timeout', '15')
fs.conf_set('client_quota', 'true')
fs.mount(filesystem_name=filesystem.encode())
seed = (token.encode() + bytes(range(256))) * 1024
seed = (seed * 2)[:256 << 10]
growth = bytes((value ^ 0x5a) for value in range(256)) * 8192
denial = {}
def read(name, expected):
    fd = fs.open(root + '/' + name, os.O_RDONLY)
    try:
        assert fs.read(fd, 0, len(expected) + 1) == expected, 'subvolume bytes changed'
    finally:
        fs.close(fd)
def write(name, data):
    fd = fs.open(root + '/' + name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data), 'short write'
        fs.fsync(fd, False)
    finally:
        fs.close(fd)
try:
    if phase == 'absent':
        try:
            fs.stat(root)
        except cephfs.ObjectNotFound:
            pass
        else:
            raise AssertionError('removed subvolume path remains')
    else:
        if phase == 'seed':
            write('baseline', seed)
        read('baseline', seed)
        if phase in ('quota-denied', 'group-quota-denied'):
            assert int(fs.getxattr(root, 'ceph.quota.max_bytes')) == int(quota), 'subvolume quota is not active'
            limit_root, limit_quota = (root, int(quota))
            if phase == 'group-quota-denied':
                assert len(ancestor) == 2, 'missing group quota evidence'
                limit_root, limit_quota = ancestor[0], int(ancestor[1])
                assert root.startswith(limit_root + '/'), 'group quota is not an ancestor'
                assert int(quota) > len(growth), 'subvolume quota could explain group denial'
            assert 0 < limit_quota < len(growth), 'write does not exceed the intended quota'
            assert int(fs.getxattr(limit_root, 'ceph.quota.max_bytes')) == limit_quota, 'limiting quota is not active'
            # Open must succeed; permission, transport and full-pool failures
            # must never be accepted as the requested quota refusal.
            target = root + '/after-resize'
            fd = fs.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                try:
                    assert fs.write(fd, growth, 0) == len(growth), 'short quota probe write'
                    fs.fsync(fd, False)
                except cephfs.DiskQuotaExceeded as exc:
                    assert exc.errno == errno.EDQUOT, 'quota refusal has unexpected errno'
                    denial = {'denial_errno': exc.errno, 'denial_type': type(exc).__name__, 'denied_bytes': len(growth), 'limit_path': limit_root, 'limit_quota': limit_quota}
                else:
                    raise AssertionError('quota admitted an oversized durable write')
                assert fs.stat(target).st_size == 0, 'rejected quota write changed file size'
            finally:
                fs.close(fd)
            fs.unlink(target)
            read('baseline', seed)
        if phase == 'grow':
            write('after-resize', growth)
        if phase in ('grow', 'verify-grown'):
            read('after-resize', growth)
        try:
            actual_quota = int(fs.getxattr(root, 'ceph.quota.max_bytes'))
        except cephfs.NoData:
            actual_quota = 0
        assert actual_quota == int(quota), 'native quota xattr mismatch'
        assert fs.getxattr(root + '/baseline', 'ceph.file.layout.pool_name').decode() == pool, 'file data pool mismatch'
        try:
            actual_namespace = fs.getxattr(root + '/baseline', 'ceph.file.layout.pool_namespace').decode()
        except cephfs.NoData:
            actual_namespace = ''
        assert actual_namespace == namespace, 'file namespace mismatch'
    print(json.dumps({'phase': phase, 'path': root, 'data_pool': pool, 'namespace': namespace, 'quota': int(quota), 'baseline_sha256': hashlib.sha256(seed).hexdigest(), 'growth_bytes': len(growth), **denial}))
finally:
    fs.shutdown()
    deadline.cancel()
`
