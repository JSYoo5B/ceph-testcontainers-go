//go:build integration && features

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

func TestCephFSSubvolumes(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSSubvolumes(t, host) })
	}
}

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
	if err := fs.ResizeSubvolume(ctx, subvolume, 8<<20); err != nil {
		t.Fatal(err)
	}
	if err := fs.ResizeSubvolumeGroup(ctx, group, 64<<20); err != nil {
		t.Fatal(err)
	}
	info, err = fs.SubvolumeInfo(ctx, subvolume.Name, group.Name)
	if err != nil || info.QuotaBytes != 8<<20 {
		t.Fatalf("subvolume quota did not grow: %+v error=%v", info, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "grow", "tenant", 8<<20)
	cephFSSubvolumeIO(t, ctx, client, filesystem, defaultInfo, "verify", "default", 0)
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
	t.Log("native CephFS volumes: existing filesystem/MDS/pools, group/default-group isolation, selected additional pool and RADOS namespace, duplicate refusal, quota growth/unlimited/no-shrink with retained bytes, safe removal and copied-handle replacement protection passed")
}

func cephFSSubvolumeIO(t *testing.T, parent context.Context, client testcontainers.Container, filesystem string, info *ceph.CephFSSubvolumeInfo, phase, token string, quota int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	args := []string{"python3", "-c", cephFSSubvolumeIOScript, filesystem, info.Path, info.DataPool, info.PoolNamespace, phase, token, strconv.FormatInt(quota, 10)}
	code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("fresh native CephFS subvolume %s: exit=%d error=%v output=%s", phase, code, err, data)
	}
	var proof map[string]any
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatalf("invalid native proof: %s error=%v", data, err)
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

const cephFSSubvolumeIOScript = `import cephfs, hashlib, json, os, sys, threading
filesystem, root, pool, namespace, phase, token, quota = sys.argv[1:]
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '25')
fs.conf_set('rados_osd_op_timeout', '15')
fs.mount(filesystem_name=filesystem.encode())
seed = (token.encode() + bytes(range(256))) * 1024
seed = (seed * 2)[:256 << 10]
growth = bytes((value ^ 0x5a) for value in range(256)) * 8192
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
    print(json.dumps({'phase': phase, 'path': root, 'data_pool': pool, 'namespace': namespace, 'quota': int(quota), 'baseline_sha256': hashlib.sha256(seed).hexdigest(), 'growth_bytes': len(growth)}))
finally:
    fs.shutdown()
    deadline.cancel()
`
