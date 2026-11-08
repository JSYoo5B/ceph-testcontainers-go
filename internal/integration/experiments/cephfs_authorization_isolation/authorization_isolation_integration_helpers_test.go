//go:build all || (integration && features)

package cephfs_authorization_isolation_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testCephFSAuthorizationIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image, _ := harness.IntegrationImages(t)
	cluster, admin := harness.NewServiceCluster(t,
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(1<<30).WithStartupBenchmark(ceph.OSDBenchmarkSkip)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)),
		cephfs.WithFilesystems(cephfs.Config{Name: harness.CephFSIsolationFSA},
			cephfs.Config{Name: harness.CephFSIsolationFSB, StandbyMDS: 1, StandbyReplay: true}))
	var fsA, fsB *cephfs.Filesystem
	for _, fs := range cephfs.Filesystems(cluster) {
		switch fs.FilesystemName {
		case harness.CephFSIsolationFSA:
			fsA = fs
		case harness.CephFSIsolationFSB:
			fsB = fs
		}
	}
	if fsA == nil || fsB == nil {
		t.Fatal("the cluster does not own both filesystems")
	}
	newClient := func(identity *ceph.ClientConfig) testcontainers.Container {
		t.Helper()
		client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), identity.WithIdentity(),
			ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import cephfs, rados"})))
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	harness.CephFSIsolationJSON(t, ctx, admin, harness.CephFSIsolationSeed, harness.CephFSIsolationFSA, harness.CephFSIsolationFSB)

	t.Run("caps per filesystem", func(t *testing.T) { testIsolationFilesystemCaps(t, ctx, cluster, newClient) })
	t.Run("snapshot of a deleted directory", func(t *testing.T) { testIsolationDeletedDirectorySnapshot(t, ctx, cluster, admin, newClient) })
	t.Run("same-named subvolumes", func(t *testing.T) { testIsolationSameNamedSubvolumes(t, ctx, fsA, admin, newClient) })
}

// testIsolationFilesystemCaps checks that MDS caps with one clause per
// filesystem grant each filesystem only its own clause, whatever the clause
// order. Both OSD data pools are writable, so a refused create is the MDS's.
// Each probe is a new libcephfs client, which authenticates with the current
// caps.
func testIsolationFilesystemCaps(t *testing.T, ctx context.Context, cluster *ceph.Container, newClient func(*ceph.ClientConfig) testcontainers.Container) {
	osd := "allow rw tag cephfs data=" + harness.CephFSIsolationFSA + ", allow rw tag cephfs data=" + harness.CephFSIsolationFSB
	clause := func(access, fs string) string { return "allow " + access + " fsname=" + fs }
	variants := []struct {
		mds            string
		writableFS     string
		readOnlyFS     string
		swappedClauses bool
	}{
		{clause("r", harness.CephFSIsolationFSA) + ", " + clause("rw", harness.CephFSIsolationFSB), harness.CephFSIsolationFSB, harness.CephFSIsolationFSA, false},
		{clause("rw", harness.CephFSIsolationFSB) + ", " + clause("r", harness.CephFSIsolationFSA), harness.CephFSIsolationFSB, harness.CephFSIsolationFSA, true},
		{clause("rw", harness.CephFSIsolationFSA) + ", " + clause("r", harness.CephFSIsolationFSB), harness.CephFSIsolationFSA, harness.CephFSIsolationFSB, false},
		{clause("r", harness.CephFSIsolationFSB) + ", " + clause("rw", harness.CephFSIsolationFSA), harness.CephFSIsolationFSA, harness.CephFSIsolationFSB, true},
	}
	identity, err := cluster.CreateClient(ctx, "iso-multi", ceph.ClientCaps{Mon: "allow r", MDS: variants[0].mds, OSD: osd})
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(identity)
	for i, variant := range variants {
		if i > 0 {
			if err := identity.UpdateCaps(ctx, ceph.ClientCaps{Mon: "allow r", MDS: variant.mds, OSD: osd}); err != nil {
				t.Fatal(err)
			}
		}
		caps, err := identity.Capabilities(ctx)
		if err != nil || caps.MDS != variant.mds {
			t.Fatalf("native MDS caps=%q error=%v, want %q", caps.MDS, err, variant.mds)
		}
		for _, fs := range []string{variant.readOnlyFS, variant.writableFS} {
			result := harness.CephFSIsolationJSON(t, ctx, client, isolationCapProbe, identity.User(), identity.KeyringPath(), fs, fmt.Sprintf("v%d", i))
			writable := fs == variant.writableFS
			if result["mount"] != "ok" || result["read"] != "seed-of-"+fs {
				t.Fatalf("caps %q on %s: %v", variant.mds, fs, result)
			}
			for _, op := range []string{"create", "mkdir"} {
				if (writable && result[op] != "ok") || (!writable && result[op] != "EACCES") {
					t.Fatalf("caps %q on %s (writable=%t): %s=%v, all %v", variant.mds, fs, writable, op, result[op], result)
				}
			}
		}
	}
	only, err := cluster.CreateClient(ctx, "iso-only-a", ceph.ClientCaps{Mon: "allow r", MDS: clause("rw", harness.CephFSIsolationFSA), OSD: "allow rw tag cephfs data=" + harness.CephFSIsolationFSA})
	if err != nil {
		t.Fatal(err)
	}
	restrictedClient := newClient(only)
	control := harness.CephFSIsolationJSON(t, ctx, restrictedClient, isolationCapProbe, only.User(), only.KeyringPath(), harness.CephFSIsolationFSA, "only")
	if control["mount"] != "ok" || control["read"] != "seed-of-"+harness.CephFSIsolationFSA || control["create"] != "ok" || control["mkdir"] != "ok" {
		t.Fatalf("per-filesystem principal failed on its authorized filesystem: %v", control)
	}
	result := harness.CephFSIsolationJSON(t, ctx, restrictedClient, isolationCapProbe, only.User(), only.KeyringPath(), harness.CephFSIsolationFSB, "only")
	if result["mount"] != "EPERM" {
		t.Fatalf("a principal without an %s clause mounted it: %v", harness.CephFSIsolationFSB, result)
	}
	for _, c := range []*ceph.ClientConfig{identity, only} {
		if err := c.Delete(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// testIsolationDeletedDirectorySnapshot checks the MDS path check for an
// inode that is reachable only through a snapshot: the live directory was
// unlinked into the MDS stray directory, so its current path is no longer
// under the principal's path.
func testIsolationDeletedDirectorySnapshot(t *testing.T, ctx context.Context, cluster *ceph.Container, admin testcontainers.Container, newClient func(*ceph.ClientConfig) testcontainers.Container) {
	identity, err := cluster.CreateClient(ctx, "iso-dir1", ceph.ClientCaps{Mon: "allow r",
		MDS: "allow rw fsname=" + harness.CephFSIsolationFSB + " path=/dir1", OSD: "allow rw tag cephfs data=" + harness.CephFSIsolationFSB})
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(identity)
	harness.CephFSIsolationJSON(t, ctx, admin, isolationSnapshotSeed, harness.CephFSIsolationFSB)
	before := harness.CephFSIsolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), harness.CephFSIsolationFSB, "before")
	after := harness.CephFSIsolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), harness.CephFSIsolationFSB, "delete")
	for phase, result := range map[string]map[string]any{"before deletion": before, "after deletion": after} {
		if result["snapshot read"] != "dir1-child-bytes" || result["snapshot size"] != float64(len("dir1-child-bytes")) ||
			result["mount /"] != "EPERM" || result["mount /dir2"] != "EPERM" {
			t.Fatalf("%s: %v", phase, result)
		}
	}
	if before["live read"] != "dir1-child-bytes" || after["deleted"] != "ok" || after["live read"] != "ENOENT" {
		t.Fatalf("the live child was not readable before or present after its deletion: before=%v after=%v", before, after)
	}
	harness.CephFSIsolationJSON(t, ctx, admin, isolationSnapshotCleanup, harness.CephFSIsolationFSB)
	if err := identity.Delete(ctx); err != nil {
		t.Fatal(err)
	}
}

// testIsolationSameNamedSubvolumes checks that a subvolume name used in two
// groups maps to two RADOS namespaces, so that the OSD cap of each group's
// authorization covers only its own file data.
func testIsolationSameNamedSubvolumes(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, admin testcontainers.Container, newClient func(*ceph.ClientConfig) testcontainers.Container) {
	type tenant struct {
		volume *subvolume.Subvolume
		info   *subvolume.Metadata
		reader *subvolume.Authorization
		object string
	}
	tenants := make([]*tenant, 0, 2)
	for _, groupName := range []string{"tenant-a", "tenant-b"} {
		group, err := subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: groupName})
		if err != nil {
			t.Fatal(err)
		}
		volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "shared-name", GroupName: group.Name, SizeBytes: 8 << 20, NamespaceIsolated: true})
		if err != nil {
			t.Fatal(err)
		}
		info, err := subvolume.Info(ctx, fs, volume.Name, volume.GroupName)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := volume.Authorize(ctx, subvolume.AuthorizationConfig{ClientID: "reader-" + groupName, Access: "r"})
		if err != nil {
			t.Fatal(err)
		}
		object := harness.CephFSIsolationJSON(t, ctx, admin, isolationTenantSeed, fs.FilesystemName, volume.Path, "bytes-of-"+groupName)["object"]
		tenants = append(tenants, &tenant{volume: volume, info: info, reader: reader, object: fmt.Sprint(object)})
	}
	a, b := tenants[0], tenants[1]
	if a.info.PoolNamespace == "" || a.info.PoolNamespace == b.info.PoolNamespace || a.info.DataPool != b.info.DataPool {
		t.Fatalf("same-named subvolumes share a RADOS namespace: %s=%q %s=%q", a.volume.GroupName, a.info.PoolNamespace, b.volume.GroupName, b.info.PoolNamespace)
	}
	for _, pair := range [][2]*tenant{{a, b}, {b, a}} {
		own, other := pair[0], pair[1]
		identity := own.reader.Client
		caps, err := identity.Capabilities(ctx)
		if err != nil || !strings.Contains(caps.OSD, "namespace="+own.info.PoolNamespace) || strings.Contains(caps.OSD, "namespace="+other.info.PoolNamespace) {
			t.Fatalf("reader of %s has OSD caps %q (error %v)", own.volume.GroupName, caps.OSD, err)
		}
		result := harness.CephFSIsolationJSON(t, ctx, newClient(identity), isolationTenantProbe, identity.User(), identity.KeyringPath(), fs.FilesystemName,
			own.volume.Path, own.info.DataPool, own.info.PoolNamespace, own.object, other.info.PoolNamespace, other.object)
		if result["own file"] != "bytes-of-"+own.volume.GroupName || result["own object"] != "bytes-of-"+own.volume.GroupName || result["other object"] != "EPERM" {
			t.Fatalf("reader of %s: %v", own.volume.GroupName, result)
		}
	}
	for _, tenant := range tenants {
		if err := tenant.reader.Deauthorize(ctx); err != nil {
			t.Fatal(err)
		}
		if err := tenant.reader.Client.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		if err := tenant.volume.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// isolationCapProbe mounts one filesystem as a principal, reads the seed and
// tries to create a file and a directory.
const isolationCapProbe = harness.CephFSIsolationPreamble + `
user, keyring, name, tag = sys.argv[1:]
fs = handle(user, keyring)
out = {"filesystem": name, "mount": attempt(lambda: fs.mount(filesystem_name=name.encode()))}
if out["mount"] == "ok":
    out["read"] = read(fs, '/shared/seed')
    out["create"] = attempt(lambda: write(fs, '/shared/file-' + tag, tag))
    out["mkdir"] = attempt(lambda: fs.mkdir(('/shared/dir-' + tag).encode(), 0o755))
    fs.unmount()
fs.shutdown()
print(json.dumps(out))
`

// isolationSnapshotSeed creates /dir1/child/file and /dir2/file and takes the
// /dir1 snapshot "before-delete".
const isolationSnapshotSeed = harness.CephFSIsolationPreamble + `
fs = handle()
fs.mount(filesystem_name=sys.argv[1].encode())
for path in (b'/dir1', b'/dir1/child', b'/dir2'):
    fs.mkdir(path, 0o755)
write(fs, '/dir1/child/file', 'dir1-child-bytes')
write(fs, '/dir2/file', 'unrelated')
fs.mkdir(b'/dir1/.snap/before-delete', 0o755)
fs.unmount()
fs.shutdown()
print(json.dumps({"snapshot": "before-delete"}))
`

// isolationSnapshotProbe mounts /dir1 as the restricted principal, optionally
// deletes the live child, and reads the child's file live and through the
// snapshot. Mounting / or /dir2 must be refused.
const isolationSnapshotProbe = harness.CephFSIsolationPreamble + `
user, keyring, name, phase = sys.argv[1:]
out = {}
for root in ('/', '/dir2'):
    fs = handle(user, keyring)
    out["mount " + root] = attempt(lambda: fs.mount(mount_root=root.encode(), filesystem_name=name.encode()))
    if out["mount " + root] == "ok":
        fs.unmount()
    fs.shutdown()
fs = handle(user, keyring)
fs.mount(mount_root=b'/dir1', filesystem_name=name.encode())
if phase == "delete":
    out["deleted"] = attempt(lambda: (fs.unlink(b'/child/file'), fs.rmdir(b'/child')))
out["live read"] = read(fs, '/child/file')
out["snapshot read"] = read(fs, '/.snap/before-delete/child/file')
try:
    out["snapshot size"] = fs.stat(b'/.snap/before-delete/child/file').st_size
except cephfs.Error as error:
    out["snapshot size"] = code(error)
fs.unmount()
fs.shutdown()
print(json.dumps(out))
`

const isolationSnapshotCleanup = harness.CephFSIsolationPreamble + `
fs = handle()
fs.mount(filesystem_name=sys.argv[1].encode())
fs.rmdir(b'/dir1/.snap/before-delete')
fs.unlink(b'/dir2/file')
for path in (b'/dir2', b'/dir1'):
    fs.rmdir(path)
fs.unmount()
fs.shutdown()
print(json.dumps({"cleaned": True}))
`

// isolationTenantSeed writes one file in a subvolume as the admin and reports
// the RADOS object that holds its first bytes.
const isolationTenantSeed = harness.CephFSIsolationPreamble + `
name, path, data = sys.argv[1:]
fs = handle()
fs.mount(filesystem_name=name.encode())
write(fs, path + '/data', data)
ino = fs.stat((path + '/data').encode()).st_ino
fs.unmount()
fs.shutdown()
print(json.dumps({"object": "%x.00000000" % ino}))
`

// isolationTenantProbe reads, as one subvolume's reader, its own file through
// libcephfs and its own object through librados, then the other subvolume's
// object in the other namespace.
const isolationTenantProbe = harness.CephFSIsolationPreamble + `import rados
user, keyring, name, path, pool, namespace, obj, other_namespace, other_obj = sys.argv[1:]
fs = handle(user, keyring)
fs.mount(mount_root=path.encode(), filesystem_name=name.encode())
out = {"own file": read(fs, '/data')}
fs.unmount()
fs.shutdown()
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.' + user, conf={'keyring': keyring})
cluster.connect(timeout=10)
try:
    with cluster.open_ioctx(pool) as io:
        for key, ns, o in (("own object", namespace, obj), ("other object", other_namespace, other_obj)):
            io.set_namespace(ns)
            try:
                out[key] = io.read(o).decode()
            except rados.Error as error:
                out[key] = errno.errorcode.get(abs(error.errno), str(error.errno)) if error.errno else type(error).__name__
finally:
    cluster.shutdown()
print(json.dumps(out))
`
