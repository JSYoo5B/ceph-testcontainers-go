//go:build all || (integration && features)

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/json"
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

const (
	isolationFSA = "iso-a"
	isolationFSB = "iso-b"
)

func testCephFSMultiFilesystemIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	image, _ := harness.IntegrationImages(t)
	cluster, admin := harness.NewServiceCluster(t,
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(1<<30).WithStartupBenchmark(ceph.OSDBenchmarkSkip)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)),
		cephfs.WithFilesystems(cephfs.Config{Name: isolationFSA},
			cephfs.Config{Name: isolationFSB, StandbyMDS: 1, StandbyReplay: true}))
	var fsA, fsB *cephfs.Filesystem
	for _, fs := range cephfs.Filesystems(cluster) {
		switch fs.FilesystemName {
		case isolationFSA:
			fsA = fs
		case isolationFSB:
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
	isolationJSON(t, ctx, admin, isolationSeed, isolationFSA, isolationFSB)

	t.Run("caps per filesystem", func(t *testing.T) { testIsolationFilesystemCaps(t, ctx, cluster, newClient) })
	t.Run("snapshot of a deleted directory", func(t *testing.T) { testIsolationDeletedDirectorySnapshot(t, ctx, cluster, admin, newClient) })
	t.Run("same-named subvolumes", func(t *testing.T) { testIsolationSameNamedSubvolumes(t, ctx, fsA, admin, newClient) })
	t.Run("survivor of a removal", func(t *testing.T) { testIsolationSurvivor(t, ctx, cluster, admin, fsA, fsB) })
}

// testIsolationFilesystemCaps checks that MDS caps with one clause per
// filesystem grant each filesystem only its own clause, whatever the clause
// order. Both OSD data pools are writable, so a refused create is the MDS's.
// Each probe is a new libcephfs client, which authenticates with the current
// caps.
func testIsolationFilesystemCaps(t *testing.T, ctx context.Context, cluster *ceph.Container, newClient func(*ceph.ClientConfig) testcontainers.Container) {
	osd := "allow rw tag cephfs data=" + isolationFSA + ", allow rw tag cephfs data=" + isolationFSB
	clause := func(access, fs string) string { return "allow " + access + " fsname=" + fs }
	variants := []struct {
		mds            string
		writableFS     string
		readOnlyFS     string
		swappedClauses bool
	}{
		{clause("r", isolationFSA) + ", " + clause("rw", isolationFSB), isolationFSB, isolationFSA, false},
		{clause("rw", isolationFSB) + ", " + clause("r", isolationFSA), isolationFSB, isolationFSA, true},
		{clause("rw", isolationFSA) + ", " + clause("r", isolationFSB), isolationFSA, isolationFSB, false},
		{clause("r", isolationFSB) + ", " + clause("rw", isolationFSA), isolationFSA, isolationFSB, true},
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
			result := isolationJSON(t, ctx, client, isolationCapProbe, identity.User(), identity.KeyringPath(), fs, fmt.Sprintf("v%d", i))
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
	only, err := cluster.CreateClient(ctx, "iso-only-a", ceph.ClientCaps{Mon: "allow r", MDS: clause("rw", isolationFSA), OSD: "allow rw tag cephfs data=" + isolationFSA})
	if err != nil {
		t.Fatal(err)
	}
	result := isolationJSON(t, ctx, newClient(only), isolationCapProbe, only.User(), only.KeyringPath(), isolationFSB, "only")
	if result["mount"] != "EPERM" {
		t.Fatalf("a principal without an %s clause mounted it: %v", isolationFSB, result)
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
		MDS: "allow rw fsname=" + isolationFSB + " path=/dir1", OSD: "allow rw tag cephfs data=" + isolationFSB})
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(identity)
	isolationJSON(t, ctx, admin, isolationSnapshotSeed, isolationFSB)
	before := isolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), isolationFSB, "before")
	after := isolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), isolationFSB, "delete")
	for phase, result := range map[string]map[string]any{"before deletion": before, "after deletion": after} {
		if result["snapshot read"] != "dir1-child-bytes" || result["snapshot size"] != float64(len("dir1-child-bytes")) ||
			result["mount /"] != "EPERM" || result["mount /dir2"] != "EPERM" {
			t.Fatalf("%s: %v", phase, result)
		}
	}
	if before["live read"] != "dir1-child-bytes" || after["deleted"] != "ok" || after["live read"] != "ENOENT" {
		t.Fatalf("the live child was not readable before or present after its deletion: before=%v after=%v", before, after)
	}
	isolationJSON(t, ctx, admin, isolationSnapshotCleanup, isolationFSB)
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
		object := isolationJSON(t, ctx, admin, isolationTenantSeed, fs.FilesystemName, volume.Path, "bytes-of-"+groupName)["object"]
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
		result := isolationJSON(t, ctx, newClient(identity), isolationTenantProbe, identity.User(), identity.KeyringPath(), fs.FilesystemName,
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

// testIsolationSurvivor removes iso-a and checks iso-b in the first reads
// after Remove returns: the same FSMap ID, active GID and standby-replay GID
// in MDSStatus, the native join_fscid of both daemons, and its bytes.
func testIsolationSurvivor(t *testing.T, ctx context.Context, cluster *ceph.Container, admin testcontainers.Container, fsA, fsB *cephfs.Filesystem) {
	if err := fsB.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fsB.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 || len(before.StandbyReplay) != 1 {
		t.Fatalf("%s before removal: %+v error=%v", isolationFSB, before, err)
	}
	if err := fsA.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fsB.MDSStatus(ctx)
	if err != nil || after.FilesystemID != before.FilesystemID || len(after.Active) != 1 || len(after.StandbyReplay) != 1 ||
		after.Active[0].GID != before.Active[0].GID || after.StandbyReplay[0].GID != before.StandbyReplay[0].GID {
		t.Fatalf("%s changed when %s was removed: before=%+v after=%+v error=%v", isolationFSB, isolationFSA, before, after, err)
	}
	data, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var fsmap struct {
		Filesystems []struct {
			ID     int64 `json:"id"`
			MDSMap struct {
				Name string `json:"fs_name"`
				Info map[string]struct {
					Name      string `json:"name"`
					JoinFSCID int64  `json:"join_fscid"`
				} `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(data, &fsmap); err != nil {
		t.Fatal(err)
	}
	if len(fsmap.Filesystems) != 1 || fsmap.Filesystems[0].MDSMap.Name != isolationFSB || len(fsmap.Filesystems[0].MDSMap.Info) != 2 {
		t.Fatalf("FSMap after removal: %s", data)
	}
	for _, info := range fsmap.Filesystems[0].MDSMap.Info {
		if info.JoinFSCID != after.FilesystemID {
			t.Fatalf("%s daemon %s has join_fscid %d, want %d: %s", isolationFSB, info.Name, info.JoinFSCID, after.FilesystemID, data)
		}
	}
	if result := isolationJSON(t, ctx, admin, isolationSurvivorRead, isolationFSB); result["read"] != "seed-of-"+isolationFSB {
		t.Fatalf("fresh read of %s: %v", isolationFSB, result)
	}
}

// isolationJSON runs a Python probe in client and decodes the JSON object it
// prints last.
func isolationJSON(t *testing.T, ctx context.Context, client testcontainers.Container, script string, args ...string) map[string]any {
	t.Helper()
	out := cephFSSubvolumeExec(t, ctx, client, append([]string{"python3", "-c", script}, args...)...)
	lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
	result := map[string]any{}
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil {
		t.Fatalf("probe output %s: %v", out, err)
	}
	t.Logf("native probe: %s", lines[len(lines)-1])
	return result
}

// isolationPreamble opens libcephfs handles for the admin or for one
// principal, and names an errno the way the probes report it.
const isolationPreamble = `import cephfs, errno, json, os, sys, threading
deadline = threading.Timer(90, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
def handle(user=None, keyring=None):
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id=user) if user else cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf')
    if keyring:
        fs.conf_set('keyring', keyring)
    fs.conf_set('client_mount_timeout', '20')
    return fs
def code(error):
    return errno.errorcode.get(abs(error.args[0]), str(error.args[0]))
def attempt(operation):
    try:
        operation()
        return "ok"
    except cephfs.Error as error:
        return code(error)
def read(fs, path):
    try:
        fd = fs.open(path.encode(), 'r')
    except cephfs.Error as error:
        return code(error)
    try:
        return fs.read(fd, 0, 64).decode()
    finally:
        fs.close(fd)
def write(fs, path, data):
    fd = fs.open(path.encode(), 'w', 0o644)
    fs.write(fd, data.encode(), 0)
    fs.fsync(fd, 0)
    fs.close(fd)
`

// isolationSeed gives each filesystem a world-writable /shared with a known
// seed file, so POSIX permissions never refuse a create.
const isolationSeed = isolationPreamble + `
for name in sys.argv[1:]:
    fs = handle()
    fs.mount(filesystem_name=name.encode())
    fs.mkdir(b'/shared', 0o777)
    fs.chmod(b'/shared', 0o777)
    write(fs, '/shared/seed', 'seed-of-' + name)
    fs.unmount()
    fs.shutdown()
print(json.dumps({"seeded": sys.argv[1:]}))
`

// isolationCapProbe mounts one filesystem as a principal, reads the seed and
// tries to create a file and a directory.
const isolationCapProbe = isolationPreamble + `
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
const isolationSnapshotSeed = isolationPreamble + `
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
const isolationSnapshotProbe = isolationPreamble + `
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

const isolationSnapshotCleanup = isolationPreamble + `
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
const isolationTenantSeed = isolationPreamble + `
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
const isolationTenantProbe = isolationPreamble + `import rados
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

const isolationSurvivorRead = isolationPreamble + `
fs = handle()
fs.mount(filesystem_name=sys.argv[1].encode())
out = {"read": read(fs, '/shared/seed')}
fs.unmount()
fs.shutdown()
print(json.dumps(out))
`
