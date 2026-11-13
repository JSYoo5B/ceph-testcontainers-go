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

// image is the Ceph image the case runs on: Squid 19.2.2, which reproduces
// all three defects. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_authorization_isolation_test.image=<image>"
//
// Squid 19.2.3 fixes the snapshot defect, 19.2.4 the subvolume one and
// 19.2.5 the caps one; 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0 reproduce
// none of them.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:bc62130f013ac78f1d9b5a774b366b0ed4ce7bda4acf8734bfadc14603d16b23"

func testCephFSAuthorizationIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_AUTHORIZATION_ISOLATION image=%s", image)
	cluster, admin := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t),
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
		client, err := testcontainers.Run(ctx, image, identity.WithIdentity(),
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

	// Each subtest is one defect and collects the symptoms its then step
	// observes; none means NOT_REPRODUCED for that defect.
	defects := []struct {
		name, result string
		symptoms     []string
		run          func(*testing.T, *[]string)
	}{
		{name: "caps per filesystem", result: "caps-per-filesystem", run: func(t *testing.T, symptoms *[]string) {
			testIsolationFilesystemCaps(t, ctx, cluster, newClient, symptoms)
		}},
		{name: "snapshot of a deleted directory", result: "snapshot-of-a-deleted-directory", run: func(t *testing.T, symptoms *[]string) {
			testIsolationDeletedDirectorySnapshot(t, ctx, cluster, admin, newClient, symptoms)
		}},
		{name: "same-named subvolumes", result: "same-named-subvolumes", run: func(t *testing.T, symptoms *[]string) {
			testIsolationSameNamedSubvolumes(t, ctx, fsA, admin, newClient, symptoms)
		}},
	}
	for i := range defects {
		t.Run(defects[i].name, func(t *testing.T) { defects[i].run(t, &defects[i].symptoms) })
	}
	if t.Failed() {
		return
	}
	for _, defect := range defects {
		result := "NOT_REPRODUCED"
		if len(defect.symptoms) > 0 {
			result = "REPRODUCED"
		}
		t.Logf("EXPERIMENT_RESULT case=cephfs_authorization_isolation/%s result=%s image=%s symptoms=%q", defect.result, result, image, defect.symptoms)
	}
}

// testIsolationFilesystemCaps reports whether MDS caps with one clause per
// filesystem grant writes on a filesystem whose clause is read-only, in
// either clause order. Both OSD data pools are writable, so a refused create
// is the MDS's. Each probe is a new libcephfs client, which authenticates
// with the current caps. A create or mkdir that succeeds in the read-only
// filesystem is a symptom; the writable filesystem, the seed reads and the
// principal without an iso-b clause are controls.
func testIsolationFilesystemCaps(t *testing.T, ctx context.Context, cluster *ceph.Container, newClient func(*ceph.ClientConfig) testcontainers.Container, symptoms *[]string) {
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
				switch {
				case !writable && result[op] == "ok":
					*symptoms = append(*symptoms, fmt.Sprintf("caps %q: %s succeeded in the read-only filesystem %s", variant.mds, op, fs))
				case (writable && result[op] != "ok") || (!writable && result[op] != "EACCES"):
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

// testIsolationDeletedDirectorySnapshot reports whether the MDS path check
// refuses an inode that is reachable only through a snapshot: the live
// directory was unlinked into the MDS stray directory, so its current path is
// no longer under the principal's path. A snapshot read that fails after the
// deletion is a symptom; the reads before it, the live child and the refused
// mounts of / and /dir2 are controls.
func testIsolationDeletedDirectorySnapshot(t *testing.T, ctx context.Context, cluster *ceph.Container, admin testcontainers.Container, newClient func(*ceph.ClientConfig) testcontainers.Container, symptoms *[]string) {
	identity, err := cluster.CreateClient(ctx, "iso-dir1", ceph.ClientCaps{Mon: "allow r",
		MDS: "allow rw fsname=" + harness.CephFSIsolationFSB + " path=/dir1", OSD: "allow rw tag cephfs data=" + harness.CephFSIsolationFSB})
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(identity)
	harness.CephFSIsolationJSON(t, ctx, admin, isolationSnapshotSeed, harness.CephFSIsolationFSB)
	before := harness.CephFSIsolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), harness.CephFSIsolationFSB, "before")
	after := harness.CephFSIsolationJSON(t, ctx, client, isolationSnapshotProbe, identity.User(), identity.KeyringPath(), harness.CephFSIsolationFSB, "delete")
	readable := func(result map[string]any) bool {
		return result["snapshot read"] == "dir1-child-bytes" && result["snapshot size"] == float64(len("dir1-child-bytes"))
	}
	for phase, result := range map[string]map[string]any{"before deletion": before, "after deletion": after} {
		if result["mount /"] != "EPERM" || result["mount /dir2"] != "EPERM" {
			t.Fatalf("%s: %v", phase, result)
		}
	}
	if !readable(before) || before["live read"] != "dir1-child-bytes" || after["deleted"] != "ok" || after["live read"] != "ENOENT" {
		t.Fatalf("the child was not readable live and through the snapshot before, or present after its deletion: before=%v after=%v", before, after)
	}
	if !readable(after) {
		*symptoms = append(*symptoms, fmt.Sprintf("after the deletion the snapshot read is %v and its size %v", after["snapshot read"], after["snapshot size"]))
	}
	harness.CephFSIsolationJSON(t, ctx, admin, isolationSnapshotCleanup, harness.CephFSIsolationFSB)
	if err := identity.Delete(ctx); err != nil {
		t.Fatal(err)
	}
}

// testIsolationSameNamedSubvolumes reports whether a subvolume name used in
// two groups maps to one RADOS namespace, so that the OSD cap of each
// group's authorization also covers the other's file data. One namespace, a
// reader cap that names the other's namespace, and a read of the other's
// object are symptoms; the readers' own file and object are controls.
func testIsolationSameNamedSubvolumes(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, admin testcontainers.Container, newClient func(*ceph.ClientConfig) testcontainers.Container, symptoms *[]string) {
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
	if a.info.PoolNamespace == "" || b.info.PoolNamespace == "" || a.info.DataPool != b.info.DataPool {
		t.Fatalf("the namespace-isolated subvolumes have no namespace or different data pools: %s=%+v %s=%+v", a.volume.GroupName, a.info, b.volume.GroupName, b.info)
	}
	shared := a.info.PoolNamespace == b.info.PoolNamespace
	if shared {
		*symptoms = append(*symptoms, fmt.Sprintf("same-named subvolumes share the RADOS namespace %q", a.info.PoolNamespace))
	}
	for _, pair := range [][2]*tenant{{a, b}, {b, a}} {
		own, other := pair[0], pair[1]
		identity := own.reader.Client
		caps, err := identity.Capabilities(ctx)
		if err != nil || !strings.Contains(caps.OSD, "namespace="+own.info.PoolNamespace) {
			t.Fatalf("reader of %s has OSD caps %q (error %v)", own.volume.GroupName, caps.OSD, err)
		}
		if !shared && strings.Contains(caps.OSD, "namespace="+other.info.PoolNamespace) {
			*symptoms = append(*symptoms, fmt.Sprintf("reader of %s has OSD caps %q, which name the other namespace", own.volume.GroupName, caps.OSD))
		}
		result := harness.CephFSIsolationJSON(t, ctx, newClient(identity), isolationTenantProbe, identity.User(), identity.KeyringPath(), fs.FilesystemName,
			own.volume.Path, own.info.DataPool, own.info.PoolNamespace, own.object, other.info.PoolNamespace, other.object)
		if result["own file"] != "bytes-of-"+own.volume.GroupName || result["own object"] != "bytes-of-"+own.volume.GroupName {
			t.Fatalf("reader of %s: %v", own.volume.GroupName, result)
		}
		switch result["other object"] {
		case "EPERM":
		case "bytes-of-" + other.volume.GroupName:
			*symptoms = append(*symptoms, fmt.Sprintf("reader of %s read the object of %s", own.volume.GroupName, other.volume.GroupName))
		default:
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
