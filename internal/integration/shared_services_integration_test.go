//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_shared_cluster))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// TestSharedClusterServices leases RBD namespaces, CephFS subvolume groups
// and RGW tenants of one shared cluster: two tenants at a time are confined
// to their own resources, releases remove them, also after a client that
// died with an open image or a mounted filesystem, and a release that finds
// a service resource of no tenant poisons the cluster.
func TestSharedClusterServices(t *testing.T) {
	shared := newSharedCluster(t, ceph.WithSharedRunOptions(
		rbd.WithPools(ceph.PoolConfig{Name: sharedRBDPool}), cephfs.WithFilesystems(), rgw.WithGateways()))
	var fsid string

	t.Run("rbd", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		a, b := shared.Lease(t), shared.Lease(t)
		fsid = sharedFSID(t, ctx, a)
		nsA, nsB := leaseRBD(t, ctx, a), leaseRBD(t, ctx, b)
		ctrA, ctrB := startTenantClientWith(t, ctx, nsA.WithClient()), startTenantClientWith(t, ctx, nsB.WithClient())
		rbdA := func(args ...string) []string {
			return append([]string{"rbd", "--id", nsA.Client().User()}, args...)
		}
		own := sharedRBDPool + "/" + nsA.Namespace() + "/"
		for _, step := range [][]string{
			rbdA("create", "--size", "8M", own+"image"),
			rbdA("snap", "create", own+"image@snap"),
			rbdA("snap", "protect", own+"image@snap"),
			rbdA("clone", own+"image@snap", own+"clone"),
			rbdA("create", "--size", "8M", own+"trashed"),
			rbdA("trash", "mv", own+"trashed"),
			rbdA("ls", "--pool", sharedRBDPool, "--namespace", nsA.Namespace()),
		} {
			tenantExpectArgv(t, ctx, a, ctrA, true, strings.Join(step[3:], " "), step...)
		}
		tenantExpectArgv(t, ctx, b, ctrB, true, "the other tenant creates its image",
			"rbd", "--id", nsB.Client().User(), "create", "--size", "8M", sharedRBDPool+"/"+nsB.Namespace()+"/image")
		other := sharedRBDPool + "/" + nsB.Namespace() + "/image"
		for _, denied := range []struct {
			label string
			argv  []string
		}{
			{"list the other tenant's namespace", rbdA("ls", "--pool", sharedRBDPool, "--namespace", nsB.Namespace())},
			{"open the other tenant's image", rbdA("info", other)},
			{"remove the other tenant's image", rbdA("rm", "--no-progress", other)},
			{"create an image in the default namespace", rbdA("create", "--size", "8M", sharedRBDPool+"/stray")},
			{"create a namespace", rbdA("namespace", "create", "--pool", sharedRBDPool, "--namespace", "stray")},
			{"add a blocklist entry", []string{"ceph", "--id", nsA.Client().User(), "--connect-timeout", "10", "osd", "blocklist", "add", "192.0.2.1:0/1", "60"}},
		} {
			tenantExpectArgv(t, ctx, a, ctrA, false, denied.label, denied.argv...)
		}
		// A client that ends with the image open and its exclusive lock held
		// leaves a watch for the OSDs to time out and a lock to break.
		tenantExpectArgv(t, ctx, a, ctrA, true, "create the held image", rbdA("create", "--size", "8M", own+"held")...)
		holder := startHolder(t, ctx, nsA.WithClient(), "IMAGE_HELD", rbdHolderScript, nsA.Client().User(), sharedRBDPool, nsA.Namespace())
		if err := holder.Terminate(ctx, testcontainersStopNow()); err != nil {
			t.Fatal(err)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("cephfs", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		a, b := shared.Lease(t), shared.Lease(t)
		groupA, groupB := leaseCephFS(t, ctx, a), leaseCephFS(t, ctx, b)
		if groupA.Path() != "/volumes/"+a.ID() || groupA.PoolNamespace() != "fsns-"+a.ID() {
			t.Fatalf("group path %s, namespace %s", groupA.Path(), groupA.PoolNamespace())
		}
		subvolume, err := groupA.CreateSubvolume(ctx, "data", 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(subvolume, groupA.Path()+"/data/") {
			t.Fatalf("subvolume path %s is outside the group", subvolume)
		}
		ctrA, ctrB := startTenantClientWith(t, ctx, groupA.WithClient()), startTenantClientWith(t, ctx, groupB.WithClient())
		fsArgs := func(group *cephfs.TenantGroup, phase, root, path string) []string {
			return []string{"python3", "-c", cephFSTenantScript, group.Client().User(), group.FilesystemName(), phase, root, path}
		}
		tenantExpectArgv(t, ctx, a, ctrA, true, "write the group directory", fsArgs(groupA, "write", groupA.Path(), "/top")...)
		tenantExpectArgv(t, ctx, a, ctrA, true, "write the subvolume", fsArgs(groupA, "write", groupA.Path(), strings.TrimPrefix(subvolume, groupA.Path())+"/file")...)
		tenantExpectArgv(t, ctx, b, ctrB, true, "the other tenant writes its group", fsArgs(groupB, "write", groupB.Path(), "/top")...)
		for _, root := range []string{"/", "/volumes", groupB.Path()} {
			tenantExpectArgv(t, ctx, a, ctrA, true, "mount "+root+" is refused", fsArgs(groupA, "denied", root, "")...)
		}
		tenantExpectArgv(t, ctx, a, ctrA, true, "file data lands in the group's namespace",
			"sh", "-c", `test -n "$(rados --id "$1" -p "$2" -N "$3" ls)"`, "sh", groupA.Client().User(), groupA.DataPool(), groupA.PoolNamespace())
		tenantExpectArgv(t, ctx, a, ctrA, false, "list the other group's data namespace",
			"rados", "--id", groupA.Client().User(), "-p", groupA.DataPool(), "-N", groupB.PoolNamespace(), "ls")
		// A client that ends with the filesystem mounted leaves a session
		// that the release evicts, without leaving a blocklist entry.
		holder := startHolder(t, ctx, groupA.WithClient(), "MOUNTED", cephFSHolderScript, groupA.Client().User(), groupA.FilesystemName(), groupA.Path())
		if err := holder.Terminate(ctx, testcontainersStopNow()); err != nil {
			t.Fatal(err)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("rgw", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		a, b := shared.Lease(t), shared.Lease(t)
		userA, userB := leaseRGW(t, ctx, a), leaseRGW(t, ctx, b)
		if userA.UserID() != a.ID()+"$user" || userA.Tenant() != a.ID() {
			t.Fatalf("RGW user %s of tenant %s", userA.UserID(), userA.Tenant())
		}
		s3A, s3B := tenantS3(t, ctx, userA), tenantS3(t, ctx, userB)
		const bucket = "/shared-name"
		s3A.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		s3A.request(t, ctx, http.MethodPut, bucket+"/object", []byte("tenant a"), http.StatusOK)
		s3B.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		s3B.request(t, ctx, http.MethodPut, bucket+"/object", []byte("tenant b"), http.StatusOK)
		if got := string(s3A.request(t, ctx, http.MethodGet, bucket+"/object", nil, http.StatusOK)); got != "tenant a" {
			t.Fatalf("tenant a read %q from its bucket", got)
		}
		if names := listBuckets(t, ctx, s3A); !slices.Equal(names, []string{"shared-name"}) {
			t.Fatalf("tenant a lists buckets %v", names)
		}
		otherBucket := "/" + userB.Tenant() + ":shared-name"
		s3A.request(t, ctx, http.MethodGet, otherBucket+"?list-type=2", nil, http.StatusForbidden)
		s3A.request(t, ctx, http.MethodGet, otherBucket+"/object", nil, http.StatusForbidden)
		s3A.request(t, ctx, http.MethodPut, otherBucket+"/intruder", []byte("x"), http.StatusForbidden)
		s3B.request(t, ctx, http.MethodGet, otherBucket+"?list-type=2", nil, http.StatusOK)
	})
	if t.Failed() {
		return
	}

	t.Run("released", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		if stats := shared.Stats(); stats.Starts != 1 || stats.Poisonings != 0 {
			t.Fatalf("clean service tenants poisoned the cluster: %+v", stats)
		}
		checker := shared.Lease(t)
		if got := sharedFSID(t, ctx, checker); got != fsid {
			t.Fatalf("the cluster changed from %s to %s", fsid, got)
		}
		cluster := testhelper.TestTenantCluster(checker)
		var namespaces []json.RawMessage
		decodeCommand(t, ctx, cluster, &namespaces, "rbd", "namespace", "ls", "--pool", sharedRBDPool, "--format", "json")
		var groups []json.RawMessage
		decodeCommand(t, ctx, cluster, &groups, "ceph", "fs", "subvolumegroup", "ls", "tc-cephfs", "--format", "json")
		gateways := clusteraccess.Gateways(t, cluster)
		if len(gateways) != 1 {
			t.Fatalf("%d gateways", len(gateways))
		}
		data, err := gateways[0].Admin(ctx, "metadata", "list", "user")
		if err != nil {
			t.Fatal(err)
		}
		var users []string
		if err := json.Unmarshal(data, &users); err != nil {
			t.Fatal(err)
		}
		if len(namespaces) != 0 || len(groups) != 0 || slices.ContainsFunc(users, func(user string) bool { return strings.Contains(user, "$") }) {
			t.Fatalf("released tenants left RBD namespaces %s, subvolume groups %s or RGW users %v", namespaces, groups, users)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("dirty service tenant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		dirty := &cleanupTB{TB: t}
		tenant := shared.Lease(dirty)
		leaseRBD(t, ctx, tenant)
		// An admin path that the gate does not cover adds a namespace that
		// belongs to no tenant.
		if _, err := servicehook.ClientCommand(ctx, testhelper.TestTenantCluster(tenant), "rbd", "namespace", "create", "--pool", sharedRBDPool, "--namespace", "stray"); err != nil {
			t.Fatal(err)
		}
		dirty.finish()
		if len(dirty.errors) != 1 || !strings.Contains(dirty.errors[0], "rbd namespace "+sharedRBDPool+"/stray was added") {
			t.Fatalf("the dirty tenant's release reported %q", dirty.errors)
		}
		next := shared.Lease(t)
		if got := sharedFSID(t, ctx, next); got == fsid {
			t.Fatalf("the lease after a dirty tenant got the poisoned cluster %s", got)
		}
		if stats := shared.Stats(); stats.Starts != 2 || stats.Recycles != 1 || stats.Poisonings != 1 {
			t.Fatalf("stats = %+v", stats)
		}
	})
}

// rbdHolderScript opens an image, writes to it, which takes its exclusive
// lock, and keeps it open.
const rbdHolderScript = `
import rados, rbd, sys, time
user, pool, namespace = sys.argv[1:]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", rados_id=user)
cluster.connect(timeout=30)
ioctx = cluster.open_ioctx(pool)
ioctx.set_namespace(namespace)
image = rbd.Image(ioctx, "held")
image.write(b"x" * 4096, 0)
print("IMAGE_HELD", flush=True)
time.sleep(3600)
`

// cephFSHolderScript mounts the group, writes a file and keeps the mount.
const cephFSHolderScript = `
import cephfs, os, sys, time
user, name, root = sys.argv[1:]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf", auth_id=user)
fs.mount(mount_root=root.encode(), filesystem_name=name.encode())
fd = fs.open("/held", os.O_CREAT | os.O_WRONLY, 0o600)
fs.write(fd, b"held", 0)
fs.fsync(fd, False)
print("MOUNTED", flush=True)
time.sleep(3600)
`

// cephFSTenantScript, as a group's client: write mounts root and writes and
// reads back path; denied expects the mount of root to be refused.
const cephFSTenantScript = `
import cephfs, os, sys
user, name, phase, root, path = sys.argv[1:]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf", auth_id=user)
fs.conf_set("client_mount_timeout", "20")
try:
    try:
        fs.mount(mount_root=root.encode(), filesystem_name=name.encode())
    except cephfs.Error as e:
        if phase == "denied" and isinstance(e, (cephfs.PermissionError, cephfs.OSError)) and getattr(e, "errno", 0) in (1, 13):
            print("refused:", e)
            sys.exit(0)
        raise
    if phase == "denied":
        print("the mount of", root, "was allowed")
        sys.exit(1)
    data = b"tenant data " * 1024
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data)
        fs.fsync(fd, False)
    finally:
        fs.close(fd)
    fd = fs.open(path, os.O_RDONLY)
    try:
        assert fs.read(fd, 0, len(data) + 1) == data
    finally:
        fs.close(fd)
finally:
    fs.shutdown()
`

// ListAllMyBucketsResult is the S3 ListBuckets response.
type listAllMyBucketsResult struct {
	Buckets []struct {
		Name string `xml:"Name"`
	} `xml:"Buckets>Bucket"`
}

func listBuckets(t *testing.T, ctx context.Context, s3 s3HTTPClient) []string {
	t.Helper()
	var listing listAllMyBucketsResult
	if err := xml.Unmarshal(s3.request(t, ctx, http.MethodGet, "/", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, bucket := range listing.Buckets {
		names = append(names, bucket.Name)
	}
	return names
}
