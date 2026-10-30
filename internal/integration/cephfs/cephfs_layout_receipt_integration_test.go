//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_layout_receipts))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// cephFSLayoutReceipt is what TestCephFSLayoutReceipt logs.
type cephFSLayoutReceipt struct {
	Filesystem  string                   `json:"filesystem"`
	Path        string                   `json:"path"`
	Inode       uint64                   `json:"inode"`
	LayoutXattr string                   `json:"layout_xattr"`
	Pool        string                   `json:"pool"`
	Namespace   string                   `json:"pool_namespace"`
	Layout      harness.StripedLayout    `json:"layout"`
	Writes      []harness.LayoutWrite    `json:"writes"`
	Snapshot    string                   `json:"snapshot"`
	SnapID      uint64                   `json:"snap_id"`
	Objects     []harness.LayoutObject   `json:"objects"`
	SnapObjects []harness.SnapshotObject `json:"snapshot_objects"`
}

// cephFSLayoutScript writes the file through libcephfs. Arguments:
// filesystem, pool namespace and name=offset for each payload in
// /tmp/layout-<name>. The empty file gets 1 MiB stripe units over two 4 MiB
// objects in the namespace; a and b are written, the directory is
// snapshotted as base, then c is written. It reads all bytes back through
// the head and the snapshot path, then prints the inode, the layout xattr
// and the snapshot id.
const cephFSLayoutScript = `import cephfs,json,os,sys
filesystem,namespace=sys.argv[1],sys.argv[2]
offsets={k:int(v) for k,v in (a.split('=') for a in sys.argv[3:])}
def payload(name): return open('/tmp/layout-'+name,'rb').read()
def write(path,names):
    fd=fs.open(path,os.O_WRONLY,0)
    try:
        for name in names:
            assert fs.write(fd,payload(name),offsets[name])==len(payload(name)),name
        fs.fsync(fd,False)
    finally:
        fs.close(fd)
def image(names):
    size=max(offsets[n]+len(payload(n)) for n in names)
    data=bytearray(size)
    for n in names:
        data[offsets[n]:offsets[n]+len(payload(n))]=payload(n)
    return bytes(data)
def read(path,size):
    fd=fs.open(path,os.O_RDONLY,0)
    try:
        data=b''
        while len(data)<size+1:
            chunk=fs.read(fd,len(data),size+1-len(data))
            if not chunk: break
            data+=chunk
        return data
    finally:
        fs.close(fd)
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','30')
fs.mount(filesystem_name=filesystem.encode())
try:
    fs.mkdir('/tc-layout',0o755)
    path='/tc-layout/file'
    fs.close(fs.open(path,os.O_CREAT|os.O_WRONLY,0o644))
    for key,value in (('stripe_unit',1<<20),('stripe_count',2),('object_size',4<<20)):
        fs.setxattr(path,'ceph.file.layout.'+key,str(value).encode(),0)
    fs.setxattr(path,'ceph.file.layout.pool_namespace',namespace.encode(),0)
    write(path,('a','b'))
    fs.mkdir('/tc-layout/.snap/base',0o755)
    snap=fs.snap_info('/tc-layout/.snap/base')['id']
    write(path,('c',))
    before,after=image(('a','b')),image(('a','b','c'))
    assert read(path,len(after))==after,'head bytes'
    assert read('/tc-layout/.snap/base/file',len(before))==before,'snapshot bytes'
    fs.sync_fs()
    print('RESULT '+json.dumps({'inode':fs.stat(path).st_ino,'snap_id':snap,
        'layout':fs.getxattr(path,'ceph.file.layout',1024).decode()},sort_keys=True))
finally:
    fs.shutdown()
`

// TestCephFSLayoutReceipt ties a CephFS file to the RADOS objects that hold
// it. libcephfs gives an empty file 1 MiB stripe units over two 4 MiB
// objects in a pool namespace, writes it, snapshots its directory and
// overwrites part of its first object. Then, with clients of their own:
//
//   - the ceph.file.layout xattr names the pool, namespace and striping;
//   - rados lists and reads the data objects "<ino hex>.<objno %08x>",
//     whose names and bytes must be what the striping formula gives for the
//     writes;
//   - librados reads the overwritten object at the snapshot id that
//     libcephfs reports, which must hold the bytes from before the
//     overwrite, in the clone that rados listsnaps reports for it;
//   - ceph osd map gives each object's PG and the OSD map epoch.
func TestCephFSLayoutReceipt(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t)
	fs, err := cephfs.Start(ctx, cluster, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	assertCephFSActive(t, ctx, cluster, fs.FilesystemName)
	const MiB, namespace = 1 << 20, "tc-layout"
	writes := []harness.LayoutWrite{
		harness.NewLayoutWrite("a", 1, 0, 8*MiB),             // the first object set, objects 0 and 1
		harness.NewLayoutWrite("b", 2, 8*MiB+MiB/2, 3*MiB/2), // objects 2 and 3 from inside a stripe unit
		harness.NewLayoutWrite("c", 3, 128<<10, 8<<10),       // after the snapshot, object 0
	}
	harness.CopyLayoutWrites(t, ctx, client, writes...)
	args := []string{"python3", "-c", cephFSLayoutScript, fs.FilesystemName, namespace}
	for _, w := range writes {
		args = append(args, w.Name+"="+strconv.FormatUint(w.Offset, 10))
	}
	var written struct {
		Inode  uint64 `json:"inode"`
		SnapID uint64 `json:"snap_id"`
		Layout string `json:"layout"`
	}
	harness.LayoutResult(t, harness.ExecOutput(t, ctx, client, args...), &written)
	receipt := cephFSLayoutReceipt{Filesystem: fs.FilesystemName, Path: "/tc-layout/file", Inode: written.Inode,
		LayoutXattr: written.Layout, Writes: writes, Snapshot: "base", SnapID: written.SnapID}
	if receipt.Inode == 0 || receipt.SnapID == 0 {
		t.Fatalf("libcephfs reported inode %d and snapshot %d", receipt.Inode, receipt.SnapID)
	}
	if receipt.Layout, receipt.Pool, receipt.Namespace, err = harness.ParseCephFSLayoutXattr(written.Layout); err != nil {
		t.Fatal(err)
	}
	if receipt.Layout != (harness.StripedLayout{StripeUnit: MiB, StripeCount: 2, ObjectSize: 4 * MiB}) || receipt.Pool != fs.DataPool || receipt.Namespace != namespace {
		t.Fatalf("layout %q, want 1 MiB units over 2 objects of 4 MiB in %s namespace %s", written.Layout, fs.DataPool, namespace)
	}

	head, snapshot := map[uint64][]byte{}, map[uint64][]byte{}
	for _, w := range writes[:2] {
		receipt.Layout.Apply(head, w.Offset, w.Data)
		receipt.Layout.Apply(snapshot, w.Offset, w.Data)
	}
	overwritten := receipt.Layout.Apply(head, writes[2].Offset, writes[2].Data)
	prefix := strconv.FormatUint(receipt.Inode, 16) + "."
	var names, want []string
	control := harness.ToolkitContainer(t, cluster)
	for _, name := range harness.RADOSListing(t, ctx, control, receipt.Pool, namespace) {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	objnos := slices.Sorted(maps.Keys(head))
	for _, objno := range objnos {
		want = append(want, fmt.Sprintf("%s%08x", prefix, objno))
	}
	if !slices.Equal(names, want) {
		t.Fatalf("data objects %v, want %v", names, want)
	}
	for i, objno := range objnos {
		object := harness.ReadLayoutObject(t, ctx, cluster, control, receipt.Pool, namespace, want[i], objno, head[objno])
		receipt.Objects = append(receipt.Objects, object)
		_, cloned := harness.CloneHolding(object.Clones, receipt.SnapID)
		if cloned != slices.Contains(overwritten, objno) {
			t.Fatalf("object %s has clones %+v; only the objects overwritten after snapshot %d should", object.Name, object.Clones, receipt.SnapID)
		}
		if cloned {
			receipt.SnapObjects = append(receipt.SnapObjects,
				harness.ReadSnapshotObject(t, ctx, control, receipt.Pool, namespace, receipt.SnapID, object, snapshot[objno]))
		}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_LAYOUT_RECEIPT %s", encoded)
}
