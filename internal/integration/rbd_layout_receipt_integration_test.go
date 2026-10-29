//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_layout_receipts))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// rbdLayoutImage is one image as librbd reports it and as its header and id
// objects hold it, decoded from the omap values that rados reads.
type rbdLayoutImage struct {
	Name            string   `json:"name"`
	ID              string   `json:"id"`
	BlockNamePrefix string   `json:"block_name_prefix"`
	Order           int      `json:"order"`
	Size            uint64   `json:"size"`
	StripeUnit      uint64   `json:"stripe_unit"`
	StripeCount     uint64   `json:"stripe_count"`
	Features        uint64   `json:"features"`
	SnapSeq         uint64   `json:"snap_seq"`
	SnapIDs         []uint64 `json:"snap_ids"`
	// Parent is the clone's parent spec from librbd; ParentOmap is the
	// header's encoded "parent" value.
	Parent     *rbdParentSpec `json:"parent,omitempty"`
	ParentOmap string         `json:"parent_omap_hex,omitempty"`
}

type rbdParentSpec struct {
	PoolID    int64  `json:"pool_id"`
	Namespace string `json:"pool_namespace"`
	ImageID   string `json:"image_id"`
	SnapID    uint64 `json:"snap_id"`
	Overlap   uint64 `json:"overlap,omitempty"`
}

// rbdLayoutReceipt is what TestRBDLayoutReceipt logs.
type rbdLayoutReceipt struct {
	Pool        string           `json:"pool"`
	PoolID      int64            `json:"pool_id"`
	Namespace   string           `json:"namespace"`
	Writes      []layoutWrite    `json:"writes"`
	Parent      rbdLayoutImage   `json:"parent"`
	Clone       rbdLayoutImage   `json:"clone"`
	Snapshot    string           `json:"snapshot"`
	SnapID      uint64           `json:"snap_id"`
	ParentData  []layoutObject   `json:"parent_objects"`
	CloneData   []layoutObject   `json:"clone_objects"`
	SnapObjects []snapshotObject `json:"snapshot_objects"`
}

// rbdLayoutWriteScript creates the images with librbd. Arguments: pool,
// namespace and name=offset for each payload in /tmp/layout-<name>; a and b
// go to the parent, then the snapshot base, then c to the parent's head and
// the clone of base, then d to the clone.
const rbdLayoutWriteScript = `import json,sys,rados,rbd
pool,namespace=sys.argv[1],sys.argv[2]
offsets={k:int(v) for k,v in (a.split('=') for a in sys.argv[3:])}
def payload(name): return open('/tmp/layout-'+name,'rb').read()
MiB=1<<20
FEATURES=rbd.RBD_FEATURE_LAYERING|rbd.RBD_FEATURE_STRIPINGV2
def identity(image):
    st=image.stat()
    return {'id':image.id(),'block_name_prefix':st['block_name_prefix'],'order':st['order'],'size':st['size'],
        'stripe_unit':image.stripe_unit(),'stripe_count':image.stripe_count(),'features':image.features(),
        'snaps':{s['name']:s['id'] for s in image.list_snaps()}}
c=rados.Rados(conffile='/etc/ceph/ceph.conf')
c.connect()
try:
    io=c.open_ioctx(pool)
    try:
        io.set_namespace(namespace)
        r=rbd.RBD()
        r.create(io,'parent',16*MiB,order=22,old_format=False,features=FEATURES,stripe_unit=MiB,stripe_count=2)
        with rbd.Image(io,'parent') as image:
            for name in ('a','b'):
                assert image.write(payload(name),offsets[name])==len(payload(name))
            image.flush()
            image.create_snap('base')
            image.protect_snap('base')
            image.write(payload('c'),offsets['c'])
            image.flush()
            parent=identity(image)
        r.clone(io,'parent','base',io,'clone',features=FEATURES,order=22,stripe_unit=MiB,stripe_count=2)
        with rbd.Image(io,'clone') as image:
            image.write(payload('d'),offsets['d'])
            image.flush()
            clone=identity(image)
            spec=image.get_parent_image_spec()
            clone['parent']={'pool_id':c.pool_lookup(spec['pool_name']),'pool_namespace':spec['pool_namespace'],
                'image_id':spec['image_id'],'snap_id':spec['snap_id']}
        print('RESULT '+json.dumps({'pool_id':c.pool_lookup(pool),'parent':parent,'clone':clone},sort_keys=True))
    finally:
        io.close()
finally:
    c.shutdown()
`

// rbdHeaderScript reads image ids and headers through librados alone and
// decodes the cls_rbd omap values. Arguments: pool, namespace, image names.
const rbdHeaderScript = `import json,struct,sys,rados
pool,namespace=sys.argv[1],sys.argv[2]
def u64(v): return struct.unpack('<Q',v)[0]
def string(v,o=0):
    n=struct.unpack_from('<I',v,o)[0]
    return v[o+4:o+4+n].decode(),o+4+n
def parent(v):
    version,compat,length=struct.unpack_from('<BBI',v,0)
    o=6
    pool_id=struct.unpack_from('<q',v,o)[0]; o+=8
    ns=''
    if version>=2: ns,o=string(v,o)
    image_id,o=string(v,o)
    snap_id=struct.unpack_from('<Q',v,o)[0]; o+=8
    if version>=2:
        present=v[o]; o+=1
        overlap=struct.unpack_from('<Q',v,o)[0] if present else 0
    else:
        overlap=struct.unpack_from('<Q',v,o)[0]
    return {'pool_id':pool_id,'pool_namespace':ns,'image_id':image_id,'snap_id':snap_id,'overlap':overlap,'version':version}
c=rados.Rados(conffile='/etc/ceph/ceph.conf')
c.connect()
try:
    io=c.open_ioctx(pool)
    try:
        io.set_namespace(namespace)
        out={}
        for name in sys.argv[3:]:
            image_id,_=string(io.read('rbd_id.'+name,4096,0))
            with rados.ReadOpCtx() as op:
                it,ret=io.get_omap_vals(op,'','',4096)
                io.operate_read_op(op,'rbd_header.'+image_id)
                omap=dict(it)
            h={'id':image_id,'block_name_prefix':string(omap['object_prefix'])[0],'order':omap['order'][0],
               'size':u64(omap['size']),'features':u64(omap['features']),'snap_seq':u64(omap['snap_seq']),
               'stripe_unit':u64(omap['stripe_unit']) if 'stripe_unit' in omap else 0,
               'stripe_count':u64(omap['stripe_count']) if 'stripe_count' in omap else 0,
               'snap_ids':sorted(int(k[len('snapshot_'):],16) for k in omap if k.startswith('snapshot_'))}
            if 'parent' in omap:
                h['parent']=parent(omap['parent'])
                h['parent_omap_hex']=omap['parent'].hex()
            out[name]=h
        print('RESULT '+json.dumps(out,sort_keys=True))
    finally:
        io.close()
finally:
    c.shutdown()
`

// TestRBDLayoutReceipt ties a layered image to the RADOS objects that hold
// it. The parent and its clone use 4 MiB objects striped in 1 MiB units over
// two objects, in an RBD namespace of the rbd package's pool. librbd writes
// the parent, snapshots it, overwrites part of its first object and writes
// one block of the clone. Then, with clients of their own:
//
//   - librados reads each image's id object and header and decodes the
//     object prefix, order, striping, features, snapshot ids, snap_seq and
//     the clone's parent spec, which must match what librbd reported;
//   - rados lists and reads every data object rbd_data.<id>.<objno>, whose
//     names and bytes must be what the striping formula gives for the
//     writes: the clone holds only the object it wrote, copied up from the
//     snapshot;
//   - librados reads the parent's overwritten object at the snapshot, which
//     must hold the bytes from before the overwrite, in the clone that
//     rados listsnaps reports for the snapshot;
//   - ceph osd map gives each object's PG and the OSD map epoch.
func TestRBDLayoutReceipt(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	cluster, client := newServiceClusterRun(t, rbdRun, image, append(roles, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)))...)
	const pool, namespace = "rbd", "tc-layout"
	if _, err := rbd.CreateNamespace(ctx, cluster, pool, namespace); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	const MiB = 1 << 20
	writes := []layoutWrite{
		newLayoutWrite("a", 1, 0, 8*MiB),             // the first object set, objects 0 and 1
		newLayoutWrite("b", 2, 8*MiB+MiB/2, 3*MiB/2), // objects 2 and 3 from inside a stripe unit
		newLayoutWrite("c", 3, 128<<10, 8<<10),       // the parent's head after the snapshot, object 0
		newLayoutWrite("d", 4, 3*MiB+4<<10, 4<<10),   // the clone, object 1
	}
	copyLayoutWrites(t, ctx, client, writes...)
	args := []string{"python3", "-c", rbdLayoutWriteScript, pool, namespace}
	for _, w := range writes {
		args = append(args, w.Name+"="+strconv.FormatUint(w.Offset, 10))
	}
	var written struct {
		PoolID int64 `json:"pool_id"`
		Parent struct {
			rbdLayoutImage
			Snaps map[string]uint64 `json:"snaps"`
		} `json:"parent"`
		Clone rbdLayoutImage `json:"clone"`
	}
	layoutResult(t, execOutput(t, ctx, client, args...), &written)
	receipt := rbdLayoutReceipt{Pool: pool, PoolID: written.PoolID, Namespace: namespace, Writes: writes, Snapshot: "base"}
	snap, ok := written.Parent.Snaps["base"]
	if !ok {
		t.Fatalf("librbd lists no snapshot base: %+v", written.Parent.Snaps)
	}
	receipt.SnapID = snap

	// The headers, read through librados alone.
	control := toolkitContainer(t, cluster)
	var headers map[string]rbdLayoutImage
	layoutResult(t, execOutput(t, ctx, control, "python3", "-c", rbdHeaderScript, pool, namespace, "parent", "clone"), &headers)
	for _, want := range []struct {
		name    string
		librbd  rbdLayoutImage
		snapIDs []uint64
		into    *rbdLayoutImage
	}{
		{"parent", written.Parent.rbdLayoutImage, []uint64{snap}, &receipt.Parent},
		{"clone", written.Clone, nil, &receipt.Clone},
	} {
		header, ok := headers[want.name]
		if !ok {
			t.Fatalf("no header read for %s: %+v", want.name, headers)
		}
		header.Name = want.name
		if err := compareRBDHeader(header, want.librbd, want.snapIDs); err != nil {
			t.Fatalf("%s: %v", want.name, err)
		}
		*want.into = header
	}
	parentSpec := rbdParentSpec{PoolID: written.PoolID, Namespace: namespace, ImageID: receipt.Parent.ID, SnapID: snap}
	if p := receipt.Clone.Parent; p == nil || p.PoolID != parentSpec.PoolID || p.Namespace != parentSpec.Namespace ||
		p.ImageID != parentSpec.ImageID || p.SnapID != parentSpec.SnapID || p.Overlap != receipt.Parent.Size {
		t.Fatalf("clone header parent %+v, want %+v with overlap %d", receipt.Clone.Parent, parentSpec, receipt.Parent.Size)
	}
	if p := written.Clone.Parent; p == nil || p.PoolID != parentSpec.PoolID || p.Namespace != parentSpec.Namespace ||
		p.ImageID != parentSpec.ImageID || p.SnapID != parentSpec.SnapID {
		t.Fatalf("librbd parent spec %+v, want %+v", written.Clone.Parent, parentSpec)
	}
	if receipt.Parent.SnapSeq != snap {
		t.Fatalf("parent snap_seq %d, want its only snapshot %d", receipt.Parent.SnapSeq, snap)
	}

	// The data objects the striping formula names, and no others.
	layout := stripedLayout{StripeUnit: receipt.Parent.StripeUnit, StripeCount: receipt.Parent.StripeCount, ObjectSize: 1 << receipt.Parent.Order}
	if err := layout.valid(); err != nil || layout != (stripedLayout{MiB, 2, 4 * MiB}) {
		t.Fatalf("parent layout %+v %v, want 1 MiB units over 2 objects of 4 MiB", layout, err)
	}
	snapshot, head, clone := map[uint64][]byte{}, map[uint64][]byte{}, map[uint64][]byte{}
	for _, w := range writes[:2] {
		layout.apply(snapshot, w.Offset, w.data)
		layout.apply(head, w.Offset, w.data)
	}
	overwritten := layout.apply(head, writes[2].Offset, writes[2].data)
	// A write to the clone copies the whole object up from the snapshot.
	for _, objno := range layout.apply(map[uint64][]byte{}, writes[3].Offset, writes[3].data) {
		clone[objno] = slices.Clone(snapshot[objno])
	}
	layout.apply(clone, writes[3].Offset, writes[3].data)
	listing := radosListing(t, ctx, control, pool, namespace)
	for _, image := range []struct {
		header  rbdLayoutImage
		objects map[uint64][]byte
		into    *[]layoutObject
	}{
		{receipt.Parent, head, &receipt.ParentData},
		{receipt.Clone, clone, &receipt.CloneData},
	} {
		prefix := image.header.BlockNamePrefix + "."
		var names, want []string
		for _, name := range listing {
			if len(name) > len(prefix) && name[:len(prefix)] == prefix {
				names = append(names, name)
			}
		}
		for _, objno := range slices.Sorted(maps.Keys(image.objects)) {
			want = append(want, fmt.Sprintf("%s%016x", prefix, objno))
		}
		if !slices.Equal(names, want) {
			t.Fatalf("%s data objects %v, want %v", image.header.Name, names, want)
		}
		for i, objno := range slices.Sorted(maps.Keys(image.objects)) {
			*image.into = append(*image.into, readLayoutObject(t, ctx, cluster, control, pool, namespace, want[i], objno, image.objects[objno]))
		}
	}
	for _, object := range receipt.ParentData {
		_, cloned := cloneHolding(object.Clones, snap)
		if cloned != slices.Contains(overwritten, object.Number) {
			t.Fatalf("parent object %s has clones %+v; only the objects overwritten after snapshot %d should", object.Name, object.Clones, snap)
		}
		if cloned {
			receipt.SnapObjects = append(receipt.SnapObjects,
				readSnapshotObject(t, ctx, control, pool, namespace, snap, object, snapshot[object.Number]))
		}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RBD_LAYOUT_RECEIPT %s", encoded)
}

// compareRBDHeader compares a header that librados decoded with what librbd
// reported for the same image.
func compareRBDHeader(header, librbd rbdLayoutImage, snapIDs []uint64) error {
	switch {
	case header.ID == "" || header.ID != librbd.ID:
		return fmt.Errorf("id object holds %q, librbd reports %q", header.ID, librbd.ID)
	case header.BlockNamePrefix != librbd.BlockNamePrefix || header.BlockNamePrefix != "rbd_data."+header.ID:
		return fmt.Errorf("object_prefix %q, librbd block_name_prefix %q, id %q", header.BlockNamePrefix, librbd.BlockNamePrefix, header.ID)
	case header.Order != librbd.Order || header.Size != librbd.Size || header.Features != librbd.Features:
		return fmt.Errorf("header order/size/features %d/%d/%#x, librbd %d/%d/%#x",
			header.Order, header.Size, header.Features, librbd.Order, librbd.Size, librbd.Features)
	case header.StripeUnit != librbd.StripeUnit || header.StripeCount != librbd.StripeCount:
		return fmt.Errorf("header striping %d/%d, librbd %d/%d", header.StripeUnit, header.StripeCount, librbd.StripeUnit, librbd.StripeCount)
	case !slices.Equal(header.SnapIDs, snapIDs):
		return fmt.Errorf("header snapshots %v, want %v", header.SnapIDs, snapIDs)
	}
	return nil
}
