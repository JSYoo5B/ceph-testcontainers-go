//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"slices"
	"testing"
)

func TestStripedLayoutApply(t *testing.T) {
	const MiB = 1 << 20
	layout := stripedLayout{StripeUnit: MiB, StripeCount: 2, ObjectSize: 4 * MiB}
	objects := map[uint64][]byte{}
	// The first object set: units alternate between objects 0 and 1.
	a := layoutPayload(1, 8*MiB)
	if touched := layout.apply(objects, 0, a); !slices.Equal(touched, []uint64{0, 1}) {
		t.Fatalf("touched %v", touched)
	}
	for unit := range uint64(8) {
		object, start := objects[unit%2], unit/2*MiB
		if !bytes.Equal(object[start:start+MiB], a[unit*MiB:(unit+1)*MiB]) {
			t.Fatalf("unit %d is not at object %d offset %d", unit, unit%2, start)
		}
	}
	// From inside a unit of the second set: object 2 gets a hole below it.
	b := layoutPayload(2, 3*MiB/2)
	if touched := layout.apply(objects, 8*MiB+MiB/2, b); !slices.Equal(touched, []uint64{2, 3}) {
		t.Fatalf("touched %v", touched)
	}
	if len(objects[2]) != MiB || !bytes.Equal(objects[2][:MiB/2], make([]byte, MiB/2)) || !bytes.Equal(objects[2][MiB/2:], b[:MiB/2]) {
		t.Fatal("object 2")
	}
	if len(objects[3]) != MiB || !bytes.Equal(objects[3], b[MiB/2:]) {
		t.Fatal("object 3")
	}
	if bytes.IndexByte(a, 0) >= 0 || bytes.Equal(a[:MiB], a[MiB:2*MiB]) {
		t.Fatal("the payload has a zero byte or repeats at a unit boundary")
	}
	if (stripedLayout{StripeUnit: 3, StripeCount: 1, ObjectSize: 4}).valid() == nil {
		t.Fatal("an object size that is not a multiple of the unit was accepted")
	}
}

func TestParseLayoutReceiptOutputs(t *testing.T) {
	// rados listsnaps on quay.io/ceph/ceph after one snapshot and an overwrite.
	clones, err := parseRadosListSnaps("rbd_data.1063b8f7e5a1.0000000000000000:\ncloneid\tsnaps\tsize\toverlap\n4\t4\t4194304\t[0~131072,139264~4055040]\nhead\t-\t4194304\n")
	if err != nil || len(clones) != 1 || clones[0].ID != 4 || !slices.Equal(clones[0].Snaps, []uint64{4}) || clones[0].Size != 4194304 {
		t.Fatalf("%+v %v", clones, err)
	}
	if _, ok := cloneHolding(clones, 4); !ok {
		t.Fatal("snapshot 4 has no clone")
	}
	if clones, err := parseRadosListSnaps("x:\ncloneid\tsnaps\tsize\toverlap\nhead\t-\t10\n"); err != nil || len(clones) != 0 {
		t.Fatalf("%+v %v", clones, err)
	}
	if _, err := parseRadosListSnaps("x:\ncloneid\tsnaps\tsize\toverlap\n"); err == nil {
		t.Fatal("a listing without head was accepted")
	}
	layout, pool, namespace, err := parseCephFSLayoutXattr("stripe_unit=1048576 stripe_count=2 object_size=4194304 pool=cephfs.tc.data pool_namespace=tc-layout")
	if err != nil || layout != (stripedLayout{1 << 20, 2, 4 << 20}) || pool != "cephfs.tc.data" || namespace != "tc-layout" {
		t.Fatalf("%+v %q %q %v", layout, pool, namespace, err)
	}
	if _, _, _, err := parseCephFSLayoutXattr("stripe_unit=1048576 stripe_count=2 object_size=4194304"); err == nil {
		t.Fatal("a layout without a pool was accepted")
	}
}
