//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// stripedLayout is how RBD images and CephFS files map their bytes to RADOS
// objects: stripe units go round-robin over StripeCount objects, and once
// those objects hold ObjectSize bytes each the next object set begins.
type stripedLayout struct {
	StripeUnit  uint64 `json:"stripe_unit"`
	StripeCount uint64 `json:"stripe_count"`
	ObjectSize  uint64 `json:"object_size"`
}

func (l stripedLayout) valid() error {
	if l.StripeUnit == 0 || l.StripeCount == 0 || l.ObjectSize == 0 || l.ObjectSize%l.StripeUnit != 0 {
		return fmt.Errorf("invalid layout %+v", l)
	}
	return nil
}

// apply writes data at offset into objects, the expected contents of each
// object by number, and returns the numbers of the objects it touched. A
// part of an object below a write that nothing wrote reads as zeros.
func (l stripedLayout) apply(objects map[uint64][]byte, offset uint64, data []byte) []uint64 {
	perObject := l.ObjectSize / l.StripeUnit
	touched := map[uint64]bool{}
	for len(data) > 0 {
		block, within := offset/l.StripeUnit, offset%l.StripeUnit
		n := min(l.StripeUnit-within, uint64(len(data)))
		stripe, position := block/l.StripeCount, block%l.StripeCount
		objno := stripe/perObject*l.StripeCount + position
		start := stripe%perObject*l.StripeUnit + within
		object := objects[objno]
		if end := start + n; uint64(len(object)) < end {
			object = append(object, make([]byte, end-uint64(len(object)))...)
		}
		copy(object[start:], data[:n])
		objects[objno] = object
		touched[objno] = true
		offset += n
		data = data[n:]
	}
	return slices.Sorted(maps.Keys(touched))
}

// layoutPayload is n bytes that never repeat at a stripe unit or object
// boundary and contain no zero byte, so a hole cannot pass for data.
func layoutPayload(tag byte, n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((i*31+int(tag)*17+i/4093)%251 + 1)
	}
	return data
}

// layoutWrite is one write of a receipt, at a fixed offset.
type layoutWrite struct {
	Name   string `json:"name"`
	Offset uint64 `json:"offset"`
	Length int    `json:"length"`
	data   []byte
}

func newLayoutWrite(name string, tag byte, offset uint64, length int) layoutWrite {
	return layoutWrite{Name: name, Offset: offset, Length: length, data: layoutPayload(tag, length)}
}

// copyLayoutWrites puts each write's bytes in /tmp/layout-<name> of client.
func copyLayoutWrites(t *testing.T, ctx context.Context, client testcontainers.Container, writes ...layoutWrite) {
	t.Helper()
	for _, w := range writes {
		if err := client.CopyToContainer(ctx, w.data, "/tmp/layout-"+w.Name, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// layoutObject is one RADOS object of a receipt, read back with rados.
type layoutObject struct {
	Name   string `json:"name"`
	Number uint64 `json:"objno"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
	Epoch  int64  `json:"osdmap_epoch"`
	PG     string `json:"pg"`
	RawPG  string `json:"raw_pg"`
	Up     []int  `json:"up"`
	Acting []int  `json:"acting"`
	// Clones are the snapshot clones that rados listsnaps reports.
	Clones []radosClone `json:"clones,omitempty"`
}

// radosClone is one line of rados listsnaps.
type radosClone struct {
	ID    uint64   `json:"id"`
	Snaps []uint64 `json:"snaps"`
	Size  int64    `json:"size"`
}

func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// radosListing lists the objects of pool and namespace with the rados tool.
func radosListing(t *testing.T, ctx context.Context, ctr testcontainers.Container, pool, namespace string) []string {
	t.Helper()
	var names []string
	for _, line := range strings.Split(execOutput(t, ctx, ctr, "rados", "-p", pool, "-N", namespace, "ls"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names = append(names, line)
		}
	}
	slices.Sort(names)
	return names
}

// readLayoutObject reads one object with the rados tool, compares its bytes
// with want and records where the OSD map places it and its snapshot clones.
func readLayoutObject(t *testing.T, ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container,
	pool, namespace, name string, objno uint64, want []byte) layoutObject {
	t.Helper()
	out := strings.Fields(execOutput(t, ctx, ctr, "sh", "-c",
		`rados -p "$1" -N "$2" get "$3" /tmp/layout-object && sha256sum /tmp/layout-object && stat -c %s /tmp/layout-object && rm -f /tmp/layout-object`,
		"sh", pool, namespace, name))
	if len(out) != 3 {
		t.Fatalf("rados get %s: %q", name, out)
	}
	size, err := strconv.Atoi(out[2])
	if err != nil {
		t.Fatal(err)
	}
	object := layoutObject{Name: name, Number: objno, Size: size, SHA256: out[0]}
	if size != len(want) || object.SHA256 != digestHex(want) {
		t.Fatalf("object %s holds %d bytes with sha256 %s, want %d bytes with %s", name, size, object.SHA256, len(want), digestHex(want))
	}
	if object.Clones, err = parseRadosListSnaps(execOutput(t, ctx, ctr, "rados", "-p", pool, "-N", namespace, "listsnaps", name)); err != nil {
		t.Fatalf("listsnaps %s: %v", name, err)
	}
	args := []string{"osd", "map", pool, name}
	if namespace != "" {
		args = append(args, namespace)
	}
	data, err := cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		t.Fatal(err)
	}
	var placement struct {
		Epoch  int64  `json:"epoch"`
		Object string `json:"objname"`
		RawPG  string `json:"raw_pgid"`
		PG     string `json:"pgid"`
		Up     []int  `json:"up"`
		Acting []int  `json:"acting"`
	}
	// The reply names a namespaced object as "<namespace>/<name>".
	located := name
	if namespace != "" {
		located = namespace + "/" + name
	}
	if err := json.Unmarshal(data, &placement); err != nil || placement.Epoch <= 0 || placement.Object != located || placement.PG == "" || len(placement.Acting) == 0 {
		t.Fatalf("osd map %s: %+v %v: %s", name, placement, err, data)
	}
	object.Epoch, object.PG, object.RawPG, object.Up, object.Acting = placement.Epoch, placement.PG, placement.RawPG, placement.Up, placement.Acting
	return object
}

// parseRadosListSnaps reads the clone lines of rados listsnaps; the head
// line has no snapshots and is left out.
//
//	rbd_data.1234.0000000000000000:
//	cloneid	snaps	size	overlap
//	4	4	4194304	[0~131072,139264~4055040]
//	head	-	4194304
func parseRadosListSnaps(out string) ([]radosClone, error) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 || !strings.HasPrefix(strings.TrimSpace(lines[1]), "cloneid") {
		return nil, fmt.Errorf("unexpected listsnaps output %q", out)
	}
	var clones []radosClone
	head := false
	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("listsnaps line %q", line)
		}
		if fields[0] == "head" {
			head = true
			continue
		}
		id, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("listsnaps line %q: %w", line, err)
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("listsnaps line %q: %w", line, err)
		}
		clone := radosClone{ID: id, Size: size}
		for _, s := range strings.Split(fields[1], ",") {
			snap, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("listsnaps line %q: %w", line, err)
			}
			clone.Snaps = append(clone.Snaps, snap)
		}
		clones = append(clones, clone)
	}
	if !head {
		return nil, fmt.Errorf("listsnaps reports no head: %q", out)
	}
	return clones, nil
}

// cloneHolding returns the clone that preserves snap.
func cloneHolding(clones []radosClone, snap uint64) (radosClone, bool) {
	for _, clone := range clones {
		if slices.Contains(clone.Snaps, snap) {
			return clone, true
		}
	}
	return radosClone{}, false
}

// layoutResult returns the JSON that a receipt script printed after RESULT,
// so that warnings on the other stream cannot corrupt it.
func layoutResult(t *testing.T, out string, into any) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), into); err != nil {
				t.Fatalf("receipt script result: %v: %s", err, rest)
			}
			return
		}
	}
	t.Fatalf("receipt script printed no result: %s", out)
}

// snapshotReadScript reads objects at a self-managed snapshot through
// librados, a client of its own. Arguments: pool, namespace, snapshot id and
// the object names; it prints each object's size and sha256 at the snapshot.
const snapshotReadScript = `import hashlib,json,sys,rados
pool,namespace,snap=sys.argv[1],sys.argv[2],int(sys.argv[3])
c=rados.Rados(conffile='/etc/ceph/ceph.conf')
c.connect()
try:
    io=c.open_ioctx(pool)
    try:
        io.set_namespace(namespace)
        io.set_read(snap)
        out={}
        for name in sys.argv[4:]:
            data=io.read(name,64<<20,0)
            out[name]={'size':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        io.set_read(rados.LIBRADOS_SNAP_HEAD)
        print('RESULT '+json.dumps(out,sort_keys=True))
    finally:
        io.close()
finally:
    c.shutdown()
`

// snapshotObject is an object's bytes at a snapshot.
type snapshotObject struct {
	Name   string `json:"name"`
	Snap   uint64 `json:"snap_id"`
	Clone  uint64 `json:"clone_id"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// readSnapshotObject reads name at snap with librados, compares it with want
// and finds the clone that listsnaps reports for the snapshot.
func readSnapshotObject(t *testing.T, ctx context.Context, ctr testcontainers.Container, pool, namespace string, snap uint64, object layoutObject, want []byte) snapshotObject {
	t.Helper()
	var read map[string]struct {
		Size   int    `json:"size"`
		SHA256 string `json:"sha256"`
	}
	layoutResult(t, execOutput(t, ctx, ctr, "python3", "-c", snapshotReadScript, pool, namespace, strconv.FormatUint(snap, 10), object.Name), &read)
	got, ok := read[object.Name]
	if !ok || got.Size != len(want) || got.SHA256 != digestHex(want) {
		t.Fatalf("%s at snapshot %d: %+v, want %d bytes with sha256 %s", object.Name, snap, got, len(want), digestHex(want))
	}
	clone, ok := cloneHolding(object.Clones, snap)
	if !ok {
		t.Fatalf("listsnaps of %s has no clone for snapshot %d: %+v", object.Name, snap, object.Clones)
	}
	return snapshotObject{Name: object.Name, Snap: snap, Clone: clone.ID, Size: got.Size, SHA256: got.SHA256}
}

// parseCephFSLayoutXattr reads a ceph.file.layout value:
// "stripe_unit=1048576 stripe_count=2 object_size=4194304 pool=data
// pool_namespace=ns", the namespace left out when there is none.
func parseCephFSLayoutXattr(value string) (stripedLayout, string, string, error) {
	fields := map[string]string{}
	for _, field := range strings.Fields(value) {
		key, v, ok := strings.Cut(field, "=")
		if !ok {
			return stripedLayout{}, "", "", fmt.Errorf("layout field %q in %q", field, value)
		}
		fields[key] = v
	}
	var layout stripedLayout
	for key, into := range map[string]*uint64{"stripe_unit": &layout.StripeUnit, "stripe_count": &layout.StripeCount, "object_size": &layout.ObjectSize} {
		n, err := strconv.ParseUint(fields[key], 10, 64)
		if err != nil {
			return stripedLayout{}, "", "", fmt.Errorf("layout %s in %q: %w", key, value, err)
		}
		*into = n
	}
	if err := layout.valid(); err != nil {
		return stripedLayout{}, "", "", err
	}
	if fields["pool"] == "" {
		return stripedLayout{}, "", "", fmt.Errorf("layout %q names no pool", value)
	}
	return layout, fields["pool"], fields["pool_namespace"], nil
}
