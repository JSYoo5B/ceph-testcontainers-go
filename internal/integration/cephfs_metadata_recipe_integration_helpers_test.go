//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// cephFSMetadataRecipeActor is an actor that runs a fixed sequence of
// metadata operations through one libcephfs mount and records, per
// operation, the native errno (0 on success) and what the binding returned:
// inode numbers, modes, link counts, owners, sizes and times (whole seconds,
// as the binding's stat reports them), bytes and names. Receipt 1 holds the
// client ID and the operations that create the root and fill the "many"
// directory with count empty files. At the "readdir" gate it lists "many"
// with opendir and readdir and publishes receipt 2 with every name it got.
// At the "recipe" gate it runs the rest, a file's data, setattr, links,
// rename, symlink, xattrs, expected errors and flock, and publishes receipt
// 3. It unmounts and shuts down after "finish" and publishes receipt 4.
// Arguments after the common ones: the root, count, the hex of the file's
// bytes, of the rename source's bytes, of the victim's bytes and of the
// xattr value.
const cephFSMetadataRecipeActor cephFSActorScript = cephFSActorHelpers + `
import fcntl
root = sys.argv[4].encode()
count = int(sys.argv[5])
data, source_data, victim_data, value = (bytes.fromhex(arg) for arg in sys.argv[6:10])
ops = []
def child(name):
    return root + b"/" + name
def record(label, path, call, *args):
    entry = {"op": label, "path": path.decode(), "errno": 0}
    ops.append(entry)
    try:
        return entry, call(*args)
    except cephfs.Error as error:
        entry["errno"] = getattr(error, "errno", None) or -1
        return entry, None
def described(entry, st):
    entry.update(inode=st.st_ino, mode=st.st_mode, nlink=st.st_nlink, uid=st.st_uid, gid=st.st_gid,
                 size=st.st_size, atime=int(st.st_atime.timestamp()), mtime=int(st.st_mtime.timestamp()))
def stat(label, path, call=None):
    entry, st = record(label, path, call or fs.stat, path)
    if st is not None:
        described(entry, st)
    return entry
def create(label, path, content):
    entry, fd = record(label, path, fs.open, path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    if fd is None:
        return
    written = record("write-" + label, path, fs.write, fd, content, 0)
    written[0]["size"] = written[1]
    record("close-" + label, path, fs.close, fd)
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
many = child(b"many")
record("mkdir", root, fs.mkdir, root, 0o755)
stat("stat-root", root)
record("mkdir-many", many, fs.mkdir, many, 0o755)
failed = 0
for index in range(count):
    try:
        fs.close(fs.open(many + b"/entry-%04d" % index, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
    except cephfs.Error:
        failed += 1
ops.append({"op": "populate", "path": many.decode(), "errno": 0, "count": count - failed})
stat("stat-many", many)
publish(1, client=fs.get_instance_id(), ops=ops)

gate("readdir")
ops = []
entry, handle = record("opendir", many, fs.opendir, many)
names = []
listed = {"op": "readdir", "path": many.decode(), "errno": 0}
try:
    while True:
        dirent = fs.readdir(handle)
        if dirent is None:
            break
        names.append(dirent.d_name.decode())
except cephfs.Error as error:
    listed["errno"] = getattr(error, "errno", None) or -1
listed.update(count=len(names), names=names)
ops.append(listed)
record("closedir", many, fs.closedir, handle)
publish(2, ops=ops)

gate("recipe")
ops = []
path = child(b"file")
entry, fd = record("open-create", path, fs.open, path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
entry, written = record("write", path, fs.write, fd, data, 0)
entry["size"] = written
record("fsync", path, fs.fsync, fd, 0)
stat("fstat", path, lambda _: fs.fstat(fd))
record("close", path, fs.close, fd)
stat("stat", path)
stat("lstat", path, fs.lstat)
entry, fd = record("open-read", path, fs.open, path, os.O_RDONLY, 0)
entry, back = record("read", path, fs.read, fd, 0, len(data) + 1)
entry.update(size=len(back), data=back.hex())
record("close-read", path, fs.close, fd)
record("chmod", path, fs.chmod, path, 0o640)
record("chown", path, fs.chown, path, 4242, 4343)
record("truncate", path, fs.truncate, path, 600)
record("utime", path, fs.utime, path, (1700000000, 1600000000))
stat("stat-setattr", path)

hard = child(b"hard")
record("link", hard, fs.link, path, hard)
stat("stat-link-source", path)
stat("stat-link", hard)

victim, source = child(b"victim"), child(b"source")
create("create-victim", victim, victim_data)
create("create-source", source, source_data)
stat("stat-victim", victim)
stat("stat-source", source)
record("rename-over", victim, fs.rename, source, victim)
stat("stat-renamed", victim)
stat("stat-source-gone", source)

link = child(b"link")
record("symlink", link, fs.symlink, b"file", link)
entry, target = record("readlink", link, fs.readlink, link, 255)
entry["target"] = target.decode()
stat("lstat-symlink", link, fs.lstat)
stat("stat-symlink", link)

record("setxattr", path, fs.setxattr, path, "user.recipe", value, 0)
record("setxattr-temp", path, fs.setxattr, path, "user.temp", b"\x00", 0)
entry, got = record("getxattr", path, fs.getxattr, path, "user.recipe", 255)
entry.update(size=len(got), value=got.hex())
entry, keys = record("listxattr", path, fs.listxattr, path, 65536)
entry["names"] = [key.decode() for key in keys[1].split(b"\x00") if key]
record("removexattr", path, fs.removexattr, path, "user.temp")
record("getxattr-removed", path, fs.getxattr, path, "user.temp", 255)
record("getxattr-missing", path, fs.getxattr, path, "user.never", 255)

record("mkdir-existing", root, fs.mkdir, root, 0o755)
stat("stat-missing", child(b"missing"))
record("rmdir-nonempty", many, fs.rmdir, many)
record("open-dir-write", many, fs.open, many, os.O_WRONLY, 0)
record("unlink-dir", many, fs.unlink, many)
record("mkdir-under-file", path + b"/sub", fs.mkdir, path + b"/sub", 0o755)
record("rmdir-file", path, fs.rmdir, path)

first = fs.open(path, os.O_RDWR, 0)
second = fs.open(path, os.O_RDWR, 0)
record("flock-exclusive", path, fs.flock, first, fcntl.LOCK_EX | fcntl.LOCK_NB, 1)
record("flock-contended", path, fs.flock, second, fcntl.LOCK_EX | fcntl.LOCK_NB, 2)
record("flock-shared-contended", path, fs.flock, second, fcntl.LOCK_SH | fcntl.LOCK_NB, 2)
record("flock-unlock", path, fs.flock, first, fcntl.LOCK_UN, 1)
record("flock-after-unlock", path, fs.flock, second, fcntl.LOCK_EX | fcntl.LOCK_NB, 2)
record("flock-unlock-second", path, fs.flock, second, fcntl.LOCK_UN, 2)
fs.close(first)
fs.close(second)
methods = [method for method in ("fcntl", "getlk", "setlk", "lockf") if hasattr(fs, method)]
ops.append({"op": "posix-lock-api", "path": "", "errno": 0, "names": methods})
publish(3, ops=ops)

gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// cephFSMetadataReader lists a recipe's root through a mount of its own, in
// another container and process than the actor, and prints, as one JSON
// line, its client ID, every entry's lstat, the names in "many", the
// symlink's target, the bytes of "file" and "victim", and the xattr names and
// value of "file". It stats everything before it reads data.
const cephFSMetadataReader = `
import cephfs, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    root = sys.argv[2].encode()
    def listing(path):
        handle = fs.opendir(path)
        names = []
        while True:
            dirent = fs.readdir(handle)
            if dirent is None:
                break
            names.append(dirent.d_name.decode())
        fs.closedir(handle)
        return [name for name in names if name not in (".", "..")]
    entries = {}
    for name in listing(root):
        st = fs.lstat(root + b"/" + name.encode())
        entries[name] = dict(op="lstat", path=name, inode=st.st_ino, mode=st.st_mode, nlink=st.st_nlink, uid=st.st_uid,
                             gid=st.st_gid, size=st.st_size, atime=int(st.st_atime.timestamp()), mtime=int(st.st_mtime.timestamp()))
    def content(name):
        fd = fs.open(root + b"/" + name, os.O_RDONLY, 0)
        try:
            return fs.read(fd, 0, 1 << 16).hex()
        finally:
            fs.close(fd)
    path = root + b"/file"
    print(json.dumps({
        "client": fs.get_instance_id(), "entries": entries, "many": listing(root + b"/many"),
        "target": fs.readlink(root + b"/link", 255).decode(), "file": content(b"file"), "victim": content(b"victim"),
        "xattrs": [key.decode() for key in fs.listxattr(path, 65536)[1].split(b"\x00") if key],
        "xattr": fs.getxattr(path, "user.recipe", 255).hex()}))
finally:
    fs.unmount()
    fs.shutdown()
`

// cephFSRecipeOp is one row of the recipe actor's table, or one entry of the
// independent reader. Fields that an operation does not return stay zero.
type cephFSRecipeOp struct {
	Op     string   `json:"op"`
	Path   string   `json:"path"`
	Errno  int      `json:"errno"`
	Inode  uint64   `json:"inode"`
	Mode   uint32   `json:"mode"`
	Nlink  uint64   `json:"nlink"`
	UID    int      `json:"uid"`
	GID    int      `json:"gid"`
	Size   int64    `json:"size"`
	Atime  int64    `json:"atime"`
	Mtime  int64    `json:"mtime"`
	Count  int      `json:"count"`
	Data   string   `json:"data"`
	Value  string   `json:"value"`
	Target string   `json:"target"`
	Names  []string `json:"names"`
}

// String renders a row for the receipt log without its names or bytes.
func (op cephFSRecipeOp) String() string {
	row := fmt.Sprintf("%s %s errno=%d", op.Op, op.Path, op.Errno)
	if op.Inode != 0 {
		row += fmt.Sprintf(" ino=%d mode=%o nlink=%d uid=%d gid=%d size=%d atime=%d mtime=%d", op.Inode, op.Mode, op.Nlink, op.UID, op.GID, op.Size, op.Atime, op.Mtime)
	} else if op.Size != 0 {
		row += fmt.Sprintf(" size=%d", op.Size)
	}
	if op.Count != 0 {
		row += fmt.Sprintf(" count=%d", op.Count)
	}
	if op.Target != "" {
		row += " target=" + op.Target
	}
	if op.Value != "" {
		row += " value=" + op.Value
	}
	if op.Names != nil && len(op.Names) < 16 {
		row += fmt.Sprintf(" names=%q", op.Names)
	}
	return row
}

// cephFSRecipeTable is a receipt's operations by label.
type cephFSRecipeTable struct {
	rows   []cephFSRecipeOp
	byName map[string]cephFSRecipeOp
}

func cephFSRecipeRows(t *testing.T, receipt cephFSActorReceipt) cephFSRecipeTable {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(receipt.Ops))
	decoder.DisallowUnknownFields()
	table := cephFSRecipeTable{byName: make(map[string]cephFSRecipeOp)}
	if err := decoder.Decode(&table.rows); err != nil {
		t.Fatalf("receipt %d operations: %v: %.1024s", receipt.Phase, err, receipt.Ops)
	}
	for _, row := range table.rows {
		if _, ok := table.byName[row.Op]; ok {
			t.Fatalf("receipt %d has operation %s twice", receipt.Phase, row.Op)
		}
		table.byName[row.Op] = row
	}
	return table
}

// get returns the row of label and fails when the actor did not record it or
// recorded another errno.
func (table cephFSRecipeTable) get(t *testing.T, label string, errno int) cephFSRecipeOp {
	t.Helper()
	row, ok := table.byName[label]
	if !ok {
		t.Fatalf("no operation %s in %+v", label, table.rows)
	}
	if row.Errno != errno {
		t.Fatalf("%s: errno %d, want %d: %s", label, row.Errno, errno, row)
	}
	return row
}

// cephFSMDSReaddirRequests returns how many readdir requests the MDS has
// handled, from the count of its req_readdir_latency perf counter.
func cephFSMDSReaddirRequests(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string) uint64 {
	t.Helper()
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "perf", "dump", "mds_server")
	if err != nil {
		t.Fatalf("perf dump mds_server of mds.%s: %v", mds, err)
	}
	var dump struct {
		Server *struct {
			Readdir *struct {
				Count *uint64 `json:"avgcount"`
			} `json:"req_readdir_latency"`
		} `json:"mds_server"`
	}
	if err := json.Unmarshal(out, &dump); err != nil || dump.Server == nil || dump.Server.Readdir == nil || dump.Server.Readdir.Count == nil {
		t.Fatalf("perf dump mds_server of mds.%s has no req_readdir_latency: %v: %.512s", mds, err, out)
	}
	return *dump.Server.Readdir.Count
}

// mdsDirfrag is one fragment of "dirfrag ls".
type mdsDirfrag struct {
	Value uint64 `json:"value"`
	Bits  int    `json:"bits"`
	Str   string `json:"str"`
}

// cephFSDirfrags returns the fragments that mds lists for path.
func cephFSDirfrags(ctx context.Context, cluster *ceph.Container, mds, path string) ([]mdsDirfrag, error) {
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dirfrag", "ls", path)
	if err != nil {
		return nil, err
	}
	var frags []mdsDirfrag
	if err := json.Unmarshal(out, &frags); err != nil {
		return nil, fmt.Errorf("dirfrag ls %s: %w: %.512s", path, err, out)
	}
	return frags, nil
}

// mdsInodeLinks is the link count and size in the MDS's own "dump inode".
type mdsInodeLinks struct {
	Nlink *uint64 `json:"nlink"`
	Size  *int64  `json:"size"`
	Path  string  `json:"path"`
}

// cephFSMDSInodeLinks reads inode's link count and size from mds.
func cephFSMDSInodeLinks(ctx context.Context, cluster *ceph.Container, mds string, inode uint64) (mdsInodeLinks, []byte, error) {
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dump", "inode", strconv.FormatUint(inode, 10))
	if err != nil {
		return mdsInodeLinks{}, out, err
	}
	var dump mdsInodeLinks
	if err := json.Unmarshal(out, &dump); err != nil || dump.Nlink == nil || dump.Size == nil {
		return dump, out, fmt.Errorf("dump inode %d has no nlink or size: %v: %.512s", inode, err, out)
	}
	return dump, out, nil
}

// testCephFSMetadataRecipe runs one native client through a fixed sequence
// of metadata operations and keeps every operation's errno and native values
// as a receipt. The MDS's mds_bal_split_size is lowered through
// OverrideConfig so that the listed directory is split into several
// fragments, which libcephfs, having no readdir size option of its own,
// fetches with one MDS readdir request each; the MDS's readdir counter shows
// how many requests the listing took. A client in another container then
// checks the final namespace, and the MDS's "dump inode" the hard-linked
// file's link count and size.
func testCephFSMetadataRecipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	cluster, fs, mds := m.cluster, m.fs, m.mds.Name

	// With the default split size of 10000 the directory stays in one
	// fragment. 1000 splits it once (into 2^mds_bal_split_bits fragments)
	// and its fragments stay above mds_bal_merge_size.
	const entries, splitSize = 1500, "1000"
	split, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "mds", Name: "mds_bal_split_size", Value: splitSize})
	if split != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := split.Restore(restoreCtx); err != nil {
				t.Errorf("restore mds_bal_split_size: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	m.waitDaemonConfig(t, ctx, "mds_bal_split_size", splitSize)
	splitBits, mergeSize := m.daemonConfig(t, ctx, "mds_bal_split_bits"), m.daemonConfig(t, ctx, "mds_bal_merge_size")
	if fragmenting := m.daemonConfig(t, ctx, "mds_bal_fragment_dirs"); fragmenting != "true" {
		t.Fatalf("mds.%s runs with mds_bal_fragment_dirs=%s", mds, fragmenting)
	}

	data, source, victim, value := make([]byte, 1000), make([]byte, 333), make([]byte, 77), []byte{0x00, 0xff, 0x0a, 0x80, 'r', 0x00}
	for _, buffer := range [][]byte{data, source, victim} {
		if _, err := rand.Read(buffer); err != nil {
			t.Fatal(err)
		}
	}
	const root = "/recipe"
	actor := startCephFSActor(t, ctx, cluster, m.client, cephFSMetadataRecipeActor, fs.FilesystemName, "/tmp/metadata-recipe",
		root, strconv.Itoa(entries), hex.EncodeToString(data), hex.EncodeToString(source), hex.EncodeToString(victim), hex.EncodeToString(value))
	created := actor.receipt(t, ctx, 1)
	setup := cephFSRecipeRows(t, created)
	for _, label := range []string{"mkdir", "mkdir-many"} {
		setup.get(t, label, 0)
	}
	if populated := setup.get(t, "populate", 0); populated.Count != entries {
		t.Fatalf("created %d of %d entries", populated.Count, entries)
	}
	rootDir := setup.get(t, "stat-root", 0)
	if rootDir.Inode == 0 || rootDir.Mode != 0o40755 {
		t.Fatalf("root after mkdir: %s", rootDir)
	}

	// The MDS splits the directory in the background, after
	// mds_bal_fragment_interval.
	seen, err := observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]mdsDirfrag, bool, error) {
		frags, err := cephFSDirfrags(probeCtx, cluster, mds, root+"/many")
		return frags, len(frags) > 1, err
	})
	if err != nil {
		t.Fatalf("%s/many was not split: %+v: %v", root, seen.Value, err)
	}
	frags := seen.Value
	before := cephFSMDSReaddirRequests(t, ctx, cluster, mds)
	actor.open(t, ctx, "readdir")
	listing := cephFSRecipeRows(t, actor.receipt(t, ctx, 2))
	requests := cephFSMDSReaddirRequests(t, ctx, cluster, mds) - before
	listing.get(t, "opendir", 0)
	listing.get(t, "closedir", 0)
	listed := listing.get(t, "readdir", 0)
	want := []string{".", ".."}
	for index := range entries {
		want = append(want, fmt.Sprintf("entry-%04d", index))
	}
	got := slices.Clone(listed.Names)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) || listed.Count != len(want) {
		t.Fatalf("readdir returned %d names, want each of %d once", listed.Count, len(want))
	}
	if after, err := cephFSDirfrags(ctx, cluster, mds, root+"/many"); err != nil || !slices.Equal(after, frags) {
		t.Fatalf("fragments changed during the listing: %+v -> %+v, %v", frags, after, err)
	}
	if requests < uint64(len(frags)) {
		t.Fatalf("listing %d fragments took %d MDS readdir requests", len(frags), requests)
	}

	actor.open(t, ctx, "recipe")
	recipe := cephFSRecipeRows(t, actor.receipt(t, ctx, 3))
	file := checkCephFSRecipe(t, recipe, data, source, victim, value)

	// The MDS holds one inode with two links. The rename unlinked the old
	// victim: the MDS keeps it with no links in a stray directory until it
	// purges it, after which "dump inode" prints nothing for it ("the inode
	// is not cached" goes to the command's status).
	links, raw, err := cephFSMDSInodeLinks(ctx, cluster, mds, file.Inode)
	if err != nil || *links.Nlink != 2 || *links.Size != 600 {
		t.Fatalf("dump inode %d: %+v, %v: %.512s", file.Inode, links, err, raw)
	}
	oldVictim := recipe.get(t, "stat-victim", 0).Inode
	gone, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (string, bool, error) {
		out, err := cluster.Ceph(probeCtx, "tell", "mds."+mds, "dump", "inode", strconv.FormatUint(oldVictim, 10))
		if err != nil {
			return "", false, err
		}
		if len(bytes.TrimSpace(out)) == 0 {
			return "not cached", true, nil
		}
		var dump mdsInodeLinks
		if err := json.Unmarshal(out, &dump); err != nil || dump.Nlink == nil {
			return "", false, fmt.Errorf("dump inode %d: %v: %.512s", oldVictim, err, out)
		}
		return fmt.Sprintf("nlink=%d path=%s", *dump.Nlink, dump.Path), *dump.Nlink == 0 && strings.Contains(dump.Path, "stray"), nil
	})
	if err != nil {
		t.Fatalf("old victim inode %d: %s: %v", oldVictim, gone.Value, err)
	}

	reader := checkCephFSRecipeNamespace(t, ctx, cluster, fs.FilesystemName, root, created.Client, recipe, data, source, value, entries)

	actor.open(t, ctx, "finish")
	if closed := actor.receipt(t, ctx, 4); !closed.Closed || closed.CloseErrno != 0 || closed.PID != created.PID {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.wait(t, ctx)

	for _, table := range []cephFSRecipeTable{setup, listing, recipe} {
		for _, row := range table.rows {
			t.Logf("recipe op: %s", row)
		}
	}
	if methods := recipe.get(t, "posix-lock-api", 0).Names; len(methods) == 0 {
		t.Log("fcntl (POSIX record) locks: cephfs.LibCephFS exposes no method for them; only flock is covered")
	}
	t.Logf("CEPHFS_METADATA_RECIPE client.%d %d ops on mds.%s: readdir of %d entries in %d fragments (mds_bal_split_size=%s split_bits=%s merge_size=%s) took %d MDS readdir requests, every name once; file ino=%d nlink=2 size=600 mode=%o uid=4242 gid=4343 (MDS dump inode nlink=%d size=%d); rename-over replaced ino=%d with %d (old: %s); symlink -> file; xattr user.recipe=%x; errnos EEXIST=%d ENOENT=%d ENOTEMPTY=%d EISDIR=%d ENOTDIR=%d ENODATA=%d flock EWOULDBLOCK=%d; reader client.%d agreed",
		created.Client, len(setup.rows)+len(listing.rows)+len(recipe.rows), mds, entries, len(frags), splitSize, splitBits, mergeSize, requests,
		file.Inode, file.Mode, *links.Nlink, *links.Size, oldVictim, recipe.get(t, "stat-renamed", 0).Inode, gone.Value, value,
		recipe.get(t, "mkdir-existing", 17).Errno, recipe.get(t, "stat-missing", 2).Errno, recipe.get(t, "rmdir-nonempty", 39).Errno,
		recipe.get(t, "open-dir-write", 21).Errno, recipe.get(t, "mkdir-under-file", 20).Errno, recipe.get(t, "getxattr-missing", 61).Errno,
		recipe.get(t, "flock-contended", 11).Errno, reader)
}

// checkCephFSRecipe checks receipt 3 and returns the file's final stat.
func checkCephFSRecipe(t *testing.T, recipe cephFSRecipeTable, data, source, victim, value []byte) cephFSRecipeOp {
	t.Helper()
	for _, label := range []string{"open-create", "fsync", "close", "open-read", "close-read", "chmod", "chown", "truncate", "utime",
		"link", "create-victim", "close-create-victim", "create-source", "close-create-source", "rename-over", "symlink",
		"setxattr", "setxattr-temp", "removexattr", "flock-exclusive", "flock-unlock", "flock-after-unlock", "flock-unlock-second"} {
		recipe.get(t, label, 0)
	}
	// Expected errors, as the binding reports them.
	for label, errno := range map[string]int{
		"mkdir-existing": 17, "stat-missing": 2, "stat-source-gone": 2, "rmdir-nonempty": 39, "open-dir-write": 21,
		"unlink-dir": 21, "mkdir-under-file": 20, "rmdir-file": 20, "getxattr-removed": 61, "getxattr-missing": 61,
		"flock-contended": 11, "flock-shared-contended": 11,
	} {
		recipe.get(t, label, errno)
	}
	if written := recipe.get(t, "write", 0); written.Size != int64(len(data)) {
		t.Fatalf("write: %s, want %d bytes", written, len(data))
	}
	created := recipe.get(t, "fstat", 0)
	if created.Inode == 0 || created.Mode != 0o100644 || created.Nlink != 1 || created.Size != int64(len(data)) || created.UID != 0 || created.GID != 0 {
		t.Fatalf("fstat after the write: %s", created)
	}
	for _, label := range []string{"stat", "lstat"} {
		if row := recipe.get(t, label, 0); row.Inode != created.Inode || row.Mode != created.Mode || row.Size != created.Size || row.Nlink != 1 {
			t.Fatalf("%s: %s, fstat %s", label, row, created)
		}
	}
	if read := recipe.get(t, "read", 0); read.Data != hex.EncodeToString(data) || read.Size != int64(len(data)) {
		t.Fatalf("read back %d bytes, want the %d written", read.Size, len(data))
	}
	file := recipe.get(t, "stat-setattr", 0)
	if file.Inode != created.Inode || file.Mode != 0o100640 || file.UID != 4242 || file.GID != 4343 || file.Size != 600 || file.Atime != 1700000000 || file.Mtime != 1600000000 || file.Nlink != 1 {
		t.Fatalf("after chmod, chown, truncate and utime: %s", file)
	}
	for _, label := range []string{"stat-link-source", "stat-link"} {
		if row := recipe.get(t, label, 0); row.Inode != file.Inode || row.Nlink != 2 || row.Size != 600 || row.Mode != file.Mode {
			t.Fatalf("%s: %s, want inode %d with two links", label, row, file.Inode)
		}
	}
	oldVictim, sourceStat, renamed := recipe.get(t, "stat-victim", 0), recipe.get(t, "stat-source", 0), recipe.get(t, "stat-renamed", 0)
	if oldVictim.Size != int64(len(victim)) || sourceStat.Size != int64(len(source)) || oldVictim.Inode == sourceStat.Inode {
		t.Fatalf("before the rename: victim %s, source %s", oldVictim, sourceStat)
	}
	if renamed.Inode != sourceStat.Inode || renamed.Size != int64(len(source)) || renamed.Nlink != 1 {
		t.Fatalf("after the rename the victim is %s, want the source %s", renamed, sourceStat)
	}
	if target := recipe.get(t, "readlink", 0); target.Target != "file" {
		t.Fatalf("readlink: %s", target)
	}
	if symlink := recipe.get(t, "lstat-symlink", 0); symlink.Mode != 0o120777 || symlink.Size != int64(len("file")) || symlink.Inode == file.Inode {
		t.Fatalf("lstat of the symlink: %s", symlink)
	}
	if followed := recipe.get(t, "stat-symlink", 0); followed.Inode != file.Inode {
		t.Fatalf("stat through the symlink: %s, want inode %d", followed, file.Inode)
	}
	if got := recipe.get(t, "getxattr", 0); got.Value != hex.EncodeToString(value) || got.Size != int64(len(value)) {
		t.Fatalf("getxattr: %s, want %x", got, value)
	}
	if listed := recipe.get(t, "listxattr", 0); !slices.Equal(listed.Names, []string{"user.recipe", "user.temp"}) && !slices.Equal(listed.Names, []string{"user.temp", "user.recipe"}) {
		t.Fatalf("listxattr: %q, want user.recipe and user.temp", listed.Names)
	}
	return file
}

// checkCephFSRecipeNamespace lists the recipe's root from another container
// and compares every name, inode, link count, mode, owner, size and time
// with the actor's own last stats, and the bytes, symlink target and xattrs.
// It returns the reader's client ID.
func checkCephFSRecipeNamespace(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem, root string, actorClient uint64, recipe cephFSRecipeTable, data, source, value []byte, entries int) uint64 {
	t.Helper()
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, cluster.ControlContainer(), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSMetadataReader, filesystem, root}, Timeout: 2 * time.Minute,
		MaxStdout: 256 << 10, MaxStderr: 64 << 10, Label: "independent CephFS metadata reader",
	})
	stop()
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent reader: exit=%d err=%v stderr=%.2048s", read.ExitCode, err, read.Stderr)
	}
	var independent struct {
		Client  uint64                    `json:"client"`
		Entries map[string]cephFSRecipeOp `json:"entries"`
		Many    []string                  `json:"many"`
		Target  string                    `json:"target"`
		File    string                    `json:"file"`
		Victim  string                    `json:"victim"`
		XAttrs  []string                  `json:"xattrs"`
		XAttr   string                    `json:"xattr"`
	}
	decoder := json.NewDecoder(strings.NewReader(lastJSONLine(string(read.Stdout))))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&independent); err != nil {
		t.Fatalf("independent reader output: %v: %.512s", err, read.Stdout)
	}
	if independent.Client == actorClient || independent.Client == 0 {
		t.Fatalf("independent reader is client.%d, the actor client.%d", independent.Client, actorClient)
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range independent.Entries {
			if !yield(name) {
				return
			}
		}
	})
	if !slices.Equal(names, []string{"file", "hard", "link", "many", "victim"}) {
		t.Fatalf("independent reader lists %q", names)
	}
	file := recipe.get(t, "stat-setattr", 0)
	want := map[string]cephFSRecipeOp{
		"file":   recipe.get(t, "stat-link-source", 0),
		"hard":   recipe.get(t, "stat-link", 0),
		"victim": recipe.get(t, "stat-renamed", 0),
		"link":   recipe.get(t, "lstat-symlink", 0),
	}
	for name, expected := range want {
		entry := independent.Entries[name]
		if entry.Inode != expected.Inode || entry.Mode != expected.Mode || entry.Nlink != expected.Nlink || entry.UID != expected.UID ||
			entry.GID != expected.GID || entry.Size != expected.Size || entry.Mtime != expected.Mtime {
			t.Fatalf("independent reader's %s: %s, the actor's %s", name, entry, expected)
		}
	}
	if file.Atime != independent.Entries["file"].Atime {
		t.Fatalf("independent reader's file atime %d, the actor set %d", independent.Entries["file"].Atime, file.Atime)
	}
	if dir := independent.Entries["many"]; dir.Mode != 0o40755 || dir.Inode == 0 {
		t.Fatalf("independent reader's many: %s", dir)
	}
	if len(independent.Many) != entries || len(slices.Compact(slices.Sorted(slices.Values(independent.Many)))) != entries {
		t.Fatalf("independent reader lists %d names in many, want %d distinct", len(independent.Many), entries)
	}
	if independent.Target != "file" || independent.File != hex.EncodeToString(data[:600]) || independent.Victim != hex.EncodeToString(source) {
		t.Fatalf("independent reader: target %q, file %d hex digits, victim %d hex digits", independent.Target, len(independent.File), len(independent.Victim))
	}
	if !slices.Equal(independent.XAttrs, []string{"user.recipe"}) || independent.XAttr != hex.EncodeToString(value) {
		t.Fatalf("independent reader's xattrs %q = %s, want only user.recipe = %x", independent.XAttrs, independent.XAttr, value)
	}
	return independent.Client
}
