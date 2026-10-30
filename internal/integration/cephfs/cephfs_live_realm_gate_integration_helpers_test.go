//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_live_realm_gate || ci_batch_cephfs_live_realm_removal))))

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSRealmActor keeps one libcephfs mount and one O_RDWR fd open for its
// whole run, with a client admin socket at a given path. It creates a
// directory and a file in it, writes and fsyncs the base bytes and publishes
// receipt 1 with the client ID and the inodes of file and directory. At the
// "overwrite" gate it overwrites the file from offset 0 through the same fd,
// fsyncs, reads the whole file back through the fd and publishes receipt 2.
// After the "finish" gate it closes, unmounts and publishes receipt 3.
// Arguments after the common ones: directory, file path, the hex of the base
// bytes and of the new bytes, admin socket path.
const cephFSRealmActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
directory, path = sys.argv[4].encode(), sys.argv[5].encode()
base, new = bytes.fromhex(sys.argv[6]), bytes.fromhex(sys.argv[7])
asok = sys.argv[8]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fs.mkdir(directory, 0o755)
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, dir_inode=fs.stat(directory).st_ino, size=stat.st_size)
gate("overwrite")
result = {}
try:
    fs.write(fd, new, 0)
    fs.fsync(fd, 0)
    stat = fs.fstat(fd)
    result.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size).hex())
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
publish(2, **result)
gate("finish")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(3, closed=True, **closing)
`

// cephFSRealmAdmin is the independent administrator: a mount of its own, in
// another container than the actor. Arguments: mode, filesystem, directory,
// snapshot name, file name. Mode "snap" makes the directory snapshot, "read"
// prints the file's bytes from HEAD and from the snapshot (a failed read
// prints its errno), and "rmsnap" removes the snapshot, where an absent one
// counts as removed.
const cephFSRealmAdmin = `
import cephfs, json, os, sys
mode, filesystem, directory, snap, leaf = sys.argv[1:6]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=filesystem.encode())
def slurp(path):
    try:
        stat = fs.stat(path.encode())
        fd = fs.open(path.encode(), os.O_RDONLY, 0)
        data = fs.read(fd, 0, stat.st_size + 1)
        fs.close(fd)
        return {"size": stat.st_size, "data": data.hex()}
    except cephfs.Error as error:
        return {"errno": getattr(error, "errno", None) or -1}
try:
    snapdir = directory + "/.snap/" + snap
    out = {"client": fs.get_instance_id()}
    if mode == "snap":
        fs.mkdir(snapdir.encode(), 0o755)
        out["dir_inode"] = fs.stat(directory.encode()).st_ino
    elif mode == "read":
        out["head"] = slurp(directory + "/" + leaf)
        out["snap"] = slurp(snapdir + "/" + leaf)
    elif mode == "rmsnap":
        try:
            fs.rmdir(snapdir.encode())
            out["removed"] = True
        except cephfs.ObjectNotFound:
            out["removed"] = False
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

type cephFSRealmAdminRead struct {
	Size  int64  `json:"size"`
	Data  string `json:"data"`
	Errno int    `json:"errno"`
}

type cephFSRealmAdminOut struct {
	Client   uint64                `json:"client"`
	DirInode uint64                `json:"dir_inode"`
	Removed  bool                  `json:"removed"`
	Head     *cephFSRealmAdminRead `json:"head"`
	Snap     *cephFSRealmAdminRead `json:"snap"`
}

func runCephFSRealmAdmin(ctx context.Context, cluster *ceph.Container, toolkit testcontainers.Container, mode, filesystem, directory, snap, leaf string) (cephFSRealmAdminOut, error) {
	runCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	res, err := cluster.RunNativeCommand(runCtx, toolkit, ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSRealmAdmin, mode, filesystem, directory, snap, leaf}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "CephFS snapshot administrator",
	})
	if err != nil || res.ExitCode != 0 || !res.Complete() {
		return cephFSRealmAdminOut{}, fmt.Errorf("administrator %s: exit=%d err=%v stderr=%.1024s", mode, res.ExitCode, err, res.Stderr)
	}
	var out cephFSRealmAdminOut
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(res.Stdout))), &out); err != nil {
		return cephFSRealmAdminOut{}, fmt.Errorf("administrator %s output: %v: %.512s", mode, err, res.Stdout)
	}
	return out, nil
}

// queryClientDumpCache returns one complete dump_cache of the client behind
// asok, which libcephfs prints from a single locked pass over its inode map.
func queryClientDumpCache(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) ([]byte, error) {
	queryCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	res, err := cluster.RunNativeCommand(queryCtx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "dump_cache"}, Timeout: 30 * time.Second,
		MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "client dump_cache",
	})
	if err != nil || res.ExitCode != 0 || !res.Complete() {
		return nil, fmt.Errorf("client dump_cache: exit=%d err=%v stderr=%.512s", res.ExitCode, err, res.Stderr)
	}
	return res.Stdout, nil
}

func testCephFSLiveSnapRealmGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	toolkit := harness.ToolkitContainer(t, m.Cluster)
	fsName := m.FS.FilesystemName
	// Snapshots are an explicit server setting, not assumed from a default.
	if _, err := m.Cluster.Ceph(ctx, "fs", "set", fsName, "allow_new_snaps", "true"); err != nil {
		t.Fatal("allow_new_snaps:", err)
	}

	base := bytes.Repeat([]byte("before-snapshot\x00\xff"), 512)
	updated := bytes.Repeat([]byte("after-snapshot!\x01\xfe"), 512)
	const (
		workdir   = "/tmp/realm-actor"
		directory = "/realm"
		leaf      = "file"
		snapName  = "live-realm-gate"
	)
	asok := workdir + "/client.asok"
	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSRealmActor, fsName, workdir,
		directory, directory+"/"+leaf, hex.EncodeToString(base), hex.EncodeToString(updated), asok)
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.DirInode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}

	// The snapshot is removed on every exit; an absent one counts as removed.
	t.Cleanup(func() {
		cleanCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if _, err := runCephFSRealmAdmin(cleanCtx, m.Cluster, toolkit, "rmsnap", fsName, directory, snapName, leaf); err != nil {
			t.Errorf("remove snapshot: %v", err)
		}
	})

	// Before the snapshot: whatever the client caches, and its derived context.
	preData, err := queryClientDumpCache(ctx, m.Cluster, m.Client, asok)
	if err != nil {
		t.Fatal(err)
	}
	preView, preErr := parseClientSnapRealms(preData, baseline.Inode)
	var preCtx clientSnapContext
	if preErr == nil {
		preCtx, preErr = preView.derive()
	}
	t.Logf("CEPHFS_LIVE_REALM before snapshot (%s): %s", classifySnapRealm(preErr), snapRealmShape(preData, baseline.Inode))

	made, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "snap", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if made.Client == 0 || made.Client == baseline.Client || made.DirInode != baseline.DirInode {
		t.Fatalf("snapshot administrator = %+v for actor %+v", made, baseline)
	}
	// The snapshot ID comes from the MDS's snap table, which the creator's
	// reply does not order; the read is bounded as its own phase.
	var snapLookErr error
	snapID, _, err := boundedPoll(ctx, realPollClock, time.Minute, time.Second, func(attemptCtx context.Context) (uint64, bool, error) {
		raw, err := m.Cluster.Ceph(attemptCtx, "tell", "mds."+m.MDS.Name, "dump", "snaps")
		if err != nil {
			snapLookErr = err
			return 0, false, nil
		}
		id, err := mdsSnapshotID(raw, baseline.DirInode, snapName)
		snapLookErr = err
		return id, err == nil, nil
	})
	if err != nil {
		t.Fatalf("snapshot ID of %q: %v (last: %v)", snapName, err, snapLookErr)
	}
	if preErr == nil && preCtx.hasSnap(snapID) {
		t.Fatalf("the context before the snapshot already lists snapshot %x: %+v", snapID, preCtx)
	}

	// The MDS pushes the new context to clients with caps in the realm
	// (Client::handle_snap); nothing orders that push before the creator's
	// reply, so the retained client's own cache is polled, bounded.
	type look struct {
		released bool
		derived  clientSnapContext
		err      error
		data     []byte
	}
	started := time.Now()
	var first []byte
	final, looks, err := boundedPoll(ctx, realPollClock, 90*time.Second, time.Second, func(attemptCtx context.Context) (look, bool, error) {
		data, err := queryClientDumpCache(attemptCtx, m.Cluster, m.Client, asok)
		if err != nil {
			return look{}, false, err
		}
		l := look{data: data}
		l.released, l.derived, l.err = snapContextGate(data, baseline.Inode, snapID)
		if first == nil {
			first = data
			t.Logf("CEPHFS_LIVE_REALM first look after snapshot (%s): %s", classifySnapRealm(l.err), snapRealmShape(data, baseline.Inode))
		}
		return l, l.released, nil
	})
	if err != nil || !final.released {
		t.Fatalf("CEPHFS_LIVE_REALM gate not released after %d looks over %s (%v): %s; derived=%+v; snapshot id=%x; the overwrite was not attempted: %s",
			looks, time.Since(started).Round(time.Second), err, classifySnapRealm(final.err), final.derived, snapID, snapRealmShape(final.data, baseline.Inode))
	}
	derived, last := final.derived, final.data
	t.Logf("CEPHFS_LIVE_REALM gate released after %d looks, %s: derived seq=%x snaps=%x chain=%x; %s",
		looks, time.Since(started).Round(time.Millisecond), derived.Seq, derived.Snaps, derived.Chain, snapRealmShape(last, baseline.Inode))

	actor.Open(t, ctx, "overwrite")
	over := actor.Receipt(t, ctx, 2)
	if over.Errno != 0 || over.Error != "" || over.PID != baseline.PID || over.Inode != baseline.Inode ||
		over.Size != int64(len(updated)) || over.Full != hex.EncodeToString(updated) {
		t.Fatalf("overwrite receipt: errno=%d error=%q pid=%d inode=%d size=%d", over.Errno, over.Error, over.PID, over.Inode, over.Size)
	}
	read, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "read", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if read.Head == nil || read.Snap == nil || read.Head.Errno != 0 || read.Snap.Errno != 0 ||
		read.Head.Data != hex.EncodeToString(updated) || read.Snap.Data != hex.EncodeToString(base) ||
		read.Head.Size != int64(len(updated)) || read.Snap.Size != int64(len(base)) {
		t.Fatalf("independent read: head=%+v snapshot=%+v", read.Head, read.Snap)
	}
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt: %+v", closed)
	}
	actor.Wait(t, ctx)
	t.Logf("CEPHFS_LIVE_REALM %s client.%d inode=%d dir=0x%x snapshot id=%x: gate released on derived membership; live fd overwrite of %d bytes; HEAD read as the new bytes and the snapshot as the %d base bytes by client.%d",
		m.Version, baseline.Client, baseline.Inode, baseline.DirInode, snapID, len(updated), len(base), read.Client)
}
