//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSCoupledActorHelpers adds attempt to an actor: it runs one call,
// appends {"op", "errno"} to ops, with 0 for success, and returns whether the
// call succeeded.
const cephFSCoupledActorHelpers = cephFSActorHelpers + `
ops = []
def attempt(op, call, *args):
    entry = {"op": op, "errno": 0}
    ops.append(entry)
    try:
        call(*args)
        return True
    except cephfs.Error as error:
        entry["errno"] = getattr(error, "errno", None) or -1
        return False
`

// cephFSFencedActor keeps one mount with an admin socket and one O_RDWR fd.
// It writes and fsyncs the base bytes and publishes receipt 1. At the
// "buffer" gate it writes the tail behind them without fsync and publishes
// receipt 2 with the size its fd reports; client_oc_max_dirty_age of 600 s
// keeps the tail in its cache. At "after" it calls fsync, write of the tail,
// fsync again and stat, in that order, and publishes receipt 3 with each
// errno. It closes the fd and unmounts after "finish" and publishes receipt
// 4. Arguments after the common ones: the admin socket, the file, the hex of
// the base and of the tail.
const cephFSFencedActor cephFSActorScript = cephFSCoupledActorHelpers + `
asok, path = sys.argv[4], sys.argv[5].encode()
base, tail = bytes.fromhex(sys.argv[6]), bytes.fromhex(sys.argv[7])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.conf_set("client_oc_max_dirty_age", "600")
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("buffer")
fs.write(fd, tail, len(base))
publish(2, size=fs.fstat(fd).st_size)
gate("after")
attempt("fsync", fs.fsync, fd, 0)
attempt("write", fs.write, fd, tail, len(base))
attempt("fsync-again", fs.fsync, fd, 0)
attempt("stat", fs.stat, path)
publish(3, ops=ops)
gate("finish")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// cephFSBarrierObserver is a second live client with an admin socket. It
// mounts and publishes receipt 1 with its client ID and the file's inode. At
// the "read" gate it opens the file for reading and writing, reads one byte
// more than its size, then appends its own marker and fsyncs, so that the
// MDS acknowledges a cap flush, and publishes receipt 2 with the size and
// the bytes it read. It unmounts after "finish" and publishes receipt 3.
// Arguments after the common ones: the admin socket, the file and the hex of
// the marker.
const cephFSBarrierObserver cephFSActorScript = cephFSActorHelpers + `
asok, path, marker = sys.argv[4], sys.argv[5].encode(), bytes.fromhex(sys.argv[6])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
publish(1, client=fs.get_instance_id(), inode=fs.stat(path).st_ino)
gate("read")
stat = fs.stat(path)
fd = fs.open(path, os.O_RDWR, 0)
data = fs.read(fd, 0, stat.st_size + 1)
fs.write(fd, marker, stat.st_size)
fs.fsync(fd, 0)
fs.close(fd)
publish(2, inode=stat.st_ino, size=stat.st_size, full=data.hex())
gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(3, closed=True, **closing)
`

// cephFSClientStatus is a libcephfs client's admin socket "status": the
// OSDMap epoch it holds, the epoch barrier it was given and whether it found
// itself blocklisted.
type cephFSClientStatus struct {
	ID          uint64 `json:"id"`
	Address     string `json:"addr_str"`
	OSDEpoch    uint32 `json:"osd_epoch"`
	Barrier     uint32 `json:"osd_epoch_barrier"`
	Blocklisted bool   `json:"blocklisted"`
}

func clientAdminStatus(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) (cephFSClientStatus, error) {
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "status"}, Timeout: 30 * time.Second,
		MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "client status",
	})
	if err != nil || result.ExitCode != 0 {
		return cephFSClientStatus{}, fmt.Errorf("client status: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
	}
	var status cephFSClientStatus
	if err := json.Unmarshal(result.Stdout, &status); err != nil || status.ID == 0 {
		return cephFSClientStatus{}, fmt.Errorf("client status: %v: %.512s", err, result.Stdout)
	}
	return status, nil
}

// waitClientStatus observes, bounded, a client's status until ready accepts it.
func waitClientStatus(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok, what string, ready func(cephFSClientStatus) bool) cephFSClientStatus {
	t.Helper()
	seen, err := observe(ctx, time.Minute, 40*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (cephFSClientStatus, bool, error) {
		status, err := clientAdminStatus(probeCtx, cluster, client, asok)
		return status, err == nil && ready(status), err
	})
	if err != nil {
		t.Fatalf("client status %s: last %+v: %v", what, seen.Value, err)
	}
	return seen.Value
}

// mdsEpochs is the OSDMap epoch and epoch barrier in an MDS's "status".
type mdsEpochs struct {
	OSDMap  uint32 `json:"osdmap_epoch"`
	Barrier uint32 `json:"osdmap_epoch_barrier"`
}

func (m *cephFSSingleMDS) epochs(ctx context.Context) (mdsEpochs, error) {
	out, err := m.cluster.Ceph(ctx, "tell", "mds."+m.mds.Name, "status")
	if err != nil {
		return mdsEpochs{}, err
	}
	var epochs mdsEpochs
	if err := json.Unmarshal(out, &epochs); err != nil || epochs.OSDMap == 0 {
		return mdsEpochs{}, fmt.Errorf("mds status: %v: %.512s", err, out)
	}
	return epochs, nil
}

// osdMapAt is the part of one OSDMap epoch that the tests read: its
// blocklist and its pools' flags.
type osdMapAt struct {
	Epoch     uint32            `json:"epoch"`
	Blocklist map[string]string `json:"blocklist"`
	Pools     []struct {
		ID    int64  `json:"pool"`
		Flags string `json:"flags_names"`
	} `json:"pools"`
}

// cephfsOSDMapAt reads one OSDMap epoch, or the current one for 0.
func cephfsOSDMapAt(ctx context.Context, cluster *ceph.Container, epoch uint32) (osdMapAt, error) {
	args := []string{"osd", "dump"}
	if epoch != 0 {
		args = append(args, strconv.FormatUint(uint64(epoch), 10))
	}
	out, err := cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil {
		return osdMapAt{}, err
	}
	var dump osdMapAt
	if err := json.Unmarshal(out, &dump); err != nil || dump.Epoch == 0 {
		return osdMapAt{}, fmt.Errorf("osd dump %d: %v: %.256s", epoch, err, out)
	}
	return dump, nil
}

// poolFull reports whether pool carries the "full" flag in dump.
func (dump osdMapAt) poolFull(pool int64) (bool, bool) {
	for _, p := range dump.Pools {
		if p.ID == pool {
			return slices.Contains(strings.Split(p.Flags, ","), "full"), true
		}
	}
	return false, false
}

// coupledOps decodes the ops of a receipt into op name and errno.
func coupledOps(t *testing.T, receipt cephFSActorReceipt) map[string]int {
	t.Helper()
	var ops []struct {
		Op    string `json:"op"`
		Errno int    `json:"errno"`
	}
	if err := json.Unmarshal(receipt.Ops, &ops); err != nil {
		t.Fatalf("receipt %d ops: %v: %s", receipt.Phase, err, receipt.Ops)
	}
	errnos := make(map[string]int, len(ops))
	for _, op := range ops {
		errnos[op.Op] = op.Errno
	}
	return errnos
}

// testCephFSBlocklistEpochBarrier evicts a live client that holds a file
// with fsynced base bytes and a buffered, unflushed tail, while a second live
// client in another container keeps its own mount. The MDS blocklists the
// client's address: the entry first appears in OSDMap epoch E, and the MDS's
// own osdmap_epoch_barrier becomes E. The evicted client receives epoch E,
// finds itself blocklisted (its admin socket status, and "I was blocklisted
// at osd epoch E" on its stderr), aborts its MDS session and drops its dirty
// data: its fsync returns ESHUTDOWN (108), a new write and a stat return
// 108, and the second fsync returns 0 because nothing is dirty any more. The
// second client then reads the file and gets the base bytes only. It appends
// a marker and fsyncs; the MDS's flush ack carries the barrier, and the
// second client's own osd_epoch_barrier becomes E. The evicted client still
// closes and unmounts with 0.
func testCephFSBlocklistEpochBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	const path, workdir, observerDir = "/fenced", "/tmp/fenced", "/tmp/barrier-observer"
	base, tail := []byte("fsynced base bytes"), []byte("buffered tail")
	asok, observerAsok := workdir+"/client.asok", observerDir+"/client.asok"
	actor := startCephFSActor(t, ctx, m.cluster, m.client, cephFSFencedActor, m.fs.FilesystemName, workdir, asok, path, hex.EncodeToString(base), hex.EncodeToString(tail))
	opened := actor.receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 || opened.Size != int64(len(base)) {
		t.Fatalf("receipt 1 = %+v", opened)
	}
	observer := startCephFSActor(t, ctx, m.cluster, toolkitContainer(t, m.cluster), cephFSBarrierObserver, m.fs.FilesystemName, observerDir, observerAsok, path, hex.EncodeToString([]byte("observer marker")))
	watching := observer.receipt(t, ctx, 1)
	if watching.Client == opened.Client || watching.Inode != opened.Inode {
		t.Fatalf("observer receipt 1 = %+v, actor %+v", watching, opened)
	}
	actor.open(t, ctx, "buffer")
	if buffered := actor.receipt(t, ctx, 2); buffered.Size != int64(len(base)+len(tail)) {
		t.Fatalf("receipt 2 = %+v", buffered)
	}
	session := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0)
	before, err := clientAdminStatus(ctx, m.cluster, m.client, asok)
	if err != nil || before.ID != opened.Client || before.Blocklisted || before.Address != session.Address {
		t.Fatalf("client status before the eviction: %+v, %v (session %s)", before, err, session.Address)
	}
	mdsBefore, err := m.epochs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapBefore, err := cephfsOSDMapAt(ctx, m.cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, listed := mapBefore.Blocklist[session.Address]; listed {
		t.Fatalf("%s is blocklisted before the eviction", session.Address)
	}
	observerBefore, err := clientAdminStatus(ctx, m.cluster, toolkitContainer(t, m.cluster), observerAsok)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := m.cluster.Ceph(ctx, "tell", "mds."+m.mds.Name, "client", "evict", "id="+strconv.FormatUint(opened.Client, 10)); err != nil {
		t.Fatal("client evict:", err)
	}
	// The first epoch whose blocklist holds the address.
	current, err := cephfsOSDMapAt(ctx, m.cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fenced uint32
	for epoch := mapBefore.Epoch + 1; epoch <= current.Epoch && fenced == 0; epoch++ {
		dump, err := cephfsOSDMapAt(ctx, m.cluster, epoch)
		if err != nil {
			t.Fatal(err)
		}
		if _, listed := dump.Blocklist[session.Address]; listed {
			fenced = epoch
		}
	}
	if fenced == 0 {
		t.Fatalf("no OSDMap epoch in %d..%d blocklists %s: %v", mapBefore.Epoch+1, current.Epoch, session.Address, current.Blocklist)
	}
	barrier, err := observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (mdsEpochs, bool, error) {
		epochs, err := m.epochs(probeCtx)
		return epochs, err == nil && epochs.Barrier >= fenced, err
	})
	if err != nil || barrier.Value.Barrier != fenced {
		t.Fatalf("mds.%s osdmap_epoch_barrier %+v (before %+v), want the blocklist epoch %d: %v", m.mds.Name, barrier.Value, mdsBefore, fenced, err)
	}
	evicted := waitClientStatus(t, ctx, m.cluster, m.client, asok, "blocklisted", func(status cephFSClientStatus) bool {
		return status.Blocklisted && status.OSDEpoch >= fenced
	})
	if gone := sessionOf(t, ctx, m, opened.Client); gone {
		t.Fatalf("mds.%s still lists client.%d", m.mds.Name, opened.Client)
	}

	actor.open(t, ctx, "after")
	errnos := coupledOps(t, actor.receipt(t, ctx, 3))
	if want := map[string]int{"fsync": 108, "write": 108, "fsync-again": 0, "stat": 108}; !maps.Equal(errnos, want) {
		t.Fatalf("evicted client's calls: %v, want %v", errnos, want)
	}
	observer.open(t, ctx, "read")
	read := observer.receipt(t, ctx, 2)
	if read.Inode != opened.Inode || read.Size != int64(len(base)) || read.Full != hex.EncodeToString(base) {
		t.Fatalf("observer read %+v, want only the %d fsynced bytes", read, len(base))
	}
	observed := waitClientStatus(t, ctx, m.cluster, toolkitContainer(t, m.cluster), observerAsok, "barrier", func(status cephFSClientStatus) bool {
		return status.Barrier >= fenced
	})
	if observed.Barrier != fenced || observed.Blocklisted || observed.OSDEpoch < fenced {
		t.Fatalf("observer status %+v, want barrier %d", observed, fenced)
	}

	actor.open(t, ctx, "finish")
	if closed := actor.receipt(t, ctx, 4); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("evicted client's closing receipt = %+v", closed)
	}
	actor.wait(t, ctx)
	confession := fmt.Sprintf("I was blocklisted at osd epoch %d", fenced)
	if !bytes.Contains(actor.result.Stderr, []byte(confession)) {
		t.Fatalf("evicted client's stderr lacks %q: %s", confession, actor.exited(ctx))
	}
	observer.open(t, ctx, "finish")
	if closed := observer.receipt(t, ctx, 3); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("observer's closing receipt = %+v", closed)
	}
	observer.wait(t, ctx)
	t.Logf("CEPHFS_EPOCH_BARRIER_RECEIPT %s mds.%s client.%d %s inode=%d: before eviction OSDMap e%d, client osd_epoch=%d barrier=%d, mds barrier=%d; client evict put the address in the blocklist of OSDMap e%d; mds osdmap_epoch_barrier %d -> %d; the evicted client reached osd_epoch=%d blocklisted=%t (barrier %d) and logged %q; then fsync=%d write=%d fsync-again=%d stat=%d; observer client.%d read %d bytes (only the fsynced ones) and holds barrier %d -> %d; the evicted client closed and unmounted with 0",
		m.version, m.mds.Name, opened.Client, session.Address, opened.Inode, mapBefore.Epoch, before.OSDEpoch, before.Barrier, mdsBefore.Barrier,
		fenced, mdsBefore.Barrier, barrier.Value.Barrier, evicted.OSDEpoch, evicted.Blocklisted, evicted.Barrier, confession,
		errnos["fsync"], errnos["write"], errnos["fsync-again"], errnos["stat"], watching.Client, read.Size, observerBefore.Barrier, observed.Barrier)
}

// sessionOf reports whether the MDS lists a session of client.
func sessionOf(t *testing.T, ctx context.Context, m *cephFSSingleMDS, client uint64) bool {
	t.Helper()
	sessions, err := m.fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(sessions, func(s cephfs.Session) bool { return s.ID == client })
}

// cephFSFullActor keeps one mount with an admin socket and one O_RDWR fd. It
// writes and fsyncs the base bytes and publishes receipt 1. At the "poke"
// gate it reads a file that another client wrote, which it has not cached,
// so that it sends an OSD read and the OSD shares its newer OSDMap, and
// publishes receipt 2 with the bytes. At the "full" gate it writes the tail
// behind the base bytes, fsyncs, creates a directory and stats the file, and
// publishes receipt 3 with each errno and the size its fd reports. At
// "repoke" it reads a second such file and publishes receipt 4. At "resume"
// it writes the tail again and fsyncs, and publishes receipt 5 the same way
// as receipt 3. It closes the fd and unmounts after "finish" and publishes
// receipt 6. Arguments after the common ones: the admin socket, the file, a
// directory to create, the hex of the base and of the tail, and the other
// client's two files.
const cephFSFullActor cephFSActorScript = cephFSCoupledActorHelpers + `
asok, path, directory = sys.argv[4], sys.argv[5].encode(), sys.argv[6].encode()
base, tail = bytes.fromhex(sys.argv[7]), bytes.fromhex(sys.argv[8])
probes = [arg.encode() for arg in sys.argv[9:11]]
def read_probe(phase, probe):
    poked = fs.open(probe, os.O_RDONLY, 0)
    publish(phase, full=fs.read(poked, 0, 4096).hex())
    fs.close(poked)
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("poke")
read_probe(2, probes[0])
gate("full")
attempt("write", fs.write, fd, tail, len(base))
attempt("fsync", fs.fsync, fd, 0)
attempt("mkdir", fs.mkdir, directory, 0o755)
attempt("stat", fs.stat, path)
publish(3, ops=ops, size=fs.fstat(fd).st_size)
gate("repoke")
read_probe(4, probes[1])
gate("resume")
del ops[:]
attempt("write", fs.write, fd, tail, len(base))
attempt("fsync", fs.fsync, fd, 0)
publish(5, ops=ops, size=fs.fstat(fd).st_size)
gate("finish")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(6, closed=True, **closing)
`

// cephFSProbeWriter writes files through a mount of its own and prints its
// client ID. Arguments: the filesystem, the hex of the bytes and the files.
const cephFSProbeWriter = `
import cephfs, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    for path in sys.argv[3:]:
        fd = fs.open(path.encode(), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        fs.write(fd, bytes.fromhex(sys.argv[2]), 0)
        fs.fsync(fd, 0)
        fs.close(fd)
    print(json.dumps({"client": fs.get_instance_id()}))
finally:
    fs.unmount()
    fs.shutdown()
`

// cephFSDataPool returns the pool of an inode's layout from the MDS's
// "dump inode".
func cephFSDataPool(t *testing.T, ctx context.Context, m *cephFSSingleMDS, inode uint64) int64 {
	t.Helper()
	out, err := m.cluster.Ceph(ctx, "tell", "mds."+m.mds.Name, "dump", "inode", strconv.FormatUint(inode, 10))
	var dump struct {
		Layout struct {
			Pool *int64 `json:"pool_id"`
		} `json:"layout"`
	}
	if err != nil || json.Unmarshal(out, &dump) != nil || dump.Layout.Pool == nil {
		t.Fatalf("dump inode %d: %v: %.512s", inode, err, out)
	}
	return *dump.Layout.Pool
}

// waitPoolFull observes, bounded, the current OSDMap until pool's "full"
// flag is full, and returns that epoch.
func waitPoolFull(t *testing.T, ctx context.Context, cluster *ceph.Container, pool int64, full bool) uint32 {
	t.Helper()
	seen, err := observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (uint32, bool, error) {
		dump, err := cephfsOSDMapAt(probeCtx, cluster, 0)
		if err != nil {
			return 0, false, err
		}
		flagged, found := dump.poolFull(pool)
		if !found {
			return dump.Epoch, false, fmt.Errorf("OSDMap e%d has no pool %d", dump.Epoch, pool)
		}
		return dump.Epoch, flagged == full, nil
	})
	if err != nil {
		t.Fatalf("pool %d full=%t not observed (last e%d): %v", pool, full, seen.Value, err)
	}
	return seen.Value
}

// testCephFSOSDFullRetainedClient fills the OSDs of a live client's file
// while the client keeps its mount and fd. Full ratios of 0.0001, 0.0002
// and 0.0003, through override.SetFullRatios, make the OSDs report themselves
// full, and the MON flags the file's data pool full in OSDMap epoch F. Once
// the client holds epoch F (it reads a file that another client wrote, and
// the OSD shares the map with it), its write returns ENOSPC (28) and its
// file keeps the base size; fsync, mkdir and stat, which only need the MDS,
// return 0. After Restore the MON clears the flag; once the client holds
// that map (it reads a second such file), the same client, without
// remounting, writes and fsyncs the tail with 0, a client in another
// container reads every byte, and the client keeps its session and is not
// blocklisted.
func testCephFSOSDFullRetainedClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	const path, workdir = "/filled", "/tmp/filled"
	base, tail := []byte("written before the OSDs filled"), []byte(" and after they drained")
	asok := workdir + "/client.asok"
	const probe, reprobe = "/probe", "/reprobe"
	probeBytes := []byte("written by another client")
	actor := startCephFSActor(t, ctx, m.cluster, m.client, cephFSFullActor, m.fs.FilesystemName, workdir, asok, path, "/made-while-full", hex.EncodeToString(base), hex.EncodeToString(tail), probe, reprobe)
	opened := actor.receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 || opened.Size != int64(len(base)) {
		t.Fatalf("receipt 1 = %+v", opened)
	}
	session := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0)
	pool := cephFSDataPool(t, ctx, m, opened.Inode)
	receipt := newRetainedClientReceipt(opened.Client, opened.Inode, pool)
	// The deferred line runs on every outcome, before the cleanup restores the
	// full ratios.
	defer func() { t.Log(receipt.line()) }()
	mapSources := retainedMapSources(t, m.cluster, m.client, asok)
	notFull := waitPoolFull(t, ctx, m.cluster, pool, false)
	receipt.NotFullEpoch = &notFull
	receipt.phase("pool-not-full")
	writeCtx, stopWrite := context.WithTimeout(ctx, 2*time.Minute)
	written, err := m.cluster.RunNativeCommand(writeCtx, toolkitContainer(t, m.cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSProbeWriter, m.fs.FilesystemName, hex.EncodeToString(probeBytes), probe, reprobe}, Timeout: 2 * time.Minute,
		MaxStdout: 4096, MaxStderr: 64 << 10, Label: "CephFS probe writer",
	})
	stopWrite()
	if err != nil || written.ExitCode != 0 || !written.Complete() {
		t.Fatalf("probe writer: exit=%d err=%v stderr=%.2048s", written.ExitCode, err, written.Stderr)
	}

	fill, err := override.SetFullRatios(ctx, m.cluster, ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .0003})
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := fill.Restore(restoreCtx); err != nil {
			t.Errorf("restore full ratios: %v", err)
		}
	})
	if err != nil {
		t.Fatal("apply full ratios:", err)
	}
	receipt.phase("ratios-applied")
	full := waitPoolFull(t, ctx, m.cluster, pool, true)
	receipt.FullEpoch = &full
	receipt.phase("pool-full")
	health, err := observeCluster(m.cluster).HealthDetails(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := health.Checks["OSD_FULL"]; !ok {
		t.Fatalf("pool %d is full in e%d without OSD_FULL: %+v", pool, full, health.Checks)
	}
	unprompted, err := clientAdminStatus(ctx, m.cluster, m.client, asok)
	if err != nil {
		t.Fatal(err)
	}
	receipt.unpromptedStatus(unprompted)
	actor.open(t, ctx, "poke")
	if poked := actor.receipt(t, ctx, 2); poked.Full != hex.EncodeToString(probeBytes) {
		t.Fatalf("poke receipt = %+v", poked)
	}
	receipt.phase("poke-completed")
	receipt.beginMapEvidence(ctx, "full map", mapSources)
	if err := receipt.waitMap(ctx, "full map", full, true, time.Minute, 40*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (cephFSClientStatus, error) {
		return clientAdminStatus(probeCtx, m.cluster, m.client, asok)
	}); err != nil {
		receipt.captureMapEvidence(ctx, "full map", mapSources)
		t.Fatal(err)
	}
	receipt.phase("full-map-held")

	actor.open(t, ctx, "full")
	whileFull := actor.receipt(t, ctx, 3)
	if errnos, want := coupledOps(t, whileFull), map[string]int{"write": 28, "fsync": 0, "mkdir": 0, "stat": 0}; !maps.Equal(errnos, want) || whileFull.Size != int64(len(base)) {
		t.Fatalf("client calls while the pool is full: %v size %d, want %v size %d", errnos, whileFull.Size, want, len(base))
	}

	receipt.phase("while-full")
	receipt.RestoreStarted = true
	if err := fill.Restore(ctx); err != nil {
		receipt.RestoreError = err.Error()
		t.Fatal("restore full ratios:", err)
	}
	drained := waitPoolFull(t, ctx, m.cluster, pool, false)
	receipt.DrainedEpoch = &drained
	receipt.phase("pool-drained")
	actor.open(t, ctx, "repoke")
	if poked := actor.receipt(t, ctx, 4); poked.Full != hex.EncodeToString(probeBytes) {
		t.Fatalf("repoke receipt = %+v", poked)
	}
	receipt.phase("repoke-completed")
	receipt.beginMapEvidence(ctx, "drained map", mapSources)
	if err := receipt.waitMap(ctx, "drained map", drained, true, time.Minute, 40*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (cephFSClientStatus, error) {
		return clientAdminStatus(probeCtx, m.cluster, m.client, asok)
	}); err != nil {
		receipt.captureMapEvidence(ctx, "drained map", mapSources)
		t.Fatal(err)
	}
	receipt.phase("drained-map-held")
	actor.open(t, ctx, "resume")
	resumed := actor.receipt(t, ctx, 5)
	if errnos, want := coupledOps(t, resumed), map[string]int{"write": 0, "fsync": 0}; !maps.Equal(errnos, want) || resumed.Size != int64(len(base)+len(tail)) {
		t.Fatalf("client calls after the restore: %v size %d, want %v size %d", errnos, resumed.Size, want, len(base)+len(tail))
	}

	whole := append(slices.Clone(base), tail...)
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := m.cluster.RunNativeCommand(readCtx, toolkitContainer(t, m.cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSIndependentReader, m.fs.FilesystemName, path}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Data   string `json:"data"`
	}
	if err != nil || read.ExitCode != 0 || !read.Complete() || json.Unmarshal([]byte(lastJSONLine(string(read.Stdout))), &independent) != nil {
		t.Fatalf("independent reader: exit=%d err=%v out=%.512s stderr=%.2048s", read.ExitCode, err, read.Stdout, read.Stderr)
	}
	if independent.Inode != opened.Inode || independent.Data != hex.EncodeToString(whole) || independent.Client == opened.Client {
		t.Fatalf("independent reader: %+v, want inode %d with %d bytes", independent, opened.Inode, len(whole))
	}
	if retained := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0); retained.Address != session.Address || retained.PID != session.PID {
		t.Fatalf("client session %+v, before %+v", retained, session)
	}
	entries, err := observeCluster(m.cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("client %s was blocklisted: %+v", session.Address, entries)
	}
	actor.open(t, ctx, "finish")
	if closed := actor.receipt(t, ctx, 6); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.wait(t, ctx)
	receipt.phase("finished")
	t.Logf("CEPHFS_OSD_FULL_RECEIPT %s client.%d inode=%d data pool %d: not full in e%d; full ratios 0.0001/0.0002/0.0003 flagged the pool full in e%d with OSD_FULL; the client held e%d before it read another client's file; with the full map the client's write=28 fsync=0 mkdir=0 stat=0, size stayed %d; after Restore the flag cleared in e%d and the same fd wrote and fsynced with 0 (size %d); reader client.%d read every byte; session kept, not blocklisted",
		m.version, opened.Client, opened.Inode, pool, notFull, full, unprompted.OSDEpoch, len(base), drained, len(whole), independent.Client)
}
