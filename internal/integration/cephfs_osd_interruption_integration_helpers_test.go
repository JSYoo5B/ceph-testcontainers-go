//go:build all || integration

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSInterruptedWriter keeps one mount with an admin socket and one
// O_RDWR fd. It writes and fsyncs the base bytes and publishes receipt 1. At
// the "write" gate it writes the tail behind them and publishes receipt 2
// with the size its fd reports and how long the write took; then it fsyncs
// once, without a retry, and publishes receipt 3 with the errno and how long
// the fsync took. At "finish" it reads the whole file through the same fd,
// stats a directory that another client made, closes the fd and unmounts,
// and publishes receipt 4. Arguments after the common ones: the admin socket,
// the file, the directory, the hex of the base and of the tail.
const cephFSInterruptedWriter cephFSActorScript = cephFSActorHelpers + `
asok, path, directory = sys.argv[4], sys.argv[5].encode(), sys.argv[6].encode()
base, tail = bytes.fromhex(sys.argv[7]), bytes.fromhex(sys.argv[8])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("write")
started = time.monotonic()
fs.write(fd, tail, len(base))
publish(2, size=fs.fstat(fd).st_size, elapsed_ms=int((time.monotonic() - started) * 1000))
started = time.monotonic()
result = {}
try:
    fs.fsync(fd, 0)
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
publish(3, **result)
gate("finish")
closing = {}
try:
    stat = fs.fstat(fd)
    closing.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size + 1).hex())
    closing.update(dir_inode=fs.stat(directory).st_ino)
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// cephFSJournalWaiter is a second live client with an admin socket. It
// mounts and publishes receipt 1. At the "mkdir" gate it makes one
// directory, once, without a retry, and publishes receipt 2 with the errno,
// how long mkdir took and the new directory's inode. It unmounts after
// "finish" and publishes receipt 3. The mkdir is the first request of its
// session that creates an inode, so the MDS allocates the inode for it and
// sends no early reply: the call returns only after the MDS's journal entry
// is committed. A later creation would use an inode that the session has
// preallocated, and the MDS would reply early. Arguments after the common
// ones: the admin socket and the directory.
const cephFSJournalWaiter cephFSActorScript = cephFSActorHelpers + `
asok, directory = sys.argv[4], sys.argv[5].encode()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
publish(1, client=fs.get_instance_id())
gate("mkdir")
started = time.monotonic()
result = {}
try:
    fs.mkdir(directory, 0o755)
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
if "errno" not in result:
    result["dir_inode"] = fs.stat(directory).st_ino
publish(2, **result)
gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(3, closed=True, **closing)
`

// objecterOp is one operation in flight in an Objecter's objecter_requests:
// the OSD it was sent to, its object and locator ("@<pool>", with ";<ns>"
// or ":<key>" behind), how long ago it was sent and its OSD sub-operations.
type objecterOp struct {
	TID      uint64   `json:"tid"`
	OSD      int      `json:"osd"`
	Object   string   `json:"object_id"`
	Locator  string   `json:"object_locator"`
	Age      float64  `json:"age"`
	Attempts int      `json:"attempts"`
	OSDOps   []string `json:"osd_ops"`
}

// pool returns the pool ID of the op's object locator.
func (op objecterOp) pool() (int64, bool) {
	rest, found := strings.CutPrefix(op.Locator, "@")
	if !found {
		return 0, false
	}
	if end := strings.IndexAny(rest, ";:"); end >= 0 {
		rest = rest[:end]
	}
	pool, err := strconv.ParseInt(rest, 10, 64)
	return pool, err == nil
}

// parseObjecterOps reads the OSD operations of an objecter_requests dump. A
// dump without an "ops" array is an error; an empty array is an answer.
func parseObjecterOps(data []byte) ([]objecterOp, error) {
	var dump struct {
		Ops *[]objecterOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}
	if dump.Ops == nil {
		return nil, fmt.Errorf("objecter_requests has no ops")
	}
	return *dump.Ops, nil
}

// objecterOpsOn returns the ops on pool whose object starts with prefix.
func objecterOpsOn(ops []objecterOp, pool int64, prefix string) []objecterOp {
	var matched []objecterOp
	for _, op := range ops {
		if id, ok := op.pool(); ok && id == pool && strings.HasPrefix(op.Object, prefix) {
			matched = append(matched, op)
		}
	}
	return matched
}

// osdObjectMap is "ceph osd map" for one object.
type osdObjectMap struct {
	PoolID        int64  `json:"pool_id"`
	PGID          string `json:"pgid"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}

func cephOSDMapObject(t *testing.T, ctx context.Context, cluster *ceph.Container, pool, object string) osdObjectMap {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "map", pool, object, "--format", "json")
	var mapped osdObjectMap
	if err != nil || json.Unmarshal(out, &mapped) != nil || len(mapped.Acting) == 0 {
		t.Fatalf("osd map %s %s: %v: %.512s", pool, object, err, out)
	}
	return mapped
}

// mdsRequestAge is an MDS op with the age that dump_ops_in_flight prints.
type mdsRequestAge struct {
	mdsRequestOp
	Age float64 `json:"age"`
}

// interruptedState is one look at both clients and the MDS while the OSD
// is frozen.
type interruptedState struct {
	write    objecterOp
	hasWrite bool
	journal  []objecterOp
	mkdir    mdsRequestAge
	hasMkdir bool
	pending  []clientMDSRequest
	raw      string
}

// objecterRequests reads objecter_requests through an admin socket in ctr,
// or from the MDS with tell when asok is empty.
func objecterRequests(ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container, asok, mds string) ([]objecterOp, string, error) {
	var out []byte
	if asok == "" {
		var err error
		if out, err = cluster.Ceph(ctx, "tell", "mds."+mds, "objecter_requests"); err != nil {
			return nil, "", fmt.Errorf("mds.%s objecter_requests: %w", mds, err)
		}
	} else {
		result, err := cluster.RunNativeCommand(ctx, ctr, ceph.NativeCommand{
			Argv: []string{"ceph", "--admin-daemon", asok, "objecter_requests"}, Timeout: 30 * time.Second,
			MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client objecter_requests",
		})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			return nil, "", fmt.Errorf("client objecter_requests: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
		}
		out = result.Stdout
	}
	ops, err := parseObjecterOps(out)
	if err != nil {
		return nil, "", fmt.Errorf("objecter_requests: %v: %.1024s", err, out)
	}
	return ops, string(out), nil
}

// testCephFSOSDInterruption freezes, with fault.Pause and nodown held
// through OverrideOSDFlag, the acting primary OSD of the first object of a
// live client's file, while a second live client in another container waits
// to make a directory. Every PG of the two-OSD cluster includes that OSD, so
// both the file's data and the MDS's journal stall at it. The writer's
// buffered write returns, and its fsync does not: its own objecter_requests
// holds the write of the file's object to the frozen OSD. The second
// client's mkdir does not return either: the MDS holds it in
// dump_ops_in_flight without journal_committed, the client's mds_requests
// holds it, and the MDS's own objecter_requests holds writes of its journal
// objects (200.*) in the metadata pool. Both are seen held for at least 5 s.
// Both sessions stay open and neither client is blocklisted. After Resume
// the same fsync and the same mkdir return 0, the MDS's history shows the
// mkdir committed and replied, and only then is nodown restored. The writer
// reads every byte through its original fd and finds the new directory,
// another client reads every byte, the MDS keeps its GID, and the sessions
// are the same.
func testCephFSOSDInterruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	const path, directory = "/interrupted", "/held"
	const writerDir, waiterDir = "/tmp/osd-writer", "/tmp/osd-waiter"
	writerAsok, waiterAsok := writerDir+"/client.asok", waiterDir+"/client.asok"
	base, tail := []byte("fsynced before the OSD froze"), []byte(" and written while it was frozen")
	writer := startCephFSActor(t, ctx, m.cluster, m.client, cephFSInterruptedWriter, m.fs.FilesystemName, writerDir, writerAsok, path, directory, hex.EncodeToString(base), hex.EncodeToString(tail))
	opened := writer.receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 || opened.Size != int64(len(base)) {
		t.Fatalf("writer receipt 1 = %+v", opened)
	}
	waiter := startCephFSActor(t, ctx, m.cluster, toolkitContainer(t, m.cluster), cephFSJournalWaiter, m.fs.FilesystemName, waiterDir, waiterAsok, directory)
	waiting := waiter.receipt(t, ctx, 1)
	if waiting.Client == 0 || waiting.Client == opened.Client {
		t.Fatalf("waiter receipt 1 = %+v, writer %+v", waiting, opened)
	}
	writerSession := cephFSRetainedSession(t, ctx, m.fs, opened.Client, 0)
	waiterSession := cephFSRetainedSession(t, ctx, m.fs, waiting.Client, 0)

	object := fmt.Sprintf("%x.00000000", opened.Inode)
	placed := cephOSDMapObject(t, ctx, m.cluster, m.fs.DataPool, object)
	journal := cephOSDMapObject(t, ctx, m.cluster, m.fs.MetadataPool, "200.00000000")
	if pool := cephFSDataPool(t, ctx, m, opened.Inode); pool != placed.PoolID {
		t.Fatalf("inode %d lives in pool %d, osd map names %d", opened.Inode, pool, placed.PoolID)
	}
	var target *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, m.cluster) {
		if osd.ID == placed.ActingPrimary {
			target = osd
		}
	}
	if target == nil || len(clusteraccess.OSDs(t, m.cluster)) != 2 || !slices.Contains(journal.Acting, target.ID) {
		t.Fatalf("osd.%d (acting %v) is not an owned OSD that also holds the journal (acting %v): %d OSDs", placed.ActingPrimary, placed.Acting, journal.Acting, len(clusteraccess.OSDs(t, m.cluster)))
	}
	if ops, raw, err := objecterRequests(ctx, m.cluster, m.client, writerAsok, ""); err != nil || len(objecterOpsOn(ops, placed.PoolID, "")) != 0 {
		t.Fatalf("writer has data ops in flight before the pause: %v: %s", err, raw)
	}

	hold, err := m.cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoDown, true)
	if hold != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := hold.Restore(restoreCtx); err != nil {
				t.Errorf("restore nodown: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal("hold nodown:", err)
	}
	pause, err := fault.Pause(ctx, m.cluster, target.Container)
	if pause != nil {
		t.Cleanup(func() {
			resumeCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := pause.Resume(resumeCtx); err != nil {
				t.Errorf("resume osd.%d: %v", target.ID, err)
			}
		})
	}
	if err != nil {
		t.Fatal("pause OSD:", err)
	}
	paused := time.Now()

	writer.open(t, ctx, "write")
	buffered := writer.receipt(t, ctx, 2)
	if buffered.Size != int64(len(base)+len(tail)) {
		t.Fatalf("writer receipt 2 = %+v", buffered)
	}
	waiter.open(t, ctx, "mkdir")

	// Both calls are held at the frozen OSD: the writer's data op and the
	// MDS's journal writes stay in flight, and the MDS and the waiter hold
	// the mkdir, until each has been held for 5 s.
	const held = 5.0
	seen, err := observe(ctx, 2*time.Minute, 40*time.Second, time.Second, func(probeCtx context.Context) (interruptedState, bool, error) {
		var state interruptedState
		clientOps, clientRaw, err := objecterRequests(probeCtx, m.cluster, m.client, writerAsok, "")
		if err != nil {
			return state, false, err
		}
		if writes := objecterOpsOn(clientOps, placed.PoolID, object); len(writes) > 0 {
			state.write, state.hasWrite = writes[0], true
		}
		mdsOps, mdsRaw, err := objecterRequests(probeCtx, m.cluster, nil, "", m.mds.Name)
		if err != nil {
			return state, false, err
		}
		state.journal = objecterOpsOn(mdsOps, journal.PoolID, "200.")
		inFlight, err := m.cluster.Ceph(probeCtx, "tell", "mds."+m.mds.Name, "dump_ops_in_flight")
		if err != nil {
			return state, false, err
		}
		var dump struct {
			Ops []mdsRequestAge `json:"ops"`
		}
		if err := json.Unmarshal(inFlight, &dump); err != nil {
			return state, false, fmt.Errorf("dump_ops_in_flight: %v: %.512s", err, inFlight)
		}
		for _, op := range dump.Ops {
			if _, found := findMDSRequest([]mdsRequestOp{op.mdsRequestOp}, waiting.Client, "mkdir", 0); found {
				state.mkdir, state.hasMkdir = op, true
			}
		}
		requests, err := m.cluster.RunNativeCommand(probeCtx, toolkitContainer(t, m.cluster), ceph.NativeCommand{
			Argv: []string{"ceph", "--admin-daemon", waiterAsok, "mds_requests"}, Timeout: 30 * time.Second,
			MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client mds_requests",
		})
		if err != nil || requests.ExitCode != 0 || !requests.Complete() {
			return state, false, fmt.Errorf("waiter mds_requests: exit=%d err=%v stderr=%.512s", requests.ExitCode, err, requests.Stderr)
		}
		if state.pending, err = parseClientMDSRequests(requests.Stdout); err != nil {
			return state, false, fmt.Errorf("waiter mds_requests: %v: %.1024s", err, requests.Stdout)
		}
		state.raw = fmt.Sprintf("writer objecter %.2048s; mds objecter %.2048s; mds mkdir %q at %q age %.1f events %v; waiter requests %+v",
			clientRaw, mdsRaw, state.mkdir.Description, state.mkdir.TypeData.FlagPoint, state.mkdir.Age, state.mkdir.events(), state.pending)
		return state, state.hasWrite && state.write.Age >= held && len(state.journal) > 0 && state.hasMkdir && state.mkdir.Age >= held, nil
	})
	if err != nil {
		t.Fatalf("the frozen OSD does not hold the writer's fsync and the mkdir: %s: %v", seen.Value.raw, err)
	}
	state := seen.Value
	if state.write.OSD != target.ID || !slices.ContainsFunc(state.write.OSDOps, func(op string) bool { return strings.HasPrefix(op, "write ") }) {
		t.Fatalf("the writer's op on %s is not a write to osd.%d: %+v", object, target.ID, state.write)
	}
	if events := state.mkdir.events(); slices.ContainsFunc(events, func(event string) bool {
		return event == "replying" || strings.HasPrefix(event, "journal_committed")
	}) || requestSafety(state.mkdir.mdsRequestOp) != "no-early-reply" {
		t.Fatalf("the MDS answered the held mkdir: %s", state.raw)
	}
	if !slices.ContainsFunc(state.pending, func(request clientMDSRequest) bool {
		return request.TID == state.mkdir.TypeData.ReqID.TID && request.Op == "mkdir" && !bool(request.GotUnsafe)
	}) {
		t.Fatalf("the waiter does not hold its mkdir: %s", state.raw)
	}
	if writer.published(ctx, 3) || waiter.published(ctx, 2) {
		t.Fatalf("a held call returned while the OSD was frozen: %s", state.raw)
	}
	for _, session := range []struct {
		before cephfs.Session
		client uint64
	}{{writerSession, opened.Client}, {waiterSession, waiting.Client}} {
		if during := cephFSRetainedSession(t, ctx, m.fs, session.client, 0); during.Address != session.before.Address {
			t.Fatalf("client.%d session while the OSD is frozen %+v, before %+v", session.client, during, session.before)
		}
	}
	t.Logf("CEPHFS_OSD_INTERRUPTION held: %s", state.raw)

	if err := pause.Resume(ctx); err != nil {
		t.Fatal("resume OSD:", err)
	}
	frozen := time.Since(paused)
	synced := writer.receipt(t, ctx, 3)
	if synced.Errno != 0 || synced.Error != "" || time.Duration(synced.ElapsedMS)*time.Millisecond < held*time.Second {
		t.Fatalf("writer's fsync after the OSD resumed: %+v", synced)
	}
	made := waiter.receipt(t, ctx, 2)
	if made.Errno != 0 || made.Error != "" || made.DirInode == 0 || time.Duration(made.ElapsedMS)*time.Millisecond < held*time.Second {
		t.Fatalf("waiter's mkdir after the OSD resumed: %+v", made)
	}
	// The MDS records the request in its history once it is cleaned up,
	// which can follow the reply.
	tid := state.mkdir.TypeData.ReqID.TID
	historic, err := observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (mdsRequestOp, bool, error) {
		out, err := m.cluster.Ceph(probeCtx, "tell", "mds."+m.mds.Name, "dump_historic_ops")
		if err != nil {
			return mdsRequestOp{}, false, err
		}
		ops, err := parseMDSRequestOps(out)
		if err != nil {
			return mdsRequestOp{}, false, fmt.Errorf("dump_historic_ops: %v: %.1024s", err, out)
		}
		op, found := findMDSRequest(ops, waiting.Client, "mkdir", tid)
		return op, found, nil
	})
	if err != nil || requestSafety(historic.Value) != "no-early-reply" || !journaledThenReplied(historic.Value.events()) {
		t.Fatalf("the MDS's history lacks the committed mkdir tid %d: %+v: %v", tid, historic.Value, err)
	}
	committed := historic.Value
	if err := hold.Restore(ctx); err != nil {
		t.Fatal("restore nodown:", err)
	}
	if err := m.cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	writer.open(t, ctx, "finish")
	whole := append(append([]byte(nil), base...), tail...)
	closed := writer.receipt(t, ctx, 4)
	if !closed.Closed || closed.CloseErrno != 0 || closed.Inode != opened.Inode || closed.Full != hex.EncodeToString(whole) || closed.DirInode != made.DirInode {
		t.Fatalf("writer's closing receipt = %+v, want inode %d with %d bytes and directory %d", closed, opened.Inode, len(whole), made.DirInode)
	}
	writer.wait(t, ctx)
	waiter.open(t, ctx, "finish")
	if done := waiter.receipt(t, ctx, 3); !done.Closed || done.CloseErrno != 0 {
		t.Fatalf("waiter's closing receipt = %+v", done)
	}
	waiter.wait(t, ctx)

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
	if independent.Inode != opened.Inode || independent.Data != hex.EncodeToString(whole) || independent.Client == opened.Client || independent.Client == waiting.Client {
		t.Fatalf("independent reader: %+v, want inode %d with %d bytes", independent, opened.Inode, len(whole))
	}
	status, err := m.fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 || status.Active[0].GID != m.mds.GID {
		t.Fatalf("mds.%s GID %d was replaced: %+v, %v", m.mds.Name, m.mds.GID, status, err)
	}
	entries, err := m.cluster.BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []cephfs.Session{writerSession, waiterSession} {
		if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
			t.Fatalf("client.%d %s was blocklisted: %+v", session.ID, session.Address, entries)
		}
	}
	t.Logf("CEPHFS_OSD_INTERRUPTION_RECEIPT %s mds.%s gid=%d: writer client.%d inode=%d object %s in pg %s acting %v; journal 200.00000000 acting %v; osd.%d paused with nodown for %s; the buffered write returned in %d ms; the writer's op tid %d (%v) to osd.%d was held %.1f s; the MDS held %d journal ops on the metadata pool and waiter client.%d's mkdir tid %d at %q for %.1f s; both sessions stayed open; after Resume the fsync returned 0 after %d ms and the mkdir 0 after %d ms (inode %d), history %v; the writer read %d bytes through its fd and found the directory; reader client.%d read every byte; no blocklist entry, same MDS GID",
		m.version, m.mds.Name, m.mds.GID, opened.Client, opened.Inode, object, placed.PGID, placed.Acting, journal.Acting, target.ID, frozen.Round(time.Millisecond),
		buffered.ElapsedMS, state.write.TID, state.write.OSDOps, state.write.OSD, state.write.Age, len(state.journal), waiting.Client, state.mkdir.TypeData.ReqID.TID,
		state.mkdir.TypeData.FlagPoint, state.mkdir.Age, synced.ElapsedMS, made.ElapsedMS, made.DirInode, committed.events(), len(whole), independent.Client)
}

// journaledThenReplied reports whether a request's events have its journal
// commit before its reply.
func journaledThenReplied(events []string) bool {
	committed := slices.IndexFunc(events, func(event string) bool { return strings.HasPrefix(event, "journal_committed") })
	replied := slices.Index(events, "replying")
	return committed >= 0 && replied > committed
}
