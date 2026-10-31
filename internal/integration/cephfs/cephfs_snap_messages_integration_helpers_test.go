//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// snapMessage is one client_snap message that a retained client decoded from
// the wire. Op is the message kind ("update", "split", "destroy") and Split
// the realm inode it names.
type snapMessage struct {
	Line  int    `json:"line"`
	Op    string `json:"op"`
	Split uint64 `json:"split"`
	Text  string `json:"-"`
}

var snapHandleLine = regexp.MustCompile(`client\.\d+ handle_snap client_snap\((update|split|destroy) split=0x([0-9a-f]+) tracelen=\d+\)`)

// parseSnapMessages picks the client_snap messages a libcephfs client decoded
// out of its debug_client 10 log, with the realm inode each names.
func parseSnapMessages(lines []string) []snapMessage {
	var messages []snapMessage
	for index, line := range lines {
		m := snapHandleLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		split, _ := strconv.ParseUint(m[2], 16, 64)
		messages = append(messages, snapMessage{Line: index, Op: m[1], Split: split, Text: line})
	}
	return messages
}

// cephFSSnapWireActor is cephFSRealmActor with client debug raised and its
// log written to a file: it keeps one mount and one O_RDWR fd open with an
// admin socket, writes and fsyncs base bytes and publishes receipt 1 with
// the client ID and the file and directory inodes, overwrites through the fd
// at the "overwrite" gate and publishes receipt 2, and after the "finish"
// gate unmounts and publishes receipt 3. Arguments after the common ones:
// directory, file, base hex, new hex, admin socket, log path.
const cephFSSnapWireActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
directory, path = sys.argv[4].encode(), sys.argv[5].encode()
base, new = bytes.fromhex(sys.argv[6]), bytes.fromhex(sys.argv[7])
asok, logfile = sys.argv[8], sys.argv[9]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
for key, value in (("client_mount_timeout", "30"), ("admin_socket", asok), ("log_file", logfile),
                   ("log_to_file", "true"), ("log_to_stderr", "false"), ("debug_ms", "1"), ("debug_client", "10")):
    fs.conf_set(key, value)
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

// snapMessageReceipt is the evidence of the client_snap message one retained
// client received as a snapshot of its realm was created.
type snapMessageReceipt struct {
	Release    string        `json:"release"`
	Platform   string        `json:"platform"`
	Run        string        `json:"run,omitempty"`
	Client     uint64        `json:"client"`
	DirInode   uint64        `json:"dir_inode"`
	SnapID     uint64        `json:"snap_id"`
	Messages   []snapMessage `json:"messages"`
	FinalSnaps []uint64      `json:"final_snaps"`
	FinalSeq   uint64        `json:"final_seq"`
}

// testCephFSSnapMessages has a retained libcephfs client hold a directory
// open at debug_client 10 while an independent administrator snapshots it.
// Its dump_cache, polled until its derived SnapContext holds the snapshot,
// is the reliable readiness gate. The test then decodes the CEPH_MSG_CLIENT_SNAP
// messages from the client's log and requires one that names the directory's
// realm, joining the wire message to the SnapContext the client then held
// (read from dump_cache). The snapshot is also read back independently to
// confirm it is the live realm's, not a stale one.
func testCephFSSnapMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	toolkit := harness.ToolkitContainer(t, m.Cluster)
	fsName := m.FS.FilesystemName
	if _, err := m.Cluster.Ceph(ctx, "fs", "set", fsName, "allow_new_snaps", "true"); err != nil {
		t.Fatal("allow_new_snaps:", err)
	}
	receipt := snapMessageReceipt{Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if run := os.Getenv("GITHUB_RUN_ID"); run != "" {
		receipt.Run = run + "/" + os.Getenv("GITHUB_RUN_ATTEMPT")
	}
	if raw, err := m.Cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "version"); err == nil {
		var version map[string]string
		if json.Unmarshal(raw, &version) == nil {
			receipt.Release = version["version"]
		}
	}
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("CEPHFS_SNAP_MESSAGES_RECEIPT %s", data)
	}()

	const (
		directory = "/snap-messages"
		leaf      = "file"
		workdir   = "/tmp/snap-messages-actor"
		snapName  = "snap-msg"
	)
	asok := workdir + "/client.asok"
	logfile := workdir + "/client.log"
	base := []byte("snap-message-base\x00\xff")
	updated := []byte("snap-message-updated\x01\xfe")
	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSSnapWireActor, fsName, workdir,
		directory, directory+"/"+leaf, hex.EncodeToString(base), hex.EncodeToString(updated), asok, logfile)
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.DirInode == 0 {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	receipt.Client, receipt.DirInode = baseline.Client, baseline.DirInode

	t.Cleanup(func() {
		cleanCtx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		if _, err := runCephFSRealmAdmin(cleanCtx, m.Cluster, toolkit, "rmsnap", fsName, directory, snapName, leaf); err != nil {
			t.Errorf("remove snapshot: %v", err)
		}
	})

	if _, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "snap", fsName, directory, snapName, leaf); err != nil {
		t.Fatal(err)
	}
	snapID, _, err := boundedPoll(ctx, realPollClock, time.Minute, time.Second, func(attemptCtx context.Context) (uint64, bool, error) {
		raw, err := m.Cluster.Ceph(attemptCtx, "tell", "mds."+m.MDS.Name, "dump", "snaps")
		if err != nil {
			return 0, false, nil
		}
		id, err := mdsSnapshotID(raw, baseline.DirInode, snapName)
		return id, err == nil, nil
	})
	if err != nil {
		t.Fatalf("snapshot ID of %q: %v", snapName, err)
	}
	receipt.SnapID = snapID

	// The MDS pushes client_snap to clients with caps in the realm; nothing
	// orders that push against the creation, so poll the retained client's
	// own dump_cache until its derived SnapContext holds the snapshot. This
	// is the reliable readiness gate: once the context holds it, the client
	// has received and logged the client_snap that delivered it.
	var derived clientSnapContext
	released, _, err := boundedPoll(ctx, realPollClock, 90*time.Second, time.Second, func(attemptCtx context.Context) (bool, bool, error) {
		data, err := queryClientDumpCache(attemptCtx, m.Cluster, m.Client, asok)
		if err != nil {
			return false, false, nil
		}
		ok, ctx, _ := snapContextGate(data, baseline.Inode, snapID)
		if ok {
			derived = ctx
		}
		return ok, ok, nil
	})
	if err != nil || !released {
		t.Fatalf("the client's realm never showed snapshot %x: %v", snapID, err)
	}
	receipt.FinalSnaps, receipt.FinalSeq = derived.Snaps, derived.Seq

	clientLog, err := m.Cluster.RunNativeCommand(ctx, m.Client, ceph.NativeCommand{
		Argv: []string{"cat", logfile}, Timeout: time.Minute, MaxStdout: 64 << 20, MaxStderr: 16 << 10, Label: "snap client log",
	})
	if err != nil || clientLog.ExitCode != 0 || !clientLog.Complete() {
		t.Fatalf("read client log: exit %d, complete %t: %v", clientLog.ExitCode, clientLog.Complete(), err)
	}
	messages := parseSnapMessages(strings.Split(string(clientLog.Stdout), "\n"))
	receipt.Messages = messages
	// The client decoded a CEPH_MSG_CLIENT_SNAP that split off the directory's
	// own realm, and its resulting SnapContext (read from dump_cache above)
	// holds the created snapshot. The message is the wire evidence; the
	// context is the state it produced.
	if !slices.ContainsFunc(messages, func(mm snapMessage) bool { return mm.Op == "split" && mm.Split == baseline.DirInode }) {
		t.Fatalf("no client_snap split of the directory realm %d: %+v", baseline.DirInode, messages)
	}
	if !slices.Contains(derived.Snaps, snapID) {
		t.Fatalf("the client's SnapContext %v does not hold snapshot %d", derived.Snaps, snapID)
	}

	// The overwrite confirms the realm that the message named is the live
	// one, and an independent reader sees the snapshot's base bytes and the
	// head's new bytes.
	actor.Open(t, ctx, "overwrite")
	if over := actor.Receipt(t, ctx, 2); over.Errno != 0 || over.Full != hex.EncodeToString(updated) {
		t.Fatalf("overwrite receipt: %+v", over)
	}
	read, err := runCephFSRealmAdmin(ctx, m.Cluster, toolkit, "read", fsName, directory, snapName, leaf)
	if err != nil {
		t.Fatal(err)
	}
	if read.Head == nil || read.Snap == nil || read.Head.Data != hex.EncodeToString(updated) || read.Snap.Data != hex.EncodeToString(base) {
		t.Fatalf("independent read: head=%+v snapshot=%+v", read.Head, read.Snap)
	}
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt: %+v", closed)
	}
	actor.Wait(t, ctx)
}
