//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// CephFSActorScript is the Python source of an actor. Its own type keeps a
// filesystem name or path from being passed as the script by mistake.
type CephFSActorScript string

// CephFSActorHelpers starts every actor script: it reads the filesystem, the
// work directory and the run's token from argv and defines publish, which
// renames a receipt for one phase into place, and gate, which waits until the
// test writes the token into a named gate file.
const CephFSActorHelpers = `
import cephfs, json, os, sys, time
name, workdir, token = sys.argv[1], sys.argv[2], sys.argv[3]
def publish(phase, **fields):
    fields.update(token=token, phase=phase, pid=os.getpid())
    target = "%s/receipt-%d" % (workdir, phase)
    with open(target + ".tmp", "w") as out:
        json.dump(fields, out)
    os.rename(target + ".tmp", target)
def gate(name):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        try:
            with open(workdir + "/" + name) as opened:
                if opened.read() == token:
                    return
        except FileNotFoundError:
            pass
        time.sleep(0.2)
    sys.exit("gate %s did not open" % name)
`

// cephFSRetainedActor keeps one libcephfs mount and one O_RDWR fd open for
// its whole run. It writes and fsyncs the base bytes, publishes receipt 1 and
// waits at the "act" gate. Behind it, through the same fd and without
// mounting or opening again, it reads the base bytes, appends the tail,
// fsyncs, renames the file and sets a binary xattr, reads the whole file back
// through the fd, and publishes receipt 2.
// It closes everything after the "finish" gate and publishes receipt 3, with
// the errno of a close or unmount that failed, as for an evicted client.
// Every receipt and gate carries the run's token; receipts are renamed into
// place, so a reader never sees half of one. Arguments: filesystem, work
// directory, token, path, new path, the hex of the base bytes, the tail and
// the xattr value, and a rank to pin the file's directory to, or "-". The
// directory is created and pinned before the file is opened.
const cephFSRetainedActor CephFSActorScript = CephFSActorHelpers + `
path, renamed = sys.argv[4].encode(), sys.argv[5].encode()
base, tail, xattr = (bytes.fromhex(arg) for arg in sys.argv[6:9])
pin = sys.argv[9]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
if pin != "-":
    directory = os.path.dirname(path)
    fs.mkdir(directory, 0o755)
    fs.setxattr(directory, "ceph.dir.pin", pin.encode(), 0)
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("act")
result = {}
try:
    result["read"] = fs.read(fd, 0, len(base)).hex()
    fs.write(fd, tail, len(base))
    fs.fsync(fd, 0)
    fs.rename(path, renamed)
    fs.setxattr(renamed, "user.receipt", xattr, 0)
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

// CephFSIndependentReader reads a file and its xattr through a mount of its
// own, in another container and process than the actor, and prints them with
// its client ID; a missing xattr prints as null. It reads one byte more than
// the size to see extra data.
const CephFSIndependentReader = `
import cephfs, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    path = sys.argv[2].encode()
    stat = fs.stat(path)
    fd = fs.open(path, os.O_RDONLY, 0)
    data = fs.read(fd, 0, stat.st_size + 1)
    fs.close(fd)
    try:
        xattr = fs.getxattr(path, "user.receipt", 255).hex()
    except cephfs.Error:
        xattr = None
    print(json.dumps({"client": fs.get_instance_id(), "inode": stat.st_ino, "size": stat.st_size, "data": data.hex(), "xattr": xattr}))
finally:
    fs.unmount()
    fs.shutdown()
`

// CephFSActorReceipt is one receipt of cephFSRetainedActor. Python writes the
// integers exactly, and they decode into exact Go integers.
type CephFSActorReceipt struct {
	Token  string `json:"token"`
	Phase  int    `json:"phase"`
	PID    int    `json:"pid"`
	Client uint64 `json:"client"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
	Read   string `json:"read"`
	Full   string `json:"full"`
	Errno  int    `json:"errno"`
	Error  string `json:"error"`
	Closed bool   `json:"closed"`
	// ElapsedMS is how long the unsafe-window actor's operation took.
	ElapsedMS int64 `json:"elapsed_ms"`
	// Count is the number of records that the stream actor wrote.
	Count int64 `json:"count"`
	// CloseErrno is the errno of a failed close or unmount.
	CloseErrno int `json:"close_errno"`
	// Ops is the metadata recipe actor's per-operation table.
	Ops json.RawMessage `json:"ops"`
	// FSID is the cluster FSID that the admission actor's client reports.
	FSID string `json:"fsid"`
	// Addrs is the admission actor's own client address vector.
	Addrs string `json:"addrs"`
	// DirInode is the inode of a directory that an OSD interruption actor
	// made or found.
	DirInode uint64 `json:"dir_inode"`
	// Lock is the conflicting record lock that the fcntl actor's F_GETLK
	// reported.
	Lock *FcntlLockView `json:"lock"`
}

// FcntlLockView is a record lock that F_GETLK reported.
type FcntlLockView struct {
	Type  int   `json:"type"`
	Start int64 `json:"start"`
	Len   int64 `json:"len"`
	PID   int   `json:"pid"`
}

// CephFSActorRun is a supervised actor and its receipts.
type CephFSActorRun struct {
	cluster *ceph.Container
	client  testcontainers.Container
	workdir string
	Token   string
	cancel  context.CancelFunc
	Done    chan struct{}
	Result  ceph.NativeCommandResult
	Err     error
}

// StartCephFSActor starts the actor in client under RunNativeCommand and
// registers its cancellation and confirmed stop before anything can fail.
func StartCephFSActor(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, script CephFSActorScript, filesystem, workdir string, args ...string) *CephFSActorRun {
	t.Helper()
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	ExecOutput(t, ctx, client, "mkdir", "-p", workdir)
	actorCtx, cancel := context.WithCancel(ctx)
	run := &CephFSActorRun{cluster: cluster, client: client, workdir: workdir, Token: hex.EncodeToString(nonce), cancel: cancel, Done: make(chan struct{})}
	argv := append([]string{"python3", "-c", string(script), filesystem, workdir, run.Token}, args...)
	go func() {
		defer close(run.Done)
		run.Result, run.Err = cluster.RunNativeCommand(actorCtx, client, ceph.NativeCommand{
			Argv: argv, Timeout: 6 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "CephFS actor",
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.Done:
		case <-time.After(2 * time.Minute):
			t.Error("retained CephFS actor did not return after cancellation")
			return
		}
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(run.Err, &unstopped) {
			stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := unstopped.Stop(stopCtx); err != nil {
				t.Errorf("stop retained CephFS actor: %v", err)
			}
		}
	})
	return run
}

// exited describes how the actor ended, with its redacted stderr tail.
func (run *CephFSActorRun) exited(ctx context.Context) string {
	stderr := run.Result.Stderr
	if len(stderr) > 4096 {
		stderr = stderr[len(stderr)-4096:]
	}
	if redacted, err := diagnostics.Redact(ctx, run.cluster, stderr); err == nil {
		stderr = redacted
	}
	return fmt.Sprintf("exit=%d timed_out=%t err=%v stderr=%s", run.Result.ExitCode, run.Result.TimedOut, run.Err, stderr)
}

// receipt waits for receipt phase and rejects one of another run or phase.
// It fails at once when the actor ends without publishing it.
func (run *CephFSActorRun) Receipt(t *testing.T, ctx context.Context, phase int) CephFSActorReceipt {
	t.Helper()
	path := run.workdir + "/receipt-" + strconv.Itoa(phase)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, out, err := SessionExec(ctx, run.client, "cat", path)
		if err == nil && code == 0 {
			decoder := json.NewDecoder(strings.NewReader(out))
			decoder.DisallowUnknownFields()
			var receipt CephFSActorReceipt
			if err := decoder.Decode(&receipt); err != nil {
				t.Fatalf("receipt %d: %v: %q", phase, err, out)
			}
			if receipt.Token != run.Token || receipt.Phase != phase || receipt.PID <= 0 {
				t.Fatalf("receipt %d belongs to another run or phase: %+v", phase, receipt)
			}
			return receipt
		}
		select {
		case <-run.Done:
			t.Fatalf("actor ended without receipt %d: %s", phase, run.exited(ctx))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt %d did not appear", phase)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("receipt %d: %v", phase, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// open opens gate name for this run only.
func (run *CephFSActorRun) Open(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	ExecOutput(t, ctx, run.client, "sh", "-c", `printf %s "$1" > "$2.tmp" && mv "$2.tmp" "$2"`, "sh", run.Token, run.workdir+"/"+name)
}

// wait waits for the actor to end and requires a complete, successful run.
func (run *CephFSActorRun) Wait(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-run.Done:
	case <-ctx.Done():
		t.Fatal("actor did not end:", ctx.Err())
	}
	if run.Err != nil || run.Result.ExitCode != 0 || !run.Result.Complete() {
		t.Fatal("actor failed:", run.exited(ctx))
	}
}

// CephFSRawSession returns the native "session ls" entry of client on one MDS,
// whose fields the Session view leaves out, such as num_leases and
// num_completed_requests. Integers stay raw JSON numbers.
func CephFSRawSession(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string, client uint64) map[string]json.RawMessage {
	t.Helper()
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "session", "ls", "--format", "json")
	if err != nil {
		t.Fatal("session ls:", err)
	}
	var sessions []map[string]json.RawMessage
	if err := json.Unmarshal(out, &sessions); err != nil {
		t.Fatalf("session ls: %v: %.512s", err, out)
	}
	for _, session := range sessions {
		if string(session["id"]) == strconv.FormatUint(client, 10) {
			return session
		}
	}
	t.Fatalf("mds.%s has no session of client.%d", mds, client)
	return nil
}

// CephFSRawSessionSummary renders the raw counters that a log line keeps.
func CephFSRawSessionSummary(session map[string]json.RawMessage) string {
	var parts []string
	for _, field := range []string{"state", "num_caps", "num_leases", "num_completed_requests", "num_completed_flushes", "reconnecting"} {
		if value, ok := session[field]; ok {
			parts = append(parts, field+"="+string(value))
		}
	}
	return strings.Join(parts, " ")
}

// CephFSRetainedSession waits until rank serves client with an open session
// and returns it.
func CephFSRetainedSession(t *testing.T, parent context.Context, fs *cephfs.Filesystem, client uint64, rank int) cephfs.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last []cephfs.Session
	var lastErr error
	for {
		last, lastErr = fs.Sessions(ctx)
		for _, session := range last {
			if session.ID == client && session.Rank == rank && session.State == "open" {
				return session
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("client.%d has no open rank %d session: error=%v sessions=%+v", client, rank, lastErr, last)
		case <-time.After(time.Second):
		}
	}
}

// mdsMigration holds an MDS's subtree export and import counters from its
// "mds" perf counters.
type mdsMigration struct {
	Exported       int64 `json:"exported"`
	ExportedInodes int64 `json:"exported_inodes"`
	Imported       int64 `json:"imported"`
	ImportedInodes int64 `json:"imported_inodes"`
}

func cephFSMigrationCounters(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string) mdsMigration {
	t.Helper()
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "perf", "dump", "mds")
	if err != nil {
		t.Fatalf("perf dump of mds.%s: %v", mds, err)
	}
	var dump struct {
		MDS *mdsMigration `json:"mds"`
	}
	if err := json.Unmarshal(out, &dump); err != nil || dump.MDS == nil {
		t.Fatalf("perf dump of mds.%s: %v: %.512s", mds, err, out)
	}
	return *dump.MDS
}

// cephFSFailover selects which daemon takes the failed rank after its active
// MDS is killed.
type cephFSFailover int

const (
	// CephFSRestart starts the killed MDS's container again; the filesystem
	// has no standby.
	CephFSRestart cephFSFailover = iota
	// CephFSStandbyTakeover lets an idle standby take rank 0.
	CephFSStandbyTakeover
	// CephFSReplayTakeover lets the journal follower of rank 0 take it.
	CephFSReplayTakeover
	// CephFSRankOneTakeover runs two active ranks, pins the client file's
	// directory to rank 1 and lets an idle standby take rank 1 while rank 0
	// keeps running.
	CephFSRankOneTakeover
)

func (f cephFSFailover) String() string {
	return [...]string{"restart", "standby", "standby-replay", "rank1-standby"}[f]
}

// ActiveRank returns the active MDS of rank.
func ActiveRank(status *cephfs.FilesystemStatus, rank int) (cephfs.MDSStatus, bool) {
	for _, active := range status.Active {
		if active.Rank == rank {
			return active, true
		}
	}
	return cephfs.MDSStatus{}, false
}

// RunCephFSRetainedClient keeps one native client, its mount and an open fd
// across a SIGKILL of the active MDS of one rank. Depending on failover, the
// rank then comes back in the killed daemon's restarted container with a new
// GID, or a standby or standby-replay daemon takes it over; with two active
// ranks the client's file lives in a directory pinned to rank 1, which fails
// while rank 0 keeps its daemon. The client's sessions continue with the same
// client ID, PID and address: the actor reads, appends, fsyncs, renames and
// sets an xattr through its original fd. A reader in another container then
// checks the inode, the bytes and the xattr. After a takeover the killed
// daemon starts again and must rejoin as a standby, a separate condition from
// the client's recovery. This proves reconnect of a client whose data was
// fsynced before the failure; it does not cover replay of an outstanding
// request.
func RunCephFSRetainedClient(t *testing.T, failover cephFSFailover, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, imageOpts := IntegrationImages(t)
	opts = append(append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure))), opts...)
	activeRanks, failedRank, pin := 1, 0, "-"
	path, renamed := "/retained", "/retained-renamed"
	switch failover {
	case CephFSStandbyTakeover, CephFSReplayTakeover:
		opts = append(opts, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1, StandbyReplay: failover == CephFSReplayTakeover}))
	case CephFSRankOneTakeover:
		opts = append(opts, cephfs.WithFilesystems(cephfs.Config{ActiveMDS: 2, StandbyMDS: 1}))
		activeRanks, failedRank, pin = 2, 1, "1"
		path, renamed = "/pinned/retained", "/pinned/retained-renamed"
	}
	cluster, client := NewServiceClusterRun(t, CEPHFSRun, image, opts...)
	filesystems := cephfs.Filesystems(cluster)
	if len(filesystems) != 1 {
		t.Fatalf("filesystems = %d, want 1", len(filesystems))
	}
	fs := filesystems[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fs.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	standbys, replays := 0, 0
	switch failover {
	case CephFSStandbyTakeover, CephFSRankOneTakeover:
		standbys = 1
	case CephFSReplayTakeover:
		replays = 1
	}
	members := activeRanks + standbys + replays
	if before.MaxMDS != activeRanks || len(before.Active) != activeRanks || len(before.Standby) != standbys || len(before.StandbyReplay) != replays {
		t.Fatalf("want %d active ranks, %d standby and %d standby-replay: %+v", activeRanks, standbys, replays, before)
	}
	old, ok := ActiveRank(before, failedRank)
	if !ok || !old.Owned {
		t.Fatalf("rank %d has no owned active MDS: %+v", failedRank, before)
	}
	// With two ranks, rank 0 keeps its daemon throughout.
	untouched, _ := ActiveRank(before, 0)
	// takeover is the daemon that must take the failed rank: the killed one
	// itself after its restart, or the standby.
	takeover := old
	switch failover {
	case CephFSStandbyTakeover, CephFSRankOneTakeover:
		takeover = before.Standby[0]
	case CephFSReplayTakeover:
		takeover = before.StandbyReplay[0]
	}
	var daemon *cephfs.MDS
	for _, member := range fs.MDSs() {
		if member.ID == old.Name {
			daemon = member
		}
	}
	if daemon == nil || len(fs.MDSs()) != members || !takeover.Owned {
		t.Fatalf("active mds.%s or its successor mds.%s is not owned: %+v", old.Name, takeover.Name, before)
	}
	cid := daemon.GetContainerID()

	base := make([]byte, 0, 266)
	for value := range 256 {
		base = append(base, byte(value))
	}
	base = append(base, 0x00, 0xff, 0x7f, 0x80, '\r', '\n', 0x1b, 0x00, 'c', 'f')
	tail := []byte{0xfe, 0x00, 't', 'a', 'i', 'l', 0x01, 0x02, 0x03, 0xff, '\n', 0x00, 0x7f, 0x80}
	xattr := []byte{0x00, 'r', 'e', 'c', 'e', 'i', 'p', 't', 0xff, '\n'}
	want := append(append([]byte{}, base...), tail...)
	var zeroBefore, oneBefore mdsMigration
	if pin != "-" {
		zeroBefore, oneBefore = cephFSMigrationCounters(t, ctx, cluster, untouched.Name), cephFSMigrationCounters(t, ctx, cluster, old.Name)
	}
	actor := StartCephFSActor(t, ctx, cluster, client, cephFSRetainedActor, fs.FilesystemName, "/tmp/retained-actor",
		path, renamed, hex.EncodeToString(base), hex.EncodeToString(tail), hex.EncodeToString(xattr), pin)

	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	if pin != "-" {
		// The pin exports the directory, with the client's open file and its
		// capabilities, to rank 1, which opens a session with the client.
		CephFSWaitForPinnedSubtree(t, ctx, cluster, fs, "/pinned", failedRank)
		// Each daemon's own counters record the migration: rank 0 exported
		// the subtree and rank 1 imported it.
		zeroAfter, oneAfter := cephFSMigrationCounters(t, ctx, cluster, untouched.Name), cephFSMigrationCounters(t, ctx, cluster, old.Name)
		if zeroAfter.Exported <= zeroBefore.Exported || zeroAfter.ExportedInodes <= zeroBefore.ExportedInodes ||
			oneAfter.Imported <= oneBefore.Imported || oneAfter.ImportedInodes <= oneBefore.ImportedInodes {
			t.Fatalf("export counters of mds.%s %+v -> %+v, import counters of mds.%s %+v -> %+v", untouched.Name, zeroBefore, zeroAfter, old.Name, oneBefore, oneAfter)
		}
		t.Logf("CEPHFS_RETAINED export of /pinned: mds.%s exported %d subtrees with %d inodes, mds.%s imported %d with %d inodes",
			untouched.Name, zeroAfter.Exported-zeroBefore.Exported, zeroAfter.ExportedInodes-zeroBefore.ExportedInodes,
			old.Name, oneAfter.Imported-oneBefore.Imported, oneAfter.ImportedInodes-oneBefore.ImportedInodes)
	}
	session := CephFSRetainedSession(t, ctx, fs, baseline.Client, failedRank)
	if session.PID != strconv.Itoa(baseline.PID) || session.EntityID != "admin" || session.Address == "" || session.Caps == 0 {
		t.Fatalf("actor session = %+v, want PID %d", session, baseline.PID)
	}
	var rankZero cephfs.Session
	if failedRank != 0 {
		rankZero = CephFSRetainedSession(t, ctx, fs, baseline.Client, 0)
	}
	rawBefore := CephFSRawSession(t, ctx, cluster, old.Name, baseline.Client)
	t.Logf("CEPHFS_RETAINED baseline fscid=%d rank%d mds=%s gid=%d cid=%s client=%d pid=%d address=%s inode=%d raw: %s",
		before.FilesystemID, failedRank, old.Name, old.GID, cid, baseline.Client, baseline.PID, session.Address, baseline.Inode, CephFSRawSessionSummary(rawBefore))

	// SIGKILL leaves the MDS no chance to hand off its rank. Failing its GID
	// lets the successor take the rank at once.
	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	if state, err := daemon.State(ctx); err != nil || state.Running {
		t.Fatalf("killed MDS still runs: %+v, %v", state, err)
	}
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal("fail MDS GID:", err)
	}
	failed := time.Now()
	if failover == CephFSRestart {
		if err := daemon.Start(ctx); err != nil {
			t.Fatal("restart MDS:", err)
		}
	}
	var after *cephfs.FilesystemStatus
	var current cephfs.MDSStatus
	for {
		after, err = fs.MDSStatus(ctx)
		if err == nil && len(after.Active) == activeRanks {
			if current, ok = ActiveRank(after, failedRank); ok && current.State == "up:active" && current.GID != old.GID {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("rank %d did not come back: %+v, %v", failedRank, after, err)
		case <-time.After(time.Second):
		}
	}
	recovered := time.Since(failed)
	// The restarted daemon has a new GID; a standby keeps the GID it had.
	wantGID := current.GID
	if failover != CephFSRestart {
		wantGID = takeover.GID
	}
	if after.FilesystemID != before.FilesystemID || current.Name != takeover.Name || current.GID != wantGID || !current.Owned || daemon.GetContainerID() != cid || len(fs.MDSs()) != members {
		t.Fatalf("rank %d is not served by mds.%s (GID %d) of the same filesystem: %+v", failedRank, takeover.Name, wantGID, after)
	}
	if failedRank != 0 {
		if zero, ok := ActiveRank(after, 0); !ok || zero.GID != untouched.GID {
			t.Fatalf("rank 0 changed from %+v while rank %d failed: %+v", untouched, failedRank, after)
		}
	}

	actor.Open(t, ctx, "act")
	acted := actor.Receipt(t, ctx, 2)
	if acted.PID != baseline.PID || acted.Errno != 0 || acted.Error != "" {
		t.Fatalf("retained fd operations failed: %+v", acted)
	}
	if acted.Read != hex.EncodeToString(base) || acted.Inode != baseline.Inode || acted.Size != int64(len(want)) {
		t.Fatalf("retained fd saw %+v, want inode %d and %d bytes", acted, baseline.Inode, len(want))
	}

	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", CephFSIndependentReader, fs.FilesystemName, renamed}, Timeout: 2 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent reader: exit=%d err=%v stderr=%.2048s", read.ExitCode, err, read.Stderr)
	}
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int64  `json:"size"`
		Data   string `json:"data"`
		XAttr  string `json:"xattr"`
	}
	if err := json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &independent); err != nil {
		t.Fatalf("independent reader output: %v: %.512s", err, read.Stdout)
	}
	data, err := hex.DecodeString(independent.Data)
	if err != nil || !bytes.Equal(data, want) || independent.Size != int64(len(want)) || independent.Inode != baseline.Inode || independent.XAttr != hex.EncodeToString(xattr) || independent.Client == baseline.Client {
		t.Fatalf("independent reader saw client=%d inode=%d size=%d xattr=%s data=%s, want inode %d and %x", independent.Client, independent.Inode, independent.Size, independent.XAttr, independent.Data, baseline.Inode, want)
	}

	// The actor is still mounted; its sessions are the ones it opened before
	// the failure, the failed rank's now served by the new GID.
	retained := CephFSRetainedSession(t, ctx, fs, baseline.Client, failedRank)
	if retained.PID != session.PID || retained.Address != session.Address || retained.EntityID != session.EntityID {
		t.Fatalf("session after the restart = %+v, before = %+v", retained, session)
	}
	if failedRank != 0 {
		if zero := CephFSRetainedSession(t, ctx, fs, baseline.Client, 0); zero.PID != rankZero.PID || zero.Address != rankZero.Address {
			t.Fatalf("rank 0 session after the takeover = %+v, before = %+v", zero, rankZero)
		}
		// The new daemon of rank 1 is authoritative for the pinned directory.
		CephFSWaitForPinnedSubtree(t, ctx, cluster, fs, "/pinned", failedRank)
	}
	rawAfter := CephFSRawSession(t, ctx, cluster, current.Name, baseline.Client)
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed || closed.PID != baseline.PID {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.Wait(t, ctx)
	sum := sha256.Sum256(want)
	t.Logf("CEPHFS_RETAINED %s fscid=%d rank%d %s/%d -> %s/%d after %s, killed cid=%s client=%d pid=%d address=%s inode=%d sha256=%x reader=%d raw: %s",
		failover, after.FilesystemID, failedRank, old.Name, old.GID, current.Name, current.GID, recovered.Round(time.Second), cid, baseline.Client, baseline.PID, retained.Address, baseline.Inode, sum, independent.Client, CephFSRawSessionSummary(rawAfter))
	if failover == CephFSRestart {
		return
	}

	// Standby capacity is a separate condition: the killed daemon starts
	// again in its container and rejoins with a new GID.
	if err := daemon.Start(ctx); err != nil {
		t.Fatal("restart killed MDS:", err)
	}
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := fs.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rejoined := restored.Standby
	if failover == CephFSReplayTakeover {
		rejoined = restored.StandbyReplay
	}
	serving, ok := ActiveRank(restored, failedRank)
	if len(restored.Active) != activeRanks || !ok || serving.GID != current.GID || len(rejoined) != 1 || rejoined[0].Name != old.Name || rejoined[0].GID == old.GID || daemon.GetContainerID() != cid {
		t.Fatalf("mds.%s did not rejoin as a %s with a new GID: %+v", old.Name, failover, restored)
	}
	t.Logf("CEPHFS_RETAINED %s capacity restored: mds.%s rejoined with GID %d", failover, old.Name, rejoined[0].GID)
}

// MDSSubtree is one entry of an MDS's "get subtrees".
type MDSSubtree struct {
	IsAuth    bool `json:"is_auth"`
	AuthFirst int  `json:"auth_first"`
	// AuthSecond is -2 unless an export or import has made the authority
	// ambiguous.
	AuthSecond int `json:"auth_second"`
	ExportPin  int `json:"export_pin"`
	Dir        struct {
		Path string `json:"path"`
	} `json:"dir"`
}

// ParseMDSSubtrees decodes "get subtrees", a list or a list wrapped in an
// object.
func ParseMDSSubtrees(data []byte) ([]MDSSubtree, error) {
	var subtrees []MDSSubtree
	if err := json.Unmarshal(data, &subtrees); err != nil {
		var wrapped struct {
			Subtrees []MDSSubtree `json:"subtrees"`
		}
		if err := json.Unmarshal(data, &wrapped); err != nil {
			return nil, err
		}
		subtrees = wrapped.Subtrees
	}
	return subtrees, nil
}

// CephFSWaitForPinnedSubtree waits until the owned active MDS of rank reports
// path as an authoritative subtree pinned to that rank.
func CephFSWaitForPinnedSubtree(t *testing.T, parent context.Context, cluster *ceph.Container, fs *cephfs.Filesystem, path string, rank int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last string
	for {
		status, err := fs.MDSStatus(ctx)
		if err == nil {
			for _, active := range status.Active {
				if active.Rank != rank || !active.Owned {
					continue
				}
				data, err := cluster.Ceph(ctx, "tell", "mds."+active.Name, "get", "subtrees", "--format", "json")
				last = fmt.Sprintf("error=%v output=%s", err, data)
				if err != nil {
					continue
				}
				subtrees, err := ParseMDSSubtrees(data)
				if err != nil {
					continue
				}
				for _, subtree := range subtrees {
					if subtree.Dir.Path == path && subtree.IsAuth && subtree.AuthFirst == rank && subtree.ExportPin == rank {
						t.Logf("native MDS subtree: %s is owned by rank%d mds.%s; export pin=%d", path, rank, active.Name, subtree.ExportPin)
						return
					}
				}
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("export pin never moved actual subtree authority to rank%d: %s", rank, last)
		case <-time.After(time.Second):
		}
	}
}

// cephFSContender is an actor that opens a file that another client holds,
// writes a payload at an offset past its end, fsyncs and reads the whole file
// back through a mount of its own. It publishes receipt 1 with its client ID
// and what it read, keeps its mount and fd open until the "release" gate, and
// publishes receipt 2 once it has closed and shut down. Arguments after the
// common ones: path, offset, hex payload.
const cephFSContender CephFSActorScript = CephFSActorHelpers + `
path, offset, payload = sys.argv[4].encode(), int(sys.argv[5]), bytes.fromhex(sys.argv[6])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_RDWR, 0)
fs.write(fd, payload, offset)
fs.fsync(fd, 0)
stat = fs.fstat(fd)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size).hex())
gate("release")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(2, closed=True, **closing)
`

// mdsClientCaps is one client's capabilities on an inode, from the MDS's
// "dump inode". The strings use Ceph's notation, such as "pAsxLsXsxFsxcrwb".
type mdsClientCaps struct {
	ClientID uint64 `json:"client_id"`
	Pending  string `json:"pending"`
	Issued   string `json:"issued"`
	Wanted   string `json:"wanted"`
	LastSent uint64 `json:"last_sent"`
}

// CephFSInodeCaps returns the capabilities that mds has issued on inode, and
// the client it treats as the inode's only user (loner), or -1.
func CephFSInodeCaps(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string, inode uint64) ([]mdsClientCaps, int64) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dump", "inode", strconv.FormatUint(inode, 10))
	if err != nil {
		t.Fatalf("dump inode %d on mds.%s: %v", inode, mds, err)
	}
	var dump struct {
		Caps  []mdsClientCaps `json:"client_caps"`
		Loner int64           `json:"loner"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatalf("dump inode %d: %v: %.512s", inode, err, out)
	}
	return dump.Caps, dump.Loner
}

// CapsOf returns the lowercase capability bits that a cap string grants for
// one class, such as "sxcrwb" for 'F' in "pAsxLsXsxFsxcrwb".
func CapsOf(caps string, class byte) string {
	index := strings.IndexByte(caps, class)
	if index < 0 {
		return ""
	}
	end := index + 1
	for end < len(caps) && caps[end] >= 'a' && caps[end] <= 'z' {
		end++
	}
	return caps[index+1 : end]
}

// RunCephFSCapabilityHandoff lets a second client write a file that a live
// first client holds with exclusive, buffered file capabilities. The MDS
// revokes those capabilities from the holder, which answers, so the second
// client finishes long before a session timeout and nobody is evicted. The
// MDS's own "dump inode" shows the holder's capabilities before, and after
// while both clients still hold the file, and both clients see each other's
// bytes: the second client reads the holder's fsynced data, and the holder
// reads the second client's write through the fd it kept open. A third
// client checks the final file.
func RunCephFSCapabilityHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, imageOpts := IntegrationImages(t)
	cluster, client := NewServiceClusterRun(t, CEPHFSRun, image, append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)))...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("want one active MDS: %+v, %v", status, err)
	}
	mds := status.Active[0].Name

	base := make([]byte, 0, 266)
	for value := range 256 {
		base = append(base, byte(value))
	}
	base = append(base, 0x00, 0xff, 0x7f, 0x80, '\r', '\n', 0x1b, 0x00, 'c', 'f')
	tail := []byte{0xfe, 0x00, 't', 'a', 'i', 'l', 0x01, 0x02, 0x03, 0xff, '\n', 0x00, 0x7f, 0x80}
	xattr := []byte{0x00, 'r', 'e', 'c', 'e', 'i', 'p', 't', 0xff, '\n'}
	payload := []byte{0xc0, 'n', 't', 'e', 'n', 'd', 0x00, 0xff, '\n', 0x7f}
	const offset = 300
	const path, renamed = "/contended", "/contended-renamed"
	holder := StartCephFSActor(t, ctx, cluster, client, cephFSRetainedActor, fs.FilesystemName, "/tmp/holder-actor",
		path, renamed, hex.EncodeToString(base), hex.EncodeToString(tail), hex.EncodeToString(xattr), "-")
	baseline := holder.Receipt(t, ctx, 1)
	session := CephFSRetainedSession(t, ctx, fs, baseline.Client, 0)
	holderCaps := func(caps []mdsClientCaps) mdsClientCaps {
		t.Helper()
		for _, entry := range caps {
			if entry.ClientID == baseline.Client {
				return entry
			}
		}
		t.Fatalf("client.%d holds no capabilities on inode %d: %+v", baseline.Client, baseline.Inode, caps)
		return mdsClientCaps{}
	}
	caps, loner := CephFSInodeCaps(t, ctx, cluster, mds, baseline.Inode)
	before := holderCaps(caps)
	if file := CapsOf(before.Issued, 'F'); loner != int64(baseline.Client) || !strings.ContainsAny(file, "x") || !strings.Contains(file, "w") || !strings.Contains(file, "b") {
		t.Fatalf("holder client.%d is not the loner with exclusive buffered writes: loner %d, caps %+v", baseline.Client, loner, before)
	}

	// The contender needs the file capabilities that the holder has; a
	// holder that did not answer would keep it waiting for session_timeout
	// (60 s) and then be evicted. The contender keeps its mount and fd open
	// until the state below is observed: once it closes, the MDS may make the
	// holder the loner again and give the capabilities back.
	started := time.Now()
	contender := StartCephFSActor(t, ctx, cluster, ToolkitContainer(t, cluster), cephFSContender, fs.FilesystemName, "/tmp/contender-actor",
		path, strconv.Itoa(offset), hex.EncodeToString(payload))
	wrote := contender.Receipt(t, ctx, 1)
	took := time.Since(started)
	seen := append(append(append([]byte{}, base...), make([]byte, offset-len(base))...), payload...)
	if wrote.Client == baseline.Client || wrote.Inode != baseline.Inode || wrote.Size != int64(len(seen)) || wrote.Full != hex.EncodeToString(seen) {
		t.Fatalf("contender %+v, want the holder's fsynced bytes and its own write", wrote)
	}
	if took > 30*time.Second {
		t.Fatalf("contender took %s: the holder did not give up its capabilities", took)
	}
	// With both clients holding the inode the state is steady; observing it
	// 10 s later shows that the result does not depend on catching the
	// revocation in progress.
	time.Sleep(10 * time.Second)
	caps, _ = CephFSInodeCaps(t, ctx, cluster, mds, baseline.Inode)
	after := holderCaps(caps)
	if file := CapsOf(after.Issued, 'F'); strings.Contains(file, "b") || strings.Contains(file, "x") || after.LastSent <= before.LastSent {
		t.Fatalf("holder capabilities were not revoked: before %+v, after %+v", before, after)
	}
	var contenderCaps *mdsClientCaps
	for _, entry := range caps {
		if entry.ClientID == wrote.Client {
			contenderCaps = &entry
		}
	}
	if contenderCaps == nil || strings.Contains(CapsOf(contenderCaps.Issued, 'F'), "x") {
		t.Fatalf("contender client.%d does not share the inode: %+v", wrote.Client, caps)
	}
	if retained := CephFSRetainedSession(t, ctx, fs, baseline.Client, 0); retained.Address != session.Address || retained.PID != session.PID {
		t.Fatalf("holder session %+v, before %+v", retained, session)
	}
	entries, err := ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("holder %s was blocklisted: %+v", session.Address, entries)
	}
	contender.Open(t, ctx, "release")
	if closed := contender.Receipt(t, ctx, 2); !closed.Closed {
		t.Fatalf("contender closing receipt = %+v", closed)
	}
	contender.Wait(t, ctx)

	// The holder, through the fd it kept open, sees the contender's write.
	holder.Open(t, ctx, "act")
	acted := holder.Receipt(t, ctx, 2)
	want := append(append(append(append([]byte{}, base...), tail...), make([]byte, offset-len(base)-len(tail))...), payload...)
	if acted.Errno != 0 || acted.Read != hex.EncodeToString(base) || acted.Full != hex.EncodeToString(want) || acted.Inode != baseline.Inode {
		t.Fatalf("holder saw %+v, want %x", acted, want)
	}
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", CephFSIndependentReader, fs.FilesystemName, renamed}, Timeout: 2 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Data   string `json:"data"`
		XAttr  string `json:"xattr"`
	}
	if err != nil || read.ExitCode != 0 || json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &independent) != nil ||
		independent.Data != hex.EncodeToString(want) || independent.Inode != baseline.Inode || independent.XAttr != hex.EncodeToString(xattr) {
		t.Fatalf("independent reader: exit=%d err=%v out=%.1024s", read.ExitCode, err, read.Stdout)
	}
	holder.Open(t, ctx, "finish")
	if closed := holder.Receipt(t, ctx, 3); !closed.Closed {
		t.Fatalf("closing receipt = %+v", closed)
	}
	holder.Wait(t, ctx)
	t.Logf("CEPHFS_CAPS inode=%d holder client.%d issued %s (loner) -> %s, last_sent %d -> %d; contender client.%d (%s) wrote %d bytes at %d in %s; holder kept session %s and read the write; reader client.%d",
		baseline.Inode, baseline.Client, before.Issued, after.Issued, before.LastSent, after.LastSent, wrote.Client, contenderCaps.Issued, len(payload), offset, took.Round(100*time.Millisecond), session.Address, independent.Client)
}

// cephFSDaemonState returns the FSMap state of one MDS GID in filesystem, such
// as "up:reconnect", or "" when the GID is not in its MDSMap. MDSStatus lists
// only active ranks, so it does not show the recovery states.
func cephFSDaemonState(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem string, gid uint64) string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal("fs dump:", err)
	}
	var dump struct {
		Filesystems []struct {
			MDSMap struct {
				Name string `json:"fs_name"`
				Info map[string]struct {
					GID   uint64 `json:"gid"`
					State string `json:"state"`
				} `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatalf("fs dump: %v: %.512s", err, out)
	}
	for _, fs := range dump.Filesystems {
		if fs.MDSMap.Name != filesystem {
			continue
		}
		for _, info := range fs.MDSMap.Info {
			if info.GID == gid {
				return info.State
			}
		}
	}
	return ""
}

// RunCephFSLateReconnect freezes a mounted client while the only active MDS
// fails over to a standby, so the client misses the new MDS's reconnect
// window, shortened to 10 s with mds_reconnect_timeout. The MDS's own state
// passes through up:reconnect for that window; then it evicts the silent
// client, whose address enters the blocklist, and its session disappears.
// When the client thaws, its open fd fails (ESHUTDOWN on Tentacle, EPERM on
// Squid), it closes and shuts down cleanly, and a fresh client still reads
// the bytes it fsynced.
// The killed daemon then rejoins as the standby.
func RunCephFSLateReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, imageOpts := IntegrationImages(t)
	cluster, client := NewServiceClusterRun(t, CEPHFSRun, image, append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)),
		cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fs.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 || len(before.Standby) != 1 {
		t.Fatalf("want one active MDS and one standby: %+v, %v", before, err)
	}
	old, takeover := before.Active[0], before.Standby[0]
	var daemon *cephfs.MDS
	for _, member := range fs.MDSs() {
		if member.ID == old.Name {
			daemon = member
		}
	}
	if daemon == nil {
		t.Fatalf("active mds.%s is not owned", old.Name)
	}
	window, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mds", Name: "mds_reconnect_timeout", Value: "10"})
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := window.Restore(restoreCtx); err != nil {
			t.Errorf("restore mds_reconnect_timeout: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	base := make([]byte, 0, 266)
	for value := range 256 {
		base = append(base, byte(value))
	}
	base = append(base, 0x00, 0xff, 0x7f, 0x80, '\r', '\n', 0x1b, 0x00, 'c', 'f')
	const path = "/late"
	actor := StartCephFSActor(t, ctx, cluster, client, cephFSRetainedActor, fs.FilesystemName, "/tmp/late-actor",
		path, "/late-renamed", hex.EncodeToString(base), "fe", "00", "-")
	baseline := actor.Receipt(t, ctx, 1)
	session := CephFSRetainedSession(t, ctx, fs, baseline.Client, 0)

	pause, err := fault.Pause(ctx, cluster, client)
	if pause != nil {
		t.Cleanup(func() {
			resumeCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := pause.Resume(resumeCtx); err != nil {
				t.Errorf("resume client: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal("pause client:", err)
	}
	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal("fail MDS GID:", err)
	}
	// Follow rank 0 through the standby's recovery states.
	failed := time.Now()
	var reconnectFirst, reconnectLast time.Duration
	var states []string
	for {
		state := cephFSDaemonState(t, ctx, cluster, fs.FilesystemName, takeover.GID)
		if state != "" && !slices.Contains(states, state) {
			states = append(states, state)
		}
		if state == "up:reconnect" {
			if reconnectFirst == 0 {
				reconnectFirst = time.Since(failed)
			}
			reconnectLast = time.Since(failed)
		}
		if state == "up:active" {
			break
		}
		if time.Since(failed) > 3*time.Minute {
			t.Fatalf("mds.%s did not take rank 0: states %v", takeover.Name, states)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if status, err := fs.MDSStatus(ctx); err != nil || len(status.Active) != 1 || status.Active[0].GID != takeover.GID {
		t.Fatalf("rank 0 is not served by mds.%s: %+v, %v", takeover.Name, status, err)
	}
	held := reconnectLast - reconnectFirst
	if reconnectFirst == 0 || held < 5*time.Second || held > time.Minute {
		t.Fatalf("rank 0 states %v, up:reconnect for %s, want the 10 s window", states, held)
	}
	sessions, err := fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(sessions, func(s cephfs.Session) bool { return s.ID == baseline.Client }) {
		t.Fatalf("the frozen client.%d kept a session: %+v", baseline.Client, sessions)
	}
	entries, err := ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("the frozen client %s is not blocklisted: %+v", session.Address, entries)
	}

	if err := pause.Resume(ctx); err != nil {
		t.Fatal("resume client:", err)
	}
	actor.Open(t, ctx, "act")
	acted := actor.Receipt(t, ctx, 2)
	// The blocklisted instance cannot use its session again: Tentacle's
	// client reports ESHUTDOWN (108), Squid's EPERM (1).
	if (acted.Errno != 108 && acted.Errno != 1) || acted.PID != baseline.PID {
		t.Fatalf("the evicted client's fd gave %+v, want errno 108 or 1", acted)
	}
	actor.Open(t, ctx, "finish")
	closed := actor.Receipt(t, ctx, 3)
	if !closed.Closed {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.Wait(t, ctx)

	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", CephFSIndependentReader, fs.FilesystemName, path}, Timeout: 2 * time.Minute,
		MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Data   string `json:"data"`
	}
	if err != nil || read.ExitCode != 0 || json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &independent) != nil ||
		independent.Data != hex.EncodeToString(base) || independent.Inode != baseline.Inode || independent.Client == baseline.Client {
		t.Fatalf("independent reader: exit=%d err=%v out=%.1024s", read.ExitCode, err, read.Stdout)
	}
	t.Logf("CEPHFS_LATE client.%d frozen across the failover of rank 0 %s/%d -> %s/%d; states %v, up:reconnect for %s from %s; evicted and blocklisted %s; old fd errno %d, close errno %d; reader client.%d read the fsynced %d bytes",
		baseline.Client, old.Name, old.GID, takeover.Name, takeover.GID, states, held.Round(time.Second), reconnectFirst.Round(time.Second), session.Address, acted.Errno, closed.CloseErrno, independent.Client, len(base))

	if err := daemon.Start(ctx); err != nil {
		t.Fatal("restart killed MDS:", err)
	}
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

// cephFSStreamActor keeps one libcephfs mount and one O_RDWR fd open, like
// cephFSRetainedActor, and appends a stream of records through it. It writes
// and fsyncs the base bytes and publishes receipt 1. After the "stream" gate,
// it appends 16-byte records, each the big-endian sequence number from 1 and
// its complement. A record counts only after a full-length write and a
// successful fsync; then the actor renames a progress receipt with the count
// into place and waits 50 ms. When the "stop" gate opens, it finishes the
// record in hand, sets a binary xattr, reads the whole file back through the
// fd and publishes receipt 2 with the count. After the "finish" gate it closes
// everything and publishes receipt 3. Arguments: filesystem, work directory,
// token, path, the hex of the base bytes and of the xattr value.
const cephFSStreamActor CephFSActorScript = CephFSActorHelpers + `
import struct
path = sys.argv[4].encode()
base, xattr = bytes.fromhex(sys.argv[5]), bytes.fromhex(sys.argv[6])
def opened(name):
    try:
        with open(workdir + "/" + name) as gate_file:
            return gate_file.read() == token
    except FileNotFoundError:
        return False
def progress(**fields):
    fields.update(token=token, pid=os.getpid())
    target = workdir + "/progress"
    with open(target + ".tmp", "w") as out:
        json.dump(fields, out)
    os.rename(target + ".tmp", target)
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
client, inode = fs.get_instance_id(), fs.fstat(fd).st_ino
publish(1, client=client, inode=inode, size=fs.fstat(fd).st_size)
gate("stream")
count, result = 0, {}
try:
    while not opened("stop"):
        if count == 4000:
            sys.exit("the stream was not stopped")
        record = struct.pack(">QQ", count + 1, (count + 1) ^ 0xFFFFFFFFFFFFFFFF)
        written = fs.write(fd, record, len(base) + 16 * count)
        if written != len(record):
            sys.exit("record %d: wrote %d bytes" % (count + 1, written))
        fs.fsync(fd, 0)
        count += 1
        progress(client=client, inode=inode, count=count)
        time.sleep(0.05)
    fs.setxattr(path, "user.receipt", xattr, 0)
    stat = fs.fstat(fd)
    result.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size + 1).hex())
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
publish(2, count=count, **result)
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

// StreamRecords returns base followed by the first count records of
// cephFSStreamActor.
func StreamRecords(base []byte, count int64) []byte {
	records := append([]byte{}, base...)
	for sequence := uint64(1); sequence <= uint64(count); sequence++ {
		records = binary.BigEndian.AppendUint64(records, sequence)
		records = binary.BigEndian.AppendUint64(records, ^sequence)
	}
	return records
}

// CephFSStreamProgress is the stream actor's progress receipt: how many
// records one client has written and fsynced to one inode.
type CephFSStreamProgress struct {
	Token  string `json:"token"`
	PID    int    `json:"pid"`
	Client uint64 `json:"client"`
	Inode  uint64 `json:"inode"`
	Count  int64  `json:"count"`
}

// CheckStreamProgress accepts next only from the run of token, with the
// process, client and inode of baseline, and with no fewer records than last.
func CheckStreamProgress(token string, baseline CephFSActorReceipt, last, next CephFSStreamProgress) error {
	if next.Token != token || next.PID != baseline.PID || next.Client != baseline.Client || next.Inode != baseline.Inode {
		return fmt.Errorf("progress %+v does not belong to the actor of %+v", next, baseline)
	}
	if next.Count < last.Count {
		return fmt.Errorf("progress went back from %d to %d records", last.Count, next.Count)
	}
	return nil
}

// progress waits until the stream actor reports at least count records and
// returns that receipt. Every receipt it reads must pass CheckStreamProgress
// against last, which it updates.
func (run *CephFSActorRun) progress(t *testing.T, ctx context.Context, baseline CephFSActorReceipt, last *CephFSStreamProgress, count int64) CephFSStreamProgress {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, out, err := SessionExec(ctx, run.client, "cat", run.workdir+"/progress")
		if err == nil && code == 0 {
			decoder := json.NewDecoder(strings.NewReader(out))
			decoder.DisallowUnknownFields()
			var next CephFSStreamProgress
			if err := decoder.Decode(&next); err != nil {
				t.Fatalf("progress: %v: %q", err, out)
			}
			if err := CheckStreamProgress(run.Token, baseline, *last, next); err != nil {
				t.Fatal(err)
			}
			*last = next
			if next.Count >= count {
				return next
			}
		}
		select {
		case <-run.Done:
			t.Fatalf("actor ended at %d records, before %d: %s", last.Count, count, run.exited(ctx))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream stayed at %d records, want %d", last.Count, count)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("progress: %v", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// SoleSubtreeAuthority returns nil when, of the subtree lists that each rank's
// MDS reported, only rank's claims path as an authoritative subtree, pinned
// to rank, with no second authority. While an export is under way, both ranks
// or neither may claim it, or the claim names the other rank as well.
func SoleSubtreeAuthority(views map[int][]MDSSubtree, path string, rank int) error {
	var owners []string
	sole := false
	for _, viewer := range slices.Sorted(maps.Keys(views)) {
		for _, subtree := range views[viewer] {
			if subtree.Dir.Path != path || !subtree.IsAuth {
				continue
			}
			owners = append(owners, fmt.Sprintf("rank%d(auth=%d,%d export_pin=%d)", viewer, subtree.AuthFirst, subtree.AuthSecond, subtree.ExportPin))
			sole = viewer == rank && subtree.AuthFirst == rank && subtree.AuthSecond == -2 && subtree.ExportPin == rank
		}
	}
	if len(owners) != 1 || !sole {
		return fmt.Errorf("%s is claimed by %v, want rank%d alone", path, owners, rank)
	}
	return nil
}

// cephFSWaitForSoleAuthority waits until the MDS of rank is the only one that
// reports path as an authoritative subtree pinned to rank. It reads every
// active rank's subtrees between two MDSStatus reads, and each read must show
// the daemons of gids, by rank: a changed daemon fails the test.
func cephFSWaitForSoleAuthority(t *testing.T, parent context.Context, cluster *ceph.Container, fs *cephfs.Filesystem, path string, rank int, gids map[int]uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	same := func() (*cephfs.FilesystemStatus, bool) {
		status, err := fs.MDSStatus(ctx)
		if err != nil {
			return nil, false
		}
		if len(status.Active) != len(gids) {
			t.Fatalf("active ranks changed from %v: %+v", gids, status)
		}
		for _, active := range status.Active {
			if gids[active.Rank] != active.GID {
				t.Fatalf("rank %d changed from GID %d: %+v", active.Rank, gids[active.Rank], status)
			}
		}
		return status, true
	}
	var last string
	for {
		if status, ok := same(); ok {
			views := map[int][]MDSSubtree{}
			for _, active := range status.Active {
				data, err := cluster.Ceph(ctx, "tell", "mds."+active.Name, "get", "subtrees", "--format", "json")
				if err != nil {
					last = fmt.Sprintf("mds.%s: %v", active.Name, err)
					break
				}
				subtrees, err := ParseMDSSubtrees(data)
				if err != nil {
					last = fmt.Sprintf("mds.%s: %v: %.512s", active.Name, err, data)
					break
				}
				views[active.Rank] = subtrees
			}
			if len(views) == len(gids) {
				err := SoleSubtreeAuthority(views, path, rank)
				if _, ok := same(); ok && err == nil {
					return
				}
				last = fmt.Sprint(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("rank%d did not become the only authority of %s: %s", rank, path, last)
		case <-time.After(time.Second):
		}
	}
}

// cephFSDirPin mounts once and, with "create", makes a directory and sets its
// ceph.dir.pin; with "set", sets the pin; with "get", prints it. A mount of
// its own has no cached inode, so "get" prints the policy that the MDS holds.
const cephFSDirPin = `
import cephfs, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    path, action = sys.argv[2].encode(), sys.argv[3]
    if action == "create":
        fs.mkdir(path, 0o755)
    if action == "get":
        print(fs.getxattr(path, "ceph.dir.pin", 64).decode())
    else:
        fs.setxattr(path, "ceph.dir.pin", sys.argv[4].encode(), 0)
finally:
    fs.unmount()
    fs.shutdown()
`

// cephFSDirectoryPin runs cephFSDirPin in the control container and returns
// what it printed.
func cephFSDirectoryPin(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem, path, action string, value ...string) string {
	t.Helper()
	runCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	result, err := cluster.RunNativeCommand(runCtx, ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", cephFSDirPin, filesystem, path, action}, value...), Timeout: 2 * time.Minute,
		MaxStdout: 4 << 10, MaxStderr: 64 << 10, Label: "CephFS directory pin",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("%s pin of %s: exit=%d err=%v stderr=%.2048s", action, path, result.ExitCode, err, result.Stderr)
	}
	return strings.TrimSpace(string(result.Stdout))
}

// RunCephFSLiveRankMigration moves a directory between two live active ranks
// while a native client streams fsynced records into a file in it, through
// one mount and one fd. The directory starts pinned to rank 0. Once the
// client has written a few records, the pin changes to 1, set by another
// client, and the stream goes on while rank 1 becomes the directory's only
// authority. Each daemon's counters record the export and the import, both
// daemons keep their GIDs, and the client keeps the address and PID of its
// sessions and is not blocklisted. After the stream stops, the client reads
// every record through its fd and a reader in another container reads the
// same bytes and xattr. The pin then goes back to 0, a fresh mount reads that
// policy, and rank 0 becomes the only authority again before the client
// closes. This proves fsynced I/O through one fd across a live export; it does
// not show the export messages or a request that was in flight at the freeze.
func RunCephFSLiveRankMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, imageOpts := IntegrationImages(t)
	cluster, client := NewServiceClusterRun(t, CEPHFSRun, image, append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)),
		cephfs.WithFilesystems(cephfs.Config{ActiveMDS: 2}))...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || status.MaxMDS != 2 || len(status.Active) != 2 || len(status.Standby) != 0 {
		t.Fatalf("want two active ranks and no standby: %+v, %v", status, err)
	}
	zero, zeroOK := ActiveRank(status, 0)
	one, oneOK := ActiveRank(status, 1)
	if !zeroOK || !oneOK || !zero.Owned || !one.Owned {
		t.Fatalf("ranks 0 and 1 are not owned: %+v", status)
	}
	gids := map[int]uint64{0: zero.GID, 1: one.GID}

	const directory, path = "/stream", "/stream/records"
	cephFSDirectoryPin(t, ctx, cluster, fs.FilesystemName, directory, "create", "0")
	cephFSWaitForSoleAuthority(t, ctx, cluster, fs, directory, 0, gids)

	base := make([]byte, 0, 266)
	for value := range 256 {
		base = append(base, byte(value))
	}
	base = append(base, 0x00, 0xff, 0x7f, 0x80, '\r', '\n', 0x1b, 0x00, 'c', 'f')
	xattr := []byte{0x00, 0xff}
	actor := StartCephFSActor(t, ctx, cluster, client, cephFSStreamActor, fs.FilesystemName, "/tmp/stream-actor",
		path, hex.EncodeToString(base), hex.EncodeToString(xattr))
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	session := CephFSRetainedSession(t, ctx, fs, baseline.Client, 0)
	if session.PID != strconv.Itoa(baseline.PID) || session.Address == "" {
		t.Fatalf("actor session = %+v, want PID %d", session, baseline.PID)
	}
	zeroBefore, oneBefore := cephFSMigrationCounters(t, ctx, cluster, zero.Name), cephFSMigrationCounters(t, ctx, cluster, one.Name)

	actor.Open(t, ctx, "stream")
	var seen CephFSStreamProgress
	requested := actor.progress(t, ctx, baseline, &seen, 3)
	cephFSDirectoryPin(t, ctx, cluster, fs.FilesystemName, directory, "set", "1")
	cephFSWaitForSoleAuthority(t, ctx, cluster, fs, directory, 1, gids)
	moved := actor.progress(t, ctx, baseline, &seen, 0)
	zeroAfter, oneAfter := cephFSMigrationCounters(t, ctx, cluster, zero.Name), cephFSMigrationCounters(t, ctx, cluster, one.Name)
	if zeroAfter.Exported <= zeroBefore.Exported || zeroAfter.ExportedInodes <= zeroBefore.ExportedInodes ||
		oneAfter.Imported <= oneBefore.Imported || oneAfter.ImportedInodes <= oneBefore.ImportedInodes {
		t.Fatalf("export counters of mds.%s %+v -> %+v, import counters of mds.%s %+v -> %+v", zero.Name, zeroBefore, zeroAfter, one.Name, oneBefore, oneAfter)
	}
	// The stream goes on under rank 1, through the same fd.
	continued := actor.progress(t, ctx, baseline, &seen, moved.Count+3)
	// The client keeps its rank 0 session and has one on rank 1, of the same
	// instance.
	for rank := range 2 {
		retained := CephFSRetainedSession(t, ctx, fs, baseline.Client, rank)
		if retained.PID != session.PID || retained.Address != session.Address || retained.EntityID != session.EntityID {
			t.Fatalf("rank %d session = %+v, want the address and PID of %+v", rank, retained, session)
		}
	}
	entries, err := ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("the streaming client %s was blocklisted: %+v", session.Address, entries)
	}

	actor.Open(t, ctx, "stop")
	stopped := actor.Receipt(t, ctx, 2)
	if stopped.PID != baseline.PID || stopped.Errno != 0 || stopped.Error != "" || stopped.Count < continued.Count {
		t.Fatalf("stream receipt %+v after %d acknowledged records", stopped, continued.Count)
	}
	want := StreamRecords(base, stopped.Count)
	if stopped.Inode != baseline.Inode || stopped.Size != int64(len(want)) || stopped.Full != hex.EncodeToString(want) {
		t.Fatalf("the fd read inode %d, %d bytes %s, want inode %d and %d records", stopped.Inode, stopped.Size, stopped.Full, baseline.Inode, stopped.Count)
	}
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, ToolkitContainer(t, cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", CephFSIndependentReader, fs.FilesystemName, path}, Timeout: 2 * time.Minute,
		MaxStdout: 256 << 10, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int64  `json:"size"`
		Data   string `json:"data"`
		XAttr  string `json:"xattr"`
	}
	if err != nil || read.ExitCode != 0 || !read.Complete() || json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &independent) != nil ||
		independent.Data != hex.EncodeToString(want) || independent.Inode != baseline.Inode || independent.XAttr != hex.EncodeToString(xattr) || independent.Client == baseline.Client {
		t.Fatalf("independent reader: exit=%d err=%v out=%.1024s", read.ExitCode, err, read.Stdout)
	}

	// The pin policy and the placement are separate conditions: the policy is
	// read through a fresh mount, then rank 0 must become the authority.
	cephFSDirectoryPin(t, ctx, cluster, fs.FilesystemName, directory, "set", "0")
	if policy := cephFSDirectoryPin(t, ctx, cluster, fs.FilesystemName, directory, "get"); policy != "0" {
		t.Fatalf("a fresh mount reads pin %q, want 0", policy)
	}
	cephFSWaitForSoleAuthority(t, ctx, cluster, fs, directory, 0, gids)
	zeroBack, oneBack := cephFSMigrationCounters(t, ctx, cluster, zero.Name), cephFSMigrationCounters(t, ctx, cluster, one.Name)
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.Wait(t, ctx)
	sum := sha256.Sum256(want)
	t.Logf("CEPHFS_MIGRATION fscid=%d %s: rank0 mds.%s/%d exported %d subtrees with %d inodes and rank1 mds.%s/%d imported %d with %d; client.%d pid=%d address=%s inode=%d records %d when pin 1 was requested, %d when rank1 alone was the authority, %d after, %d in all (sha256=%x); reader client.%d; back to rank0: mds.%s exported %d, mds.%s imported %d",
		status.FilesystemID, directory, zero.Name, zero.GID, zeroAfter.Exported-zeroBefore.Exported, zeroAfter.ExportedInodes-zeroBefore.ExportedInodes,
		one.Name, one.GID, oneAfter.Imported-oneBefore.Imported, oneAfter.ImportedInodes-oneBefore.ImportedInodes,
		baseline.Client, baseline.PID, session.Address, baseline.Inode, requested.Count, moved.Count, continued.Count, stopped.Count, sum, independent.Client,
		one.Name, oneBack.Exported-oneAfter.Exported, zero.Name, zeroBack.Imported-zeroAfter.Imported)
}

// CephFSUnsafeActor mounts once with a client admin socket at a given path,
// creates a file in a new directory, fsyncs it and syncs the filesystem, and
// publishes receipt 1 with the directory's inode. At the "rename" gate it renames the file within the
// directory once, without a retry, and publishes receipt 2 with the result
// and the time the call took. At the "finish" gate it stats the new name
// through its own mount, unmounts and publishes receipt 3. Arguments after the
// common ones: directory, source path, target path, admin socket path.
const CephFSUnsafeActor CephFSActorScript = CephFSActorHelpers + `
directory, source, target, asok = sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode(), sys.argv[7]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("admin_socket", asok)
fs.mount(filesystem_name=name.encode())
fs.mkdir(directory, 0o755)
fd = fs.open(source, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
fs.write(fd, b"unsafe-window", 0)
fs.fsync(fd, 0)
fs.close(fd)
fs.sync_fs()
stat = fs.stat(source)
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size, dir_inode=fs.stat(directory).st_ino)
gate("rename")
started = time.monotonic()
result = {}
try:
    fs.rename(source, target)
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
publish(2, **result)
gate("finish")
closing = {}
try:
    closing.update(inode=fs.stat(target).st_ino)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(3, closed=True, **closing)
`

// MDSRequestOp is one client request in an MDS's dump_ops_in_flight or
// dump_historic_ops.
type MDSRequestOp struct {
	Description string  `json:"description"`
	Duration    float64 `json:"duration"`
	TypeData    struct {
		FlagPoint string `json:"flag_point"`
		ReqID     struct {
			Entity struct {
				Type string `json:"type"`
				Num  uint64 `json:"num"`
			} `json:"entity"`
			TID uint64 `json:"tid"`
		} `json:"reqid"`
		Events []struct {
			Event string `json:"event"`
		} `json:"events"`
	} `json:"type_data"`
}

func ParseMDSRequestOps(data []byte) ([]MDSRequestOp, error) {
	var dump struct {
		Ops *[]MDSRequestOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}
	if dump.Ops == nil {
		return nil, errors.New("the dump has no ops")
	}
	return *dump.Ops, nil
}

// FindMDSRequest returns the request of client whose operation is op, such
// as "rename", from its description "client_request(client.N:TID op ...",
// and whose TID is tid, or any TID for 0.
func FindMDSRequest(ops []MDSRequestOp, client uint64, op string, tid uint64) (MDSRequestOp, bool) {
	for _, candidate := range ops {
		reqid := candidate.TypeData.ReqID
		prefix := fmt.Sprintf("client_request(client.%d:%d %s ", client, reqid.TID, op)
		if reqid.Entity.Type == "client" && reqid.Entity.Num == client && (tid == 0 || reqid.TID == tid) && strings.HasPrefix(candidate.Description, prefix) {
			return candidate, true
		}
	}
	return MDSRequestOp{}, false
}

func (op MDSRequestOp) Events() []string {
	events := make([]string, 0, len(op.TypeData.Events))
	for _, event := range op.TypeData.Events {
		events = append(events, event.Event)
	}
	return events
}

// RequestSafety classifies a request from its MDS events: "unsafe" after an
// early reply and before its journal commit, "safe" when the commit and the
// final reply followed the early reply, and "no-early-reply" when the MDS
// replied only once, after the commit.
func RequestSafety(op MDSRequestOp) string {
	early, committed, replied := -1, -1, -1
	for index, event := range op.Events() {
		switch {
		case event == "early_replied":
			early = index
		case strings.HasPrefix(event, "journal_committed"):
			committed = index
		case event == "replying":
			replied = index
		}
	}
	switch {
	case early < 0:
		return "no-early-reply"
	case committed < 0 && replied < 0:
		return "unsafe"
	case committed > early && replied > committed:
		return "safe"
	}
	return "inconsistent"
}

// unsafeFlag reads a client's got_unsafe, a number or a boolean.
type unsafeFlag bool

func (f *unsafeFlag) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "1", "true":
		*f = true
	case "0", "false":
		*f = false
	default:
		return fmt.Errorf("got_unsafe %s", data)
	}
	return nil
}

// ClientMDSRequest is one request in a libcephfs client's mds_requests.
type ClientMDSRequest struct {
	TID       uint64     `json:"tid"`
	Op        string     `json:"op"`
	MDS       int        `json:"mds"`
	TargetIno string     `json:"target_ino"`
	GotUnsafe unsafeFlag `json:"got_unsafe"`
}

// ParseClientMDSRequests reads mds_requests in either shape that clients
// print: a "requests" array, or one "request" object per request, the key
// repeated, in an object that is empty when nothing is pending.
func ParseClientMDSRequests(data []byte) ([]ClientMDSRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("mds_requests is not an object: %v", err)
	}
	requests := []ClientMDSRequest{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		switch key {
		case "requests":
			var list []ClientMDSRequest
			if err := decoder.Decode(&list); err != nil {
				return nil, err
			}
			requests = append(requests, list...)
		case "request":
			var request ClientMDSRequest
			if err := decoder.Decode(&request); err != nil {
				return nil, err
			}
			requests = append(requests, request)
		default:
			return nil, fmt.Errorf("mds_requests has an unknown field %v", key)
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("mds_requests does not end: %v", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("mds_requests has data after its object")
	}
	return requests, nil
}

// unsafeRenameState is one look at a client's rename from both sides: the
// MDS's ops in flight and the client's own pending requests.
type unsafeRenameState struct {
	Tid      uint64
	op       MDSRequestOp
	InFlight bool
	Request  ClientMDSRequest
	Pending  bool
}

// JoinUnsafeRename finds a client's rename in parsed observations of both
// sides. With tid 0 the MDS's rename in flight names the TID. Otherwise each
// side is searched for that TID on its own, so a request that only one side
// still holds counts as held there.
func JoinUnsafeRename(ops []MDSRequestOp, requests []ClientMDSRequest, client, tid uint64) unsafeRenameState {
	state := unsafeRenameState{Tid: tid}
	state.op, state.InFlight = FindMDSRequest(ops, client, "rename", tid)
	if state.InFlight && tid == 0 {
		state.Tid = state.op.TypeData.ReqID.TID
	}
	for _, request := range requests {
		if state.Tid != 0 && request.TID == state.Tid {
			state.Request, state.Pending = request, true
		}
	}
	return state
}

// RenameSafe is the completion condition of a held rename: neither side
// holds its TID any more, and the MDS's history has that request committed
// and replied.
func RenameSafe(state unsafeRenameState, historic []MDSRequestOp, client uint64) bool {
	op, found := FindMDSRequest(historic, client, "rename", state.Tid)
	return state.Tid != 0 && !state.InFlight && !state.Pending && found && RequestSafety(op) == "safe"
}

// ClientMDSRequests reads a libcephfs client's pending requests through its
// admin socket, bounded. A query that fails or prints what does not parse
// fails the test; an empty pending set is an answer.
func ClientMDSRequests(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) []ClientMDSRequest {
	t.Helper()
	requests, err := QueryClientMDSRequests(ctx, cluster, client, asok)
	if err != nil {
		t.Fatal(err)
	}
	return requests
}

// QueryClientMDSRequests is ClientMDSRequests for a probe: it returns an
// error instead of failing the test.
func QueryClientMDSRequests(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) ([]ClientMDSRequest, error) {
	queryCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	result, err := cluster.RunNativeCommand(queryCtx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "mds_requests"}, Timeout: 30 * time.Second,
		MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client mds_requests",
	})
	if err != nil || result.ExitCode != 0 {
		return nil, fmt.Errorf("client mds_requests: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
	}
	requests, err := ParseClientMDSRequests(result.Stdout)
	if err != nil {
		return nil, fmt.Errorf("client mds_requests: %v: %.1024s", err, result.Stdout)
	}
	return requests, nil
}

// cephFSUnsafeObservation is one look with its bounded raw form.
type cephFSUnsafeObservation struct {
	unsafeRenameState
	raw string
}

// observeUnsafeRename reads the MDS's ops in flight and, through the actor's
// admin socket, the client's pending requests, each bounded, and joins them
// for the client's rename with tid, or the TID that the MDS names for 0. A
// query that fails or prints what does not parse fails the test; an empty
// pending set is an answer.
func observeUnsafeRename(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, mds, asok string, clientID, tid uint64) cephFSUnsafeObservation {
	t.Helper()
	data, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dump_ops_in_flight")
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v", mds, err)
	}
	ops, err := ParseMDSRequestOps(data)
	if err != nil {
		t.Fatalf("dump_ops_in_flight of mds.%s: %v: %.1024s", mds, err, data)
	}
	requests := ClientMDSRequests(t, ctx, cluster, client, asok)
	observed := cephFSUnsafeObservation{unsafeRenameState: JoinUnsafeRename(ops, requests, clientID, tid)}
	observed.raw = fmt.Sprintf("tid %d: mds op %q flag_point=%q events=%v; client requests %+v", observed.Tid, observed.op.Description, observed.op.TypeData.FlagPoint, observed.op.Events(), requests)
	return observed
}

// CephFSUnsafeHold is one rename that the filesystem's single active MDS
// answered early and unsafely while its own mds_log_pause held journal
// submission, observed as held on both sides.
type CephFSUnsafeHold struct {
	*CephFSSingleMDS
	Asok     string
	Actor    *CephFSActorRun
	Baseline CephFSActorReceipt
	Renamed  CephFSActorReceipt
	Pause    *override.ConfigHandle
	Held     cephFSUnsafeObservation
}

// CephFSSingleMDS is a cluster with one filesystem served by one active,
// owned MDS, and a client container.
type CephFSSingleMDS struct {
	Cluster *ceph.Container
	Client  testcontainers.Container
	FS      *cephfs.Filesystem
	MDS     cephfs.MDSStatus
	Version string
}

// StartCephFSSingleMDS starts the cluster with secure Messenger v2 and checks
// that its one active MDS runs without mds_log_pause. Options such as a
// filesystem with a standby come after the defaults.
func StartCephFSSingleMDS(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) *CephFSSingleMDS {
	t.Helper()
	image, imageOpts := IntegrationImages(t)
	cluster, client := NewServiceClusterRun(t, CEPHFSRun, image, append(append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure))), opts...)...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("want one active MDS: %+v, %v", status, err)
	}
	m := &CephFSSingleMDS{Cluster: cluster, Client: client, FS: fs, MDS: status.Active[0]}
	if paused := m.DaemonConfig(t, ctx, "mds_log_pause"); paused != "false" {
		t.Fatalf("mds.%s runs with mds_log_pause=%s", m.MDS.Name, paused)
	}
	version, err := cluster.Ceph(ctx, "tell", "mds."+m.MDS.Name, "version")
	if err != nil {
		t.Fatal(err)
	}
	m.Version = strings.Join(strings.Fields(string(version)), " ")
	return m
}

// pauseJournal sets mds_log_pause through override.SetConfig and waits until the
// running MDS applies it. The restoration is registered before it returns, so
// it is in place before the caller admits an operation.
func (m *CephFSSingleMDS) pauseJournal(t *testing.T, ctx context.Context) *override.ConfigHandle {
	t.Helper()
	pause, err := override.SetConfig(ctx, m.Cluster, ceph.ConfigSetting{Section: "mds", Name: "mds_log_pause", Value: "true"})
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := pause.Restore(restoreCtx); err != nil {
			t.Errorf("restore mds_log_pause: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	m.WaitDaemonConfig(t, ctx, "mds_log_pause", "true")
	return pause
}

// killMDS SIGKILLs the MDS, which drops anything that its paused journal
// held, and fails its GID so that the restarted daemon takes the rank at
// once. It returns the daemon and its container ID.
func (m *CephFSSingleMDS) killMDS(t *testing.T, ctx context.Context) (*cephfs.MDS, string) {
	t.Helper()
	var daemon *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == m.MDS.Name {
			daemon = member
		}
	}
	if daemon == nil || len(m.FS.MDSs()) != 1 || !m.MDS.Owned {
		t.Fatalf("active mds.%s is not the one owned MDS: %+v", m.MDS.Name, m.FS.MDSs())
	}
	cid := daemon.GetContainerID()
	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	if state, err := daemon.State(ctx); err != nil || state.Running {
		t.Fatalf("killed MDS still runs: %+v, %v", state, err)
	}
	if _, err := m.Cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(m.MDS.GID, 10)); err != nil {
		t.Fatal("fail MDS GID:", err)
	}
	return daemon, cid
}

// restartMDS starts the killed daemon again and waits until rank 0 is active
// under a new GID, in the same container, without mds_log_pause.
func (m *CephFSSingleMDS) restartMDS(t *testing.T, ctx context.Context, daemon *cephfs.MDS, cid string) cephfs.MDSStatus {
	t.Helper()
	if err := daemon.Start(ctx); err != nil {
		t.Fatal("restart MDS:", err)
	}
	var current cephfs.MDSStatus
	for {
		after, err := m.FS.MDSStatus(ctx)
		if err == nil && len(after.Active) == 1 && after.Active[0].State == "up:active" && after.Active[0].GID != m.MDS.GID {
			current = after.Active[0]
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("rank 0 did not come back: %+v, %v", after, err)
		case <-time.After(time.Second):
		}
	}
	if current.Name != m.MDS.Name || !current.Owned || daemon.GetContainerID() != cid || len(m.FS.MDSs()) != 1 {
		t.Fatalf("rank 0 is not served by the restarted mds.%s in container %s: %+v", m.MDS.Name, cid, current)
	}
	if paused := m.DaemonConfig(t, ctx, "mds_log_pause"); paused != "false" {
		t.Fatalf("restarted mds.%s runs with mds_log_pause=%s", m.MDS.Name, paused)
	}
	return current
}

// DaemonConfig reads a setting from the running MDS itself, not from the
// central configuration.
func (h *CephFSSingleMDS) DaemonConfig(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	out, err := h.Cluster.Ceph(ctx, "tell", "mds."+h.MDS.Name, "config", "get", name)
	var values map[string]string
	if err != nil || json.Unmarshal(out, &values) != nil {
		t.Fatalf("config get %s on mds.%s: %v: %.256s", name, h.MDS.Name, err, out)
	}
	return values[name]
}

func (h *CephFSSingleMDS) WaitDaemonConfig(t *testing.T, ctx context.Context, name, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for h.DaemonConfig(t, ctx, name) != want {
		if time.Now().After(deadline) {
			t.Fatalf("mds.%s did not apply %s=%s", h.MDS.Name, name, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// HistoricRename reads an MDS's dump_historic_ops and finds the client's
// rename with tid in it.
func (h *CephFSUnsafeHold) HistoricRename(t *testing.T, ctx context.Context, mds string, tid uint64) ([]MDSRequestOp, MDSRequestOp, bool) {
	t.Helper()
	historic, err := h.Cluster.Ceph(ctx, "tell", "mds."+mds, "dump_historic_ops")
	if err != nil {
		t.Fatal("dump_historic_ops:", err)
	}
	ops, err := ParseMDSRequestOps(historic)
	if err != nil {
		t.Fatalf("dump_historic_ops: %v: %.1024s", err, historic)
	}
	op, found := FindMDSRequest(ops, h.Baseline.Client, "rename", tid)
	return ops, op, found
}

// HoldCephFSUnsafeRename starts a cluster with one filesystem and one active
// MDS, and an actor with a fsynced file. It sets mds_log_pause through
// override.SetConfig, reads it back from the running daemon, and lets the actor
// rename the file once. The rename must return with the early reply, and
// twice, 3 s apart, the MDS must list the client's request in flight after
// "early_replied" and without "journal_committed" while the client's admin
// socket lists the same TID as got_unsafe. The pause's restoration and the
// actor's cancellation and join are registered before the rename is admitted.
func HoldCephFSUnsafeRename(t *testing.T, ctx context.Context) *CephFSUnsafeHold {
	t.Helper()
	h := &CephFSUnsafeHold{CephFSSingleMDS: StartCephFSSingleMDS(t, ctx)}
	cluster, client, fs := h.Cluster, h.Client, h.FS
	if early := h.DaemonConfig(t, ctx, "mds_early_reply"); early != "true" {
		t.Fatalf("mds.%s runs with mds_early_reply=%s", h.MDS.Name, early)
	}

	const workdir = "/tmp/unsafe-actor"
	h.Asok = workdir + "/client.asok"
	h.Actor = StartCephFSActor(t, ctx, cluster, client, CephFSUnsafeActor, fs.FilesystemName, workdir, "/unsafe", "/unsafe/source", "/unsafe/target", h.Asok)
	h.Baseline = h.Actor.Receipt(t, ctx, 1)
	if h.Baseline.Client == 0 || h.Baseline.Inode == 0 || h.Baseline.Size != int64(len("unsafe-window")) {
		t.Fatalf("baseline receipt = %+v", h.Baseline)
	}
	// The pause's restoration is registered before the rename is admitted;
	// the actor's cancellation and join were registered when it started.
	h.Pause = h.pauseJournal(t, ctx)

	h.Actor.Open(t, ctx, "rename")
	h.Renamed = h.Actor.Receipt(t, ctx, 2)
	if h.Renamed.Errno != 0 || h.Renamed.Error != "" || h.Renamed.PID != h.Baseline.PID {
		t.Fatalf("rename receipt = %+v", h.Renamed)
	}
	// The window is shown by native state, not by the call's return: both
	// sides must hold the same request, twice, 3 s apart. The first look
	// takes the TID from the MDS; it is fixed from then on.
	for look := range 2 {
		observed := observeUnsafeRename(t, ctx, cluster, client, h.MDS.Name, h.Asok, h.Baseline.Client, h.Held.Tid)
		inode := fmt.Sprintf("0x%x", h.Baseline.Inode)
		if !observed.InFlight || RequestSafety(observed.op) != "unsafe" || !observed.Pending || !bool(observed.Request.GotUnsafe) ||
			observed.Request.Op != "rename" || observed.Request.MDS != 0 || observed.Request.TargetIno != inode {
			t.Fatalf("look %d: the rename is not held between its unsafe and safe replies: %s", look, observed.raw)
		}
		h.Held = observed
		if look == 0 {
			time.Sleep(3 * time.Second)
		}
	}
	t.Logf("CEPHFS_UNSAFE held: %s", h.Held.raw)
	return h
}

// ReadNamespace checks the result with a client in another container: the
// new name has the file's original inode and the old name is gone. It returns
// the reader's client ID.
func (h *CephFSUnsafeHold) ReadNamespace(t *testing.T, ctx context.Context) uint64 {
	t.Helper()
	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := h.Cluster.RunNativeCommand(readCtx, ToolkitContainer(t, h.Cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", `import cephfs,json,sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    target = fs.stat(b"/unsafe/target").st_ino
    try:
        fs.stat(b"/unsafe/source")
        source = 0
    except cephfs.ObjectNotFound:
        source = 2
    print(json.dumps({"client": fs.get_instance_id(), "target": target, "source_errno": source}))
finally:
    fs.unmount()
    fs.shutdown()`, h.FS.FilesystemName}, Timeout: 2 * time.Minute, MaxStdout: 4096, MaxStderr: 64 << 10, Label: "independent namespace reader",
	})
	stop()
	var namespace struct {
		Client      uint64 `json:"client"`
		Target      uint64 `json:"target"`
		SourceErrno int    `json:"source_errno"`
	}
	if err != nil || read.ExitCode != 0 || json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &namespace) != nil ||
		namespace.Target != h.Baseline.Inode || namespace.SourceErrno != 2 || namespace.Client == h.Baseline.Client {
		t.Fatalf("independent reader: exit=%d err=%v out=%.512s stderr=%.512s", read.ExitCode, err, read.Stdout, read.Stderr)
	}
	return namespace.Client
}

// finish lets the actor stat the new name through its own mount and unmount.
func (h *CephFSUnsafeHold) Finish(t *testing.T, ctx context.Context) {
	t.Helper()
	h.Actor.Open(t, ctx, "finish")
	if closed := h.Actor.Receipt(t, ctx, 3); !closed.Closed || closed.Inode != h.Baseline.Inode || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt = %+v", closed)
	}
	h.Actor.Wait(t, ctx)
}

// RunCephFSUnsafeRequestWindow holds one rename between its early, unsafe
// reply and its journal commit, and observes that window from both sides
// (HoldCephFSUnsafeRename). After the pause is restored, the client no longer
// holds the request, the MDS's historic ops show it committed and replied,
// and an independent client finds the new name with the original inode. One
// active MDS keeps its GID throughout.
func RunCephFSUnsafeRequestWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	h := HoldCephFSUnsafeRename(t, ctx)
	tid := h.Held.Tid
	if err := h.Pause.Restore(ctx); err != nil {
		t.Fatal("restore mds_log_pause:", err)
	}
	h.WaitDaemonConfig(t, ctx, "mds_log_pause", "false")
	var committed MDSRequestOp
	deadline := time.Now().Add(time.Minute)
	for {
		observed := observeUnsafeRename(t, ctx, h.Cluster, h.Client, h.MDS.Name, h.Asok, h.Baseline.Client, tid)
		ops, op, _ := h.HistoricRename(t, ctx, h.MDS.Name, tid)
		if RenameSafe(observed.unsafeRenameState, ops, h.Baseline.Client) {
			committed = op
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the rename did not become safe: %s; historic %q %v", observed.raw, op.Description, op.Events())
		}
		time.Sleep(500 * time.Millisecond)
	}
	after, err := h.FS.MDSStatus(ctx)
	if err != nil || len(after.Active) != 1 || after.Active[0].GID != h.MDS.GID {
		t.Fatalf("mds.%s changed during the window: %+v, %v", h.MDS.Name, after, err)
	}
	reader := h.ReadNamespace(t, ctx)
	h.Finish(t, ctx)
	t.Logf("CEPHFS_UNSAFE %s mds.%s gid=%d client.%d tid=%d inode=%d: rename returned in %d ms with mds_log_pause applied; held unsafe on both sides for two looks 3 s apart; after the restore committed and replied (events %v, %.1f s in the MDS); reader client.%d found the target and ENOENT for the source",
		h.Version, h.MDS.Name, h.MDS.GID, h.Baseline.Client, tid, h.Baseline.Inode, h.Renamed.ElapsedMS, committed.Events(), committed.Duration, reader)
}

// ReplayedRequest reports whether an MDS op is a client's replay of a request
// that got an unsafe reply from an earlier MDS generation: its description
// carries the REPLAY flag, the MDS sent no early reply, and it journaled the
// request before replying.
func ReplayedRequest(op MDSRequestOp) bool {
	if !slices.Contains(strings.Fields(op.Description), "REPLAY") {
		return false
	}
	committed, replied := -1, -1
	for index, event := range op.Events() {
		switch {
		case event == "early_replied":
			return false
		case strings.HasPrefix(event, "journal_committed") && committed < 0:
			committed = index
		case event == "replying" && replied < 0:
			replied = index
		}
	}
	return committed >= 0 && replied > committed
}

// RunCephFSUnsafeRequestReplay holds one rename unsafe
// (HoldCephFSUnsafeRename), then SIGKILLs the MDS, so the request never
// reaches the journal. The pause is restored while no MDS runs, and the same
// daemon starts again in its container with a new GID. Meanwhile, and after
// the restart until the replay, the same client keeps the TID as got_unsafe.
// The restarted daemon must have mds_log_pause false, and its historic ops
// must show the client's request with that TID, flagged REPLAY, journaled and
// replied without an early reply. The client then no longer holds it, and an
// independent client finds the new name with the original inode. The test
// issues no second rename; the replay is the client library's own.
func RunCephFSUnsafeRequestReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	h := HoldCephFSUnsafeRename(t, ctx)
	tid := h.Held.Tid
	// SIGKILL drops the paused journal entry with the process.
	daemon, cid := h.killMDS(t, ctx)
	killed := time.Now()
	// The pause goes before the daemon starts again, so the restarted
	// process never reads it.
	if err := h.Pause.Restore(ctx); err != nil {
		t.Fatal("restore mds_log_pause:", err)
	}
	pending := observeClientRequest(t, ctx, h, tid)
	if !pending.found || !bool(pending.request.GotUnsafe) || pending.request.Op != "rename" {
		t.Fatalf("with no MDS the client does not hold rename %d as unsafe: %s", tid, pending.raw)
	}
	current := h.restartMDS(t, ctx, daemon, cid)
	recovered := time.Since(killed)

	var replayed MDSRequestOp
	deadline := time.Now().Add(2 * time.Minute)
	for {
		request := observeClientRequest(t, ctx, h, tid)
		_, op, found := h.HistoricRename(t, ctx, current.Name, tid)
		if found && ReplayedRequest(op) && !request.found {
			replayed = op
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rename %d was not replayed: client %s; historic found=%t %q %v", tid, request.raw, found, op.Description, op.Events())
		}
		time.Sleep(500 * time.Millisecond)
	}
	reader := h.ReadNamespace(t, ctx)
	h.Finish(t, ctx)
	t.Logf("CEPHFS_UNSAFE_REPLAY %s client.%d tid=%d inode=%d: rename returned in %d ms and was held unsafe on both sides; mds.%s killed (gid %d, cid %s) with the request unjournaled; with no MDS the client held it as unsafe (%s); restarted as gid %d after %s with mds_log_pause=false; replayed as %q, events %v; reader client.%d found the target and ENOENT for the source",
		h.Version, h.Baseline.Client, tid, h.Baseline.Inode, h.Renamed.ElapsedMS, h.MDS.Name, h.MDS.GID, cid, pending.raw, current.GID, recovered.Round(time.Second), replayed.Description, replayed.Events(), reader)
}

// cephFSClientRequest is the client's own view of one request, read from its
// admin socket alone, for when no MDS can be asked.
type cephFSClientRequest struct {
	request ClientMDSRequest
	found   bool
	raw     string
}

func observeClientRequest(t *testing.T, ctx context.Context, h *CephFSUnsafeHold, tid uint64) cephFSClientRequest {
	t.Helper()
	requests := ClientMDSRequests(t, ctx, h.Cluster, h.Client, h.Asok)
	observed := cephFSClientRequest{raw: fmt.Sprintf("client requests %+v", requests)}
	for _, request := range requests {
		if request.TID == tid {
			observed.request, observed.found = request, true
		}
	}
	return observed
}

// cephFSFlushActor mounts once with a client admin socket at a given path,
// creates a file in a new directory, writes and fsyncs the base bytes and
// publishes receipt 1. At the "append" gate it writes the tail behind them
// through the same fd, publishes receipt 2 with the size that its own mount
// sees, and fsyncs, once, without a retry; receipt 3 has the result and the
// time the fsync took. At the "finish" gate it reads the whole file through
// the fd, closes, unmounts and publishes receipt 4. Arguments after the
// common ones: directory, path, the hex of the base bytes and of the tail,
// admin socket path.
const cephFSFlushActor CephFSActorScript = CephFSActorHelpers + `
directory, path = sys.argv[4].encode(), sys.argv[5].encode()
base, tail = bytes.fromhex(sys.argv[6]), bytes.fromhex(sys.argv[7])
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
publish(1, client=fs.get_instance_id(), inode=stat.st_ino, size=stat.st_size)
gate("append")
fs.write(fd, tail, len(base))
publish(2, size=fs.fstat(fd).st_size)
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
    closing.update(inode=stat.st_ino, size=stat.st_size, full=fs.read(fd, 0, stat.st_size).hex())
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(4, closed=True, **closing)
`

// ClientInodeCaps is one inode in a libcephfs client's dump_cache: its size
// as the client sees it, the capabilities it changed and has not sent, and
// the ones it sent in a cap flush that the MDS has not acknowledged, with the
// flush TIDs. Ceph spells the flushing field "flushings_caps".
type ClientInodeCaps struct {
	Ino          string            `json:"ino"`
	SnapID       *string           `json:"snapid"`
	Size         uint64            `json:"size"`
	DirtyCaps    string            `json:"dirty_caps"`
	FlushingCaps string            `json:"flushings_caps"`
	FlushingTIDs map[string]uint64 `json:"flushing_cap_tid"`
}

// flushing reports whether a cap flush of the inode waits for its ack.
func (c ClientInodeCaps) Flushing() bool {
	return c.FlushingCaps != "" && len(c.FlushingTIDs) > 0
}

// ParseClientCache finds inode in dump_cache, which lists inode and dentry
// objects in one array, bare or under "cache". A dentry also names the inode
// it links ("ino") but has no "snapid"; the inode itself is the entry with
// the head snapshot ID. found is false when the client does not cache it.
func ParseClientCache(data []byte, inode uint64) (ClientInodeCaps, bool, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		var wrapped struct {
			Cache *[]json.RawMessage `json:"cache"`
		}
		if json.Unmarshal(data, &wrapped) != nil || wrapped.Cache == nil {
			return ClientInodeCaps{}, false, fmt.Errorf("dump_cache is neither an array nor a cache object: %v", err)
		}
		entries = *wrapped.Cache
	}
	want := fmt.Sprintf("0x%x", inode)
	for _, entry := range entries {
		var caps ClientInodeCaps
		if err := json.Unmarshal(entry, &caps); err != nil {
			return ClientInodeCaps{}, false, fmt.Errorf("dump_cache entry: %v: %.256s", err, entry)
		}
		if caps.Ino == want && caps.SnapID != nil && *caps.SnapID == "head" {
			return caps, true, nil
		}
	}
	return ClientInodeCaps{}, false, nil
}

// clientInode reads inode from the actor's dump_cache, bounded. A client that
// does not cache the inode fails the test.
func (m *CephFSSingleMDS) clientInode(t *testing.T, ctx context.Context, asok string, inode uint64) (ClientInodeCaps, string) {
	t.Helper()
	queryCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	result, err := m.Cluster.RunNativeCommand(queryCtx, m.Client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "dump_cache"}, Timeout: 30 * time.Second,
		MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "client dump_cache",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("client dump_cache: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
	}
	caps, found, err := ParseClientCache(result.Stdout, inode)
	if err != nil || !found {
		t.Fatalf("client dump_cache has no inode 0x%x (%v): %.1024s", inode, err, result.Stdout)
	}
	return caps, fmt.Sprintf("client size=%d dirty=%q flushing=%q tids=%v", caps.Size, caps.DirtyCaps, caps.FlushingCaps, caps.FlushingTIDs)
}

// MDSInodeSize reads an inode's size from the MDS's own "dump inode": the
// size that the MDS applied, not one a client holds in a flush.
func (m *CephFSSingleMDS) MDSInodeSize(t *testing.T, ctx context.Context, mds string, inode uint64) uint64 {
	t.Helper()
	size, err := MDSInodeSizeOf(ctx, m.Cluster, mds, inode)
	if err != nil {
		t.Fatal(err)
	}
	return size
}

// MDSInodeSizeOf is MDSInodeSize for an observe probe.
func MDSInodeSizeOf(ctx context.Context, cluster *ceph.Container, mds string, inode uint64) (uint64, error) {
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "dump", "inode", strconv.FormatUint(inode, 10))
	if err != nil {
		return 0, fmt.Errorf("dump inode %d on mds.%s: %w", inode, mds, err)
	}
	var dump struct {
		Size *uint64 `json:"size"`
	}
	if err := json.Unmarshal(out, &dump); err != nil || dump.Size == nil {
		return 0, fmt.Errorf("dump inode %d on mds.%s has no size: %v: %.512s", inode, mds, err, out)
	}
	return *dump.Size, nil
}

// published reports whether the actor has published receipt phase, without
// waiting for it.
func (run *CephFSActorRun) Published(ctx context.Context, phase int) bool {
	code, _, err := SessionExec(ctx, run.client, "test", "-e", run.workdir+"/receipt-"+strconv.Itoa(phase))
	return err == nil && code == 0
}

// RunCephFSFlushingCapReplay follows one cap flush across an MDS crash. The
// actor fsyncs a file's base bytes, then, with mds_log_pause applied on the
// running MDS, appends a tail through the same fd and fsyncs again. Its data
// reaches the OSDs, but the MDS cannot journal the cap flush that carries the
// new size, so it sends no ack: twice, 3 s apart, the client's dump_cache
// holds a flushing cap with a flush TID and the full size, the MDS's own
// dump inode still has the base size, and the fsync has not returned. The MDS
// is then SIGKILLed, so the flush never reaches the journal, and the pause is
// restored while no MDS runs; the client still holds the flush. After the
// daemon restarts in its container with a new GID, the client's flush is
// acknowledged without a second fsync: the client holds no flushing cap, the
// MDS has the full size, the original fsync returns 0, and a client in
// another container reads every byte.
func RunCephFSFlushingCapReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := StartCephFSSingleMDS(t, ctx)
	// The bytes travel as hex arguments; Linux limits one argument to
	// 128 KiB, so the file stays well below 64 KiB.
	base := make([]byte, 16<<10)
	for i := range base {
		base[i] = byte(i*7 + i>>8)
	}
	tail := []byte("\x00flushing-cap\xff\r\n\x1b")
	tail = append(tail, bytes.Repeat([]byte{0xa5, 0x5a}, 2048)...)
	whole := append(append([]byte{}, base...), tail...)

	const workdir = "/tmp/flush-actor"
	asok := workdir + "/client.asok"
	actor := StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSFlushActor, m.FS.FilesystemName, workdir,
		"/flush", "/flush/file", hex.EncodeToString(base), hex.EncodeToString(tail), asok)
	baseline := actor.Receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	if size := m.MDSInodeSize(t, ctx, m.MDS.Name, baseline.Inode); size != uint64(len(base)) {
		t.Fatalf("mds.%s has size %d for the fsynced base of %d bytes", m.MDS.Name, size, len(base))
	}
	// The pause's restoration is registered before the append is admitted;
	// the actor's cancellation and join were registered when it started.
	pause := m.pauseJournal(t, ctx)

	actor.Open(t, ctx, "append")
	if appended := actor.Receipt(t, ctx, 2); appended.Size != int64(len(whole)) || appended.PID != baseline.PID {
		t.Fatalf("append receipt = %+v", appended)
	}
	// The window is shown by native state on both sides, not by a blocked
	// call: twice, 3 s apart.
	var held string
	for look := range 2 {
		var caps ClientInodeCaps
		deadline := time.Now().Add(30 * time.Second)
		for {
			caps, held = m.clientInode(t, ctx, asok, baseline.Inode)
			if caps.Flushing() || time.Now().After(deadline) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		size := m.MDSInodeSize(t, ctx, m.MDS.Name, baseline.Inode)
		if !caps.Flushing() || caps.Size != uint64(len(whole)) || size != uint64(len(base)) || actor.Published(ctx, 3) {
			t.Fatalf("look %d: the cap flush is not held: %s; mds size=%d; fsync returned=%t", look, held, size, actor.Published(ctx, 3))
		}
		held += fmt.Sprintf("; mds size=%d", size)
		if look == 0 {
			time.Sleep(3 * time.Second)
		}
	}
	t.Logf("CEPHFS_FLUSH held: %s", held)

	daemon, cid := m.killMDS(t, ctx)
	killed := time.Now()
	// The pause goes before the daemon starts again, so the restarted
	// process never reads it.
	if err := pause.Restore(ctx); err != nil {
		t.Fatal("restore mds_log_pause:", err)
	}
	down, downRaw := m.clientInode(t, ctx, asok, baseline.Inode)
	if !down.Flushing() || actor.Published(ctx, 3) {
		t.Fatalf("with no MDS the client does not hold the cap flush: %s; fsync returned=%t", downRaw, actor.Published(ctx, 3))
	}
	current := m.restartMDS(t, ctx, daemon, cid)
	recovered := time.Since(killed)

	var flushed string
	deadline := time.Now().Add(2 * time.Minute)
	for {
		caps, raw := m.clientInode(t, ctx, asok, baseline.Inode)
		size := m.MDSInodeSize(t, ctx, current.Name, baseline.Inode)
		if !caps.Flushing() && size == uint64(len(whole)) {
			flushed = raw + fmt.Sprintf("; mds size=%d", size)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cap flush was not acknowledged: %s; mds size=%d", raw, size)
		}
		time.Sleep(500 * time.Millisecond)
	}
	synced := actor.Receipt(t, ctx, 3)
	if synced.Errno != 0 || synced.Error != "" || synced.PID != baseline.PID {
		t.Fatalf("fsync receipt = %+v", synced)
	}

	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := m.Cluster.RunNativeCommand(readCtx, ToolkitContainer(t, m.Cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", CephFSIndependentReader, m.FS.FilesystemName, "/flush/file"}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "independent CephFS reader",
	})
	stop()
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent reader: exit=%d err=%v stderr=%.2048s", read.ExitCode, err, read.Stderr)
	}
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int64  `json:"size"`
		Data   string `json:"data"`
	}
	if err := json.Unmarshal([]byte(LastJSONLine(string(read.Stdout))), &independent); err != nil {
		t.Fatalf("independent reader output: %v: %.512s", err, read.Stdout)
	}
	if independent.Inode != baseline.Inode || independent.Size != int64(len(whole)) || independent.Data != hex.EncodeToString(whole) || independent.Client == baseline.Client {
		t.Fatalf("independent reader saw client=%d inode=%d size=%d, want inode %d with %d bytes", independent.Client, independent.Inode, independent.Size, baseline.Inode, len(whole))
	}
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 4); !closed.Closed || closed.CloseErrno != 0 || closed.Full != hex.EncodeToString(whole) || closed.Inode != baseline.Inode {
		t.Fatalf("closing receipt: closed=%t errno=%d inode=%d size=%d", closed.Closed, closed.CloseErrno, closed.Inode, closed.Size)
	}
	actor.Wait(t, ctx)
	sum := sha256.Sum256(whole)
	t.Logf("CEPHFS_FLUSH_REPLAY %s client.%d inode=%d: %d base bytes fsynced, %d appended; with mds_log_pause the fsync held a cap flush on both sides for two looks 3 s apart; mds.%s killed (gid %d, cid %s); with no MDS: %s; restarted as gid %d after %s with mds_log_pause=false; acknowledged: %s; fsync returned 0 after %d ms; reader client.%d read %d bytes (sha256=%x)",
		m.Version, baseline.Client, baseline.Inode, len(base), len(tail), m.MDS.Name, m.MDS.GID, cid, downRaw, current.GID, recovered.Round(time.Second), flushed, synced.ElapsedMS, independent.Client, len(whole), sum)
}
