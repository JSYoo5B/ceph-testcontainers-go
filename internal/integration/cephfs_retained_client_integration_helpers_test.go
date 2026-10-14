//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSRetainedActor keeps one libcephfs mount and one O_RDWR fd open for
// its whole run. It writes and fsyncs the base bytes, publishes receipt 1 and
// waits at the "act" gate. Behind it, through the same fd and without
// mounting or opening again, it reads the base bytes, appends the tail,
// fsyncs, renames the file and sets a binary xattr, then publishes receipt 2.
// It closes everything after the "finish" gate and publishes receipt 3.
// Every receipt and gate carries the run's token; receipts are renamed into
// place, so a reader never sees half of one. Arguments: filesystem, work
// directory, token, path, new path, and the hex of the base bytes, the tail
// and the xattr value.
const cephFSRetainedActor = `
import cephfs, json, os, sys, time
name, workdir, token = sys.argv[1], sys.argv[2], sys.argv[3]
path, renamed = sys.argv[4].encode(), sys.argv[5].encode()
base, tail, xattr = (bytes.fromhex(arg) for arg in sys.argv[6:9])
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
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
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
    result.update(inode=stat.st_ino, size=stat.st_size)
except cephfs.Error as error:
    result.update(errno=getattr(error, "errno", None) or -1, error=str(error)[:200])
publish(2, **result)
gate("finish")
fs.close(fd)
fs.unmount()
fs.shutdown()
publish(3, closed=True)
`

// cephFSIndependentReader reads a file and its xattr through a mount of its
// own, in another container and process than the actor, and prints them with
// its client ID. It reads one byte more than the size to see extra data.
const cephFSIndependentReader = `
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
    xattr = fs.getxattr(path, "user.receipt", 255)
    print(json.dumps({"client": fs.get_instance_id(), "inode": stat.st_ino, "size": stat.st_size, "data": data.hex(), "xattr": xattr.hex()}))
finally:
    fs.unmount()
    fs.shutdown()
`

// cephFSActorReceipt is one receipt of cephFSRetainedActor. Python writes the
// integers exactly, and they decode into exact Go integers.
type cephFSActorReceipt struct {
	Token  string `json:"token"`
	Phase  int    `json:"phase"`
	PID    int    `json:"pid"`
	Client uint64 `json:"client"`
	Inode  uint64 `json:"inode"`
	Size   int64  `json:"size"`
	Read   string `json:"read"`
	Errno  int    `json:"errno"`
	Error  string `json:"error"`
	Closed bool   `json:"closed"`
}

// cephFSActorRun is a supervised actor and its receipts.
type cephFSActorRun struct {
	cluster *ceph.Container
	client  testcontainers.Container
	workdir string
	token   string
	cancel  context.CancelFunc
	done    chan struct{}
	result  ceph.NativeCommandResult
	err     error
}

// startCephFSActor starts the actor in client under RunNativeCommand and
// registers its cancellation and confirmed stop before anything can fail.
func startCephFSActor(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, filesystem, workdir string, args ...string) *cephFSActorRun {
	t.Helper()
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	execOutput(t, ctx, client, "mkdir", "-p", workdir)
	actorCtx, cancel := context.WithCancel(ctx)
	run := &cephFSActorRun{cluster: cluster, client: client, workdir: workdir, token: hex.EncodeToString(nonce), cancel: cancel, done: make(chan struct{})}
	argv := append([]string{"python3", "-c", cephFSRetainedActor, filesystem, workdir, run.token}, args...)
	go func() {
		defer close(run.done)
		run.result, run.err = cluster.RunNativeCommand(actorCtx, client, ceph.NativeCommand{
			Argv: argv, Timeout: 6 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "retained CephFS actor",
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(2 * time.Minute):
			t.Error("retained CephFS actor did not return after cancellation")
			return
		}
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(run.err, &unstopped) {
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
func (run *cephFSActorRun) exited(ctx context.Context) string {
	stderr := run.result.Stderr
	if len(stderr) > 4096 {
		stderr = stderr[len(stderr)-4096:]
	}
	if redacted, err := run.cluster.RedactSecrets(ctx, stderr); err == nil {
		stderr = redacted
	}
	return fmt.Sprintf("exit=%d timed_out=%t err=%v stderr=%s", run.result.ExitCode, run.result.TimedOut, run.err, stderr)
}

// receipt waits for receipt phase and rejects one of another run or phase.
// It fails at once when the actor ends without publishing it.
func (run *cephFSActorRun) receipt(t *testing.T, ctx context.Context, phase int) cephFSActorReceipt {
	t.Helper()
	path := run.workdir + "/receipt-" + strconv.Itoa(phase)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, out, err := sessionExec(ctx, run.client, "cat", path)
		if err == nil && code == 0 {
			decoder := json.NewDecoder(strings.NewReader(out))
			decoder.DisallowUnknownFields()
			var receipt cephFSActorReceipt
			if err := decoder.Decode(&receipt); err != nil {
				t.Fatalf("receipt %d: %v: %q", phase, err, out)
			}
			if receipt.Token != run.token || receipt.Phase != phase || receipt.PID <= 0 {
				t.Fatalf("receipt %d belongs to another run or phase: %+v", phase, receipt)
			}
			return receipt
		}
		select {
		case <-run.done:
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
func (run *cephFSActorRun) open(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	execOutput(t, ctx, run.client, "sh", "-c", `printf %s "$1" > "$2.tmp" && mv "$2.tmp" "$2"`, "sh", run.token, run.workdir+"/"+name)
}

// wait waits for the actor to end and requires a complete, successful run.
func (run *cephFSActorRun) wait(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-run.done:
	case <-ctx.Done():
		t.Fatal("actor did not end:", ctx.Err())
	}
	if run.err != nil || run.result.ExitCode != 0 || !run.result.Complete() {
		t.Fatal("actor failed:", run.exited(ctx))
	}
}

// cephFSRawSession returns the native "session ls" entry of client on one MDS,
// whose fields the Session view leaves out, such as num_leases and
// num_completed_requests. Integers stay raw JSON numbers.
func cephFSRawSession(t *testing.T, ctx context.Context, cluster *ceph.Container, mds string, client uint64) map[string]json.RawMessage {
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

// cephFSRawSessionSummary renders the raw counters that a log line keeps.
func cephFSRawSessionSummary(session map[string]json.RawMessage) string {
	var parts []string
	for _, field := range []string{"state", "num_caps", "num_leases", "num_completed_requests", "num_completed_flushes", "reconnecting"} {
		if value, ok := session[field]; ok {
			parts = append(parts, field+"="+string(value))
		}
	}
	return strings.Join(parts, " ")
}

// cephFSRetainedSession waits until rank 0 serves client with an open session
// and returns it.
func cephFSRetainedSession(t *testing.T, parent context.Context, fs *cephfs.Filesystem, client uint64) cephfs.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last []cephfs.Session
	var lastErr error
	for {
		last, lastErr = fs.Sessions(ctx)
		for _, session := range last {
			if session.ID == client && session.Rank == 0 && session.State == "open" {
				return session
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("client.%d has no open rank 0 session: error=%v sessions=%+v", client, lastErr, last)
		case <-time.After(time.Second):
		}
	}
}

// cephFSFailover selects which daemon takes rank 0 after the active MDS is
// killed.
type cephFSFailover int

const (
	// cephFSRestart starts the killed MDS's container again; the filesystem
	// has no standby.
	cephFSRestart cephFSFailover = iota
	// cephFSStandbyTakeover lets an idle standby take rank 0.
	cephFSStandbyTakeover
	// cephFSReplayTakeover lets the journal follower of rank 0 take it.
	cephFSReplayTakeover
)

func (f cephFSFailover) String() string {
	return [...]string{"restart", "standby", "standby-replay"}[f]
}

// testCephFSRetainedClient keeps one native client, its mount and an open fd
// across a SIGKILL of the filesystem's active MDS. Depending on failover,
// rank 0 then comes back in the killed daemon's restarted container with a
// new GID, or a standby or standby-replay daemon takes it over. The client's
// session continues with the same client ID, PID and address: the actor
// reads, appends, fsyncs, renames and sets an xattr through its original fd.
// A reader in another container then checks the inode, the bytes and the
// xattr. After a takeover the killed daemon starts again and must rejoin as a
// standby, a separate condition from the client's recovery. This proves
// reconnect of a client whose data was fsynced before the failure; it does
// not cover replay of an outstanding request or several ranks.
func testCephFSRetainedClient(t *testing.T, failover cephFSFailover, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, imageOpts := integrationImages(t)
	opts = append(append(imageOpts, ceph.WithMessengerMode(ceph.MessengerV2Secure)), opts...)
	if failover != cephFSRestart {
		opts = append(opts, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1, StandbyReplay: failover == cephFSReplayTakeover}))
	}
	cluster, client := newServiceClusterRun(t, cephfsRun, image, opts...)
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
	standbys, replays, members := 0, 0, 1
	switch failover {
	case cephFSStandbyTakeover:
		standbys, members = 1, 2
	case cephFSReplayTakeover:
		replays, members = 1, 2
	}
	if before.MaxMDS != 1 || len(before.Active) != 1 || len(before.Standby) != standbys || len(before.StandbyReplay) != replays || !before.Active[0].Owned || before.Active[0].Rank != 0 {
		t.Fatalf("want one owned active MDS, %d standby and %d standby-replay: %+v", standbys, replays, before)
	}
	old := before.Active[0]
	// takeover is the daemon that must take rank 0: the killed one itself
	// after its restart, or the standby.
	takeover := old
	switch failover {
	case cephFSStandbyTakeover:
		takeover = before.Standby[0]
	case cephFSReplayTakeover:
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
	const path, renamed = "/retained", "/retained-renamed"
	actor := startCephFSActor(t, ctx, cluster, client, fs.FilesystemName, "/tmp/retained-actor",
		path, renamed, hex.EncodeToString(base), hex.EncodeToString(tail), hex.EncodeToString(xattr))

	baseline := actor.receipt(t, ctx, 1)
	if baseline.Client == 0 || baseline.Inode == 0 || baseline.Size != int64(len(base)) {
		t.Fatalf("baseline receipt = %+v", baseline)
	}
	session := cephFSRetainedSession(t, ctx, fs, baseline.Client)
	if session.PID != strconv.Itoa(baseline.PID) || session.EntityID != "admin" || session.Address == "" || session.Caps == 0 {
		t.Fatalf("actor session = %+v, want PID %d", session, baseline.PID)
	}
	rawBefore := cephFSRawSession(t, ctx, cluster, old.Name, baseline.Client)
	t.Logf("CEPHFS_RETAINED baseline fscid=%d mds=%s gid=%d cid=%s client=%d pid=%d address=%s inode=%d raw: %s",
		before.FilesystemID, old.Name, old.GID, cid, baseline.Client, baseline.PID, session.Address, baseline.Inode, cephFSRawSessionSummary(rawBefore))

	// SIGKILL leaves the MDS no chance to hand off its rank. Failing its GID
	// lets the successor take rank 0 at once.
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
	if failover == cephFSRestart {
		if err := daemon.Start(ctx); err != nil {
			t.Fatal("restart MDS:", err)
		}
	}
	var after *cephfs.FilesystemStatus
	for {
		after, err = fs.MDSStatus(ctx)
		if err == nil && len(after.Active) == 1 && after.Active[0].State == "up:active" && after.Active[0].GID != old.GID {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("rank 0 did not come back: %+v, %v", after, err)
		case <-time.After(time.Second):
		}
	}
	current := after.Active[0]
	recovered := time.Since(failed)
	// The restarted daemon has a new GID; a standby keeps the GID it had.
	wantGID := current.GID
	if failover != cephFSRestart {
		wantGID = takeover.GID
	}
	if after.FilesystemID != before.FilesystemID || current.Name != takeover.Name || current.GID != wantGID || current.Rank != 0 || !current.Owned || daemon.GetContainerID() != cid || len(fs.MDSs()) != members {
		t.Fatalf("rank 0 is not served by mds.%s (GID %d) of the same filesystem: %+v", takeover.Name, wantGID, after)
	}

	actor.open(t, ctx, "act")
	acted := actor.receipt(t, ctx, 2)
	if acted.PID != baseline.PID || acted.Errno != 0 || acted.Error != "" {
		t.Fatalf("retained fd operations failed: %+v", acted)
	}
	if acted.Read != hex.EncodeToString(base) || acted.Inode != baseline.Inode || acted.Size != int64(len(want)) {
		t.Fatalf("retained fd saw %+v, want inode %d and %d bytes", acted, baseline.Inode, len(want))
	}

	readCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	read, err := cluster.RunNativeCommand(readCtx, cluster.ControlContainer(), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSIndependentReader, fs.FilesystemName, renamed}, Timeout: 2 * time.Minute,
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
	if err := json.Unmarshal([]byte(lastJSONLine(string(read.Stdout))), &independent); err != nil {
		t.Fatalf("independent reader output: %v: %.512s", err, read.Stdout)
	}
	data, err := hex.DecodeString(independent.Data)
	if err != nil || !bytes.Equal(data, want) || independent.Size != int64(len(want)) || independent.Inode != baseline.Inode || independent.XAttr != hex.EncodeToString(xattr) || independent.Client == baseline.Client {
		t.Fatalf("independent reader saw client=%d inode=%d size=%d xattr=%s data=%s, want inode %d and %x", independent.Client, independent.Inode, independent.Size, independent.XAttr, independent.Data, baseline.Inode, want)
	}

	// The actor is still mounted; its session is the one it opened before
	// the failure, now served by the new GID.
	retained := cephFSRetainedSession(t, ctx, fs, baseline.Client)
	if retained.PID != session.PID || retained.Address != session.Address || retained.EntityID != session.EntityID {
		t.Fatalf("session after the restart = %+v, before = %+v", retained, session)
	}
	rawAfter := cephFSRawSession(t, ctx, cluster, current.Name, baseline.Client)
	actor.open(t, ctx, "finish")
	if closed := actor.receipt(t, ctx, 3); !closed.Closed || closed.PID != baseline.PID {
		t.Fatalf("closing receipt = %+v", closed)
	}
	actor.wait(t, ctx)
	sum := sha256.Sum256(want)
	t.Logf("CEPHFS_RETAINED %s fscid=%d rank0 %s/%d -> %s/%d after %s, killed cid=%s client=%d pid=%d address=%s inode=%d sha256=%x reader=%d raw: %s",
		failover, after.FilesystemID, old.Name, old.GID, current.Name, current.GID, recovered.Round(time.Second), cid, baseline.Client, baseline.PID, retained.Address, baseline.Inode, sum, independent.Client, cephFSRawSessionSummary(rawAfter))
	if failover == cephFSRestart {
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
	if failover == cephFSReplayTakeover {
		rejoined = restored.StandbyReplay
	}
	if len(restored.Active) != 1 || restored.Active[0].GID != current.GID || len(rejoined) != 1 || rejoined[0].Name != old.Name || rejoined[0].GID == old.GID || daemon.GetContainerID() != cid {
		t.Fatalf("mds.%s did not rejoin as a %s with a new GID: %+v", old.Name, failover, restored)
	}
	t.Logf("CEPHFS_RETAINED %s capacity restored: mds.%s rejoined with GID %d", failover, old.Name, rejoined[0].GID)
}
