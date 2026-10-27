//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSFlockActor is an actor that keeps one mount and one O_RDWR fd on a
// file and takes flock locks on it. It creates the file's directory if it is
// missing, opens the file and publishes receipt 1 with its client ID and the
// file's inode. Then for each operation in its list it waits at the gate
// "op-<n>", runs it and publishes receipt n+2 with its errno and how long the
// call took. An operation is "ex-nb" (LOCK_EX|LOCK_NB), "ex" (a blocking
// LOCK_EX) or "un" (LOCK_UN), always with lock owner 1 of this client. After
// the "finish" gate it closes the fd, unmounts and publishes the last receipt.
// Arguments after the common ones: the file and the comma-separated
// operations.
const cephFSFlockActor cephFSActorScript = cephFSActorHelpers + `
import fcntl
path, operations = sys.argv[4].encode(), sys.argv[5].split(",")
modes = {"ex-nb": fcntl.LOCK_EX | fcntl.LOCK_NB, "ex": fcntl.LOCK_EX, "un": fcntl.LOCK_UN}
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
try:
    fs.mkdir(os.path.dirname(path), 0o755)
except cephfs.ObjectExists:
    pass
fd = fs.open(path, os.O_CREAT | os.O_RDWR, 0o644)
publish(1, client=fs.get_instance_id(), inode=fs.fstat(fd).st_ino)
for index, operation in enumerate(operations):
    gate("op-%d" % index)
    started = time.monotonic()
    result = {"errno": 0}
    try:
        fs.flock(fd, modes[operation], 1)
    except cephfs.Error as error:
        result["errno"] = getattr(error, "errno", None) or -1
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    publish(index + 2, **result)
gate("finish")
closing = {}
try:
    fs.close(fd)
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=getattr(error, "errno", None) or -1)
fs.shutdown()
publish(len(operations) + 2, closed=True, **closing)
`

// flockActor is one cephFSFlockActor with its operations.
type flockActor struct {
	*cephFSActorRun
	client uint64
	inode  uint64
	ops    []string
}

func startFlockActor(t *testing.T, ctx context.Context, m *cephFSSingleMDS, in testcontainers.Container, workdir, path string, ops ...string) *flockActor {
	t.Helper()
	run := startCephFSActor(t, ctx, m.cluster, in, cephFSFlockActor, m.fs.FilesystemName, workdir, path, strings.Join(ops, ","))
	opened := run.receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Inode == 0 {
		t.Fatalf("flock actor receipt 1 = %+v", opened)
	}
	return &flockActor{cephFSActorRun: run, client: opened.Client, inode: opened.Inode, ops: ops}
}

// start opens the gate of operation index without waiting for its receipt.
func (a *flockActor) start(t *testing.T, ctx context.Context, index int) {
	t.Helper()
	a.open(t, ctx, fmt.Sprintf("op-%d", index))
}

// run runs operation index and returns its receipt.
func (a *flockActor) run(t *testing.T, ctx context.Context, index int) cephFSActorReceipt {
	t.Helper()
	a.start(t, ctx, index)
	return a.receipt(t, ctx, index+2)
}

// close lets the actor close its fd and unmount, and waits for it.
func (a *flockActor) close(t *testing.T, ctx context.Context) {
	t.Helper()
	a.open(t, ctx, "finish")
	if closed := a.receipt(t, ctx, len(a.ops)+2); !closed.Closed || closed.CloseErrno != 0 {
		t.Fatalf("closing receipt of client.%d = %+v", a.client, closed)
	}
	a.wait(t, ctx)
}

// waitingFlock is the MDS's view of one client's blocked flock request.
type waitingFlock struct {
	tid         uint64
	description string
	flagPoint   string
	events      []string
}

// mdsWaitingFlock observes, bounded, the MDS's dump_ops_in_flight until it
// holds client's setfilelock request with present, or no longer holds it
// without present.
func mdsWaitingFlock(t *testing.T, ctx context.Context, m *cephFSSingleMDS, client uint64, present bool) waitingFlock {
	t.Helper()
	seen, err := observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (waitingFlock, bool, error) {
		data, err := m.cluster.Ceph(probeCtx, "tell", "mds."+m.mds.Name, "dump_ops_in_flight")
		if err != nil {
			return waitingFlock{}, false, err
		}
		ops, err := parseMDSRequestOps(data)
		if err != nil {
			return waitingFlock{}, false, fmt.Errorf("%w: %.512s", err, data)
		}
		op, found := findMDSRequest(ops, client, "setfilelock", 0)
		if !found {
			return waitingFlock{}, !present, nil
		}
		return waitingFlock{tid: op.TypeData.ReqID.TID, description: op.Description, flagPoint: op.TypeData.FlagPoint, events: op.events()}, present, nil
	})
	if err != nil {
		t.Fatalf("client.%d setfilelock in flight=%t not observed: last %+v: %v", client, present, seen.Value, err)
	}
	return seen.Value
}

// testCephFSFlockContention lets two live clients, in two containers,
// contend for one flock on one file. The holder takes an exclusive lock; the
// contender's non-blocking exclusive lock returns EWOULDBLOCK, and its
// blocking one waits in the MDS: dump_ops_in_flight holds its setfilelock
// request (rule 2 is flock, type 2 exclusive, wait 1) at "failed to add lock,
// waiting", and the call does not return. When the holder unlocks, the MDS
// grants the waiting lock: the contender's call returns 0 and the request
// leaves the MDS, and now the former holder's non-blocking lock returns
// EWOULDBLOCK. Neither client is evicted or blocklisted.
func testCephFSFlockContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	const path = "/locks/file"
	holder := startFlockActor(t, ctx, m, m.client, "/tmp/flock-holder", path, "ex-nb", "un", "ex-nb")
	contender := startFlockActor(t, ctx, m, toolkitContainer(t, m.cluster), "/tmp/flock-contender", path, "ex-nb", "ex", "un")
	if holder.client == contender.client || holder.inode != contender.inode {
		t.Fatalf("holder client.%d inode %d, contender client.%d inode %d", holder.client, holder.inode, contender.client, contender.inode)
	}
	holderSession := cephFSRetainedSession(t, ctx, m.fs, holder.client, 0)
	contenderSession := cephFSRetainedSession(t, ctx, m.fs, contender.client, 0)

	if locked := holder.run(t, ctx, 0); locked.Errno != 0 {
		t.Fatalf("holder's exclusive lock: %+v", locked)
	}
	refused := contender.run(t, ctx, 0)
	if refused.Errno != 11 {
		t.Fatalf("contender's non-blocking lock while the holder holds it: %+v, want EWOULDBLOCK (11)", refused)
	}

	contender.start(t, ctx, 1)
	waiting := mdsWaitingFlock(t, ctx, m, contender.client, true)
	inode := fmt.Sprintf("#0x%x ", holder.inode)
	if waiting.flagPoint != "failed to add lock, waiting" || !strings.Contains(waiting.description, "rule 2, type 2,") ||
		!strings.Contains(waiting.description, "wait 1 ") || !strings.Contains(waiting.description, inode) {
		t.Fatalf("the MDS does not hold client.%d's exclusive flock waiting on inode %d: %+v", contender.client, holder.inode, waiting)
	}
	// The MDS keeps the request waiting, and the call does not return.
	time.Sleep(3 * time.Second)
	if still := mdsWaitingFlock(t, ctx, m, contender.client, true); still.tid != waiting.tid || still.flagPoint != waiting.flagPoint {
		t.Fatalf("waiting flock changed: %+v, before %+v", still, waiting)
	}
	if contender.published(ctx, 3) {
		t.Fatal("the contender's blocking lock returned while the holder held the lock")
	}

	unlocked := time.Now()
	if released := holder.run(t, ctx, 1); released.Errno != 0 {
		t.Fatalf("holder's unlock: %+v", released)
	}
	granted := contender.receipt(t, ctx, 3)
	if granted.Errno != 0 || time.Duration(granted.ElapsedMS)*time.Millisecond < 3*time.Second {
		t.Fatalf("contender's blocking lock: %+v, want 0 after waiting for the unlock", granted)
	}
	grantedAfter := time.Since(unlocked)
	mdsWaitingFlock(t, ctx, m, contender.client, false)
	reversed := holder.run(t, ctx, 2)
	if reversed.Errno != 11 {
		t.Fatalf("former holder's non-blocking lock while the contender holds it: %+v, want EWOULDBLOCK (11)", reversed)
	}
	if released := contender.run(t, ctx, 2); released.Errno != 0 {
		t.Fatalf("contender's unlock: %+v", released)
	}

	entries, err := observeCluster(m.cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []struct {
		before cephfs.Session
		client uint64
	}{{holderSession, holder.client}, {contenderSession, contender.client}} {
		if after := cephFSRetainedSession(t, ctx, m.fs, session.client, 0); after.Address != session.before.Address || after.PID != session.before.PID {
			t.Fatalf("client.%d session %+v, before %+v", session.client, after, session.before)
		}
		if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.before.Address }) {
			t.Fatalf("client.%d %s was blocklisted: %+v", session.client, session.before.Address, entries)
		}
	}
	holder.close(t, ctx)
	contender.close(t, ctx)
	t.Logf("CEPHFS_FLOCK_RECEIPT %s mds.%s inode=%d: holder client.%d LOCK_EX|LOCK_NB=0; contender client.%d LOCK_EX|LOCK_NB=%d; its blocking LOCK_EX waited in the MDS as %q at %q (events %v); holder LOCK_UN=0, the waiting lock was granted %d ms after it started (%s after the unlock) and left the MDS; holder LOCK_EX|LOCK_NB then %d; both sessions kept, neither blocklisted",
		m.version, m.mds.Name, holder.inode, holder.client, contender.client, refused.Errno, waiting.description, waiting.flagPoint, waiting.events,
		granted.ElapsedMS, grantedAfter.Round(time.Millisecond), reversed.Errno)
}
