//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

package cephfs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// testCephFSFcntlSetlkwReplay shows that a POSIX record lock request blocked
// in F_SETLKW is replayed across an MDS standby takeover and granted only
// after the holder unlocks. Two ceph-fuse mounts of one client hold one file:
// the holder takes a write lock on the first bytes, and the contender's
// blocking F_SETLKW for the same range waits in the active MDS
// (dump_ops_in_flight shows its setfilelock request at "failed to add lock,
// waiting"). The active MDS is then killed; the standby takes over, replays
// the queued request, and still holds it waiting, with the contender's call
// not returned. When the holder unlocks, the new MDS grants the waiting lock:
// the contender's F_SETLKW returns 0, its request leaves the MDS, and an
// independent client reads the file's bytes. The lock range, the holder's
// PID that F_GETLK reported before the takeover, and the waiting request's
// shape are kept in the receipt.
func testCephFSFcntlSetlkwReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))
	before, err := m.FS.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 || len(before.Standby) != 1 || !before.Standby[0].Owned {
		t.Fatalf("one owned active and standby required: %+v, %v", before, err)
	}
	old, successor := before.Active[0], before.Standby[0]
	var daemon *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == old.Name {
			daemon = member
		}
	}
	if daemon == nil || old.GID == 0 || successor.GID == 0 || old.GID == successor.GID {
		t.Fatal("missing distinct owned MDS generations")
	}
	image, refusal := fuseClientImage(t, ctx, m)
	if refusal != nil {
		t.Logf("selected explicit opt-in FUSE image after %v", refusal)
	}
	const root, point = "/fcntl-setlkw", "/mnt/cephfs"
	var made struct {
		Inode uint64 `json:"inode"`
	}
	fuseExec(t, ctx, harness.ToolkitContainer(t, m.Cluster), &made, `
import cephfs, json, sys
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    fs.mkdir(sys.argv[2].encode(), 0o755)
    print(json.dumps({"inode": fs.stat(sys.argv[2].encode()).st_ino}))
finally:
    fs.unmount()
    fs.shutdown()
`, m.FS.FilesystemName, root)
	if made.Inode == 0 {
		t.Fatal("missing root directory inode")
	}
	identity, err := m.Cluster.CreateClient(ctx, "fcntl-setlkw", ceph.ClientCaps{
		Mon: "allow r", MDS: "allow rw path=" + root, OSD: "allow rw tag cephfs data=" + m.FS.FilesystemName,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mounts []*fuseMount
	var states []fuseMountState
	for range 2 {
		ctr, err := startFuseClient(t, ctx, m, image, identity, fuseMinimumSettings)
		if err != nil {
			t.Fatal(err)
		}
		if _, refused := admitFuse(t, ctx, ctr); refused != nil {
			t.Fatal(refused)
		}
		inspected, err := ctr.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		image = inspected.Image // The second client reuses the first's immutable image.
		mount := startFuseMount(t, ctx, m, ctr, identity, root, point)
		mounts = append(mounts, mount)
		states = append(states, mount.ready(t, ctx, m.FS))
	}
	if states[0].session.ID == states[1].session.ID {
		t.Fatal("the two FUSE mounts share a session")
	}
	cleaned := make([]bool, len(mounts))
	t.Cleanup(func() {
		for i, mount := range mounts {
			if cleaned[i] {
				continue
			}
			cleanCtx, stop := context.WithTimeout(context.Background(), 90*time.Second)
			if err := cleanFcntlTakeoverMount(cleanCtx, m, mount); err != nil {
				t.Errorf("owned FUSE cleanup: %v", err)
			}
			stop()
		}
	})

	harness.ExecOutput(t, ctx, mounts[0].ctr, "mkdir", "-p", point+"/locks")
	const lockFile = point + "/locks/file"
	holder := startFcntlActor(t, ctx, m, mounts[0].ctr, "/tmp/fcntl-setlkw-holder", lockFile, "wr-nb", "un")
	contender := startFcntlActor(t, ctx, m, mounts[1].ctr, "/tmp/fcntl-setlkw-contender", lockFile, "get", "wr", "un")
	if holder.inode != contender.inode {
		t.Fatalf("holder inode %d, contender inode %d", holder.inode, contender.inode)
	}
	clientB := states[1].session.ID

	receipt := map[string]any{"filesystem": m.FS.FilesystemName, "inode": holder.inode, "before_mds": old.Name, "before_gid": old.GID,
		"successor": successor.Name, "successor_gid": successor.GID, "holder_client": states[0].session.ID, "contender_client": clientB}
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("CEPHFS_FCNTL_SETLKW_REPLAY_RECEIPT %s", data)
	}()

	if locked := holder.run(t, ctx, 0); locked.Errno != 0 {
		t.Fatalf("holder's F_SETLK write lock: %+v", locked)
	}
	// F_GETLK across the two clients reports the holder's write lock on the
	// range through the MDS. The holder is a remote client, so its PID is not
	// attributed: CephFS reports l_pid 0 for a lock held by another client.
	queried := contender.run(t, ctx, 0)
	if queried.Errno != 0 || queried.Lock == nil || queried.Lock.Type != 1 || queried.Lock.Start != 0 || queried.Lock.Len != 4096 {
		t.Fatalf("contender's F_GETLK: %+v lock %+v, want the holder's F_WRLCK on [0,4096)", queried, queried.Lock)
	}
	receipt["getlk_lock"], receipt["remote_pid"] = queried.Lock, queried.Lock.PID

	// The contender's blocking F_SETLKW waits in the active MDS.
	contender.start(t, ctx, 1)
	waiting := mdsWaitingFlock(t, ctx, m, clientB, true)
	inode := fmt.Sprintf("#0x%x ", holder.inode)
	if waiting.flagPoint != "failed to add lock, waiting" || !strings.Contains(waiting.description, "rule 1, type 2,") ||
		!strings.Contains(waiting.description, "wait 1 ") || !strings.Contains(waiting.description, inode) {
		t.Fatalf("the active MDS does not hold client.%d's F_SETLKW waiting on inode %d: %+v", clientB, holder.inode, waiting)
	}
	receipt["waiting_before"] = map[string]any{"tid": waiting.tid, "flag_point": waiting.flagPoint, "description": waiting.description}
	if contender.Published(ctx, 3) {
		t.Fatal("the contender's F_SETLKW returned before the takeover")
	}

	// Kill the active MDS and let the standby take over.
	oldCID := daemon.GetContainerID()
	receipt["old_container"] = oldCID
	zero := time.Duration(0)
	if err := daemon.Stop(ctx, &zero); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	after, _, err := boundedPoll(ctx, realPollClock, 2*time.Minute, 500*time.Millisecond, func(probe context.Context) (*cephfs.FilesystemStatus, bool, error) {
		status, err := m.FS.MDSStatus(probe)
		if err != nil {
			return status, false, nil
		}
		return status, len(status.Active) == 1 && status.Active[0].Name == successor.Name && status.Active[0].GID == successor.GID && status.Active[0].State == "up:active", nil
	})
	if err != nil || after.FilesystemID != before.FilesystemID || after.Active[0].Rank != 0 || !after.Active[0].Owned {
		t.Fatalf("standby takeover: %+v, %v", after, err)
	}
	m.MDS = after.Active[0]

	// The new MDS replayed the queued request: it still holds the contender's
	// F_SETLKW waiting, as the same request (same tid), and the call has not
	// returned.
	replayed := mdsWaitingFlock(t, ctx, m, clientB, true)
	if replayed.flagPoint != "failed to add lock, waiting" || !strings.Contains(replayed.description, inode) || replayed.tid != waiting.tid {
		t.Fatalf("the new MDS does not hold client.%d's replayed F_SETLKW as tid %d: %+v", clientB, waiting.tid, replayed)
	}
	receipt["waiting_after"] = map[string]any{"tid": replayed.tid, "flag_point": replayed.flagPoint, "description": replayed.description}
	if contender.Published(ctx, 3) {
		t.Fatal("the contender's F_SETLKW returned while the holder still held the lock after the takeover")
	}

	// The holder unlocks; the new MDS grants the waiting lock.
	unlocked := time.Now()
	if released := holder.run(t, ctx, 1); released.Errno != 0 {
		t.Fatalf("holder's F_UNLCK after the takeover: %+v", released)
	}
	granted := contender.Receipt(t, ctx, 3)
	if granted.Errno != 0 {
		t.Fatalf("contender's F_SETLKW after the unlock: %+v", granted)
	}
	receipt["granted_elapsed_ms"], receipt["granted_after_unlock_ms"] = granted.ElapsedMS, time.Since(unlocked).Milliseconds()
	mdsWaitingFlock(t, ctx, m, clientB, false)
	if released := contender.run(t, ctx, 2); released.Errno != 0 {
		t.Fatalf("contender's F_UNLCK: %+v", released)
	}
	holder.close(t, ctx)
	contender.close(t, ctx)

	independent := readCephFSIndependently(t, ctx, m, root+"/locks/file")
	if independent.Inode != holder.inode {
		t.Fatalf("independent read inode %d, want %d", independent.Inode, holder.inode)
	}
	receipt["independent_reader_inode"] = independent.Inode

	// Restore the old MDS as the standby and unmount cleanly.
	if err := daemon.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i, mount := range mounts {
		cleanCtx, stop := context.WithTimeout(context.Background(), 90*time.Second)
		err := cleanFcntlTakeoverMount(cleanCtx, m, mount)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		cleaned[i] = true
	}
}
