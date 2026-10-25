//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
)

// This extends the existing POSIX actor with a disjoint-range control and a
// write through the original descriptor. It never reopens that descriptor or
// automatically reissues a failed lock or data operation.
const fcntlTakeoverActor cephFSActorScript = cephFSActorHelpers + `
import fcntl, struct
path, operations, payload = sys.argv[4], sys.argv[5].split(","), bytes.fromhex(sys.argv[6])
fd = os.open(path, os.O_RDWR)
def identity(**extra):
    with open("/proc/self/stat") as opened:
        stat = opened.read()
    start = stat[stat.rfind(")")+2:].split()[19]
    with open("/proc/self/fdinfo/%d" % fd) as opened:
        mount = [line.split()[1] for line in opened if line.startswith("mnt_id:")][0]
    return dict(start_ticks=start, fd=fd, mount_id=int(mount), device=os.fstat(fd).st_dev, **extra)
def receipt(phase, **fields):
    publish(phase, inode=os.fstat(fd).st_ino, ops=identity(**fields.pop("ops", {})), **fields)
receipt(1)
for index, operation in enumerate(operations):
    gate("op-%d" % index)
    result = dict(errno=0)
    try:
        if operation == "get":
            got = struct.unpack("hhqqi4x", fcntl.fcntl(fd, fcntl.F_GETLK, struct.pack("hhqqi4x", fcntl.F_WRLCK, os.SEEK_SET, 0, 4096, 0)))
            result["lock"] = dict(type=got[0], start=got[2], len=got[3], pid=got[4])
        elif operation == "write":
            written = os.pwrite(fd, payload, 0)
            os.fsync(fd)
            result["ops"] = dict(written=written, data=os.pread(fd, len(payload)+1, 0).hex())
        else:
            start = 8192 if operation in ("disjoint", "undisjoint") else 0
            mode = fcntl.LOCK_UN if operation in ("unlock", "undisjoint") else fcntl.LOCK_EX | fcntl.LOCK_NB
            fcntl.lockf(fd, mode, 4096, start, os.SEEK_SET)
    except OSError as error:
        result["errno"] = error.errno or -1
    receipt(index+2, **result)
gate("finish")
os.close(fd)
publish(len(operations)+2, closed=True)
`

type fcntlTakeoverRun struct {
	*cephFSActorRun
	first cephFSActorReceipt
	ops   []string
}

func startFcntlTakeoverActor(t *testing.T, ctx context.Context, m *cephFSSingleMDS, ctr testcontainers.Container, work, path string, payload []byte, ops ...string) *fcntlTakeoverRun {
	t.Helper()
	run := startCephFSActor(t, ctx, m.cluster, ctr, fcntlTakeoverActor, m.fs.FilesystemName, work, path, strings.Join(ops, ","), hex.EncodeToString(payload))
	a := &fcntlTakeoverRun{cephFSActorRun: run, ops: ops}
	a.first = a.read(t, ctx, 1)
	if _, err := fcntlTakeoverFDView(a.first); err != nil {
		t.Fatal(err)
	}
	return a
}

func (a *fcntlTakeoverRun) read(t *testing.T, parent context.Context, phase int) cephFSActorReceipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	r := a.receipt(t, ctx, phase)
	if !time.Now().Before(deadline) || ctx.Err() != nil {
		t.Fatalf("fcntl receipt %d completed after its phase deadline", phase)
	}
	return r
}

func (a *fcntlTakeoverRun) op(t *testing.T, ctx context.Context, index int) cephFSActorReceipt {
	t.Helper()
	a.open(t, ctx, "op-"+strconv.Itoa(index))
	r := a.read(t, ctx, index+2)
	if err := sameFcntlTakeoverFD(a.first, r); err != nil {
		t.Fatalf("fcntl %s: %v", a.ops[index], err)
	}
	return r
}

func (a *fcntlTakeoverRun) finish(t *testing.T, ctx context.Context) {
	t.Helper()
	a.open(t, ctx, "finish")
	r := a.read(t, ctx, len(a.ops)+2)
	if !r.Closed || r.PID != a.first.PID || r.CloseErrno != 0 {
		t.Fatalf("fcntl close: %+v", r)
	}
	a.wait(t, ctx)
}

func fuseTakeoverProcessStart(t *testing.T, ctx context.Context, ctr testcontainers.Container, pid string) string {
	t.Helper()
	return strings.TrimSpace(execOutput(t, ctx, ctr, "python3", "-c", `import sys; s=open("/proc/"+sys.argv[1]+"/stat").read();print(s[s.rfind(")")+2:].split()[19])`, pid))
}

// Stop the actor callbacks before this callback (LIFO), then unmount while
// the foreground FUSE process and MDS are still reachable. If unmount fails,
// the separately registered process/container/cluster cleanups still run.
func cleanFcntlTakeoverMount(ctx context.Context, m *cephFSSingleMDS, mount *fuseMount) error {
	result, err := m.cluster.RunNativeCommand(ctx, mount.ctr, ceph.NativeCommand{
		Argv: []string{"umount", mount.point}, Timeout: 30 * time.Second, MaxStdout: 4096, MaxStderr: 4096, Label: "owned FUSE unmount",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		return fmt.Errorf("unmount %s: exit=%d err=%v stderr=%.512s", mount.point, result.ExitCode, err, result.Stderr)
	}
	select {
	case <-mount.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if mount.err != nil || mount.result.ExitCode != 0 || !mount.result.Complete() {
		return fmt.Errorf("FUSE exit after unmount: %s", mount.exited(ctx))
	}
	_, _, err = boundedPoll(ctx, realPollClock, 30*time.Second, 250*time.Millisecond, func(probe context.Context) (bool, bool, error) {
		state, err := mount.state(probe, m.fs)
		if err != nil {
			return false, false, err
		}
		gone := !state.mounted && len(state.processes) == 0 && !state.sessioned
		return gone, gone, nil
	})
	return err
}

func testCephFSFcntlStandbyTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))
	before, err := m.fs.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 || len(before.Standby) != 1 || !before.Standby[0].Owned {
		t.Fatalf("one owned active/standby required: %+v, %v", before, err)
	}
	old, successor := before.Active[0], before.Standby[0]
	var daemon *cephfs.MDS
	for _, member := range m.fs.MDSs() {
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
	const root, point = "/fcntl-takeover", "/mnt/cephfs"
	var made struct {
		Inode uint64 `json:"inode"`
	}
	fuseExec(t, ctx, toolkitContainer(t, m.cluster), &made, `
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
`, m.fs.FilesystemName, root)
	if made.Inode == 0 {
		t.Fatal("missing restricted directory inode")
	}
	identity, err := m.cluster.CreateClient(ctx, "fcntl-takeover", ceph.ClientCaps{
		Mon: "allow r", MDS: "allow rw path=" + root, OSD: "allow rw tag cephfs data=" + m.fs.FilesystemName,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mounts []*fuseMount
	var states []fuseMountState
	var processStarts []string
	var images []string
	for range 2 {
		ctr, err := startFuseClient(t, ctx, m, image, identity, fuseMinimumSettings)
		if err != nil {
			t.Fatal(err)
		}
		if _, refused := admitFuse(t, ctx, ctr); refused != nil {
			t.Fatal(refused)
		}
		inspected, err := ctr.Inspect(ctx)
		if err != nil || inspected == nil || !strings.HasPrefix(inspected.Image, "sha256:") {
			t.Fatalf("FUSE immutable image identity: %v", err)
		}
		images = append(images, inspected.Image)
		image = inspected.Image // The second client uses the first one's immutable image.
		mount := startFuseMount(t, ctx, m, ctr, identity, root, point)
		mounts = append(mounts, mount)
		state := mount.ready(t, ctx, m.fs)
		states = append(states, state)
		processStarts = append(processStarts, fuseTakeoverProcessStart(t, ctx, ctr, state.processes[0]))
	}
	if states[0].session.ID == states[1].session.ID || images[0] != images[1] {
		t.Fatal("FUSE clients are not distinct sessions of one immutable image")
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
	base := bytes.Repeat([]byte("before-lock\x00\xff"), 512)
	updated := bytes.Repeat([]byte("after--lock\x01\xfe"), 512)
	path := point + "/file"
	var wrote fuseRead
	fuseExec(t, ctx, mounts[0].ctr, &wrote, fuseWriteScript, path, hex.EncodeToString(base))
	holder := startFcntlTakeoverActor(t, ctx, m, mounts[0].ctr, "/tmp/fcntl-takeover-a", path, updated, "lock", "unlock")
	contender := startFcntlTakeoverActor(t, ctx, m, mounts[1].ctr, "/tmp/fcntl-takeover-b", path, updated, "lock", "get", "disjoint", "undisjoint", "lock", "get", "lock", "write", "unlock")
	if holder.first.Inode != contender.first.Inode || holder.first.Inode != wrote.Inode {
		t.Fatal("FUSE actors did not retain the seeded file inode")
	}
	holderLocked := holder.op(t, ctx, 0)
	if holderLocked.Errno != 0 {
		t.Fatalf("initial holder lock: %+v", holderLocked)
	}
	preDenied, preLock := contender.op(t, ctx, 0), contender.op(t, ctx, 1)
	if !fcntlConflictResult(preDenied.Errno) || preLock.Errno != 0 {
		t.Fatalf("pre-takeover conflict/query: %+v / %+v", preDenied, preLock)
	}
	disjoint, disjointUnlock := contender.op(t, ctx, 2), contender.op(t, ctx, 3)
	if disjoint.Errno != 0 || disjointUnlock.Errno != 0 {
		t.Fatalf("disjoint range [8192,12288) lock/unlock: %+v / %+v", disjoint, disjointUnlock)
	}
	oldCID := daemon.GetContainerID()
	zero := time.Duration(0)
	if err := daemon.Stop(ctx, &zero); err != nil {
		t.Fatal(err)
	}
	if state, err := daemon.State(ctx); err != nil || state.Running {
		t.Fatalf("old active MDS not stopped: %+v, %v", state, err)
	}
	if _, err := m.cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	after, _, err := boundedPoll(ctx, realPollClock, 2*time.Minute, 500*time.Millisecond, func(probe context.Context) (*cephfs.FilesystemStatus, bool, error) {
		status, err := m.fs.MDSStatus(probe)
		if err != nil {
			return status, false, nil
		}
		return status, len(status.Active) == 1 && status.Active[0].Name == successor.Name && status.Active[0].GID == successor.GID && status.Active[0].State == "up:active", nil
	})
	if err != nil || after.FilesystemID != before.FilesystemID || after.Active[0].Rank != 0 || !after.Active[0].Owned {
		t.Fatalf("standby takeover: %+v, %v", after, err)
	}
	m.mds = after.Active[0]
	var retained []cephfs.Session
	for i, mount := range mounts {
		state := mount.ready(t, ctx, m.fs)
		if state.session.ID != states[i].session.ID || state.session.Address != states[i].session.Address || state.session.PID != states[i].session.PID || state.processes[0] != states[i].processes[0] ||
			fuseTakeoverProcessStart(t, ctx, mount.ctr, state.processes[0]) != processStarts[i] {
			t.Fatal("FUSE process or client session was replaced during takeover")
		}
		retained = append(retained, state.session)
	}
	// A remains behind its next gate: no unlock or second lock request may
	// reconstruct the lock before B independently observes recovery.
	if holder.published(ctx, 3) {
		t.Fatal("holder made another request before post-takeover proof")
	}
	postDenied, postLock := contender.op(t, ctx, 4), contender.op(t, ctx, 5)
	if err := fcntlTakeoverConflict(preDenied, postDenied); err != nil {
		t.Fatal(err)
	}
	if postLock.Errno != 0 {
		t.Fatalf("post-takeover F_GETLK errno=%d", postLock.Errno)
	}
	if err := sameFcntlTakeoverLock(preLock.Lock, postLock.Lock); err != nil {
		t.Fatal(err)
	}
	if holder.published(ctx, 3) {
		t.Fatal("holder advanced while B observed its reconstructed lock")
	}
	unlocked := holder.op(t, ctx, 1)
	granted := contender.op(t, ctx, 6)
	if unlocked.Errno != 0 || granted.Errno != 0 {
		t.Fatalf("unlock then acquire: %+v / %+v", unlocked, granted)
	}
	write := contender.op(t, ctx, 7)
	writeFD, err := fcntlTakeoverFDView(write)
	if err != nil || write.Errno != 0 || writeFD.Written != len(updated) || writeFD.Data != hex.EncodeToString(updated) {
		t.Fatalf("retained contender fd write/fsync: %+v, %v", writeFD, err)
	}
	if r := contender.op(t, ctx, 8); r.Errno != 0 {
		t.Fatalf("contender unlock: %+v", r)
	}
	read, err := m.cluster.RunNativeCommand(ctx, toolkitContainer(t, m.cluster), ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSIndependentReader, m.fs.FilesystemName, root + "/file"}, Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "independent fcntl takeover reader",
	})
	var independent struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int64  `json:"size"`
		Data   string `json:"data"`
	}
	if err != nil || read.ExitCode != 0 || !read.Complete() || json.Unmarshal([]byte(lastJSONLine(string(read.Stdout))), &independent) != nil ||
		independent.Client == 0 || independent.Client == retained[0].ID || independent.Client == retained[1].ID || independent.Inode != holder.first.Inode || independent.Size != int64(len(updated)) || independent.Data != hex.EncodeToString(updated) {
		t.Fatalf("independent post-takeover read: %+v err=%v exit=%d", independent, err, read.ExitCode)
	}
	holder.finish(t, ctx)
	contender.finish(t, ctx)
	if err := daemon.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_, _, err = boundedPoll(ctx, realPollClock, time.Minute, 500*time.Millisecond, func(probe context.Context) (bool, bool, error) {
		status, err := m.fs.MDSStatus(probe)
		if err != nil {
			return false, false, nil
		}
		for _, member := range status.Standby {
			if member.Name == old.Name && member.GID != old.GID && member.Owned {
				return true, true, nil
			}
		}
		return false, false, nil
	})
	if err != nil {
		t.Fatal("old MDS did not rejoin as standby:", err)
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
	status, err := m.cluster.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(map[string]any{"fsid": status.FSID, "filesystem": m.fs.FilesystemName, "before_mds": old, "successor": successor, "after_mds": after.Active[0], "old_container": oldCID,
		"before_sessions": []cephfs.Session{states[0].session, states[1].session}, "retained_sessions": retained, "fuse_process_starts": processStarts, "fuse_images": images,
		"holder_fd": holder.first, "contender_fd": contender.first, "holder_lock": holderLocked, "disjoint": disjoint, "disjoint_unlock": disjointUnlock, "pre_denied": preDenied, "pre_lock": preLock, "post_denied": postDenied, "post_lock": postLock,
		"holder_requests_before_post_proof": 1, "holder_unlock": unlocked, "contender_granted": granted, "write_bytes": len(updated), "independent_reader": independent.Client,
		"fd_inode": independent.Inode, "mount_cleanup": cleaned, "old_mds_rejoined": true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_FCNTL_TAKEOVER_RECEIPT %s", receipt)
}
