//go:build all || integration

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// byteQuotaLimit is the max_bytes the administrator sets on the live
	// client's directory, which holds only the actor's file.
	byteQuotaLimit = 4 << 20
	// byteQuotaChunk is the size of one write and fsync.
	byteQuotaChunk = 64 << 10
	// byteQuotaOvershoot bounds how far past the limit the client may keep
	// acknowledging writes before EDQUOT.
	byteQuotaOvershoot = 32 << 20
)

// cephFSByteQuotaActor keeps one libcephfs mount and one O_RDWR fd open for
// its whole run. It creates the directory and the empty file and publishes
// receipt 1. Behind the "limited" gate it waits until its own client reads
// ceph.quota.max_bytes as the limit, then writes and fsyncs pattern chunks
// from offset 0, pausing briefly between them, until a write or fsync fails
// or the overshoot bound is passed, and publishes receipt 2. Behind the
// "relaxed" gate it waits until the quota reads as absent, writes one more
// chunk and publishes receipt 3. It closes after the "finish" gate and
// publishes receipt 4. Arguments: filesystem, work directory, token,
// directory, file, limit, chunk size, overshoot bound.
const cephFSByteQuotaActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
import errno
directory, path = sys.argv[4].encode(), sys.argv[5].encode()
limit, chunk, overshoot = (int(arg) for arg in sys.argv[6:9])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
def quota():
    try:
        return int(fs.getxattr(directory, "ceph.quota.max_bytes"))
    except cephfs.Error as error:
        if error.errno == errno.ENODATA:
            return 0
        raise
def pattern(offset, size):
    return bytes((offset + i) % 251 for i in range(size))
def observe(want, timeout=90):
    deadline, samples = time.monotonic() + timeout, 0
    while time.monotonic() < deadline:
        samples += 1
        if quota() == want:
            return samples
        time.sleep(0.2)
    sys.exit("the client did not read max_bytes=%d" % want)
fs.mkdir(directory, 0o755)
fd = fs.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
fs.fsync(fd, 0)
def state(phase, **extra):
    stat = fs.fstat(fd)
    publish(phase, client=fs.get_instance_id(), inode=stat.st_ino, dir_inode=fs.stat(directory).st_ino, size=stat.st_size, **extra)
state(1)
gate("limited")
samples = observe(limit)
acked, attempts, failure = 0, 0, 0
started = time.monotonic()
while acked <= limit + overshoot and time.monotonic() - started < 120:
    attempts += 1
    try:
        written = fs.write(fd, pattern(acked, chunk), acked)
        if written != chunk:
            sys.exit("short write of %d bytes" % written)
        fs.fsync(fd, 0)
    except cephfs.Error as error:
        failure = error.errno
        break
    acked += written
    time.sleep(0.05)
state(2, ops=dict(limit=limit, samples=samples, acked=acked, attempts=attempts, errno=failure, size=fs.fstat(fd).st_size))
gate("relaxed")
samples = observe(0)
written = fs.write(fd, pattern(acked, chunk), acked)
fs.fsync(fd, 0)
state(3, ops=dict(samples=samples, written=written, size=fs.fstat(fd).st_size))
gate("finish")
fs.close(fd)
fs.unmount()
fs.shutdown()
publish(4, closed=True)
`

// cephFSByteQuotaAdmin sets, reads or inspects max_bytes of the directory as
// a client of its own and reports the quota, the directory's recursive bytes
// and the file's size and SHA-256. Arguments: mode ("set", "inspect"),
// filesystem, directory, file, value.
const cephFSByteQuotaAdmin = `
import cephfs, errno, hashlib, json, os, sys
mode, filesystem, directory, path, value = sys.argv[1:6]
directory, path = directory.encode(), path.encode()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=filesystem.encode())
try:
    if mode == "set":
        fs.setxattr(directory, "ceph.quota.max_bytes", value.encode(), 0)
    try:
        quota, present = int(fs.getxattr(directory, "ceph.quota.max_bytes")), True
    except cephfs.Error as error:
        if error.errno != errno.ENODATA:
            raise
        quota, present = 0, False
    out = dict(client=fs.get_instance_id(), dir_inode=fs.stat(directory).st_ino, limit=quota, present=present,
               rbytes=int(fs.getxattr(directory, "ceph.dir.rbytes")))
    try:
        stat = fs.stat(path)
        fd = fs.open(path, os.O_RDONLY, 0)
        try:
            data = fs.read(fd, 0, stat.st_size + 1)
        finally:
            fs.close(fd)
        out.update(inode=stat.st_ino, size=len(data), sha256=hashlib.sha256(data).hexdigest())
    except cephfs.Error as error:
        out.update(file_errno=error.errno)
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

type cephFSByteQuotaAdminResult struct {
	Client    uint64 `json:"client"`
	DirInode  uint64 `json:"dir_inode"`
	Limit     uint64 `json:"limit"`
	Present   bool   `json:"present"`
	RBytes    uint64 `json:"rbytes"`
	Inode     uint64 `json:"inode"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	FileErrno int    `json:"file_errno"`
}

func runCephFSByteQuotaAdmin(ctx context.Context, cluster *ceph.Container, toolkit testcontainers.Container, mode, filesystem, directory, path string, limit uint64) (cephFSByteQuotaAdminResult, error) {
	result, err := cluster.RunNativeCommand(ctx, toolkit, ceph.NativeCommand{
		Argv:    []string{"python3", "-c", cephFSByteQuotaAdmin, mode, filesystem, directory, path, strconv.FormatUint(limit, 10)},
		Timeout: time.Minute, MaxStdout: 16 << 10, MaxStderr: 16 << 10, Label: "CephFS byte quota administrator",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		return cephFSByteQuotaAdminResult{}, fmt.Errorf("byte quota administrator %s: exit=%d err=%v stderr=%.512s", mode, result.ExitCode, err, result.Stderr)
	}
	var out cephFSByteQuotaAdminResult
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(result.Stdout))), &out); err != nil {
		return out, fmt.Errorf("byte quota administrator output: %w", err)
	}
	return out, nil
}

// testCephFSLiveByteQuota runs the byte-quota recipe on one live client:
//
//  1. The actor creates its directory and empty file and keeps the fd open.
//  2. An independent administrator sets max_bytes; behind the gate the actor
//     waits until its own client reads that limit, then writes chunks until
//     EDQUOT. judgeByteQuotaSeries bounds the overshoot.
//  3. The administrator reads the exact acknowledged bytes back.
//  4. The administrator removes the quota; the actor waits until it reads the
//     quota as absent and writes one more chunk, which the administrator
//     reads back too.
//
// The quota is removed and verified before the client and cluster go away.
func testCephFSLiveByteQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx)
	toolkit := harness.ToolkitContainer(t, m.Cluster)
	const directory, path = "/live-byte-quota", "/live-byte-quota/data"
	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSByteQuotaActor, m.FS.FilesystemName, "/tmp/live-byte-quota-actor",
		directory, path, strconv.Itoa(byteQuotaLimit), strconv.Itoa(byteQuotaChunk), strconv.Itoa(byteQuotaOvershoot))
	first := actor.Receipt(t, ctx, 1)
	if first.Client == 0 || first.Inode == 0 || first.DirInode == 0 || first.Size != 0 {
		t.Fatalf("byte quota baseline: %+v", first)
	}
	harness.CephFSRetainedSession(t, ctx, m.FS, first.Client, m.MDS.Rank)
	admin := func(mode string, limit uint64) cephFSByteQuotaAdminResult {
		t.Helper()
		out, err := runCephFSByteQuotaAdmin(ctx, m.Cluster, toolkit, mode, m.FS.FilesystemName, directory, path, limit)
		if err != nil || out.Client == 0 || out.Client == first.Client || out.DirInode != first.DirInode || out.FileErrno != 0 || out.Inode != first.Inode {
			t.Fatalf("byte quota administrator %s: %+v, %v", mode, out, err)
		}
		return out
	}
	before := admin("inspect", 0)
	if before.Present || before.Limit != 0 || before.Size != 0 {
		t.Fatalf("fresh directory prior byte quota: %+v", before)
	}
	restored := false
	restore := func(cleanCtx context.Context) error {
		observed, err := runCephFSByteQuotaAdmin(cleanCtx, m.Cluster, toolkit, "inspect", m.FS.FilesystemName, directory, path, 0)
		if err == nil && observed.Present {
			observed, err = runCephFSByteQuotaAdmin(cleanCtx, m.Cluster, toolkit, "set", m.FS.FilesystemName, directory, path, 0)
		}
		if err != nil || observed.DirInode != first.DirInode || observed.Present || observed.Limit != 0 {
			return fmt.Errorf("byte quota restore not verified: %+v, %v", observed, err)
		}
		return nil
	}
	t.Cleanup(func() {
		if restored {
			return
		}
		cleanCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := restore(cleanCtx); err != nil {
			t.Errorf("restore directory byte quota: %v", err)
		}
	})

	limitedBy := admin("set", byteQuotaLimit)
	if !limitedBy.Present || limitedBy.Limit != byteQuotaLimit {
		t.Fatalf("set max_bytes: %+v", limitedBy)
	}
	actor.Open(t, ctx, "limited")
	limited := actor.Receipt(t, ctx, 2)
	if limited.PID != first.PID || limited.Client != first.Client || limited.Inode != first.Inode || limited.DirInode != first.DirInode {
		t.Fatalf("the limited series changed client, process or file: %+v", limited)
	}
	var series byteQuotaSeries
	decoder := json.NewDecoder(bytes.NewReader(limited.Ops))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&series); err != nil {
		t.Fatalf("limited series receipt: %v: %s", err, limited.Ops)
	}
	if err := judgeByteQuotaSeries(series, byteQuotaLimit, byteQuotaChunk, byteQuotaOvershoot); err != nil {
		t.Fatalf("live byte quota: %v: %s", err, limited.Ops)
	}
	denied := admin("inspect", 0)
	if denied.Size != *series.Acked || denied.SHA256 != byteQuotaDigest(*series.Acked) {
		t.Fatalf("independent read after EDQUOT: %+v, want %d acknowledged bytes", denied, *series.Acked)
	}
	harness.CephFSRetainedSession(t, ctx, m.FS, first.Client, m.MDS.Rank)

	relaxedBy := admin("set", 0)
	if relaxedBy.Present {
		t.Fatalf("remove max_bytes: %+v", relaxedBy)
	}
	actor.Open(t, ctx, "relaxed")
	relaxed := actor.Receipt(t, ctx, 3)
	var after struct {
		Samples uint64 `json:"samples"`
		Written int64  `json:"written"`
		Size    int64  `json:"size"`
	}
	if err := json.Unmarshal(relaxed.Ops, &after); err != nil || relaxed.PID != first.PID || relaxed.Client != first.Client ||
		after.Samples == 0 || after.Written != byteQuotaChunk || after.Size != *series.Acked+byteQuotaChunk {
		t.Fatalf("relaxed write: %+v %s, %v", relaxed, relaxed.Ops, err)
	}
	read := admin("inspect", 0)
	if read.Size != after.Size || read.SHA256 != byteQuotaDigest(after.Size) {
		t.Fatalf("independent read after relaxation: %+v, want %d bytes", read, after.Size)
	}
	cleanCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	if err := restore(cleanCtx); err != nil {
		t.Fatal(err)
	}
	restored = true
	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 4); !closed.Closed || closed.CloseErrno != 0 || closed.PID != first.PID {
		t.Fatalf("byte quota actor close: %+v", closed)
	}
	actor.Wait(t, ctx)
	current, err := m.FS.MDSStatus(ctx)
	if err != nil || len(current.Active) != 1 || current.Active[0] != m.MDS {
		t.Fatalf("MDS generation changed: %+v, %v", current, err)
	}
	data, err := json.Marshal(map[string]any{"filesystem": m.FS.FilesystemName, "mds": m.MDS, "native_version": m.Version,
		"limit": byteQuotaLimit, "chunk": byteQuotaChunk, "overshoot_bound": byteQuotaOvershoot,
		"baseline": first, "prior": before, "limited_by": limitedBy, "limited": series, "denied_read": denied,
		"relaxed_by": relaxedBy, "relaxed_size": after.Size, "relaxed_read": read, "quota_restored_absent": restored,
		"scope": "max_bytes on one live libcephfs client, enforced by the client within the overshoot bound; no wire-message or consumer claim"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_LIVE_BYTE_QUOTA_RECEIPT %s", data)
}
