//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_live_quota))))

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

const cephFSQuotaActor cephFSActorScript = cephFSActorHelpers + cephFSQuotaObservation + `
import errno
directory, baseline_path, create_path = (arg.encode() for arg in sys.argv[4:7])
base, new = (bytes.fromhex(arg) for arg in sys.argv[7:9])
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
def quota():
    try:
        return int(fs.getxattr(directory, "ceph.quota.max_files"))
    except cephfs.Error as error:
        if error.errno == errno.ENODATA:
            return 0
        raise
def sample():
    return dict(limit=quota(), files=int(fs.getxattr(directory, "ceph.dir.rfiles")),
                directories=int(fs.getxattr(directory, "ceph.dir.rsubdirs")),
                entries=int(fs.getxattr(directory, "ceph.dir.rentries")))
fs.mkdir(directory, 0o755)
fd = fs.open(baseline_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
fs.write(fd, base, 0)
fs.fsync(fd, 0)
def publish_state(phase, **extra):
    stat = fs.fstat(fd)
    publish(phase, client=fs.get_instance_id(), inode=stat.st_ino,
            dir_inode=fs.stat(directory).st_ino, size=stat.st_size,
            full=fs.read(fd, 0, stat.st_size+1).hex(), **extra)
initial = observe_quota(sample, 0)
entries = initial["entries"]
publish_state(1, ops=initial)
for phase, gate_name, limit in [(2, "limited", entries), (3, "relaxed", entries+1)]:
    gate(gate_name)
    result = observe_quota(sample, limit, entries)
    result["create_errno"] = 0
    try:
        created = fs.open(create_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
    except cephfs.Error as error:
        result["create_errno"] = error.errno
    else:
        try:
            fs.write(created, new, 0)
            fs.fsync(created, 0)
            stat = fs.fstat(created)
            result.update(created_inode=stat.st_ino, created_size=stat.st_size,
                          created_data=fs.read(created, 0, stat.st_size+1).hex())
        finally:
            fs.close(created)
    publish_state(phase, ops=result)
gate("finish")
fs.close(fd)
fs.unmount()
fs.shutdown()
publish(4, closed=True)
`

const cephFSQuotaAdmin = `
import cephfs, errno, json, os, sys
mode, filesystem, directory, value = sys.argv[1:5]
directory = directory.encode()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=filesystem.encode())
def read(path):
    try:
        stat = fs.stat(path)
        fd = fs.open(path, os.O_RDONLY, 0)
        try:
            return dict(inode=stat.st_ino, size=stat.st_size, data=fs.read(fd, 0, stat.st_size+1).hex())
        finally:
            fs.close(fd)
    except cephfs.Error as error:
        return dict(errno=error.errno)
try:
    if mode == "set":
        fs.setxattr(directory, "ceph.quota.max_files", value.encode(), 0)
    try:
        raw = fs.getxattr(directory, "ceph.quota.max_files")
        quota, present = int(raw), True
    except cephfs.Error as error:
        if error.errno != errno.ENODATA:
            raise
        quota, present = 0, False
    out = dict(client=fs.get_instance_id(), dir_inode=fs.stat(directory).st_ino,
               limit=quota, present=present, files=int(fs.getxattr(directory, "ceph.dir.rfiles")),
               directories=int(fs.getxattr(directory, "ceph.dir.rsubdirs")),
               entries=int(fs.getxattr(directory, "ceph.dir.rentries")))
    if mode == "read":
        out.update(baseline=read(directory+b"/baseline"), created=read(directory+b"/created"))
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

type cephFSQuotaFile struct {
	Inode uint64 `json:"inode"`
	Size  int64  `json:"size"`
	Data  string `json:"data"`
	Errno int    `json:"errno"`
}

type cephFSQuotaAdminResult struct {
	Client      uint64           `json:"client"`
	DirInode    uint64           `json:"dir_inode"`
	Limit       uint64           `json:"limit"`
	Present     bool             `json:"present"`
	Files       uint64           `json:"files"`
	Directories uint64           `json:"directories"`
	Entries     uint64           `json:"entries"`
	Baseline    *cephFSQuotaFile `json:"baseline"`
	Created     *cephFSQuotaFile `json:"created"`
}

func runCephFSQuotaAdmin(ctx context.Context, cluster *ceph.Container, toolkit testcontainers.Container, mode, filesystem, directory string, limit uint64) (cephFSQuotaAdminResult, error) {
	result, err := cluster.RunNativeCommand(ctx, toolkit, ceph.NativeCommand{
		Argv:    []string{"python3", "-c", cephFSQuotaAdmin, mode, filesystem, directory, strconv.FormatUint(limit, 10)},
		Timeout: time.Minute, MaxStdout: 128 << 10, MaxStderr: 16 << 10, Label: "CephFS quota administrator",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		return cephFSQuotaAdminResult{}, fmt.Errorf("quota administrator %s: exit=%d err=%v stderr=%.512s", mode, result.ExitCode, err, result.Stderr)
	}
	var out cephFSQuotaAdminResult
	if err := json.Unmarshal([]byte(lastJSONLine(string(result.Stdout))), &out); err != nil {
		return out, fmt.Errorf("quota administrator output: %w", err)
	}
	return out, nil
}

func testCephFSLiveQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	toolkit := toolkitContainer(t, m.cluster)
	const directory = "/live-quota"
	base := bytes.Repeat([]byte("quota-baseline\x00\xff"), 256)
	created := bytes.Repeat([]byte("quota-relaxed\x01\xfe"), 256)
	actor := startCephFSActor(t, ctx, m.cluster, m.client, cephFSQuotaActor, m.fs.FilesystemName, "/tmp/live-quota-actor",
		directory, directory+"/baseline", directory+"/created", hex.EncodeToString(base), hex.EncodeToString(created))
	first := actor.receipt(t, ctx, 1)
	if first.Client == 0 || first.Inode == 0 || first.DirInode == 0 || first.Size != int64(len(base)) || first.Full != hex.EncodeToString(base) {
		t.Fatalf("quota baseline: %+v", first)
	}
	cephFSRetainedSession(t, ctx, m.fs, first.Client, m.mds.Rank)
	before, err := runCephFSQuotaAdmin(ctx, m.cluster, toolkit, "inspect", m.fs.FilesystemName, directory, 0)
	if err != nil || before.Client == 0 || before.Client == first.Client || before.DirInode != first.DirInode || before.Present || before.Limit != 0 ||
		before.Files != 1 || before.Directories == 0 || before.Entries != before.Files+before.Directories {
		t.Fatalf("fresh directory prior quota: %+v, %v", before, err)
	}
	restored := false
	restore := func(cleanCtx context.Context) error {
		observed, err := runCephFSQuotaAdmin(cleanCtx, m.cluster, toolkit, "inspect", m.fs.FilesystemName, directory, 0)
		if err != nil {
			return err
		}
		if observed.DirInode != first.DirInode {
			return fmt.Errorf("quota directory changed inode: %d", observed.DirInode)
		}
		if observed.Present {
			observed, err = runCephFSQuotaAdmin(cleanCtx, m.cluster, toolkit, "set", m.fs.FilesystemName, directory, 0)
		}
		if err != nil || observed.Present || observed.Limit != 0 {
			return fmt.Errorf("quota restore not verified: %+v, %v", observed, err)
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
			t.Errorf("restore directory quota: %v", err)
		}
	})
	set := func(limit uint64) cephFSQuotaAdminResult {
		t.Helper()
		out, err := runCephFSQuotaAdmin(ctx, m.cluster, toolkit, "set", m.fs.FilesystemName, directory, limit)
		if err != nil || out.Client == first.Client || out.Client == 0 || out.DirInode != first.DirInode || !out.Present || out.Limit != limit {
			t.Fatalf("set max_files=%d: %+v, %v", limit, out, err)
		}
		return out
	}
	limitedBy := set(before.Entries)
	actor.open(t, ctx, "limited")
	limited := actor.receipt(t, ctx, 2)
	if _, err := validateLiveQuotaStep(first, limited, before.Entries, base, nil, int(syscall.EDQUOT)); err != nil {
		t.Fatal(err)
	}
	// Prove the denied create did not leave a file before authorizing a retry
	// after the administrator's separately acknowledged relaxation.
	denied, err := runCephFSQuotaAdmin(ctx, m.cluster, toolkit, "read", m.fs.FilesystemName, directory, 0)
	if err != nil || denied.Client == 0 || denied.Client == first.Client || denied.DirInode != first.DirInode ||
		denied.Created == nil || denied.Created.Errno != int(syscall.ENOENT) || denied.Created.Inode != 0 || denied.Created.Size != 0 || denied.Created.Data != "" {
		t.Fatalf("independent denied-create read: %+v, %v", denied, err)
	}
	cephFSRetainedSession(t, ctx, m.fs, first.Client, m.mds.Rank)
	relaxedBy := set(before.Entries + 1)
	actor.open(t, ctx, "relaxed")
	relaxed := actor.receipt(t, ctx, 3)
	step, err := validateLiveQuotaStep(first, relaxed, before.Entries+1, base, created, 0)
	if err != nil {
		t.Fatal(err)
	}
	cephFSRetainedSession(t, ctx, m.fs, first.Client, m.mds.Rank)
	read, err := runCephFSQuotaAdmin(ctx, m.cluster, toolkit, "read", m.fs.FilesystemName, directory, 0)
	if err != nil || read.Client == 0 || read.Client == first.Client || read.DirInode != first.DirInode ||
		read.Baseline == nil || read.Baseline.Errno != 0 || read.Baseline.Inode != first.Inode || read.Baseline.Size != int64(len(base)) || read.Baseline.Data != hex.EncodeToString(base) ||
		read.Created == nil || read.Created.Errno != 0 || read.Created.Inode != step.CreatedInode || read.Created.Size != int64(len(created)) || read.Created.Data != hex.EncodeToString(created) {
		t.Fatalf("independent quota data readback: %+v, %v", read, err)
	}
	cleanCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	if err := restore(cleanCtx); err != nil {
		t.Fatal(err)
	}
	restored = true
	actor.open(t, ctx, "finish")
	if closed := actor.receipt(t, ctx, 4); !closed.Closed || closed.CloseErrno != 0 || closed.PID != first.PID {
		t.Fatalf("quota actor close: %+v", closed)
	}
	actor.wait(t, ctx)
	current, err := m.fs.MDSStatus(ctx)
	if err != nil || len(current.Active) != 1 || current.Active[0] != m.mds {
		t.Fatalf("MDS generation changed: %+v, %v", current, err)
	}
	status, err := observeCluster(m.cluster).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"fsid": status.FSID, "filesystem": m.fs.FilesystemName, "mds": m.mds,
		"native_version": m.version, "baseline": first, "prior": before, "limited_by": limitedBy, "limited": limited,
		"denied_read": denied, "relaxed_by": relaxedBy, "relaxed": relaxed, "independent_read": read,
		"quota_restored_absent": restored, "scope": "max_files on one live libcephfs client; no byte-quota, metrics, reclaim or wire claim"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_LIVE_QUOTA_RECEIPT %s", data)
}
