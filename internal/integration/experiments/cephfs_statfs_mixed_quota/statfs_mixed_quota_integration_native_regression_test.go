//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_statfs_mixed_quota)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_statfs_mixed_quota_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_statfs_mixed_quota_test.image=<image>"
//
// Tentacle 20.2.5 is fixed; neither it nor Squid 19.2.5 or Umbrella 21.2.0
// reproduces it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

// statfsRoot is the directory of the filesystem that holds the layout.
const statfsRoot = "/statfs-mixed-quota"

// statfsScript mounts the filesystem with a libcephfs client of its own and
// runs one of three modes. "prepare" makes the directories of the layout,
// sets their quotas and writes one file into each. "observe" waits, bounded,
// until every directory's recursive bytes are the layout's, keeps every
// reading, and then reads each directory's inode, quota attributes,
// recursive statistics and statfs once. "remove" unlinks the files, removes
// the quotas and the directories and reports what is left.
const statfsScript = `import cephfs,errno,json,os,sys,time
mode,filesystem,root=sys.argv[1],sys.argv[2],sys.argv[3].encode()
spec=json.loads(sys.argv[4])
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
fs.mount(filesystem_name=filesystem.encode())
def at(path):
    return root+b"/"+path.encode()
def quota(path,name):
    try:
        return int(fs.getxattr(path,name)),True
    except cephfs.Error as error:
        if error.errno!=errno.ENODATA:
            raise
        return 0,False
try:
    out=dict(client=fs.get_instance_id(),client_quota=fs.conf_get("client_quota"),client_quota_df=fs.conf_get("client_quota_df"))
    if mode=="prepare":
        fs.mkdir(root,0o755)
        for d in spec["dirs"]:
            path=at(d["path"])
            fs.mkdir(path,0o755)
            for name in ("max_bytes","max_files"):
                if d[name]:
                    fs.setxattr(path,"ceph.quota."+name,str(d[name]).encode(),0)
            fd=fs.open(path+b"/data",os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644)
            try:
                fs.write(fd,bytes(spec["file_bytes"]),0)
                fs.fsync(fd,0)
            finally:
                fs.close(fd)
    elif mode=="observe":
        want={d["path"]:d["rbytes"] for d in spec["dirs"]}
        start=time.monotonic()
        out.update(polls=[],converged=False)
        while True:
            seen={path:int(fs.getxattr(at(path),"ceph.dir.rbytes")) for path in want}
            out["polls"].append(dict(after_ms=int((time.monotonic()-start)*1000),rbytes=seen))
            out["converged"]=seen==want
            if out["converged"] or time.monotonic()-start>spec["deadline_seconds"]:
                break
            time.sleep(2)
        out["dirs"]=[]
        for d in spec["dirs"]:
            path=at(d["path"])
            max_bytes,bytes_present=quota(path,"ceph.quota.max_bytes")
            max_files,files_present=quota(path,"ceph.quota.max_files")
            seen=dict(path=d["path"],inode=fs.stat(path).st_ino,max_bytes=max_bytes,max_bytes_present=bytes_present,
                      max_files=max_files,max_files_present=files_present)
            for name in ("rbytes","rfiles","rsubdirs"):
                seen[name]=int(fs.getxattr(path,"ceph.dir."+name))
            stat=fs.statfs(path)
            for name in ("f_bsize","f_blocks","f_bfree","f_bavail","f_files"):
                seen[name]=stat[name]
            out["dirs"].append(seen)
    else:
        failed=[]
        def attempt(what,call,*args):
            try:
                call(*args)
            except cephfs.Error as error:
                if error.errno!=errno.ENOENT:
                    failed.append("%s: %s"%(what,error))
        for d in reversed(spec["dirs"]):
            path=at(d["path"])
            attempt("unlink "+d["path"],fs.unlink,path+b"/data")
            for name in ("max_bytes","max_files"):
                if d[name]:
                    attempt("remove %s of %s"%(name,d["path"]),fs.setxattr,path,"ceph.quota."+name,b"0",0)
            attempt("rmdir "+d["path"],fs.rmdir,path)
        attempt("rmdir the root",fs.rmdir,root)
        try:
            fs.stat(root)
            failed.append("the root is still there")
        except cephfs.Error as error:
            if error.errno!=errno.ENOENT:
                failed.append("stat the root: %s"%error)
        out["failed"]=failed
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

// TestCephFSStatfsWithNestedByteAndFileQuotas reports whether libcephfs
// statfs at a directory that has only a file quota, below a directory that
// has a byte quota, reports the data pool instead of the byte quota.
//
// Given the layout (a parent with a byte quota; below it a directory with
// only a file quota, one with only a byte quota, one with both and one with
// none; and a directory without a byte quota above it) and the conditions
// (the quotas read back, the recursive bytes reached the layout's), when a
// fresh mount reads statfs at every directory, then the defect is REPRODUCED
// if only the file-quota directory does not report its parent's byte quota,
// and NOT_REPRODUCED if it does, as the controls do. Tentacle 20.2.4
// reproduces it.
//
// A failure of the given or when steps fails the test, and so does a control
// directory that does not report its byte quota; the result does not.
func TestCephFSStatfsWithNestedByteAndFileQuotas(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_STATFS_MIXED_QUOTA image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	t.Logf("python3-cephfs: %s", harness.ExecOutput(t, ctx, client, "python3", "-c", "import cephfs;print(cephfs.__file__, cephfs.LibCephFS().version())"))
	spec, err := json.Marshal(map[string]any{"dirs": statfsLayout, "file_bytes": statfsFileBytes, "deadline_seconds": 90})
	if err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context, mode string, into any) (string, error) {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", statfsScript, mode, fs.FilesystemName, statfsRoot, string(spec)},
			Timeout: 3 * time.Minute, MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "CephFS statfs " + mode,
		})
		out := string(result.Stdout)
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			return out, fmt.Errorf("%s: exit=%d err=%v stderr=%.1024s", mode, result.ExitCode, err, result.Stderr)
		}
		if err := json.Unmarshal(result.Stdout, into); err != nil {
			return out, fmt.Errorf("%s: %w: %s", mode, err, out)
		}
		return out, nil
	}
	// The layout is removed before the client and the cluster go away, also
	// after a failure, which stays the test's first failure.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer stop()
		var removed struct {
			Failed []string `json:"failed"`
		}
		if _, err := run(cleanup, "remove", &removed); err != nil || len(removed.Failed) > 0 {
			t.Errorf("remove the layout: %v, %v", err, removed.Failed)
		}
	})
	var prepared statfsReport
	if _, err := run(ctx, "prepare", &prepared); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var report statfsReport
	raw, err := run(ctx, "observe", &report)
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	if report.Client == prepared.Client {
		t.Fatalf("condition not established: the observing client is the preparing client %d", prepared.Client)
	}
	t.Logf("CEPHFS_STATFS_MIXED_QUOTA_RECEIPT prepared by client %d; observed by a fresh mount: %s", prepared.Client, raw)
	// The symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	if err := judgeStatfs(statfsLayout, report); err != nil {
		defect, ok := strings.CutPrefix(err.Error(), "observed defect: ")
		if !ok {
			t.Fatal(err)
		}
		symptoms = append(symptoms, defect)
	}
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_statfs_mixed_quota result=%s image=%s symptoms=%q", result, image, symptoms)
}
