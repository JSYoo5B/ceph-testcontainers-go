//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_readdir_after_snapdiff)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_readdir_after_snapdiff_test

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
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_readdir_after_snapdiff_test.image=<image>"
//
// No release is fixed: Squid 19.2.5, Tentacle 20.2.5 and Umbrella 21.2.0
// reproduce it too.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

// snapdiffRoot is the directory of the filesystem that holds the case.
const snapdiffRoot = "/readdir-after-snapdiff"

// snapdiffDir is one directory of the case: <root>/<Name> is snapshotted
// twice, and <root>/<Name>/sub holds the two files. Changed is the file
// that is written to between the snapshots.
type snapdiffDir struct {
	Name    string   `json:"name"`
	Files   []string `json:"files"`
	Changed string   `json:"changed"`
}

var (
	// The two names have the same dentry hash under the default
	// (rjenkins) directory hash; the test reads that back from the
	// positions the client reports.
	snapdiffTrigger = snapdiffDir{Name: "trigger", Files: []string{"collision_2379", "collision_2690"}, Changed: "collision_2690"}
	snapdiffControl = snapdiffDir{Name: "control", Files: []string{"plain_first", "plain_second"}, Changed: "plain_second"}
)

// snapdiffScript mounts the filesystem with a libcephfs client of its own
// and runs one of four modes. "prepare" makes each directory with its two
// files, snapshots it as s1, writes to the changed file and snapshots it as
// s2. "warm" does, for each directory on one mount: list the s2 state of
// "sub" entry by entry, read the snapdiff of "sub" between s1 and s2 to its
// end, and list the s2 state again through a new handle. "fresh" only
// lists the s2 state of each directory. "remove" removes the snapshots and
// everything below the root and reports what is left.
const snapdiffScript = `import cephfs,errno,json,os,sys
mode,filesystem,root=sys.argv[1],sys.argv[2],sys.argv[3]
dirs=json.loads(sys.argv[4])
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
fs.mount(filesystem_name=filesystem.encode())
def at(*names):
    return "/".join((root,)+names).encode()
def listing(path):
    handle=fs.opendir(path)
    found=[]
    try:
        while True:
            item=fs.readdir(handle)
            if item is None:
                break
            if item.d_name not in (b".",b".."):
                found.append(dict(name=item.d_name.decode(),next=item.d_off))
    finally:
        fs.closedir(handle)
    return found
out=dict(client=fs.get_instance_id())
try:
    if mode=="prepare":
        fs.mkdir(at(),0o755)
        for d in dirs:
            fs.mkdir(at(d["name"]),0o755)
            fs.mkdir(at(d["name"],"sub"),0o755)
            for name in d["files"]:
                fs.close(fs.open(at(d["name"],"sub",name),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644))
            fs.mkdir(at(d["name"],".snap","s1"),0o755)
            fd=fs.open(at(d["name"],"sub",d["changed"]),os.O_WRONLY,0o644)
            fs.write(fd,b"written between the snapshots",0)
            fs.fsync(fd,0)
            fs.close(fd)
            fs.mkdir(at(d["name"],".snap","s2"),0o755)
    elif mode=="warm":
        for d in dirs:
            run=dict(before=listing(at(d["name"],".snap","s2","sub")),diff=[])
            handle=fs.opensnapdiff(at(d["name"]),b"sub",b"s1",b"s2")
            try:
                while True:
                    item=handle.readdir()
                    if item is None:
                        break
                    if item.d_name not in (b".",b".."):
                        run["diff"].append(dict(name=item.d_name.decode(),snap=item.d_snapid))
            finally:
                handle.close()
            run["after"]=listing(at(d["name"],".snap","s2","sub"))
            out[d["name"]]=run
    elif mode=="fresh":
        for d in dirs:
            out[d["name"]]=listing(at(d["name"],".snap","s2","sub"))
    else:
        failed=[]
        def attempt(what,call,*args):
            try:
                call(*args)
            except cephfs.Error as error:
                if error.errno!=errno.ENOENT:
                    failed.append("%s: %s"%(what,error))
        for d in dirs:
            for snap in ("s1","s2"):
                attempt("rmdir snapshot %s of %s"%(snap,d["name"]),fs.rmdir,at(d["name"],".snap",snap))
            for name in d["files"]:
                attempt("unlink "+name,fs.unlink,at(d["name"],"sub",name))
            attempt("rmdir sub of "+d["name"],fs.rmdir,at(d["name"],"sub"))
            attempt("rmdir "+d["name"],fs.rmdir,at(d["name"]))
        attempt("rmdir the root",fs.rmdir,at())
        try:
            fs.stat(at())
            failed.append("the root is still there")
        except cephfs.Error as error:
            if error.errno!=errno.ENOENT:
                failed.append("stat the root: %s"%error)
        out["failed"]=failed
finally:
    fs.unmount()
    fs.shutdown()
print(json.dumps(out))
`

// TestCephFSListingAfterSnapdiff reports whether a libcephfs mount that
// read a snapdiff of a snapshot directory loses an entry when it lists the
// snapshot again.
//
// Given two snapshotted directories (the trigger, whose two names share a
// dentry hash and the second of which changed between the snapshots, and the
// control, whose names do not share one) and the conditions (the first
// listing on the warm mount and a fresh mount's listing are the snapshot,
// the snapdiff names only the changed entry, and the positions show the
// layout), when the warm mount lists the second snapshot, reads the snapdiff
// and lists it again, then the defect is REPRODUCED if the second listing of
// the trigger snapshot is not exactly its names while the control's is, and
// NOT_REPRODUCED if both are exact. Tentacle 20.2.4 reproduces it.
//
// A failure of the given or when steps fails the test, and so does a control
// snapshot that is not listed in full; the result does not.
func TestCephFSListingAfterSnapdiff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_READDIR_AFTER_SNAPDIFF image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	t.Logf("python3-cephfs: %s", harness.ExecOutput(t, ctx, client, "python3", "-c", "import cephfs;print(cephfs.__file__, cephfs.LibCephFS().version(), hasattr(cephfs.LibCephFS, 'opensnapdiff'))"))
	dirs, err := json.Marshal([]snapdiffDir{snapdiffTrigger, snapdiffControl})
	if err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context, mode string, into any) error {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", snapdiffScript, mode, fs.FilesystemName, snapdiffRoot, string(dirs)},
			Timeout: 3 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "CephFS listing after snapdiff " + mode,
		})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			return fmt.Errorf("%s: exit=%d err=%v stderr=%.1024s", mode, result.ExitCode, err, result.Stderr)
		}
		if err := json.Unmarshal(result.Stdout, into); err != nil {
			return fmt.Errorf("%s: %w: %.1024s", mode, err, result.Stdout)
		}
		return nil
	}
	// The snapshots and the files are removed before the client and the
	// cluster go away, also after a failure, which stays the test's first
	// failure.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
		defer stop()
		var removed struct {
			Failed []string `json:"failed"`
		}
		if err := run(cleanup, "remove", &removed); err != nil || len(removed.Failed) > 0 {
			t.Errorf("remove the snapshots and the files: %v, %v", err, removed.Failed)
		}
	})
	var prepared struct {
		Client uint64 `json:"client"`
	}
	if err := run(ctx, "prepare", &prepared); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var warm struct {
		Client  uint64 `json:"client"`
		Trigger dirRun `json:"trigger"`
		Control dirRun `json:"control"`
	}
	if err := run(ctx, "warm", &warm); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var fresh struct {
		Client  uint64     `json:"client"`
		Trigger []dirEntry `json:"trigger"`
		Control []dirEntry `json:"control"`
	}
	if err := run(ctx, "fresh", &fresh); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	for _, seen := range []struct {
		dir   snapdiffDir
		run   dirRun
		fresh []dirEntry
	}{{snapdiffTrigger, warm.Trigger, fresh.Trigger}, {snapdiffControl, warm.Control, fresh.Control}} {
		t.Logf("CEPHFS_READDIR_AFTER_SNAPDIFF_RECEIPT %s: warm client %d listed %s; snapdiff s1..s2 named %v; listed again %s; fresh client %d listed %s",
			seen.dir.Name, warm.Client, positions(seen.run.Before), seen.run.Diff, positions(seen.run.After), fresh.Client, positions(seen.fresh))
	}
	// The symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	if verdict := judgeAfterSnapdiff(snapdiffTrigger.Files, snapdiffControl.Files, snapdiffTrigger.Changed, snapdiffControl.Changed,
		warm.Trigger, warm.Control, fresh.Trigger, fresh.Control); verdict != "" {
		defect, ok := strings.CutPrefix(verdict, "observed defect: ")
		if !ok {
			t.Fatal(verdict)
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
	t.Logf("EXPERIMENT_RESULT case=cephfs_readdir_after_snapdiff result=%s image=%s symptoms=%q", result, image, symptoms)
}
