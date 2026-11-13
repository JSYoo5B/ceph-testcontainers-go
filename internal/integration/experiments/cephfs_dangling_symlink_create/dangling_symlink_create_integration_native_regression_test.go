//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_dangling_symlink_create)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_dangling_symlink_create_test

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
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_dangling_symlink_create_test.image=<image>"
//
// No release is fixed: Squid 19.2.3 and every later release that was run
// (19.2.5, 20.2.5, 21.2.0) reproduce it. Squid 19.2.2, which predates the
// defect, does not.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// createRoot is the directory of the filesystem that holds the cases.
const createRoot = "/dangling-symlink-create"

// createScript mounts the filesystem with a libcephfs client of its own and
// runs one of four modes. "prepare" makes each case's directory with its
// files and symlinks. "act" reads each dangling symlink and its target and
// then runs the case's operation at an unused name, at a regular file, at a
// symlink to an existing file and at the dangling symlink, keeping each
// errno; a symlink it makes has the text it is given. "observe" reads every
// name of each case, "source" among them, with lstat and readlink and lists
// the directory of the symlink's target. "remove" removes
// everything below the root and reports what is left.
const createScript = `import cephfs,errno,json,os,stat,sys
mode,filesystem,root=sys.argv[1],sys.argv[2],sys.argv[3]
cases,payload=json.loads(sys.argv[4]),sys.argv[5].encode()
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
fs.mount(filesystem_name=filesystem.encode())
kinds={stat.S_IFDIR:"dir",stat.S_IFLNK:"symlink",stat.S_IFREG:"file"}
def at(case,*names):
    return "/".join((root,case["dir"])+names).encode()
def entry(path):
    try:
        st=fs.lstat(path)
    except cephfs.Error as error:
        if error.errno!=errno.ENOENT:
            raise
        return dict(type="")
    kind=kinds.get(stat.S_IFMT(st.st_mode),oct(st.st_mode))
    return dict(type=kind,inode=st.st_ino,target=fs.readlink(path,4096).decode() if kind=="symlink" else "")
def names(path):
    found=[]
    handle=fs.opendir(path)
    try:
        while True:
            item=fs.readdir(handle)
            if item is None:
                break
            if item.d_name not in (b".",b".."):
                found.append(item.d_name.decode())
    finally:
        fs.closedir(handle)
    return sorted(found)
def touch(path):
    fs.close(fs.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644))
def create(case,name):
    path=at(case,name)
    try:
        if case["op"]=="mkdir":
            fs.mkdir(path,0o755)
        elif case["op"]=="symlink":
            fs.symlink(payload,path)
        elif case["op"]=="link":
            fs.link(at(case,"source"),path)
        else:
            fs.mknod(path,stat.S_IFREG|0o644,0)
        return 0
    except cephfs.Error as error:
        return error.errno
def remove(path,failed):
    try:
        if entry(path)["type"]=="dir":
            for name in names(path):
                remove(path+b"/"+name.encode(),failed)
            fs.rmdir(path)
        else:
            fs.unlink(path)
    except cephfs.Error as error:
        if error.errno!=errno.ENOENT:
            failed.append("%s: %s"%(path.decode(),error))
try:
    out=dict(client=fs.get_instance_id(),cases=[])
    if mode=="prepare":
        fs.mkdir(root.encode(),0o755)
        for case in cases:
            fs.mkdir(at(case),0o755)
            fs.mkdir(at(case,"there"),0o755)
            for name in ("kept","occupied","source"):
                touch(at(case,name))
            fs.symlink(case["live_target"].encode(),at(case,"live"))
            fs.symlink(case["link_target"].encode(),at(case,"link"))
    elif mode=="act":
        for case in cases:
            seen=dict(op=case["op"],form=case["form"],link_before=entry(at(case,"link")),target_before=entry(at(case,"there","absent")))
            for name in ("fresh","occupied","live","link"):
                seen[name]=create(case,name)
            out["cases"].append(seen)
    elif mode=="observe":
        for case in cases:
            seen=dict(op=case["op"],form=case["form"],target=entry(at(case,"there","absent")),there=names(at(case,"there")))
            for name in ("fresh","occupied","live","kept","source","link"):
                seen[name]=entry(at(case,name))
            out["cases"].append(seen)
    else:
        failed=[]
        remove(root.encode(),failed)
        if entry(root.encode())["type"]!="":
            failed.append("the root is still there")
        out["failed"]=failed
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

// TestCephFSCreateAtADanglingSymlink reports whether mkdir, symlink, hard
// link and mknod through libcephfs follow a symlink in the last component
// of the new name when its target does not exist.
//
// Given a directory per case with a dangling symlink, a regular file and a
// symlink to an existing file, and the controls (each operation creates at
// an unused name and reports EEXIST at the regular file and at the live
// symlink), when a second mount runs each operation at the dangling symlink
// and a third mount reads what the names are, then the defect is REPRODUCED
// if an operation succeeded and made its entry at the symlink's target, and
// NOT_REPRODUCED if every operation reported EEXIST and made nothing.
// Tentacle 20.2.4 reproduces it; Squid 19.2.2 does not.
//
// A failure of the given or when steps fails the test, and so does a result
// at the dangling symlink that is neither the contract nor the defect; the
// result does not.
func TestCephFSCreateAtADanglingSymlink(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_DANGLING_SYMLINK_CREATE image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	t.Logf("python3-cephfs: %s", harness.ExecOutput(t, ctx, client, "python3", "-c", "import cephfs;print(cephfs.__file__, cephfs.LibCephFS().version())"))
	cases := createCases(createRoot)
	spec, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context, mode string, into any) (string, error) {
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", createScript, mode, fs.FilesystemName, createRoot, string(spec), createPayload},
			Timeout: 3 * time.Minute, MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "CephFS create at a symlink " + mode,
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
	// The cases are removed before the client and the cluster go away, also
	// after a failure, which stays the test's first failure.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer stop()
		var removed struct {
			Failed []string `json:"failed"`
		}
		if _, err := run(cleanup, "remove", &removed); err != nil || len(removed.Failed) > 0 {
			t.Errorf("remove the cases: %v, %v", err, removed.Failed)
		}
	})
	var prepared struct {
		Client uint64 `json:"client"`
	}
	if _, err := run(ctx, "prepare", &prepared); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var acted struct {
		Client uint64      `json:"client"`
		Cases  []createAct `json:"cases"`
	}
	actedRaw, err := run(ctx, "act", &acted)
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var observed struct {
		Client uint64       `json:"client"`
		Cases  []createSeen `json:"cases"`
	}
	observedRaw, err := run(ctx, "observe", &observed)
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	if acted.Client == prepared.Client || observed.Client == acted.Client {
		t.Fatalf("condition not established: the clients %d, %d and %d of the three steps are not three mounts", prepared.Client, acted.Client, observed.Client)
	}
	t.Logf("CEPHFS_DANGLING_SYMLINK_CREATE_RECEIPT acted: %s", actedRaw)
	t.Logf("CEPHFS_DANGLING_SYMLINK_CREATE_RECEIPT observed by a later mount: %s", observedRaw)
	// The symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	if err := judgeCreate(cases, acted.Cases, observed.Cases); err != nil {
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
	t.Logf("EXPERIMENT_RESULT case=cephfs_dangling_symlink_create result=%s image=%s symptoms=%q", result, image, symptoms)
}
