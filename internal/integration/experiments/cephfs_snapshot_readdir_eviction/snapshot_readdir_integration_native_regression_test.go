//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_snapshot_readdir_eviction)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_snapshot_readdir_eviction_test

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
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_snapshot_readdir_eviction_test.image=<image>"
//
// No release is fixed: Squid 19.2.2 and 19.2.5, Tentacle 20.2.5 and
// Umbrella 21.2.0 reproduce it too.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

const (
	// snapshotRoot is the directory of the filesystem that holds the case.
	snapshotRoot = "/snapshot-readdir-eviction"
	// snapshotName is the snapshot of snapshotRoot's "dir".
	snapshotName = "s1"
	// snapshotSettle is how long a listing client waits after its lookups
	// before it reads on. A client trims its cache on its tick, once a
	// second by default, so the wait spans several ticks.
	snapshotSettle = 4
)

// snapshotScript mounts the filesystem with a libcephfs client of its own
// and runs one of three modes. "prepare" fills "dir" with the snapshot's
// files and "other" with the files to look up, snapshots "dir" and replaces
// its files with the live ones. "list" mounts with the given
// client_cache_size (0 keeps the default) and the client's log in a file of
// its own, opens the snapshot directory, reads the first entries, looks up
// every file of "other", waits, reads the rest through the same handle and
// lists "dir" half a second later, so that the log's millisecond timestamps
// tell the two listings apart; after the unmount it reads from the client's log the readdir
// requests it sent for the snapshot handle and whether it dropped its
// dentry of the snapshot between the two reads, and removes the log.
// "remove" removes the snapshot and everything below the root and reports
// what is left.
const snapshotScript = `import cephfs,datetime,errno,json,os,sys,time
mode,filesystem,root=sys.argv[1],sys.argv[2],sys.argv[3]
spec=json.loads(sys.argv[4])
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
log=None
if mode=="list":
    log="/tmp/snapshot-readdir-eviction-%d.log"%os.getpid()
    for name,value in (("log_file",log),("log_to_file","true"),("log_to_stderr","false"),("err_to_stderr","false"),("debug_client","15")):
        fs.conf_set(name,value)
    if spec["cache"]:
        fs.conf_set("client_cache_size",str(spec["cache"]))
fs.mount(filesystem_name=filesystem.encode())
def at(*names):
    return "/".join((root,)+names).encode()
def touch(path):
    fs.close(fs.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o644))
def take(handle,limit):
    found=[]
    while limit is None or len(found)<limit:
        item=fs.readdir(handle)
        if item is None:
            break
        if item.d_name not in (b".",b".."):
            found.append(item.d_name.decode())
    return found
def listing(path):
    handle=fs.opendir(path)
    try:
        return take(handle,None)
    finally:
        fs.closedir(handle)
out=dict(client=fs.get_instance_id())
marks=[]
try:
    if mode=="prepare":
        for directory in ((),("dir",),("other",)):
            fs.mkdir(at(*directory),0o755)
        for name in spec["snapshot_names"]:
            touch(at("dir",name))
        for name in spec["other_names"]:
            touch(at("other",name))
        fs.mkdir(at("dir",".snap",spec["snapshot"]),0o755)
        for name in spec["snapshot_names"]:
            fs.unlink(at("dir",name))
        for name in spec["live_names"]:
            touch(at("dir",name))
    elif mode=="list":
        out["cache"]=int(fs.conf_get("client_cache_size"))
        handle=fs.opendir(at("dir",".snap",spec["snapshot"]))
        try:
            names=take(handle,spec["first"])
            out["first"]=len(names)
            marks.append(time.time())
            for name in spec["other_names"]:
                fs.stat(at("other",name))
            out["stats"]=len(spec["other_names"])
            time.sleep(spec["settle"])
            marks.append(time.time())
            names+=take(handle,None)
            marks.append(time.time())
        finally:
            fs.closedir(handle)
        time.sleep(0.5)
        out["names"]=names
        out["live"]=listing(at("dir"))
    else:
        failed=[]
        def attempt(what,call,*args):
            try:
                call(*args)
            except cephfs.Error as error:
                if error.errno!=errno.ENOENT:
                    failed.append("%s: %s"%(what,error))
        attempt("rmdir the snapshot",fs.rmdir,at("dir",".snap",spec["snapshot"]))
        for directory in ("dir","other"):
            try:
                for name in listing(at(directory)):
                    attempt("unlink "+name,fs.unlink,at(directory,name))
            except cephfs.Error as error:
                if error.errno!=errno.ENOENT:
                    failed.append("list %s: %s"%(directory,error))
            attempt("rmdir "+directory,fs.rmdir,at(directory))
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
if mode=="list":
    out.update(requests=0,head_requests=0,trimmed=False,evidence=[])
    trim="trim_dentry unlinking dn %s in dir"%spec["snapshot"]
    with open(log,errors="replace") as lines:
        for line in lines:
            request="send_request client_request(" in line and " readdir #" in line
            if not request and trim not in line:
                continue
            at_time=datetime.datetime.strptime(line.split(" ",1)[0],"%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            if request and at_time<=marks[2]:
                out["requests"]+=1
                if "//"+spec["snapshot"]+" " not in line:
                    out["head_requests"]+=1
                    if out["head_requests"]<=2:
                        out["evidence"].append(line.strip()[:240])
            elif not request and marks[0]<=at_time<=marks[1]:
                out["trimmed"]=True
                out["evidence"].append(line.strip()[:240])
    out["log_bytes"]=os.path.getsize(log)
    os.unlink(log)
print(json.dumps(out))
`

// TestCephFSSnapshotListingAcrossCacheEviction reports whether a libcephfs
// handle of a snapshot directory returns entries of the live directory once
// the client's cache trim drops the snapshot's dentries.
//
// Given a snapshotted directory with more entries than one readdir reply and
// a live directory of other names, and the conditions and controls (a client
// with the default cache lists the snapshot exactly, both clients list the
// live directory exactly, and the small-cache client dropped its dentry of
// the snapshot between its first entries and the rest), when a client with a
// small cache reads the first entries, looks up files elsewhere, waits a few
// ticks and reads on, then the defect is REPRODUCED if the entries read after
// the trim hold names of the live directory, and NOT_REPRODUCED if the
// listing is the snapshot. Tentacle 20.2.4 reproduces it.
//
// A failure of the given or when steps fails the test, and so does a listing
// that is neither the snapshot nor the defect; the result does not.
func TestCephFSSnapshotListingAcrossCacheEviction(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_SNAPSHOT_READDIR_EVICTION image=%s", image)
	cluster, client := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	t.Logf("python3-cephfs: %s", harness.ExecOutput(t, ctx, client, "python3", "-c", "import cephfs;print(cephfs.__file__, cephfs.LibCephFS().version())"))
	want, live := snapshotNames(snapshotPrefix, snapshotEntries), snapshotNames(livePrefix, snapshotEntries)
	run := func(ctx context.Context, mode string, cache int, into any) error {
		spec, err := json.Marshal(map[string]any{
			"snapshot": snapshotName, "snapshot_names": want, "live_names": live, "other_names": snapshotNames("other-", snapshotOthers),
			"first": snapshotFirst, "cache": cache, "settle": snapshotSettle,
		})
		if err != nil {
			return err
		}
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", snapshotScript, mode, fs.FilesystemName, snapshotRoot, string(spec)},
			Timeout: 5 * time.Minute, MaxStdout: 4 << 20, MaxStderr: 16 << 10, Label: "CephFS snapshot listing " + mode,
		})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			return fmt.Errorf("%s: exit=%d err=%v stderr=%.1024s", mode, result.ExitCode, err, result.Stderr)
		}
		if err := json.Unmarshal(result.Stdout, into); err != nil {
			return fmt.Errorf("%s: %w: %.1024s", mode, err, result.Stdout)
		}
		return nil
	}
	// The snapshot and the files are removed before the client and the
	// cluster go away, also after a failure, which stays the test's first
	// failure.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
		defer stop()
		var removed struct {
			Failed []string `json:"failed"`
		}
		if err := run(cleanup, "remove", 0, &removed); err != nil || len(removed.Failed) > 0 {
			t.Errorf("remove the snapshot and the files: %v, %v", err, removed.Failed)
		}
	})
	var prepared struct {
		Client uint64 `json:"client"`
	}
	if err := run(ctx, "prepare", 0, &prepared); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	var control, trigger snapshotListing
	for _, listing := range []struct {
		name  string
		cache int
		into  *snapshotListing
	}{{"default cache", 0, &control}, {"small cache", snapshotCache, &trigger}} {
		if err := run(ctx, "list", listing.cache, listing.into); err != nil {
			t.Fatalf("unavailable prerequisite: %v", err)
		}
		got := *listing.into
		t.Logf("CEPHFS_SNAPSHOT_READDIR_EVICTION_RECEIPT %s: client %d with client_cache_size %d read %d snapshot entries, looked up %d files elsewhere, waited %d s and read on; snapshot dentry dropped in between: %t; readdir requests for the handle: %d, of them without the snapshot in the path: %d; snapshot listing: %s; live listing: %s; client log: %q",
			listing.name, got.Client, got.Cache, got.First, got.Stats, snapshotSettle, got.Trimmed, got.Requests, got.HeadRequests,
			snapshotDiffer(want, live, got.Names), snapshotDiffer(live, want, got.Live), got.Evidence)
	}
	// The symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	if err := judgeSnapshotListings(want, live, control, trigger); err != nil {
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
	t.Logf("EXPERIMENT_RESULT case=cephfs_snapshot_readdir_eviction result=%s image=%s symptoms=%q", result, image, symptoms)
}
