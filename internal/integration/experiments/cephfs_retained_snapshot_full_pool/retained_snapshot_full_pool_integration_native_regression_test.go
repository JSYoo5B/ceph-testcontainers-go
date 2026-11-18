//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_retained_snapshot_full_pool)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_retained_snapshot_full_pool_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// image is the Ceph image the case runs on: Tentacle 20.2.0, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_retained_snapshot_full_pool_test.image=<image>"
//
// Tentacle 20.2.1 and Squid 19.2.7 are fixed; Squid 19.2.6 reproduces it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.0-all"

// fullRatios are the thresholds that make the cluster's few written bytes
// exceed the full ratio.
var fullRatios = ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .0003}

// fullnessChecks are the health checks that low thresholds raise.
var fullnessChecks = []string{"OSD_FULL", "OSD_BACKFILLFULL", "OSD_NEARFULL", "POOL_FULL", "POOL_NEARFULL", "POOL_BACKFILLFULL"}

// retainedScript mounts the filesystem with a libcephfs client of its own and
// runs one of two modes. "write" writes the content of a label (its SHA-256
// repeated to the given size) to a file, replacing what it held, and syncs
// it. "observe" reads each named file and reports its size and SHA-256 or the
// errno, lists each named directory, and parses the subvolume metadata file
// by section.
const retainedScript = `import cephfs,configparser,errno,hashlib,json,os,sys
mode,filesystem,spec=sys.argv[1],sys.argv[2],json.loads(sys.argv[3])
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","30")
fs.mount(filesystem_name=filesystem.encode())
def content(path):
    fd=fs.open(path.encode(),os.O_RDONLY)
    try:
        data=b""
        while True:
            chunk=fs.read(fd,len(data),1<<20)
            if not chunk:
                return data
            data+=chunk
    finally:
        fs.close(fd)
def read(path):
    try:
        data=content(path)
    except cephfs.Error as error:
        return dict(errno=error.errno)
    return dict(errno=0,size=len(data),sha256=hashlib.sha256(data).hexdigest())
def names(path):
    try:
        handle=fs.opendir(path.encode())
    except cephfs.Error as error:
        return dict(errno=error.errno,names=[])
    found=[]
    try:
        while True:
            item=fs.readdir(handle)
            if item is None:
                break
            if item.d_name not in (b".",b".."):
                found.append(item.d_name.decode())
    finally:
        fs.closedir(handle)
    return dict(errno=0,names=sorted(found))
def meta(path):
    try:
        data=content(path)
    except cephfs.Error as error:
        return dict(errno=error.errno,sections={})
    parser=configparser.ConfigParser(interpolation=None)
    parser.read_string(data.decode())
    return dict(errno=0,sections={s:dict(parser.items(s)) for s in parser.sections()})
try:
    out=dict(client=fs.get_instance_id())
    if mode=="write":
        block=hashlib.sha256(spec["label"].encode()).digest()
        data=block*(spec["size"]//len(block))
        fd=fs.open(spec["path"].encode(),os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o644)
        try:
            fs.write(fd,data,0)
            fs.fsync(fd,0)
        finally:
            fs.close(fd)
        out["sha256"]=hashlib.sha256(data).hexdigest()
    else:
        out["files"]={k:read(v) for k,v in spec["files"].items()}
        out["dirs"]={k:names(v) for k,v in spec["dirs"].items()}
        out["meta"]=meta(spec["meta"])
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

// observed is the report of retainedScript's "observe" mode.
type observed struct {
	Files map[string]fileSeen `json:"files"`
	Dirs  map[string]dirSeen  `json:"dirs"`
	Meta  metaSeen            `json:"meta"`
}

// TestCephFSRetainedSnapshotRemovalOnAFullPool reports whether a removal
// with retained snapshots that cannot persist the retained descriptor
// because the cluster is full leaves the subvolume inconsistent.
//
// Given a v2 subvolume whose snapshot and live incarnation hold different
// content, and a cluster whose full thresholds were lowered until OSD_FULL
// is raised, when "fs subvolume rm --retain-snapshots" fails and the
// thresholds are restored, then the defect is REPRODUCED if the persisted
// metadata still names the live incarnation while it is gone, if getpath,
// the snapshot or its getpath is lost, or if a retry does not retain the
// snapshot, and NOT_REPRODUCED if the failed removal changed nothing and the
// retry retains the snapshot. Tentacle 20.2.0 reproduces it; Tentacle 20.2.1
// does not.
//
// A failure of the given or when steps fails the test, and so does a state
// that is neither the contract nor the defect; the result does not.
func TestCephFSRetainedSnapshotRemovalOnAFullPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	run := func(mode string, spec, into any) string {
		t.Helper()
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", retainedScript, mode, fs.FilesystemName, string(data)},
			Timeout: 3 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "CephFS retained snapshot " + mode,
		})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			t.Fatalf("unavailable prerequisite: %s: exit=%d err=%v stderr=%.1024s", mode, result.ExitCode, err, result.Stderr)
		}
		if err := json.Unmarshal(result.Stdout, into); err != nil {
			t.Fatalf("unavailable prerequisite: %s: %v: %s", mode, err, result.Stdout)
		}
		return string(result.Stdout)
	}
	command := func(args ...string) cephOutcome {
		t.Helper()
		commandCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
		defer stop()
		out, err := cluster.Ceph(commandCtx, append([]string{"fs", "subvolume"}, args...)...)
		var failure *ceph.NativeFailure
		switch {
		case err == nil:
			return cephOutcome{OK: true, Out: string(out)}
		case errors.As(err, &failure):
			return cephOutcome{Errno: failure.Errno, Reason: failure.Reason, Out: string(out)}
		default:
			t.Fatalf("unavailable prerequisite: fs subvolume %s: %v", args[0], err)
			return cephOutcome{}
		}
	}
	write := func(file, label string) {
		t.Helper()
		var written struct {
			SHA256 string `json:"sha256"`
		}
		run("write", map[string]any{"path": file, "label": label, "size": retainedPayloadBytes}, &written)
		if written.SHA256 != retainedDigest(label) {
			t.Fatalf("unavailable prerequisite: %s holds %s, want %s", file, written.SHA256, retainedDigest(label))
		}
	}

	// Given: a subvolume whose snapshot holds one content and whose live
	// incarnation another.
	volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: retainedName})
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	write(volume.Path+"/data", snapshotLabel)
	snapshot, err := volume.CreateSnapshot(ctx, retainedSnapshot)
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}
	write(volume.Path+"/data", liveLabel)
	report := removalReport{LiveSHA: retainedDigest(liveLabel), SnapshotSHA: retainedDigest(snapshotLabel)}
	getPath := command("getpath", fs.FilesystemName, retainedName)
	snapshotGetPath := command("snapshot", "getpath", fs.FilesystemName, retainedName, retainedSnapshot)
	report.LivePath, report.SnapshotPath = strings.TrimSpace(getPath.Out), strings.TrimSpace(snapshotGetPath.Out)
	if !getPath.OK || !snapshotGetPath.OK || report.LivePath != volume.Path || report.SnapshotPath != snapshot.Path {
		t.Fatalf("control failed: getpath %s %q and snapshot getpath %s %q, want %s and %s", getPath, report.LivePath, snapshotGetPath, report.SnapshotPath, volume.Path, snapshot.Path)
	}
	base := path.Dir(report.LivePath)
	spec := map[string]any{
		"files": map[string]string{"live": report.LivePath + "/data", "snapshot": report.SnapshotPath + "/data"},
		"dirs":  map[string]string{"base": base, "trash": base + "/.trash"},
		"meta":  base + "/.meta",
	}
	var before observed
	beforeRaw := run("observe", spec, &before)
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT before: %s", beforeRaw)
	global := before.Meta.Sections["GLOBAL"]
	if before.Meta.Errno != 0 || global["version"] != "2" || global["type"] != "subvolume" || global["state"] != "complete" || global["path"] != report.LivePath {
		t.Fatalf("condition not established: the subvolume is not a complete v2 subvolume at %s: %+v", report.LivePath, before.Meta)
	}
	if before.Files["live"].SHA256 != report.LiveSHA || before.Files["snapshot"].SHA256 != report.SnapshotSHA {
		t.Fatalf("control failed: the live and snapshot files hold %s and %s, want %s and %s", before.Files["live"].SHA256, before.Files["snapshot"].SHA256, report.LiveSHA, report.SnapshotSHA)
	}
	original, err := observer.FullRatios(ctx)
	if err != nil {
		t.Fatalf("unavailable prerequisite: %v", err)
	}

	// When: the removal runs while the cluster is full, and the thresholds
	// are restored afterwards.
	handle, err := override.SetFullRatios(ctx, cluster, fullRatios)
	// A failed application may still own a partial override, and the
	// returned handle is never nil.
	t.Cleanup(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := handle.Restore(restoreCtx); err != nil {
			t.Errorf("restore the full ratios: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("condition not established: lower the full ratios: %v", err)
	}
	full := waitHealth(t, ctx, observer, func(checks map[string]ceph.HealthCheck) bool { _, ok := checks["OSD_FULL"]; return ok })
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT full: health %s with %v", full.Status, healthCodes(full))
	epoch := waitFullMap(t, ctx, cluster)
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT full: every pool is flagged full at osdmap epoch %d, and every OSD has that map", epoch)
	removal, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv:    []string{"ceph", "--connect-timeout", "5", "fs", "subvolume", "rm", fs.FilesystemName, retainedName, "--retain-snapshots"},
		Timeout: 3 * time.Minute, MaxStdout: 16 << 10, MaxStderr: 16 << 10, Label: "CephFS full-pool retained snapshot removal",
	})
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT removal on the full cluster: exit=%d error=%v stderr=%s",
		removal.ExitCode, err, removal.Stderr)
	if err != nil || !removal.Complete() {
		t.Fatalf("condition not established: unavailable observation of the removal: %v", err)
	}
	report.Trigger = fullRemovalTrigger{exitCode: removal.ExitCode, stderr: string(removal.Stderr)}
	if removal.ExitCode == 0 {
		var unexpected observed
		t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT after a removal that succeeded: %s", run("observe", spec, &unexpected))
	}
	if err := report.Trigger.verify(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Restore(ctx); err != nil {
		t.Fatalf("condition not established: restore the full ratios: %v", err)
	}
	restored, err := observer.FullRatios(ctx)
	if err != nil || restored.FSID != original.FSID || restored.Ratios != original.Ratios {
		t.Fatalf("condition not established: the full ratios read back as %+v (%v), want %+v", restored, err, original)
	}
	healthy := waitHealth(t, ctx, observer, func(checks map[string]ceph.HealthCheck) bool {
		return !slices.ContainsFunc(fullnessChecks, func(code string) bool { _, ok := checks[code]; return ok })
	})
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT restored: ratios %+v, health %s with %v", restored.Ratios, healthy.Status, healthCodes(healthy))

	// Then: what the failed removal left, and whether a retry retains the
	// snapshot.
	var after observed
	afterRaw := run("observe", spec, &after)
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT after the failed removal: %s", afterRaw)
	report.Meta, report.Live, report.Snapshot = after.Meta, after.Files["live"], after.Files["snapshot"]
	report.GetPath = command("getpath", fs.FilesystemName, retainedName)
	report.SnapshotGetPath = command("snapshot", "getpath", fs.FilesystemName, retainedName, retainedSnapshot)
	info := command("info", fs.FilesystemName, retainedName)
	report.Retry = command("rm", fs.FilesystemName, retainedName, "--retain-snapshots")
	if report.Retry.OK {
		report.RetryInfo = command("info", fs.FilesystemName, retainedName)
		var state struct {
			State string `json:"state"`
		}
		if report.RetryInfo.OK && json.Unmarshal([]byte(report.RetryInfo.Out), &state) == nil {
			report.RetryState = state.State
		}
		report.RetryGetPath = command("getpath", fs.FilesystemName, retainedName)
		report.RetrySnapshotGetPath = command("snapshot", "getpath", fs.FilesystemName, retainedName, retainedSnapshot)
		if report.RetrySnapshotGetPath.OK {
			var retained observed
			run("observe", map[string]any{
				"files": map[string]string{"snapshot": strings.TrimSpace(report.RetrySnapshotGetPath.Out) + "/data"},
				"dirs":  map[string]string{}, "meta": base + "/.meta",
			}, &retained)
			report.RetrySnapshot = retained.Files["snapshot"]
		}
	}
	t.Logf("CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT volumes interface: getpath %s %q, snapshot getpath %s %q, info %s, retry %s, after the retry info %s state %q, getpath %s, snapshot getpath %s %q, snapshot file %+v",
		report.GetPath, strings.TrimSpace(report.GetPath.Out), report.SnapshotGetPath, strings.TrimSpace(report.SnapshotGetPath.Out), info,
		report.Retry, report.RetryInfo, report.RetryState, report.RetryGetPath, report.RetrySnapshotGetPath, strings.TrimSpace(report.RetrySnapshotGetPath.Out), report.RetrySnapshot)
	symptoms, err := judgeRemoval(report)
	if err != nil {
		t.Fatal(err)
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_retained_snapshot_full_pool result=%s image=%s symptoms=%q", result, image, symptoms)

	// Cleanup: removing the last snapshot of a retained subvolume removes it.
	// A subvolume the defect left inconsistent cannot be removed through the
	// volumes interface and goes with the cluster.
	if result == "REPRODUCED" {
		t.Logf("the inconsistent subvolume %s is left to the cluster's termination", retainedName)
		return
	}
	if removed := command("snapshot", "rm", fs.FilesystemName, retainedName, retainedSnapshot); !removed.OK {
		t.Errorf("remove the retained snapshot: %s", removed)
	}
	if names, err := subvolume.Names(ctx, fs, ""); err != nil || slices.Contains(names, retainedName) {
		t.Errorf("the retained subvolume is still listed: %v %v", names, err)
	}
}

// waitFullMap waits until every pool of the cluster carries the full flag and
// every OSD has that osdmap, so that no OSD that has not seen the full map
// can accept a write from a client with an older map. It reads each OSD's
// newest map through its admin socket and returns the osdmap's epoch.
func waitFullMap(t *testing.T, ctx context.Context, cluster *ceph.Container) uint32 {
	t.Helper()
	osds := harness.OSDContainers(t, cluster)
	waitCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
	defer stop()
	var last string
	for {
		epoch, done, why := fullMapReached(waitCtx, cluster, osds)
		if done {
			return epoch
		}
		last = why
		select {
		case <-waitCtx.Done():
			t.Fatalf("condition not established: the full map did not reach every OSD: %v: %s", waitCtx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

// osdStatusScript prints the status of every OSD whose admin socket is in
// the container.
const osdStatusScript = `for s in /var/run/ceph/ceph-osd.*.asok; do timeout 10 ceph --admin-daemon "$s" status || exit 1; done`

// statusNumber is a number of an OSD's admin socket status, which some
// releases print as a JSON string.
type statusNumber uint32

func (n *statusNumber) UnmarshalJSON(data []byte) error {
	value, err := strconv.ParseUint(strings.Trim(string(data), `"`), 10, 32)
	*n = statusNumber(value)
	return err
}

// fullMapReached reads the osdmap and the newest map of each OSD once.
func fullMapReached(ctx context.Context, cluster *ceph.Container, osds []testcontainers.Container) (uint32, bool, string) {
	data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return 0, false, err.Error()
	}
	var dump struct {
		Epoch uint32 `json:"epoch"`
		Pools []struct {
			Name  string `json:"pool_name"`
			Flags string `json:"flags_names"`
		} `json:"pools"`
		OSDs []struct {
			ID int `json:"osd"`
			Up int `json:"up"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return 0, false, err.Error()
	}
	if len(dump.Pools) == 0 || len(dump.OSDs) == 0 {
		return 0, false, "the osdmap has no pools or no OSDs"
	}
	for _, pool := range dump.Pools {
		if !slices.Contains(strings.Split(pool.Flags, ","), "full") {
			return 0, false, fmt.Sprintf("pool %s has flags %q at epoch %d", pool.Name, pool.Flags, dump.Epoch)
		}
	}
	maps := map[int]uint32{}
	for _, osd := range osds {
		execCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		code, reader, err := osd.Exec(execCtx, []string{"sh", "-c", osdStatusScript}, tcexec.Multiplexed())
		var out []byte
		if err == nil {
			out, err = io.ReadAll(reader)
		}
		stop()
		if err != nil || code != 0 {
			return 0, false, fmt.Sprintf("OSD status: exit %d, %v: %.512s", code, err, out)
		}
		decoder := json.NewDecoder(bytes.NewReader(out))
		for decoder.More() {
			var status struct {
				WhoAmI    statusNumber `json:"whoami"`
				NewestMap statusNumber `json:"newest_map"`
			}
			if err := decoder.Decode(&status); err != nil {
				return 0, false, fmt.Sprintf("OSD status: %v: %.512s", err, out)
			}
			maps[int(status.WhoAmI)] = uint32(status.NewestMap)
		}
	}
	for _, osd := range dump.OSDs {
		newest, ok := maps[osd.ID]
		switch {
		case osd.Up != 1:
			return 0, false, fmt.Sprintf("osd.%d is down", osd.ID)
		case !ok:
			return 0, false, fmt.Sprintf("osd.%d has no admin socket in the OSD containers", osd.ID)
		case newest < dump.Epoch:
			return 0, false, fmt.Sprintf("osd.%d has map %d, the full map is %d", osd.ID, newest, dump.Epoch)
		}
	}
	return dump.Epoch, true, ""
}

// waitHealth polls the cluster's health until done accepts its checks.
func waitHealth(t *testing.T, ctx context.Context, observer *ceph.Observer, done func(map[string]ceph.HealthCheck) bool) ceph.HealthSnapshot {
	t.Helper()
	waitCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
	defer stop()
	var last ceph.HealthSnapshot
	for {
		snapshot, err := observer.HealthDetails(waitCtx)
		if err == nil {
			last = snapshot
			if done(snapshot.Checks) {
				return snapshot
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("condition not established: the health checks %v did not change as required: %v (last error %v)", healthCodes(last), waitCtx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// healthCodes are the codes of a health snapshot's checks.
func healthCodes(snapshot ceph.HealthSnapshot) []string {
	codes := make([]string, 0, len(snapshot.Checks))
	for code := range snapshot.Checks {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes
}
