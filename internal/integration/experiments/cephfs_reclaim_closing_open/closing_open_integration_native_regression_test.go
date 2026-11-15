//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_reclaim_closing_open)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_reclaim_closing_open_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// image is the Ceph image the case runs on: Tentacle 20.2.2, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_reclaim_closing_open_test.image=<image>"
//
// Tentacle 20.2.3 and Umbrella 21.2.0 carry the correction and do not
// reproduce it; no Squid release does.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all"

// The reclaim UUIDs of the two flows. Each target session carries its
// flow's UUID, and the flow's reclaimer adopts it after the RESET.
const (
	closingControlUUID = "closing-open-control"
	closingOverlapUUID = "closing-open-overlap"
)

// closingShutdownTimeout is the reclaimers' client_shutdown_timeout in
// seconds: how long a session close waits for the MDS before the client
// drops its own entry for the session.
const closingShutdownTimeout = 10

// closingHold is how long, in seconds, the overlap flow's mount is given
// while the OSD writes are still paused. It is shorter than the mount's
// client_mount_timeout of 30 seconds.
const closingHold = 15

// closingScript is the libcephfs actor of the case, driven through ctypes
// because the Python binding has no start_reclaim, finish_reclaim or
// set_uuid. Each flow is the RESET sequence of an NFS-style reclaimer: a
// target handle mounts with the flow's UUID; a fresh reclaimer handle
// RESETs that UUID, finishes the reclaim and adopts the UUID with
// ceph_set_uuid, which closes the reclaimer's first session; the reclaimer
// then mounts, which opens a session on the same rank with the UUID. The
// overlap flow sets the OSD pause flags before ceph_set_uuid, so the MDS
// cannot journal the close: the client gives up after its shutdown timeout
// and drops its session entry while the MDS keeps the session closing, and
// the mount's open reaches that closing session. The mount runs in a thread
// for the hold, and the flags are cleared after it. Session listings come
// from the active MDS, the client log lines from each reclaimer's own log.
// The actor prints one JSON report, clears the pause flags whatever
// happened, and leaves the handles to the process exit.
const closingScript = `import cephfs, ctypes as C, json, os, subprocess, sys, threading, time
fsname, control_uuid, overlap_uuid = sys.argv[1], sys.argv[2], sys.argv[3]
shutdown_timeout, hold = sys.argv[4], int(sys.argv[5])
lib = C.CDLL(cephfs.__file__)
P = C.c_void_p
for name, res, args in [
    ("ceph_create", C.c_int, [C.POINTER(P), C.c_char_p]),
    ("ceph_conf_read_file", C.c_int, [P, C.c_char_p]),
    ("ceph_conf_set", C.c_int, [P, C.c_char_p, C.c_char_p]),
    ("ceph_init", C.c_int, [P]),
    ("ceph_select_filesystem", C.c_int, [P, C.c_char_p]),
    ("ceph_set_uuid", None, [P, C.c_char_p]),
    ("ceph_start_reclaim", C.c_int, [P, C.c_char_p, C.c_uint]),
    ("ceph_finish_reclaim", None, [P]),
    ("ceph_mount", C.c_int, [P, C.c_char_p]),
    ("ceph_get_instance_id", C.c_uint64, [P]),
]:
    fn = getattr(lib, name)
    fn.restype, fn.argtypes = res, args
def ceph(*args):
    r = subprocess.run(["ceph"] + list(args), capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError("ceph %s: exit %d: %s" % (" ".join(args), r.returncode, r.stderr.strip()[-400:]))
    return r.stdout
def sessions():
    found = []
    for s in json.loads(ceph("tell", "mds.%s:0" % fsname, "session", "ls", "--format=json")):
        found.append({"id": s["id"], "state": s["state"], "uuid": s.get("client_metadata", {}).get("uuid", "")})
    return sorted(found, key=lambda s: s["id"])
def check(rc, what):
    if rc < 0:
        raise RuntimeError("%s: native rc=%d" % (what, rc))
def fresh(log, extra):
    h = P()
    check(lib.ceph_create(C.byref(h), None), "ceph_create")
    check(lib.ceph_conf_read_file(h, b"/etc/ceph/ceph.conf"), "ceph_conf_read_file")
    for k, v in [("client_mount_timeout", "30"), ("log_to_file", "true"), ("log_to_stderr", "false"),
                 ("log_file", log), ("debug_client", "10")] + extra:
        check(lib.ceph_conf_set(h, k.encode(), v.encode()), "ceph_conf_set " + k)
    check(lib.ceph_init(h), "ceph_init")
    check(lib.ceph_select_filesystem(h, fsname.encode()), "ceph_select_filesystem")
    return h
def client_log(path):
    keys = ("_open_mds_session", "_close_mds_session", "_closed_mds_session", "rejected us",
            "did not respond to session close", "handle_client_session", "handle_client_reclaim_reply")
    return [line.rstrip()[:400] for line in open(path, errors="replace") if any(k in line for k in keys)]
paused = False
def flow(name, uuid, overlap):
    global paused
    f = {"uuid": uuid}
    target = fresh("/tmp/closing-open-%s-target.log" % name, [])
    lib.ceph_set_uuid(target, uuid.encode())
    check(lib.ceph_mount(target, b"/"), "target mount")
    f["target"] = lib.ceph_get_instance_id(target)
    f["target_sessions"] = sessions()
    log = "/tmp/closing-open-%s-reclaimer.log" % name
    reclaimer = fresh(log, [("client_shutdown_timeout", shutdown_timeout)])
    f["reclaim_rc"] = lib.ceph_start_reclaim(reclaimer, uuid.encode(), 1)
    f["reclaimer"] = lib.ceph_get_instance_id(reclaimer)
    check(f["reclaim_rc"], "ceph_start_reclaim")
    lib.ceph_finish_reclaim(reclaimer)
    f["reclaimed_sessions"] = sessions()
    if overlap:
        ceph("osd", "pause")
        paused = True
        f["paused_flags"] = sorted(json.loads(ceph("osd", "dump", "--format=json"))["flags_set"])
    start = time.monotonic()
    lib.ceph_set_uuid(reclaimer, uuid.encode())
    f["close_seconds"] = round(time.monotonic() - start, 3)
    f["closing_sessions"] = sessions()
    result = {}
    def mount():
        result["rc"] = lib.ceph_mount(reclaimer, b"/")
    worker = threading.Thread(target=mount, daemon=True)
    worker.start()
    if overlap:
        worker.join(hold)
        f["held"] = {"returned": "rc" in result, "rc": result.get("rc"), "sessions": sessions()}
        ceph("osd", "unpause")
        paused = False
    worker.join(120)
    if "rc" not in result:
        raise RuntimeError("%s: the reclaimer's mount did not return" % name)
    f["mount_rc"] = result["rc"]
    deadline = time.monotonic() + 30
    while True:
        f["settled_sessions"] = sessions()
        if time.monotonic() > deadline or not any(s["state"] in ("opening", "closing", "killing") for s in f["settled_sessions"]):
            break
        time.sleep(1)
    f["client_log"] = client_log(log)
    return f
try:
    report = {"control": flow("control", control_uuid, False)}
    report["overlap"] = flow("overlap", overlap_uuid, True)
    print(json.dumps(report), flush=True)
finally:
    if paused:
        ceph("osd", "unpause")
sys.stdout.flush()
os._exit(0)
`

// TestCephFSReclaimOpenOnClosingSession reports whether the MDS refuses a
// client's session open that reaches the client's own session while that
// session is still closing, as a duplicated session uuid.
//
// Given a cluster with one filesystem and one active MDS at debug_mds 10,
// a control flow (a target handle mounted with a UUID, a reclaimer that
// RESETs it, adopts the UUID and mounts again, which succeeds), and an
// overlap flow with its own target and reclaimer whose adoption of the UUID
// runs while the OSD writes are paused, so that the client gives up waiting
// for its session close and the MDS still lists the session as closing;
// when the overlap reclaimer mounts and the pause is cleared after a hold;
// then the defect is REPRODUCED if the mount failed with EPERM during the
// hold with the MDS logging a duplicated-session-uuid refusal of that
// client, and NOT_REPRODUCED if the mount waited, the MDS handled the open
// again after the close was logged, and the mount succeeded with a session
// carrying the UUID. Tentacle 20.2.2 reproduces it; Tentacle 20.2.3 does
// not.
//
// A failure of the given or when steps fails the test, and so does an
// outcome that is neither the contract nor the defect; the result does not.
func TestCephFSReclaimOpenOnClosingSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_RECLAIM_CLOSING_OPEN image=%s", image)
	cluster, client := harness.NewSingleImageCluster(t, harness.CEPHFSRun, image, harness.FunctionalOSDConfig(t))
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if len(fs.MDSs()) != 1 {
		t.Fatalf("unavailable prerequisite: the filesystem has %d MDS daemons, want one", len(fs.MDSs()))
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	// The pause flags are cleared before the cluster goes away, also after a
	// failure, which stays the test's first failure.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer stop()
		if out, err := cluster.Ceph(cleanup, "osd", "unpause"); err != nil {
			t.Errorf("clear the OSD pause flags: %v: %s", err, out)
		}
	})
	if out, err := cluster.Ceph(ctx, "config", "set", "mds", "debug_mds", "10"); err != nil {
		t.Fatalf("unavailable prerequisite: set debug_mds: %v: %s", err, out)
	}
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"python3", "-c", closingScript, fs.FilesystemName, closingControlUUID, closingOverlapUUID,
			fmt.Sprint(closingShutdownTimeout), fmt.Sprint(closingHold)},
		Timeout: 8 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "CephFS reclaim open on a closing session",
	})
	if err != nil || result.ExitCode != 0 || !result.Complete() {
		t.Fatalf("unavailable prerequisite: the actor: exit=%d err=%v stderr=%.2048s stdout=%.2048s", result.ExitCode, err, result.Stderr, result.Stdout)
	}
	t.Logf("CEPHFS_RECLAIM_CLOSING_OPEN_RECEIPT actor: %s", result.Stdout)
	var report closingReport
	if err := json.Unmarshal(result.Stdout, &report); err != nil {
		t.Fatalf("unavailable prerequisite: the actor's report: %v", err)
	}
	if err := judgeClosingControl(report.Control, closingControlUUID, closingShutdownTimeout); err != nil {
		t.Fatalf("control failed: %v", err)
	}
	if err := judgeClosingCondition(report.Overlap, closingOverlapUUID, closingShutdownTimeout); err != nil {
		t.Fatal(err)
	}
	mdsLog, err := closingMDSLog(fs.MDSs()[0], report.Control.Reclaimer, report.Overlap.Reclaimer)
	if err != nil {
		t.Fatalf("unavailable prerequisite: the MDS log: %v", err)
	}
	for _, line := range mdsLog {
		t.Logf("CEPHFS_RECLAIM_CLOSING_OPEN_RECEIPT mds: %s", line)
	}
	if closingIndex(mdsLog, 0, closingRequestClose, report.Overlap.Reclaimer) < 0 {
		t.Fatal("unavailable prerequisite: the MDS log has no session close request of the overlap reclaimer; debug_mds did not apply")
	}
	reproduced, err := judgeClosingOverlap(report.Overlap, closingOverlapUUID, mdsLog)
	if err != nil {
		t.Fatal(err)
	}
	var symptoms []string
	verdict := "NOT_REPRODUCED"
	if reproduced {
		verdict = "REPRODUCED"
		symptoms = append(symptoms, closingSymptom)
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_reclaim_closing_open result=%s image=%s symptoms=%q", verdict, image, symptoms)
}

// closingSessionLine selects the session lines of the MDS log.
var closingSessionLine = regexp.MustCompile(`client_session\(|_session_logged|journal_close_session|(add|remove)_session|duplicated session uuid|existing session|ESession`)

// closingMDSLog reads the MDS container's log to its end and returns, in
// their order, the session lines (session requests, journaled session
// events, session map changes and duplicated-UUID refusals) that name one of
// the reclaimers' client entities.
// A log that cannot be opened or read to its end is an error, whatever was
// read of it.
func closingMDSLog(mds *cephfs.MDS, clients ...uint64) (found []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	logs, err := mds.Logs(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, logs.Close()) }()
	var entities []*regexp.Regexp
	for _, id := range clients {
		entities = append(entities, regexp.MustCompile(fmt.Sprintf(`client\.%d\b`, id)))
	}
	scanner := bufio.NewScanner(logs)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !closingSessionLine.MatchString(line) {
			continue
		}
		for _, entity := range entities {
			if entity.MatchString(line) {
				found = append(found, line)
				break
			}
		}
	}
	return found, scanner.Err()
}
