//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// This file is the native recipe for a UUID-targeted CephFS session RESET. It
// drives the pinned libcephfs C entry points through a test-owned Python
// ctypes actor, because the Python binding has no start_reclaim or
// finish_reclaim, and hands every verdict to the Docker-free judge in
// cephfs_reclaim_reset_helpers_test.go. It is test tooling only: nothing here
// is a public helper, a product mount dependency or a stability claim.
//
// What it exercises is the RESET path. State-preserving reclaim appears only
// as the refusal it gets: the pinned server rejects every reclaim flag except
// CEPH_RECLAIM_RESET, and the recipe asks once with flags 0 to record that
// -EINVAL from the MDS. The replacement client gets a new native client ID and opens its own descriptor.
// No old fd, capability or lock is adopted, and no NFS grace protocol is
// involved. The blocklist and OSD epoch effects of the eviction are not
// observed.

// reclaimCABI declares every libcephfs entry point the actors call. The first
// block lists exactly the symbols of ResetABIContract by the same type names,
// so the eligibility probe reports what the actors really declared, and the
// Go side compares it with the contract derived from libcephfs.h. The second
// block holds the entry points that only the actors use. A void function is
// declared with the restype "None"; ceph_start_reclaim returns a signed
// negative errno, never ctypes.get_errno().
const reclaimCABI = `
import ctypes as C, fcntl, hashlib
_lib = C.CDLL(cephfs.__file__)
lib = _lib
_T = {"c_int": C.c_int, "c_uint": C.c_uint, "c_uint64": C.c_uint64, "c_void_p": C.c_void_p,
      "c_char_p": C.c_char_p, "None": None, "POINTER(c_void_p)": C.POINTER(C.c_void_p),
      "POINTER(c_int)": C.POINTER(C.c_int)}
CONTRACT = {
 "ceph_create": ("c_int", ["POINTER(c_void_p)", "c_char_p"]),
 "ceph_conf_read_file": ("c_int", ["c_void_p", "c_char_p"]),
 "ceph_conf_set": ("c_int", ["c_void_p", "c_char_p", "c_char_p"]),
 "ceph_init": ("c_int", ["c_void_p"]),
 "ceph_select_filesystem": ("c_int", ["c_void_p", "c_char_p"]),
 "ceph_set_uuid": ("None", ["c_void_p", "c_char_p"]),
 "ceph_start_reclaim": ("c_int", ["c_void_p", "c_char_p", "c_uint"]),
 "ceph_finish_reclaim": ("None", ["c_void_p"]),
 "ceph_release": ("c_int", ["c_void_p"]),
 "ceph_get_instance_id": ("c_uint64", ["c_void_p"]),
 "ceph_version": ("c_char_p", ["POINTER(c_int)", "POINTER(c_int)", "POINTER(c_int)"]),
}
EXTRA = {
 "ceph_set_session_timeout": ("None", ["c_void_p", "c_uint"]),
 "ceph_mount": ("c_int", ["c_void_p", "c_char_p"]),
 "ceph_unmount": ("c_int", ["c_void_p"]),
 "ceph_shutdown": ("None", ["c_void_p"]),
 "ceph_open": ("c_int", ["c_void_p", "c_char_p", "c_int", "c_uint"]),
 "ceph_close": ("c_int", ["c_void_p", "c_int"]),
 "ceph_flock": ("c_int", ["c_void_p", "c_int", "c_int", "c_uint64"]),
 "ceph_read": ("c_int", ["c_void_p", "c_int", "c_void_p", "c_int64", "c_int64"]),
}
_T["c_int64"] = C.c_int64
def declare(name, spec):
    fn = getattr(_lib, name)
    fn.restype = _T[spec[0]]
    fn.argtypes = [_T[a] for a in spec[1]]
def emit(doc):
    print("RECLAIM_JSON " + json.dumps(doc, sort_keys=True), flush=True)
`

// reclaimActorPreamble declares every symbol, failing the actor at once when
// one does not resolve, and defines the handle helpers. fresh creates a
// handle, reads the cluster configuration, initializes it and selects the
// filesystem; the caller owns the returned handle.
const reclaimActorPreamble = reclaimCABI + `
for _name, _spec in list(CONTRACT.items()) + list(EXTRA.items()):
    declare(_name, _spec)
def check(rc):
    if rc < 0:
        raise RuntimeError("native rc=%d" % rc)
    return rc
def fresh(fsname, log=None):
    h = C.c_void_p()
    check(lib.ceph_create(C.byref(h), None))
    try:
        check(lib.ceph_conf_read_file(h, b"/etc/ceph/ceph.conf"))
        check(lib.ceph_conf_set(h, b"client_mount_timeout", b"30"))
        if log:
            check(lib.ceph_conf_set(h, b"log_to_file", b"true"))
            check(lib.ceph_conf_set(h, b"log_to_stderr", b"false"))
            check(lib.ceph_conf_set(h, b"log_file", log.encode()))
            check(lib.ceph_conf_set(h, b"debug_client", b"10"))
        check(lib.ceph_init(h))
        check(lib.ceph_select_filesystem(h, fsname.encode()))
        return h
    except BaseException:
        lib.ceph_release(h)
        raise
def handle_id(h):
    return "%d:%x" % (os.getpid(), h.value)
`

// ReclaimABIProbe is the eligibility probe. It resolves each contract symbol
// without failing on a missing one, so the Go reader reports it as an explicit
// blocker, reports the types it declared, the loaded library and extension
// hashes and the library's own version, and creates and releases one handle
// that it never initializes. Arguments: the executor's image config ID.
const ReclaimABIProbe = `
import cephfs, hashlib, json, sys
` + reclaimCABI + `
symbols = {}
for name, spec in CONTRACT.items():
    try:
        declare(name, spec)
        symbols[name] = {"resolved": True, "restype": spec[0], "argtypes": spec[1]}
    except AttributeError:
        symbols[name] = {"resolved": False, "restype": spec[0], "argtypes": spec[1]}
maps = sorted(set(line.split()[-1] for line in open("/proc/self/maps") if "libcephfs.so" in line))
if len(maps) != 1:
    sys.exit("expected exactly one mapped libcephfs, found %r" % maps)
def digest(path):
    with open(path, "rb") as source:
        return hashlib.sha256(source.read()).hexdigest()
major, minor, patch = C.c_int(), C.c_int(), C.c_int()
lib.ceph_version(C.byref(major), C.byref(minor), C.byref(patch))
created, released, initialized = -1, -1, False
if all(entry["resolved"] for entry in symbols.values()):
    handle = C.c_void_p()
    created = lib.ceph_create(C.byref(handle), None)
    if created == 0:
        initialized = lib.ceph_get_instance_id(handle) != 0
        released = lib.ceph_release(handle)
emit({"image_config_id": sys.argv[1], "extension_sha256": digest(cephfs.__file__),
      "library": {"path": maps[0], "sha256": digest(maps[0]),
                  "version": {"major": major.value, "minor": minor.value, "patch": patch.value}},
      "symbols": symbols,
      "probe_handle": {"create_rc": created, "release_rc": released, "initialized": initialized}})
`

// ReclaimSeed writes two files through one mount and fsyncs them: the first
// carries the acknowledged payload that the replacement client must read back
// after RESET, the second is the unrelated live client's lock file. Arguments:
// the filesystem, the hex of the payload and the two paths.
const ReclaimSeed = `
import cephfs, hashlib, json, os, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
payload = bytes.fromhex(sys.argv[2])
for path in sys.argv[3:5]:
    fd = fs.open(path.encode(), os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    written = fs.write(fd, payload, 0)
    assert written == len(payload), written
    fs.fsync(fd, 0)
    fs.close(fd)
print("RECLAIM_JSON " + json.dumps({"client": fs.get_instance_id(), "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}))
fs.unmount()
fs.shutdown()
`

// ReclaimHolder is a long-lived client. It sets its UUID and a 600-second
// session timeout before mounting, takes an exclusive non-blocking flock on a
// file through its own descriptor and publishes receipt 1 with its native
// client ID. After the "finish" gate it releases everything and publishes
// receipt 2. Arguments: the file and the UUID.
const ReclaimHolder CephFSActorScript = CephFSActorHelpers + reclaimActorPreamble + `
path, uuid = sys.argv[4:6]
h = fresh(name)
lib.ceph_set_uuid(h, uuid.encode())
lib.ceph_set_session_timeout(h, 600)
check(lib.ceph_mount(h, None))
fd = check(lib.ceph_open(h, path.encode(), os.O_RDONLY, 0))
check(lib.ceph_flock(h, fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1))
publish(1, client=lib.ceph_get_instance_id(h), uuid=uuid)
gate("finish")
check(lib.ceph_flock(h, fd, fcntl.LOCK_UN, 1))
check(lib.ceph_close(h, fd))
check(lib.ceph_unmount(h))
lib.ceph_shutdown(h)
publish(2, closed=True)
`

// ReclaimAttempt makes one start_reclaim call with the given flags on a
// handle of its own that has not made any reclaim call, and releases the
// handle. The handle logs at debug_client 10 to a file of its own, and the
// results of the client_reclaim_reply messages that arrived from the MDS
// (handle_client_reclaim_reply) are read back from it, so a server refusal is
// told apart from the client's own argument checks. Arguments: the
// filesystem, the target UUID, the handle's own UUID, the flags and the log
// path.
const ReclaimAttempt = `
import cephfs, json, os, re, sys
` + reclaimActorPreamble + `
name, target, own, flags, log = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
h = fresh(name, log)
lib.ceph_set_uuid(h, own.encode())
rc = lib.ceph_start_reclaim(h, target.encode(), flags)
released = lib.ceph_release(h)
replies = [int(m.group(1)) for m in re.finditer(r"handle_client_reclaim_reply client_reclaim_reply\((-?\d+) e ", open(log).read())]
emit({"handle": handle_id(h), "initialized": True, "prior_reclaim_calls": 0, "flags": flags,
      "target": target, "own_uuid": own, "rc": rc, "release_rc": released, "mds_replies": replies})
`

// ReclaimReclaimer is the RESET client. On a handle of its own it makes one
// start_reclaim call with CEPH_RECLAIM_RESET and publishes receipt 1 with the
// attempt before it mounts. At the "mount" gate it finishes the reclaim, takes
// over the former UUID, mounts, opens the seeded file on a new descriptor,
// reads it and publishes receipt 2. After the "finish" gate it closes
// everything and publishes receipt 3. Arguments: the target UUID, its own UUID
// for the attempt and the seeded file.
const ReclaimReclaimer CephFSActorScript = CephFSActorHelpers + reclaimActorPreamble + `
target, own, path = sys.argv[4:7]
h = fresh(name)
lib.ceph_set_uuid(h, own.encode())
rc = lib.ceph_start_reclaim(h, target.encode(), 1)
publish(1, handle=handle_id(h), initialized=True, prior_reclaim_calls=0, flags=1, target=target, own_uuid=own, rc=rc)
if rc != 0:
    lib.ceph_release(h)
    sys.exit(3)
gate("mount")
lib.ceph_finish_reclaim(h)
lib.ceph_set_uuid(h, target.encode())
check(lib.ceph_mount(h, None))
fd = check(lib.ceph_open(h, path.encode(), os.O_RDONLY, 0))
buf = C.create_string_buffer(4096)
size = check(lib.ceph_read(h, fd, buf, 4096, 0))
data = buf.raw[:size]
publish(2, client=lib.ceph_get_instance_id(h), size=size, sha256=hashlib.sha256(data).hexdigest(), new_fd=True)
gate("finish")
check(lib.ceph_close(h, fd))
check(lib.ceph_unmount(h))
lib.ceph_shutdown(h)
publish(3, closed=True)
`

// ReclaimLockProbe tries a non-blocking exclusive flock on two files through
// a mount of its own, in another container and process than the holders, and
// releases a lock it was granted. Errno 0 means granted, 11 EWOULDBLOCK.
// Arguments: the filesystem and the two paths.
const ReclaimLockProbe = `
import cephfs, json, os, sys, fcntl
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
out = {"client": fs.get_instance_id()}
for key, path in (("a_errno", sys.argv[2]), ("u_errno", sys.argv[3])):
    fd = fs.open(path.encode(), os.O_RDONLY, 0)
    errno = 0
    try:
        fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 77)
    except cephfs.Error as error:
        errno = abs(getattr(error, "errno", None) or -1)
    else:
        fs.flock(fd, fcntl.LOCK_UN, 77)
    out[key] = errno
    fs.close(fd)
print("RECLAIM_JSON " + json.dumps(out))
fs.unmount()
fs.shutdown()
`

// ReclaimReceipt is one receipt of the reclaim actors. Unknown fields are
// rejected, and a missing return value stays nil.
type ReclaimReceipt struct {
	Token             string `json:"token"`
	Phase             int    `json:"phase"`
	PID               int    `json:"pid"`
	Client            uint64 `json:"client"`
	UUID              string `json:"uuid"`
	Handle            string `json:"handle"`
	Initialized       bool   `json:"initialized"`
	PriorReclaimCalls int    `json:"prior_reclaim_calls"`
	Flags             uint   `json:"flags"`
	Target            string `json:"target"`
	OwnUUID           string `json:"own_uuid"`
	RC                *int   `json:"rc"`
	Size              int    `json:"size"`
	SHA256            string `json:"sha256"`
	NewFd             bool   `json:"new_fd"`
	Closed            bool   `json:"closed"`
}

// reclaimRegisteredReceipt waits for receipt phase of an actor, stamps the
// time at which the test saw it and rejects a receipt of another run or phase.
// It fails at once when the actor ends without publishing it.
func reclaimRegisteredReceipt(t *testing.T, ctx context.Context, run *CephFSActorRun, phase int) (ReclaimReceipt, time.Time) {
	t.Helper()
	path := run.workdir + "/receipt-" + strconv.Itoa(phase)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, out, err := SessionExec(ctx, run.client, "cat", path)
		if err == nil && code == 0 {
			seen := time.Now()
			decoder := json.NewDecoder(strings.NewReader(out))
			decoder.DisallowUnknownFields()
			var receipt ReclaimReceipt
			if err := decoder.Decode(&receipt); err != nil {
				t.Fatalf("receipt %d: %v: %q", phase, err, out)
			}
			if receipt.Token != run.Token || receipt.Phase != phase || receipt.PID <= 0 {
				t.Fatalf("receipt %d belongs to another run or phase: %+v", phase, receipt)
			}
			return receipt, seen
		}
		select {
		case <-run.Done:
			t.Fatalf("actor ended without receipt %d: %s", phase, run.exited(ctx))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt %d did not appear", phase)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("receipt %d: %v", phase, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ReclaimJSONLine returns the document of the last RECLAIM_JSON line of an
// output, so library noise on the same stream cannot corrupt it.
func ReclaimJSONLine(out string) ([]byte, error) {
	const prefix = "RECLAIM_JSON "
	var found []byte
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			found = []byte(rest)
		}
	}
	if found == nil {
		return nil, fmt.Errorf("no %q line in %.256q", strings.TrimSpace(prefix), out)
	}
	return found, nil
}

// ReclaimAttemptResult is the printed result of ReclaimAttempt.
type ReclaimAttemptResult struct {
	Handle            string `json:"handle"`
	Initialized       bool   `json:"initialized"`
	PriorReclaimCalls int    `json:"prior_reclaim_calls"`
	Flags             uint   `json:"flags"`
	Target            string `json:"target"`
	OwnUUID           string `json:"own_uuid"`
	RC                *int   `json:"rc"`
	ReleaseRC         *int   `json:"release_rc"`
	MDSReplies        []int  `json:"mds_replies"`
}

// ParseReclaimAttempt reads one attempt. It rejects unknown fields, and a
// missing return value, release code or reply list stays nil.
func ParseReclaimAttempt(out string) (ReclaimAttemptResult, error) {
	var r ReclaimAttemptResult
	doc, err := ReclaimJSONLine(out)
	if err != nil {
		return r, err
	}
	decoder := json.NewDecoder(bytes.NewReader(doc))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, fmt.Errorf("reclaim attempt result: %w", err)
	}
	return r, nil
}

// reclaimLockResult is the printed result of ReclaimLockProbe.
type reclaimLockResult struct {
	Client uint64 `json:"client"`
	AErrno *int   `json:"a_errno"`
	UErrno *int   `json:"u_errno"`
}

// ParseReclaimLock reads the lock probe. Only EWOULDBLOCK (11) means held and
// only 0 means free; any other errno and any missing value is unknown.
func ParseReclaimLock(out string) (a, u ResetLock, err error) {
	doc, err := ReclaimJSONLine(out)
	if err != nil {
		return ResetLockUnknown, ResetLockUnknown, err
	}
	var r reclaimLockResult
	decoder := json.NewDecoder(bytes.NewReader(doc))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return ResetLockUnknown, ResetLockUnknown, fmt.Errorf("lock probe result: %w", err)
	}
	return classifyReclaimLock(r.AErrno), classifyReclaimLock(r.UErrno), nil
}

func classifyReclaimLock(errno *int) ResetLock {
	switch {
	case errno == nil:
		return ResetLockUnknown
	case *errno == 0:
		return ResetLockFree
	case *errno == 11:
		return ResetLockHeld
	}
	return ResetLockUnknown
}

// reclaimAttemptOf turns an attempt result into the judge's attempt. The
// release code is checked separately: a handle that cannot be released is not
// a clean attempt.
func reclaimAttemptOf(r ReclaimAttemptResult, start, end time.Time) ResetAttempt {
	return ResetAttempt{Handle: r.Handle, Initialized: r.Initialized, PriorReclaimCalls: r.PriorReclaimCalls,
		Flags: r.Flags, Target: r.Target, OwnUUID: r.OwnUUID, RC: r.RC, Replies: r.MDSReplies, Start: start, End: end}
}

// runReclaimAttempt runs ReclaimAttempt in the helper container, requires a
// clean release and returns the timed attempt.
func runReclaimAttempt(t *testing.T, ctx context.Context, m *CephFSSingleMDS, helper testcontainers.Container, name, target, own string, flags uint) ResetAttempt {
	t.Helper()
	log := "/tmp/reclaim-attempt-" + own + ".log"
	start := time.Now()
	out := ExecOutput(t, ctx, helper, "python3", "-c", ReclaimAttempt, m.FS.FilesystemName, target, own, strconv.FormatUint(uint64(flags), 10), log)
	end := time.Now()
	r, err := ParseReclaimAttempt(out)
	if err != nil {
		t.Fatal(err)
	}
	if r.ReleaseRC == nil || *r.ReleaseRC != 0 || r.MDSReplies == nil {
		t.Fatalf("the %s handle was not released cleanly or its log was not read: %+v", name, r)
	}
	return reclaimAttemptOf(r, start, end)
}

// reclaimAttemptString renders an attempt for the receipt, with the return
// value as a number or "unrecorded".
func reclaimAttemptString(a ResetAttempt) string {
	rc := "unrecorded"
	if a.RC != nil {
		rc = strconv.Itoa(*a.RC)
	}
	return fmt.Sprintf("handle=%s initialized=%t prior_reclaim_calls=%d flags=%d target=%s own_uuid=%s rc=%s mds_replies=%v", a.Handle, a.Initialized, a.PriorReclaimCalls, a.Flags, a.Target, a.OwnUUID, rc, a.Replies)
}

var reclaimVersionRE = regexp.MustCompile(`ceph version (\d+)\.(\d+)\.(\d+) `)

var reclaimSemver = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)$`)

// ReclaimExpectedRelease reads the release the MDS reports. StartCephFSSingleMDS
// keeps the daemon's "tell version" answer, a JSON object whose "version" is a
// bare semantic version (for example {"version":"20.2.4","release":"tentacle",
// "release_type":"stable"}), with its whitespace collapsed. The version must be
// a string of exactly three decimal parts; a missing, null or non-string field
// is an error. Raw "ceph version X.Y.Z (...)" CLI text is accepted only when
// the answer is not a JSON object.
func ReclaimExpectedRelease(answer string) (major, minor, patch int64, err error) {
	version := ""
	var doc map[string]json.RawMessage
	if json.Unmarshal([]byte(answer), &doc) == nil && doc != nil {
		raw, ok := doc["version"]
		if !ok {
			return 0, 0, 0, fmt.Errorf("daemon version answer has no version field: %.200q", answer)
		}
		if err := json.Unmarshal(raw, &version); err != nil || len(raw) == 0 || raw[0] != '"' {
			return 0, 0, 0, fmt.Errorf("daemon version field is not a string: %.64s", raw)
		}
	} else if match := reclaimVersionRE.FindStringSubmatch(answer); match != nil {
		version = match[1] + "." + match[2] + "." + match[3]
	} else {
		return 0, 0, 0, fmt.Errorf("no ceph version in %.200q", answer)
	}
	match := reclaimSemver.FindStringSubmatch(version)
	if match == nil {
		return 0, 0, 0, fmt.Errorf("version %q is not major.minor.patch", version)
	}
	var parts [3]int64
	for i := range parts {
		if parts[i], err = strconv.ParseInt(match[i+1], 10, 64); err != nil {
			return 0, 0, 0, err
		}
	}
	return parts[0], parts[1], parts[2], nil
}

// reclaimExecutor is one container that runs native clients, with its
// verified eligibility probe.
type reclaimExecutor struct {
	name      string
	container testcontainers.Container
	probe     ResetABIProbe
}

// probeReclaimExecutor runs the eligibility probe in a container, binds it to
// the container's actual image config ID and applies the ABI gate. A probe
// that fails or does not match is a blocker, never a reason to switch library.
func probeReclaimExecutor(t *testing.T, ctx context.Context, name string, c testcontainers.Container, major, minor, patch int64) reclaimExecutor {
	t.Helper()
	inspect, err := c.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect %s: %v", name, err)
	}
	out := ExecOutput(t, ctx, c, "python3", "-c", ReclaimABIProbe, inspect.Image)
	doc, err := ReclaimJSONLine(out)
	if err != nil {
		t.Fatalf("%s ABI probe: %v", name, err)
	}
	probe, err := ParseResetABIProbe(doc)
	if err != nil {
		t.Fatalf("%s ABI probe unreadable: %v", name, err)
	}
	if err := CheckResetABIProbe(probe, major, minor, patch); err != nil {
		t.Fatalf("%s is not eligible for the RESET recipe: %v", name, err)
	}
	t.Logf("RECLAIM_RESET_EXECUTOR %s image_config=%s library=%s library_sha256=%s extension_sha256=%s version=%d.%d.%d",
		name, probe.ImageConfigID, probe.LibraryPath, probe.LibrarySHA256, probe.ExtensionSHA256, probe.Major, probe.Minor, probe.Patch)
	return reclaimExecutor{name: name, container: c, probe: probe}
}

// reclaimObserver takes the timed observations of the RESET sequence: one
// session listing of the active MDS and one independent lock probe of the
// two held files, bracketed as a whole.
type reclaimObserver struct {
	m      *CephFSSingleMDS
	helper testcontainers.Container
	aPath  string
	uPath  string
}

func (o *reclaimObserver) observe(t *testing.T, ctx context.Context) ResetObservation {
	t.Helper()
	obs := ResetObservation{Start: time.Now()}
	data, err := o.m.Cluster.Ceph(ctx, "tell", "mds."+o.m.MDS.Name, "session", "ls", "--format", "json")
	if err != nil {
		t.Fatalf("session ls: %v", err)
	}
	if obs.Sessions, err = ParseResetSessions(data); err != nil {
		t.Fatalf("session ls: %v: %.512s", err, data)
	}
	out := ExecOutput(t, ctx, o.helper, "python3", "-c", ReclaimLockProbe, o.m.FS.FilesystemName, o.aPath, o.uPath)
	if obs.ALock, obs.ULock, err = ParseReclaimLock(out); err != nil {
		t.Fatalf("lock probe: %v", err)
	}
	obs.End = time.Now()
	return obs
}

// reclaimNonce returns a short random token that keeps the UUIDs of one run
// apart from any leftover of another.
func reclaimNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw)
}

// RunCephFSReclaimReset runs the native RESET recipe on one active MDS:
//
//  1. The release and the ABI of both executors are qualified.
//  2. A seed client fsyncs the payload. Client A (UUID, 600-second session
//     timeout) holds an exclusive flock on the seeded file; the unrelated
//     client U holds another file's lock. Independent probes see both held.
//  3. A's actor is SIGKILLed after its process ownership is verified. Its
//     session and lock must still stand, so ordinary timeout cannot stand in
//     for the RESET.
//  4. A fresh handle asks to RESET a UUID that no session carries and must get
//     -ENOENT, with A and U untouched.
//  5. A second fresh handle asks to reclaim A's UUID with flags 0, the
//     state-preserving request. The MDS must refuse it with -EINVAL, and A's
//     session and lock must still stand.
//  6. A third fresh handle RESETs A's UUID and must get 0. Before it mounts,
//     A's session is gone, A's lock is free and U's session and lock stand.
//  7. The same handle finishes the reclaim, takes over the former UUID,
//     mounts as a new client and reads the original bytes through a new
//     descriptor.
//
// JudgeResetSequence decides; the receipt is logged on every outcome.
func RunCephFSReclaimReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := StartCephFSSingleMDS(t, ctx)
	helper := ToolkitContainer(t, m.Cluster)
	major, minor, patch, err := ReclaimExpectedRelease(m.Version)
	if err != nil {
		t.Fatal(err)
	}
	executors := []reclaimExecutor{
		probeReclaimExecutor(t, ctx, "client", m.Client, major, minor, patch),
		probeReclaimExecutor(t, ctx, "toolkit", helper, major, minor, patch),
	}

	nonce := reclaimNonce(t)
	const aPath, uPath = "/reclaim-reset-a", "/reclaim-reset-u"
	payload := []byte("reclaim-acknowledged-data\x00\xff")
	seedSum := sha256.Sum256(payload)
	seq := ResetSequence{
		AUUID:      "reclaim-a-" + nonce,
		UUUID:      "reclaim-u-" + nonce,
		SeedSize:   len(payload),
		SeedSHA256: hex.EncodeToString(seedSum[:]),
	}
	missingTarget, missingOwn, preserveOwn, resetOwn := "reclaim-missing-"+nonce, "reclaim-m-"+nonce, "reclaim-p-"+nonce, "reclaim-b-"+nonce
	var outcome ResetOutcome
	var killedToReset time.Duration
	defer func() {
		// The receipt is emitted before the cluster is torn down, whatever the
		// outcome, so a failed run keeps the ledger.
		t.Logf("RECLAIM_RESET_RECEIPT mds=%s executors=[%s %s; %s %s] verdict=%d reason=%q a=client.%d u=client.%d b=client.%d kill_to_reset_observed=%s missing={%s} preserve={%s} reset={%s} observations(after_kill=%d after_missing=%d after_preserve=%d after_reset=%d); not observed: blocklist and OSD epoch effects, RESET call latency, any adopted capability or lock (the MDS refuses state-preserving reclaim)",
			m.Version, executors[0].name, executors[0].probe.ImageConfigID, executors[1].name, executors[1].probe.ImageConfigID,
			outcome.Verdict, outcome.Reason, seq.A, seq.U, seq.B, killedToReset, reclaimAttemptString(seq.Missing), reclaimAttemptString(seq.Preserve), reclaimAttemptString(seq.Reset),
			len(seq.AfterKill), len(seq.AfterMissing), len(seq.AfterPreserve), len(seq.AfterReset))
	}()

	seedOut := ExecOutput(t, ctx, helper, "python3", "-c", ReclaimSeed, m.FS.FilesystemName, hex.EncodeToString(payload), aPath, uPath)
	seedDoc, err := ReclaimJSONLine(seedOut)
	if err != nil {
		t.Fatal("seed:", err)
	}
	var seeded struct {
		Client uint64 `json:"client"`
		Size   int    `json:"size"`
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(seedDoc, &seeded); err != nil || seeded.Size != seq.SeedSize || seeded.SHA256 != seq.SeedSHA256 {
		t.Fatalf("seed wrote %s, want %d bytes with sha256 %s (%v)", seedDoc, seq.SeedSize, seq.SeedSHA256, err)
	}

	holderA := StartCephFSActor(t, ctx, m.Cluster, m.Client, ReclaimHolder, m.FS.FilesystemName, "/tmp/reclaim-holder-a", aPath, seq.AUUID)
	a, _ := reclaimRegisteredReceipt(t, ctx, holderA, 1)
	holderU := StartCephFSActor(t, ctx, m.Cluster, helper, ReclaimHolder, m.FS.FilesystemName, "/tmp/reclaim-holder-u", uPath, seq.UUUID)
	u, _ := reclaimRegisteredReceipt(t, ctx, holderU, 1)
	if a.Client == 0 || u.Client == 0 || a.Client == u.Client || a.UUID != seq.AUUID || u.UUID != seq.UUUID {
		t.Fatalf("holder identities: A %+v, U %+v", a, u)
	}
	seq.A, seq.U = a.Client, u.Client

	observer := &reclaimObserver{m: m, helper: helper, aPath: aPath, uPath: uPath}
	before := observer.observe(t, ctx)
	if before.Sessions.Status(seq.A) != ResetPresent || before.Sessions.Status(seq.U) != ResetPresent ||
		before.ALock != ResetLockHeld || before.ULock != ResetLockHeld {
		t.Fatalf("holders are not established before the kill: sessions %v, locks %d/%d", before.Sessions, before.ALock, before.ULock)
	}

	cmdline := ExecOutput(t, ctx, m.Client, "cat", "/proc/"+strconv.Itoa(a.PID)+"/cmdline")
	if !strings.Contains(cmdline, holderA.Token) {
		t.Fatal("holder A's PID does not belong to its actor")
	}
	ExecOutput(t, ctx, m.Client, "sh", "-c", `kill -9 "$1"`, "sh", strconv.Itoa(a.PID))
	select {
	case <-holderA.Done:
	case <-time.After(30 * time.Second):
		t.Fatal("holder A was not joined after SIGKILL")
	}
	exit := holderA.Result.ExitCode
	seq.AExitStatus = &exit
	seq.AKilledAt = time.Now()
	if holderA.Err != nil || holderA.Result.TimedOut {
		t.Fatalf("holder A outcome: %s", holderA.exited(ctx))
	}

	seq.AfterKill = append(seq.AfterKill, observer.observe(t, ctx), observer.observe(t, ctx))

	seq.Missing = runReclaimAttempt(t, ctx, m, helper, "missing-UUID", missingTarget, missingOwn, CephReclaimReset)
	seq.AfterMissing = append(seq.AfterMissing, observer.observe(t, ctx), observer.observe(t, ctx))
	seq.Preserve = runReclaimAttempt(t, ctx, m, helper, "state-preserving", seq.AUUID, preserveOwn, 0)
	seq.AfterPreserve = append(seq.AfterPreserve, observer.observe(t, ctx), observer.observe(t, ctx))

	resetStart := time.Now()
	reclaimer := StartCephFSActor(t, ctx, m.Cluster, helper, ReclaimReclaimer, m.FS.FilesystemName, "/tmp/reclaim-reset", seq.AUUID, resetOwn, aPath)
	attempt, resetSeen := reclaimRegisteredReceipt(t, ctx, reclaimer, 1)
	seq.Reset = ResetAttempt{Handle: attempt.Handle, Initialized: attempt.Initialized, PriorReclaimCalls: attempt.PriorReclaimCalls,
		Flags: attempt.Flags, Target: attempt.Target, OwnUUID: attempt.OwnUUID, RC: attempt.RC, Start: resetStart, End: resetSeen}
	seq.AfterReset = append(seq.AfterReset, observer.observe(t, ctx), observer.observe(t, ctx))
	killedToReset = time.Since(seq.AKilledAt)
	if killedToReset > 2*time.Minute {
		t.Fatalf("RESET was observed %s after the kill, too late to exclude the 600-second session timeout from attribution", killedToReset)
	}

	seq.MountStart = time.Now()
	reclaimer.Open(t, ctx, "mount")
	read, _ := reclaimRegisteredReceipt(t, ctx, reclaimer, 2)
	seq.B, seq.ReadByClientID, seq.ReadSize, seq.ReadSHA256, seq.ReadFromNewFd = read.Client, read.Client, read.Size, read.SHA256, read.NewFd

	status, statusErr := m.FS.MDSStatus(ctx)
	if statusErr != nil || len(status.Active) != 1 || status.Active[0].GID != m.MDS.GID {
		t.Fatalf("the active MDS changed during the recipe: %+v %v", status, statusErr)
	}

	outcome = JudgeResetSequence(seq)

	reclaimer.Open(t, ctx, "finish")
	if closed, _ := reclaimRegisteredReceipt(t, ctx, reclaimer, 3); !closed.Closed {
		t.Fatalf("replacement client did not close: %+v", closed)
	}
	reclaimer.Wait(t, ctx)
	holderU.Open(t, ctx, "finish")
	if closed, _ := reclaimRegisteredReceipt(t, ctx, holderU, 2); !closed.Closed {
		t.Fatalf("unrelated client did not close: %+v", closed)
	}
	holderU.Wait(t, ctx)

	switch outcome.Verdict {
	case ResetEstablished:
	case ResetContradicted:
		t.Fatalf("RESET contradicted: %s", outcome.Reason)
	default:
		t.Fatalf("RESET inconclusive: %s", outcome.Reason)
	}
}
