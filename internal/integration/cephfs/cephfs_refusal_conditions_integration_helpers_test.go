//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSRefusalProbe is a client that plays the SUT once: it opens a new
// session and prints one JSON line with its client ID and the errno of the
// mount (0 when it succeeded). In "size" mode it then creates the file
// /limits/<name> and reports the errno of each operation against the limit
// in argv[3] and the final file size; in "read" mode it reads the file at
// argv[4]. Arguments: filesystem, mode, limit, path.
const cephFSRefusalProbe = `
import cephfs, json, os, sys
name, mode, limit, path = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4].encode()
def errno_of(error):
    code = getattr(error, "errno", None) or (error.args[0] if error.args and isinstance(error.args[0], int) else -1)
    return abs(code)
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.init()
try:
    fs.mount(filesystem_name=name.encode())
except cephfs.Error as error:
    client = fs.get_instance_id()
    fs.shutdown()
    print(json.dumps({"client": client, "errno": errno_of(error), "error": str(error)[:200]}))
    sys.exit(0)
result = {"client": fs.get_instance_id(), "errno": 0}
if mode == "size":
    ops = {}
    def attempt(op, call):
        try:
            call()
            ops[op] = 0
        except cephfs.Error as error:
            ops[op] = errno_of(error)
    fs.mkdirs(b"/limits", 0o755)
    fd = fs.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    attempt("small_write", lambda: fs.write(fd, b"s" * 4096, 0))
    attempt("boundary_write", lambda: fs.write(fd, b"b", limit - 1))
    attempt("extend_write", lambda: fs.write(fd, b"e", limit))
    attempt("extend_truncate", lambda: fs.ftruncate(fd, limit + 1))
    attempt("shrink_truncate", lambda: fs.ftruncate(fd, 1000))
    fs.fsync(fd, 0)
    result.update(ops=ops, size=fs.fstat(fd).st_size)
    fs.close(fd)
elif mode == "read":
    fd = fs.open(path, os.O_RDONLY, 0)
    result["read"] = fs.read(fd, 0, 4096).decode()
    fs.close(fd)
fs.unmount()
fs.shutdown()
print(json.dumps(result))
`

// cephFSRefusalHolder keeps one session open across the refusal. It mounts
// and publishes receipt 1 with its client ID. At the "write" gate it writes
// and fsyncs the file in argv[4] through that session and publishes receipt
// 2 with the errno (0 on success) and the file size. At the "finish" gate it
// unmounts and publishes receipt 3.
const cephFSRefusalHolder harness.CephFSActorScript = harness.CephFSActorHelpers + `
path = sys.argv[4].encode()
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
publish(1, client=fs.get_instance_id())
gate("write")
try:
    fs.mkdirs(b"/refusal", 0o755)
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    fs.write(fd, b"existing-session", 0)
    fs.fsync(fd, 0)
    size = fs.fstat(fd).st_size
    fs.close(fd)
    publish(2, client=fs.get_instance_id(), errno=0, size=size)
except cephfs.Error as error:
    publish(2, client=fs.get_instance_id(), errno=abs(getattr(error, "errno", -1) or -1), error=str(error)[:200])
gate("finish")
fs.unmount()
fs.shutdown()
publish(3, closed=True)
`

// cephFSRefusalOutcome is the line a cephFSRefusalProbe prints.
type cephFSRefusalOutcome struct {
	Client uint64         `json:"client"`
	Errno  int            `json:"errno"`
	Error  string         `json:"error"`
	Ops    map[string]int `json:"ops"`
	Size   int64          `json:"size"`
	Read   string         `json:"read"`
	// Log is the client's other output, such as libcephfs log lines.
	Log string `json:"-"`
}

// cephFSFilesystemSettings is what fs get reports for both conditions.
type cephFSFilesystemSettings struct {
	ID          int64
	MaxFileSize int64
	Refused     bool
}

func readCephFSFilesystemSettings(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) cephFSFilesystemSettings {
	t.Helper()
	var native struct {
		ID     int64 `json:"id"`
		MDSMap struct {
			MaxFileSize int64 `json:"max_file_size"`
			Flags       struct {
				Refused *bool `json:"refuse_client_session"`
			} `json:"flags_state"`
		} `json:"mdsmap"`
	}
	data := harness.MustCeph(t, ctx, cluster, "fs", "get", name, "--format", "json")
	if err := json.Unmarshal(data, &native); err != nil || native.MDSMap.Flags.Refused == nil || native.MDSMap.MaxFileSize == 0 {
		t.Fatalf("fs get %s: %v: %.512s", name, err, data)
	}
	return cephFSFilesystemSettings{ID: native.ID, MaxFileSize: native.MDSMap.MaxFileSize, Refused: *native.MDSMap.Flags.Refused}
}

func runCephFSRefusalProbe(t *testing.T, ctx context.Context, client testcontainers.Container, fs *cephfs.Filesystem, mode string, limit int64, path string) cephFSRefusalOutcome {
	t.Helper()
	code, out, err := harness.SessionExec(ctx, client, "python3", "-c", cephFSRefusalProbe, fs.FilesystemName, mode, strconv.FormatInt(limit, 10), path)
	// libcephfs logs to stderr, which may follow the result line.
	var outcome cephFSRefusalOutcome
	decoded := false
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "{") {
			decoded = json.Unmarshal([]byte(line), &outcome) == nil
		} else {
			outcome.Log += line + "\n"
		}
	}
	if err != nil || code != 0 || !decoded {
		t.Fatalf("probe %s: code=%d err=%v out=%s", mode, code, err, out)
	}
	t.Logf("CEPHFS_REFUSAL probe %s %+v", mode, outcome)
	return outcome
}

// cephFSSessionIDs lists the sorted client IDs of fs's sessions.
func cephFSSessionIDs(t *testing.T, ctx context.Context, fs *cephfs.Filesystem) []uint64 {
	t.Helper()
	sessions, err := cephfssession.List(ctx, fs)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uint64, 0, len(sessions))
	for _, session := range sessions {
		ids = append(ids, session.ID)
	}
	slices.Sort(ids)
	return ids
}

func restoreInCleanup(t *testing.T, name string, restore func(context.Context) error) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := restore(ctx); err != nil {
			t.Errorf("restore %s: %v", name, err)
		}
	})
}

// testCephFSRefusalConditions runs both conditions on one filesystem, one
// after the other; each restores its own change before the next starts.
func testCephFSRefusalConditions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, opts...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	original := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName)
	if original.Refused || original.MaxFileSize <= cephfs.MinMaxFileSize {
		t.Fatalf("unexpected default settings: %+v", original)
	}
	t.Logf("CEPHFS_REFUSAL original %+v", original)
	t.Run("max_file_size", func(t *testing.T) { testCephFSMaxFileSize(t, ctx, cluster, client, fs, original) })
	t.Run("refuse_client_session", func(t *testing.T) { testCephFSNewSessionRefusal(t, ctx, cluster, client, fs, original) })
}

// A write that ends past the limit and a truncate past it fail with EFBIG,
// while a small write, a write ending exactly at the limit and a shrinking
// truncate succeed. After Restore the same operations all succeed.
func testCephFSMaxFileSize(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, fs *cephfs.Filesystem, original cephFSFilesystemSettings) {
	if change, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize-1); change != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("limit below the native minimum: handle=%v error=%v", change, err)
	}
	change, err := fs.OverrideMaxFileSize(ctx, cephfs.MinMaxFileSize)
	if change != nil {
		restoreInCleanup(t, "max_file_size", change.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}
	limited := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName)
	if limited.ID != original.ID || limited.MaxFileSize != cephfs.MinMaxFileSize || limited.Refused {
		t.Fatalf("limited settings = %+v, original %+v", limited, original)
	}
	if again, err := fs.OverrideMaxFileSize(ctx, 1<<20); again != nil || err == nil {
		t.Fatal("an overlapping max_file_size override was admitted")
	}
	denied := runCephFSRefusalProbe(t, ctx, client, fs, "size", cephfs.MinMaxFileSize, "/limits/limited")
	const efbig = 27
	want := map[string]int{"small_write": 0, "boundary_write": 0, "extend_write": efbig, "extend_truncate": efbig, "shrink_truncate": 0}
	if denied.Errno != 0 || !maps.Equal(denied.Ops, want) || denied.Size != 1000 {
		t.Fatalf("limited operations = %+v, want %v and size 1000", denied, want)
	}

	// An outside edit is refused instead of overwritten.
	harness.MustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "max_file_size", strconv.Itoa(2*cephfs.MinMaxFileSize))
	if err := change.Restore(ctx); err == nil || !strings.Contains(err.Error(), "outside this override") {
		t.Fatalf("restore over an outside edit: %v", err)
	}
	if edited := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName); edited.MaxFileSize != 2*cephfs.MinMaxFileSize {
		t.Fatalf("refused restore changed the outside edit: %+v", edited)
	}
	harness.MustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "max_file_size", strconv.Itoa(cephfs.MinMaxFileSize))

	if err := change.Restore(ctx); err != nil {
		t.Fatal("restore max_file_size:", err)
	}
	if restored := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName); restored != original {
		t.Fatalf("restored settings = %+v, want %+v", restored, original)
	}
	allowed := runCephFSRefusalProbe(t, ctx, client, fs, "size", cephfs.MinMaxFileSize, "/limits/restored")
	for op, errno := range allowed.Ops {
		if errno != 0 {
			t.Fatalf("restored filesystem still refuses %s: %+v", op, allowed)
		}
	}
	if allowed.Errno != 0 || len(allowed.Ops) != len(want) || allowed.Size != 1000 {
		t.Fatalf("restored operations = %+v", allowed)
	}
}

// A session opened before the refusal keeps working while a new mount is
// rejected with EPERM and never appears in the MDS session list. After
// Restore a new session mounts and reads what the old session wrote.
func testCephFSNewSessionRefusal(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, fs *cephfs.Filesystem, original cephFSFilesystemSettings) {
	const path = "/refusal/existing"
	holder := startCephFSActor(t, ctx, cluster, client, cephFSRefusalHolder, fs.FilesystemName, "/tmp/refusal-holder", path)
	opened := holder.Receipt(t, ctx, 1)
	if opened.Client == 0 || findCephFSSession(t, ctx, fs, strconv.FormatUint(opened.Client, 10)) == nil {
		t.Fatalf("holder client.%d has no session", opened.Client)
	}

	// A filesystem that already refuses new sessions gives no change to own.
	harness.MustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "refuse_client_session", "true")
	if change, err := cephfssession.OverrideRefusal(ctx, fs); change != nil || err == nil {
		t.Fatalf("refusal over a native refusal: handle=%v error=%v", change, err)
	}
	harness.MustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "refuse_client_session", "false")

	change, err := cephfssession.OverrideRefusal(ctx, fs)
	if change != nil {
		restoreInCleanup(t, "refuse_client_session", change.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}
	refused := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName)
	if refused.ID != original.ID || !refused.Refused || refused.MaxFileSize != original.MaxFileSize {
		t.Fatalf("refusing settings = %+v, original %+v", refused, original)
	}
	if again, err := cephfssession.OverrideRefusal(ctx, fs); again != nil || err == nil {
		t.Fatal("an overlapping refusal was admitted")
	}

	// The MDS itself rejects the new client, so the EPERM is not an
	// authorization failure, and no session is added.
	const eperm = 1
	before := cephFSSessionIDs(t, ctx, fs)
	rejected := runCephFSRefusalProbe(t, ctx, client, fs, "mount", 0, "-")
	if rejected.Errno != eperm || !strings.Contains(rejected.Log, "new sessions are not permitted") {
		t.Fatalf("new session during the refusal = %+v, want EPERM from the MDS rejection", rejected)
	}
	if after := cephFSSessionIDs(t, ctx, fs); !slices.Equal(after, before) || !slices.Contains(after, opened.Client) {
		t.Fatalf("sessions after the rejected mount = %v, before %v", after, before)
	}
	holder.Open(t, ctx, "write")
	wrote := holder.Receipt(t, ctx, 2)
	if wrote.Errno != 0 || wrote.Client != opened.Client || wrote.Size != int64(len("existing-session")) {
		t.Fatalf("existing session during the refusal = %+v", wrote)
	}
	if session := findCephFSSession(t, ctx, fs, strconv.FormatUint(opened.Client, 10)); session == nil || session.State != "open" {
		t.Fatalf("existing session after its write = %+v", session)
	}

	if err := change.Restore(ctx); err != nil {
		t.Fatal("restore refuse_client_session:", err)
	}
	if restored := readCephFSFilesystemSettings(t, ctx, cluster, fs.FilesystemName); restored != original {
		t.Fatalf("restored settings = %+v, want %+v", restored, original)
	}
	admitted := runCephFSRefusalProbe(t, ctx, client, fs, "read", 0, path)
	if admitted.Errno != 0 || admitted.Client == 0 || admitted.Client == opened.Client || admitted.Read != "existing-session" {
		t.Fatalf("new session after Restore = %+v", admitted)
	}
	holder.Open(t, ctx, "finish")
	if closed := holder.Receipt(t, ctx, 3); !closed.Closed {
		t.Fatalf("holder did not close: %+v", closed)
	}
	holder.Wait(t, ctx)
	testCephFSReconnectRefusal(t, ctx, cluster, client, fs)
}

// A session that must reconnect to a restarted MDS during the refusal counts
// as a new one: the MDS drops it and the client is blocklisted, so its I/O
// does not come back after Restore. A new session after Restore works.
func testCephFSReconnectRefusal(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, fs *cephfs.Filesystem) {
	// This holder never passes its "write" gate: its I/O would block for good.
	holder := startCephFSActor(t, ctx, cluster, client, cephFSRefusalHolder, fs.FilesystemName, "/tmp/refusal-reconnect", "/refusal/reconnect")
	opened := holder.Receipt(t, ctx, 1)
	session := findCephFSSession(t, ctx, fs, strconv.FormatUint(opened.Client, 10))
	if session == nil {
		t.Fatalf("reconnecting holder client.%d has no session", opened.Client)
	}
	before, err := fs.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 {
		t.Fatalf("MDS status before the failover = %+v, %v", before, err)
	}
	change, err := cephfssession.OverrideRefusal(ctx, fs)
	if change != nil {
		restoreInCleanup(t, "refuse_client_session for the reconnect", change.Restore)
	}
	if err != nil {
		t.Fatal(err)
	}
	harness.MustCeph(t, ctx, cluster, "mds", "fail", fs.FilesystemName+":0")
	deadline := time.Now().Add(3 * time.Minute)
	for {
		status, err := fs.MDSStatus(ctx)
		if err == nil && len(status.Active) == 1 && status.Active[0].GID != before.Active[0].GID && status.Active[0].State == "up:active" &&
			findCephFSSession(t, ctx, fs, strconv.FormatUint(opened.Client, 10)) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rank 0 did not come back without the reconnecting session: %+v, %v", status, err)
		}
		time.Sleep(2 * time.Second)
	}
	entries, err := harness.ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == session.Address }) {
		t.Fatalf("refused reconnecting client %s is not blocklisted: %+v", session.Address, entries)
	}
	if err := change.Restore(ctx); err != nil {
		t.Fatal("restore refuse_client_session after the failover:", err)
	}
	if after := runCephFSRefusalProbe(t, ctx, client, fs, "mount", 0, "-"); after.Errno != 0 || after.Client == 0 {
		t.Fatalf("new session after the failover and Restore = %+v", after)
	}
}
