//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_sessions))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
)

// The holder opens a session, makes "durable" stable with fsync, leaves
// "-volatile" only in its cache and waits. After the test resumes it, the
// holder reports what its old session returns and whether a new one works.
const cephFSSessionHolder = `
import cephfs, json, os, sys, time
def mount():
    fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
    fs.conf_set("client_mount_timeout", "30")
    fs.mount(filesystem_name=sys.argv[1].encode())
    return fs
fs = mount()
fd = fs.open(b"/held", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
fs.write(fd, b"durable", 0)
fs.fsync(fd, 0)
fs.write(fd, b"-volatile", 7)
with open("/tmp/holder.id.tmp", "w") as out:
    out.write(str(fs.get_instance_id()))
os.rename("/tmp/holder.id.tmp", "/tmp/holder.id")
while not os.path.exists("/tmp/holder.go"):
    time.sleep(0.2)
result = {}
try:
    fs.write(fd, b"after", 0)
    fs.fsync(fd, 0)
    result["old_errno"] = 0
except cephfs.OSError as error:
    result["old_errno"] = error.errno
fresh = mount()
result["fresh_id"] = fresh.get_instance_id()
stat = fresh.stat(b"/held")
reopened = fresh.open(b"/held", os.O_RDONLY, 0)
result["fresh_content"] = fresh.read(reopened, 0, stat.st_size).decode()
fresh.close(reopened)
fresh.unmount()
with open("/tmp/holder.result.tmp", "w") as out:
    json.dump(result, out)
os.rename("/tmp/holder.result.tmp", "/tmp/holder.result")
`

// The reader needs the capabilities the frozen holder still owns.
const cephFSSessionReader = `
import cephfs, json, os, sys, time
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
start = time.monotonic()
stat = fs.stat(b"/held")
fd = fs.open(b"/held", os.O_RDONLY, 0)
content = fs.read(fd, 0, stat.st_size).decode()
print(json.dumps({"seconds": time.monotonic() - start, "content": content}))
fs.close(fd)
fs.unmount()
`

// A frozen client keeps its CephFS session and capabilities until the MDS
// gives up on it. Shortened session limits make the MDS evict and blocklist
// the client within a phase, another client then sees only fsynced data, and
// the resumed client must open a new session.
func TestCephFSPausedClientEviction(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterRun(t, cephfs.Run, image, opts...)
			filesystems := cephfs.Filesystems(cluster)
			if len(filesystems) != 1 {
				t.Fatalf("filesystems = %d, want 1", len(filesystems))
			}
			fs := filesystems[0]
			if err := fs.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			defaults := cephfs.SessionTimeouts{Timeout: time.Minute, Autoclose: 5 * time.Minute}
			if current, err := fs.SessionTimeouts(ctx); err != nil || current != defaults {
				t.Fatalf("default session timeouts = %+v, %v", current, err)
			}
			if change, err := fs.TemporarySessionTimeouts(ctx, cephfs.SessionTimeouts{Timeout: 29 * time.Second, Autoclose: time.Minute}); change != nil || err == nil {
				t.Fatal("timeout below the native minimum was accepted")
			}
			short := cephfs.SessionTimeouts{Timeout: 30 * time.Second, Autoclose: 30 * time.Second}
			change, err := fs.TemporarySessionTimeouts(ctx, short)
			if change != nil {
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					if err := change.Restore(cleanupCtx); err != nil {
						t.Errorf("restore session timeouts: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal("shorten session timeouts:", err)
			}
			if current, err := fs.SessionTimeouts(ctx); err != nil || current != short {
				t.Fatalf("shortened session timeouts = %+v, %v", current, err)
			}
			if again, err := fs.TemporarySessionTimeouts(ctx, defaults); again != nil || err == nil {
				t.Fatal("overlapping session timeouts override was admitted")
			}
			// An outside edit is refused instead of overwritten, and Restore
			// proceeds once the owned value is back.
			mustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "session_autoclose", "90")
			if err := change.Restore(ctx); err == nil || !strings.Contains(err.Error(), "outside this override") {
				t.Fatalf("restore over an outside edit: %v", err)
			}
			if current, err := fs.SessionTimeouts(ctx); err != nil || current.Autoclose != 90*time.Second {
				t.Fatalf("refused restore changed the outside edit: %+v, %v", current, err)
			}
			mustCeph(t, ctx, cluster, "fs", "set", fs.FilesystemName, "session_autoclose", "30")

			execOutput(t, ctx, client, "sh", "-c", `printf '%s' "$1" > /tmp/holder.py && nohup python3 /tmp/holder.py "$2" > /tmp/holder.log 2>&1 &`, "sh", cephFSSessionHolder, fs.FilesystemName)
			holderID := waitClientFile(t, ctx, client, "/tmp/holder.id", "/tmp/holder.log")
			holder := findCephFSSession(t, ctx, fs, holderID)
			if holder == nil {
				t.Fatalf("holder client.%s has no session", holderID)
			}
			if holder.State != "open" || holder.EntityID != "admin" || holder.Root != "/" || holder.Caps == 0 || holder.Address == "" {
				t.Fatalf("holder session = %+v", *holder)
			}
			t.Logf("CEPHFS_SESSION holder %+v", *holder)

			pause, err := cluster.PauseContainer(ctx, client)
			if pause != nil {
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					if err := pause.Resume(cleanupCtx); err != nil {
						t.Errorf("resume holder: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal("pause holder:", err)
			}
			paused := time.Now()
			type readResult struct {
				Seconds float64 `json:"seconds"`
				Content string  `json:"content"`
			}
			reads := make(chan readResult, 1)
			readErrs := make(chan string, 1)
			go func() {
				// The control container runs the second client, so the
				// frozen holder cannot block it at the container level.
				code, out, err := sessionExec(ctx, cluster.ControlContainer(), "python3", "-c", cephFSSessionReader, fs.FilesystemName)
				var result readResult
				if err != nil || code != 0 || json.Unmarshal([]byte(lastLine(out)), &result) != nil {
					readErrs <- strconv.Itoa(code) + " " + out
					return
				}
				reads <- result
			}()

			for findCephFSSession(t, ctx, fs, holderID) != nil {
				if time.Since(paused) > 3*time.Minute {
					t.Fatal("the MDS kept the frozen holder's session")
				}
				time.Sleep(2 * time.Second)
			}
			evicted := time.Since(paused)
			t.Logf("CEPHFS_SESSION evicted after %s", evicted.Round(time.Second))
			if evicted < 15*time.Second {
				t.Fatalf("session closed %s after the pause, before the 30s autoclose could expire", evicted)
			}
			var read readResult
			select {
			case read = <-reads:
			case out := <-readErrs:
				t.Fatal("second client read failed:", out)
			case <-ctx.Done():
				t.Fatal("second client did not finish")
			}
			t.Logf("CEPHFS_SESSION reader waited %.1fs and read %q", read.Seconds, read.Content)
			if read.Content != "durable" {
				t.Fatalf("second client read %q, want only the fsynced data", read.Content)
			}
			if read.Seconds < 10 {
				t.Fatalf("second client finished in %.1fs without waiting for the holder's capabilities", read.Seconds)
			}
			entries, err := cluster.BlocklistEntries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == holder.Address }) {
				t.Fatalf("evicted holder %s is not blocklisted: %+v", holder.Address, entries)
			}

			if err := pause.Resume(ctx); err != nil {
				t.Fatal("resume holder:", err)
			}
			execOutput(t, ctx, client, "touch", "/tmp/holder.go")
			var outcome struct {
				OldErrno     int    `json:"old_errno"`
				FreshID      uint64 `json:"fresh_id"`
				FreshContent string `json:"fresh_content"`
			}
			if err := json.Unmarshal([]byte(waitClientFile(t, ctx, client, "/tmp/holder.result", "/tmp/holder.log")), &outcome); err != nil {
				t.Fatal(err)
			}
			t.Logf("CEPHFS_SESSION resumed holder %+v", outcome)
			// ESHUTDOWN: the blocklisted instance cannot send again.
			if outcome.OldErrno != 108 {
				t.Fatalf("old session errno = %d, want 108; log: %s", outcome.OldErrno, execOutput(t, ctx, client, "cat", "/tmp/holder.log"))
			}
			if strconv.FormatUint(outcome.FreshID, 10) == holderID || outcome.FreshContent != "durable" {
				t.Fatalf("new session = %+v", outcome)
			}

			if err := change.Restore(ctx); err != nil {
				t.Fatal("restore session timeouts:", err)
			}
			if current, err := fs.SessionTimeouts(ctx); err != nil || current != defaults {
				t.Fatalf("restored session timeouts = %+v, %v", current, err)
			}
		})
	}
}

func findCephFSSession(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, id string) *cephfs.Session {
	t.Helper()
	sessions, err := fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if strconv.FormatUint(session.ID, 10) == id {
			return &session
		}
	}
	return nil
}

func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return lines[len(lines)-1]
}
