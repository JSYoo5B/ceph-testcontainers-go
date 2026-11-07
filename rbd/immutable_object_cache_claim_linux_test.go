package rbd

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCacheInterruptAfterClearingDaemonDirectoryWithoutBirthTime(t *testing.T) {
	f := newCacheShellFixture(t)
	f.program(t, "stat", `case "$2" in
    %w) command -p stat -c '-' "$3" ;;
    %d:%i:%w) command -p stat -c '%d:%i:-' "$3" ;;
    *) command -p stat "$@" ;;
esac`)
	f.program(t, "ceph-immutable-object-cache", `for arg do
    case "$arg" in --immutable_object_cache_path=*) cache=${arg#*=} ;; esac
done
find "$cache" -mindepth 1 -delete || exit 1
printf daemon-data > "$cache/object" || exit 1
kill -KILL "$PPID"`)
	f.reserve(t)
	if err := f.start(t).Run(); err == nil {
		t.Fatal("expected the launcher to be killed during native initialization")
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	object := filepath.Join(f.cache, ".ceph-immutable-cache", "objects", "object")
	if b, err := os.ReadFile(object); err != nil || string(b) != "daemon-data" {
		t.Fatalf("native cache file: %q, %v", b, err)
	}
	if err := os.Rename(f.cache, f.cache+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.cache, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(f.cache, "foreign")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=0", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "keep" {
		t.Fatalf("replacement sentinel %q: %v", b, err)
	}
	if err := os.Rename(f.cache, f.cache+".foreign"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.cache+".original", f.cache); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
	f.assertReleased(t)
}

func TestCacheDisplacedPrivateReservationPreservesOutsideState(t *testing.T) {
	f := newCacheShellFixture(t)
	f.reserve(t)
	own, err := net.Listen("unix", filepath.Join(f.root, "bound"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = own.Close() })
	pid, _ := f.process(t, f.privateSocket())
	f.env = append(f.env, "LIVE_PID="+strconv.Itoa(pid), "BOUND="+own.Addr().String())
	f.program(t, "ceph-immutable-object-cache", `for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) ln "$BOUND" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$LIVE_PID" > "${arg#*=}" || exit 1 ;;
    esac
done`)
	out, err := f.start(t).Output()
	if err != nil || !strings.Contains(string(out), "ready=1") {
		t.Fatalf("start %q: %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	state := f.pidfile + ".lease"
	stage := filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage")
	if err := os.Rename(state, state+".original"); err != nil {
		t.Fatal(err)
	}
	checkRefusal := func() {
		t.Helper()
		f.check(t, "stopped=0", "stop", "0", f.sock, f.pidfile)
		f.check(t, "removed=0", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
		if _, err := os.Stat(f.sock); err != nil {
			t.Fatalf("altered published socket: %v", err)
		}
	}
	checkRefusal()
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	checkRefusal()
	sentinel := filepath.Join(state, "owner")
	if err := os.WriteFile(sentinel, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	checkRefusal()
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "foreign" {
		t.Fatalf("private replacement %q: %v", data, err)
	}
	if err := os.Rename(state, state+".foreign"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(state+".original", state); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1") // first data acknowledgement lost
	if err := os.Rename(state, state+".original"); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=0", "release-reservation", "", "", state, stage)
	if err := os.Rename(state+".original", state); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "release-reservation", "", "", state, stage)
	// A new occupant at the retired name must survive both a release replay
	// and the final removal of the original independent witness.
	if err := os.Rename(state+".foreign", state); err != nil {
		t.Fatal(err)
	}
	f.release(t, stage)
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "foreign" {
		t.Fatalf("release replay changed %q: %v", data, err)
	}
}

func TestCachePathClaimArbitratesEquivalentEmptyPaths(t *testing.T) {
	first := newCacheShellFixture(t)
	if err := os.Mkdir(first.cache, 0700); err != nil {
		t.Fatal(err)
	}
	second := newCacheShellFixture(t)
	alias := filepath.Join(second.root, "alias")
	if err := os.Symlink(filepath.Dir(first.cache), alias); err != nil {
		t.Fatal(err)
	}
	second.cache = filepath.Join(alias, filepath.Base(first.cache))
	a, b := first.claimPath(t), second.claimPath(t)
	if a != b {
		t.Fatalf("two names for the same cache produced %s and %s", a, b)
	}
	t.Cleanup(func() {
		target, _ := os.Readlink(a)
		if target == "sh:"+first.pidfile+".lease" || target == "sh:"+second.pidfile+".lease" {
			_ = os.Remove(a)
		}
	})
	var wg sync.WaitGroup
	start := make(chan struct{})
	result := make(chan cacheShellFixture, 2)
	for _, f := range []cacheShellFixture{first, second} {
		wg.Add(1)
		go func(f cacheShellFixture) {
			defer wg.Done()
			<-start
			if err := f.command(t, "claim", a, f.pidfile+".lease").Run(); err == nil {
				result <- f
			}
		}(f)
	}
	close(start)
	wg.Wait()
	close(result)
	if len(result) != 1 {
		t.Fatalf("obtained %d claims", len(result))
	}
	winner := <-result

	out, err := second.command(t, "inspect", second.cache, second.sock).Output()
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
	if err != nil || inspection.path != "claimed" {
		t.Fatalf("claimed path inspection %+v: %v", inspection, err)
	}
	winner.check(t, "removed=1", "unclaim", a, winner.pidfile+".lease")
}

func TestCacheInspectNonDirectoryAncestor(t *testing.T) {
	f := newCacheShellFixture(t)
	ancestor := filepath.Join(f.root, "file")
	if err := os.WriteFile(ancestor, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := f.command(t, "inspect", filepath.Join(ancestor, "cache"), f.sock).Output()
	if err != nil {
		t.Fatal(err)
	}
	state, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
	if err != nil || state.path != "invalid-parent" {
		t.Fatalf("ancestor inspected as %+v: %v", state, err)
	}
	out, err = f.command(t, "inspect", f.cache, filepath.Join(ancestor, "sock")).Output()
	if err != nil {
		t.Fatal(err)
	}
	state, err = parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
	if err != nil || !state.invalidSocketParent || state.path != "none" {
		t.Fatalf("socket ancestor inspected as %+v: %v", state, err)
	}
	f.program(t, "python3", "exit 127")
	out, err = f.command(t, "inspect", f.cache, f.sock).Output()
	if err != nil {
		t.Fatal(err)
	}
	state, err = parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
	if err != nil || state.rename {
		t.Fatalf("missing syscall helper: %+v, %v", state, err)
	}
	if _, err = os.Stat(f.pidfile + ".lease"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect mutated the filesystem: %v", err)
	}
}

func TestCacheClaimStableAcrossMissingParentCreation(t *testing.T) {
	f := newCacheShellFixture(t)
	f.cache = filepath.Join(f.root, "new", "parent", "cache")
	before := f.claimPath(t)
	f.reserve(t) // reserving a sibling stage creates the missing parents
	if after := f.claimPath(t); after != before {
		t.Fatalf("creating ancestors changed claim %s to %s", before, after)
	}
	f.program(t, "ceph-immutable-object-cache", "exit 1")
	out, err := f.start(t).Output()
	if err != nil || !strings.Contains(string(out), "exit=1") || strings.Contains(string(out), "conflict=") {
		t.Fatalf("self-conflict after ancestor creation: %q, %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(filepath.Dir(f.cache), "."+filepath.Base(f.pidfile)+".stage"))
}

func TestCacheInterruptedReservationDoesNotAdoptForeignSeed(t *testing.T) {
	f := newCacheShellFixture(t)
	dir := f.pidfile + ".lease"
	f.program(t, "stat", `case "$3" in *.pending-sh) kill -KILL "$PPID"; exit 1 ;; esac
command -p stat "$@"`)
	if err := f.command(t, "reserve", dir).Run(); err == nil {
		t.Fatal("expected reserve to die before recording its directory identity")
	}
	seed := dir + ".pending-sh"
	if err := os.WriteFile(filepath.Join(seed, "foreign"), []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=0", "release-reservation", dir, "", dir)
	if got, err := os.ReadFile(filepath.Join(seed, "foreign")); err != nil || string(got) != "foreign" {
		t.Fatalf("unverified seed: %q, %v", got, err)
	}
	if err := os.Rename(seed, seed+".unverified"); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "release-reservation", dir, "", dir)
}

func TestCacheReservationNoReplaceRequiresOwnershipReadback(t *testing.T) {
	f := newCacheShellFixture(t)
	dir := f.pidfile + ".lease"
	f.program(t, "mv", `case "$3" in *.lease) mkdir "$3" && echo foreign > "$3/foreign" || exit 1; exit 0 ;; esac
command -p mv "$@"`)
	f.check(t, "", "reserve", dir) // Model coreutils versions that report a skipped -nT rename as success.
	f.check(t, "reserved=0", "confirm-reservation", dir)
	f.check(t, "removed=1", "release-reservation", "", "", dir)
	f.check(t, "removed=1", "forget-reservation", dir)
	if got, err := os.ReadFile(filepath.Join(dir, "foreign")); err != nil || string(got) != "foreign\n" {
		t.Fatalf("replacement %q: %v", got, err)
	}
}

func TestCacheMarkerConstructionInterruptionPreservesExistingRoot(t *testing.T) {
	for _, point := range []string{"marker", "objects"} {
		t.Run(point, func(t *testing.T) {
			f := newCacheShellFixture(t)
			if err := os.Mkdir(f.cache, 0700); err != nil {
				t.Fatal(err)
			}
			out, err := f.command(t, "inspect", f.cache, f.sock).Output()
			if err != nil {
				t.Fatal(err)
			}
			info, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
			if err != nil {
				t.Fatal(err)
			}
			claim, stage := info.claim, filepath.Join(f.cache, "."+filepath.Base(f.pidfile)+".stage")
			f.check(t, "", "claim", claim, f.pidfile+".lease")
			t.Cleanup(func() {
				if target, _ := os.Readlink(claim); target == "sh:"+f.pidfile+".lease" {
					_ = os.Remove(claim)
				}
			})
			f.check(t, "", "reserve", f.pidfile+".lease")
			f.check(t, "", "reserve", stage, info.identity)
			f.program(t, "mkdir", `command -p mkdir "$@" || exit 1
case "$1" in *.stage/marker`+map[string]string{"marker": "", "objects": "/objects"}[point]+`) kill -KILL "$PPID" ;; esac`)
			if err := f.command(t, "start", "admin", f.cache, f.sock, "1024", f.asok, f.pidfile, "empty", info.identity, claim).Run(); err == nil {
				t.Fatal("expected interruption building the private marker tree")
			}
			f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
			f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "0")
			f.release(t, stage)
			entries, err := os.ReadDir(f.cache)
			if err != nil || len(entries) != 0 {
				t.Fatalf("initially empty cache after interruption: %v, %v", entries, err)
			}
		})
	}
}

// Simulate a lost reply after the shell checked the claim but before its
// child removed it. Even killing the parent shell leaves the kernel lock
// held by the actual unlinker, fencing a successor's atomic claim.
func TestCacheUnclaimFencesDelayedChild(t *testing.T) {
	f := newCacheShellFixture(t)
	claim := f.claimPath(t)
	owner := f.pidfile + ".lease"
	f.check(t, "", "claim", claim, owner)
	t.Cleanup(func() {
		if s, _ := os.Readlink(claim); s == "sh:"+owner || s == "sh:successor" {
			_ = os.Remove(claim)
		}
	})
	f.program(t, "rm", `echo x >&3; read -r go <&4 || exit 1
exec /bin/rm "$@"`)
	pipe := func() (*os.File, *os.File) {
		t.Helper()
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
		return r, w
	}
	ready, signal := pipe()
	gate, release := pipe()
	old := f.command(t, "unclaim", claim, owner)
	old.ExtraFiles = []*os.File{signal, gate}
	if err := old.Start(); err != nil {
		t.Fatal(err)
	}
	if err := signal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(ready, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := old.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := old.Wait(); err == nil {
		t.Fatal("old shell was not interrupted")
	}
	if err := exec.CommandContext(t.Context(), "flock", "-n", "/tmp", "true").Run(); err == nil {
		t.Fatal("unlinker released the container lock before removing the claim")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	blocked := exec.CommandContext(ctx, "sh", "-c", immutableObjectCacheScript, "sh", "claim", claim, "successor")
	if err := blocked.Run(); err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("successor completed before the pending unlink: %v, %v", err, ctx.Err())
	}
	if _, err := release.WriteString("go\n"); err != nil {
		t.Fatal(err)
	}
	f.check(t, "", "claim", claim, "successor")
	if target, err := os.Readlink(claim); err != nil || target != "sh:successor" {
		t.Fatalf("successor claim %q, %v", target, err)
	}
	// Use the real rm for the successor's ordinary release.
	if err := os.Remove(filepath.Join(f.root, "rm")); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "unclaim", claim, "successor")
}
