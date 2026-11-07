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
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type cacheShellFixture struct {
	root, cache, sock, asok, pidfile string
	env                              []string
}

func newCacheShellFixture(t *testing.T) cacheShellFixture {
	t.Helper()
	root := t.TempDir()
	return cacheShellFixture{root: root, cache: filepath.Join(root, "data"), sock: filepath.Join(root, "sock"),
		asok: filepath.Join(root, "admin"), pidfile: filepath.Join(root, "pid"), env: append(os.Environ(), "PATH="+root+":"+os.Getenv("PATH"))}
}

func (f cacheShellFixture) program(t *testing.T, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.root, name), []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
}

func (f cacheShellFixture) command(t *testing.T, op string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "sh", append([]string{"-c", immutableObjectCacheScript, "sh", op}, args...)...)
	cmd.Env = f.env
	return cmd
}

func (f cacheShellFixture) check(t *testing.T, want, op string, args ...string) {
	t.Helper()
	out, err := f.command(t, op, args...).Output()
	if err != nil || strings.TrimSpace(string(out)) != want {
		t.Fatalf("%s: %q, %v; want %q", op, out, err, want)
	}
}

func (f cacheShellFixture) start(t *testing.T) *exec.Cmd {
	t.Helper()
	return f.command(t, "start", "admin", f.cache, f.sock, "1024", f.asok, f.pidfile, "none", "")
}

func TestCacheStartNeverDispatched(t *testing.T) {
	f := newCacheShellFixture(t)
	f.check(t, "", "reserve", f.pidfile)
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	if err := f.start(t).Run(); err == nil {
		t.Fatal("late launch accepted a revoked reservation")
	}
	entries, err := os.ReadDir(f.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("late launch left files: %v, %v", entries, err)
	}
}

func TestCacheDaemonizedParentSuccessWithoutDaemon(t *testing.T) {
	f := newCacheShellFixture(t)
	f.program(t, "ceph-immutable-object-cache", "exit 0")
	f.program(t, "sleep", "exit 0") // skip only the readiness delay
	f.check(t, "", "reserve", f.pidfile)
	out, err := f.start(t).Output()
	if err != nil || strings.TrimSpace(string(out)) != "exit=0\npid=\nready=0" {
		t.Fatalf("launcher: %q, %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	for _, name := range []string{f.cache, f.pidfile + ".lease"} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			t.Fatalf("cleanup left %s: %v", name, err)
		}
	}
}

func TestCacheFinishedLaunchWithMissingEvidence(t *testing.T) {
	for _, failure := range []string{"pin", "replaced", "deleted", "marker", "deleted-after-start"} {
		t.Run(failure, func(t *testing.T) {
			f := newCacheShellFixture(t)
			listener, err := net.Listen("unix", filepath.Join(f.root, "original"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			// Keep the owned process waitable by the test after the launcher exits.
			// Its argv carries the exact same unique PID option as the native child.
			process := func(socket string) (int, <-chan struct{}) {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "sh", "-c", "while :; do sleep 0.02; done", "daemon",
					"--pid-file="+f.pidfile, "--immutable_object_cache_sock="+socket)
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() { _ = cmd.Wait(); close(done) }()
				t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
				return cmd.Process.Pid, done
			}
			pid, done := process(f.sock)
			_, unrelated := process(f.sock + ".foreign")
			if failure == "pin" {
				process(f.sock) // a second matching process requires another Stop
			}
			f.env = append(f.env, "CACHE_ROOT="+f.root, "LIVE_PID="+strconv.Itoa(pid), "FAULT="+failure)
			f.program(t, "ceph-immutable-object-cache", `
command -p ln "$CACHE_ROOT/original" "$CACHE_ROOT/sock" || exit 1
echo "$LIVE_PID" > "$CACHE_ROOT/pid"
case "$FAULT" in
    replaced) mv "$CACHE_ROOT/data" "$CACHE_ROOT/saved" && mkdir "$CACHE_ROOT/data" && echo foreign > "$CACHE_ROOT/data/foreign" ;;
    deleted) rmdir "$CACHE_ROOT/data" ;;
esac
`)
			switch failure {
			case "pin":
				f.program(t, "ln", `case "$2" in *.pin) exit 1 ;; esac
command -p ln "$@"`)
			case "marker":
				f.program(t, "stat", `case "$2" in
    %w) command -p stat -c '-' "$3" ;;
    %d:%i:%w) command -p stat -c '%d:%i:-' "$3" ;;
    *) command -p stat "$@" ;;
esac`)
				f.program(t, "mkdir", `case "$1" in *.owner) exit 1 ;; esac
command -p mkdir "$@"`)
			}
			f.check(t, "", "reserve", f.pidfile)
			out, err := f.start(t).Output()
			if failure == "deleted-after-start" {
				if err != nil || !strings.Contains(string(out), "ready=1") {
					t.Fatalf("confirmed startup: %q, %v", out, err)
				}
				if err := os.RemoveAll(f.cache); err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("injected startup evidence failure was ignored")
			}
			// The lost start reply, then deletion of the PID file, leave only
			// the process's unique argument available for reconciliation.
			if err := os.Remove(f.pidfile); err != nil {
				t.Fatal(err)
			}
			if failure == "pin" {
				f.check(t, "stopped=0", "stop", "0", f.sock, f.pidfile)
			}
			f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
			select {
			case <-unrelated:
				t.Fatal("stop signaled a process using a different socket")
			default:
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("stop left the owned process running")
			}
			remove := func(want string) {
				t.Helper()
				f.check(t, want, "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
			}
			switch failure {
			case "pin":
				if err := os.Rename(f.sock, f.sock+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.sock, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
				remove("removed=0")
				if got, err := os.ReadFile(f.sock); err != nil || string(got) != "foreign" {
					t.Fatalf("foreign socket replacement: %q, %v", got, err)
				}
				if err := os.Rename(f.sock+".original", f.sock); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				remove("removed=0")
				if got, err := os.ReadFile(filepath.Join(f.cache, "foreign")); err != nil || string(got) != "foreign\n" {
					t.Fatalf("foreign cache directory: %q, %v", got, err)
				}
				if err := os.Rename(f.cache, f.cache+".foreign"); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(f.root, "saved"), f.cache); err != nil {
					t.Fatal(err)
				}
			case "marker":
				// A filesystem without birth times needs its marker before a
				// recursive removal can be authorized. Preserve the directory.
				remove("removed=0")
				if err := os.Rename(f.cache, f.cache+".unverified"); err != nil {
					t.Fatal(err)
				}
			}
			remove("removed=1")
			for _, name := range []string{f.cache, f.sock, f.asok, f.pidfile, f.pidfile + ".lease"} {
				if _, err := os.Lstat(name); !os.IsNotExist(err) {
					t.Fatalf("owned path after Restore: %s: %v", name, err)
				}
			}
		})
	}
}

// A Docker backend can deliver an exec after its caller loses the reply. Run
// the actual supervisor cancel and delayed invocation against a temporary marker.
type delayedCacheReservation struct {
	*fakeCacheClient
	root       string
	saved      []string
	failCancel bool
}

func (c *delayedCacheReservation) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	switch args[2] {
	case supervised.ShellScript:
		c.saved = append([]string(nil), args...)
		c.saved[4] = filepath.Join(c.root, filepath.Base(args[4]))
		return 0, nil, context.Canceled
	case supervised.ShellCancelScript:
		if c.failCancel {
			c.failCancel = false
			return 0, nil, errors.New("first cancellation lost its reply")
		}
		args = append([]string(nil), args...)
		args[4] = filepath.Join(c.root, filepath.Base(args[4]))
	default:
		if args[4] != "release-reservation" {
			return c.fakeCacheClient.Exec(ctx, args, opts...)
		}
	}
	out, err := exec.CommandContext(ctx, args[0], args[1:]...).Output()
	if err != nil {
		return 0, nil, err
	}
	return clustertest.Reply(args, 0, out)
}

func TestCacheReservationReplyLossFencesLateDispatch(t *testing.T) {
	f := newCacheShellFixture(t)
	c := &delayedCacheReservation{fakeCacheClient: newFakeCacheClient(), root: f.root, failCancel: true}
	lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{Path: f.cache, Socket: f.sock})
	if lease == nil || err == nil || lease.reserveMarker == "" || lease.launchSent {
		t.Fatalf("lost reservation: %+v, %v", lease, err)
	}
	if err := os.WriteFile(f.cache, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := exec.CommandContext(t.Context(), c.saved[0], c.saved[1:]...).Run(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(f.cache); err != nil || string(got) != "foreign" {
		t.Fatalf("foreign resource after late dispatch: %q, %v", got, err)
	}
	entries, err := os.ReadDir(f.root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("late reservation recreated state: %v, %v", entries, err)
	}
}
