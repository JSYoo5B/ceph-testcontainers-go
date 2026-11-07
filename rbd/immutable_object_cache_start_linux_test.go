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
	"github.com/google/uuid"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type cacheShellFixture struct {
	root, cache, sock, asok, pidfile string
	env                              []string
}

func newCacheShellFixture(t *testing.T) cacheShellFixture {
	t.Helper()
	root := t.TempDir()
	f := cacheShellFixture{root: root, cache: filepath.Join(root, "data"), sock: filepath.Join(root, "sock"),
		asok: filepath.Join(root, "admin"), pidfile: filepath.Join(root, uuid.NewString()+".pid"), env: append(os.Environ(), "PATH="+root+":"+os.Getenv("PATH"))}
	f.cleanTestAlias(t)
	return f
}
func (f cacheShellFixture) cleanTestAlias(t *testing.T) {
	t.Helper()
	alias := filepath.Dir(f.privateSocket())
	t.Cleanup(func() {
		if dest, err := os.Readlink(alias); err == nil && dest == f.pidfile+".lease" {
			if err := os.Remove(alias); err != nil {
				t.Error(err)
			}
		}
	})
}

func (f cacheShellFixture) privateSocket() string {
	return filepath.Join("/tmp", "tc-ioc-"+filepath.Base(f.pidfile), "s")
}

func (f cacheShellFixture) claimPath(t *testing.T) string {
	t.Helper()
	out, err := f.command(t, "inspect", f.cache, f.sock).Output()
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
	if err != nil || inspection.claim == "" {
		t.Fatalf("inspect: %+v, %v", inspection, err)
	}
	return inspection.claim
}

func (f cacheShellFixture) reserve(t *testing.T) {
	t.Helper()
	claim := f.claimPath(t)
	f.check(t, "", "claim", claim, f.pidfile+".lease")
	t.Cleanup(func() {
		if target, _ := os.Readlink(claim); target == "sh:"+f.pidfile+".lease" {
			_ = os.Remove(claim)
		}
	})
	f.check(t, "", "reserve", f.pidfile+".lease")
	f.check(t, "", "reserve", filepath.Join(filepath.Dir(f.cache), "."+filepath.Base(f.pidfile)+".stage"))
}

func (f cacheShellFixture) release(t *testing.T, stage string) {
	t.Helper()
	dirs := []string{f.pidfile + ".lease", stage}
	f.check(t, "removed=1", "release-reservation", append([]string{"", ""}, dirs...)...)
	f.check(t, "removed=1", "unclaim", f.claimPath(t), f.pidfile+".lease")
	f.check(t, "removed=1", "forget-reservation", dirs...)
}

func (f cacheShellFixture) process(t *testing.T, socket string) (int, <-chan struct{}) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "sh", "-c", "while :; do sleep 0.02; done", "daemon",
		"--pid-file="+filepath.Join(f.pidfile+".lease", "pid"), "--immutable_object_cache_sock="+socket)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
	return cmd.Process.Pid, done
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
	return f.command(t, "start", "admin", f.cache, f.sock, "1024", f.asok, f.pidfile, "none", "", f.claimPath(t))
}

func TestCacheStartNeverDispatched(t *testing.T) {
	f := newCacheShellFixture(t)
	f.reserve(t)
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	if err := f.start(t).Run(); err == nil {
		t.Fatal("late launch accepted a revoked reservation")
	}
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
	entries, err := os.ReadDir(f.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("late launch left files: %v, %v", entries, err)
	}
}

func TestCacheDaemonizedParentSuccessWithoutDaemon(t *testing.T) {
	f := newCacheShellFixture(t)
	f.program(t, "ceph-immutable-object-cache", "exit 0")
	f.program(t, "sleep", "exit 0") // skip only the readiness delay
	f.reserve(t)
	out, err := f.start(t).Output()
	if err != nil || strings.TrimSpace(string(out)) != "exit=0\npid=\nready=0" {
		t.Fatalf("launcher: %q, %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
	for _, name := range []string{f.cache, f.pidfile + ".lease"} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			t.Fatalf("cleanup left %s: %v", name, err)
		}
	}
}

func TestCacheFinishedLaunchWithMissingEvidence(t *testing.T) {
	for _, failure := range []string{"pin", "replaced", "deleted", "deleted-after-start"} {
		t.Run(failure, func(t *testing.T) {
			f := newCacheShellFixture(t)
			listener, err := net.Listen("unix", filepath.Join(f.root, "original"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			// Keep the owned process waitable by the test after the launcher exits.
			// Its argv carries the exact same unique PID option as the native child.
			pid, done := f.process(t, f.privateSocket())
			_, unrelated := f.process(t, f.privateSocket()+".foreign")
			if failure == "pin" {
				f.process(t, f.privateSocket()) // a second matching process requires another Stop
			}
			f.env = append(f.env, "CACHE_ROOT="+f.root, "LIVE_PID="+strconv.Itoa(pid), "FAULT="+failure)
			f.program(t, "ceph-immutable-object-cache", `
for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) command -p ln "$CACHE_ROOT/original" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$LIVE_PID" > "${arg#*=}" || exit 1 ;;
    esac
done
case "$FAULT" in
    replaced) mv "$CACHE_ROOT/data" "$CACHE_ROOT/saved" && mkdir "$CACHE_ROOT/data" && echo foreign > "$CACHE_ROOT/data/foreign" ;;
    deleted) rm -rf "$CACHE_ROOT/data" ;;
esac
`)
			switch failure {
			case "pin":
				f.program(t, "ln", `case "$*" in *.pin) exit 1 ;; esac
command -p ln "$@"`)

			}
			f.reserve(t)
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
			if err := os.Remove(filepath.Join(f.pidfile+".lease", "pid")); err != nil {
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
				// The socket pin failed before the public path could be created.
				// Cleanup must leave an outside replacement alone.
				if err := os.WriteFile(f.sock, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
				remove("removed=1")
				if got, err := os.ReadFile(f.sock); err != nil || string(got) != "foreign" {
					t.Fatalf("foreign socket replacement: %q, %v", got, err)
				}
				if err := os.Rename(f.sock, f.sock+".foreign"); err != nil {
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

			}
			remove("removed=1")
			f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
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
	claim      string
	delay      string
	saved      []string
	failCancel bool
}

func (c *delayedCacheReservation) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	switch args[2] {
	case supervised.ShellScript:
		if args[11] != c.delay || c.saved != nil {
			return c.fakeCacheClient.Exec(ctx, args, opts...)
		}
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
		if args[4] == "inspect" {
			return clustertest.Reply(args, 0, []byte("binary=1\nplugin=1\nflock=1\nrename=1\nsocket=0\npath=none\nclaim="+c.claim+"\n"))
		}
		if args[4] != "release-reservation" && args[4] != "forget-reservation" && args[4] != "unclaim" {
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
	for _, delay := range []string{"claim", "reserve"} {
		t.Run(delay, func(t *testing.T) {
			f := newCacheShellFixture(t)
			c := &delayedCacheReservation{fakeCacheClient: newFakeCacheClient(), root: f.root, claim: f.claimPath(t), delay: delay, failCancel: true}
			lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{Path: f.cache, Socket: f.sock})
			if lease == nil || err == nil || lease.reserveMarker == "" || lease.launchSent || len(c.saved) <= 12 || c.saved[11] != delay {
				t.Fatalf("lost %s: %+v, %v", delay, lease, err)
			}
			t.Cleanup(func() {
				if target, _ := os.Readlink(c.claim); target == lease.owner+":"+lease.pidFile+".lease" {
					_ = os.Remove(c.claim)
				}
			})
			requested := c.claim
			if delay == "reserve" {
				requested = lease.pidFile + ".lease"
			}
			if c.saved[12] != requested {
				t.Fatalf("saved %s target %s, want %s", delay, c.saved[12], requested)
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
			if _, err := os.Lstat(requested); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("late %s recreated %s: %v", delay, requested, err)
			}
			if got, err := os.ReadFile(f.cache); err != nil || string(got) != "foreign" {
				t.Fatalf("foreign resource after late %s: %q, %v", delay, got, err)
			}
			entries, err := os.ReadDir(f.root)
			if err != nil || len(entries) != 1 {
				t.Fatalf("late %s left files: %v, %v", delay, entries, err)
			}
		})
	}
}

func TestCacheReservationProbesBothFilesystems(t *testing.T) {
	for _, role := range []string{"socket", "cache"} {
		for _, attempt := range []string{"first", "occupied"} {
			t.Run(role+"/"+attempt, func(t *testing.T) {
				f := newCacheShellFixture(t)
				socketParent, cacheParent := filepath.Join(f.root, "runtime"), filepath.Join(f.root, "storage")
				if err := os.Mkdir(cacheParent, 0700); err != nil {
					t.Fatal(err)
				}
				control, stage := filepath.Join(socketParent, "pid.lease"), filepath.Join(cacheParent, ".pid.stage")
				target := map[string]string{"socket": control, "cache": stage}[role]
				f.env = append(f.env, "FAIL_SEED="+target+".pending-sh", "ATTEMPT="+attempt)
				f.program(t, "mv", `if test "$2" = "$FAIL_SEED"; then exit 1; fi
exec /bin/mv "$@"`)
				f.program(t, "python3", `if test "$PWD" = "$FAIL_SEED" && test "${3:-}" = probe.from; then
    if test "$ATTEMPT" = first || test -L probe.to; then exit 22; fi
fi
exec /usr/bin/python3 "$@"`)
				dirs := []string{control}
				if role == "cache" {
					f.check(t, "", "reserve", control)
					dirs = append(dirs, stage)
				}
				cmd := f.command(t, "reserve", target)
				if err := cmd.Run(); err == nil || cmd.ProcessState.ExitCode() != 4 {
					t.Fatalf("reserve %s: %v", role, err)
				}
				if _, err := os.Lstat(target); !os.IsNotExist(err) {
					t.Fatalf("unsupported reservation published: %v", err)
				}
				f.check(t, "removed=1", "release-reservation", append([]string{target, ""}, dirs...)...)
				f.check(t, "removed=1", "forget-reservation", dirs...)
				for _, parent := range []string{socketParent, cacheParent} {
					entries, err := os.ReadDir(parent)
					if err != nil || len(entries) != 0 {
						t.Fatalf("reservation leak %s: %v, %v", parent, entries, err)
					}
				}
				for _, name := range []string{"python3", "mv"} {
					if err := os.Remove(filepath.Join(f.root, name)); err != nil {
						t.Fatal(err)
					}
				}
				f.check(t, "", "reserve", target)
				f.check(t, "removed=1", "release-reservation", "", "", target)
				f.check(t, "removed=1", "forget-reservation", target)
			})
		}
	}
}
