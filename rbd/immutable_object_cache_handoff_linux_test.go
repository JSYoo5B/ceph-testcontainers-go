package rbd

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func (f *cacheShellFixture) startForHandoff(t *testing.T) {
	t.Helper()
	f.reserve(t)
	bound, err := net.Listen("unix", filepath.Join(f.root, "bound"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bound.Close() })
	pid, _ := f.process(t, f.privateSocket())
	f.env = append(f.env, "BOUND="+bound.Addr().String(), "LIVE_PID="+strconv.Itoa(pid))
	f.program(t, "ceph-immutable-object-cache", `for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) ln "$BOUND" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$LIVE_PID" > "${arg#*=}" || exit 1 ;;
    esac
done`)
	out, err := f.start(t).Output()
	if err != nil || !strings.Contains(string(out), "ready=1") {
		t.Fatalf("start: %q, %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
}

func listenOutside(t *testing.T, socket string) *net.UnixListener {
	t.Helper()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	l.SetUnlinkOnClose(false)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func assertCacheListenerAt(t *testing.T, l *net.UnixListener, path string) {
	t.Helper()
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer func() { _ = c.Close() }()
	if err := l.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	accepted, err := l.Accept()
	if err != nil {
		t.Fatalf("listener did not receive connection to %s: %v", path, err)
	}
	if _, err := io.WriteString(accepted, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "ok" {
		t.Fatalf("response over %s: %q, %v", path, b, err)
	}
}

func TestCacheRestoreRechecksThePublicEntryItMoved(t *testing.T) {
	for _, kind := range []string{"listener", "directory", "symlink", "second-listener", "interrupted-transfer"} {
		t.Run(kind, func(t *testing.T) {
			f := newCacheShellFixture(t)
			f.startForHandoff(t)
			finish := func(want string) { t.Helper(); f.check(t, want, "remove", f.cache, f.sock, f.asok, f.pidfile, "1") }
			// Pause the real production rename at its call site, after inspection.
			// The outside writer does not cooperate with the fixture's flock.
			once := filepath.Join(f.root, "pause-once")
			if err := os.WriteFile(once, nil, 0600); err != nil {
				t.Fatal(err)
			}
			f.env = append(f.env, "PAUSE_ONCE="+once, "KIND="+kind)
			f.program(t, "python3", `case "$3:$4" in
    */sock:*/socket.handoff)
        if test -f "$PAUSE_ONCE"; then
            rm "$PAUSE_ONCE" || exit 1
            printf x >&3; read -r go <&4 || exit 1
            command -p python3 "$@" || exit $?
            case "$KIND" in second-listener|interrupted-transfer) printf x >&3; read -r go <&4 || exit 1 ;; esac
            exit 0
        fi ;;
esac
command -p python3 "$@"`)
			pipe := func() (*os.File, *os.File) {
				t.Helper()
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
				return r, w
			}
			begun, notify := pipe()
			gate, resume := pipe()
			if err := begun.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			cmd := f.command(t, "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
			var out bytes.Buffer
			cmd.Stdout = &out
			cmd.ExtraFiles = []*os.File{notify, gate}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if err := notify.Close(); err != nil {
				t.Fatal(err)
			}
			wait := func() {
				t.Helper()
				if _, err := io.ReadFull(begun, make([]byte, 1)); err != nil {
					t.Fatal(err)
				}
			}
			proceed := func() {
				t.Helper()
				if _, err := resume.WriteString("go\n"); err != nil {
					t.Fatal(err)
				}
			}
			wait()
			owned := filepath.Join(f.root, "owned")
			if err := os.Rename(f.sock, owned); err != nil {
				t.Fatal(err)
			}
			var first *net.UnixListener
			switch kind {
			case "directory":
				if err := os.Mkdir(f.sock, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(f.sock, "foreign"), []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("unrelated-target", f.sock); err != nil {
					t.Fatal(err)
				}
			default:
				first = listenOutside(t, f.sock)
			}
			proceed()
			held := filepath.Join(filepath.Dir(f.privateSocket()), "socket.handoff")
			switch kind {
			case "second-listener":
				wait()
				second := listenOutside(t, f.sock)
				proceed()
				if err := cmd.Wait(); err != nil || !strings.Contains(out.String(), "held=socket") {
					t.Fatalf("contended return: %q, %v", out.String(), err)
				}
				assertCacheListenerAt(t, second, f.sock)
				assertCacheListenerAt(t, first, held)
				finish("held=socket\nremoved=0")
				if err := os.Rename(f.sock, filepath.Join(f.root, "second")); err != nil {
					t.Fatal(err)
				}
				finish("removed=0") // return the retained listener to the now-free name
			case "interrupted-transfer":
				wait()
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				proceed()
				if err := cmd.Wait(); err == nil {
					t.Fatal("expected a lost handoff outcome")
				}
				// No acknowledgement identifies what entered the private slot.
				// Report its location for recovery instead of unlinking it.
				finish("held=socket\nremoved=0")
				assertCacheListenerAt(t, first, held)
				if err := os.Rename(held, f.sock); err != nil {
					t.Fatal(err)
				}
			default:
				if err := cmd.Wait(); err != nil || strings.TrimSpace(out.String()) != "removed=0" {
					t.Fatalf("outside replacement: %q, %v", out.String(), err)
				}
			}
			switch kind {
			case "directory":
				if got, err := os.ReadFile(filepath.Join(f.sock, "foreign")); err != nil || string(got) != "foreign" {
					t.Fatalf("outside directory: %q, %v", got, err)
				}
			case "symlink":
				if got, err := os.Readlink(f.sock); err != nil || got != "unrelated-target" {
					t.Fatalf("outside symlink: %q, %v", got, err)
				}
			default:
				assertCacheListenerAt(t, first, f.sock)
			}
			if err := os.Rename(f.sock, filepath.Join(f.root, "outside")); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(owned, f.sock); err != nil {
				t.Fatal(err)
			}
			finish("removed=1")
			f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
			f.assertReleased(t)
		})
	}
}

func TestCacheHandoffPreservesAnOccupiedPrivateDestination(t *testing.T) {
	f := newCacheShellFixture(t)
	f.startForHandoff(t)
	held := filepath.Join(f.pidfile+".lease", "socket.handoff")
	if err := os.Mkdir(held, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(held, "outside"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	f.check(t, "held=socket\nremoved=0", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	if b, err := os.ReadFile(filepath.Join(held, "outside")); err != nil || string(b) != "outside" {
		t.Fatalf("private destination changed: %q, %v", b, err)
	}
	if _, err := os.Lstat(f.sock); err != nil {
		t.Fatalf("public name changed: %v", err)
	}
	if err := os.Rename(held, filepath.Join(f.root, "outside")); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
}

func TestCacheRefusesDisplacedStageBeforeLaunch(t *testing.T) {
	f := newCacheShellFixture(t)
	f.reserve(t)
	stage := filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage")
	if err := os.Rename(stage, stage+".owned"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "foreign"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := f.start(t).Output()
	if err != nil || strings.TrimSpace(string(out)) != "conflict=1" {
		t.Fatalf("displaced stage: %q, %v", out, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	f.check(t, "removed=0", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	if b, err := os.ReadFile(filepath.Join(stage, "foreign")); err != nil || string(b) != "outside" {
		t.Fatalf("foreign stage: %q, %v", b, err)
	}
	if err := os.Rename(stage, stage+".foreign"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(stage+".owned", stage); err != nil {
		t.Fatal(err)
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, stage)
}

func TestCacheRestoreReturnsDisplacedDirectories(t *testing.T) {
	for _, subject := range []string{"root", "marker"} {
		for _, outcome := range []string{"vacant", "reoccupied", "interrupted", "owned-interrupted"} {
			t.Run(subject+"/"+outcome, func(t *testing.T) {
				f := newCacheShellFixture(t)
				parent, initial, created, identity := f.root, "none", "1", ""
				public, slot := f.cache, "root.handoff"
				if subject == "marker" {
					if err := os.Mkdir(f.cache, 0700); err != nil {
						t.Fatal(err)
					}
					parent, initial, created = f.cache, "empty", "0"
					public, slot = filepath.Join(f.cache, ".ceph-immutable-cache"), "reclaimed"
					out, err := f.command(t, "inspect", f.cache, f.sock).Output()
					if err != nil {
						t.Fatal(err)
					}
					info, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", strings.TrimSpace(string(out))))
					if err != nil {
						t.Fatal(err)
					}
					identity = info.identity
				}
				stage := filepath.Join(parent, "."+filepath.Base(f.pidfile)+".stage")
				claim := f.claimPath(t)
				f.check(t, "", "claim", claim, f.pidfile+".lease")
				t.Cleanup(func() {
					if target, _ := os.Readlink(claim); target == "sh:"+f.pidfile+".lease" {
						_ = os.Remove(claim)
					}
				})
				f.check(t, "", "reserve", f.pidfile+".lease")
				f.check(t, "", "reserve", stage, identity)
				f.program(t, "ceph-immutable-object-cache", "exit 1")
				out, err := f.command(t, "start", "admin", f.cache, f.sock, "1024", f.asok, f.pidfile, initial, identity, claim).Output()
				if err != nil || !strings.Contains(string(out), "exit=1") {
					t.Fatalf("start %q: %v", out, err)
				}
				f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
				owned := filepath.Join(f.root, "saved")
				f.env = append(f.env, "SLOT="+slot, "SAVED="+owned, "OUTCOME="+outcome)
				// This writer takes no fixture lock and runs after the source
				// check. Invoke the real syscall before simulating a lost reply.
				f.program(t, "python3", `if test "${4##*/}" = "$SLOT" && ! test -e "$SAVED.once"; then
    touch "$SAVED.once" || exit 1
    if test "$OUTCOME" != owned-interrupted; then
        mv -T "$3" "$SAVED" && mkdir "$3" && echo first > "$3/sentinel" || exit 1
    fi
    command -p python3 "$@" || exit $?
    case "$OUTCOME" in
        reoccupied) mkdir "$3" && echo second > "$3/sentinel" || exit 1 ;;
        owned-interrupted) mkdir "$3" && echo successor > "$3/sentinel" && kill -KILL "$PPID" ;;
        interrupted) kill -KILL "$PPID" ;;
    esac
    exit 0
fi
exec /usr/bin/python3 "$@"`)
				remove := func(want string) { t.Helper(); f.check(t, want, "remove", f.cache, f.sock, f.asok, f.pidfile, created) }
				held := filepath.Join(stage, slot)
				assertSentinel := func(dir, want string) {
					t.Helper()
					b, e := os.ReadFile(filepath.Join(dir, "sentinel"))
					if e != nil || string(b) != want+"\n" {
						t.Fatalf("sentinel at %s: %q, %v", dir, b, e)
					}
				}
				if strings.Contains(outcome, "interrupted") {
					if err := f.command(t, "remove", f.cache, f.sock, f.asok, f.pidfile, created).Run(); err == nil {
						t.Fatal("cleanup was not interrupted")
					}
				}
				switch outcome {
				case "vacant":
					remove("removed=0")
				case "reoccupied":
					remove("held=" + subject + "\nremoved=0")
					remove("held=" + subject + "\nremoved=0")
					assertSentinel(public, "second")
					assertSentinel(held, "first")
					if err := os.Rename(public, filepath.Join(f.root, "second")); err != nil {
						t.Fatal(err)
					}
					remove("removed=0")
				case "interrupted":
					remove("held=" + subject + "\nremoved=0")
					assertSentinel(held, "first")
					if err := os.Rename(held, public); err != nil {
						t.Fatal(err)
					}
				}
				if outcome != "owned-interrupted" {
					assertSentinel(public, "first")
					if err := os.Rename(public, filepath.Join(f.root, "first")); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(owned, public); err != nil {
						t.Fatal(err)
					}
				}
				remove("removed=1")
				remove("removed=1")
				f.release(t, stage)
				if outcome == "owned-interrupted" {
					assertSentinel(public, "successor")
					if err := os.Rename(public, filepath.Join(f.root, "successor")); err != nil {
						t.Fatal(err)
					}
				}
				if created == "1" {
					if _, err := os.Stat(f.cache); !os.IsNotExist(err) {
						t.Fatalf("cache survived: %v", err)
					}
				} else if entries, err := os.ReadDir(f.cache); err != nil || len(entries) != 0 {
					t.Fatalf("cache dir: %v, %v", entries, err)
				}
			})
		}
	}
}

func TestCacheRestoreReturnsRootWithNewChild(t *testing.T) {
	for _, outcome := range []string{"vacant", "reoccupied", "late-child", "lost-return"} {
		t.Run(outcome, func(t *testing.T) {
			f := newCacheShellFixture(t)
			f.startForHandoff(t)
			f.env = append(f.env, "OUTCOME="+outcome)
			// Add the file after inspection without replacing the root inode.
			// An independent writer may also bind the just-vacated public name.
			f.program(t, "python3", `case "${3##*/}:${4##*/}" in
    *:root.handoff)
        if test "$OUTCOME" != late-child; then echo outside > "$3/child" || exit 1; fi
        command -p python3 "$@" || exit $?
        if test "$OUTCOME" = reoccupied; then mkdir "$3" && echo successor > "$3/other" || exit 1; fi
        exit 0 ;;
    root.handoff:*)
        command -p python3 "$@" || exit $?
        if test "$OUTCOME" = lost-return; then kill -KILL "$PPID"; fi
        exit 0 ;;
esac
exec /usr/bin/python3 "$@"`)
			if outcome == "late-child" {
				f.program(t, "rmdir", `case "$1" in */root.handoff) echo outside > "$1/child" || exit 1 ;; esac
exec /bin/rmdir "$@"`)
			}
			remove := func(want string) { t.Helper(); f.check(t, want, "remove", f.cache, f.sock, f.asok, f.pidfile, "1") }
			read := func(name, want string) {
				t.Helper()
				b, err := os.ReadFile(name)
				if err != nil || string(b) != want+"\n" {
					t.Fatalf("file %s: %q, %v", name, b, err)
				}
			}
			stage := filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage")
			if outcome == "reoccupied" {
				for range 2 {
					remove("held=root\nremoved=0")
					read(filepath.Join(stage, "root.handoff", "child"), "outside")
					read(filepath.Join(f.cache, "other"), "successor")
				}
				if err := os.Rename(f.cache, filepath.Join(f.root, "successor")); err != nil {
					t.Fatal(err)
				}
			}
			if outcome == "lost-return" {
				if err := f.command(t, "remove", f.cache, f.sock, f.asok, f.pidfile, "1").Run(); err == nil {
					t.Fatal("expected interruption after returning directory")
				}
				read(filepath.Join(f.cache, "child"), "outside")
			}
			remove("removed=1")
			remove("removed=1")
			f.release(t, stage)
			read(filepath.Join(f.cache, "child"), "outside")
			if entries, err := os.ReadDir(f.cache); err != nil || len(entries) != 1 || entries[0].Name() != "child" {
				t.Fatalf("returned cache directory: %v, %v", entries, err)
			}
			f.assertReleased(t)
		})
	}
}
