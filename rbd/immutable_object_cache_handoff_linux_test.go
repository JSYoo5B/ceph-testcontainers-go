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
