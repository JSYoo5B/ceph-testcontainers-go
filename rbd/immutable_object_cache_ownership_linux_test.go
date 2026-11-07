package rbd

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func (f cacheShellFixture) assertReleased(t *testing.T) {
	t.Helper()
	for _, resource := range []string{f.pidfile + ".lease", filepath.Dir(f.privateSocket()),
		filepath.Join(filepath.Dir(f.cache), "."+filepath.Base(f.pidfile)+".stage")} {
		if _, err := os.Lstat(resource); !os.IsNotExist(err) {
			t.Fatalf("unreleased cache reservation %s: %v", resource, err)
		}
	}
}

func TestCacheKilledLauncherDirectoryPublication(t *testing.T) {
	for _, phase := range []string{"unrecorded", "published", "published-no-birth"} {
		t.Run(phase, func(t *testing.T) {
			f := newCacheShellFixture(t)
			switch phase {
			case "unrecorded":
				f.program(t, "stat", `case "$3" in *.stage/data) kill -KILL "$PPID"; exit 1 ;; esac
command -p stat "$@"`)
			case "published-no-birth":
				f.program(t, "stat", `case "$2" in
    %w) command -p stat -c '-' "$3" ;;
    %d:%i:%w) command -p stat -c '%d:%i:-' "$3" ;;
    *) command -p stat "$@" ;;
esac`)
				fallthrough
			case "published":
				f.program(t, "mv", `command -p mv "$@" || exit 1
case "$2" in *.stage/data) kill -KILL "$PPID" ;; esac`)
			}
			f.reserve(t)
			if err := f.start(t).Run(); err == nil {
				t.Fatal("expected the start script to die")
			}
			f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
			remove := func(want string) {
				t.Helper()
				f.check(t, want, "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
			}
			if phase == "unrecorded" {
				if _, err := os.Stat(f.cache); !os.IsNotExist(err) {
					t.Fatalf("an unrecorded cache directory was published: %v", err)
				}
				if err := os.Mkdir(f.cache, 0700); err != nil {
					t.Fatal(err)
				}
				remove("removed=1")
				if _, err := os.Stat(f.cache); err != nil {
					t.Fatalf("removed the outside directory: %v", err)
				}
			} else {
				// Even a replacement empty directory cannot authorize deletion.
				if err := os.Rename(f.cache, f.cache+".owned"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(f.cache, 0700); err != nil {
					t.Fatal(err)
				}
				remove("removed=0")
				if err := os.Rename(f.cache, f.cache+".foreign"); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(f.cache+".owned", f.cache); err != nil {
					t.Fatal(err)
				}
				if phase == "published-no-birth" {
					marker := filepath.Join(f.cache, ".ceph-immutable-cache")
					if err := os.Rename(marker, marker+".saved"); err != nil {
						t.Fatal(err)
					}
					remove("removed=0")
					if err := os.Rename(marker+".saved", marker); err != nil {
						t.Fatal(err)
					}
				}
				remove("removed=1")
				if _, err := os.Stat(f.cache); !os.IsNotExist(err) {
					t.Fatalf("published cache directory survived Restore: %v", err)
				}
			}
			f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
			f.assertReleased(t)
		})
	}
}

func TestCacheKilledLauncherBeforeSocketPublication(t *testing.T) {
	f := newCacheShellFixture(t)
	original := filepath.Join(f.root, "original")
	listener, err := net.Listen("unix", original)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	pid, done := f.process(t, f.privateSocket())
	f.env = append(f.env, "BOUND="+original, "PID="+strconv.Itoa(pid))
	f.program(t, "ceph-immutable-object-cache", `for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) ln "$BOUND" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$PID" > "${arg#*=}" || exit 1 ;;
    esac
done
kill -KILL "$PPID"`)
	f.reserve(t)
	if err := f.start(t).Run(); err == nil {
		t.Fatal("expected the launching shell to die")
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop left the owned process alive")
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
	f.assertReleased(t)
	for _, resource := range []string{f.cache, f.sock, f.asok, f.pidfile} {
		if _, err := os.Stat(resource); !os.IsNotExist(err) {
			t.Fatalf("owned resource survived Restore: %s: %v", resource, err)
		}
	}
}

func TestCacheCompetingPublicListenerRemainsReachable(t *testing.T) {
	for _, outcome := range []string{"failed-launch", "ready-private-listener"} {
		t.Run(outcome, func(t *testing.T) {
			f := newCacheShellFixture(t)
			f.reserve(t)
			own, err := net.Listen("unix", filepath.Join(f.root, "own"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = own.Close() })
			pid, _ := f.process(t, f.privateSocket())
			f.env = append(f.env, "RESULT="+outcome, "BOUND="+own.Addr().String(), "PID="+strconv.Itoa(pid))
			f.program(t, "ceph-immutable-object-cache", `echo x >&3; read -r go <&4 || exit 1
test "$RESULT" != failed-launch || exit 1
for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) ln "$BOUND" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$PID" > "${arg#*=}" || exit 1 ;;
    esac
done`)
			pipe := func() (*os.File, *os.File) {
				t.Helper()
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
				return r, w
			}
			ready, notify := pipe()
			gate, release := pipe()
			start := f.start(t)
			start.ExtraFiles = []*os.File{notify, gate}
			if err := start.Start(); err != nil {
				t.Fatal(err)
			}
			if err := notify.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(ready, make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			foreign, err := net.Listen("unix", f.sock)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = foreign.Close() })
			if _, err := release.WriteString("go\n"); err != nil {
				t.Fatal(err)
			}
			err = start.Wait()
			if err != nil {
				t.Fatal(err)
			}
			f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
			f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
			f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
			f.assertReleased(t)
			conn, err := net.DialTimeout("unix", f.sock, time.Second)
			if err != nil {
				t.Fatalf("lost the foreign listener: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			unix := foreign.(*net.UnixListener)
			if err := unix.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			peer, err := unix.Accept()
			if err != nil {
				t.Fatalf("connection went to a different listener: %v", err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			if _, err := io.WriteString(peer, "ok"); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 2)
			if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ok" {
				t.Fatalf("foreign listener response: %q, %v", buf, err)
			}
		})
	}
}
