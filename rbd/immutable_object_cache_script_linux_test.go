package rbd

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the production shell against real directory and socket inodes
// with a substitute daemon. The no-birth-time cases mask only stat's %w.
func TestCacheRestorePreservesReplacedPaths(t *testing.T) {
	for _, tc := range []struct {
		name, initial string
		noBirthTime   bool
	}{
		{"none", "none", false},
		{"empty", "empty", false},
		{"none-no-birth", "none", true},
		{"empty-no-birth", "empty", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := tc.initial
			root := t.TempDir()
			closeOnCleanup := func(c io.Closer) {
				t.Cleanup(func() {
					if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
						t.Errorf("close cache test resource: %v", err)
					}
				})
			}
			if tc.noBirthTime {
				stat := `#!/bin/sh
case "$2" in
    %w) command -p stat -c '-' "$3" ;;
    %d:%i:%w) command -p stat -c '%d:%i:-' "$3" ;;
    *) command -p stat "$@" ;;
esac
`
				if err := os.WriteFile(filepath.Join(root, "stat"), []byte(stat), 0700); err != nil {
					t.Fatal(err)
				}
			}
			cache, sock := filepath.Join(root, "cache"), filepath.Join(root, "client.sock")
			asok, pidfile := filepath.Join(root, "admin.sock"), filepath.Join(root, "daemon.pid")
			if initial == "empty" {
				if err := os.Mkdir(cache, 0700); err != nil {
					t.Fatal(err)
				}
			}
			listener, err := net.Listen("unix", filepath.Join(root, "daemon-socket"))
			if err != nil {
				t.Fatal(err)
			}
			closeOnCleanup(listener)
			// Returning a PID and publishing the socket lets start record the
			// files. The process has already exited when stop checks its PID.
			path := root + ":" + os.Getenv("PATH")
			daemon := `#!/bin/sh
printf x >&3
read -r resume <&4 || exit 1
for arg do
    case "$arg" in
        --immutable_object_cache_sock=*) ln "$TEST_SOCKET" "${arg#*=}" || exit 1 ;;
        --pid-file=*) echo "$$" > "${arg#*=}" || exit 1 ;;
    esac
done
`
			if err := os.WriteFile(filepath.Join(root, "ceph-immutable-object-cache"), []byte(daemon), 0700); err != nil {
				t.Fatal(err)
			}
			command := func(args ...string) *exec.Cmd {
				cmd := exec.CommandContext(t.Context(), "sh", append([]string{"-c", immutableObjectCacheScript, "sh"}, args...)...)
				cmd.Env = append(os.Environ(), "PATH="+path, "TEST_SOCKET="+filepath.Join(root, "daemon-socket"))
				return cmd
			}
			run := func(args ...string) string {
				t.Helper()
				out, err := command(args...).CombinedOutput()
				if err != nil {
					t.Fatalf("%s: %s: %v", args[0], out, err)
				}
				return strings.TrimSpace(string(out))
			}
			// A canceled Exec can return the lease while its start script still
			// runs in the container. Hold that script before its PID publication.
			signalR, signalW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			closeOnCleanup(signalR)
			closeOnCleanup(signalW)
			gateR, gateW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			closeOnCleanup(gateR)
			closeOnCleanup(gateW)
			inspection, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", run("inspect", cache, sock)))
			if err != nil || inspection.path != initial {
				t.Fatalf("inspect: %+v, %v", inspection, err)
			}
			if got := run("reserve", pidfile); got != "" {
				t.Fatal(got)
			}
			start := command("start", "admin", cache, sock, "1024", asok, pidfile, initial, inspection.identity)
			start.ExtraFiles = []*os.File{signalW, gateR}
			if err := start.Start(); err != nil {
				t.Fatal(err)
			}
			if err := signalW.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(signalR, make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			if got := run("stop", "0", sock, pidfile); got != "stopped=0" {
				t.Fatalf("accepted an in-flight start with no published PID: %s", got)
			}
			if _, err := gateW.WriteString("resume\n"); err != nil {
				t.Fatal(err)
			}
			if err := start.Wait(); err != nil {
				t.Fatal(err)
			}
			if got := run("stop", "0", sock, pidfile); got != "stopped=1" {
				t.Fatalf("settled start: %s", got)
			}
			created := bit(initial == "none")
			remove := func() string { return run("remove", cache, sock, asok, pidfile, created) }
			// Replace the directory first, then restore it and replace the socket.
			// Each conflict must preserve the foreign bytes until the owner moves
			// the replacement away; the same lease can then finish its cleanup.
			replacements := []string{cache, sock}
			if tc.noBirthTime {
				// Keep the parent inode intact but replace its ownership marker.
				// The device/inode check on the parent alone would accept this.
				marker := filepath.Join(cache, "."+filepath.Base(pidfile)+".owner")
				replacements = append(replacements, marker)
			}
			for _, file := range replacements {
				if err := os.Rename(file, file+".original"); err != nil {
					t.Fatal(err)
				}
				marker := file
				if file != sock {
					if err := os.Mkdir(file, 0700); err != nil {
						t.Fatal(err)
					}
					marker = filepath.Join(file, "foreign")
				}
				if err := os.WriteFile(marker, []byte("foreign bytes"), 0600); err != nil {
					t.Fatal(err)
				}
				if got := remove(); got != "removed=0" {
					t.Fatalf("accepted replacement %s: %s", file, got)
				}
				if got, err := os.ReadFile(marker); err != nil || string(got) != "foreign bytes" {
					t.Fatalf("modified replacement %s: %q, %v", marker, got, err)
				}
				if err := os.Rename(file, filepath.Join(root, filepath.Base(file)+".foreign")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(file+".original", file); err != nil {
					t.Fatal(err)
				}
			}
			if got := remove(); got != "removed=1" {
				t.Fatalf("cleanup after restoring original paths: %s", got)
			}
			for _, file := range []string{sock, pidfile, pidfile + ".lease"} {
				if _, err := os.Lstat(file); !os.IsNotExist(err) {
					t.Fatalf("owned path %s after cleanup: %v", file, err)
				}
			}
			entries, err := os.ReadDir(cache)
			if initial == "none" && !os.IsNotExist(err) || initial == "empty" && (err != nil || len(entries) != 0) {
				t.Fatalf("cache after cleanup: %v, %v", entries, err)
			}
		})
	}
}

func TestCacheRestoreAfterPrelaunchConflict(t *testing.T) {
	root := t.TempDir()
	cache, sock := filepath.Join(root, "cache"), filepath.Join(root, "socket")
	asok, pidfile := filepath.Join(root, "admin"), filepath.Join(root, "pid")
	if err := os.WriteFile(cache, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	// Inspection found an absent path. An outside writer won the race to
	// create it, so the daemon was never invoked.
	run := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "sh", append([]string{"-c", immutableObjectCacheScript, "sh"}, args...)...).Output()
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	if got := run("reserve", pidfile); got != "" {
		t.Fatal(got)
	}
	if got := run("start", "admin", cache, sock, "1024", asok, pidfile, "none", ""); got != "exit=1\npid=\nready=0" {
		t.Fatalf("prelaunch conflict: %s", got)
	}
	if got := run("stop", "0", sock, pidfile); got != "stopped=1" {
		t.Fatalf("refused launch did not settle: %s", got)
	}
	if got := run("remove", cache, sock, asok, pidfile, "1"); got != "removed=1" {
		t.Fatalf("refused launch did not restore: %s", got)
	}
	if body, err := os.ReadFile(cache); err != nil || string(body) != "foreign" {
		t.Fatalf("foreign file after Restore: %q, %v", body, err)
	}
	if _, err := os.Lstat(pidfile + ".lease"); !os.IsNotExist(err) {
		t.Fatalf("reservation survived cleanup: %v", err)
	}
}
