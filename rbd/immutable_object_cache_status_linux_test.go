package rbd

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// The substitute daemon publishes a real PID file and socket. The production
// status script must recover its identity even when no startup PID reached Go.
func TestCacheStatusRecoversIdentityAndRejectsIncompleteTraversal(t *testing.T) {
	f := newCacheShellFixture(t)
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
	f.program(t, "ceph", `printf '{"objecter":{"op_r":0}}\n'`)
	// Discard the startup response, as happens when Docker loses its reply.
	if err := f.start(t).Run(); err != nil {
		t.Fatal(err)
	}
	status := func(knownPID int) (ImmutableObjectCacheStatus, error) {
		out, err := f.command(t, "status", strconv.Itoa(knownPID), f.sock, f.asok, f.cache, f.pidfile).Output()
		if err != nil {
			return ImmutableObjectCacheStatus{}, err
		}
		return parseImmutableObjectCacheStatus(immutableObjectCacheOutput{op: "status", out: out})
	}
	for _, knownPID := range []int{0, pid} {
		got, err := status(knownPID)
		if err != nil || got.PID != pid || !got.Running || got.CachedObjects != 0 || got.CachedBytes != 0 {
			t.Fatalf("known PID %d: %+v, %v", knownPID, got, err)
		}
	}
	object := filepath.Join(f.cache, ".ceph-immutable-cache", "objects", "object")
	if err := os.WriteFile(object, []byte("cached"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "printf '6\\n'"} {
		f.program(t, "find", prefix+"\nexit 1")
		if got, err := status(0); err == nil {
			t.Fatalf("failed traversal returned complete counts: %+v", got)
		}
	}
	if err := os.Remove(filepath.Join(f.root, "find")); err != nil {
		t.Fatal(err)
	}
	if got, err := status(0); err != nil || got.CachedObjects != 1 || got.CachedBytes != 6 {
		t.Fatalf("observation retry: %+v, %v", got, err)
	}
	f.check(t, "stopped=1", "stop", "0", f.sock, f.pidfile)
	if got, err := status(pid); err != nil || got.Running || got.PID != 0 || got.CachedObjects != 1 {
		t.Fatalf("stopped daemon: %+v, %v", got, err)
	}
	f.check(t, "removed=1", "remove", f.cache, f.sock, f.asok, f.pidfile, "1")
	f.release(t, filepath.Join(f.root, "."+filepath.Base(f.pidfile)+".stage"))
	f.assertReleased(t)
}
