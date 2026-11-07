//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// TestImmutableObjectCache reads a clone through the RBD immutable object
// cache in a client container with an identity limited to the pool: the
// first read promotes every parent object, a second read makes no RADOS
// read, and after Stop the client reads the same data from RADOS. Restore
// deletes the cache directory that the lease created. A second lease on a
// cache directory that existed before keeps the directory and deletes only
// what the lease put there. Each daemon is identified as the process that
// holds the listening socket, with its start time, so its exit is checked
// apart from the lease's own process checks.
func TestImmutableObjectCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "tc-ioc"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	const id = "ioc-client"
	identity, err := cluster.CreateClient(ctx, id, ceph.ClientCaps{Mon: "profile rbd", OSD: "profile rbd pool=" + pool})
	if err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, harness.StaticFacts(cluster).Images.RBDAux, identity.WithIdentity(), ceph.WithIdleEntrypoint())
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	rbdCLI := func(args ...string) string {
		return harness.ExecOutput(t, ctx, client, append([]string{"rbd", "--id", id}, args...)...)
	}
	// 64 MiB in 4 MiB objects: 16 parent objects.
	rbdCLI("create", "--size", "64", "--image-feature", "layering", pool+"/parent")
	rbdCLI("bench", "--io-type", "write", "--io-size", "4M", "--io-total", "64M", "--io-pattern", "seq", pool+"/parent")
	rbdCLI("snap", "create", pool+"/parent@base")
	rbdCLI("snap", "protect", pool+"/parent@base")
	rbdCLI("clone", pool+"/parent@base", pool+"/child")
	export := func(image string, options map[string]string) (string, error) {
		args := []string{"rbd", "--id", id}
		for name, value := range options {
			args = append(args, "--"+name+"="+value)
		}
		return checkedRBDExportDigest(ctx, client, pool+"/"+image, args...)
	}
	checksum := func(image string, options map[string]string) string {
		t.Helper()
		digest, err := export(image, options)
		if err != nil {
			t.Fatal(err)
		}
		return digest
	}
	// A failed export is never hashed into a checksum.
	if digest, err := export("tc-missing", nil); err == nil || !strings.Contains(err.Error(), "rbd export exited") {
		t.Fatalf("the export of a missing image returned digest %q, %v", digest, err)
	}
	want := checksum("parent@base", nil)
	// The daemon reads each parent object from RADOS once and writes it into
	// its cache directory after the client's read returned, so the last
	// files can appear a moment later. More than 16 RADOS reads fails at
	// once.
	readThrough := func(lease *rbd.ImmutableObjectCacheLease) harness.Observation[rbd.ImmutableObjectCacheStatus] {
		t.Helper()
		if got := checksum("child", lease.ClientOptions()); got != want {
			t.Fatalf("the clone read through the cache has checksum %s, the parent %s", got, want)
		}
		cached, err := harness.Observe(ctx, 30*time.Second, 10*time.Second, 200*time.Millisecond, func(probeCtx context.Context) (rbd.ImmutableObjectCacheStatus, bool, error) {
			status, err := lease.Status(probeCtx)
			switch {
			case err != nil:
				return status, false, err
			case !status.Running || status.RADOSReads > 16 || status.CachedObjects > 16:
				return status, false, fmt.Errorf("unexpected cache state %+v", status)
			}
			return status, status.CachedObjects == 16 && status.CachedBytes == 64<<20 && status.RADOSReads == 16, nil
		})
		if err != nil {
			t.Fatalf("status after the first read: %+v, %v", cached.Value, err)
		}
		return cached
	}

	lease, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: id})
	restoreImmutableObjectCacheAtCleanup(t, lease)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: id, Path: "/var/cache/other"}); !errors.Is(err, ceph.ErrInvalidRequest) || second != nil {
		t.Fatalf("a second daemon on the same socket: %v", err)
	}
	// The first daemon has cached no objects. A different socket still shares
	// the same cache directory and must be refused before the second launch.
	second, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{
		ID: id, Path: lease.Config().Path, Socket: "/run/ceph/tc-second-cache.sock",
	})
	restoreImmutableObjectCacheAtCleanup(t, second)
	if second != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("different socket on a live cache directory: %v, %v", second, err)
	}
	status, err := lease.Status(ctx)
	if err != nil || !status.Running || status.CachedObjects != 0 || status.RADOSReads != 0 {
		t.Fatalf("status before a read: %+v, %v", status, err)
	}
	daemon := immutableObjectCacheDaemon(t, ctx, client, lease, status)
	cached := readThrough(lease)
	first := cached.Value
	if got := checksum("child", lease.ClientOptions()); got != want {
		t.Fatalf("the second read has checksum %s", got)
	}
	if again, err := lease.Status(ctx); err != nil || again.RADOSReads != first.RADOSReads || again.CachedObjects != 16 {
		t.Fatalf("status after the second read: %+v, %v", again, err)
	}

	if err := lease.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	afterStop := daemon.assertExited(t, ctx, client, "Stop")
	stopped, err := lease.Status(ctx)
	if err != nil || stopped.Running || stopped.CachedObjects != 16 {
		t.Fatalf("status after Stop: %+v, %v", stopped, err)
	}
	if got := checksum("child", lease.ClientOptions()); got != want {
		t.Fatalf("the read without the daemon has checksum %s", got)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	afterRestore := daemon.assertExited(t, ctx, client, "Restore")
	harness.ExecCommand(t, ctx, client, "sh", "-c", `test ! -e "$0" && test ! -L "$0"`, lease.Config().Path)
	daemon.assertLeaseFilesRemoved(t, ctx, client)
	t.Logf("pid %d (start time %s) cached %d objects (%d bytes) with %d RADOS reads, %d status reads after the first client read; a second read added none; after Stop its PID was %s and the client read the same data from RADOS; after Restore its PID was %s and the socket, admin socket, PID file and the cache directory that the lease created were gone",
		daemon.pid, daemon.start, first.CachedObjects, first.CachedBytes, first.RADOSReads, cached.Attempts, afterStop, afterRestore)

	// A cache directory that existed, empty, before the lease: Restore of a
	// running daemon stops it and keeps the directory, now empty again.
	const existing, existingSocket = "/var/cache/tc-ioc-existing", "/run/tc-ioc-existing/cache.sock"
	harness.ExecCommand(t, ctx, client, "mkdir", existing)
	kept, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: id, Path: existing, Socket: existingSocket})
	restoreImmutableObjectCacheAtCleanup(t, kept)
	if err != nil {
		t.Fatal(err)
	}
	status, err = kept.Status(ctx)
	if err != nil || !status.Running || status.CachedObjects != 0 {
		t.Fatalf("status of the lease on an existing directory before a read: %+v, %v", status, err)
	}
	keptDaemon := immutableObjectCacheDaemon(t, ctx, client, kept, status)
	if keptDaemon.pid == daemon.pid && keptDaemon.start == daemon.start {
		t.Fatalf("the second daemon has the identity of the first: %+v", keptDaemon)
	}
	keptCached := readThrough(kept).Value
	if err := kept.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	keptAfterRestore := keptDaemon.assertExited(t, ctx, client, "Restore")
	harness.ExecCommand(t, ctx, client, "sh", "-c", `test -d "$0" && test ! -L "$0" && test -z "$(ls -A "$0")"`, existing)
	keptDaemon.assertLeaseFilesRemoved(t, ctx, client)
	t.Logf("pid %d (start time %s) cached %d objects in the existing %s; after Restore its PID was %s, the cached objects, socket, admin socket and PID file were gone and the directory remained",
		keptDaemon.pid, keptDaemon.start, keptCached.CachedObjects, existing, keptAfterRestore)
}

// restoreImmutableObjectCacheAtCleanup restores lease, a started or
// partially started lease, when the test ends, before its client container
// is terminated, with its own time bound: the test's context is already
// cancelled then. A Restore error fails the test.
func restoreImmutableObjectCacheAtCleanup(t *testing.T, lease *rbd.ImmutableObjectCacheLease) {
	t.Helper()
	if lease == nil {
		return
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := lease.Restore(ctx); err != nil {
			t.Errorf("restore the immutable object cache at cleanup: %v", err)
		}
	})
}

// cacheDaemonIdentity is one ceph-immutable-object-cache process: its PID
// and the start time in /proc/<pid>/stat, which a process that reuses the
// PID does not share, and the files that its lease put beside its socket.
type cacheDaemonIdentity struct {
	pid    int
	start  string
	socket string
	files  []string
}

// cacheSocketHolders prints "<pid> <stat>" for each process that has the
// listening Unix socket bound to $0 open: /proc/net/unix gives the socket's
// inode (flags 00010000 mark a listening socket) and /proc/<pid>/fd the
// processes that hold it. The client path and the daemon's bound path are
// hard links to the same filesystem inode.
const cacheSocketHolders = `inode=$(awk '$4 == "00010000" && NF >= 8 { print $7, $8 }' /proc/net/unix |
    while read -r ino bound; do if test "$bound" -ef "$0"; then echo "$ino"; fi; done)
test -n "$inode" || exit 0
for fd in /proc/[0-9]*/fd/*; do
    test "$(readlink "$fd" 2>/dev/null)" = "socket:[$inode]" && echo "${fd%/fd/*}"
done | sort -u | while read -r proc; do
    stat=$(cat "$proc/stat" 2>/dev/null) && echo "${proc#/proc/} $stat"
done
true`

// cacheLeaseFiles lists the socket $0 and the immutable-object-cache-*
// admin sockets and PID files in its directory; with $1 = pid it prints the
// PID files' contents instead.
const cacheLeaseFiles = `cd "$(dirname "$0")" 2>/dev/null || exit 0
for f in "$(basename "$0")" immutable-object-cache-*.asok immutable-object-cache-*.pid; do
    if test -e "$f" || test -L "$f"; then
        if test "${1:-}" = pid; then case "$f" in *.pid) cat "$f"; echo ;; esac; else echo "$f"; fi
    fi
done`

// immutableObjectCacheDaemon identifies the daemon of lease without the
// lease's PID or its process checks: as the only process that holds the
// lease's listening socket. It checks that the lease and the daemon's PID
// file name the same process and that the lease put one admin socket and one
// PID file beside the socket.
func immutableObjectCacheDaemon(t *testing.T, ctx context.Context, client testcontainers.Container, lease *rbd.ImmutableObjectCacheLease, status rbd.ImmutableObjectCacheStatus) cacheDaemonIdentity {
	t.Helper()
	socket := lease.Config().Socket
	out := harness.ExecOutput(t, ctx, client, "sh", "-c", cacheSocketHolders, socket)
	lines := strings.Split(out, "\n")
	if out == "" || len(lines) != 1 {
		t.Fatalf("processes holding the listening socket %s: %q", socket, out)
	}
	pidText, stat, _ := strings.Cut(lines[0], " ")
	pid, err := strconv.Atoi(pidText)
	if err != nil {
		t.Fatalf("socket holder %q: %v", lines[0], err)
	}
	state, start, ok := procStatState(stat)
	if !ok || state == "Z" || state == "X" {
		t.Fatalf("stat of the socket holder %d: %q", pid, stat)
	}
	if status.PID != pid {
		t.Fatalf("the lease reports PID %d; the process holding %s is %d", status.PID, socket, pid)
	}
	if pidFile := harness.ExecOutput(t, ctx, client, "sh", "-c", cacheLeaseFiles, socket, "pid"); pidFile != pidText {
		t.Fatalf("the daemon's PID files hold %q; the process holding %s is %d", pidFile, socket, pid)
	}
	files := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c", cacheLeaseFiles, socket))
	base := path.Base(socket)
	if len(files) != 3 || files[0] != base || !strings.HasSuffix(files[1], ".asok") || !strings.HasSuffix(files[2], ".pid") {
		t.Fatalf("files of the lease beside %s: %q", socket, files)
	}
	return cacheDaemonIdentity{pid: pid, start: start, socket: socket, files: files}
}

// assertExited checks that the identified process no longer runs: its
// /proc entry is gone, belongs to a process with another start time, or is
// a zombie that the container's init has not reaped yet. It returns which.
func (d cacheDaemonIdentity) assertExited(t *testing.T, ctx context.Context, client testcontainers.Container, after string) string {
	t.Helper()
	stat := harness.ExecOutput(t, ctx, client, "sh", "-c", `cat "/proc/$0/stat" 2>/dev/null; true`, strconv.Itoa(d.pid))
	if stat == "" {
		return "gone"
	}
	state, start, ok := procStatState(stat)
	if !ok {
		t.Fatalf("stat of PID %d after %s: %q", d.pid, after, stat)
	}
	if start == d.start && state != "Z" && state != "X" {
		t.Fatalf("the daemon, PID %d started at %s, still runs after %s in state %s", d.pid, d.start, after, state)
	}
	if start != d.start {
		return "reused by another process"
	}
	return "a zombie"
}

// assertLeaseFilesRemoved checks that the socket, admin socket and PID file
// that the daemon's lease created are gone.
func (d cacheDaemonIdentity) assertLeaseFilesRemoved(t *testing.T, ctx context.Context, client testcontainers.Container) {
	t.Helper()
	left := strings.Fields(harness.ExecOutput(t, ctx, client, "sh", "-c", cacheLeaseFiles, d.socket))
	for _, file := range d.files {
		if slices.Contains(left, file) {
			t.Fatalf("Restore left %s beside %s: %q", file, d.socket, left)
		}
	}
	if len(left) != 0 {
		t.Fatalf("immutable object cache files beside %s after Restore: %q", d.socket, left)
	}
}

// procStatState returns the state and start time fields of a
// /proc/<pid>/stat line; the command name before them may hold spaces and
// parentheses.
func procStatState(stat string) (state, start string, ok bool) {
	end := strings.LastIndexByte(stat, ')')
	if end < 0 {
		return "", "", false
	}
	fields := strings.Fields(stat[end+1:])
	if len(fields) < 20 {
		return "", "", false
	}
	return fields[0], fields[19], true
}
