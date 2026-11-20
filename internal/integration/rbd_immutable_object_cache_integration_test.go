//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"

	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
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
	for _, setting := range [][2]string{{"osd_op_history_size", "10000"}, {"osd_op_history_duration", "3600"}} {
		harness.MustCeph(t, ctx, cluster, "config", "set", "osd", setting[0], setting[1])
	}
	osds, err := cluster.OSDs(ctx)
	if err != nil || len(osds) != 1 {
		t.Fatalf("expected one serving OSD: %v, %v", osds, err)
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
	var parent struct {
		Prefix string `json:"block_name_prefix"`
	}
	if err := json.Unmarshal([]byte(rbdCLI("info", pool+"/parent", "--format", "json")), &parent); err != nil || parent.Prefix == "" {
		t.Fatalf("parent data object prefix: %+v, %v", parent, err)
	}
	objects := make(map[string]bool, 16)
	for i := range 16 {
		objects[fmt.Sprintf("%s.%016x", parent.Prefix, i)] = true
	}
	// Parent reads are judged by the OSD's replies, logged at debug_ms 1.
	// The OSD adds a finished operation to its op history only when the
	// last reference to the request is released, after the reply and in no
	// fixed order across PGs, so the history can lag a read that the
	// client already received. The messenger logs a reply before it sends
	// it, and the bytes of the log seen so far are no boundary either: lines
	// already submitted can reach the container log later. So the client
	// stats a new, absent marker object before and another after each read,
	// and the window is the log between the two markers' replies. Every
	// reply the client received before a marker is logged before that
	// marker's reply.
	level, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: fmt.Sprintf("osd.%d", osds[0].ID), Name: "debug_ms", Level: "1/5"})
	t.Cleanup(func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := level.Restore(restoreCtx); err != nil {
			t.Errorf("restore debug_ms of osd.%d: %v", osds[0].ID, err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	osdLog := func(probeCtx context.Context) ([]byte, error) {
		logs, err := osds[0].Logs(probeCtx)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(logs)
		return data, errors.Join(err, logs.Close())
	}
	markers := 0
	stat := func(marker string) {
		t.Helper()
		code, out, err := client.Exec(ctx, []string{"rados", "--id", id, "-p", pool, "stat", marker}, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		text, _ := io.ReadAll(out)
		if code == 0 || !strings.Contains(string(text), "No such file or directory") {
			t.Fatalf("stat of the absent marker %s exited %d: %s", marker, code, text)
		}
	}
	// parentReads runs read between an opening and a closing marker and
	// returns the parent data objects whose reads the OSD answered to the
	// client between the two markers' replies.
	parentReads := func(read func()) map[string]bool {
		t.Helper()
		markers++
		open, close := fmt.Sprintf("tc-reply-open-%d", markers), fmt.Sprintf("tc-reply-close-%d", markers)
		stat(open)
		read()
		stat(close)
		observed, err := harness.Observe(ctx, 30*time.Second, 10*time.Second, 100*time.Millisecond, func(probeCtx context.Context) (map[string]bool, bool, error) {
			log, err := osdLog(probeCtx)
			if err != nil {
				return nil, false, err
			}
			window, from, found, err := harness.OSDLogBetweenMarkers(log, open, close)
			if err != nil || !found {
				return nil, false, err
			}
			replied, err := immutableCacheParentReplies(window, from, objects)
			return replied, err == nil, err
		})
		if err != nil {
			t.Fatalf("OSD replies between markers %s and %s: %v", open, close, err)
		}
		return observed.Value
	}
	var want string
	if err := immutableCacheReadCount(parentReads(func() { want = checksum("parent@base", nil) }), 16); err != nil {
		t.Fatal(err)
	}
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
	hit := parentReads(func() {
		if got := checksum("child", lease.ClientOptions()); got != want {
			t.Fatalf("the second read has checksum %s", got)
		}
	})
	if err := immutableCacheReadCount(hit, 0); err != nil {
		t.Fatal(err)
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
	fallback := parentReads(func() {
		if got := checksum("child", lease.ClientOptions()); got != want {
			t.Fatalf("the read without the daemon has checksum %s", got)
		}
	})
	if err := immutableCacheReadCount(fallback, 16); err != nil {
		t.Fatal(err)
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
	// Keep the socket on a separate mount so the lease must support the
	// rename operations on both its socket and cache filesystems.
	const existing, existingSocket = "/var/cache/tc-ioc-existing", "/dev/shm/tc-ioc-existing/cache.sock"
	harness.ExecCommand(t, ctx, client, "mkdir", existing)
	cacheDevice := harness.ExecOutput(t, ctx, client, "stat", "-c", "%d", existing)
	socketDevice := harness.ExecOutput(t, ctx, client, "stat", "-c", "%d", "/dev/shm")
	if cacheDevice == socketDevice {
		t.Fatalf("cache directory and socket share device %s", cacheDevice)
	}
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
	harness.ExecCommand(t, ctx, client, "python3", "-c", `import os,sys; p=sys.argv[1]; assert not os.path.islink(p) and os.path.isdir(p); assert not os.listdir(p)`, existing)
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
	if pidFile := harness.ExecOutput(t, ctx, client, "python3", "-c", cacheLeaseFiles, socket, "pid"); pidFile != pidText {
		t.Fatalf("the daemon's PID files hold %q; the process holding %s is %d", pidFile, socket, pid)
	}
	files := strings.Fields(harness.ExecOutput(t, ctx, client, "python3", "-c", cacheLeaseFiles, socket))
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
	stat := harness.ExecOutput(t, ctx, client, "python3", "-c", readProcessStat, strconv.Itoa(d.pid))
	if stat == "absent" {
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
	left := strings.Fields(harness.ExecOutput(t, ctx, client, "python3", "-c", cacheLeaseFiles, d.socket))
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
