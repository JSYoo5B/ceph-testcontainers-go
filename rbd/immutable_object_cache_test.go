package rbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// fakeCacheDaemon is a ceph-immutable-object-cache process.
type fakeCacheDaemon struct {
	pid     int
	sock    string
	running bool
	reads   uint64
}

// fakeCacheClient models the client container's files and processes that the
// immutable object cache operations read and change: each path's kind, the
// cache directory's objects and the daemons it started.
type fakeCacheClient struct {
	testcontainers.Container
	mu      sync.Mutex
	binary  bool
	plugin  bool
	files   map[string]string // path -> "dir", "file" or "socket"
	pids    map[string]int    // PID file -> its PID
	objects map[string][2]int64
	daemons []*fakeCacheDaemon
	nextPID int
	// startExit is the daemonizing parent's status; noSocket keeps the daemon
	// from opening its socket. execFail fails the exec of that operation
	// after the operation ran.
	startExit int
	noSocket  bool
	execFail  string
}

func newFakeCacheClient() *fakeCacheClient {
	return &fakeCacheClient{binary: true, plugin: true, files: map[string]string{"/var/run/ceph": "dir"}, objects: map[string][2]int64{}, pids: map[string]int{}, nextPID: 100}
}

// daemon is the running process that the script calls ours: the PID, or
// the PID file's when the PID is 0, with the socket on its command line.
func (f *fakeCacheClient) daemon(pid, sock, pidfile string) *fakeCacheDaemon {
	if pid == "0" {
		pid = fmt.Sprint(f.pids[pidfile])
	}
	for _, d := range f.daemons {
		if fmt.Sprint(d.pid) == pid && d.sock == sock && d.running {
			return d
		}
	}
	return nil
}

func (f *fakeCacheClient) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(args) < 5 || args[0] != "sh" || args[2] != immutableObjectCacheScript {
		return clustertest.Reply(args, 2, nil)
	}
	op, a := args[4], args[5:]
	var out []string
	defer func() {
		if op == f.execFail {
			f.execFail = ""
		}
	}()
	switch op {
	case "inspect":
		out = append(out, "binary="+bit(f.binary), "plugin="+bit(f.plugin), "socket="+bit(f.files[a[1]] != ""))
		switch kind := f.files[a[0]]; {
		case kind == "":
			out = append(out, "path=none")
		case kind != "dir":
			out = append(out, "path=other")
		case f.objects[a[0]][0] > 0:
			out = append(out, "path=full")
		default:
			out = append(out, "path=empty")
		}
	case "start":
		path, sock, pidfile := a[1], a[2], a[5]
		f.files[path] = "dir"
		out = append(out, fmt.Sprintf("exit=%d", f.startExit))
		if f.startExit != 0 {
			out = append(out, "pid=", "ready=0")
			break
		}
		f.nextPID++
		d := &fakeCacheDaemon{pid: f.nextPID, sock: sock, running: true}
		f.daemons = append(f.daemons, d)
		f.files[pidfile], f.pids[pidfile] = "file", d.pid
		f.files[a[4]] = "socket"
		ready := "0"
		if !f.noSocket {
			f.files[sock], ready = "socket", "1"
		}
		out = append(out, fmt.Sprintf("pid=%d", d.pid), "ready="+ready)
	case "status":
		if d := f.daemon(a[0], a[1], a[4]); d != nil {
			out = append(out, "running=1", fmt.Sprintf(`perf={"objecter": {"op_r": %d}}`, d.reads))
		} else {
			out = append(out, "running=0")
		}
		if f.files[a[3]] == "dir" {
			out = append(out, fmt.Sprintf("files=%d", f.objects[a[3]][0]), fmt.Sprintf("bytes=%d", f.objects[a[3]][1]))
		}
	case "stop":
		if d := f.daemon(a[0], a[1], a[2]); d != nil {
			// TERM, then KILL; the daemon leaves its socket file behind.
			d.running = false
		}
		out = append(out, "stopped=1")
	case "remove":
		path, sock, asok, pidfile, created := a[0], a[1], a[2], a[3], a[4]
		delete(f.files, sock)
		delete(f.files, asok)
		delete(f.files, pidfile)
		delete(f.pids, pidfile)
		delete(f.objects, path)
		if created == "1" {
			delete(f.files, path)
		}
		out = append(out, "removed=1")
	}
	if op == f.execFail {
		return 0, nil, errors.New("injected exec failure")
	}
	return clustertest.Reply(args, 0, []byte(strings.Join(out, "\n")+"\n"))
}

// read is a librbd client that reads n parent objects through the cache:
// the daemon promotes each object once.
func (f *fakeCacheClient) read(path string, n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.daemons {
		if d.running && f.files[d.sock] == "socket" {
			if cached := f.objects[path]; cached[0] < n {
				d.reads += uint64(n - cached[0])
				f.objects[path] = [2]int64{n, n << 22}
			}
		}
	}
}

func (f *fakeCacheClient) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, d := range f.daemons {
		if d.running {
			count++
		}
	}
	return count
}

func bit(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func TestImmutableObjectCacheLifecycle(t *testing.T) {
	client := newFakeCacheClient()
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{ID: "rbd-client"})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Config.Path != DefaultImmutableObjectCachePath || lease.Config.Socket != DefaultImmutableObjectCacheSocket || lease.Config.MaxSize != 1<<30 {
		t.Fatalf("defaults %+v", lease.Config)
	}
	if options := lease.ClientOptions(); options["rbd_plugins"] != "parent_cache" || options["immutable_object_cache_sock"] != DefaultImmutableObjectCacheSocket ||
		options["rbd_parent_cache_enabled"] != "true" {
		t.Fatalf("client options %v", options)
	}
	client.read(lease.Config.Path, 16)
	client.read(lease.Config.Path, 16)
	status, err := lease.Status(t.Context())
	if err != nil || !status.Running || status.CachedObjects != 16 || status.CachedBytes != 16<<22 || status.RADOSReads != 16 || status.PID == 0 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status, err := lease.Status(t.Context()); err != nil || status.Running || status.CachedObjects != 16 || status.RADOSReads != 0 {
		t.Fatalf("status after Stop %+v, %v", status, err)
	}
	if client.files[DefaultImmutableObjectCacheSocket] != "socket" {
		t.Fatal("Stop removed the socket file")
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(client.files) != 1 || len(client.objects) != 0 {
		t.Fatalf("Restore left %v and %v", client.files, client.objects)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("repeated Restore: %v", err)
	}
	if _, err := lease.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Status after Restore: %v", err)
	}
}

func TestImmutableObjectCacheRestoreStopsAndKeepsAnExistingDirectory(t *testing.T) {
	client := newFakeCacheClient()
	client.files["/cache"] = "dir"
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{Path: "/cache", Socket: "/run/ioc/sock", MaxSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client.read("/cache", 3)
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if client.running() != 0 || client.files["/cache"] != "dir" || client.objects["/cache"][0] != 0 || client.files["/run/ioc/sock"] != "" {
		t.Fatalf("after Restore: %d daemons, files %v, objects %v", client.running(), client.files, client.objects)
	}
}

func TestImmutableObjectCacheDaemonThatExited(t *testing.T) {
	client := newFakeCacheClient()
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	client.daemons[0].running = false
	// A new process with another socket reuses nothing of the lease's.
	client.daemons = append(client.daemons, &fakeCacheDaemon{pid: client.daemons[0].pid, sock: "/other.sock", running: true})
	client.mu.Unlock()
	if status, err := lease.Status(t.Context()); err != nil || status.Running {
		t.Fatalf("status of an exited daemon %+v, %v", status, err)
	}
	if err := lease.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if client.running() != 1 || !client.daemons[1].running {
		t.Fatal("Restore stopped a process that is not the lease's")
	}
}

func TestImmutableObjectCacheStartFailuresLeaveNothing(t *testing.T) {
	for name, fault := range map[string]func(*fakeCacheClient){
		"daemon exits": func(f *fakeCacheClient) { f.startExit = 1 },
		"no socket":    func(f *fakeCacheClient) { f.noSocket = true },
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeCacheClient()
			fault(client)
			lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
			if !errors.Is(err, ceph.ErrNativeFailure) || lease != nil {
				t.Fatalf("start with %s: %v, %v", name, lease, err)
			}
			if client.running() != 0 || len(client.files) != 1 {
				t.Fatalf("after a failed start: %d daemons, files %v", client.running(), client.files)
			}
		})
	}
}

func TestImmutableObjectCacheRefusals(t *testing.T) {
	for name, test := range map[string]struct {
		setup  func(*fakeCacheClient)
		config ImmutableObjectCache
		kind   error
	}{
		"no daemon":           {func(f *fakeCacheClient) { f.binary = false }, ImmutableObjectCache{}, ceph.ErrImageCapability},
		"no plugin":           {func(f *fakeCacheClient) { f.plugin = false }, ImmutableObjectCache{}, ceph.ErrImageCapability},
		"socket exists":       {func(f *fakeCacheClient) { f.files[DefaultImmutableObjectCacheSocket] = "socket" }, ImmutableObjectCache{}, ceph.ErrInvalidRequest},
		"full cache path":     {func(f *fakeCacheClient) { f.files["/c"], f.objects["/c"] = "dir", [2]int64{1, 1} }, ImmutableObjectCache{Path: "/c"}, ceph.ErrInvalidRequest},
		"cache path a file":   {func(f *fakeCacheClient) { f.files["/c"] = "file" }, ImmutableObjectCache{Path: "/c"}, ceph.ErrInvalidRequest},
		"relative path":       {nil, ImmutableObjectCache{Path: "cache"}, ceph.ErrInvalidRequest},
		"root path":           {nil, ImmutableObjectCache{Path: "/"}, ceph.ErrInvalidRequest},
		"unclean socket":      {nil, ImmutableObjectCache{Socket: "/run/../sock"}, ceph.ErrInvalidRequest},
		"socket in the cache": {nil, ImmutableObjectCache{Path: "/c", Socket: "/c/sock"}, ceph.ErrInvalidRequest},
		"cache in the socket": {nil, ImmutableObjectCache{Path: "/s/c", Socket: "/s"}, ceph.ErrInvalidRequest},
		"bad ID":              {nil, ImmutableObjectCache{ID: "a b"}, ceph.ErrInvalidRequest},
		"option-like ID":      {nil, ImmutableObjectCache{ID: "-x"}, ceph.ErrInvalidRequest},
		"negative size":       {nil, ImmutableObjectCache{MaxSize: -1}, ceph.ErrInvalidRequest},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeCacheClient()
			if test.setup != nil {
				test.setup(client)
			}
			before := fmt.Sprint(client.files, client.objects)
			lease, err := StartImmutableObjectCache(t.Context(), client, test.config)
			if !errors.Is(err, test.kind) || lease != nil {
				t.Fatalf("%v, %v", lease, err)
			}
			if after := fmt.Sprint(client.files, client.objects); after != before || client.running() != 0 {
				t.Fatalf("a refusal changed %s to %s", before, after)
			}
		})
	}
	if _, err := StartImmutableObjectCache(t.Context(), nil, ImmutableObjectCache{}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil client: %v", err)
	}
}

// An exec failure after the daemon started leaves the lease without its
// PID; Restore finds the daemon by its PID file and stops it.
func TestImmutableObjectCacheUnconfirmedStartKeepsTheLease(t *testing.T) {
	client := newFakeCacheClient()
	client.execFail = "start"
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err == nil || lease == nil || client.running() != 1 {
		t.Fatalf("an exec failure of start: %v, %v, %d daemons", lease, err, client.running())
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Running {
		t.Fatalf("status of the unconfirmed daemon %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if client.running() != 0 || len(client.files) != 1 {
		t.Fatalf("after Restore: %d daemons, files %v", client.running(), client.files)
	}
}
