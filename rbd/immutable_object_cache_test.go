package rbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
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
	// after the operation ran. statusDrop leaves that field out of the status
	// report.
	startExit  int
	noSocket   bool
	execFail   string
	statusDrop string
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
	if len(args) > 11 && args[2] == supervised.ShellScript && args[11] == "reserve" {
		line := supervised.ShellEndPrefix + "0\n"
		header := binary.BigEndian.AppendUint32([]byte{2, 0, 0, 0}, uint32(len(line)))
		return 0, bytes.NewReader(append(header, line...)), nil
	}
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
			out = append(out, "path=empty", "identity=original-dir")
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
			out = append(out, "dir=1", fmt.Sprintf("files=%d", f.objects[a[3]][0]), fmt.Sprintf("bytes=%d", f.objects[a[3]][1]))
		} else {
			out = append(out, "dir=0")
		}
		out = slices.DeleteFunc(out, func(line string) bool { return f.statusDrop != "" && strings.HasPrefix(line, f.statusDrop+"=") })
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
	if config := lease.Config(); config.Path != DefaultImmutableObjectCachePath || config.Socket != DefaultImmutableObjectCacheSocket || config.MaxSize != 1<<30 {
		t.Fatalf("defaults %+v", config)
	}
	if options := lease.ClientOptions(); options["rbd_plugins"] != "parent_cache" || options["immutable_object_cache_sock"] != DefaultImmutableObjectCacheSocket ||
		options["rbd_parent_cache_enabled"] != "true" {
		t.Fatalf("client options %v", options)
	}
	client.read(DefaultImmutableObjectCachePath, 16)
	client.read(DefaultImmutableObjectCachePath, 16)
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

// The lease acts on the path and socket it started with: an edited copy of
// Config redirects neither ClientOptions, Status nor Restore, Restore stops
// the original daemon, and an unrelated directory and socket stay.
func TestImmutableObjectCacheEditedConfigRedirectsNothing(t *testing.T) {
	client := newFakeCacheClient()
	client.files["/other"], client.objects["/other"] = "dir", [2]int64{2, 8}
	client.files["/other.sock"] = "socket"
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err != nil {
		t.Fatal(err)
	}
	client.read(DefaultImmutableObjectCachePath, 3)
	config := lease.Config()
	config.Path, config.Socket = "/other", "/other.sock"
	if lease.Config().Path != DefaultImmutableObjectCachePath || lease.ClientOptions()["immutable_object_cache_sock"] != DefaultImmutableObjectCacheSocket {
		t.Fatalf("an edited copy changed the lease: %+v, %v", lease.Config(), lease.ClientOptions())
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Running || status.CachedObjects != 3 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if client.running() != 0 {
		t.Fatal("Restore left the original daemon running")
	}
	if client.files["/other"] != "dir" || client.objects["/other"] != [2]int64{2, 8} || client.files["/other.sock"] != "socket" {
		t.Fatalf("Restore changed unrelated files: %v, %v", client.files, client.objects)
	}
	if client.files[DefaultImmutableObjectCachePath] != "" || client.files[DefaultImmutableObjectCacheSocket] != "" {
		t.Fatalf("Restore left the lease's files: %v", client.files)
	}
}

// A Restore whose stop or remove exec fails keeps the lease, and a retry
// finishes the same cleanup on the original path and socket.
func TestImmutableObjectCacheRestoreRetriesAfterAFailedStep(t *testing.T) {
	for _, step := range []string{"stop", "remove"} {
		t.Run(step, func(t *testing.T) {
			client := newFakeCacheClient()
			client.files["/other"], client.objects["/other"] = "dir", [2]int64{1, 1}
			lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
			if err != nil {
				t.Fatal(err)
			}
			client.read(DefaultImmutableObjectCachePath, 2)
			client.execFail = step
			if err := lease.Restore(t.Context()); err == nil || lease.complete {
				t.Fatalf("a failed %s: %v, complete %v", step, err, lease.complete)
			}
			if err := lease.Restore(t.Context()); err != nil || !lease.complete {
				t.Fatalf("retry: %v, complete %v", err, lease.complete)
			}
			if client.running() != 0 || len(client.files) != 2 || client.files["/other"] != "dir" || client.objects["/other"] != [2]int64{1, 1} ||
				client.objects[DefaultImmutableObjectCachePath] != [2]int64{} {
				t.Fatalf("after the retry: %d daemons, files %v, objects %v", client.running(), client.files, client.objects)
			}
		})
	}
}

func cacheOutput(op string, lines ...string) immutableObjectCacheOutput {
	return immutableObjectCacheOutput{op: op, out: []byte(strings.Join(lines, "\n") + "\n")}
}

func TestImmutableObjectCacheStatusOutput(t *testing.T) {
	perf := `perf={"objecter": {"op_r": 0}}`
	for name, test := range map[string]struct {
		lines []string
		want  ImmutableObjectCacheStatus
	}{
		"zero values":    {[]string{"running=1", perf, "dir=1", "files=0", "bytes=0"}, ImmutableObjectCacheStatus{Running: true, PID: 7}},
		"no directory":   {[]string{"running=0", "dir=0"}, ImmutableObjectCacheStatus{PID: 7}},
		"stopped counts": {[]string{"running=0", "dir=1", "files=2", "bytes=9223372036854775807"}, ImmutableObjectCacheStatus{PID: 7, CachedObjects: 2, CachedBytes: 1<<63 - 1}},
		"reads":          {[]string{"running=1", `perf={"objecter": {"op_r": 18446744073709551615}}`, "dir=0"}, ImmutableObjectCacheStatus{Running: true, PID: 7, RADOSReads: 1<<64 - 1}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseImmutableObjectCacheStatus(cacheOutput("status", test.lines...), 7)
			if err != nil || got != test.want {
				t.Fatalf("%+v, %v", got, err)
			}
		})
	}
	for name, test := range map[string]struct {
		lines []string
		want  string
	}{
		"no output":         {nil, "status output: running: the field is missing"},
		"missing files":     {[]string{"running=0", "dir=1", "bytes=0"}, "files: the field is missing"},
		"missing bytes":     {[]string{"running=0", "dir=1", "files=0"}, "bytes: the field is missing"},
		"missing dir":       {[]string{"running=0", "files=0", "bytes=0"}, "dir: the field is missing"},
		"missing perf":      {[]string{"running=1", "dir=0"}, "perf: the field is missing"},
		"empty files":       {[]string{"running=0", "dir=1", "files=", "bytes=0"}, `files: "" is not an unsigned decimal`},
		"malformed bytes":   {[]string{"running=0", "dir=1", "files=0", "bytes=1e+09"}, `bytes: "1e+09" is not an unsigned decimal`},
		"negative files":    {[]string{"running=0", "dir=1", "files=-1", "bytes=0"}, `files: "-1" is not an unsigned decimal`},
		"bytes overflow":    {[]string{"running=0", "dir=1", "files=0", "bytes=9223372036854775808"}, "bytes: 9223372036854775808 does not fit in 63 bits"},
		"files overflow":    {[]string{"running=0", "dir=1", "files=99999999999999999999", "bytes=0"}, "does not fit in"},
		"malformed running": {[]string{"running=yes", "dir=0"}, `running: "yes" is neither 0 nor 1`},
		"repeated field":    {[]string{"running=0", "dir=0", "dir=1"}, "dir: the field is repeated"},
		"unknown field":     {[]string{"running=0", "dir=0", "pid=1"}, "pid: the field is not one of"},
		"stray line":        {[]string{"running=0", "dir=0", "warning"}, `the line "warning" is not key=value`},
		"missing op_r":      {[]string{"running=1", `perf={"objecter": {}}`, "dir=0"}, "objecter.op_r is missing"},
		"malformed perf":    {[]string{"running=1", "perf=", "dir=0"}, "parse the immutable object cache counters"},
		"op_r overflow":     {[]string{"running=1", `perf={"objecter": {"op_r": 18446744073709551616}}`, "dir=0"}, "parse the immutable object cache counters"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseImmutableObjectCacheStatus(cacheOutput("status", test.lines...), 7)
			if err == nil || !strings.Contains(err.Error(), test.want) || errors.Is(err, ceph.ErrNativeFailure) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

func TestImmutableObjectCacheStartOutput(t *testing.T) {
	for name, test := range map[string]struct {
		lines []string
		want  immutableObjectCacheStart
	}{
		"ready":          {[]string{"exit=0", "pid=42", "ready=1"}, immutableObjectCacheStart{pid: 42, ready: true}},
		"no PID file":    {[]string{"exit=1", "pid=", "ready=0"}, immutableObjectCacheStart{exit: 1}},
		"highest exit":   {[]string{"exit=255", "pid=", "ready=0"}, immutableObjectCacheStart{exit: 255}},
		"never a socket": {[]string{"exit=0", "pid=42", "ready=0"}, immutableObjectCacheStart{pid: 42}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseImmutableObjectCacheStart(cacheOutput("start", test.lines...))
			if err != nil || got != test.want {
				t.Fatalf("%+v, %v", got, err)
			}
		})
	}
	for name, test := range map[string]struct {
		lines []string
		want  string
	}{
		"missing exit":  {[]string{"pid=1", "ready=1"}, "exit: the field is missing"},
		"missing pid":   {[]string{"exit=0", "ready=0"}, "pid: the field is missing"},
		"missing ready": {[]string{"exit=0", "pid=1"}, "ready: the field is missing"},
		"malformed pid": {[]string{"exit=0", "pid=12a", "ready=1"}, `pid: "12a" is not an unsigned decimal`},
		"exit overflow": {[]string{"exit=256", "pid=", "ready=0"}, "exit: 256 does not fit in 8 bits"},
		"pid overflow":  {[]string{"exit=0", "pid=99999999999999999999", "ready=1"}, "pid: 99999999999999999999 does not fit in"},
		"ready, no PID": {[]string{"exit=0", "pid=", "ready=1"}, "pid: a ready daemon has no PID"},
		"ready, PID 0":  {[]string{"exit=0", "pid=0", "ready=1"}, "pid: a ready daemon has no PID"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseImmutableObjectCacheStart(cacheOutput("start", test.lines...))
			if err == nil || !strings.Contains(err.Error(), "parse the immutable object cache start output: "+test.want) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

func TestImmutableObjectCacheInspectAndFlagOutput(t *testing.T) {
	if got, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", "binary=0", "plugin=1", "socket=0", "path=none")); err != nil ||
		got != (immutableObjectCacheInspection{plugin: true, path: "none"}) {
		t.Fatalf("inspection %+v, %v", got, err)
	}
	for name, lines := range map[string][]string{
		"missing plugin": {"binary=1", "socket=0", "path=none"},
		"unknown path":   {"binary=1", "plugin=1", "socket=0", "path=dir"},
		"malformed flag": {"binary=true", "plugin=1", "socket=0", "path=none"},
	} {
		if _, err := parseImmutableObjectCacheInspection(cacheOutput("inspect", lines...)); err == nil ||
			!strings.Contains(err.Error(), "parse the immutable object cache inspect output") {
			t.Fatalf("inspection %s: %v", name, err)
		}
	}
	if stopped, err := parseImmutableObjectCacheFlag(cacheOutput("stop", "stopped=0"), "stopped"); err != nil || stopped {
		t.Fatalf("stopped=0: %v, %v", stopped, err)
	}
	for _, out := range []immutableObjectCacheOutput{cacheOutput("stop"), cacheOutput("stop", "stopped=2"), cacheOutput("stop", "stopped=1", "stopped=1")} {
		if _, err := parseImmutableObjectCacheFlag(out, "stopped"); err == nil {
			t.Fatalf("%q parsed", out.out)
		}
	}
}

// A malformed status report fails Status with the parse context, and the
// lease still restores the cache.
func TestImmutableObjectCacheMalformedStatusKeepsTheLease(t *testing.T) {
	client := newFakeCacheClient()
	client.statusDrop = "files"
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Status(t.Context()); err == nil || !strings.Contains(err.Error(), "files: the field is missing") || errors.Is(err, ceph.ErrNativeFailure) {
		t.Fatalf("a status without files: %v", err)
	}
	if err := lease.Restore(t.Context()); err != nil || client.running() != 0 {
		t.Fatalf("Restore after a malformed status: %v, %d daemons", err, client.running())
	}
}
