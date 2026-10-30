package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Defaults of ImmutableObjectCache.
const (
	DefaultImmutableObjectCachePath    = "/var/cache/ceph/immutable-object-cache"
	DefaultImmutableObjectCacheSocket  = "/var/run/ceph/immutable-object-cache.sock"
	DefaultImmutableObjectCacheMaxSize = 1 << 30
)

// ImmutableObjectCache configures the RBD immutable object cache daemon,
// ceph-immutable-object-cache, that StartImmutableObjectCache starts in a
// client container. The daemon caches whole parent-image objects of clones
// for the librbd clients in that container that enable the parent cache.
type ImmutableObjectCache struct {
	// ID is the cephx ID, client.<ID>, that the daemon uses to read the
	// parent objects; empty means admin. The container's ceph.conf must name
	// its keyring, as ceph.ClientConfig.WithIdentity does.
	ID string
	// Path is the cache directory; empty means
	// DefaultImmutableObjectCachePath. It must be absent or empty.
	Path string
	// Socket is the path of the daemon's client socket; empty means
	// DefaultImmutableObjectCacheSocket. It must not exist, and it must not
	// be under Path.
	Socket string
	// MaxSize is immutable_object_cache_max_size in bytes; 0 means
	// DefaultImmutableObjectCacheMaxSize. The daemon treats it as a target,
	// not a hard limit.
	MaxSize int64
}

// ImmutableObjectCacheStatus is what the client container reports for the
// daemon and its cache directory.
type ImmutableObjectCacheStatus struct {
	// Running means that the daemon the lease started still runs.
	Running bool
	// PID is the daemon's process ID in the client container.
	PID int
	// CachedObjects and CachedBytes count the files under the cache
	// directory: one per cached parent object.
	CachedObjects int
	CachedBytes   int64
	// RADOSReads is the daemon's objecter read count while Running: the
	// parent objects it read from RADOS to cache them. The daemon has no
	// cache hit counters of its own.
	RADOSReads uint64
}

// ImmutableObjectCacheLease owns one daemon that StartImmutableObjectCache
// started, with its socket and cache directory.
type ImmutableObjectCacheLease struct {
	// Config is the request with its defaults filled in.
	Config ImmutableObjectCache

	mu          sync.Mutex
	client      testcontainers.Container
	adminSocket string
	pidFile     string
	pid         int
	// createdPath means that the cache directory did not exist before.
	createdPath bool
	complete    bool
}

var immutableObjectCacheID = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// immutableObjectCacheScript runs one operation in the client container and
// reports its result as key=value lines; it exits non-zero only when the
// container cannot run it. A process is the lease's daemon only while its
// command line holds the lease's socket, so a reused PID is never signaled.
const immutableObjectCacheScript = `set -u
ours() {
    test -n "$1" && test -r "/proc/$1/cmdline" &&
        tr '\0' '\n' < "/proc/$1/cmdline" | grep -qxF -- "--immutable_object_cache_sock=$2"
}
op=$1
shift
case "$op" in
inspect)
    path=$1 sock=$2
    if command -v ceph-immutable-object-cache >/dev/null 2>&1; then echo binary=1; else echo binary=0; fi
    dir=$(ceph-conf --show-config-value plugin_dir 2>/dev/null)
    if test -n "$dir" && test -f "$dir/librbd/libceph_librbd_parent_cache.so"; then echo plugin=1; else echo plugin=0; fi
    if test -e "$sock" || test -L "$sock"; then echo socket=1; else echo socket=0; fi
    if test -L "$path"; then echo path=other
    elif test -d "$path"; then
        if test -z "$(ls -A "$path")"; then echo path=empty; else echo path=full; fi
    elif test -e "$path"; then echo path=other
    else echo path=none
    fi
    ;;
start)
    id=$1 path=$2 sock=$3 size=$4 asok=$5 pidfile=$6
    mkdir -p "$path" "$(dirname "$sock")" "$(dirname "$asok")" "$(dirname "$pidfile")" || exit 1
    ceph-immutable-object-cache --id "$id" --immutable_object_cache_path="$path" \
        --immutable_object_cache_sock="$sock" --immutable_object_cache_max_size="$size" \
        --admin-socket="$asok" --pid-file="$pidfile" >/dev/null 2>&1
    echo "exit=$?"
    tries=0
    while test "$tries" -lt 150; do
        if test -S "$sock" && test -s "$pidfile"; then break; fi
        sleep 0.2
        tries=$((tries + 1))
    done
    echo "pid=$(cat "$pidfile" 2>/dev/null)"
    if test -S "$sock" && ours "$(cat "$pidfile" 2>/dev/null)" "$sock"; then echo ready=1; else echo ready=0; fi
    ;;
status)
    pid=$1 sock=$2 asok=$3 path=$4 pidfile=$5
    test "$pid" -gt 0 || pid=$(cat "$pidfile" 2>/dev/null)
    if ours "$pid" "$sock"; then
        echo running=1
        echo "perf=$(ceph --admin-daemon "$asok" perf dump objecter 2>/dev/null | tr -d '\n')"
    else
        echo running=0
    fi
    if test -d "$path"; then
        find "$path" -type f -printf '%s\n' | awk '{ n++; b += $1 } END { print "files=" n + 0; print "bytes=" b + 0 }'
    fi
    ;;
stop)
    pid=$1 sock=$2 pidfile=$3
    test "$pid" -gt 0 || pid=$(cat "$pidfile" 2>/dev/null)
    for signal in TERM KILL; do
        ours "$pid" "$sock" || break
        kill -s "$signal" "$pid" 2>/dev/null
        tries=0
        while ours "$pid" "$sock" && test "$tries" -lt 50; do
            sleep 0.2
            tries=$((tries + 1))
        done
    done
    if ours "$pid" "$sock"; then echo stopped=0; else echo stopped=1; fi
    ;;
remove)
    path=$1 sock=$2 asok=$3 pidfile=$4 created=$5
    rm -f "$sock" "$asok" "$pidfile" || exit 1
    if test "$created" = 1; then rm -rf "$path" || exit 1
    elif test -d "$path"; then find "$path" -mindepth 1 -delete || exit 1
    fi
    echo removed=1
    ;;
*)
    exit 2
    ;;
esac
`

// StartImmutableObjectCache starts the RBD immutable object cache daemon
// in client, a container that the caller owns, and returns its lease. A
// librbd client in the same container uses the cache when it sets the
// lease's ClientOptions; other clients read the parents from RADOS. The
// container must have ceph-immutable-object-cache on its PATH and the librbd
// parent_cache plugin in its plugin directory: the official Ceph image has
// both, and an image without them is refused with ceph.ErrImageCapability.
// An existing socket or a non-empty cache directory is refused with
// ceph.ErrInvalidRequest, and nothing is changed.
func StartImmutableObjectCache(ctx context.Context, client testcontainers.Container, config ImmutableObjectCache) (*ImmutableObjectCacheLease, error) {
	config, err := normalizeImmutableObjectCache(client, config)
	if err != nil {
		return nil, err
	}
	state, err := immutableObjectCacheOp(ctx, client, "inspect", config.Path, config.Socket)
	if err != nil {
		return nil, fmt.Errorf("inspect the immutable object cache container: %w", err)
	}
	switch {
	case state["binary"] != "1":
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "ceph-immutable-object-cache",
			Err: errors.New("the client container has no ceph-immutable-object-cache executable")}
	case state["plugin"] != "1":
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "librbd parent_cache plugin",
			Err: errors.New("the client container has no librbd parent_cache plugin")}
	case state["socket"] != "0":
		return nil, servicehook.Invalid(fmt.Errorf("the immutable object cache socket %s already exists", config.Socket))
	case state["path"] != "none" && state["path"] != "empty":
		return nil, servicehook.Invalid(fmt.Errorf("the immutable object cache path %s is not an absent or empty directory", config.Path))
	}
	token := uuid.NewString()[:8]
	lease := &ImmutableObjectCacheLease{
		Config:      config,
		client:      client,
		adminSocket: path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".asok"),
		pidFile:     path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".pid"),
		createdPath: state["path"] == "none",
	}
	started, err := immutableObjectCacheOp(ctx, client, "start", config.ID, config.Path, config.Socket,
		strconv.FormatInt(config.MaxSize, 10), lease.adminSocket, lease.pidFile)
	if err != nil {
		// The daemon may run; the lease can still stop and remove it.
		return lease, fmt.Errorf("start the immutable object cache: %w", err)
	}
	lease.pid, _ = strconv.Atoi(started["pid"])
	if started["ready"] == "1" {
		return lease, nil
	}
	cause := errors.New("the daemon did not open its socket")
	if code := started["exit"]; code != "0" {
		cause = fmt.Errorf("the daemon exited with status %s", code)
	}
	failure := &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "start the immutable object cache", Err: cause}
	if err := lease.restore(context.WithoutCancel(ctx)); err != nil {
		return lease, errors.Join(failure, err)
	}
	return nil, failure
}

func normalizeImmutableObjectCache(client testcontainers.Container, config ImmutableObjectCache) (ImmutableObjectCache, error) {
	if client == nil {
		return config, servicehook.Invalid(errors.New("the immutable object cache requires a client container"))
	}
	if config.ID == "" {
		config.ID = "admin"
	}
	if config.Path == "" {
		config.Path = DefaultImmutableObjectCachePath
	}
	if config.Socket == "" {
		config.Socket = DefaultImmutableObjectCacheSocket
	}
	if config.MaxSize == 0 {
		config.MaxSize = DefaultImmutableObjectCacheMaxSize
	}
	switch {
	case !immutableObjectCacheID.MatchString(config.ID) || strings.HasPrefix(config.ID, "-"):
		return config, servicehook.Invalid(fmt.Errorf("invalid cephx ID %q", config.ID))
	case !cleanAbsolute(config.Path) || config.Path == "/":
		return config, servicehook.Invalid(fmt.Errorf("the cache path %q is not a clean absolute path below /", config.Path))
	case !cleanAbsolute(config.Socket) || config.Socket == "/":
		return config, servicehook.Invalid(fmt.Errorf("the socket %q is not a clean absolute path below /", config.Socket))
	case config.Socket == config.Path || strings.HasPrefix(config.Socket, config.Path+"/") || strings.HasPrefix(config.Path, config.Socket+"/"):
		return config, servicehook.Invalid(errors.New("the socket and the cache path must not contain each other"))
	case config.MaxSize < 0:
		return config, servicehook.Invalid(fmt.Errorf("negative cache size %d", config.MaxSize))
	}
	return config, nil
}

func cleanAbsolute(p string) bool {
	return strings.HasPrefix(p, "/") && path.Clean(p) == p && !strings.ContainsAny(p, "\x00\r\n")
}

// ClientOptions returns the Ceph options that make a librbd client in the
// lease's container use the cache, for a ceph.conf section, the rbd CLI
// (as --name=value) or rados.Rados(conf=...).
func (l *ImmutableObjectCacheLease) ClientOptions() map[string]string {
	return map[string]string{
		"rbd_parent_cache_enabled":    "true",
		"rbd_plugins":                 "parent_cache",
		"immutable_object_cache_sock": l.Config.Socket,
	}
}

// Status reports the daemon and the cache directory.
func (l *ImmutableObjectCacheLease) Status(ctx context.Context) (ImmutableObjectCacheStatus, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.complete {
		return ImmutableObjectCacheStatus{}, servicehook.Invalid(errors.New("the immutable object cache lease was restored"))
	}
	return l.status(ctx)
}

func (l *ImmutableObjectCacheLease) status(ctx context.Context) (ImmutableObjectCacheStatus, error) {
	values, err := immutableObjectCacheOp(ctx, l.client, "status", strconv.Itoa(l.pid), l.Config.Socket, l.adminSocket, l.Config.Path, l.pidFile)
	if err != nil {
		return ImmutableObjectCacheStatus{}, fmt.Errorf("read the immutable object cache: %w", err)
	}
	status := ImmutableObjectCacheStatus{Running: values["running"] == "1", PID: l.pid}
	status.CachedObjects, _ = strconv.Atoi(values["files"])
	status.CachedBytes, _ = strconv.ParseInt(values["bytes"], 10, 64)
	if status.Running {
		var perf struct {
			Objecter struct {
				OpR uint64 `json:"op_r"`
			} `json:"objecter"`
		}
		if err := json.Unmarshal([]byte(values["perf"]), &perf); err != nil {
			return status, fmt.Errorf("parse the immutable object cache counters: %w", err)
		}
		status.RADOSReads = perf.Objecter.OpR
	}
	return status, nil
}

// Stop stops the daemon, with SIGTERM and then SIGKILL, and keeps the cache
// directory and socket file: librbd clients that open an image afterwards
// fail to register with the cache and read the parents from RADOS. A daemon
// that already exited needs no stop.
func (l *ImmutableObjectCacheLease) Stop(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.complete {
		return nil
	}
	return l.stop(ctx)
}

// stop finds the daemon by its PID file when the start was not confirmed.
func (l *ImmutableObjectCacheLease) stop(ctx context.Context) error {
	values, err := immutableObjectCacheOp(ctx, l.client, "stop", strconv.Itoa(l.pid), l.Config.Socket, l.pidFile)
	if err != nil {
		return fmt.Errorf("stop the immutable object cache: %w", err)
	}
	if values["stopped"] != "1" {
		return &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "stop the immutable object cache",
			Err: errors.New("the daemon still runs after SIGKILL")}
	}
	return nil
}

// Restore stops the daemon if it runs and removes its socket, admin socket,
// PID file and cached objects. It removes the cache directory only when the
// lease created it. A repeated call after success does nothing.
func (l *ImmutableObjectCacheLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.restore(ctx)
}

func (l *ImmutableObjectCacheLease) restore(ctx context.Context) error {
	if l.complete {
		return nil
	}
	if err := l.stop(ctx); err != nil {
		return err
	}
	created := "0"
	if l.createdPath {
		created = "1"
	}
	values, err := immutableObjectCacheOp(ctx, l.client, "remove", l.Config.Path, l.Config.Socket, l.adminSocket, l.pidFile, created)
	if err != nil || values["removed"] != "1" {
		return errors.Join(errors.New("remove the immutable object cache files"), err)
	}
	l.complete = true
	return nil
}

func immutableObjectCacheOp(ctx context.Context, client testcontainers.Container, op string, args ...string) (map[string]string, error) {
	out, err := multicluster.Exec(ctx, client, append([]string{"sh", "-c", immutableObjectCacheScript, "sh", op}, args...)...)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			values[key] = value
		}
	}
	return values, nil
}
