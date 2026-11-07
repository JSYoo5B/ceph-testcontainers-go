package rbd

import (
	"context"
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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
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
// started, with its socket and cache directory. It signals the process only
// while its command line holds the lease's unique PID file, so a reused PID is never
// stopped. To count cache use, read Status.RADOSReads: rados df read
// counters lag behind and do not show it.
type ImmutableObjectCacheLease struct {
	// config is the request with its defaults filled in, fixed at the start:
	// Status, Stop, Restore and ClientOptions act on its path and socket.
	config ImmutableObjectCache

	mu          sync.Mutex
	client      testcontainers.Container
	adminSocket string
	pidFile     string
	pid         int
	// createdPath means that the cache directory did not exist before.
	createdPath bool
	stopped     bool
	complete    bool
}

var immutableObjectCacheID = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// immutableObjectCacheScript runs one operation in the client container and
// reports its result as key=value lines; it exits non-zero only when the
// container cannot run it. A process is the lease's daemon only while its
// command line holds the lease's PID file, so a reused PID is never signaled.
const immutableObjectCacheScript = `set -u
ours() {
    test -n "$1" && test -r "/proc/$1/cmdline" &&
        tr '\0' '\n' < "/proc/$1/cmdline" | grep -qxF -- "--pid-file=$2"
}
exists() { test -e "$1" || test -L "$1"; }
same_file_or_absent() {
    ! exists "$1" || { test ! -L "$1" && test "$1" -ef "$2"; }
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
        if test -z "$(ls -A "$path")"; then
            echo path=empty
            printf 'identity=' && stat -c '%d:%i:%w' "$path" || exit 1
        else echo path=full; fi
    elif test -e "$path"; then echo path=other
    else echo path=none
    fi
    ;;
start)
    id=$1 path=$2 sock=$3 size=$4 asok=$5 pidfile=$6
    state=$pidfile.lease marker=.${pidfile##*/}.owner
    # Refusals before reservation have no daemon or cached files to restore.
    if ! mkdir -p "$(dirname "$sock")" || ! mkdir -m 700 "$state"; then
        printf 'exit=1\npid=\nready=0\nunreserved=1\n'
        exit 0
    fi
    before_launch_failure() {
        touch "$state/not-launched" && echo 1 > "$state/exit" || exit 1
        printf 'exit=1\npid=\nready=0\n'
        exit 0
    }
    if exists "$sock" || exists "$asok" || exists "$pidfile"; then before_launch_failure; fi
    if test "$7" = none; then
        mkdir -p "$(dirname "$path")" && mkdir "$path" || before_launch_failure
        touch "$state/created" || exit 1
    else
        test ! -L "$path" && test -d "$path" && test -z "$(ls -A "$path")" &&
            test "$(stat -c '%d:%i:%w' "$path")" = "$8" || before_launch_failure
    fi
    stat -c '%d:%i:%w' "$path" > "$state/owner" || before_launch_failure
    ceph-immutable-object-cache --id "$id" --immutable_object_cache_path="$path" \
        --immutable_object_cache_sock="$sock" --immutable_object_cache_max_size="$size" \
        --admin-socket="$asok" --pid-file="$pidfile" >/dev/null 2>&1
    exitcode=$?
    echo "exit=$exitcode"
    tries=0
    while test "$tries" -lt 150; do
        if test -S "$sock" && test -s "$pidfile"; then break; fi
        sleep 0.2
        tries=$((tries + 1))
    done
    test ! -L "$path" && test "$(stat -c '%d:%i:%w' "$path")" = "$(cat "$state/owner")" || exit 1
    # Older filesystems omit birth time, so an inode number can be reused.
    # The daemon clears its cache on startup: place the marker only afterwards.
    if test "$(stat -c '%w' "$path")" = -; then
        mkdir "$path/$marker" && stat -c '%d:%i' "$path/$marker" > "$state/marker" || exit 1
    fi
    # Pin the actual files, including their inodes if the daemon unlinks
    # them during shutdown. A later file at the same name belongs to its creator.
    for file in "$sock" "$asok" "$pidfile"; do
        if exists "$file"; then
            test ! -L "$file" && ln "$file" "$state/${file##*/}.pin" || exit 1
        fi
    done
    printf '%s\n' "$exitcode" > "$state/exit.tmp" && mv "$state/exit.tmp" "$state/exit" || exit 1
    echo "pid=$(cat "$pidfile" 2>/dev/null)"
    if test -S "$sock" && ours "$(cat "$pidfile" 2>/dev/null)" "$pidfile"; then echo ready=1; else echo ready=0; fi
    ;;
status)
    pid=$1 sock=$2 asok=$3 path=$4 pidfile=$5
    test "$pid" -gt 0 || pid=$(cat "$pidfile" 2>/dev/null)
    if ours "$pid" "$pidfile"; then
        echo running=1
        echo "perf=$(ceph --admin-daemon "$asok" perf dump objecter 2>/dev/null | tr -d '\n')"
    else
        echo running=0
    fi
    if test -d "$path"; then
        echo dir=1
        find "$path" -type f -printf '%s\n' | awk '{ n++; b += $1 } END { printf "files=%d\nbytes=%.0f\n", n, b }'
    else
        echo dir=0
    fi
    ;;
stop)
    pid=$1 sock=$2 pidfile=$3
    # A lost Exec reply can leave start still running before its daemon
    # writes a PID. Wait for a later retry rather than declaring it stopped.
    state=$pidfile.lease
    if ! test -f "$state/exit"; then echo stopped=0; exit 0; fi
    test "$pid" -gt 0 || pid=$(cat "$state/${pidfile##*/}.pin" 2>/dev/null)
    if test -z "$pid" && test "$(cat "$state/exit")" = 0; then
        # After the readiness budget the daemon can still publish its PID.
        # Stop only that process; cleanup refuses any unpinned late files.
        pid=$(cat "$pidfile" 2>/dev/null)
        if ! ours "$pid" "$pidfile"; then echo stopped=0; exit 0; fi
    fi
    for signal in TERM KILL; do
        ours "$pid" "$pidfile" || break
        kill -s "$signal" "$pid" 2>/dev/null
        tries=0
        while ours "$pid" "$pidfile" && test "$tries" -lt 50; do
            sleep 0.2
            tries=$((tries + 1))
        done
    done
    if ours "$pid" "$pidfile"; then echo stopped=0; else echo stopped=1; fi
    ;;
remove)
    path=$1 sock=$2 asok=$3 pidfile=$4 created=$5
    state=$pidfile.lease marker=.${pidfile##*/}.owner
    # stop confirmed this receipt. Its removal is the last cleanup step,
    # so a retry after a lost remove reply has nothing left to unlink.
    if ! exists "$state"; then
        for file in "$sock" "$asok" "$pidfile"; do
            if exists "$file"; then echo removed=0; exit 0; fi
        done
        if test "$created" = 1; then
            if exists "$path"; then echo removed=0; exit 0; fi
        elif test -L "$path" || ! test -d "$path" || test -n "$(ls -A "$path")"; then
            echo removed=0; exit 0
        fi
        echo removed=1; exit 0
    fi
    if test -f "$state/not-launched"; then
        if test -f "$state/created" && exists "$path"; then
            test ! -L "$path" && test "$(stat -c '%d:%i:%w' "$path")" = "$(cat "$state/owner")" &&
                rmdir "$path" || { echo removed=0; exit 0; }
        fi
        rm -rf "$state" || exit 1
        echo removed=1; exit 0
    fi
    for file in "$sock" "$asok" "$pidfile"; do
        same_file_or_absent "$file" "$state/${file##*/}.pin" || { echo removed=0; exit 0; }
    done
    if exists "$path"; then
        test ! -L "$path" && cd -P "$path" || { echo removed=0; exit 0; }
        test "$(stat -c '%d:%i:%w' .)" = "$(cat "$state/owner")" || { echo removed=0; exit 0; }
        if ! test -f "$state/emptied"; then
            if test -f "$state/marker"; then
                test ! -L "$marker" && test -d "$marker" &&
                    test "$(stat -c '%d:%i' "$marker")" = "$(cat "$state/marker")" || { echo removed=0; exit 0; }
            fi
            # Relative traversal stays in the opened directory across renames.
            find . -mindepth 1 ! -path "./$marker" -delete || exit 1
            touch "$state/emptied" || exit 1
        fi
        if test -f "$state/marker" && test -d "$marker"; then rmdir "$marker" || exit 1; fi
        if test "$created" = 1; then
            test . -ef "$path" && rmdir "$path" || exit 1
        fi
    elif ! test -f "$state/emptied"; then
        echo removed=0; exit 0
    fi
    for file in "$sock" "$asok" "$pidfile"; do
        same_file_or_absent "$file" "$state/${file##*/}.pin" && rm -f "$file" || exit 1
    done
    rm -rf "$state" || exit 1
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
// An existing socket, a non-empty cache directory or an invalid path is
// refused with ceph.ErrInvalidRequest, and nothing is changed. A daemon that
// exits or never opens its socket fails the call with ceph.ErrNativeFailure,
// and startup attempts to restore the prior state. A lost start reply or a
// failed cleanup returns a non-nil lease with the error. Keep that lease and
// call Restore with a fresh bounded context; a cleanup error needs attention.
//
//purpose:construction,condition
func StartImmutableObjectCache(ctx context.Context, client testcontainers.Container, config ImmutableObjectCache) (*ImmutableObjectCacheLease, error) {
	config, err := normalizeImmutableObjectCache(client, config)
	if err != nil {
		return nil, err
	}
	state, err := inspectImmutableObjectCache(ctx, client, config)
	if err != nil {
		return nil, fmt.Errorf("inspect the immutable object cache container: %w", err)
	}
	switch {
	case !state.binary:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "ceph-immutable-object-cache",
			Err: errors.New("the client container has no ceph-immutable-object-cache executable")}
	case !state.plugin:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "librbd parent_cache plugin",
			Err: errors.New("the client container has no librbd parent_cache plugin")}
	case state.socket:
		return nil, servicehook.Invalid(fmt.Errorf("the immutable object cache socket %s already exists", config.Socket))
	case state.path != "none" && state.path != "empty":
		return nil, servicehook.Invalid(fmt.Errorf("the immutable object cache path %s is not an absent or empty directory", config.Path))
	}
	token := uuid.NewString()[:8]
	lease := &ImmutableObjectCacheLease{
		config:      config,
		client:      client,
		adminSocket: path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".asok"),
		pidFile:     path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".pid"),
		createdPath: state.path == "none",
	}
	started, err := lease.start(ctx, state)
	if err != nil {
		// The daemon may run; the lease can still stop and remove it, and
		// finds it by its PID file.
		return lease, fmt.Errorf("start the immutable object cache: %w", err)
	}
	lease.pid = started.pid
	if started.ready {
		return lease, nil
	}
	cause := errors.New("the daemon did not open its socket")
	if started.exit != 0 {
		cause = fmt.Errorf("the daemon exited with status %d", started.exit)
	}
	failure := &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "start the immutable object cache", Err: cause}
	if started.unreserved {
		return nil, failure
	}
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

// Config returns a copy of the configuration that the lease started the
// daemon with, its defaults filled in. It is descriptive only: changing the
// copy changes nothing that the lease's methods act on.
//
//purpose:handoff
func (l *ImmutableObjectCacheLease) Config() ImmutableObjectCache {
	return l.config
}

// ClientOptions returns the Ceph options that make a librbd client in the
// lease's container use the cache, for a ceph.conf section, the rbd CLI
// (as --name=value) or rados.Rados(conf=...).
//
//purpose:handoff
func (l *ImmutableObjectCacheLease) ClientOptions() map[string]string {
	return map[string]string{
		"rbd_parent_cache_enabled":    "true",
		"rbd_plugins":                 "parent_cache",
		"immutable_object_cache_sock": l.config.Socket,
	}
}

// Status reports the daemon and the cache directory.
//
//purpose:observation
func (l *ImmutableObjectCacheLease) Status(ctx context.Context) (ImmutableObjectCacheStatus, error) {
	if err := clusterutil.LockMutex(ctx, &l.mu); err != nil {
		return ImmutableObjectCacheStatus{}, err
	}
	defer l.mu.Unlock()
	if l.complete {
		return ImmutableObjectCacheStatus{}, servicehook.Invalid(errors.New("the immutable object cache lease was restored"))
	}
	return l.status(ctx)
}

func (l *ImmutableObjectCacheLease) status(ctx context.Context) (ImmutableObjectCacheStatus, error) {
	out, err := immutableObjectCacheOp(ctx, l.client, "status", strconv.Itoa(l.pid), l.config.Socket, l.adminSocket, l.config.Path, l.pidFile)
	if err != nil {
		return ImmutableObjectCacheStatus{}, fmt.Errorf("read the immutable object cache: %w", err)
	}
	return parseImmutableObjectCacheStatus(out, l.pid)
}

// Stop stops the daemon, with SIGTERM and then SIGKILL, and keeps the cache
// directory and socket file: librbd clients that open an image afterwards
// fail to register with the cache and read the parents from RADOS. A daemon
// that already exited needs no stop.
//
//purpose:condition
func (l *ImmutableObjectCacheLease) Stop(ctx context.Context) error {
	if err := clusterutil.LockMutex(ctx, &l.mu); err != nil {
		return err
	}
	defer l.mu.Unlock()
	return l.stop(ctx)
}

// stop finds the daemon by its PID file when the start was not confirmed.
func (l *ImmutableObjectCacheLease) stop(ctx context.Context) error {
	if l.stopped || l.complete {
		return nil
	}
	out, err := immutableObjectCacheOp(ctx, l.client, "stop", strconv.Itoa(l.pid), l.config.Socket, l.pidFile)
	if err != nil {
		return fmt.Errorf("stop the immutable object cache: %w", err)
	}
	stopped, err := parseImmutableObjectCacheFlag(out, "stopped")
	if err != nil {
		return err
	}
	if !stopped {
		return &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "stop the immutable object cache",
			Err: errors.New("could not confirm daemon startup has settled and the owned process has stopped")}
	}
	l.stopped = true
	return nil
}

// Restore stops the daemon if it runs and removes its socket, admin socket,
// PID file and cached objects. It removes the cache directory only when the
// lease created it. If the reserved directory or a socket or PID file has
// been replaced, Restore preserves the replacement and reports a conflict.
// A repeated call after success does nothing.
//
//purpose:construction,lifetime
func (l *ImmutableObjectCacheLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if err := clusterutil.LockMutex(ctx, &l.mu); err != nil {
		return err
	}
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
	out, err := immutableObjectCacheOp(ctx, l.client, "remove", l.config.Path, l.config.Socket, l.adminSocket, l.pidFile, created)
	if err != nil {
		return fmt.Errorf("remove the immutable object cache files: %w", err)
	}
	removed, err := parseImmutableObjectCacheFlag(out, "removed")
	if err != nil {
		return err
	}
	if !removed {
		return errors.New("remove the immutable object cache files: the lease could not verify ownership of its reserved paths")
	}
	l.complete = true
	return nil
}

// immutableObjectCacheOp runs one operation of immutableObjectCacheScript and
// returns its key=value lines, unvalidated: each operation's parser checks
// the fields it needs.
func immutableObjectCacheOp(ctx context.Context, client testcontainers.Container, op string, args ...string) (immutableObjectCacheOutput, error) {
	out, err := multicluster.Exec(ctx, client, append([]string{"sh", "-c", immutableObjectCacheScript, "sh", op}, args...)...)
	if err != nil {
		return immutableObjectCacheOutput{}, err
	}
	return immutableObjectCacheOutput{op: op, out: out}, nil
}
