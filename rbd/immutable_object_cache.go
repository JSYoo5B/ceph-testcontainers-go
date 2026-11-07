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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
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
	// be under Path. The librbd client socket path is limited to 107 bytes.
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
// while its command line holds both the lease's unique PID file and its
// private listening socket, so a reused PID is never stopped. Librbd reaches
// that socket at the configured path through a hard link. Status.RADOSReads
// measures the daemon's reads from RADOS.
type ImmutableObjectCacheLease struct {
	// config is the request with its defaults filled in, fixed at the start:
	// Status, Stop, Restore and ClientOptions act on its path and socket.
	config ImmutableObjectCache

	mu          sync.Mutex
	client      testcontainers.Container
	adminSocket string
	pidFile     string
	pid         int
	createdPath bool // the cache directory did not exist before startup
	launchSent  bool // start was submitted after successful reservation
	// A pending reservation must be canceled before its directory is removed.
	reserveMarker string
	reservations  []string
	stopped       bool
	complete      bool
}

const immutableCacheReserveLabel = "reserve the immutable object cache lease"

var immutableObjectCacheID = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// immutableObjectCacheScript runs one operation in the client container and
// reports its result as key=value lines; it exits non-zero only when the
// container cannot run it. A process is the lease's daemon only while its
// command line holds the lease's PID file and configured socket.
const immutableObjectCacheScript = `set -u
ours() {
    test -n "$1" && test -r "/proc/$1/cmdline" &&
        tr '\0' '\n' 2>/dev/null < "/proc/$1/cmdline" | CACHE_PID="$2" CACHE_SOCK="$3" awk '
            $0 == "--pid-file=" ENVIRON["CACHE_PID"] { pid = 1 }
            $0 == "--immutable_object_cache_sock=" ENVIRON["CACHE_SOCK"] { sock = 1 }
            END { exit !(pid && sock) }'
}
find_daemon() {
    for proc in /proc/[0-9]*/cmdline; do
        candidate=${proc#/proc/}; candidate=${candidate%/cmdline}
        if ours "$candidate" "$1" "$2"; then echo "$candidate"; return; fi
    done
}
exists() { test -e "$1" || test -L "$1"; }
# Bind under the private reservation. A short alias keeps the daemon's Unix
# socket addresses within sockaddr_un even when the public parent is deep.
cache_paths() {
    state=$pidfile.lease
    alias=/tmp/tc-ioc-${pidfile##*/}
    private_sock=$alias/s
    private_admin=$alias/a
    private_pid=$state/pid
    marker=.${pidfile##*/}.owner
}
publish_file() {
    source=$1 public=$2 name=$3
    ln -T "$source" "$state/$name.pin" && ln -T "$state/$name.pin" "$public" || return 1
    touch "$state/$name.published"
}
check_public() {
    ! exists "$1" || { test ! -L "$1" && test "$1" -ef "$state/$2.pin"; } ||
        ! test -f "$state/$2.published"
}
remove_public() {
    if exists "$1" && test ! -L "$1" && test "$1" -ef "$state/$2.pin"; then rm -f "$1"; else check_public "$1" "$2"; fi
}
release_control() {
    if test "$created" = 1 && exists "$stage"; then
        if exists "$stage/data/$marker"; then rmdir "$stage/data/$marker" || return 1; fi
        if exists "$stage/data"; then rmdir "$stage/data" || return 1; fi
        rmdir "$stage" || return 1
    fi
    if test "$(readlink "$alias" 2>/dev/null)" = "$state"; then rm -f "$alias" || return 1; fi
    rm -rf "$state"
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
reserve)
    state=$1
    # The shell supervisor can stop and reap its command. Make the actual
    # reservation in that process, so no descendant can reserve after cancel.
    mkdir -p "$(dirname "$state")" || exit 1
    exec mkdir -m 700 "$state"
    ;;
release-reservation)
    for state do
        if exists "$state"; then rmdir "$state" || exit 1; fi
    done
    echo removed=1
    ;;
start)
    id=$1 path=$2 sock=$3 size=$4 asok=$5 pidfile=$6
    cache_paths
    stage=$(dirname "$path")/.${pidfile##*/}.stage
    # Stop and start compete for one claim in the pre-existing reservation.
    # A delayed start after Restore cannot re-create that reservation.
    birth=$(cut -d' ' -f22 "/proc/$$/stat") || exit 1
    ln -sT "$$:$birth" "$state/launch" || exit 1
    trap 'touch "$state/finished"' EXIT
    before_launch_failure() {
        printf 'exit=1\npid=\nready=0\n'
        exit 0
    }
    if exists "$sock" || exists "$asok" || exists "$pidfile"; then before_launch_failure; fi
    ln -sT "$state" "$alias" || before_launch_failure
    if test "$7" = none; then
        # Record the directory identity before exposing it under the caller's
        # name. Until publication, cleanup only needs the private staging area.
        mkdir "$stage/data" || before_launch_failure
        stat -c '%d:%i:%w' "$stage/data" > "$state/owner" || before_launch_failure
        if test "$(stat -c '%w' "$stage/data")" = -; then
            touch "$state/needs-marker" && mkdir "$stage/data/$marker" &&
                stat -c '%d:%i' "$stage/data/$marker" > "$state/marker" || before_launch_failure
        fi
        mv -nT "$stage/data" "$path" && ! exists "$stage/data" || before_launch_failure
    else
        test ! -L "$path" && test -d "$path" && test -z "$(ls -A "$path")" &&
            test "$(stat -c '%d:%i:%w' "$path")" = "$8" || before_launch_failure
        printf '%s\n' "$8" > "$state/owner" || before_launch_failure
        if test "$(stat -c '%w' "$path")" = -; then touch "$state/needs-marker" || before_launch_failure; fi
    fi
    touch "$state/dispatched" || before_launch_failure
    ceph-immutable-object-cache --id "$id" --immutable_object_cache_path="$path" \
        --immutable_object_cache_sock="$private_sock" --immutable_object_cache_max_size="$size" \
        --admin-socket="$private_admin" --pid-file="$private_pid" >/dev/null 2>&1
    exitcode=$?
    echo "exit=$exitcode"
    tries=0
    while test "$exitcode" = 0 && test "$tries" -lt 150; do
        if test -S "$private_sock" && test -s "$private_pid"; then break; fi
        sleep 0.2
        tries=$((tries + 1))
    done
    test ! -L "$path" && test "$(stat -c '%d:%i:%w' "$path")" = "$(cat "$state/owner")" || exit 1
    # The daemon clears its cache on startup: renew the fallback marker.
    if test -f "$state/needs-marker"; then
        if ! exists "$path/$marker"; then mkdir "$path/$marker" || exit 1; fi
        test ! -L "$path/$marker" && test -d "$path/$marker" &&
            stat -c '%d:%i' "$path/$marker" > "$state/marker" || exit 1
    fi
    pid=$(cat "$private_pid" 2>/dev/null)
    echo "pid=$pid"
    if test "$exitcode" != 0 || ! test -S "$private_sock" || ! ours "$pid" "$private_pid" "$private_sock"; then
        echo ready=0; exit 0
    fi
    # Every public inode comes from the lease's private directory. Publication
    # cannot replace a competing listener, and cleanup never adopts one.
    if test -S "$private_admin"; then publish_file "$state/a" "$asok" admin || exit 1; fi
    publish_file "$private_pid" "$pidfile" pidfile && publish_file "$state/s" "$sock" socket || exit 1
    echo ready=1
    ;;
status)
    pid=$1 sock=$2 asok=$3 path=$4 pidfile=$5
    cache_paths
    test "$pid" -gt 0 || pid=$(cat "$private_pid" 2>/dev/null)
    if ours "$pid" "$private_pid" "$private_sock"; then
        echo running=1
        echo "perf=$(ceph --admin-daemon "$private_admin" perf dump objecter 2>/dev/null | tr -d '\n')"
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
    cache_paths
    if test -d "$state" && ! test -f "$state/finished"; then
        if ln -sT canceled "$state/launch" 2>/dev/null; then echo stopped=1; exit 0; fi
        owner=$(readlink "$state/launch") || exit 1
        if test "$owner" = canceled; then echo stopped=1; exit 0; fi
        launcher=${owner%:*}
        if test "$(cut -d' ' -f22 "/proc/$launcher/stat" 2>/dev/null)" = "${owner#*:}"; then
            echo stopped=0; exit 0
        fi
    fi
    test "$pid" -gt 0 || pid=$(cat "$private_pid" 2>/dev/null)
    if ! ours "$pid" "$private_pid" "$private_sock"; then
        # A daemonizing child can outlive its launcher before publishing its
        # PID, or can exit and remove its PID file. Match its two arguments.
        pid=$(find_daemon "$private_pid" "$private_sock")
    fi
    for signal in TERM KILL; do
        ours "$pid" "$private_pid" "$private_sock" || break
        kill -s "$signal" "$pid" 2>/dev/null
        tries=0
        while ours "$pid" "$private_pid" "$private_sock" && test "$tries" -lt 50; do
            sleep 0.2
            tries=$((tries + 1))
        done
    done
    # A parent may have forked a child while it was being signaled.
    if test -n "$(find_daemon "$private_pid" "$private_sock")"; then echo stopped=0; else echo stopped=1; fi
    ;;
remove)
    path=$1 sock=$2 asok=$3 pidfile=$4 created=$5
    cache_paths
    stage=$(dirname "$path")/.${pidfile##*/}.stage
    # Removing the reservation is the last step after all owned paths are released.
    if ! exists "$state"; then echo removed=1; exit 0; fi
    check_public "$sock" socket && check_public "$asok" admin && check_public "$pidfile" pidfile || { echo removed=0; exit 0; }
    # An unpublished or already reclaimed directory is still in staging.
    # Its former public name can belong to someone else.
    if { test "$created" = 1 || test -f "$state/dispatched"; } &&
        test -s "$state/owner" && exists "$path" &&
        { test "$created" = 0 || ! exists "$stage/data"; }; then
        test ! -L "$path" && cd -P "$path" || { echo removed=0; exit 0; }
        test "$(stat -c '%d:%i:%w' .)" = "$(cat "$state/owner")" || { echo removed=0; exit 0; }
        if test -f "$state/needs-marker" && { test "$created" = 1 || ! test -f "$state/emptied"; }; then
            test ! -L "$marker" && test -d "$marker" &&
                test "$(stat -c '%d:%i' "$marker")" = "$(cat "$state/marker")" || { echo removed=0; exit 0; }
        fi
        if test -f "$state/dispatched" && ! test -f "$state/emptied"; then
            # Relative traversal stays in the opened directory across renames.
            find . -mindepth 1 ! -path "./$marker" -delete || exit 1
            touch "$state/emptied" || exit 1
        fi
        if test "$created" = 1; then
            # Move the verified empty directory, with its marker, back into
            # staging first. A retry can never remove a new public directory.
            test -z "$(find . -mindepth 1 ! -path "./$marker" -print -quit)" &&
                test . -ef "$path" && mv -nT "$path" "$stage/data" && test . -ef "$stage/data" ||
                { echo removed=0; exit 0; }
        elif test -f "$state/marker" && test -d "$marker"; then
            rmdir "$marker" || exit 1
        fi
    fi
    remove_public "$sock" socket && remove_public "$asok" admin && remove_public "$pidfile" pidfile || exit 1
    release_control || exit 1
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
	token := uuid.NewString()
	lease := &ImmutableObjectCacheLease{
		config:      config,
		client:      client,
		adminSocket: path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token[:8]+".asok"),
		pidFile:     path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".pid"),
		createdPath: state.path == "none",
	}
	directories := []string{lease.pidFile + ".lease"}
	if lease.createdPath {
		directories = append(directories, path.Join(path.Dir(config.Path), "."+path.Base(lease.pidFile)+".stage"))
	}
	for _, directory := range directories {
		report, err := supervised.RunShell(ctx, client, immutableCacheReserveLabel, 10, "sh", "-c", immutableObjectCacheScript, "sh", "reserve", directory)
		if err == nil && !report.Stopped && report.ExitCode != 0 {
			failure := fmt.Errorf("%s: exit %d", immutableCacheReserveLabel, report.ExitCode)
			if len(lease.reservations) > 0 {
				if cleanupErr := lease.Restore(context.WithoutCancel(ctx)); cleanupErr != nil {
					return lease, errors.Join(failure, cleanupErr)
				}
			}
			return nil, failure
		}
		lease.reservations = append(lease.reservations, directory)
		if err != nil {
			lease.reserveMarker, _ = supervised.Unconfirmed(err)
			return lease, fmt.Errorf("%s: %w", immutableCacheReserveLabel, err)
		}
		if report.Stopped {
			return lease, errors.New("immutable object cache lease reservation timed out")
		}
	}
	lease.launchSent = true
	started, err := lease.start(ctx, state)
	if err != nil {
		// Restore can fence a delayed launch or find the owned daemon if
		// the script finished without a PID report.
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
	case len(config.Socket) > 107:
		return config, servicehook.Invalid(errors.New("the cache socket path exceeds the 107-byte librbd client socket limit"))
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

// stop reconciles the reservation, launcher and daemon after a lost reply.
func (l *ImmutableObjectCacheLease) stop(ctx context.Context) error {
	if l.stopped || l.complete {
		return nil
	}
	if !l.launchSent {
		if l.reserveMarker != "" {
			if err := supervised.CancelShell(ctx, l.client, immutableCacheReserveLabel, l.reserveMarker); err != nil {
				return err
			}
			l.reserveMarker = ""
		}
		l.stopped = true
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
// lease created it. A missing directory needs no cleanup. If the reserved directory or a socket or PID file has
// been replaced, Restore preserves the replacement and reports a conflict.
// A competing listener that claimed the client socket before publication
// keeps its address.
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
	op := "remove"
	args := []string{l.config.Path, l.config.Socket, l.adminSocket, l.pidFile, created}
	if !l.launchSent {
		op, args = "release-reservation", l.reservations
	}
	out, err := immutableObjectCacheOp(ctx, l.client, op, args...)
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
