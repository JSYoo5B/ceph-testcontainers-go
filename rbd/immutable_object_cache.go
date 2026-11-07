package rbd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// DefaultImmutableObjectCachePath. It must be absent or empty. The lease
	// stores the daemon's objects in a private subdirectory of Path.
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
	// CachedObjects and CachedBytes count the daemon's object files and
	// their bytes. Lease ownership records are excluded.
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
	reserveMarker        string
	cleanupMarker        string
	reservations         []string
	owner                string
	resourcesRemoved     bool
	controlsReleased     bool
	uncertainReservation string
	claim                string
	complete             bool
}

const immutableCacheReserveLabel = "reserve the immutable object cache lease"
const immutableCacheRollbackTimeout = 30 * time.Second

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
    state=$pidfile.lease control=$pidfile.lease
    alias=/tmp/tc-ioc-${pidfile##*/}
    private_sock=$alias/s
    private_admin=$alias/a
    private_pid=$state/pid
    marker=.ceph-immutable-cache
}
# Keep a second link to each reservation's identity beside the directory.
# The expected token comes from the Go lease, outside all container paths.
owned_dir() {
    test -d "$1" && test ! -L "$1" && test -f "$1/.identity" && test ! -L "$1/.identity" &&
        test ! -L "$1/.identity" && test ! -L "$2" && test "$1/.identity" -ef "$2" &&
        test "$(head -n 1 "$2")" = "$0:$(stat -c '%d:%i:%w' "$1")"
}
pin_control() {
    test ! -L "$control" && test -d "$control" || return 1
    exec 7<"$control" || return 1
    state=/proc/$$/fd/7/.
    owned_dir "$state" "$control.witness" && test "$state" -ef "$control"
}
path_claim() {
    canonical=$(realpath -m -- "$1") || return 1
    # Mountinfo identifies the filesystem and the root of each bind mount.
    # This key stays stable when missing parents are created and when the same
    # directory is addressed through another mount of that filesystem.
    identity=$(CACHE_PATH="$canonical" awk '
        function decode(s) {
            gsub(/\\040/, " ", s); gsub(/\\011/, "\t", s)
            gsub(/\\012/, "\n", s); gsub(/\\134/, sprintf("%c",92), s)
            return s
        }
        BEGIN { p=ENVIRON["CACHE_PATH"] }
        {
            m=decode($5)
            if (length(m)>=n && (m=="/" || p==m || index(p,m "/")==1)) {
                n=length(m); root=decode($4); dev=$3
                tail=(m=="/" ? p : substr(p,n+1))
            }
        }
        END { if (!n) exit 1; printf "%s:%s%s", dev, (root=="/" ? "" : root), tail }
        ' /proc/self/mountinfo) || return 1
    hash=$(printf '%s' "$identity" | sha256sum) || return 1
    printf '/tmp/tc-ioc-path-%s' "${hash%% *}"
}
# A non-directory ancestor is an invalid pathname. An unreadable directory
# remains an inspection error rather than a malformed-request verdict.
parent_state() {
    parent=$(dirname -- "$1") || return 1
    while ! exists "$parent"; do parent=$(dirname -- "$parent") || return 1; done
    if ! test -d "$parent"; then echo invalid; elif test -x "$parent"; then echo valid; else return 1; fi
}
publish_file() {
    source=$1 public=$2 name=$3
    ln -T "$source" "$state/$name.pin" || return 1
    if ! ln -T "$state/$name.pin" "$public"; then
        if exists "$public"; then conflict; fi
        return 1
    fi
    touch "$state/$name.published"
}
# No emulation: a failed syscall leaves both names alone, including across
# mounts. GNU mv on older client images can copy and unlink on EXDEV.
rename_noreplace() {
    python3 -c 'import ctypes, os, sys
lib = ctypes.CDLL(None, use_errno=True)
rename = getattr(lib, "renameat2", None)
if rename is None: sys.exit(38)
rc = rename(-100, os.fsencode(sys.argv[1]), -100, os.fsencode(sys.argv[2]), 1)
sys.exit(ctypes.get_errno() if rc else 0)' "$1" "$2"
}
held_public() { printf 'held=%s\nremoved=0\n' "$1"; exit 0; }
refuse_remove() { echo removed=0; exit 0; }
owned_public() { test ! -L "$1" && test "$1" -ef "$state/$2.pin"; }
check_public() {
    ! exists "$1" || owned_public "$1" "$2" || owned_public "$state/$2.released" "$2" ||
        ! test -f "$state/$2.published"
}
# Recover a no-replace rename whose reply was lost. A foreign entry may
# return only to a vacant public name. Keep an unrecognized private entry
# intact and report its recovery name.
settle_handoff() {
    slot=$state/$2.handoff captured=$state/$2.captured
    if exists "$captured"; then
        test ! -L "$captured" && test -f "$captured" && test "$(head -n 1 "$captured")" = "$0" || held_public "$2"
    fi
    if ! exists "$slot"; then
        if exists "$captured"; then rm -f "$captured" || exit 1; fi
        return
    fi
    if exists "$captured"; then
        test "$(stat -c '%d:%i:%w:%f' "$slot")" = "$(tail -n 1 "$captured")" || held_public "$2"
        rename_noreplace "$slot" "$1" || held_public "$2"
        rm "$captured" || exit 1
        refuse_remove
    fi
    owned_public "$slot" "$2" || held_public "$2"
    if ! owned_public "$state/$2.released" "$2"; then
        ln -T "$state/$2.pin" "$state/$2.released" || exit 1
    fi
    # Only the authenticated private directory receives an unlink.
    rm "$slot" || exit 1
}
remove_public() {
    settle_handoff "$1" "$2"
    if ! exists "$1" || owned_public "$state/$2.released" "$2"; then return; fi
    if ! owned_public "$1" "$2"; then check_public "$1" "$2" || refuse_remove; return; fi
    slot=$state/$2.handoff
    rename_noreplace "$1" "$slot" || {
        result=$?
        if test "$result" = 17; then held_public "$2"; fi
        if test "$result" = 2 && ! exists "$1" && ! exists "$slot"; then return; fi
        exit 1
    }
    # Compare the inode actually moved rather than the one observed before
    # the rename. If an outside writer won, save its identity before trying
    # to restore the name. A crash before this record leaves the entry held.
    if ! owned_public "$slot" "$2"; then
        identity=$(stat -c '%d:%i:%w:%f' "$slot") || held_public "$2"
        (set -C; printf '%s\n%s\n' "$0" "$identity" > "$state/$2.captured") || held_public "$2"
    fi
    settle_handoff "$1" "$2"
}
# Only remove known control files. In particular, never traverse an outside
# directory that has appeared under a reservation's former name.
release_dir() {
    dir=$1 guard=$dir.witness seed=$dir.pending-$0 retired=$dir.releasing-$0
    if exists "$seed"; then
        if ! exists "$guard" && test -f "$seed/.identity" && test ! -L "$seed" &&
            test "$(cat "$seed/.identity")" = "$0:$(stat -c '%d:%i:%w' "$seed")"; then
            ln -T "$seed/.identity" "$guard" || return 1
        fi
        owned_dir "$seed" "$guard" || return 1
        mv -nT "$seed" "$retired" && ! exists "$seed" || return 1
    fi
    if exists "$dir" && ! exists "$retired" && ! grep -qx drained "$guard" 2>/dev/null; then
        owned_dir "$dir" "$guard" || return 1
        mv -nT "$dir" "$retired" && ! exists "$dir" || return 1
    fi
    if exists "$retired"; then
        test ! -L "$retired" && test -d "$retired" || return 1
        (cd -P "$retired" &&
            test "$(head -n 1 "$guard" 2>/dev/null)" = "$0:$(stat -c '%d:%i:%w' .)" &&
            if exists .identity; then
                owned_dir . "$guard" &&
                rm -f -- launch finished owner dispatched emptied path-claim marker payload unpublished-marker \
                    a s pid admin.pin socket.pin pidfile.pin admin.published socket.published pidfile.published \
                    admin.released socket.released pidfile.released &&
                test -z "$(find . -mindepth 1 ! -name .identity -print -quit)" &&
                printf 'drained\n' >> "$guard" && rm .identity
            else grep -qx drained "$guard"; fi &&
            test . -ef "$retired" && rmdir "$retired") || return 1
    fi
    if exists "$guard"; then
        test ! -L "$guard" && test -f "$guard" &&
            test "$(head -n 1 "$guard" | cut -d: -f1)" = "$0" && grep -qx drained "$guard"
    else
        { test "$dir" = "$uncertain" || { test "$dir" = "$absent_stage" && ! exists "$(dirname "$dir")"; }; } &&
            ! exists "$dir" && ! exists "$seed" && ! exists "$retired"
    fi
}
op=$1
shift
# A Docker exec can outlive its transport response. Serialize every Restore
# mutation and new path claim on this stable container inode. Descendants
# retain FD 9; even a delayed unlink outlives neither the lock nor its fence.
case "$op" in
claim|reserve|stop|remove|release-reservation|forget-reservation|unclaim)
    exec 9</tmp || exit 1
    while :; do
        flock -E 75 -w 1 -x 9 && break
        test "$?" = 75 || exit 1
    done
    ;;
esac
case "$op" in
inspect)
    path=$1 sock=$2
    if rename_noreplace "" "" 2>/dev/null; then exit 1
    elif test "$?" = 2; then echo rename=1
    else echo rename=0; fi
    if command -v flock >/dev/null 2>&1; then echo flock=1; else echo flock=0; fi
    if command -v ceph-immutable-object-cache >/dev/null 2>&1; then echo binary=1; else echo binary=0; fi
    dir=$(ceph-conf --show-config-value plugin_dir 2>/dev/null)
    if test -n "$dir" && test -f "$dir/librbd/libceph_librbd_parent_cache.so"; then echo plugin=1; else echo plugin=0; fi
    parent_kind=$(parent_state "$path") && socket_parent=$(parent_state "$sock") || exit 1
    if test "$socket_parent" = invalid; then echo socket=invalid-parent
    elif exists "$sock"; then echo socket=1
    else echo socket=0; fi
    claim=$(path_claim "$path") || exit 1
    printf 'claim=%s\n' "$claim"
    if test "$parent_kind" = invalid; then echo path=invalid-parent
    elif exists "$claim"; then echo path=claimed
    elif test -L "$path"; then echo path=other
    elif test -d "$path"; then
        contents=$(ls -A "$path") || exit 1
        if test -z "$contents"; then
            echo path=empty
            printf 'identity=' && stat -c '%d:%i:%w' "$path" || exit 1
        else echo path=full; fi
    elif test -e "$path"; then echo path=other
    else echo path=none
    fi
    ;;
claim)
    if exists "$1"; then exit 3; fi
    exec ln -sT "$0:$2" "$1"
    ;;
claim-held)
    if exists "$1"; then echo held=1; else echo held=0; fi
    ;;
unclaim)
    if test "$(readlink "$1" 2>/dev/null)" = "$0:$2"; then rm "$1" || exit 1; fi
    echo removed=1
    ;;
confirm-reservation)
    if owned_dir "$1" "$1.witness"; then echo reserved=1; else echo reserved=0; fi
    ;;
reserve)
    dir=$1 seed=$1.pending-$0
    parent=$(dirname "$dir") || exit 1
    if test -n "${2:-}"; then
        if test -L "$parent" || ! test -d "$parent"; then exit 3; fi
        identity=$(stat -c '%d:%i:%w' "$parent") || exit 1
        test "$identity" = "$2" || exit 3
    else
        mkdir -p "$parent" || exit 1
    fi
    if exists "$dir" || exists "$dir.witness" || exists "$seed"; then exit 3; fi
    mkdir -m 700 "$seed" || exit 1
    identity=$(stat -c '%d:%i:%w' "$seed") || exit 1
    printf '%s:%s\n' "$0" "$identity" > "$seed/.identity" && ln -T "$seed/.identity" "$dir.witness" || exit 1
    # The final publication runs in the supervisor's directly reaped process.
    exec mv -nT "$seed" "$dir"
    ;;
release-reservation)
    uncertain=$1 absent_stage=$2; shift 2
    for dir do
        release_dir "$dir" || { echo removed=0; exit 0; }
    done
    echo removed=1
    ;;
forget-reservation)
    # Go received positive acknowledgements for data and control release.
    # Only the external witnesses belonging to this lease may be removed.
    for dir do
        guard=$dir.witness
        if test ! -L "$guard" && test -f "$guard" &&
            test "$(head -n 1 "$guard" | cut -d: -f1)" = "$0" && grep -qx drained "$guard"; then
            rm "$guard" || exit 1
        fi
    done
    echo removed=1
    ;;
start)
    id=$1 path=$2 sock=$3 size=$4 asok=$5 pidfile=$6
    cache_paths
    stage=$(dirname "$path")/.${pidfile##*/}.stage
    conflict() { printf 'conflict=1\n'; exit 0; }
    pin_control || conflict
    if test "$7" = empty; then stage=$path/.${pidfile##*/}.stage; fi
    owned_dir "$stage" "$stage.witness" || conflict
    # Stop and start compete for one claim in the pre-existing reservation.
    # A delayed start after Restore cannot re-create that reservation.
    birth=$(cut -d' ' -f22 "/proc/$$/stat") || exit 1
    ln -sT "$$:$birth" "$state/launch" || exit 1
    trap 'touch "$state/finished"' EXIT
    if exists "$sock" || exists "$asok" || exists "$pidfile"; then conflict; fi
    claim=$9
    printf '%s\n' "$claim" > "$state/path-claim" || exit 1
    test "$(readlink "$claim" 2>/dev/null)" = "$0:$control" || conflict
    ln -sT "$control" "$alias" || exit 1
    if test "$7" = none; then
        if exists "$path"; then conflict; fi
        mkdir "$stage/data" || exit 1
        root=$stage/data
    else
        if test -L "$path" || ! test -d "$path"; then conflict; fi
        stage_name=${stage##*/}
        others=$(find "$path" -mindepth 1 -maxdepth 1 ! -name "$stage_name" ! -name "$stage_name.witness" -print -quit) || exit 1
        if test -n "$others" || test "$(stat -c '%d:%i:%w' "$path")" != "$8"; then conflict; fi
        root=$path
    fi
    stat -c '%d:%i:%w' "$root" > "$state/owner" || exit 1
    # The daemon initializes only objects/. Its sibling ownership marker
    # survives initialization, daemon loss and a restart of the container.
    mkdir "$stage/marker" && ln -sT "$0:$control" "$stage/marker/owner" &&
        stat -c '%d:%i' "$stage/marker" > "$state/marker" &&
        mkdir "$stage/marker/objects" && stat -c '%d:%i:%w' "$stage/marker/objects" > "$state/payload" || exit 1
    mv -nT "$stage/marker" "$root/$marker" || { if exists "$root/$marker"; then conflict; fi; exit 1; }
    if exists "$stage/marker"; then conflict; fi
    if test "$7" = none; then
        mv -nT "$root" "$path" || { if exists "$path"; then conflict; fi; exit 1; }
        if exists "$root"; then conflict; fi
    fi
    touch "$state/dispatched" || exit 1
    ceph-immutable-object-cache --id "$id" --immutable_object_cache_path="$path/$marker/objects" \
        --immutable_object_cache_sock="$private_sock" --immutable_object_cache_max_size="$size" \
        --admin-socket="$private_admin" --pid-file="$private_pid" >/dev/null 2>&1
    exitcode=$?
    tries=0
    while test "$exitcode" = 0 && test "$tries" -lt 150; do
        if test -S "$private_sock" && test -s "$private_pid"; then break; fi
        sleep 0.2
        tries=$((tries + 1))
    done
    test ! -L "$path" && test "$(stat -c '%d:%i:%w' "$path")" = "$(cat "$state/owner")" || exit 1
    pid=$(cat "$private_pid" 2>/dev/null)
    if test "$exitcode" != 0 || ! test -S "$private_sock" || ! ours "$pid" "$private_pid" "$private_sock"; then
        printf 'exit=%s\npid=%s\nready=0\n' "$exitcode" "$pid"; exit 0
    fi
    # Every public inode comes from the lease's private directory. Publication
    # cannot replace a competing listener, and cleanup never adopts one.
    if test -S "$private_admin"; then publish_file "$state/a" "$asok" admin || exit 1; fi
    publish_file "$private_pid" "$pidfile" pidfile && publish_file "$state/s" "$sock" socket || exit 1
    printf 'exit=0\npid=%s\nready=1\n' "$pid"
    ;;
status)
    pid=$1 sock=$2 asok=$3 path=$4 pidfile=$5
    cache_paths
    pin_control || exit 1
    test "$pid" -gt 0 || pid=$(cat "$private_pid" 2>/dev/null)
    if ours "$pid" "$private_pid" "$private_sock"; then
        echo running=1
        echo "perf=$(ceph --admin-daemon "$private_admin" perf dump objecter 2>/dev/null | tr -d '\n')"
    else
        echo running=0
    fi
    if test -d "$path/$marker/objects"; then
        test "$(readlink "$path/$marker/owner")" = "$0:$control" || exit 1
        echo dir=1
        find "$path/$marker/objects" -type f -printf '%s\n' | awk '{ n++; b += $1 } END { printf "files=%d\nbytes=%.0f\n", n, b }'
    else
        echo dir=0
    fi
    ;;
stop)
    pid=$1 sock=$2 pidfile=$3
    cache_paths
    pin_control || { echo stopped=0; exit 0; }
    if ! test -f "$state/finished"; then
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
    pin_control || { echo removed=0; exit 0; }
    if test "$created" = 0; then stage=$path/.${pidfile##*/}.stage; fi
    if test "$created" = 1 || exists "$path"; then
        stage_name=$stage
        test ! -L "$stage_name" && test -d "$stage_name" || { echo removed=0; exit 0; }
        exec 8<"$stage_name" || exit 1
        stage=/proc/$$/fd/8/.
        owned_dir "$stage" "$stage_name.witness" && test "$stage" -ef "$stage_name" || { echo removed=0; exit 0; }
    fi
    # An interrupted build of the marker tree has never been sent to Ceph.
    # Remove only the known entries inside the authenticated staging directory.
    if exists "$stage/marker"; then
        touch "$state/unpublished-marker" || exit 1
        test ! -L "$stage/marker" && test -d "$stage/marker" || { echo removed=0; exit 0; }
        if exists "$stage/marker/objects"; then rmdir "$stage/marker/objects" || exit 1; fi
        if exists "$stage/marker/owner"; then
            test "$(readlink "$stage/marker/owner")" = "$0:$control" && rm "$stage/marker/owner" || exit 1
        fi
        rmdir "$stage/marker" || exit 1
    fi
    # Pin the parent used for all three public names. A later change to a
    # parent symlink cannot redirect a compensating rename.
    exec 6<"$(dirname "$sock")" || exit 1
    parent=/proc/$$/fd/6/.
    test "$parent/${control##*/}" -ef "$state" || refuse_remove
    sock=$parent/${sock##*/} asok=$parent/${asok##*/} pidfile=$parent/${pidfile##*/}
    settle_handoff "$sock" socket; settle_handoff "$asok" admin; settle_handoff "$pidfile" pidfile
    check_public "$sock" socket && check_public "$asok" admin && check_public "$pidfile" pidfile || { echo removed=0; exit 0; }
    # A staged directory was either never published or already reclaimed.
    root=$path
    if test "$created" = 1 && exists "$stage/data"; then root=$stage/data; fi
    if test -s "$state/owner" && exists "$root" && ! test -f "$state/emptied" && ! exists "$stage/reclaimed"; then
        test ! -L "$root" && cd -P "$root" && test "$(stat -c '%d:%i:%w' .)" = "$(cat "$state/owner")" ||
            { echo removed=0; exit 0; }
        if test -f "$state/marker" && ! test -f "$state/unpublished-marker"; then
            test ! -L "$marker" && test "$(readlink "$marker/owner" 2>/dev/null)" = "$0:$control" &&
                test "$(stat -c '%d:%i' "$marker")" = "$(cat "$state/marker")" || { echo removed=0; exit 0; }
            if test "$created" = 1 && test "$root" = "$path"; then
                test -z "$(find . -mindepth 1 -maxdepth 1 ! -name "$marker" -print -quit)" &&
                    test . -ef "$path" && mv -nT "$path" "$stage/data" && test . -ef "$stage/data" ||
                    { echo removed=0; exit 0; }
            fi
            # Move the authenticated subtree into private staging before the
            # recursive traversal. After this transition a reused public path is
            # independent of this cleanup, including after a lost reply.
            mv -nT "$marker" "$stage/reclaimed" && ! exists "$marker" || exit 1
        fi
    fi
    if exists "$stage/reclaimed"; then
        test ! -L "$stage/reclaimed" && cd -P "$stage/reclaimed" &&
            test "$(stat -c '%d:%i' .)" = "$(cat "$state/marker")" || { echo removed=0; exit 0; }
        if ! test -f "$state/emptied"; then
            test "$(readlink owner 2>/dev/null)" = "$0:$control" || { echo removed=0; exit 0; }
            if exists objects; then
                (test ! -L objects && cd -P objects && test "$(stat -c '%d:%i:%w' .)" = "$(cat "$state/payload")" &&
                    find . -mindepth 1 -delete) || { echo removed=0; exit 0; }
                rmdir objects || exit 1
            fi
            touch "$state/emptied" || exit 1
        fi
        if exists owner; then test "$(readlink owner)" = "$0:$control" && rm owner || exit 1; fi
        test . -ef "$stage/reclaimed" && rmdir "$stage/reclaimed" || exit 1
    else
        touch "$state/emptied" || exit 1
    fi
    if test "$created" = 1 && exists "$stage/data"; then rmdir "$stage/data" || exit 1; fi
    remove_public "$sock" socket; remove_public "$asok" admin; remove_public "$pidfile" pidfile
    if test "$(readlink "$alias" 2>/dev/null)" = "$control"; then rm -f "$alias" || exit 1; fi
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
// container needs util-linux flock, ceph-immutable-object-cache, the librbd
// parent_cache plugin and python3 with access to libc's renameat2 syscall. A missing prerequisite is ceph.ErrImageCapability.
// An existing socket, a non-empty cache directory or an invalid path is
// refused with ceph.ErrInvalidRequest. Contention for the cache path or the
// public socket during startup also returns ceph.ErrInvalidRequest; if rollback
// fails the caller receives the lease for a later Restore. A daemon that
// exits or never opens its socket fails the call with ceph.ErrNativeFailure,
// and startup attempts to restore the prior state. A lost start reply or a
// failed cleanup returns a non-nil lease with the error. Automatic rollback
// uses a 30-second budget subject to an earlier caller deadline; its
// supervisor bounds any subsequent cancellation handshake. Keep that lease and
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
	case !state.rename:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "renameat2", Err: errors.New("the immutable cache requires python3 with ctypes and libc renameat2")}
	case !state.flock:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "flock", Err: errors.New("the immutable cache requires util-linux flock")}
	case !state.binary:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "ceph-immutable-object-cache",
			Err: errors.New("the client container has no ceph-immutable-object-cache executable")}
	case !state.plugin:
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "librbd parent_cache plugin",
			Err: errors.New("the client container has no librbd parent_cache plugin")}
	case state.socket || state.invalidSocketParent:
		err := fmt.Errorf("the immutable object cache socket %s already exists", config.Socket)
		if state.invalidSocketParent {
			err = fmt.Errorf("an ancestor of the immutable object cache socket %s is not a directory", config.Socket)
		}
		return nil, servicehook.Invalid(err)
	case state.path != "none" && state.path != "empty":
		err := fmt.Errorf("the immutable object cache path %s is not an absent or empty directory", config.Path)
		if state.path == "claimed" {
			err = fmt.Errorf("the immutable object cache path %s already has a lease claim at %s", config.Path, state.claim)
		}
		if state.path == "invalid-parent" {
			err = fmt.Errorf("an ancestor of the immutable object cache path %s is not a directory", config.Path)
		}
		return nil, servicehook.Invalid(err)
	}
	token := uuid.NewString()
	lease := &ImmutableObjectCacheLease{
		owner:       uuid.NewString(),
		config:      config,
		client:      client,
		adminSocket: path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token[:8]+".asok"),
		pidFile:     path.Join(path.Dir(config.Socket), "immutable-object-cache-"+token+".pid"),
		createdPath: state.path == "none",
	}
	lease.claim = state.claim
	claimReport, err := supervised.RunShell(ctx, client, immutableCacheReserveLabel, 0, "sh", "-c", immutableObjectCacheScript, lease.owner, "claim", lease.claim, lease.pidFile+".lease")
	if err != nil {
		lease.reserveMarker, _ = supervised.Unconfirmed(err)
		return lease, fmt.Errorf("claim immutable object cache path: %w", err)
	}
	if claimReport.Stopped {
		return lease.rollbackStart(ctx, errors.New("immutable object cache path claim timed out"))
	}
	if claimReport.ExitCode != 0 {
		out, inspectErr := immutableObjectCacheOp(ctx, client, lease.owner, "claim-held", lease.claim)
		if inspectErr != nil {
			return nil, inspectErr
		}
		held, inspectErr := parseImmutableObjectCacheFlag(out, "held")
		if inspectErr != nil {
			return nil, inspectErr
		}
		if held || claimReport.ExitCode == 3 {
			return nil, immutableCacheConflict()
		}
		return nil, fmt.Errorf("claim immutable object cache path: exit %d", claimReport.ExitCode)
	}
	stageParent := config.Path
	if lease.createdPath {
		stageParent = path.Dir(config.Path)
	}
	directories := []string{lease.pidFile + ".lease", path.Join(stageParent, "."+path.Base(lease.pidFile)+".stage")}
	for i, directory := range directories {
		var parentIdentity string
		if i == 1 && !lease.createdPath {
			parentIdentity = state.identity
		}
		report, err := supervised.RunShell(ctx, client, immutableCacheReserveLabel, 0, "sh", "-c", immutableObjectCacheScript, lease.owner, "reserve", directory, parentIdentity)
		if err == nil && !report.Stopped && report.ExitCode != 0 {
			failure := fmt.Errorf("%s: exit %d", immutableCacheReserveLabel, report.ExitCode)
			if report.ExitCode == 3 {
				failure = immutableCacheConflict()
			} else {
				lease.reservations = append(lease.reservations, directory)
				lease.uncertainReservation = directory
			}
			return lease.rollbackStart(ctx, failure)
		}
		lease.reservations = append(lease.reservations, directory)
		if err != nil || report.Stopped {
			lease.uncertainReservation = directory
		}
		if err != nil {
			lease.reserveMarker, _ = supervised.Unconfirmed(err)
			return lease, fmt.Errorf("%s: %w", immutableCacheReserveLabel, err)
		}
		if report.Stopped {
			return lease.rollbackStart(ctx, errors.New("immutable object cache lease reservation timed out"))
		}
		out, verifyErr := immutableObjectCacheOp(ctx, client, lease.owner, "confirm-reservation", directory)
		if verifyErr != nil {
			return lease.rollbackStart(ctx, verifyErr)
		}
		reserved, verifyErr := parseImmutableObjectCacheFlag(out, "reserved")
		if verifyErr != nil {
			return lease.rollbackStart(ctx, verifyErr)
		}
		if !reserved {
			failure := immutableCacheConflict()
			return lease.rollbackStart(ctx, failure)
		}
	}
	lease.launchSent = true
	started, err := lease.start(ctx, state)
	if err != nil {
		failure := fmt.Errorf("start the immutable object cache: %w", err)
		var native *ceph.NativeFailure
		if errors.As(err, &native) || started.replied {
			return lease.rollbackStart(ctx, failure)
		}
		// The caller retains the lease after a transport failure with no
		// terminal reply; Restore can fence even a delayed dispatch.
		return lease, failure
	}
	lease.pid = started.pid
	if started.ready {
		return lease, nil
	}
	cause := errors.New("the daemon did not open its socket")
	if started.exit != 0 {
		cause = fmt.Errorf("the daemon exited with status %d", started.exit)
	}
	var failure error = &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "start the immutable object cache", Err: cause}
	if started.conflict {
		failure = immutableCacheConflict()
	}
	return lease.rollbackStart(ctx, failure)
}

func (l *ImmutableObjectCacheLease) rollbackStart(ctx context.Context, cause error) (*ImmutableObjectCacheLease, error) {
	deadline := time.Now().Add(immutableCacheRollbackTimeout)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	rollback, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	if err := l.restore(rollback); err != nil {
		return l, errors.Join(cause, err)
	}
	return nil, cause
}

func immutableCacheConflict() error {
	return servicehook.Invalid(errors.New("immutable object cache path or socket is already claimed or changed during startup"))
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
	out, err := immutableObjectCacheOp(ctx, l.client, l.owner, "status", strconv.Itoa(l.pid), l.config.Socket, l.adminSocket, l.config.Path, l.pidFile)
	if err != nil {
		return ImmutableObjectCacheStatus{}, fmt.Errorf("read the immutable object cache: %w", err)
	}
	return parseImmutableObjectCacheStatus(out, l.pid)
}

// Stop stops the daemon, with SIGTERM and then SIGKILL, and keeps the cache
// directory and socket file: librbd clients that open an image afterwards
// fail to register with the cache and read the parents from RADOS. A daemon
// that already exited needs no stop. Restore operations and new cache path
// claims in the same client container serialize their filesystem changes.
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
	if l.resourcesRemoved || l.complete {
		return nil
	}
	if !l.launchSent {
		if l.reserveMarker != "" {
			if err := supervised.CancelShell(ctx, l.client, immutableCacheReserveLabel, l.reserveMarker); err != nil {
				return err
			}
			l.reserveMarker = ""
		}
		return nil
	}
	out, err := l.cleanupOp(ctx, "stop", strconv.Itoa(l.pid), l.config.Socket, l.pidFile)
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
	return nil
}

// Restore stops the daemon if it runs and removes its socket, admin socket,
// PID file and cached objects. It removes the cache directory only when the
// lease created it. A missing cache directory needs no cleanup. If a private
// reservation, the cache directory or a published file was replaced, Restore
// preserves the replacement and reports a conflict. Restore verifies its private
// reservations against independent witnesses before reading or deleting them.
// Restore moves each published name into its private reservation before
// checking the inode it can unlink. If an outside writer replaces a name
// during that move, the name may briefly be absent. Restore puts the
// replacement back only if the name is vacant. If another writer already
// bound the name, the error identifies the retained entry for recovery.
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
	if l.launchSent && !l.resourcesRemoved {
		if err := l.confirmRemoval(ctx, "remove", l.config.Path, l.config.Socket, l.adminSocket, l.pidFile, created); err != nil {
			return err
		}
		l.resourcesRemoved = true
	}
	if !l.controlsReleased {
		var absentStage string
		if l.resourcesRemoved && !l.createdPath {
			absentStage = path.Join(l.config.Path, "."+path.Base(l.pidFile)+".stage")
		}
		if err := l.confirmRemoval(ctx, "release-reservation", append([]string{l.uncertainReservation, absentStage}, l.reservations...)...); err != nil {
			return err
		}
		l.controlsReleased = true
	}
	if err := l.confirmRemoval(ctx, "forget-reservation", l.reservations...); err != nil {
		return err
	}
	if err := l.confirmRemoval(ctx, "unclaim", l.claim, l.pidFile+".lease"); err != nil {
		return err
	}
	l.complete = true
	return nil
}

func (l *ImmutableObjectCacheLease) confirmRemoval(ctx context.Context, op string, args ...string) error {
	out, err := l.cleanupOp(ctx, op, args...)
	if err != nil {
		return fmt.Errorf("remove the immutable object cache files: %w", err)
	}
	fields, err := out.fields("removed", "held")
	if err != nil {
		return err
	}
	removed, err := fields.flag("removed")
	if err != nil {
		return err
	}
	if _, held := fields.values["held"]; held && (removed || op != "remove") {
		return fields.errorf("held", "a retained public entry requires an incomplete remove")
	}
	if !removed {
		if held, ok := fields.values["held"]; ok {
			if _, err := fields.oneOf("held", "socket", "admin", "pidfile"); err != nil {
				return err
			}
			recovery := path.Join(l.pidFile+".lease", held+".handoff")
			alias := path.Join("/tmp", "tc-ioc-"+path.Base(l.pidFile), held+".handoff")
			return fmt.Errorf("immutable cache: retained a conflicting %s entry at %s (short path %s); inspect it and the public name before retrying", held, recovery, alias)
		}
		if op == "release-reservation" && l.uncertainReservation != "" {
			return fmt.Errorf("restore immutable cache: could not authenticate reservation %s or its unpublished seed %s.pending-%s; preserve any replacement and inspect before retrying", l.uncertainReservation, l.uncertainReservation, l.owner)
		}
		return fmt.Errorf("remove the immutable object cache files: %s could not verify ownership; inspect the cache path %s and reservations %s before retrying", op, l.config.Path, strings.Join(l.reservations, ", "))
	}
	return nil
}

// cleanupOp fences a dispatch that may still arrive after a lost Exec reply.
// The shell's flock is inherited by any rm, mv or find still running after the
// supervisor reaps its direct child. The next command waits for that lock under
// its own caller context. There is no separate duration cap on cache cleanup.
func (l *ImmutableObjectCacheLease) cleanupOp(ctx context.Context, op string, args ...string) (immutableObjectCacheOutput, error) {
	const label = "reconcile the immutable object cache"
	if l.cleanupMarker != "" {
		if err := supervised.CancelShell(ctx, l.client, label, l.cleanupMarker); err != nil {
			return immutableObjectCacheOutput{}, err
		}
		l.cleanupMarker = ""
	}
	argv := append([]string{"sh", "-c", immutableObjectCacheScript, l.owner, op}, args...)
	limit := 0
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return immutableObjectCacheOutput{}, context.DeadlineExceeded
		}
		// The shell accepts whole seconds. Round up so it does not shorten
		// the caller's deadline, and never pass zero for a bounded context.
		seconds := int64((remaining-1)/time.Second) + 1
		limit = int(min(seconds, int64(math.MaxInt)))
	}
	report, err := supervised.RunShellLimited(ctx, l.client, label, limit, 1024, argv...)
	if err != nil {
		l.cleanupMarker, _ = supervised.Unconfirmed(err)
		return immutableObjectCacheOutput{}, err
	}
	if report.Stopped {
		return immutableObjectCacheOutput{}, context.DeadlineExceeded
	}
	if report.ExitCode != 0 || report.StdoutTruncated {
		return immutableObjectCacheOutput{}, fmt.Errorf("immutable cache %s command failed with exit %d", op, report.ExitCode)
	}
	return immutableObjectCacheOutput{op: op, out: report.Stdout}, nil
}

// immutableObjectCacheOp runs one operation of immutableObjectCacheScript and
// returns its key=value lines, unvalidated: each operation's parser checks
// the fields it needs.
func immutableObjectCacheOp(ctx context.Context, client testcontainers.Container, key, op string, args ...string) (immutableObjectCacheOutput, error) {
	out, err := multicluster.Exec(ctx, client, append([]string{"sh", "-c", immutableObjectCacheScript, key, op}, args...)...)
	if err != nil {
		return immutableObjectCacheOutput{}, err
	}
	return immutableObjectCacheOutput{op: op, out: out}, nil
}
