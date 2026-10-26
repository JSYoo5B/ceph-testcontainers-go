//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

// The ceph-fuse client containers need /dev/fuse, CAP_SYS_ADMIN and an
// unconfined AppArmor profile, which no required CI environment is proven
// to allow, so this is an optional batch: build it with the cephfs_fuse tag
// (go test -tags all,cephfs_fuse) or run it with tag_scenarios.py optional
// --batch cephfs_fuse. See docs/research/ceph-fuse-mount.md.

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// fuseSettings is one set of container settings for a ceph-fuse client
// container: the /dev/fuse device, CAP_SYS_ADMIN and security options. Only
// the mounting client container gets them; the cluster's daemons and the
// fixture's own client container keep their default privileges.
type fuseSettings struct {
	name        string
	device      bool
	sysAdmin    bool
	securityOpt []string
}

// fuseMinimumSettings are the smallest settings that mounted on native,
// rootful Docker Engine with its default AppArmor and seccomp profiles.
var fuseMinimumSettings = fuseSettings{name: "device+SYS_ADMIN+apparmor=unconfined", device: true, sysAdmin: true, securityOpt: []string{"apparmor=unconfined"}}

// fuseSmallerSettings are the settings below the minimum that the test
// tries first, in order. The default seccomp profile already allows mount(2)
// to a container with CAP_SYS_ADMIN, so seccomp=unconfined alone does not
// replace the AppArmor allowance.
var fuseSmallerSettings = []fuseSettings{
	{name: "none"},
	{name: "device", device: true},
	{name: "device+SYS_ADMIN", device: true, sysAdmin: true},
	{name: "device+SYS_ADMIN+seccomp=unconfined", device: true, sysAdmin: true, securityOpt: []string{"seccomp=unconfined"}},
}

func (s fuseSettings) customizer() testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
		if s.device {
			host.Devices = append(host.Devices, container.DeviceMapping{PathOnHost: "/dev/fuse", PathInContainer: "/dev/fuse", CgroupPermissions: "rwm"})
		}
		if s.sysAdmin {
			host.CapAdd = append(host.CapAdd, "SYS_ADMIN")
		}
		host.SecurityOpt = append(host.SecurityOpt, s.securityOpt...)
	})
}

// fuseRefusal is the typed refusal of a FUSE mount, decided before anything
// in the cluster or the container's mount table changes. Requirement names
// what is missing: the ceph-fuse binary, the /dev/fuse device, CAP_SYS_ADMIN,
// a security profile that allows the mount, or the engine's device mapping.
type fuseRefusal struct {
	Requirement string
	Detail      string
}

func (r *fuseRefusal) Error() string {
	return "ceph-fuse mount refused: " + r.Requirement + ": " + r.Detail
}

// Requirements that a fuseRefusal names.
const (
	fuseNeedsBinary   = "ceph-fuse binary"
	fuseNeedsDevice   = "/dev/fuse device"
	fuseNeedsSysAdmin = "CAP_SYS_ADMIN"
	fuseNeedsProfile  = "security profile"
	fuseNeedsEngine   = "engine device mapping"
	fuseNeedsMount    = "fuse mount"
)

// fuseAdmissionScript inspects the container without touching the cluster:
// the ceph-fuse binary, the /dev/fuse device, CAP_SYS_ADMIN in the effective
// set and the security label, and then the kernel's answer to one FUSE
// mount(2) of the device on a scratch directory, detached at once. Nobody
// serves the connection, so the probe neither blocks nor reaches Ceph.
const fuseAdmissionScript = `
import ctypes, errno, json, os
result = {"binary": os.access("/usr/bin/ceph-fuse", os.X_OK)}
with open("/proc/self/status") as status:
    effective = [line.split()[1] for line in status if line.startswith("CapEff:")][0]
result["cap_sys_admin"] = bool(int(effective, 16) >> 21 & 1)
try:
    with open("/proc/self/attr/current") as label:
        result["label"] = label.read().strip("\x00\n ")
except OSError as error:
    result["label"] = "unreadable: %s" % os.strerror(error.errno)
try:
    fd = os.open("/dev/fuse", os.O_RDWR)
except OSError as error:
    result["device_errno"] = error.errno
else:
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    target = b"/run/fuse-admission"
    os.makedirs(target, exist_ok=True)
    options = ("fd=%d,rootmode=40000,user_id=0,group_id=0" % fd).encode()
    if libc.mount(b"fuse-admission", target, b"fuse", 0, options) == 0:
        result["mount_errno"] = 0
        libc.umount2(target, 2)
    else:
        result["mount_errno"] = ctypes.get_errno()
    os.close(fd)
print(json.dumps(result))
`

type fuseAdmission struct {
	Binary      bool   `json:"binary"`
	CapSysAdmin bool   `json:"cap_sys_admin"`
	Label       string `json:"label"`
	DeviceErrno *int   `json:"device_errno"`
	MountErrno  *int   `json:"mount_errno"`
}

// refusal classifies the admission: nil when a mount can work.
func (a fuseAdmission) refusal() *fuseRefusal {
	switch {
	case !a.Binary:
		return &fuseRefusal{fuseNeedsBinary, "the image has no executable /usr/bin/ceph-fuse"}
	case a.DeviceErrno != nil:
		return &fuseRefusal{fuseNeedsDevice, "open /dev/fuse: errno " + strconv.Itoa(*a.DeviceErrno)}
	case a.MountErrno == nil:
		return &fuseRefusal{fuseNeedsMount, "the probe reported no mount result"}
	case *a.MountErrno == 1:
		return &fuseRefusal{fuseNeedsSysAdmin, fmt.Sprintf("mount(2) of fuse: EPERM (CAP_SYS_ADMIN effective=%t)", a.CapSysAdmin)}
	case *a.MountErrno == 13:
		return &fuseRefusal{fuseNeedsProfile, fmt.Sprintf("mount(2) of fuse: EACCES under security label %q", a.Label)}
	case *a.MountErrno != 0:
		return &fuseRefusal{fuseNeedsMount, "mount(2) of fuse: errno " + strconv.Itoa(*a.MountErrno)}
	}
	return nil
}

// admitFuse runs the admission in ctr and returns its typed refusal, or nil.
func admitFuse(t *testing.T, ctx context.Context, ctr testcontainers.Container) (fuseAdmission, *fuseRefusal) {
	t.Helper()
	code, out, err := sessionExec(ctx, ctr, "python3", "-c", fuseAdmissionScript)
	var admission fuseAdmission
	if err != nil || code != 0 || json.Unmarshal([]byte(lastJSONLine(out)), &admission) != nil {
		t.Fatalf("FUSE admission probe: exit=%d err=%v out=%.1024s", code, err, out)
	}
	return admission, admission.refusal()
}

// fuseClientImage returns the image for the FUSE client containers: the
// control image when it ships ceph-fuse, otherwise the official image of the
// cluster's release, which does. The official role images keep their scope
// and do not ship ceph-fuse.
func fuseClientImage(t *testing.T, ctx context.Context, m *cephFSSingleMDS) (string, *fuseRefusal) {
	t.Helper()
	control := m.cluster.ControlImage()
	if code, _, err := sessionExec(ctx, m.client, "test", "-x", "/usr/bin/ceph-fuse"); err != nil {
		t.Fatal(err)
	} else if code == 0 {
		return control, nil
	}
	refused := &fuseRefusal{fuseNeedsBinary, "control image " + control + " has no executable /usr/bin/ceph-fuse"}
	version := m.cluster.CephVersion()
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
		t.Fatalf("%v, and Ceph version %q names no official image", refused, version)
	}
	return "quay.io/ceph/ceph:v" + version, refused
}

// startFuseClient starts an idle client container of identity with
// settings. An engine that cannot map /dev/fuse fails the creation, and the
// result is a fuseRefusal.
func startFuseClient(t *testing.T, ctx context.Context, m *cephFSSingleMDS, image string, identity *ceph.ClientConfig, settings fuseSettings) (testcontainers.Container, error) {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, image, m.cluster.WithClientIdentity(identity), ceph.WithIdleEntrypoint(), settings.customizer())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		if settings.device {
			return nil, &fuseRefusal{fuseNeedsEngine, fmt.Sprintf("settings %s: %v", settings.name, err)}
		}
		t.Fatal(err)
	}
	return ctr, nil
}

// fuseMount is one ceph-fuse process, run in the foreground under
// RunNativeCommand, and its mount point.
type fuseMount struct {
	cluster  *ceph.Container
	ctr      testcontainers.Container
	user     string
	root     string
	point    string
	hostname string
	cancel   context.CancelFunc
	done     chan struct{}
	result   ceph.NativeCommandResult
	err      error
}

// startFuseMount starts ceph-fuse for identity's root on point in ctr and
// registers its cancellation and confirmed stop before anything can fail.
func startFuseMount(t *testing.T, ctx context.Context, m *cephFSSingleMDS, ctr testcontainers.Container, identity *ceph.ClientConfig, root, point string) *fuseMount {
	t.Helper()
	execOutput(t, ctx, ctr, "mkdir", "-p", point)
	mountCtx, cancel := context.WithCancel(ctx)
	mount := &fuseMount{cluster: m.cluster, ctr: ctr, user: identity.User(), root: root, point: point, cancel: cancel, done: make(chan struct{})}
	mount.hostname = execOutput(t, ctx, ctr, "cat", "/proc/sys/kernel/hostname")
	argv := []string{"ceph-fuse", "-f", "--id", identity.User(), "--client_fs", m.fs.FilesystemName, "--client_mount_timeout", "30", "-r", root, point}
	go func() {
		defer close(mount.done)
		mount.result, mount.err = m.cluster.RunNativeCommand(mountCtx, ctr, ceph.NativeCommand{
			Argv: argv, Timeout: 15 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 256 << 10, Label: "ceph-fuse",
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-mount.done:
		case <-time.After(2 * time.Minute):
			t.Error("ceph-fuse did not return after cancellation")
			return
		}
		var unstopped *ceph.NativeCommandUnstoppedError
		if errors.As(mount.err, &unstopped) {
			stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := unstopped.Stop(stopCtx); err != nil {
				t.Errorf("stop ceph-fuse: %v", err)
			}
		}
	})
	return mount
}

// exited describes how ceph-fuse ended, with its redacted stderr tail.
func (f *fuseMount) exited(ctx context.Context) string {
	stderr := f.result.Stderr
	if len(stderr) > 2048 {
		stderr = stderr[len(stderr)-2048:]
	}
	if redacted, err := f.cluster.RedactSecrets(ctx, stderr); err == nil {
		stderr = redacted
	}
	return fmt.Sprintf("exit=%d timed_out=%t err=%v stderr=%s", f.result.ExitCode, f.result.TimedOut, f.err, strings.TrimSpace(string(stderr)))
}

// fuseFailures are the messages with which libfuse reports a refused mount,
// by the requirement that the admission names.
var fuseFailures = map[string]string{
	fuseNeedsDevice:   "fuse: device not found",
	fuseNeedsSysAdmin: "mount failed: Operation not permitted",
	fuseNeedsProfile:  "fuse: mount failed: Permission denied",
}

// failure returns the lines in which libfuse and ceph-fuse report why the
// mount did not start.
func (f *fuseMount) failure() string {
	var lines []string
	for line := range strings.Lines(string(f.result.Stderr)) {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "fuse:") || strings.HasPrefix(line, "fusermount:") || strings.Contains(line, "failed to start") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, " | ")
}

// wait waits, bounded, until ceph-fuse has returned.
func (f *fuseMount) wait(t *testing.T, ctx context.Context, bound time.Duration) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(bound):
		t.Fatalf("ceph-fuse on %s did not return within %s", f.point, bound)
	case <-ctx.Done():
		t.Fatal("ceph-fuse did not return:", ctx.Err())
	}
}

// fuseMountState is what the container and the MDS report about one mount.
type fuseMountState struct {
	mounted   bool
	processes []string
	session   cephfs.Session
	sessioned bool
}

// fuseState reads the container's mount table and ceph-fuse processes and
// the MDS sessions of f's identity, host and root.
func (f *fuseMount) state(ctx context.Context, fs *cephfs.Filesystem) (fuseMountState, error) {
	var state fuseMountState
	code, mounts, err := sessionExec(ctx, f.ctr, "cat", "/proc/mounts")
	if err != nil || code != 0 {
		return state, fmt.Errorf("read /proc/mounts: exit %d: %v", code, err)
	}
	for line := range strings.Lines(mounts) {
		if fields := strings.Fields(line); len(fields) >= 3 && fields[1] == f.point && fields[2] == "fuse.ceph-fuse" {
			state.mounted = true
		}
	}
	// The ceph-fuse processes whose last argument is this mount point.
	code, pids, err := sessionExec(ctx, f.ctr, "sh", "-c", `for p in /proc/[0-9]*; do
  [ "$(cat "$p/comm" 2>/dev/null)" = ceph-fuse ] || continue
  [ "$(tr '\0' '\n' < "$p/cmdline" 2>/dev/null | tail -n 1)" = "$1" ] && echo "${p#/proc/}"
done; true`, "sh", f.point)
	if err != nil || code != 0 {
		return state, fmt.Errorf("list ceph-fuse processes: exit %d: %v", code, err)
	}
	state.processes = strings.Fields(pids)
	sessions, err := fs.Sessions(ctx)
	if err != nil {
		return state, err
	}
	for _, session := range sessions {
		if session.EntityID == f.user && session.Hostname == f.hostname && session.Root == f.root {
			if state.sessioned {
				return state, fmt.Errorf("two sessions of %s on %s: %+v and %+v", f.user, f.hostname, state.session, session)
			}
			state.session, state.sessioned = session, true
		}
	}
	return state, nil
}

// ready waits, bounded, until the mount is in the container's mount table
// and the MDS holds an open session of f's identity, host and root from the
// running ceph-fuse process.
func (f *fuseMount) ready(t *testing.T, ctx context.Context, fs *cephfs.Filesystem) fuseMountState {
	t.Helper()
	seen, err := observe(ctx, 2*time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (fuseMountState, bool, error) {
		select {
		case <-f.done:
			return fuseMountState{}, false, errors.New("ceph-fuse ended")
		default:
		}
		state, err := f.state(probeCtx, fs)
		if err != nil {
			return state, false, err
		}
		return state, state.mounted && state.sessioned && state.session.State == "open" &&
			len(state.processes) == 1 && state.session.PID == state.processes[0], nil
	})
	if err != nil {
		t.Fatalf("ceph-fuse mount of %s on %s not ready: last %+v: %v; %s", f.root, f.point, seen.Value, err, f.exited(ctx))
	}
	return seen.Value
}

// gone waits, bounded, until the container has neither the mount nor a
// ceph-fuse process for it and the MDS no session of f's identity, host and
// root.
func (f *fuseMount) gone(t *testing.T, ctx context.Context, fs *cephfs.Filesystem) {
	t.Helper()
	seen, err := observe(ctx, 2*time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (fuseMountState, bool, error) {
		state, err := f.state(probeCtx, fs)
		return state, err == nil && !state.mounted && len(state.processes) == 0 && !state.sessioned, err
	})
	if err != nil {
		t.Fatalf("ceph-fuse mount of %s on %s left: %+v: %v", f.root, f.point, seen.Value, err)
	}
}

// fcntlLockActor keeps one O_RDWR fd on a file in a FUSE mount and takes
// POSIX (fcntl) record locks on its first 4096 bytes. It opens the file and
// publishes receipt 1 with its inode. Then for each operation it waits at
// gate "op-<n>", runs it and publishes receipt n+2 with its errno and how
// long the call took: "wr-nb" is F_SETLK with F_WRLCK, "wr" a blocking
// F_SETLKW with F_WRLCK, "un" F_SETLK with F_UNLCK, and "get" F_GETLK for a
// write lock, whose answer it publishes. After the "finish" gate it closes
// the fd and publishes the last receipt. Arguments after the common ones:
// the file and the comma-separated operations. The filesystem argument is
// unused: the actor reaches CephFS only through the kernel's FUSE mount.
const fcntlLockActor cephFSActorScript = cephFSActorHelpers + `
import fcntl, struct
path, operations = sys.argv[4], sys.argv[5].split(",")
length = 4096
modes = {"wr-nb": fcntl.LOCK_EX | fcntl.LOCK_NB, "wr": fcntl.LOCK_EX, "un": fcntl.LOCK_UN}
fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
publish(1, inode=os.fstat(fd).st_ino)
for index, operation in enumerate(operations):
    gate("op-%d" % index)
    started = time.monotonic()
    result = {"errno": 0}
    try:
        if operation == "get":
            query = struct.pack("hhqqi4x", fcntl.F_WRLCK, os.SEEK_SET, 0, length, 0)
            got = struct.unpack("hhqqi4x", fcntl.fcntl(fd, fcntl.F_GETLK, query))
            result["lock"] = {"type": got[0], "start": got[2], "len": got[3], "pid": got[4]}
        else:
            fcntl.lockf(fd, modes[operation], length, 0, os.SEEK_SET)
    except OSError as error:
        result["errno"] = error.errno or -1
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    publish(index + 2, **result)
gate("finish")
closing = {}
try:
    os.close(fd)
except OSError as error:
    closing["close_errno"] = error.errno or -1
publish(len(operations) + 2, closed=True, **closing)
`

// fcntlActor is one fcntlLockActor with its operations.
type fcntlActor struct {
	*cephFSActorRun
	inode uint64
	ops   []string
}

func startFcntlActor(t *testing.T, ctx context.Context, m *cephFSSingleMDS, in testcontainers.Container, workdir, path string, ops ...string) *fcntlActor {
	t.Helper()
	run := startCephFSActor(t, ctx, m.cluster, in, fcntlLockActor, m.fs.FilesystemName, workdir, path, strings.Join(ops, ","))
	opened := run.receipt(t, ctx, 1)
	if opened.Inode == 0 {
		t.Fatalf("fcntl actor receipt 1 = %+v", opened)
	}
	return &fcntlActor{cephFSActorRun: run, inode: opened.Inode, ops: ops}
}

func (a *fcntlActor) start(t *testing.T, ctx context.Context, index int) {
	t.Helper()
	a.open(t, ctx, fmt.Sprintf("op-%d", index))
}

func (a *fcntlActor) run(t *testing.T, ctx context.Context, index int) cephFSActorReceipt {
	t.Helper()
	a.start(t, ctx, index)
	return a.receipt(t, ctx, index+2)
}

// close lets the actor close its fd and waits for it; it returns the
// closing receipt.
func (a *fcntlActor) close(t *testing.T, ctx context.Context) cephFSActorReceipt {
	t.Helper()
	a.open(t, ctx, "finish")
	closed := a.receipt(t, ctx, len(a.ops)+2)
	if !closed.Closed {
		t.Fatalf("closing receipt = %+v", closed)
	}
	a.wait(t, ctx)
	return closed
}

// conflicts reports whether errno is the conflict answer of F_SETLK, which
// POSIX allows as EACCES or EAGAIN.
func fcntlConflict(errno int) bool { return errno == 11 || errno == 13 }

// fuseExec runs a short Python snippet in ctr and decodes its JSON line.
func fuseExec(t *testing.T, ctx context.Context, ctr testcontainers.Container, value any, script string, args ...string) {
	t.Helper()
	code, out, err := sessionExec(ctx, ctr, append([]string{"python3", "-c", script}, args...)...)
	if err != nil || code != 0 || json.Unmarshal([]byte(lastJSONLine(out)), value) != nil {
		t.Fatalf("python in %s: exit=%d err=%v out=%.1024s", ctr.GetContainerID()[:12], code, err, out)
	}
}

// fuseWriteScript writes the hex bytes argv[2] to argv[1] through the mount,
// fsyncs and closes, and prints the inode and size.
const fuseWriteScript = `
import json, os, sys
fd = os.open(sys.argv[1], os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
os.write(fd, bytes.fromhex(sys.argv[2]))
os.fsync(fd)
stat = os.fstat(fd)
os.close(fd)
print(json.dumps({"inode": stat.st_ino, "size": stat.st_size}))
`

// fuseReadScript reads argv[1] through the mount and prints its bytes as hex
// with its inode, or the errno of the failing call.
const fuseReadScript = `
import json, os, sys
try:
    with open(sys.argv[1], "rb") as opened:
        data = opened.read()
        inode = os.fstat(opened.fileno()).st_ino
    print(json.dumps({"inode": inode, "data": data.hex()}))
except OSError as error:
    print(json.dumps({"errno": error.errno}))
`

type fuseRead struct {
	Inode uint64 `json:"inode"`
	Size  int64  `json:"size"`
	Data  string `json:"data"`
	Errno int    `json:"errno"`
}

// cephFSAdminMkdir creates path through libcephfs in the control container.
func cephFSAdminMkdir(t *testing.T, ctx context.Context, m *cephFSSingleMDS, path string) {
	t.Helper()
	var made struct {
		Inode uint64 `json:"inode"`
	}
	fuseExec(t, ctx, clusteraccess.ControlContainer(t, m.cluster), &made, `
import cephfs, json, sys
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    fs.mkdir(sys.argv[2].encode(), 0o755)
    print(json.dumps({"inode": fs.stat(sys.argv[2].encode()).st_ino}))
finally:
    fs.unmount()
    fs.shutdown()
`, m.fs.FilesystemName, path)
	if made.Inode == 0 {
		t.Fatalf("mkdir %s: %+v", path, made)
	}
}

// blocklisted reports whether address is in the OSDMap blocklist.
func blocklisted(t *testing.T, ctx context.Context, cluster *ceph.Container, address string) bool {
	t.Helper()
	entries, err := cluster.BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == address })
}

// staleState waits, bounded, until a mount whose ceph-fuse process was
// killed has no process left, and returns what remains: the dead mount in
// the container and the session on the MDS.
func (f *fuseMount) staleState(t *testing.T, ctx context.Context, fs *cephfs.Filesystem) fuseMountState {
	t.Helper()
	seen, err := observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (fuseMountState, bool, error) {
		state, err := f.state(probeCtx, fs)
		return state, err == nil && len(state.processes) == 0, err
	})
	if err != nil {
		t.Fatalf("killed ceph-fuse on %s: %+v: %v", f.point, seen.Value, err)
	}
	return seen.Value
}

// cleanStale unmounts a dead FUSE mount and evicts its session, which is
// what a fixture must do after a killed or cancelled ceph-fuse, and waits
// until both are gone. It returns whether the evicted address was
// blocklisted.
func (f *fuseMount) cleanStale(t *testing.T, ctx context.Context, m *cephFSSingleMDS, session cephfs.Session) bool {
	t.Helper()
	execOutput(t, ctx, f.ctr, "umount", f.point)
	if _, err := m.cluster.Ceph(ctx, "tell", "mds."+m.mds.Name, "client", "evict", "id="+strconv.FormatUint(session.ID, 10)); err != nil {
		t.Fatal("evict:", err)
	}
	f.gone(t, ctx, m.fs)
	return blocklisted(t, ctx, m.cluster, session.Address)
}

// testCephFSFuseMount mounts a path-restricted CephX identity's directory
// with ceph-fuse in client containers. The settings below the minimum are
// tried first: the admission probe refuses each with a typed reason, and
// ceph-fuse itself then fails and leaves no mount, process or session. With
// the minimum settings, two containers mount; bytes written through one
// mount are read back through libcephfs and through the other mount, and a
// mount of the filesystem root is refused. The two mounts contend for one
// POSIX record lock, and the MDS holds the waiting F_SETLKW. The holder's
// ceph-fuse is then killed while it holds the lock, the other mount
// unmounts cleanly, and a third mount is cancelled through its context.
func testCephFSFuseMount(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	image, controlRefusal := fuseClientImage(t, ctx, m)
	if controlRefusal != nil {
		t.Logf("%v; the FUSE client containers use %s", controlRefusal, image)
	}
	const root, point = "/fuse", "/mnt/cephfs"
	cephFSAdminMkdir(t, ctx, m, root)
	identity, err := m.cluster.CreateClient(ctx, "fuse-mount", ceph.ClientCaps{
		Mon: "allow r", MDS: "allow rw path=" + root, OSD: "allow rw tag cephfs data=" + m.fs.FilesystemName,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Settings below the minimum.
	var refusals []string
	for _, settings := range fuseSmallerSettings {
		ctr, err := startFuseClient(t, ctx, m, image, identity, settings)
		if err != nil {
			refusals = append(refusals, settings.name+": "+err.Error())
			continue
		}
		_, refused := admitFuse(t, ctx, ctr)
		if settings.name == "none" && (refused == nil || refused.Requirement != fuseNeedsDevice) {
			t.Fatalf("a client container without settings: admission %v, want the /dev/fuse device refused", refused)
		}
		if refused == nil {
			refusals = append(refusals, settings.name+": admitted")
		} else {
			attempt := startFuseMount(t, ctx, m, ctr, identity, root, point)
			attempt.wait(t, ctx, 2*time.Minute)
			if want, known := fuseFailures[refused.Requirement]; attempt.err != nil || attempt.result.ExitCode == 0 ||
				(known && !strings.Contains(attempt.failure(), want)) {
				t.Fatalf("ceph-fuse with settings %s, which the admission refused (%v): %s", settings.name, refused, attempt.exited(ctx))
			}
			attempt.gone(t, ctx, m.fs)
			refusals = append(refusals, fmt.Sprintf("%s: %v; ceph-fuse exit %d: %s", settings.name, refused, attempt.result.ExitCode, attempt.failure()))
		}
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Fatal(err)
		}
	}
	for _, refusal := range refusals {
		t.Logf("settings %s", refusal)
	}

	// The minimum settings.
	var clients []testcontainers.Container
	for range 2 {
		ctr, err := startFuseClient(t, ctx, m, image, identity, fuseMinimumSettings)
		if err != nil {
			t.Fatal(err)
		}
		if _, refused := admitFuse(t, ctx, ctr); refused != nil {
			t.Fatalf("settings %s: %v", fuseMinimumSettings.name, refused)
		}
		clients = append(clients, ctr)
	}
	a, b := clients[0], clients[1]
	started := time.Now()
	mountA := startFuseMount(t, ctx, m, a, identity, root, point)
	stateA := mountA.ready(t, ctx, m.fs)
	readyAfter := time.Since(started)
	mountB := startFuseMount(t, ctx, m, b, identity, root, point)
	stateB := mountB.ready(t, ctx, m.fs)
	if stateA.session.ID == stateB.session.ID {
		t.Fatalf("both mounts have client.%d", stateA.session.ID)
	}

	// The identity's caps admit only its directory.
	whole := startFuseMount(t, ctx, m, b, identity, "/", "/mnt/root")
	whole.wait(t, ctx, 2*time.Minute)
	if whole.err != nil || whole.result.ExitCode == 0 {
		t.Fatalf("ceph-fuse of / with caps for %s: %s", root, whole.exited(ctx))
	}
	whole.gone(t, ctx, m.fs)

	// Bytes through one mount, read back through libcephfs and the other.
	want := make([]byte, 4096+17)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	var wrote fuseRead
	fuseExec(t, ctx, a, &wrote, fuseWriteScript, point+"/data", hex.EncodeToString(want))
	independent := readCephFSIndependently(t, ctx, m, root+"/data")
	if wrote.Size != int64(len(want)) || independent.Data != hex.EncodeToString(want) || independent.Size != wrote.Size || independent.Inode != wrote.Inode {
		t.Fatalf("written through FUSE %+v, read through libcephfs inode %d size %d data equal %t", wrote, independent.Inode, independent.Size, independent.Data == hex.EncodeToString(want))
	}
	var other fuseRead
	fuseExec(t, ctx, b, &other, fuseReadScript, point+"/data")
	if other.Errno != 0 || other.Data != hex.EncodeToString(want) || other.Inode != wrote.Inode {
		t.Fatalf("read through the second mount: inode %d errno %d equal %t", other.Inode, other.Errno, other.Data == hex.EncodeToString(want))
	}

	// One POSIX record lock between the two mounts.
	execOutput(t, ctx, a, "mkdir", "-p", point+"/locks")
	const lockFile = point + "/locks/file"
	holder := startFcntlActor(t, ctx, m, a, "/tmp/fcntl-holder", lockFile, "wr-nb", "un", "wr-nb", "wr-nb")
	contender := startFcntlActor(t, ctx, m, b, "/tmp/fcntl-contender", lockFile, "wr-nb", "get", "wr", "un", "wr-nb", "wr-nb", "un")
	if holder.inode != contender.inode {
		t.Fatalf("holder inode %d, contender inode %d", holder.inode, contender.inode)
	}
	clientA, clientB := stateA.session.ID, stateB.session.ID
	if locked := holder.run(t, ctx, 0); locked.Errno != 0 {
		t.Fatalf("holder's F_SETLK write lock: %+v", locked)
	}
	refused := contender.run(t, ctx, 0)
	if !fcntlConflict(refused.Errno) {
		t.Fatalf("contender's F_SETLK while the holder holds the lock: %+v, want EAGAIN or EACCES", refused)
	}
	queried := contender.run(t, ctx, 1)
	if queried.Errno != 0 || queried.Lock == nil || queried.Lock.Type != 1 || queried.Lock.Start != 0 || queried.Lock.Len != 4096 {
		t.Fatalf("contender's F_GETLK: %+v lock %+v, want the holder's F_WRLCK on [0, 4096)", queried, queried.Lock)
	}
	contender.start(t, ctx, 2)
	waiting := mdsWaitingFlock(t, ctx, m, clientB, true)
	inode := fmt.Sprintf("#0x%x ", holder.inode)
	if waiting.flagPoint != "failed to add lock, waiting" || !strings.Contains(waiting.description, "rule 1, type 2,") ||
		!strings.Contains(waiting.description, "wait 1 ") || !strings.Contains(waiting.description, inode) {
		t.Fatalf("the MDS does not hold client.%d's fcntl write lock waiting on inode %d: %+v", clientB, holder.inode, waiting)
	}
	time.Sleep(3 * time.Second)
	if still := mdsWaitingFlock(t, ctx, m, clientB, true); still.tid != waiting.tid || still.flagPoint != waiting.flagPoint {
		t.Fatalf("waiting fcntl lock changed: %+v, before %+v", still, waiting)
	}
	if contender.published(ctx, 4) {
		t.Fatal("the contender's F_SETLKW returned while the holder held the lock")
	}
	unlocked := time.Now()
	if released := holder.run(t, ctx, 1); released.Errno != 0 {
		t.Fatalf("holder's F_UNLCK: %+v", released)
	}
	granted := contender.receipt(t, ctx, 4)
	if granted.Errno != 0 || time.Duration(granted.ElapsedMS)*time.Millisecond < 3*time.Second {
		t.Fatalf("contender's F_SETLKW: %+v, want 0 after waiting for the unlock", granted)
	}
	grantedAfter := time.Since(unlocked)
	mdsWaitingFlock(t, ctx, m, clientB, false)
	reversed := holder.run(t, ctx, 2)
	if !fcntlConflict(reversed.Errno) {
		t.Fatalf("former holder's F_SETLK while the contender holds the lock: %+v", reversed)
	}
	if released := contender.run(t, ctx, 3); released.Errno != 0 {
		t.Fatalf("contender's F_UNLCK: %+v", released)
	}
	if relocked := holder.run(t, ctx, 3); relocked.Errno != 0 {
		t.Fatalf("holder's second F_SETLK: %+v", relocked)
	}

	// The holder's ceph-fuse is killed while it holds the lock.
	execOutput(t, ctx, a, "kill", "-KILL", stateA.processes[0])
	mountA.wait(t, ctx, time.Minute)
	if mountA.err != nil || mountA.result.ExitCode == 0 {
		t.Fatalf("killed ceph-fuse: %s", mountA.exited(ctx))
	}
	stale := mountA.staleState(t, ctx, m.fs)
	var deadRead fuseRead
	fuseExec(t, ctx, a, &deadRead, fuseReadScript, point+"/data")
	if !stale.mounted || !stale.sessioned || stale.session.ID != clientA || stale.session.State != "open" || deadRead.Errno != 107 {
		t.Fatalf("after the kill: %+v, read errno %d; want the dead mount, the open session of client.%d and ENOTCONN", stale, deadRead.Errno, clientA)
	}
	heldByDead := contender.run(t, ctx, 4)
	if !fcntlConflict(heldByDead.Errno) {
		t.Fatalf("contender's F_SETLK while the killed holder's session is open: %+v", heldByDead)
	}
	holderClosed := holder.close(t, ctx)
	evictedBlocklisted := mountA.cleanStale(t, ctx, m, stateA.session)
	if afterEviction := contender.run(t, ctx, 5); afterEviction.Errno != 0 {
		t.Fatalf("contender's F_SETLK after the killed holder's eviction: %+v", afterEviction)
	}
	if released := contender.run(t, ctx, 6); released.Errno != 0 {
		t.Fatalf("contender's last F_UNLCK: %+v", released)
	}
	if closed := contender.close(t, ctx); closed.CloseErrno != 0 {
		t.Fatalf("contender's close: %+v", closed)
	}

	// The other mount unmounts cleanly.
	execOutput(t, ctx, b, "umount", point)
	mountB.wait(t, ctx, time.Minute)
	if mountB.err != nil || mountB.result.ExitCode != 0 {
		t.Fatalf("ceph-fuse after umount: %s", mountB.exited(ctx))
	}
	mountB.gone(t, ctx, m.fs)
	if blocklisted(t, ctx, m.cluster, stateB.session.Address) {
		t.Fatalf("client.%d %s blocklisted after a clean unmount", clientB, stateB.session.Address)
	}

	// A third mount is cancelled through its context.
	cancelled := startFuseMount(t, ctx, m, b, identity, root, "/mnt/cancel")
	stateC := cancelled.ready(t, ctx, m.fs)
	cancelledAt := time.Now()
	cancelled.cancel()
	cancelled.wait(t, ctx, time.Minute)
	stoppedAfter := time.Since(cancelledAt)
	var unstopped *ceph.NativeCommandUnstoppedError
	if errors.As(cancelled.err, &unstopped) || !errors.Is(cancelled.err, context.Canceled) {
		t.Fatalf("cancelled ceph-fuse: %s", cancelled.exited(ctx))
	}
	afterCancel := cancelled.staleState(t, ctx, m.fs)
	if !afterCancel.sessioned || afterCancel.session.ID != stateC.session.ID {
		t.Fatalf("after the cancellation: %+v, want the session of client.%d", afterCancel, stateC.session.ID)
	}
	cancelledBlocklisted := cancelled.cleanStale(t, ctx, m, stateC.session)

	for _, ctr := range clients {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Fatal(err)
		}
		if _, err := ctr.State(ctx); err == nil {
			t.Fatalf("client container %s still exists", ctr.GetContainerID()[:12])
		}
	}

	t.Logf("CEPHFS_FUSE_MOUNT_RECEIPT %s image=%s settings=%s: client.%s mounts %s only (a mount of / exits %d); mount A client.%d (MDS session pid %s on %s) ready %s after the start, mount B client.%d; %d bytes written through A (inode %d) read back through libcephfs and through B; killed ceph-fuse (exit %d) left the mount (ENOTCONN) and an open session until umount and eviction (blocklisted %t); umount of B ended ceph-fuse with 0 and closed its session, not blocklisted; a cancelled ceph-fuse stopped in %s and left the mount (stale %t) and session until umount and eviction (blocklisted %t); client containers removed",
		m.version, image, fuseMinimumSettings.name, identity.User(), root, whole.result.ExitCode, clientA, stateA.session.PID, mountA.hostname, readyAfter.Round(time.Millisecond),
		clientB, len(want), wrote.Inode, mountA.result.ExitCode, evictedBlocklisted, stoppedAfter.Round(time.Millisecond), afterCancel.mounted, cancelledBlocklisted)
	t.Logf("CEPHFS_FCNTL_LOCK_RECEIPT %s mds.%s inode=%d: holder client.%d F_SETLK F_WRLCK [0,4096)=0; contender client.%d F_SETLK=%d, F_GETLK=%+v; its F_SETLKW waited in the MDS as %q at %q (events %v); holder F_UNLCK=0, the waiting lock was granted %d ms after it started (%s after the unlock) and left the MDS; holder F_SETLK then %d; the killed holder's lock refused the contender (%d) until its eviction (holder close errno %d), then F_SETLK=0",
		m.version, m.mds.Name, holder.inode, clientA, clientB, refused.Errno, *queried.Lock, waiting.description, waiting.flagPoint, waiting.events,
		granted.ElapsedMS, grantedAfter.Round(time.Millisecond), reversed.Errno, heldByDead.Errno, holderClosed.CloseErrno)
}

// A path-restricted CephX identity mounts its directory with ceph-fuse in
// client containers that get /dev/fuse, CAP_SYS_ADMIN and an unconfined
// AppArmor profile; smaller settings are refused with a typed reason before
// any mount. Bytes written through the mount are read back through
// libcephfs, two mounts contend for one POSIX record lock that the MDS holds
// waiting, and a killed, an unmounted and a cancelled ceph-fuse leave no
// mount, process, session or container behind once cleaned up.
func TestCephFSFuseMount(t *testing.T) {
	testCephFSFuseMount(t)
}
