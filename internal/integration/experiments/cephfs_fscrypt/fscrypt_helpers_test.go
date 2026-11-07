//go:build all || integration

package cephfs_fscrypt_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Client paths of the case. Each mounts CephFS in a client container that
// the test starts and owns.
const (
	kernelPath = "kernel"
	fusePath   = "fuse"
)

// Requirements that a fscryptRefusal names. Each one is a missing
// prerequisite of the environment or the client build, decided before the
// test changes anything in the cluster or in the client container's mount
// table, or, for fscryptNeedsClient, from the client's answer to a read-only
// policy query on an empty directory of a fresh mount.
const (
	fscryptNeedsKernelFS = "kernel ceph filesystem"
	fscryptNeedsBinary   = "ceph-fuse binary"
	fscryptNeedsDevice   = "/dev/fuse device"
	fscryptNeedsSysAdmin = "CAP_SYS_ADMIN"
	fscryptNeedsProfile  = "security profile"
	fscryptNeedsAESKey   = "AES Cephx key"
	fscryptNeedsClient   = "client fscrypt support"
)

// fscryptRefusal is the typed refusal of one client path: what is missing
// and the native answer that shows it.
type fscryptRefusal struct {
	Path        string
	Requirement string
	Detail      string
}

func (r *fscryptRefusal) Error() string {
	return "CephFS fscrypt over the " + r.Path + " client refused: " + r.Requirement + ": " + r.Detail
}

// Errno values of the Linux x86_64 and arm64 ABIs that the classification
// reads.
const (
	errnoEPERM      = 1
	errnoENOENT     = 2
	errnoEACCES     = 13
	errnoENODEV     = 19
	errnoEINVAL     = 22
	errnoENOTTY     = 25
	errnoENOSYS     = 38
	errnoENODATA    = 61
	errnoEOPNOTSUPP = 95
	errnoENOKEY     = 126
)

// fscryptAdmission is what fscryptProbe's "admit" command reports about a
// client container: the kernel release and whether it has registered the
// ceph and fuse filesystem types (read from /proc/filesystems, so nothing
// is loaded), the ceph-fuse binary, the /dev/fuse device, CAP_SYS_ADMIN in
// the effective set and the security label.
type fscryptAdmission struct {
	Kernel      string `json:"kernel"`
	Machine     string `json:"machine"`
	KernelCeph  bool   `json:"kernel_ceph"`
	KernelFuse  bool   `json:"kernel_fuse"`
	CephFuse    bool   `json:"ceph_fuse"`
	DeviceErrno *int   `json:"device_errno"`
	CapSysAdmin bool   `json:"cap_sys_admin"`
	Label       string `json:"label"`
}

// refusal classifies the admission of path, without mounting: nil when a
// mount can be tried.
func (a fscryptAdmission) refusal(path string) *fscryptRefusal {
	refuse := func(requirement, detail string) *fscryptRefusal {
		return &fscryptRefusal{Path: path, Requirement: requirement, Detail: detail}
	}
	switch path {
	case kernelPath:
		if !a.KernelCeph {
			return refuse(fscryptNeedsKernelFS, "kernel "+a.Kernel+" has not registered the ceph filesystem type (/proc/filesystems); the test never loads a module")
		}
	case fusePath:
		switch {
		case !a.CephFuse:
			return refuse(fscryptNeedsBinary, "the image has no executable /usr/bin/ceph-fuse")
		case !a.KernelFuse:
			return refuse(fscryptNeedsKernelFS, "kernel "+a.Kernel+" has not registered the fuse filesystem type (/proc/filesystems)")
		case a.DeviceErrno != nil:
			return refuse(fscryptNeedsDevice, "open /dev/fuse: errno "+strconv.Itoa(*a.DeviceErrno))
		}
	default:
		return refuse("client path", "unknown client path "+strconv.Quote(path))
	}
	if !a.CapSysAdmin {
		return refuse(fscryptNeedsSysAdmin, "CAP_SYS_ADMIN is not in the effective set")
	}
	return nil
}

// mountRefusal classifies the errno of a refused mount(2) of path that the
// admission let through: a missing privilege or security allowance is a
// refusal, any other errno is a failed supported operation (nil).
func mountRefusal(path string, errno int, admission fscryptAdmission) *fscryptRefusal {
	switch errno {
	case errnoEPERM:
		return &fscryptRefusal{Path: path, Requirement: fscryptNeedsSysAdmin, Detail: fmt.Sprintf("mount(2): EPERM (CAP_SYS_ADMIN effective=%t)", admission.CapSysAdmin)}
	case errnoEACCES:
		return &fscryptRefusal{Path: path, Requirement: fscryptNeedsProfile, Detail: fmt.Sprintf("mount(2): EACCES under security label %q", admission.Label)}
	case errnoENODEV:
		return &fscryptRefusal{Path: path, Requirement: fscryptNeedsKernelFS, Detail: "mount(2): ENODEV, the kernel has no such filesystem type"}
	}
	return nil
}

// policyRefusal classifies the client's answer to FS_IOC_GET_ENCRYPTION_POLICY_EX
// on an empty, unencrypted directory of a fresh mount. ENODATA ("no
// policy") is the answer of a client that implements the fscrypt ioctls.
// ENOTTY and EOPNOTSUPP are the kernel's answers for a filesystem without
// fscrypt. Through FUSE, ENOTTY is libfuse's answer for a directory ioctl
// when ceph-fuse did not ask for directory ioctls, and EINVAL and ENOSYS are
// ceph-fuse's answers for an ioctl its handler does not know. Any other errno, and a policy on the new
// directory, is a failed supported operation (nil, ok false).
func policyRefusal(path string, errno int) (refusal *fscryptRefusal, ok bool) {
	switch errno {
	case errnoENODATA:
		return nil, true
	case errnoENOTTY, errnoEOPNOTSUPP, errnoEINVAL, errnoENOSYS:
		return &fscryptRefusal{Path: path, Requirement: fscryptNeedsClient, Detail: "FS_IOC_GET_ENCRYPTION_POLICY_EX on an empty directory: errno " + strconv.Itoa(errno)}, true
	}
	return nil, false
}

// fuseProcess is one ceph-fuse process, run in the foreground under
// RunNativeCommand by a worker goroutine. The worker assigns result and err
// and then closes done; nothing reads them before done is closed.
type fuseProcess struct {
	cancel context.CancelFunc
	done   chan struct{}
	result ceph.NativeCommandResult
	err    error
}

// errFuseOutstanding reports a ceph-fuse call that has not returned yet.
var errFuseOutstanding = errors.New("ceph-fuse call has not returned")

// stop cancels the ceph-fuse call and waits up to wait for its worker to
// return. Only then does it read the call's error, and a command that the
// call could not confirm as stopped is stopped within its own bound of
// wait. A worker that does not return within wait is reported with
// errFuseOutstanding, and its result is left alone; terminating the
// container ends the command and so the call, and stop can be called again
// after that.
func (p *fuseProcess) stop(wait time.Duration) error {
	p.cancel()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-timer.C:
		return fmt.Errorf("%w within %s of its cancellation", errFuseOutstanding, wait)
	}
	var unstopped *ceph.NativeCommandUnstoppedError
	if !errors.As(p.err, &unstopped) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := unstopped.Stop(ctx); err != nil {
		return fmt.Errorf("stop ceph-fuse: %w", err)
	}
	return nil
}

// probeExec is one fscryptProbe run through harness.SessionExec: its exit
// code, its combined output and the exec error.
type probeExec struct {
	Code   int
	Output string
	Err    error
}

// decode decodes the run's JSON result into value, or names why the run
// has none.
func (e probeExec) decode(value any) error {
	if e.Err != nil {
		return fmt.Errorf("exec: %w", e.Err)
	}
	if e.Code != 0 {
		return fmt.Errorf("exit %d: %.1024s", e.Code, e.Output)
	}
	if err := json.Unmarshal([]byte(harness.LastJSONLine(e.Output)), value); err != nil {
		return fmt.Errorf("no JSON result: %w: %.1024s", err, e.Output)
	}
	return nil
}

// emergencyUnmountError checks the lazy unmount of point that a cleanup
// after a failure runs: umount is the probe's umount2 call and listing the
// probe's mount listing below point taken after it. It returns nil once the
// listing shows no mount below point and umount2 either succeeded or
// answered EINVAL, which means that nothing was mounted there, as after a
// mount that failed before it appeared. Otherwise it names the failed step,
// including a mount that is still there.
func emergencyUnmountError(point string, umount, listing probeExec) error {
	var unmounted struct {
		Errno *int `json:"errno"`
	}
	if err := umount.decode(&unmounted); err != nil {
		return fmt.Errorf("umount2 %s: %w", point, err)
	}
	if unmounted.Errno == nil {
		return fmt.Errorf("umount2 %s: no errno in %.1024s", point, umount.Output)
	}
	var mounts struct {
		Mounts *[][]string `json:"mounts"`
	}
	if err := listing.decode(&mounts); err != nil {
		return fmt.Errorf("mount listing after umount2 %s: %w", point, err)
	}
	if mounts.Mounts == nil {
		return fmt.Errorf("mount listing after umount2 %s: no mounts in %.1024s", point, listing.Output)
	}
	if len(*mounts.Mounts) != 0 {
		return fmt.Errorf("umount2 %s: errno %d, still mounted: %v", point, *unmounted.Errno, *mounts.Mounts)
	}
	if errno := *unmounted.Errno; errno != 0 && errno != errnoEINVAL {
		return fmt.Errorf("umount2 %s: errno %d", point, errno)
	}
	return nil
}

// fscryptProbe is the native probe of the case, run with python3 in a
// client container. Each command prints one JSON object. It calls the
// fscrypt ioctls of the Linux UAPI directly, so no fscrypt tool is needed.
// The kernel copies a variable-size argument through the pointer; FUSE
// forwards only _IOC_SIZE(cmd) bytes, so for ceph-fuse the probe uses the
// fixed-size request numbers that ceph-fuse's handler accepts (a 24-byte v2
// policy, a 32-byte policy query and a key argument with a 64-byte raw
// key). A master key is read from standard input and never printed; a key
// identifier is a hash of the key and is printed.
const fscryptProbe = `
import base64, ctypes, json, os, re, stat, sys
import fcntl

def iowr(nr, size): return 0xC0000000 | size << 16 | 0x66 << 8 | nr
def ior(nr, size): return 0x80000000 | size << 16 | 0x66 << 8 | nr

REQUESTS = {
    "kernel": {"set": ior(19, 12), "get": iowr(22, 9), "add": iowr(23, 80), "remove": iowr(24, 64), "status": iowr(26, 128)},
    "fuse": {"set": iowr(19, 24), "get": iowr(22, 32), "add": iowr(23, 144), "remove": iowr(24, 64), "status": iowr(26, 128)},
}
SPEC_IDENTIFIER = 2

def emit(**result):
    print(json.dumps(result))
    sys.exit(0)

def ioctl(path, request, argument):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as error:
        return error.errno, b""
    try:
        buffer = bytearray(argument)
        fcntl.ioctl(fd, request, buffer, True)
        return 0, bytes(buffer)
    except OSError as error:
        return error.errno, b""
    finally:
        os.close(fd)

def spec(identifier):
    return (SPEC_IDENTIFIER).to_bytes(4, "little") + bytes(4) + bytes.fromhex(identifier) + bytes(16)

def entry(path):
    st = os.lstat(path)
    item = {"name": os.path.basename(path).hex(), "mode": st.st_mode, "size": st.st_size, "ino": st.st_ino, "mtime_ns": st.st_mtime_ns}
    if stat.S_ISREG(st.st_mode):
        try:
            with open(path, "rb") as f:
                item["data"] = f.read().hex()
        except OSError as error:
            item["errno"] = error.errno
    elif stat.S_ISLNK(st.st_mode):
        try:
            item["target"] = os.fsencode(os.readlink(path)).hex()
        except OSError as error:
            item["errno"] = error.errno
    elif stat.S_ISDIR(st.st_mode):
        try:
            item["children"] = len(os.listdir(path))
        except OSError as error:
            item["errno"] = error.errno
    return item

command, args = sys.argv[1], sys.argv[2:]

if command == "requests":
    emit(**REQUESTS)

if command == "admit":
    result = {"kernel": os.uname().release, "machine": os.uname().machine, "ceph_fuse": os.access("/usr/bin/ceph-fuse", os.X_OK)}
    with open("/proc/filesystems") as filesystems:
        types = [line.split()[-1] for line in filesystems if line.strip()]
    result["kernel_ceph"], result["kernel_fuse"] = "ceph" in types, "fuse" in types
    with open("/proc/self/status") as status:
        effective = [line.split()[1] for line in status if line.startswith("CapEff:")][0]
    result["cap_sys_admin"] = bool(int(effective, 16) >> 21 & 1)
    try:
        with open("/proc/self/attr/current") as label:
            result["label"] = label.read().strip("\x00\n ")
    except OSError as error:
        result["label"] = "unreadable: %s" % os.strerror(error.errno)
    try:
        os.close(os.open("/dev/fuse", os.O_RDWR))
    except OSError as error:
        result["device_errno"] = error.errno
    emit(**result)

if command == "kmount":
    # user, filesystem, root, target, extra options. The secret and the MON
    # addresses come from the identity's files and are never printed.
    user, filesystem, root, target, extra = args
    with open("/etc/ceph/ceph.conf") as conf:
        hosts = re.search(r"^\s*mon[ _]host\s*=\s*(.+)$", conf.read(), re.M).group(1)
    addresses = re.findall(r"v2:([0-9.]+:[0-9]+)", hosts)
    if not addresses:
        emit(errno=-1, error="no IPv4 Messenger v2 MON address in mon_host")
    with open("/etc/ceph/ceph.client.%s.keyring" % user) as keyring:
        secret = re.search(r"key\s*=\s*(\S+)", keyring.read()).group(1)
    # A decoded Cephx key starts with its type, little-endian: 1 is AES.
    key_type = int.from_bytes(base64.b64decode(secret)[:2], "little")
    if key_type != 1:
        emit(errno=-1, key_type=key_type, error="the identity's Cephx key is not AES")
    options = "name=%s,secret=%s,mds_namespace=%s,ms_mode=prefer-crc" % (user, secret, filesystem)
    if extra:
        options += "," + extra
    os.makedirs(target, exist_ok=True)
    source = ",".join(addresses) + ":" + root
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    if libc.mount(source.encode(), target.encode(), b"ceph", 0, options.encode()) != 0:
        emit(errno=ctypes.get_errno(), monitors=len(addresses), key_type=key_type)
    emit(errno=0, monitors=len(addresses), key_type=key_type)

if command == "umount":
    # target, flags (2 is MNT_DETACH)
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    if libc.umount2(args[0].encode(), int(args[1])) != 0:
        emit(errno=ctypes.get_errno())
    emit(errno=0)

if command == "mounts":
    with open("/proc/mounts") as mounts:
        emit(mounts=[line.split()[1:3] for line in mounts if line.split()[1].startswith(args[0])])

if command == "getpolicy":
    mode, path = args
    code, out = ioctl(path, REQUESTS[mode]["get"], (24).to_bytes(8, "little") + bytes(24))
    if code:
        emit(errno=code)
    emit(errno=0, version=out[8], contents=out[9], filenames=out[10], flags=out[11], identifier=out[16:32].hex())

if command == "setpolicy":
    mode, path, identifier = args
    policy = bytes([2, 1, 4, 3]) + bytes(4) + bytes.fromhex(identifier)
    code, _ = ioctl(path, REQUESTS[mode]["set"], policy)
    emit(errno=code)

if command == "addkey":
    mode, path = args
    raw = sys.stdin.buffer.read()
    argument = (SPEC_IDENTIFIER).to_bytes(4, "little") + bytes(36) + len(raw).to_bytes(4, "little") + bytes(4) + bytes(32) + raw
    if mode == "fuse":
        argument += bytes(64 - len(raw))
    code, out = ioctl(path, REQUESTS[mode]["add"], argument)
    if code:
        emit(errno=code)
    emit(errno=0, identifier=out[8:24].hex())

if command == "keystatus":
    mode, path, identifier = args
    code, out = ioctl(path, REQUESTS[mode]["status"], spec(identifier) + bytes(88))
    if code:
        emit(errno=code)
    emit(errno=0, status=int.from_bytes(out[64:68], "little"), flags=int.from_bytes(out[68:72], "little"), users=int.from_bytes(out[72:76], "little"))

if command == "removekey":
    mode, path, identifier = args
    code, out = ioctl(path, REQUESTS[mode]["remove"], spec(identifier) + bytes(24))
    if code:
        emit(errno=code)
    emit(errno=0, flags=int.from_bytes(out[40:44], "little"))

if command == "seed":
    # directory, file name, hex data, second file name, link name, link
    # target, subdirectory name
    directory, name, data, second, link, target, sub = args
    path = os.path.join(directory, name)
    for file, content in ((path, bytes.fromhex(data)), (os.path.join(directory, second), bytes.fromhex(data)[::-1])):
        with open(file, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
    os.symlink(target, os.path.join(directory, link))
    os.mkdir(os.path.join(directory, sub))
    fd = os.open(directory, os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    emit(ino=os.stat(path).st_ino, dir_ino=os.stat(directory).st_ino, size=os.stat(path).st_size)

if command == "inspect":
    # directory, then plaintext names to look up directly
    directory, plain = args[0], args[1:]
    try:
        names = sorted(os.listdir(os.fsencode(directory)))
    except OSError as error:
        emit(errno=error.errno)
    lookups = {}
    for name in plain:
        try:
            os.lstat(os.path.join(directory, name))
            lookups[name] = 0
        except OSError as error:
            lookups[name] = error.errno
    emit(errno=0, entries=[entry(os.path.join(os.fsencode(directory), name)) for name in names], lookups=lookups)

if command == "retain":
    # mode, mount point, identifier, retained file, other file of the same
    # directory. One fd stays open across the first removal of the key.
    mode, point, identifier, retained, other = args
    result = {}
    fd = os.open(retained, os.O_RDONLY)
    size = os.fstat(fd).st_size
    result["before"] = os.pread(fd, size, 0).hex()
    code, out = ioctl(point, REQUESTS[mode]["remove"], spec(identifier) + bytes(24))
    result["remove_errno"], result["remove_flags"] = code, int.from_bytes(out[40:44], "little") if out else None
    code, out = ioctl(point, REQUESTS[mode]["status"], spec(identifier) + bytes(88))
    result["busy_status"] = int.from_bytes(out[64:68], "little") if not code else -code
    try:
        result["retained"] = os.pread(fd, size, 0).hex()
    except OSError as error:
        result["retained_errno"] = error.errno
    try:
        os.close(os.open(other, os.O_RDONLY))
        result["other_errno"] = 0
    except OSError as error:
        result["other_errno"] = error.errno
    os.close(fd)
    code, out = ioctl(point, REQUESTS[mode]["remove"], spec(identifier) + bytes(24))
    result["final_errno"], result["final_flags"] = code, int.from_bytes(out[40:44], "little") if out else None
    code, out = ioctl(point, REQUESTS[mode]["status"], spec(identifier) + bytes(88))
    result["final_status"] = int.from_bytes(out[64:68], "little") if not code else -code
    try:
        os.close(os.open(retained, os.O_RDONLY))
        result["reopen_errno"] = 0
    except OSError as error:
        result["reopen_errno"] = error.errno
    emit(**result)

if command == "mkdir":
    try:
        os.mkdir(args[0])
        emit(errno=0)
    except OSError as error:
        emit(errno=error.errno)

if command == "rmdir":
    try:
        os.rmdir(args[0])
        emit(errno=0)
    except OSError as error:
        emit(errno=error.errno)

emit(errno=-1, error="unknown command " + command)
`

// The official FUSE build qualified with directory ioctls in the experiment.
const qualifiedFuseImage = "quay.io/ceph/ceph@sha256:c9e6ed4ac3fcf67b28f38358bb9619dee4d1dfb96ddd4b9b583eee316a15f04d"

// Key status values of FS_IOC_GET_ENCRYPTION_KEY_STATUS and the removal
// status flags of FS_IOC_REMOVE_ENCRYPTION_KEY.
const (
	keyAbsent              = 1
	keyPresent             = 2
	keyIncompletelyRemoved = 3
	removalFilesBusy       = 1
)

type fscryptRetained struct {
	Before        string `json:"before"`
	RemoveErrno   int    `json:"remove_errno"`
	RemoveFlags   *int   `json:"remove_flags"`
	BusyStatus    int    `json:"busy_status"`
	Retained      string `json:"retained"`
	RetainedErrno int    `json:"retained_errno"`
	OtherErrno    int    `json:"other_errno"`
	FinalErrno    int    `json:"final_errno"`
	FinalFlags    *int   `json:"final_flags"`
	FinalStatus   int    `json:"final_status"`
	ReopenErrno   int    `json:"reopen_errno"`
}

func (r fscryptRetained) matches(plaintext []byte) bool {
	return r.Before == hex.EncodeToString(plaintext) && r.RemoveErrno == 0 &&
		r.RemoveFlags != nil && *r.RemoveFlags&removalFilesBusy != 0 && r.BusyStatus == keyIncompletelyRemoved &&
		r.RetainedErrno == 0 && r.Retained == r.Before && r.OtherErrno == errnoENOKEY &&
		r.FinalStatus == keyAbsent && (r.ReopenErrno == errnoENOENT || r.ReopenErrno == errnoENOKEY)
}

// qualifiedView identifies the observed client cohorts for metadata and the
// privileged empty-directory policy. Other builds still run the capability probe.
func (a fscryptAdmission) qualifiedView(clientPath, version, image string) bool {
	if a.Machine != "x86_64" || !strings.HasPrefix(a.Kernel, "6.8.0-") {
		return false
	}
	switch clientPath {
	case kernelPath:
		return slices.Contains([]string{"19.2.5", "20.2.4", "21.2.0"}, version)
	case fusePath:
		return version == "21.2.0" && image == qualifiedFuseImage
	default:
		return false
	}
}

type fscryptEntry struct {
	Name     string `json:"name"`
	Mode     uint32 `json:"mode"`
	Size     int64  `json:"size"`
	Ino      uint64 `json:"ino"`
	MtimeNS  int64  `json:"mtime_ns"`
	Data     string `json:"data"`
	Target   string `json:"target"`
	Children *int   `json:"children"`
	Errno    int    `json:"errno"`
}

func (e fscryptEntry) plainName() string {
	name, _ := hex.DecodeString(e.Name)
	return string(name)
}

// The fixture uses a 39-byte symlink target. On the qualified kernel build
// its encoded target occupies 96 bytes. FUSE exposes the plaintext length.
func (keyed fscryptEntry) checkLockedMetadata(clientPath string, locked fscryptEntry) error {
	if locked.Mode != keyed.Mode || locked.MtimeNS != keyed.MtimeNS {
		return fmt.Errorf("%s: locked mode/mtime differ on inode %d", clientPath, keyed.Ino)
	}
	want := keyed.Size
	switch keyed.Mode & 0o170000 {
	case 0o100000:
	case 0o120000:
		if clientPath == kernelPath {
			want = 96
		}
	default:
		return nil
	}
	if locked.Size != want {
		return fmt.Errorf("%s: locked inode %d size %d, want %d", clientPath, keyed.Ino, locked.Size, want)
	}
	return nil
}
