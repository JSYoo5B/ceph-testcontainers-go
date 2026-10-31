//go:build all || integration

package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// The block-device rows of issue 7. A test-owned keeper holds a backing file,
// a loop device and one device-mapper node; one BlueStore OSD runs with that
// node as its main device. The judges below read what the keeper, the OSD
// container and the client report and decide what the device did. None of them
// observes anything itself.

// blockDeviceKeeperScript runs in the privileged keeper, which sees the engine
// host's /dev. It prints one JSON object per action and exits 0 unless its own
// arguments are wrong, so a refused kernel step is a receipt, not a crash.
//
//	create NAME FILE BYTES       backing file, loop, linear mapping
//	table NAME LOOP SECTORS KIND [MS]  load linear, error or delay and swap it in
//	stat NAME                    the mapping's I/O counters, open count and table
//	remove NAME LOOP FILE        remove the mapping, detach the loop, delete the file
//	targets -                    the device-mapper targets the kernel has registered
const blockDeviceKeeperScript = `import json, os, subprocess, sys

def run(*argv):
    r = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout.strip(), r.stderr.strip()[-400:]

def number(path):
    st = os.stat(path)
    return os.major(st.st_rdev), os.minor(st.st_rdev)

def table(name, loop, sectors, kind, ms):
    if kind == "linear":
        return "0 %d linear %s 0" % (sectors, loop)
    if kind == "error":
        return "0 %d error" % sectors
    if kind == "delay":
        return "0 %d delay %s 0 %d" % (sectors, loop, ms)
    raise SystemExit("unknown table kind " + kind)

action, name = sys.argv[1], sys.argv[2]
out = {"action": action, "name": name}
node = "/dev/mapper/" + name
if action == "create":
    path, size = sys.argv[3], int(sys.argv[4])
    with open(path, "xb") as f:
        f.truncate(size)
    out["file"] = path
    rc, loop, err = run("losetup", "--find", "--show", path)
    if rc != 0:
        out["error"] = "losetup: " + err
    else:
        out["loop"] = loop
        # Without udev in the keeper, --verifyudev makes dmsetup create the
        # node itself; with udev on the engine host it keeps udev's node.
        rc, _, err = run("dmsetup", "create", "--verifyudev", name, "--table", table(name, loop, size // 512, "linear", 0))
        if rc != 0:
            out["error"] = "dmsetup create: " + err
        else:
            out["mapped"] = True
            try:
                out["major"], out["minor"] = number(node)
                out["path"] = node
                rc, size_out, err = run("blockdev", "--getsize64", node)
                out["bytes"] = int(size_out) if rc == 0 else None
            except OSError as e:
                out["error"] = "node: %s" % e
elif action == "table":
    loop, sectors, kind = sys.argv[3], int(sys.argv[4]), sys.argv[5]
    ms = int(sys.argv[6]) if len(sys.argv) > 6 else 0
    out["kind"] = kind
    rc, _, err = run("dmsetup", "load", name, "--table", table(name, loop, sectors, kind, ms))
    if rc != 0:
        run("dmsetup", "clear", name)
        out["loaded"], out["error"] = False, err
    else:
        out["loaded"] = True
        for step in ("suspend", "resume"):
            rc, _, err = run("dmsetup", step, name)
            if rc != 0:
                out["error"] = step + ": " + err
                break
    out["table"] = run("dmsetup", "table", name)[1]
elif action == "stat":
    major, minor = number(node)
    with open("/sys/dev/block/%d:%d/stat" % (major, minor)) as f:
        fields = [int(x) for x in f.read().split()]
    out.update({"reads": fields[0], "read_sectors": fields[2], "writes": fields[4], "write_sectors": fields[6], "in_flight": fields[8]})
    rc, opened, err = run("dmsetup", "info", "-c", "--noheadings", "-o", "open", name)
    out["open"] = int(opened) if rc == 0 else None
    out["table"] = run("dmsetup", "table", name)[1]
elif action == "remove":
    loop, path = sys.argv[3], sys.argv[4]
    rc, _, err = run("dmsetup", "remove", "--verifyudev", name)
    out["removed"] = rc == 0
    if rc != 0:
        out["error"] = "dmsetup remove: " + err
    out["mapping_gone"] = run("dmsetup", "info", name)[0] != 0
    if out["mapping_gone"]:
        rc, _, err = run("losetup", "-d", loop)
        if rc != 0:
            out["error"] = "losetup -d: " + err
        rc, listed, _ = run("losetup", "-j", path)
        out["loop_released"] = rc == 0 and listed == ""
        if out["loop_released"]:
            os.unlink(path)
            out["file_removed"] = True
elif action == "targets":
    rc, listed, err = run("dmsetup", "targets")
    out["targets"] = [line.split()[0] for line in listed.splitlines() if line.strip()] if rc == 0 else None
    if rc != 0:
        out["error"] = err
else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`

// blockDeviceObserverScript runs in the OSD container. It reads, without
// opening anything, every descriptor of each ceph-osd process: the device
// number behind it (stat follows the descriptor link without an open) and its
// flags from fdinfo. It also reads the store's block link and fsid. Argument:
// the OSD data directory.
const blockDeviceObserverScript = `import json, os, stat, sys
osd_dir = sys.argv[1]
out = {"pids": [], "fds": [], "o_path": os.O_PATH, "o_direct": os.O_DIRECT}
for pid in sorted(p for p in os.listdir("/proc") if p.isdigit()):
    try:
        with open("/proc/%s/comm" % pid) as f:
            if f.read().strip() != "ceph-osd":
                continue
        fds = os.listdir("/proc/%s/fd" % pid)
    except OSError:
        continue
    out["pids"].append(int(pid))
    for fd in fds:
        try:
            st = os.stat("/proc/%s/fd/%s" % (pid, fd))
            flags = None
            with open("/proc/%s/fdinfo/%s" % (pid, fd)) as f:
                for line in f:
                    if line.startswith("flags:"):
                        flags = int(line.split()[1], 8)
        except OSError:
            continue
        if stat.S_ISBLK(st.st_mode):
            out["fds"].append({"pid": int(pid), "fd": int(fd), "rdev": "%d:%d" % (os.major(st.st_rdev), os.minor(st.st_rdev)), "flags": flags})
try:
    out["block_link"] = os.readlink(os.path.join(osd_dir, "block"))
except OSError as e:
    out["block_link_error"] = str(e)
with open(os.path.join(osd_dir, "fsid")) as f:
    out["fsid"] = f.read().strip()
print(json.dumps(out))`

// blockTimedWrites writes COUNT objects of SIZE bytes named PREFIX-N and
// records each write's duration, as logicalWrites does without the pause.
// Arguments: pool, prefix, count, size, the operation timeout in seconds.
const blockTimedWrites = `import errno, hashlib, json, rados, sys, time
pool, prefix, count, size, timeout = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
ledger, errnos, seconds = [], {}, []
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": timeout}) as c:
    with c.open_ioctx(pool) as io:
        for n in range(count):
            key = "%s-%d" % (prefix, n)
            data = hashlib.sha256(key.encode()).digest() * (size // 32)
            entry = {"key": key, "sha256": hashlib.sha256(data).hexdigest()}
            started = time.monotonic()
            try:
                io.write_full(key, data)
                entry["outcome"] = "acked"
            except rados.Error as e:
                code = abs(getattr(e, "errno", 0) or 0)
                entry["outcome"] = "unknown" if code in (errno.ETIMEDOUT, errno.EINTR) or isinstance(e, rados.TimedOut) else "failed"
                errnos[key] = code
            seconds.append(round(time.monotonic() - started, 4))
            ledger.append(entry)
print(json.dumps({"errnos": errnos, "seconds": seconds}))
print(json.dumps(ledger))`

// blockDeviceContainerPath is where the OSD container sees the mapped node.
const blockDeviceContainerPath = "/dev/tc-osd-block"

// blockKeeperReply is one action's receipt from blockDeviceKeeperScript.
type blockKeeperReply struct {
	Action       string   `json:"action"`
	Name         string   `json:"name"`
	File         string   `json:"file"`
	Loop         string   `json:"loop"`
	Path         string   `json:"path"`
	Mapped       bool     `json:"mapped"`
	Major        uint32   `json:"major"`
	Minor        uint32   `json:"minor"`
	Bytes        *int64   `json:"bytes"`
	Kind         string   `json:"kind"`
	Loaded       bool     `json:"loaded"`
	Table        string   `json:"table"`
	Error        string   `json:"error"`
	Reads        int64    `json:"reads"`
	ReadSectors  int64    `json:"read_sectors"`
	Writes       int64    `json:"writes"`
	WriteSectors int64    `json:"write_sectors"`
	InFlight     int64    `json:"in_flight"`
	Open         *int     `json:"open"`
	Removed      bool     `json:"removed"`
	MappingGone  bool     `json:"mapping_gone"`
	LoopReleased bool     `json:"loop_released"`
	FileRemoved  bool     `json:"file_removed"`
	Targets      []string `json:"targets"`
}

func parseBlockKeeperReply(raw []byte, action string) (blockKeeperReply, error) {
	var r blockKeeperReply
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("keeper %s reply: %w", action, err)
	}
	if r.Action != action {
		return r, fmt.Errorf("keeper reply is for %q, want %q", r.Action, action)
	}
	return r, nil
}

// blockAttachView is what blockDeviceObserverScript reports.
type blockAttachView struct {
	PIDs    []int     `json:"pids"`
	FDs     []blockFD `json:"fds"`
	OPath   int64     `json:"o_path"`
	ODirect int64     `json:"o_direct"`
	Link    string    `json:"block_link"`
	LinkErr string    `json:"block_link_error"`
	FSID    string    `json:"fsid"`
}

type blockFD struct {
	PID   int    `json:"pid"`
	FD    int    `json:"fd"`
	RDev  string `json:"rdev"`
	Flags *int64 `json:"flags"`
}

// Attachment verdicts.
const (
	blockAttached    = "attached"     // one ceph-osd holds an I/O descriptor of the device, read-write, and its store names the device
	blockNotAttached = "not-attached" // anything less; the reason says what is missing
)

// judgeBlockAttachment decides whether the OSD daemon opened the mapped device.
// A matching device number alone is not enough: an O_PATH reference has no
// I/O operations, so only a read-write descriptor without O_PATH counts, and
// flags that could not be read never count. The store must be the OSD's own
// (fsid) and link block to the mapped node.
func judgeBlockAttachment(v blockAttachView, number, uuid string) (string, string) {
	switch {
	case len(v.PIDs) != 1:
		return blockNotAttached, fmt.Sprintf("%d ceph-osd processes", len(v.PIDs))
	case v.Link != blockDeviceContainerPath:
		return blockNotAttached, fmt.Sprintf("block links to %q %s", v.Link, v.LinkErr)
	case v.FSID != uuid:
		return blockNotAttached, fmt.Sprintf("store fsid %q is not the OSD's %q", v.FSID, uuid)
	case v.OPath == 0:
		return blockNotAttached, "the observer did not report O_PATH"
	}
	io, paths, other := 0, 0, 0
	for _, fd := range v.FDs {
		switch {
		case fd.PID != v.PIDs[0]:
			continue
		case fd.RDev != number:
			other++
		case fd.Flags == nil:
			continue
		case *fd.Flags&v.OPath != 0:
			paths++
		case *fd.Flags&3 == 2: // O_RDWR
			io++
		}
	}
	if io == 0 {
		return blockNotAttached, fmt.Sprintf("no read-write descriptor of %s (%d O_PATH, %d other devices)", number, paths, other)
	}
	return blockAttached, fmt.Sprintf("%d read-write descriptors of %s", io, number)
}

// Exposure verdicts: whether the workload's I/O went through the mapping.
const (
	blockExposed    = "exposed"     // the mapping's write counters moved and the idle control's did not
	blockNotExposed = "not-exposed" // the mapping saw no writes
	blockControlRan = "control-ran" // the unused control mapping moved, so the counters prove nothing
)

func judgeBlockExposure(before, after, idleBefore, idleAfter blockKeeperReply) string {
	switch {
	case idleAfter.Writes != idleBefore.Writes || idleAfter.Reads != idleBefore.Reads:
		return blockControlRan
	case after.Writes > before.Writes && after.WriteSectors > before.WriteSectors:
		return blockExposed
	}
	return blockNotExposed
}

// blockTimedLedger is blockTimedWrites's output.
type blockTimedLedger struct {
	Ledger  []attachMutation
	Errnos  map[string]int
	Seconds []float64
}

func parseBlockTimedLedger(out string) (blockTimedLedger, error) {
	var l blockTimedLedger
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return l, errors.New("timed writes printed fewer than two lines")
	}
	var head struct {
		Errnos  map[string]int `json:"errnos"`
		Seconds []float64      `json:"seconds"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-2]), &head); err != nil {
		return l, fmt.Errorf("timed writes head: %w", err)
	}
	ledger, err := parseAttachLedger([]byte(lines[len(lines)-1]))
	if err != nil {
		return l, err
	}
	if len(head.Seconds) != len(ledger) {
		return l, fmt.Errorf("%d durations for %d writes", len(head.Seconds), len(ledger))
	}
	return blockTimedLedger{Ledger: ledger, Errnos: head.Errnos, Seconds: head.Seconds}, nil
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	s := slices.Clone(values)
	sort.Float64s(s)
	return s[len(s)/2]
}

// Delay verdicts.
const (
	blockDelayed       = "delayed"        // every write acknowledged, the median slower by most of the delay, and fast again after restore
	blockDelayLost     = "delay-lost"     // a write under the delay was not acknowledged
	blockDelayWeak     = "delay-not-seen" // the median did not slow by most of the delay
	blockDelayLingered = "delay-lingered" // the writes after restore were still slow
)

// judgeBlockDelay compares the median write time under the delay with the
// baseline's. Each write commits through at least one device write, so the
// median must grow by at least three quarters of the configured delay.
func judgeBlockDelay(base, delayed, restored blockTimedLedger, delayMS int) string {
	for _, m := range delayed.Ledger {
		if m.Outcome != mutAcked {
			return blockDelayLost
		}
	}
	d := float64(delayMS) / 1000
	switch {
	case median(delayed.Seconds) < median(base.Seconds)+0.75*d:
		return blockDelayWeak
	case median(restored.Seconds) > median(base.Seconds)+0.5*d:
		return blockDelayLingered
	}
	return blockDelayed
}

// The OSD log lines that name a failed device I/O.
var blockIOErrorMarkers = []string{"Input/output error", "Unexpected IO error", "unexpected aio error", "(5) ", "error -5"}

// Error-window verdicts.
const (
	blockOSDAborted  = "osd-aborted"      // the OSD exited and its log names a failed I/O
	blockClientEIO   = "client-eio"       // a client write failed with EIO and the OSD kept running
	blockAckedInside = "acked-in-fault"   // a write was acknowledged while every sector returned an error
	blockNoEvidence  = "fault-unobserved" // writes stopped but neither the daemon nor the client shows the error
)

// blockErrorPhase is what the error window leaves behind.
type blockErrorPhase struct {
	Writes  blockTimedLedger
	Running bool
	Log     []string
}

func judgeBlockError(p blockErrorPhase) string {
	for _, m := range p.Writes.Ledger {
		if m.Outcome == mutAcked {
			return blockAckedInside
		}
	}
	marked := slices.ContainsFunc(p.Log, func(line string) bool {
		return slices.ContainsFunc(blockIOErrorMarkers, func(w string) bool { return strings.Contains(line, w) })
	})
	if !p.Running && marked {
		return blockOSDAborted
	}
	if p.Running {
		for _, code := range p.Writes.Errnos {
			if code == 5 {
				return blockClientEIO
			}
		}
	}
	return blockNoEvidence
}

// blockChainState is what the test knows about its own chain, for cleanup.
type blockChainState struct {
	Name, File, Loop string
	Mapped           bool
}
