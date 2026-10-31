//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// This file is the supervised table control of the block-device fault
// fixture (issue 7). A table change of a device-mapper node is three kernel
// steps: load the new table into the inactive slot, suspend the node, and
// resume it, which swaps the inactive table in. Any of them can be cut short:
// the caller's context ends, the step's process is abandoned while the kernel
// still works on it, or another actor changes the node. A timed-out step is
// not a rollback, so the control never infers the state from what it asked
// for. It observes the exact node by its name, UUID and device number before
// and after every step, classifies what it sees against its own record, and
// acts only on a state the record explains.

// blockControlScript runs in the privileged keeper. Every dmsetup step is
// started detached, with its exit status written to a file, and waited for up
// to a bound; a step still running at the bound is left to the kernel and
// reported as not done, never killed. Arguments: action, name, then per
// action:
//
//	create  uuid lower sectors   linear mapping over lower, with the UUID
//	observe                      the node's state (see dmControlView)
//	step    op wait [table]      op is load, suspend, resume, clear or remove
//	settled op                   whether an abandoned step has finished
//	write   offset blocks seed   start a direct write of a known pattern
//	written                      whether that write has finished, and its status
//	read    offset blocks        sha256 of a direct read
//	loop    path size            attach a new backing file to a free loop
//	loopid  loop                 the backing file and inode of a loop
//	unloop  loop path            detach the loop and remove the file
const blockControlScript = `import hashlib, json, os, subprocess, sys, time
action, name = sys.argv[1], sys.argv[2]
work = "/tc-blk"
out = {"action": action, "name": name}
node = "/dev/mapper/" + name
def run(*argv):
    r = subprocess.run(argv, capture_output=True, text=True, timeout=20)
    return r.returncode, r.stdout.strip(), r.stderr.strip()[-400:]
def rcfile(tag):
    return "%s/ctl-%s-%s.rc" % (work, name, tag)
def detached(tag, argv):
    for suffix in ("", ".err"):
        try:
            os.unlink(rcfile(tag) + suffix)
        except FileNotFoundError:
            pass
    script = '"$@" 2>"%s.err"; echo $? > "%s.tmp" && mv "%s.tmp" "%s"' % ((rcfile(tag),) * 4)
    subprocess.Popen(["sh", "-c", script, "sh"] + argv, start_new_session=True,
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def settled(tag, wait):
    deadline = time.monotonic() + wait
    while True:
        try:
            with open(rcfile(tag)) as f:
                rc = int(f.read().strip())
            with open(rcfile(tag) + ".err") as f:
                err = f.read().strip()[-400:]
            return {"done": True, "rc": rc, "error": err}
        except FileNotFoundError:
            pass
        if time.monotonic() >= deadline:
            return {"done": False}
        time.sleep(0.1)
def pattern(seed, blocks):
    block = hashlib.sha256(seed.encode()).digest() * (65536 // 32)
    return block * blocks
if action == "create":
    uuid, lower, sectors = sys.argv[3], sys.argv[4], int(sys.argv[5])
    rc, _, err = run("dmsetup", "create", "--verifyudev", name, "--uuid", uuid, "--table", "0 %d linear %s 0" % (sectors, lower))
    out["created"], out["error"] = rc == 0, err
elif action == "observe":
    rc, info, err = run("dmsetup", "info", name)
    out["exists"] = rc == 0
    if rc == 0:
        fields = {}
        for line in info.splitlines():
            key, _, value = line.partition(":")
            fields[key.strip()] = value.strip()
        out["state"] = fields.get("State", "")
        out["uuid"] = fields.get("UUID", "")
        out["open"] = int(fields.get("Open count", "-1"))
        out["event"] = int(fields.get("Event number", "-1"))
        major, _, minor = fields.get("Major, minor", "").partition(",")
        out["major"], out["minor"] = int(major), int(minor)
        tables = fields.get("Tables present", "")
        out["live"] = run("dmsetup", "table", name)[1] if "LIVE" in tables else None
        out["inactive"] = run("dmsetup", "table", "--inactive", name)[1] if "INACTIVE" in tables else None
        with open("/sys/dev/block/%d:%d/inflight" % (out["major"], out["minor"])) as f:
            out["in_flight"] = [int(x) for x in f.read().split()]
elif action == "step":
    op, wait = sys.argv[3], float(sys.argv[4])
    argv = ["dmsetup", op, name]
    if op == "load":
        argv += ["--table", sys.argv[5]]
    elif op == "remove":
        argv = ["dmsetup", "remove", "--verifyudev", name]
    out["op"] = op
    detached(op, argv)
    out.update(settled(op, wait))
elif action == "settled":
    out["op"] = sys.argv[3]
    out.update(settled(sys.argv[3], 0))
elif action == "write":
    offset, blocks, seed = int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    data = pattern(seed, blocks)
    src = "%s/ctl-%s-pattern" % (work, name)
    with open(src, "wb") as f:
        f.write(data)
    out["sha256"] = hashlib.sha256(data).hexdigest()
    detached("write", ["dd", "if=" + src, "of=" + node, "bs=65536", "seek=%d" % offset, "count=%d" % blocks, "oflag=direct", "status=none"])
    out["started"] = True
elif action == "written":
    out.update(settled("write", 0))
elif action == "read":
    offset, blocks = int(sys.argv[3]), int(sys.argv[4])
    r = subprocess.run(["dd", "if=" + node, "bs=65536", "skip=%d" % offset, "count=%d" % blocks, "iflag=direct", "status=none"], capture_output=True, timeout=20)
    out["rc"] = r.returncode
    out["sha256"] = hashlib.sha256(r.stdout).hexdigest() if r.returncode == 0 else ""
elif action == "loop":
    path, size = sys.argv[3], int(sys.argv[4])
    with open(path, "xb") as f:
        f.truncate(size)
    rc, loop, err = run("losetup", "--find", "--show", path)
    out["loop"], out["error"], out["file"] = loop if rc == 0 else "", err, path
    out["inode"] = os.stat(path).st_ino
    if rc == 0:
        st = os.stat(loop)
        out["major"], out["minor"] = os.major(st.st_rdev), os.minor(st.st_rdev)
elif action == "loopid":
    loop = sys.argv[3]
    try:
        with open("/sys/block/%s/loop/backing_file" % os.path.basename(loop)) as f:
            out["file"] = f.read().strip()
        out["inode"] = os.stat(out["file"]).st_ino
    except OSError as e:
        out["error"] = str(e)
elif action == "unloop":
    loop, path = sys.argv[3], sys.argv[4]
    rc, _, err = run("losetup", "-d", loop)
    out["detached"], out["error"] = rc == 0, err
    if rc == 0:
        os.unlink(path)
        out["file_removed"] = True
else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`

// dmControlReply is one reply of blockControlScript.
type dmControlReply struct {
	Action  string `json:"action"`
	Name    string `json:"name"`
	Created bool   `json:"created"`
	Error   string `json:"error"`
	// observe
	Exists   bool    `json:"exists"`
	State    string  `json:"state"`
	UUID     string  `json:"uuid"`
	Open     int     `json:"open"`
	Event    int     `json:"event"`
	Major    uint32  `json:"major"`
	Minor    uint32  `json:"minor"`
	Live     *string `json:"live"`
	Inactive *string `json:"inactive"`
	InFlight []int64 `json:"in_flight"`
	// step, settled, written
	Op   string `json:"op"`
	Done bool   `json:"done"`
	RC   int    `json:"rc"`
	// write, read
	SHA256  string `json:"sha256"`
	Started bool   `json:"started"`
	// loop, loopid, unloop
	Loop        string `json:"loop"`
	File        string `json:"file"`
	Inode       uint64 `json:"inode"`
	Detached    bool   `json:"detached"`
	FileRemoved bool   `json:"file_removed"`
}

func parseDMControlReply(raw []byte, action string) (dmControlReply, error) {
	var r dmControlReply
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("control %s reply: %w: %s", action, err, raw)
	}
	if r.Action != action {
		return r, fmt.Errorf("control reply is for %q, want %q", r.Action, action)
	}
	return r, nil
}

// dmRecord is what the control knows about the node it owns: its identity,
// the live table it last confirmed, and a step it started without seeing it
// finish.
type dmRecord struct {
	Name, UUID   string
	Major, Minor uint32
	// Live is the live table the control last observed after a step it
	// confirmed, or after creating the node.
	Live string
	// Loaded is the table the control loaded into the inactive slot and has
	// not seen swapped in, or "".
	Loaded string
	// Unsettled is a step the control started and has not seen finish, or "".
	Unsettled string
}

// The states of an owned node, as classified against its record and the table
// the control wants live.
const (
	dmGone            = "gone"             // no node of that name
	dmIdentityChanged = "identity-changed" // the name now has another UUID or device number
	dmForeign         = "foreign"          // a table neither the record nor the wanted one, live or inactive
	dmStepUnsettled   = "step-unsettled"   // a step the control started is still running in the kernel
	dmApplied         = "applied"          // the wanted table is live, active, nothing inactive
	dmUnchanged       = "unchanged"        // the recorded table is live, active, nothing inactive
	dmLoaded          = "loaded"           // recorded table live, wanted table inactive, active
	dmSuspended       = "suspended"        // recorded table live, nothing inactive, suspended
	dmSuspendedLoaded = "suspended-loaded" // recorded table live, wanted table inactive, suspended
	dmSwappedOnHold   = "swapped-on-hold"  // wanted table live, suspended
	// dmLoadRefused is what apply returns when the kernel refused the wanted
	// table; the node keeps the recorded one.
	dmLoadRefused = "load-refused"
)

// normTable makes table strings comparable: dmsetup prints a trailing space
// after a target without arguments.
func normTable(s string) string { return strings.Join(strings.Fields(s), " ") }

// classifyDM classifies an observation of the node of rec against rec and the
// wanted live table. Identity comes first: a node that is not the recorded one
// is never acted on, whatever its tables. A step the control has not seen
// finish comes next: until it ends, any other step could block behind it or
// race it.
func classifyDM(rec dmRecord, want string, v dmControlReply) string {
	if !v.Exists {
		return dmGone
	}
	if v.UUID != rec.UUID || v.Major != rec.Major || v.Minor != rec.Minor {
		return dmIdentityChanged
	}
	if rec.Unsettled != "" {
		return dmStepUnsettled
	}
	live, inactive := "", ""
	if v.Live != nil {
		live = normTable(*v.Live)
	}
	if v.Inactive != nil {
		inactive = normTable(*v.Inactive)
	}
	recorded, wanted := normTable(rec.Live), normTable(want)
	suspended := v.State == "SUSPENDED"
	switch {
	case v.State != "ACTIVE" && !suspended:
		return dmForeign
	case inactive != "" && inactive != wanted:
		return dmForeign
	case inactive != "" && rec.Loaded == "":
		// The wanted table is inactive, but the control did not load it.
		return dmForeign
	case live == wanted && inactive == "" && !suspended:
		return dmApplied
	case live == wanted && inactive == "" && suspended:
		return dmSwappedOnHold
	case live != recorded:
		return dmForeign
	case inactive == "" && !suspended:
		return dmUnchanged
	case inactive == "" && suspended:
		return dmSuspended
	case !suspended:
		return dmLoaded
	}
	return dmSuspendedLoaded
}

// dmForward is the next step that moves a node in state toward the wanted
// table, or "" when it is there or when nothing may be done.
func dmForward(state string) string {
	switch state {
	case dmUnchanged, dmSuspended:
		return "load"
	case dmLoaded:
		return "suspend"
	case dmSuspendedLoaded, dmSwappedOnHold:
		return "resume"
	}
	return ""
}

// dmRefuses reports the states in which the control does nothing at all to
// the node: it is not the owned node, or another actor or an unsettled step
// holds it.
func dmRefuses(state string) bool {
	return slices.Contains([]string{dmGone, dmIdentityChanged, dmForeign, dmStepUnsettled}, state)
}

// Release verdicts of an owned node.
const (
	dmReleasable  = "releasable"
	dmReleaseHeld = "held"
)

// judgeDMRelease decides whether the owned node may be removed: it must be the
// recorded node, with the recorded table live and active, no inactive table,
// no unsettled step, no opener and no I/O in flight. Anything else keeps the
// node, its loop and its file, with the reason.
func judgeDMRelease(rec dmRecord, v dmControlReply) (string, string) {
	state := classifyDM(rec, rec.Live, v)
	switch {
	case state != dmApplied:
		return dmReleaseHeld, "state " + state
	case v.Open != 0:
		return dmReleaseHeld, fmt.Sprintf("%d openers", v.Open)
	case len(v.InFlight) != 2:
		return dmReleaseHeld, "in-flight counters not read"
	case v.InFlight[0] != 0 || v.InFlight[1] != 0:
		return dmReleaseHeld, fmt.Sprintf("%d reads and %d writes in flight", v.InFlight[0], v.InFlight[1])
	}
	return dmReleasable, "recorded table live, idle"
}

// loopIdentity is a loop device as the control attached it: the loop node and
// the inode of its backing file.
type loopIdentity struct {
	Loop  string
	File  string
	Inode uint64
}

// judgeLoopRelease allows detaching loop only while it still backs the file
// the control attached: a loop number the kernel gave to another file after a
// release is someone else's.
func judgeLoopRelease(own loopIdentity, seen dmControlReply) (bool, string) {
	switch {
	case seen.Error != "":
		return false, "loop not readable: " + seen.Error
	case seen.File != own.File || seen.Inode != own.Inode:
		return false, fmt.Sprintf("%s now backs %s (inode %d), not %s (inode %d)", own.Loop, seen.File, seen.Inode, own.File, own.Inode)
	}
	return true, "still backs the attached file"
}
