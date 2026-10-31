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
//	step    op wait id [table]   op is load, suspend, resume, clear or remove;
//	                             id names this one step's status file
//	settled id                   whether the step id has finished
//	write   offset blocks seed   start a direct write of a known pattern
//	written                      whether that write has finished, and its status
//	read    offset blocks        sha256 of a direct read
//	loop    path size            attach a new backing file to a free loop
//	loopid  loop                 the backing file, inode and holders of a loop
//	hold    loop seconds         keep the loop open, without a holder, a while
//	rmfile  path inode           remove a released loop's file of that inode
//	unloop  loop path            detach the loop and, once the kernel has
//	                             released it, remove the file
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
    op, wait, sid = sys.argv[3], float(sys.argv[4]), sys.argv[5]
    argv = ["dmsetup", op, name]
    if op == "load":
        argv += ["--table", sys.argv[6]]
    elif op == "remove":
        argv = ["dmsetup", "remove", "--verifyudev", name]
    out["op"], out["id"] = op, sid
    detached("step-" + sid, argv)
    out.update(settled("step-" + sid, wait))
elif action == "settled":
    out["id"] = sys.argv[3]
    out.update(settled("step-" + sys.argv[3], 0))
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
        out["holders"] = sorted(os.listdir("/sys/block/%s/holders" % os.path.basename(loop)))
    except OSError as e:
        out["error"] = str(e)
elif action == "hold":
    mark = "%s/ctl-hold-%s" % (work, os.path.basename(sys.argv[3]))
    try:
        os.unlink(mark)
    except FileNotFoundError:
        pass
    detached("hold", ["sh", "-c", 'exec 3<"$1"; : >"$3"; sleep "$2"', "sh", sys.argv[3], sys.argv[4], mark])
    deadline = time.monotonic() + 10
    while not os.path.exists(mark) and time.monotonic() < deadline:
        time.sleep(0.05)
    out["started"] = os.path.exists(mark)
elif action == "rmfile":
    path, inode = sys.argv[3], int(sys.argv[4])
    if os.stat(path).st_ino != inode:
        raise SystemExit("%s is not inode %d" % (path, inode))
    os.unlink(path)
    out["file_removed"] = True
elif action == "unloop":
    loop, path = sys.argv[3], sys.argv[4]
    base = os.path.basename(loop)
    holders = sorted(os.listdir("/sys/block/%s/holders" % base))
    out["holders"] = holders
    if holders:
        out["detached"], out["error"] = False, "held by " + " ".join(holders)
    else:
        rc, _, err = run("losetup", "-d", loop)
        out["error"] = err
        # losetup -d on a busy loop only sets autoclear and succeeds; the
        # loop is released once its backing file is gone from sysfs.
        released = rc == 0 and not os.path.exists("/sys/block/%s/loop/backing_file" % base)
        out["detached"], out["lazy"] = released, rc == 0 and not released
        if released:
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
	ID   string `json:"id"`
	Done bool   `json:"done"`
	RC   int    `json:"rc"`
	// write, read
	SHA256  string `json:"sha256"`
	Started bool   `json:"started"`
	// loop, loopid, unloop
	Loop        string   `json:"loop"`
	File        string   `json:"file"`
	Inode       uint64   `json:"inode"`
	Detached    bool     `json:"detached"`
	FileRemoved bool     `json:"file_removed"`
	Holders     []string `json:"holders"`
	// Lazy is a detach the kernel accepted for a busy loop and has not done.
	Lazy bool `json:"lazy"`
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
	// Unsettled is a step the control may have started and has not seen
	// finish, or "". It is recorded before the step is dispatched, so a lost
	// reply or an ended context leaves it here, with the table of a load
	// (UnsettledTable) and the id of its status file (UnsettledID).
	Unsettled, UnsettledTable, UnsettledID string
}

// begin records a step before it is dispatched.
func (rec *dmRecord) begin(op, table, id string) {
	rec.Unsettled, rec.UnsettledTable, rec.UnsettledID = op, table, id
}

// settle records the reply of the recorded step: nothing while it is not
// done, and once it is, what a successful step changed.
func (rec *dmRecord) settle(r dmControlReply) {
	if !r.Done {
		return
	}
	op, table := rec.Unsettled, rec.UnsettledTable
	rec.begin("", "", "")
	if r.RC != 0 {
		return
	}
	switch op {
	case "load":
		rec.Loaded = table
	case "clear":
		rec.Loaded = ""
	case "resume":
		if rec.Loaded != "" {
			rec.Live, rec.Loaded = rec.Loaded, ""
		}
	}
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
	case inactive != "" && inactive != normTable(rec.Loaded):
		// The wanted table is inactive, but it is not the table the control
		// loaded: the control loaded none, or another actor replaced it.
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

// loopIdentity is a loop device as the control attached it: the loop node,
// its device number and the inode of its backing file.
type loopIdentity struct {
	Loop  string
	File  string
	Inode uint64
	// Dev is the loop's major:minor, the lower device of mappings over it.
	Dev string
}

// judgeLoopRelease allows detaching loop only while it still backs the file
// the control attached, nothing holds it, and no mapping the fixture built on
// it was kept or is in an unknown state (kept): a loop number the kernel gave
// to another file after a release is someone else's, and a loop under a
// remaining mapping is still in use.
func judgeLoopRelease(own loopIdentity, seen dmControlReply, kept []string) (bool, string) {
	switch {
	case seen.Error != "":
		return false, "loop not readable: " + seen.Error
	case seen.File != own.File || seen.Inode != own.Inode:
		return false, fmt.Sprintf("%s now backs %s (inode %d), not %s (inode %d)", own.Loop, seen.File, seen.Inode, own.File, own.Inode)
	case len(kept) != 0:
		return false, fmt.Sprintf("%s is under kept mappings %v", own.Loop, kept)
	case len(seen.Holders) != 0:
		return false, fmt.Sprintf("%s is held by %v", own.Loop, seen.Holders)
	}
	return true, "still backs the attached file, unheld"
}
