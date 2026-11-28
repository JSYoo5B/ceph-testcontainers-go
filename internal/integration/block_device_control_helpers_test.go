//go:build all || integration

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// These aliases keep the model and native fixtures on the production state
// machine, and the actions that fault/blockio dispatches (observe, step,
// settled, loopid) on its keeper control script.
type dmView = blockio.View
type dmRecord = blockio.Record
type loopIdentity = blockio.LoopIdentity

const (
	dmGone            = blockio.Gone
	dmIdentityChanged = blockio.IdentityChanged
	dmForeign         = blockio.Foreign
	dmStepUnsettled   = blockio.StepUnsettled
	dmApplied         = blockio.Applied
	dmUnchanged       = blockio.Unchanged
	dmLoaded          = blockio.Loaded
	dmSuspended       = blockio.Suspended
	dmSuspendedLoaded = blockio.SuspendedLoaded
	dmSwappedOnHold   = blockio.SwappedOnHold
	dmReleasable      = blockio.Releasable
	dmReleaseHeld     = blockio.ReleaseHeld
)

var (
	normTable           = blockio.Normalize
	classifyDM          = blockio.Classify
	dmForward           = blockio.Forward
	judgeDMRelease      = blockio.JudgeRelease
	judgeLoopRelease    = blockio.JudgeLoopRelease
)

// blockTestScript runs the fixture's own keeper actions next to Script: it
// builds and releases the scratch loops and mappings that the control acts
// on (create, creation, loop, rmfile, unloop) and runs direct I/O and a loop
// opener (write, written, read, hold). fault/blockio dispatches none of them:
// its keeper creates, detaches and unlinks through its owner script.
const blockTestScript = `import hashlib, json, os, subprocess, sys, time
action, name = sys.argv[1], sys.argv[2]
work = "/tc-blk"
out = {"action": action, "name": name}
node = "/dev/mapper/" + name
def run(*argv):
    r = subprocess.run(argv, capture_output=True, text=True, timeout=20)
    return r.returncode, r.stdout.strip(), r.stderr.strip()[-400:]
def creation_path(token):
    return "%s/ctl-create-%s-%s.json" % (work, name, token)
def save_creation(token):
    if token:
        path = creation_path(token)
        with open(path + ".tmp", "w") as f:
            json.dump(dict(out, done=True), f)
        os.replace(path + ".tmp", path)
def remove_backing(path, inode):
    rc, links, err = run("losetup", "--list", "--associated", path, "--noheadings", "--output", "NAME")
    if rc != 0 or links:
        raise RuntimeError("backing associations not confirmed absent: " + (err or links))
    if os.stat(path, follow_symlinks=False).st_ino != inode:
        raise RuntimeError("backing file identity changed")
    os.unlink(path)
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
    save_creation(sys.argv[6] if len(sys.argv) > 6 else "")
elif action == "creation":
    try:
        with open(creation_path(sys.argv[3])) as f:
            out.update(json.load(f))
        out["action"] = action
    except FileNotFoundError:
        out["done"] = False
elif action == "loop":
    path, size = sys.argv[3], int(sys.argv[4])
    out.update(loop="", file="", inode=0, error="")
    try:
        with open(path, "xb") as f:
            out["file"], out["inode"] = path, os.fstat(f.fileno()).st_ino
            f.truncate(size)
        rc, loop, err = run("losetup", "--find", "--show", path)
        out["loop"], out["error"] = loop if rc == 0 else "", err
        if rc == 0:
            st = os.stat(loop)
            out["major"], out["minor"] = os.major(st.st_rdev), os.minor(st.st_rdev)
    except OSError as e:
        out["major"], out["minor"], out["error"] = 0, 0, str(e)
    save_creation(sys.argv[5] if len(sys.argv) > 5 else "")
elif action == "rmfile":
    path, inode = sys.argv[3], int(sys.argv[4])
    remove_backing(path, inode)
    out["file_removed"] = True
elif action == "unloop":
    loop, path, inode = sys.argv[3], sys.argv[4], int(sys.argv[5])
    base = os.path.basename(loop)
    out.update(holders=[], detached=False, error="")
    try:
        with open("/sys/block/%s/loop/backing_file" % base) as f:
            backing = f.read().strip()
        if backing != path or os.stat(backing, follow_symlinks=False).st_ino != inode:
            raise RuntimeError("loop backing identity changed")
        holders = sorted(os.listdir("/sys/block/%s/holders" % base))
        out["holders"] = holders
        if holders:
            raise RuntimeError("held by " + " ".join(holders))
        rc, _, err = run("losetup", "-d", loop)
        if rc != 0:
            raise RuntimeError("loop detach: " + err)
        # A busy loop uses lazy detach. Only an empty successful kernel
        # association query permits removal of the backing file.
        rc, links, err = run("losetup", "--list", "--associated", path, "--noheadings", "--output", "NAME")
        if rc != 0:
            raise RuntimeError("loop association: " + err)
        out["detached"], out["lazy"] = not links, bool(links)
        if not links:
            remove_backing(path, inode)
            out["file_removed"] = True
    except (OSError, RuntimeError) as e:
        out["error"] = str(e)
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
else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`

// dmLoadRefused is what apply returns when the kernel refused the wanted
// table; the node keeps the recorded one.
const dmLoadRefused = "load-refused"

// dmRefuses reports the states in which the control does nothing at all to
// the node: it is not the owned node, or another actor or an unsettled step
// holds it.
func dmRefuses(state string) bool {
	return slices.Contains([]string{dmGone, dmIdentityChanged, dmForeign, dmStepUnsettled}, state)
}

// dmControlReply is a reply of either script: Script's fields and those of
// the fixture's own actions.
type dmControlReply struct {
	dmView
	Created     bool   `json:"created"`
	SHA256      string `json:"sha256"`
	Loop        string `json:"loop"`
	Detached    bool   `json:"detached"`
	FileRemoved bool   `json:"file_removed"`
	// Lazy is a detach the kernel accepted for a busy loop and has not done.
	Lazy bool `json:"lazy"`
}

// testActionFields are the fields each of blockTestScript's actions always
// reports.
var testActionFields = map[string][]string{
	"create":   {"created", "error"},
	"creation": {"done"},
	"loop":     {"loop", "file", "inode", "error"},
	"rmfile":   {"file_removed"},
	"unloop":   {"holders", "detached", "error"},
	"write":    {"sha256", "started"},
	"written":  {"done"},
	"read":     {"rc", "sha256"},
	"hold":     {"started"},
}

// controlScript is the script that runs action.
func controlScript(action string) string {
	if _, test := testActionFields[action]; test {
		return blockTestScript
	}
	return blockio.Script
}

// parseDMControlReply parses a reply of Script with blockio.Parse and one of
// blockTestScript with the fields its action always reports.
func parseDMControlReply(raw []byte, action string) (dmControlReply, error) {
	var r dmControlReply
	required, test := testActionFields[action]
	if !test {
		view, err := blockio.Parse(raw, action)
		r.dmView = view
		return r, err
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("control %s reply: %w: %s", action, err, raw)
	}
	if r.Action != action {
		return r, fmt.Errorf("control reply is for %q, want %q", r.Action, action)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return r, err
	}
	required = append([]string{"action", "name"}, required...)
	switch {
	case action == "loop" && r.Loop != "":
		required = append(required, "major", "minor")
	case action == "written" && r.Done:
		required = append(required, "rc", "error")
	}
	for _, name := range required {
		value, present := fields[name]
		if !present || string(value) == "null" {
			return r, fmt.Errorf("control %s reply lacks non-null %s", action, name)
		}
	}
	return r, nil
}

const (
	blockKeeperDir   = "/tc-blk"
	dmControlBytes   = int64(64 << 20)
	dmControlSectors = dmControlBytes / 512
	// dmStepWait bounds how long the control waits for one dmsetup step
	// before it leaves the step to the kernel.
	dmStepWait = 3 * time.Second
)

// dmKeeper runs Script and blockTestScript in the privileged keeper.
type dmKeeper struct{ ctr testcontainers.Container }

func (k dmKeeper) exec(ctx context.Context, action, name string, args ...string) (dmControlReply, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	code, reader, err := k.ctr.Exec(ctx, append([]string{"python3", "-I", "-c", controlScript(action), action, name}, args...), tcexec.Multiplexed())
	if err != nil {
		return dmControlReply{}, err
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return dmControlReply{}, err
	}
	if code != 0 {
		return dmControlReply{}, fmt.Errorf("control %s %s exited %d: %s", action, name, code, raw)
	}
	reply, err := parseDMControlReply(raw, action)
	if err == nil && reply.Name != name {
		err = fmt.Errorf("control %s reply is for %q, want %q", action, reply.Name, name)
	}
	return reply, err
}

func (k dmKeeper) do(t *testing.T, ctx context.Context, action, name string, args ...string) dmControlReply {
	t.Helper()
	r, err := k.exec(ctx, action, name, args...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// dmControl is the supervised control of one owned node, with the receipts of
// what it saw and did.
type dmControl struct {
	keeper dmKeeper
	rec    dmRecord
	// creation is a dispatched create whose terminal receipt was lost.
	creation string
	// lower is the major:minor of the device the node maps onto.
	lower string
	log   []map[string]any
	// refused is the kernel's reason for the last load it refused.
	refused string
}

func (c *dmControl) observe(t *testing.T, ctx context.Context) dmView {
	t.Helper()
	return c.keeper.do(t, ctx, "observe", c.rec.Name).dmView
}

func (c *dmControl) note(entry map[string]any) { c.log = append(c.log, entry) }

func (c *dmControl) dev() string { return fmt.Sprintf("%d:%d", c.rec.Major, c.rec.Minor) }

// dispatch records the step with its table and the id of its status file,
// then starts it and waits up to wait. The step may be running in the kernel
// once the command is sent, so an error keeps it recorded as unsettled: its
// outcome is unknown until its status file says otherwise.
func (c *dmControl) dispatch(ctx context.Context, op, table string, wait time.Duration) (dmControlReply, error) {
	id := randomName(op + "-")
	c.rec.Begin(op, table, id)
	args := []string{op, strconv.FormatFloat(wait.Seconds(), 'f', 1, 64), id}
	if op == "load" {
		args = append(args, table)
	}
	if op == "remove" && c.rec.UUID != "" {
		args = append(args, c.rec.UUID)
	}
	r, err := c.keeper.exec(ctx, "step", c.rec.Name, args...)
	if err == nil && r.ID != id {
		err = fmt.Errorf("step reply is for %q, want %q", r.ID, id)
	}
	if err != nil {
		return r, err
	}
	c.rec.Settle(r.dmView)
	return r, nil
}

// poll reads the status of the recorded unsettled step once and records it.
func (c *dmControl) poll(ctx context.Context) (dmControlReply, error) {
	r, err := c.keeper.exec(ctx, "settled", c.rec.Name, c.rec.UnsettledID)
	if err == nil && r.ID != c.rec.UnsettledID {
		err = fmt.Errorf("settled reply is for %q, want %q", r.ID, c.rec.UnsettledID)
	}
	if err != nil {
		return r, err
	}
	c.rec.Settle(r.dmView)
	return r, nil
}

func (c *dmControl) step(t *testing.T, ctx context.Context, op, table string) dmControlReply {
	t.Helper()
	r, err := c.dispatch(ctx, op, table, dmStepWait)
	if err != nil {
		c.note(map[string]any{"step": op, "unknown": err.Error()})
		t.Fatalf("%s %s: outcome unknown, kept as unsettled: %v", op, c.rec.Name, err)
	}
	c.note(map[string]any{"step": op, "done": r.Done, "rc": r.RC, "error": r.Error})
	if r.Done && r.RC != 0 && op != "load" {
		t.Fatalf("%s %s failed: %s", op, c.rec.Name, r.Error)
	}
	return r
}

// apply moves the node toward want, one observed step at a time, and returns
// the state it leaves the node in. It stops after the step stopAfter, as a
// caller whose context ends there would, and whenever the state refuses
// action. It never starts a step while one is unsettled. A table the kernel
// refuses to load (a target it does not have) returns dmLoadRefused with the
// node unchanged.
func (c *dmControl) apply(t *testing.T, ctx context.Context, want, stopAfter string) string {
	t.Helper()
	for range 4 {
		v := c.observe(t, ctx)
		state := classifyDM(c.rec, want, v)
		c.note(map[string]any{"want": want, "state": state, "live": v.Live, "inactive": v.Inactive, "dm_state": v.State, "open": v.Open, "in_flight": v.InFlight})
		if state == dmApplied {
			c.rec.Live = want
		}
		op := dmForward(state)
		if op == "" {
			return state
		}
		r := c.step(t, ctx, op, want)
		if !r.Done {
			return dmStepUnsettled
		}
		if r.RC != 0 {
			c.refused = r.Error
			if state := classifyDM(c.rec, c.rec.Live, c.observe(t, ctx)); state != dmApplied {
				t.Fatalf("a refused load left %s %s", c.rec.Name, state)
			}
			return dmLoadRefused
		}
		if op == stopAfter {
			return classifyDM(c.rec, want, c.observe(t, ctx))
		}
	}
	t.Fatalf("%s did not reach %q in four steps", c.rec.Name, want)
	return ""
}

// revert drops a table the control loaded and has not swapped in, resuming a
// node it suspended, so the recorded table stays live.
func (c *dmControl) revert(t *testing.T, ctx context.Context, want string) string {
	t.Helper()
	switch state := classifyDM(c.rec, want, c.observe(t, ctx)); state {
	case dmLoaded:
		c.step(t, ctx, "clear", "")
	case dmSuspendedLoaded:
		c.step(t, ctx, "clear", "")
		c.step(t, ctx, "resume", "")
	default:
		return state
	}
	return classifyDM(c.rec, c.rec.Live, c.observe(t, ctx))
}

// settle waits for an unsettled step to finish, up to bound.
func (c *dmControl) settle(t *testing.T, ctx context.Context, bound time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(bound); c.rec.Unsettled != ""; time.Sleep(200 * time.Millisecond) {
		op := c.rec.Unsettled
		r, err := c.poll(ctx)
		if err != nil {
			t.Fatalf("%s %s: %v", c.rec.Name, op, err)
		}
		if r.Done {
			c.note(map[string]any{"settled": op, "rc": r.RC, "error": r.Error})
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s did not finish within %s", c.rec.Name, op, bound)
		}
	}
}

// releaseOwned never acts on a node while creation or a table operation is
// unresolved. It verifies absence before the lower device can be released.
func (c *dmControl) releaseOwned(ctx context.Context) (string, string) {
	keep := func(why string) (string, string) {
		c.note(map[string]any{"release": dmReleaseHeld, "why": why})
		return dmReleaseHeld, why
	}
	if c.creation != "" {
		r, err := c.keeper.exec(ctx, "creation", c.rec.Name, c.creation)
		if err != nil || !r.Done {
			return keep(fmt.Sprintf("creation is unsettled: %v", err))
		}
		c.creation = ""
	}
	if c.rec.Unsettled != "" {
		if r, err := c.poll(ctx); err != nil || !r.Done {
			return keep(fmt.Sprintf("unsettled %s: %v", c.rec.Unsettled, err))
		}
	}
	v, err := c.keeper.exec(ctx, "observe", c.rec.Name)
	if err != nil {
		return keep("observe: " + err.Error())
	}
	if !v.Exists {
		c.note(map[string]any{"release": dmReleasable, "why": "confirmed absent"})
		return dmReleasable, "confirmed absent"
	}
	if c.rec.Major == 0 {
		if err := c.bind(v); err != nil {
			return keep(err.Error())
		}
	}
	if verdict, why := judgeDMRelease(c.rec, v.dmView); verdict != dmReleasable {
		return keep(why)
	}
	if r, err := c.dispatch(ctx, "remove", "", 10*time.Second); err != nil || !r.Done || r.RC != 0 {
		return keep(fmt.Sprintf("remove %v %+v", err, r))
	}
	v, err = c.keeper.exec(ctx, "observe", c.rec.Name)
	if err != nil || v.Exists {
		return keep(fmt.Sprintf("mapping absence not confirmed after removal: %v", err))
	}
	c.note(map[string]any{"release": dmReleasable, "why": "confirmed removed"})
	return dmReleasable, "confirmed removed"
}

func (c *dmControl) release(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()
	return c.releaseOwned(ctx)
}

// bind records the number only after observing the UUID and the exact
// originally requested table. It can run in cleanup after a lost readback.
func (c *dmControl) bind(v dmControlReply) error {
	if !v.Exists || v.UUID != c.rec.UUID || v.Major == 0 || v.Live == nil ||
		c.rec.Live == "" || normTable(*v.Live) != normTable(c.rec.Live) || v.Inactive != nil || v.State != "ACTIVE" {
		return fmt.Errorf("cannot bind %s to its planned UUID and linear table", c.rec.Name)
	}
	c.rec.Major, c.rec.Minor = v.Major, v.Minor
	return nil
}

// dmBacking records a loop dispatch before invoking the keeper. If its
// reply is lost, cleanup reads the keeper's terminal creation receipt.
type dmBacking struct {
	name, path, creation string
	loopIdentity
}

func (l *dmBacking) accept(r dmControlReply) {
	if r.File != l.path || r.Inode == 0 {
		return
	}
	l.File, l.Inode, l.Loop = r.File, r.Inode, r.Loop
	if r.Major != 0 {
		l.Dev = fmt.Sprintf("%d:%d", r.Major, r.Minor)
	}
}

func (l *dmBacking) release(ctx context.Context, keeper dmKeeper, kept []string) (dmControlReply, error) {
	if len(kept) != 0 {
		return dmControlReply{}, fmt.Errorf("backing %s under kept mappings %v", l.name, kept)
	}
	if l.creation != "" {
		r, err := keeper.exec(ctx, "creation", l.name, l.creation)
		if err != nil || !r.Done {
			return r, fmt.Errorf("backing %s creation is unsettled: %v", l.name, err)
		}
		l.creation = ""
		l.accept(r)
	}
	if l.File == "" || l.Inode == 0 {
		return dmControlReply{}, fmt.Errorf("backing %s identity unavailable", l.name)
	}
	inode := strconv.FormatUint(l.Inode, 10)
	if l.Loop == "" {
		r, err := keeper.exec(ctx, "rmfile", "-", l.File, inode)
		if err == nil && !r.FileRemoved {
			err = fmt.Errorf("backing %s was not removed: %s", l.name, r.Error)
		}
		return r, err
	}
	seen, err := keeper.exec(ctx, "loopid", "-", l.Loop)
	if err != nil {
		return seen, err
	}
	if ok, why := judgeLoopRelease(l.loopIdentity, seen.dmView, nil); !ok {
		return seen, fmt.Errorf("%s", why)
	}
	r, err := keeper.exec(ctx, "unloop", "-", l.Loop, l.File, inode)
	if err == nil && (!r.Detached || !r.FileRemoved) {
		err = fmt.Errorf("loop %s or its backing was not released: %s", l.Loop, r.Error)
	}
	return r, err
}

// dmFixture is the keeper and what the test dispatched in it.
type dmFixture struct {
	keeper   dmKeeper
	loops    []*dmBacking
	controls []*dmControl
}

func (f *dmFixture) createLoop(ctx context.Context, name string, size int64) (*dmBacking, error) {
	l := &dmBacking{name: name, path: blockKeeperDir + "/" + name, creation: randomName("loop-")}
	f.loops = append(f.loops, l)
	r, err := f.keeper.exec(ctx, "loop", name, l.path, strconv.FormatInt(size, 10), l.creation)
	if err != nil {
		return l, err
	}
	l.creation = ""
	l.accept(r)
	if r.Error != "" || l.Loop == "" || l.Dev == "" {
		return l, fmt.Errorf("loop for %s: %s", name, r.Error)
	}
	return l, nil
}

func (f *dmFixture) loop(t *testing.T, ctx context.Context, name string) (loopIdentity, string) {
	t.Helper()
	l, err := f.createLoop(ctx, name, dmControlBytes)
	if err != nil {
		t.Fatal(err)
	}
	return l.loopIdentity, l.Dev
}

func (f *dmFixture) createNode(ctx context.Context, name, uuid, lower string, sectors int64) (*dmControl, error) {
	c := &dmControl{keeper: f.keeper, rec: dmRecord{Name: name, UUID: uuid, Live: fmt.Sprintf("0 %d linear %s 0", sectors, lower)},
		lower: lower, creation: randomName("node-")}
	f.controls = append(f.controls, c)
	r, err := f.keeper.exec(ctx, "create", name, uuid, lower, strconv.FormatInt(sectors, 10), c.creation)
	if err != nil {
		return c, err
	}
	c.creation = ""
	if !r.Created {
		return c, fmt.Errorf("create %s: %s", name, r.Error)
	}
	v, err := f.keeper.exec(ctx, "observe", name)
	if err != nil {
		return c, err
	}
	return c, c.bind(v)
}

func (f *dmFixture) node(t *testing.T, ctx context.Context, name, lower string) *dmControl {
	t.Helper()
	c, err := f.createNode(ctx, name, "TC-CTL-"+name, lower, dmControlSectors)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func errorTable() string { return fmt.Sprintf("0 %d error", dmControlSectors) }

func randomName(prefix string) string {
	raw := make([]byte, 4)
	_, _ = rand.Read(raw)
	return prefix + hex.EncodeToString(raw)
}
