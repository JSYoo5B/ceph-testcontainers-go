//go:build all || integration

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// These aliases keep the existing model and native fixtures on the production
// state machine and keeper control script. Direct I/O is a test-only extension.
const blockControlScript = blockio.TestScript

type dmControlReply = blockio.View
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
	dmLoadRefused     = blockio.LoadRefused
	dmReleasable      = blockio.Releasable
	dmReleaseHeld     = blockio.ReleaseHeld
)

var (
	parseDMControlReply = blockio.Parse
	normTable           = blockio.Normalize
	classifyDM          = blockio.Classify
	dmForward           = blockio.Forward
	dmRefuses           = blockio.Refuses
	judgeDMRelease      = blockio.JudgeRelease
	judgeLoopRelease    = blockio.JudgeLoopRelease
)

const (
	blockKeeperDir   = "/tc-blk"
	dmControlBytes   = int64(64 << 20)
	dmControlSectors = dmControlBytes / 512
	// dmStepWait bounds how long the control waits for one dmsetup step
	// before it leaves the step to the kernel.
	dmStepWait = 3 * time.Second
)

// dmKeeper runs blockControlScript in the privileged keeper.
type dmKeeper struct{ ctr testcontainers.Container }

func (k dmKeeper) exec(ctx context.Context, action, name string, args ...string) (dmControlReply, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	code, reader, err := k.ctr.Exec(ctx, append([]string{"python3", "-I", "-c", blockControlScript, action, name}, args...), tcexec.Multiplexed())
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

func (c *dmControl) observe(t *testing.T, ctx context.Context) dmControlReply {
	t.Helper()
	return c.keeper.do(t, ctx, "observe", c.rec.Name)
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
	c.rec.Settle(r)
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
	c.rec.Settle(r)
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
	if verdict, why := judgeDMRelease(c.rec, v); verdict != dmReleasable {
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
	if ok, why := judgeLoopRelease(l.loopIdentity, seen, nil); !ok {
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
