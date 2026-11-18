//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scratch_tmpfs))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// TestBlockDeviceControlSupervision drives the supervised table control of
// the block-device fault fixture through every way a table change can be left
// half-done, on scratch device-mapper nodes that a privileged keeper builds
// from loop devices over its own tmpfs. No node of the engine host is touched.
//
//   - Partial apply and restore: a load outlives its wait, and the control
//     stops after the load and after the suspend. Each state is observed as such, the release gate keeps the
//     node, and the change is then reverted or completed from what was
//     observed.
//   - Foreign table changes: another actor swaps a live table, loads an
//     inactive one, or replaces the control's own load. The control refuses to act or release until the node is
//     back to the state its record explains.
//   - Identity reuse: a node of the same name created again, and a loop number
//     given to another file, are not the owned resources and are never acted on
//     or released as such.
//   - Lazy loop detach: a loop kept open by a process that is no holder is
//     detached only lazily; the file stays until the kernel releases it.
//   - Pending I/O and a cancelled step: a write is in flight on a node whose
//     lower node is suspended, so the control's suspend blocks past its bound.
//     The step is left to the kernel, nothing else is started or released,
//     and once the lower node resumes the suspend finishes, the in-flight write
//     completes on the old table, and a write queued behind the suspension
//     meets the table the resume installs.
func TestBlockDeviceControlSupervision(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image := harness.EssentialImage(t)
	keeper, err := testcontainers.Run(ctx, image, testcontainers.WithEntrypoint("sleep", "infinity"),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
			h.Privileged = true
			h.Binds = append(h.Binds, "/dev:/dev")
			h.Tmpfs = map[string]string{blockKeeperDir: "size=" + strconv.FormatInt(5*dmControlBytes, 10)}
		}))
	if keeper != nil {
		t.Cleanup(func() {
			if err := keeper.Terminate(context.Background()); err != nil {
				t.Error("block control keeper cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	f := &dmFixture{keeper: dmKeeper{ctr: keeper}}
	prefix := randomName("tcctl-")
	receipt := map[string]any{}
	// Registered before the keeper's Terminate, so it runs first: every node
	// the gate allows is removed, in reverse order of creation, and then every
	// loop that still backs its own file. A node that is kept, or whose state
	// or removal is unknown, keeps every device below it: the node it maps
	// onto, and that node's loop and file.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		var held []string
		kept := map[string][]string{}
		keep := func(c *dmControl, why string) {
			held = append(held, c.rec.Name+": "+why)
			kept[c.lower] = append(kept[c.lower], c.rec.Name)
		}
		for i := len(f.controls) - 1; i >= 0; i-- {
			c := f.controls[i]
			if over := kept[c.dev()]; len(over) != 0 {
				keep(c, fmt.Sprintf("under kept mappings %v", over))
				continue
			}
			if verdict, why := c.releaseOwned(cleanup); verdict != dmReleasable {
				keep(c, why)
			}
		}
		for _, l := range f.loops {
			if _, err := l.release(cleanup, f.keeper, kept[l.Dev]); err != nil {
				held = append(held, l.name+": "+err.Error())
			}
		}
		logs := map[string]any{}
		for _, c := range f.controls {
			logs[c.rec.Name] = c.log
		}
		receipt["controls"], receipt["held_at_cleanup"] = logs, held
		t.Logf("BLOCK_CONTROL_RECEIPT %s", mustMarshal(receipt))
		if len(held) != 0 {
			t.Errorf("owned block resources kept: %v", held)
		}
	})

	t.Run("partial apply and restore", func(t *testing.T) {
		_, lower := f.loop(t, ctx, prefix+"-a")
		c := f.node(t, ctx, prefix+"-a", lower)
		linear := c.rec.Live
		// A load still running when its wait ends: the control keeps it as
		// unsettled with its table, and once it finishes records the
		// inactive table as its own load.
		r, err := c.dispatch(ctx, "load", errorTable(), 0)
		if err != nil {
			t.Fatal(err)
		}
		receipt["late_load"] = map[string]any{"done_in_wait": r.Done, "unsettled": c.rec.Unsettled}
		c.settle(t, ctx, 30*time.Second)
		if state := classifyDM(c.rec, errorTable(), c.observe(t, ctx)); state != dmLoaded || normTable(c.rec.Loaded) != normTable(errorTable()) {
			t.Fatalf("after a load that finished past its wait: %s, loaded %q", state, c.rec.Loaded)
		}
		if state := c.revert(t, ctx, errorTable()); state != dmApplied {
			t.Fatalf("revert of the late load: %s", state)
		}
		if state := c.apply(t, ctx, errorTable(), "load"); state != dmLoaded {
			t.Fatalf("after a stop at the load: %s", state)
		}
		if verdict, why := judgeDMRelease(c.rec, c.observe(t, ctx)); verdict != dmReleaseHeld {
			t.Fatalf("a node with a loaded table was releasable: %s", why)
		}
		if state := c.revert(t, ctx, errorTable()); state != dmApplied || normTable(c.rec.Live) != normTable(linear) {
			t.Fatalf("revert of the load: %s, live %q", state, c.rec.Live)
		}
		if state := c.apply(t, ctx, errorTable(), "suspend"); state != dmSuspendedLoaded {
			t.Fatalf("after a stop at the suspend: %s", state)
		}
		if verdict, _ := judgeDMRelease(c.rec, c.observe(t, ctx)); verdict != dmReleaseHeld {
			t.Fatal("a suspended node was releasable")
		}
		if state := c.apply(t, ctx, errorTable(), ""); state != dmApplied {
			t.Fatalf("completing the fault from the suspended state: %s", state)
		}
		if r := c.keeper.do(t, ctx, "read", c.rec.Name, "0", "1"); r.RC == 0 {
			t.Fatal("a read through the applied error table succeeded")
		}
		// Partial restore: stop after the suspend of the linear table, then
		// complete it.
		if state := c.apply(t, ctx, linear, "suspend"); state != dmSuspendedLoaded {
			t.Fatalf("after a stop at the restore's suspend: %s", state)
		}
		if state := c.apply(t, ctx, linear, ""); state != dmApplied {
			t.Fatalf("completing the restore: %s", state)
		}
		w := c.keeper.do(t, ctx, "write", c.rec.Name, "0", "4", "restored")
		waitWritten(t, ctx, c, 30*time.Second, 0)
		if r := c.keeper.do(t, ctx, "read", c.rec.Name, "0", "4"); r.SHA256 != w.SHA256 {
			t.Fatalf("readback after the restore %q, wrote %q", r.SHA256, w.SHA256)
		}
		if verdict, why := c.release(t, ctx); verdict != dmReleasable {
			t.Fatalf("release of the restored node: %s", why)
		}
	})

	t.Run("foreign table changes", func(t *testing.T) {
		_, lower := f.loop(t, ctx, prefix+"-f")
		c := f.node(t, ctx, prefix+"-f", lower)
		linear := c.rec.Live
		// Another actor swaps in a half-size linear table: the same target,
		// so only the exact table tells it apart.
		other := &dmControl{keeper: f.keeper, rec: dmRecord{Name: c.rec.Name}}
		half := fmt.Sprintf("0 %d linear %s 0", dmControlSectors/2, lower)
		other.step(t, ctx, "load", half)
		other.step(t, ctx, "suspend", "")
		other.step(t, ctx, "resume", "")
		before := c.observe(t, ctx)
		if state := c.apply(t, ctx, errorTable(), ""); state != dmForeign {
			t.Fatalf("a foreign live table: %s", state)
		}
		after := c.observe(t, ctx)
		if after.Event != before.Event || after.Live == nil || normTable(*after.Live) != normTable(half) {
			t.Fatalf("the control acted on a foreign table: event %d -> %d, live %v", before.Event, after.Event, after.Live)
		}
		if verdict, _ := c.release(t, ctx); verdict != dmReleaseHeld {
			t.Fatal("a node with a foreign table was releasable")
		}
		// The other actor puts the recorded table back; then it loads an
		// inactive table of its own, which is the very table the control
		// wants, and still not the control's.
		other.step(t, ctx, "load", linear)
		other.step(t, ctx, "suspend", "")
		other.step(t, ctx, "resume", "")
		other.step(t, ctx, "load", errorTable())
		if state := c.apply(t, ctx, errorTable(), ""); state != dmForeign {
			t.Fatalf("an inactive table the control did not load: %s", state)
		}
		other.step(t, ctx, "clear", "")
		// The control loads a table of its own, and another actor replaces
		// it with the very table the control is then asked for.
		if state := c.apply(t, ctx, errorTable(), "load"); state != dmLoaded {
			t.Fatalf("the control's own load: %s", state)
		}
		other.step(t, ctx, "clear", "")
		other.step(t, ctx, "load", half)
		before = c.observe(t, ctx)
		if state := c.apply(t, ctx, half, ""); state != dmForeign {
			t.Fatalf("an inactive table that replaced the control's load: %s", state)
		}
		after = c.observe(t, ctx)
		if after.Event != before.Event || after.State != "ACTIVE" || after.Inactive == nil || normTable(*after.Inactive) != normTable(half) {
			t.Fatalf("the control acted on a replaced load: event %d -> %d, state %s, inactive %v", before.Event, after.Event, after.State, after.Inactive)
		}
		if verdict, _ := c.release(t, ctx); verdict != dmReleaseHeld {
			t.Fatal("a node with a replaced load was releasable")
		}
		// The other actor puts the control's table back, and the control
		// drops its own load.
		other.step(t, ctx, "clear", "")
		other.step(t, ctx, "load", errorTable())
		if state := c.revert(t, ctx, errorTable()); state != dmApplied {
			t.Fatalf("revert of the restored load: %s", state)
		}
		if state := c.apply(t, ctx, linear, ""); state != dmApplied {
			t.Fatalf("after the foreign changes were undone: %s", state)
		}
		if verdict, why := c.release(t, ctx); verdict != dmReleasable {
			t.Fatalf("release: %s", why)
		}
	})

	t.Run("identity reuse", func(t *testing.T) {
		loop, lower := f.loop(t, ctx, prefix+"-r")
		c := f.node(t, ctx, prefix+"-r", lower)
		if verdict, why := c.release(t, ctx); verdict != dmReleasable {
			t.Fatalf("release: %s", why)
		}
		// Another actor creates a node of the same name, over the same loop.
		impostor := "TC-OTHER-" + c.rec.Name
		if r := f.keeper.do(t, ctx, "create", c.rec.Name, impostor, lower, strconv.FormatInt(dmControlSectors, 10)); !r.Created {
			t.Fatalf("create the reused name: %s", r.Error)
		}
		v := c.observe(t, ctx)
		receipt["reused_node"] = map[string]any{"uuid": v.UUID, "major": v.Major, "minor": v.Minor, "owned_minor": c.rec.Minor}
		if state := classifyDM(c.rec, c.rec.Live, v); state != dmIdentityChanged {
			t.Fatalf("a node created again under the owned name: %s", state)
		}
		if state := c.apply(t, ctx, errorTable(), ""); state != dmIdentityChanged {
			t.Fatalf("the control acted on a reused name: %s", state)
		}
		if verdict, _ := c.releaseOwned(ctx); verdict != dmReleaseHeld {
			t.Fatal("a reused name was releasable")
		}
		if v := c.observe(t, ctx); !v.Exists || v.UUID != impostor {
			t.Fatalf("cleanup touched the foreign node: %+v", v)
		}
		theirs := &dmControl{keeper: f.keeper, rec: dmRecord{Name: c.rec.Name, UUID: impostor}}
		theirs.step(t, ctx, "remove", "")
		// The loop is released and the kernel may give its number to the
		// next file; the owned identity must not match that file.
		if r := f.keeper.do(t, ctx, "unloop", "-", loop.Loop, loop.File, strconv.FormatUint(loop.Inode, 10)); !r.Detached {
			t.Fatalf("detach %s: %s", loop.Loop, r.Error)
		}
		f.loops = f.loops[:len(f.loops)-1]
		next, _ := f.loop(t, ctx, prefix+"-n")
		oldBacking := dmBacking{name: c.rec.Name, loopIdentity: loop}
		seen, releaseErr := oldBacking.release(ctx, f.keeper, nil)
		receipt["reused_loop"] = map[string]any{"owned": loop, "next": next, "seen": seen, "verdict": fmt.Sprint(releaseErr)}
		if releaseErr == nil {
			t.Fatal("a loop number given to another file was releasable as the owned one")
		}
		if next.Loop == loop.Loop && seen.Inode != next.Inode {
			t.Fatalf("the reused loop %s reports inode %d, the new file has %d", loop.Loop, seen.Inode, next.Inode)
		}
	})

	t.Run("lazy loop detach", func(t *testing.T) {
		loop, _ := f.loop(t, ctx, prefix+"-z")
		// An opener that is no holder keeps the loop busy: the detach is
		// accepted lazily and the file must stay.
		if r := f.keeper.do(t, ctx, "hold", "-", loop.Loop, "5"); !r.Started {
			t.Fatalf("hold %s open: %+v", loop.Loop, r)
		}
		seen := f.keeper.do(t, ctx, "loopid", "-", loop.Loop)
		if ok, why := judgeLoopRelease(loop, seen, nil); !ok {
			t.Fatalf("an open loop without holders: %s", why)
		}
		r := f.keeper.do(t, ctx, "unloop", "-", loop.Loop, loop.File, strconv.FormatUint(loop.Inode, 10))
		receipt["lazy_detach"] = r
		if r.Detached || !r.Lazy || r.FileRemoved {
			t.Fatalf("detach of a busy loop: %+v", r)
		}
		// The opener exits and the kernel releases the loop; only then is
		// the file removed.
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			if f.keeper.do(t, ctx, "loopid", "-", loop.Loop).Error != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s was not released after its opener exited", loop.Loop)
			}
		}
		if r := f.keeper.do(t, ctx, "rmfile", "-", loop.File, strconv.FormatUint(loop.Inode, 10)); !r.FileRemoved {
			t.Fatalf("remove %s: %+v", loop.File, r)
		}
		f.loops = f.loops[:len(f.loops)-1]
	})

	t.Run("pending I/O and a cancelled step", func(t *testing.T) {
		_, backing := f.loop(t, ctx, prefix+"-p")
		lower := f.node(t, ctx, prefix+"-pl", backing)
		top := f.node(t, ctx, prefix+"-pt", fmt.Sprintf("%d:%d", lower.rec.Major, lower.rec.Minor))
		linear := top.rec.Live
		// Another actor suspends the lower node: I/O through the top node
		// now waits there.
		holder := &dmControl{keeper: f.keeper, rec: dmRecord{Name: lower.rec.Name}}
		holder.step(t, ctx, "suspend", "")
		inflight := top.keeper.do(t, ctx, "write", top.rec.Name, "0", "1", "in-flight")
		v := waitInFlight(t, ctx, top, 10*time.Second)
		receipt["pending"] = map[string]any{"open": v.Open, "in_flight": v.InFlight}

		// The control's change of the top node: the load completes, the
		// suspend waits for the in-flight write and passes its bound.
		if state := top.apply(t, ctx, errorTable(), ""); state != dmStepUnsettled || top.rec.Unsettled != "suspend" {
			t.Fatalf("a suspend behind pending I/O: %s, unsettled %q", state, top.rec.Unsettled)
		}
		v = top.observe(t, ctx)
		receipt["cancelled"] = map[string]any{"state": v.State, "open": v.Open, "in_flight": v.InFlight, "live": v.Live, "inactive": v.Inactive}
		if v.State != "ACTIVE" || v.Inactive == nil || v.InFlight[1] == 0 {
			t.Fatalf("while the suspend waits: %+v", v)
		}
		if verdict, _ := judgeDMRelease(top.rec, v); verdict != dmReleaseHeld {
			t.Fatal("a node with an unsettled suspend and a write in flight was releasable")
		}
		if state := top.apply(t, ctx, errorTable(), ""); state != dmStepUnsettled {
			t.Fatalf("the control acted past an unsettled step: %s", state)
		}
		if r := top.keeper.do(t, ctx, "written", top.rec.Name); r.Done {
			t.Fatalf("the in-flight write finished while the lower node was suspended: %+v", r)
		}

		// The lower node resumes: the write completes on the old table and
		// the abandoned suspend finishes.
		holder.step(t, ctx, "resume", "")
		waitWritten(t, ctx, top, 30*time.Second, 0)
		top.settle(t, ctx, 30*time.Second)
		v = top.observe(t, ctx)
		if state := classifyDM(top.rec, errorTable(), v); state != dmSuspendedLoaded {
			t.Fatalf("after the suspend finished: %s (%+v)", state, v)
		}
		if r := lower.keeper.do(t, ctx, "read", lower.rec.Name, "0", "1"); r.SHA256 != inflight.SHA256 {
			t.Fatalf("the in-flight write reads back %q through the lower node, wrote %q", r.SHA256, inflight.SHA256)
		}

		// A write queued behind the suspension meets the table the resume
		// installs.
		queued := top.keeper.do(t, ctx, "write", top.rec.Name, "1", "1", "queued")
		waitOpen(t, ctx, top, 10*time.Second)
		if r := top.keeper.do(t, ctx, "written", top.rec.Name); r.Done {
			t.Fatalf("a write to the suspended node finished: %+v", r)
		}
		if verdict, _ := judgeDMRelease(top.rec, top.observe(t, ctx)); verdict != dmReleaseHeld {
			t.Fatal("a suspended node with a queued write was releasable")
		}
		if state := top.apply(t, ctx, errorTable(), ""); state != dmApplied {
			t.Fatalf("completing the fault: %s", state)
		}
		r := waitWritten(t, ctx, top, 30*time.Second, -1)
		receipt["queued"] = map[string]any{"rc": r.RC, "error": r.Error}
		if r.RC == 0 {
			t.Fatal("a write queued behind the suspension succeeded on the error table")
		}
		if got := lower.keeper.do(t, ctx, "read", lower.rec.Name, "1", "1"); got.SHA256 == queued.SHA256 {
			t.Fatal("the queued write reached the store through the error table")
		}

		// Restore, then release top before lower.
		if state := top.apply(t, ctx, linear, ""); state != dmApplied {
			t.Fatalf("restore: %s", state)
		}
		if verdict, why := top.release(t, ctx); verdict != dmReleasable {
			t.Fatalf("release top: %s", why)
		}
		if verdict, why := lower.release(t, ctx); verdict != dmReleasable {
			t.Fatalf("release lower: %s", why)
		}
	})
}

// waitWritten waits for the node's background write to finish and requires
// its exit status to be want, or any failure when want is negative.
func waitWritten(t *testing.T, ctx context.Context, c *dmControl, bound time.Duration, want int) dmControlReply {
	t.Helper()
	for deadline := time.Now().Add(bound); ; time.Sleep(200 * time.Millisecond) {
		r := c.keeper.do(t, ctx, "written", c.rec.Name)
		if r.Done {
			if (want >= 0 && r.RC != want) || (want < 0 && r.RC == 0) {
				t.Fatalf("write to %s exited %d (%s), want %d", c.rec.Name, r.RC, r.Error, want)
			}
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("write to %s did not finish within %s", c.rec.Name, bound)
		}
	}
}

// waitInFlight waits until the node has a write in flight.
func waitInFlight(t *testing.T, ctx context.Context, c *dmControl, bound time.Duration) dmControlReply {
	t.Helper()
	for deadline := time.Now().Add(bound); ; time.Sleep(200 * time.Millisecond) {
		v := c.observe(t, ctx)
		if len(v.InFlight) == 2 && v.InFlight[1] > 0 {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has no write in flight: %+v", c.rec.Name, v)
		}
	}
}

// waitOpen waits until something holds the node open.
func waitOpen(t *testing.T, ctx context.Context, c *dmControl, bound time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(bound); ; time.Sleep(200 * time.Millisecond) {
		if c.observe(t, ctx).Open > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing opened %s", c.rec.Name)
		}
	}
}
