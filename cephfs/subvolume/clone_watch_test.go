package subvolume

import (
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cloneWatchSource is a scripted cloneStatusRead: the native status and the
// failure of the next reads, both changed by the test while the watch
// goroutine reads them.
type cloneWatchSource struct {
	mu     sync.Mutex
	status CloneStatus
	fail   error
	block  bool
	reads  int
}

func newCloneWatchSource() *cloneWatchSource {
	return &cloneWatchSource{status: CloneStatus{State: "pending", SourceFilesystem: "fixture", SourceSubvolume: "volume", SourceGroup: "group", SourceSnapshot: "checkpoint"}}
}

func (s *cloneWatchSource) read(ctx context.Context) (*CloneStatus, error) {
	s.mu.Lock()
	s.reads++
	block, fail := s.block, s.fail
	status := s.status
	status.Progress = maps.Clone(s.status.Progress)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail != nil {
		return nil, fail
	}
	return &status, nil
}

func (s *cloneWatchSource) update(change func(*cloneWatchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
}

func (s *cloneWatchSource) progress(percentage string) {
	s.update(func(s *cloneWatchSource) {
		s.status.State = "in-progress"
		s.status.Progress = map[string]string{"percentage cloned": percentage, "files cloned": "1/2"}
	})
}

func (s *cloneWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// awaitReads waits until the source has started n more reads, each of which
// sampled the state as it is now or later.
func (s *cloneWatchSource) awaitReads(t *testing.T, n int) {
	t.Helper()
	want := s.readCount() + n
	deadline := time.Now().Add(5 * time.Second)
	for s.readCount() < want {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
		time.Sleep(time.Millisecond)
	}
}

func fastCloneWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

func startTestCloneWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *cloneWatchSource) *CloneWatch {
	t.Helper()
	w, err := startCloneWatch(ctx, settings, "copy", source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func nextCloneEvent(t *testing.T, w *CloneWatch) CloneWatchEvent {
	t.Helper()
	select {
	case event, ok := <-w.Events():
		if !ok {
			t.Fatalf("events closed: %v", w.Stop())
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	return CloneWatchEvent{}
}

// requireCloneWatchClosed drains the buffered events and fails unless the
// channel is closed after them.
func requireCloneWatchClosed(t *testing.T, w *CloneWatch) []CloneWatchEvent {
	t.Helper()
	var rest []CloneWatchEvent
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				return rest
			}
			rest = append(rest, event)
		case <-time.After(5 * time.Second):
			t.Fatal("events not closed")
		}
	}
}

func awaitCloneWatchDone(t *testing.T, w *CloneWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

func TestCloneWatchBaselineIsBufferedBeforeWatchReturns(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Err != nil || !reflect.DeepEqual(*event.Status, source.status) {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("Watch returned before the baseline was buffered")
	}
}

func TestCloneWatchUnchangedStatusProducesNoEvent(t *testing.T) {
	source := newCloneWatchSource()
	source.progress("10.00%")
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(4), source)
	nextCloneEvent(t, w)
	source.awaitReads(t, 5)
	// A fresh but equal Progress map is not a change.
	source.progress("10.00%")
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged status", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := requireCloneWatchClosed(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged status: %+v", rest)
	}
}

func TestCloneWatchReportsProgressInOrderAndEndsAfterTheTerminalStatus(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(8), source)
	// Unread by the consumer until the end: none may be replaced by a later one.
	for _, percentage := range []string{"25.00%", "75.00%"} {
		source.progress(percentage)
		source.awaitReads(t, 3)
	}
	source.update(func(s *cloneWatchSource) {
		s.status = CloneStatus{State: "complete"}
	})
	awaitCloneWatchDone(t, w)
	reads := source.readCount()
	if err := w.Stop(); err != nil {
		t.Fatalf("a delivered terminal status ended the watch with %v", err)
	}
	events := requireCloneWatchClosed(t, w)
	want := []struct{ state, percentage string }{{"pending", ""}, {"in-progress", "25.00%"}, {"in-progress", "75.00%"}, {"complete", ""}}
	if len(events) != len(want) {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) || event.Err != nil || event.Status.State != want[i].state || event.Status.Progress["percentage cloned"] != want[i].percentage {
			t.Fatalf("event %d = %+v", i, event)
		}
		if i > 0 && event.Kind != ceph.WatchChanged {
			t.Fatalf("event %d kind = %s", i, event.Kind)
		}
	}
	if last := events[3].Status; last.SourceSubvolume != "" || last.Progress != nil {
		t.Fatalf("the complete status gained source or progress: %+v", last)
	}
	time.Sleep(20 * time.Millisecond)
	if source.readCount() != reads {
		t.Fatal("the watch read again after the terminal status")
	}
}

func TestCloneWatchFailedAndCanceledClonesAreOutcomesNotWatchErrors(t *testing.T) {
	for _, terminal := range []CloneStatus{
		{State: "failed", SourceFilesystem: "fixture", SourceSubvolume: "volume", SourceGroup: "group", SourceSnapshot: "checkpoint", FailureErrno: 21, FailureMessage: "Is a directory"},
		{State: "canceled", SourceFilesystem: "fixture", SourceSubvolume: "volume", SourceGroup: "group", SourceSnapshot: "checkpoint", FailureErrno: 4, FailureMessage: "user interrupted clone operation"},
	} {
		t.Run(terminal.State, func(t *testing.T) {
			source := newCloneWatchSource()
			w := startTestCloneWatch(t, t.Context(), fastCloneWatch(4), source)
			source.update(func(s *cloneWatchSource) { s.status = terminal })
			awaitCloneWatchDone(t, w)
			if err := w.Stop(); err != nil {
				t.Fatalf("Stop = %v", err)
			}
			events := requireCloneWatchClosed(t, w)
			if len(events) != 2 || events[1].Kind != ceph.WatchChanged || events[1].Err != nil || !reflect.DeepEqual(*events[1].Status, terminal) {
				t.Fatalf("events = %+v", events)
			}
		})
	}
}

func TestCloneWatchTerminalBaselineIsTheOnlyEvent(t *testing.T) {
	source := newCloneWatchSource()
	source.status = CloneStatus{State: "complete"}
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(1), source)
	if events := requireCloneWatchClosed(t, w); len(events) != 1 || events[0].Kind != ceph.WatchBaseline || events[0].Status.State != "complete" {
		t.Fatalf("events = %+v", events)
	}
	if err := w.Stop(); err != nil || source.readCount() != 1 {
		t.Fatalf("Stop = %v after %d reads", err, source.readCount())
	}
}

func TestCloneWatchFailedBaselineStartsNothing(t *testing.T) {
	source := newCloneWatchSource()
	boom := errors.New("boom")
	source.fail = boom
	if w, err := startCloneWatch(t.Context(), fastCloneWatch(1), "copy", source.read); w != nil || !errors.Is(err, boom) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	source.fail = cloneIdentityRefusal("native clone UUID, inode or birth time was replaced; refusing mutation")
	if w, err := startCloneWatch(t.Context(), fastCloneWatch(1), "copy", source.read); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("watch of a replaced baseline = %v, %v", w, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source = newCloneWatchSource()
	if w, err := startCloneWatch(ctx, fastCloneWatch(1), "copy", source.read); w != nil || !errors.Is(err, context.Canceled) || source.readCount() != 0 {
		t.Fatalf("watch with a canceled context = %v, %v after %d reads", w, err, source.readCount())
	}
}

func TestCloneWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(64), source)
	baseline := nextCloneEvent(t, w)
	boom := errors.New("transient")
	source.update(func(s *cloneWatchSource) { s.fail = boom })
	gap := nextCloneEvent(t, w)
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, boom) || gap.Status != nil || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *cloneWatchSource) { s.fail = nil })
	var resumed CloneWatchEvent
	for resumed = nextCloneEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextCloneEvent(t, w) {
		if !errors.Is(resumed.Err, boom) {
			t.Fatalf("gap = %+v", resumed)
		}
	}
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Status, baseline.Status) {
		t.Fatalf("resumed = %+v (an unchanged status after a gap is still reported)", resumed)
	}
	source.progress("50.00%")
	changed := nextCloneEvent(t, w)
	if changed.Kind != ceph.WatchChanged || changed.Sequence != resumed.Sequence+1 || changed.Status.State != "in-progress" {
		t.Fatalf("changed = %+v", changed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestCloneWatchIdentityChangeEndsTheWatch(t *testing.T) {
	for name, drift := range map[string]error{
		"target replaced":     cloneIdentityRefusal("native clone UUID, inode or birth time was replaced; refusing mutation"),
		"filesystem replaced": fsbase.IdentityRefusal("filesystem or its original pools were replaced; refusing operation"),
	} {
		t.Run(name, func(t *testing.T) {
			source := newCloneWatchSource()
			w := startTestCloneWatch(t, t.Context(), fastCloneWatch(4), source)
			source.update(func(s *cloneWatchSource) { s.fail = drift })
			awaitCloneWatchDone(t, w)
			err := w.Stop()
			if !errors.Is(err, drift) || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), `watch clone "copy"`) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireCloneWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("a drifted read produced events: %+v", rest)
			}
		})
	}
}

func TestCloneWatchSnapshotsAreIndependent(t *testing.T) {
	source := newCloneWatchSource()
	source.progress("10.00%")
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(8), source)
	baseline := nextCloneEvent(t, w)
	// A consumer that edits a delivered status changes neither the comparison
	// state nor a later event.
	baseline.Status.State = "complete"
	baseline.Status.Progress["percentage cloned"] = "99.00%"
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.progress("20.00%")
	first := nextCloneEvent(t, w)
	first.Status.Progress["files cloned"] = "edited"
	source.progress("30.00%")
	second := nextCloneEvent(t, w)
	if first.Status.State != "in-progress" || first.Status.Progress["percentage cloned"] != "20.00%" {
		t.Fatalf("first change = %+v", first.Status)
	}
	if second.Status == first.Status || second.Status.Progress["percentage cloned"] != "30.00%" || second.Status.Progress["files cloned"] != "1/2" {
		t.Fatalf("a later poll shared a delivered status: %+v then %+v", first.Status, second.Status)
	}
}

func TestCloneWatchStalledConsumerOverflowsWithAnAttributableCause(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(2), source)
	source.progress("10.00%")
	source.awaitReads(t, 2)
	source.progress("20.00%")
	awaitCloneWatchDone(t, w)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), `watch clone "copy": event 3 (changed)`) {
		t.Fatalf("Stop = %v", err)
	}
	rest := requireCloneWatchClosed(t, w)
	if len(rest) != 2 || rest[0].Kind != ceph.WatchBaseline || rest[1].Status.Progress["percentage cloned"] != "10.00%" {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestCloneWatchStopAndContextEndDuringQuery(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(1), source)
	source.update(func(s *cloneWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	stopped := make(chan error, 1)
	go func() { stopped <- w.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not interrupt the query")
	}
	if rest := requireCloneWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}

	source = newCloneWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestCloneWatch(t, ctx, fastCloneWatch(1), source)
	source.update(func(s *cloneWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	awaitCloneWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	requireCloneWatchClosed(t, w)
}

func TestCloneWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	source := newCloneWatchSource()
	w := startTestCloneWatch(t, t.Context(), fastCloneWatch(1), source)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireCloneWatchClosed(t, w)
	var zero *CloneWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&CloneWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

// lockedCloneControl serializes the clone fixture container, so the test can
// edit its canned output while the watch goroutine reads it.
type lockedCloneControl struct {
	mu sync.Mutex
	*cloneLifecycleFixtureContainer
}

func (c *lockedCloneControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cloneLifecycleFixtureContainer.Exec(ctx, args, opts...)
}

func (c *lockedCloneControl) edit(change func(*cloneLifecycleFixtureContainer)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	change(c.cloneLifecycleFixtureContainer)
}

func (c *lockedCloneControl) calls() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.Calls)
}

// awaitStatusReads waits until the fixture has answered n more status
// queries.
func (c *lockedCloneControl) awaitStatusReads(t *testing.T, n int) {
	t.Helper()
	count := func() int {
		reads := 0
		for _, call := range c.calls() {
			if strings.Join(call, " ") == cloneStatusQuery {
				reads++
			}
		}
		return reads
	}
	want := count() + n
	for deadline := time.Now().Add(5 * time.Second); count() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

func newCloneWatchFixture(t *testing.T) (*fstest.Filesystem, *lockedCloneControl, *Clone) {
	t.Helper()
	fs, base, clone := cloneLifecycleFixture(t)
	control := &lockedCloneControl{cloneLifecycleFixtureContainer: base}
	fs.Core.Cluster().Container = control
	return fs, control, clone
}

const cloneInProgressStatus = `{"status":{"state":"in-progress","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"},"progress_report":{"percentage cloned":"50.00%","amount cloned":"4M/8M","files cloned":"1/2"}}}`

func TestCloneWatchHoldsNoGateBetweenReadsOrDuringDelivery(t *testing.T) {
	fs, control, clone := newCloneWatchFixture(t)
	cluster := fs.Core.Cluster()
	w, err := clone.Watch(t.Context(), ceph.WatchConfig{Interval: time.Second, QueryTimeout: 100 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireUnlocked := func(phase string) {
		t.Helper()
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if !fs.SetupMu.TryLock() {
				t.Fatalf("the watch kept the subvolume operation gate %s", phase)
			}
			fs.SetupMu.Unlock()
			if mutex := testhelper.TestState(cluster).Mutex; !mutex.TryLock() {
				t.Fatalf("the watch kept the topology lock %s", phase)
			} else {
				mutex.Unlock()
			}
		}
	}
	requireUnlocked("between reads")
	// The unread baseline fills the buffer, so the in-progress status waits in
	// delivery: the consumer can still run fixture operations, here a status
	// read and the native cancellation of the clone it watches.
	control.edit(func(c *cloneLifecycleFixtureContainer) { c.Output[cloneStatusQuery] = cloneInProgressStatus })
	control.awaitStatusReads(t, 1)
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if status, err := clone.Status(t.Context()); err != nil || status.State != "in-progress" {
		t.Fatalf("consumer status during the watch = %+v, %v", status, err)
	}
	if err := clone.Cancel(t.Context()); err != nil {
		t.Fatalf("consumer cancellation during the watch = %v", err)
	}
	if baseline := nextCloneEvent(t, w); baseline.Kind != ceph.WatchBaseline || baseline.Status.State != "pending" {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextCloneEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.State != "in-progress" || changed.Status.Progress["amount cloned"] != "4M/8M" {
		t.Fatalf("changed = %+v", changed)
	}
	if canceled := nextCloneEvent(t, w); canceled.Kind != ceph.WatchChanged || canceled.Status.State != "canceled" {
		t.Fatalf("terminal = %+v", canceled)
	}
	if rest := requireCloneWatchClosed(t, w); len(rest) != 0 {
		t.Fatalf("events after the terminal status = %+v", rest)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("a canceled clone ended the watch with %v", err)
	}
	for _, call := range control.calls() {
		if slices.Contains(call, "module") && slices.Contains(call, "enable") {
			t.Fatalf("a read changed the volumes module: %v", call)
		}
	}
}

func TestCloneWatchHeldGateIsAGapNotABlockedWatch(t *testing.T) {
	fs, control, clone := newCloneWatchFixture(t)
	w, err := clone.Watch(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	nextCloneEvent(t, w)
	before := len(control.calls())
	// A consumer that holds the gate longer than QueryTimeout gets a gap
	// rather than a blocked watch, and the watch resumes after it.
	fs.SetupMu.Lock()
	gap := nextCloneEvent(t, w)
	fs.SetupMu.Unlock()
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) || gap.Status != nil {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextCloneEvent(t, w); event.Kind != ceph.WatchResumed; event = nextCloneEvent(t, w) {
		if event.Kind != ceph.WatchGap {
			t.Fatalf("event after the gate was released = %+v", event)
		}
	}
	if len(control.calls()) == before {
		t.Fatal("the watch resumed without a read")
	}
	// A failed native query is a gap too, never a terminal or empty status.
	control.edit(func(c *cloneLifecycleFixtureContainer) { c.Fail = cloneStatusQuery })
	if gap := nextCloneEvent(t, w); gap.Kind != ceph.WatchGap || gap.Err == nil || gap.Status != nil {
		t.Fatalf("failed status query = %+v", gap)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestCloneWatchEndsWhenTheFixtureEndsOrTheTargetChanges(t *testing.T) {
	for name, change := range map[string]func(*fstest.Filesystem, *lockedCloneControl, *Clone){
		"terminated cluster": func(fs *fstest.Filesystem, _ *lockedCloneControl, _ *Clone) {
			mutex := testhelper.TestState(fs.Core.Cluster()).Mutex
			mutex.Lock()
			testhelper.TestSetClosed(fs.Core.Cluster(), true)
			mutex.Unlock()
		},
		"removed filesystem": func(fs *fstest.Filesystem, _ *lockedCloneControl, _ *Clone) {
			mutex := testhelper.TestState(fs.Core.Cluster()).Mutex
			mutex.Lock()
			fs.Unregistered = true
			mutex.Unlock()
		},
		"replaced filesystem": func(_ *fstest.Filesystem, c *lockedCloneControl, _ *Clone) {
			c.edit(func(c *cloneLifecycleFixtureContainer) {
				c.Output["fs dump --format json"] = strings.Replace(c.Output["fs dump --format json"], `"id":41`, `"id":42`, 1)
			})
		},
		"replaced target UUID": func(_ *fstest.Filesystem, c *lockedCloneControl, _ *Clone) {
			c.edit(func(c *cloneLifecycleFixtureContainer) {
				c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
			})
		},
		"replaced target inode": func(_ *fstest.Filesystem, c *lockedCloneControl, _ *Clone) {
			c.edit(func(c *cloneLifecycleFixtureContainer) {
				c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], `"inode":71`, `"inode":72`)
			})
		},
		"replaced source": func(_ *fstest.Filesystem, c *lockedCloneControl, _ *Clone) {
			c.edit(func(c *cloneLifecycleFixtureContainer) {
				c.Output[cloneStatusQuery] = strings.Replace(cloneLifecycleStatus("pending"), `"snapshot":"checkpoint"`, `"snapshot":"other"`, 1)
			})
		},
		"removed target": func(fs *fstest.Filesystem, _ *lockedCloneControl, clone *Clone) {
			fs.SetupMu.Lock()
			clone.identity.removed = true
			fs.SetupMu.Unlock()
		},
	} {
		t.Run(name, func(t *testing.T) {
			fs, control, clone := newCloneWatchFixture(t)
			w, err := clone.Watch(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Stop() })
			change(fs, control, clone)
			awaitCloneWatchDone(t, w)
			if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), `watch clone "copy"`) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireCloneWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("events = %+v", rest)
			}
			for _, call := range control.calls() {
				if slices.Contains(call, "cancel") || slices.Contains(call, "rm") || slices.Contains(call, "enable") {
					t.Fatalf("the watch mutated the fixture: %v", call)
				}
			}
		})
	}
}

func TestCloneWatchRefusesInvalidConfigAndUnownedClones(t *testing.T) {
	_, control, clone := newCloneWatchFixture(t)
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := clone.Watch(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch clone") {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if calls := control.calls(); len(calls) != 0 {
		t.Fatalf("an invalid config ran commands: %v", calls)
	}
	if w, err := (&Clone{Name: "copy"}).Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("unowned clone = %v, %v", w, err)
	}
	clone.identity.incarnation = nil
	if w, err := clone.Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("uncaptured incarnation = %v, %v", w, err)
	}
	got, err := watch.Validate(0, 0, 0, cloneWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 3 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
}
