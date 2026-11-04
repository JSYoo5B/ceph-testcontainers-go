package quiesce

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/volbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// setWatchSource is a scripted set read: the native state and the failure of
// the next reads, both changed by the test while the watch goroutine reads
// them.
type setWatchSource struct {
	mu    sync.Mutex
	state State
	fail  error
	block bool
	reads int
}

func newSetWatchSource() *setWatchSource {
	return &setWatchSource{state: State{ID: "tc-set", Version: 12, State: "QUIESCED", Timeout: 10, Expiration: 30, Members: []string{"file:/volumes/group/volume/unique-id"}}}
}

func (s *setWatchSource) read(ctx context.Context) (*State, error) {
	s.mu.Lock()
	s.reads++
	block, fail := s.block, s.fail
	state := s.state
	state.Members = slices.Clone(s.state.Members)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail != nil {
		return nil, fail
	}
	return &state, nil
}

func (s *setWatchSource) update(change func(*setWatchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
}

func (s *setWatchSource) transition(state string) {
	s.update(func(s *setWatchSource) {
		s.state.State = state
		s.state.Version++
	})
}

func (s *setWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *setWatchSource) awaitReads(t *testing.T, n int) {
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

func fastSetWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

func startTestSetWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *setWatchSource) *SetWatch {
	t.Helper()
	w, err := startSetWatch(ctx, settings, "tc-set", 12, source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func nextSetEvent(t *testing.T, w *SetWatch) SetWatchEvent {
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
	return SetWatchEvent{}
}

// requireSetWatchClosed drains the buffered events and fails unless the
// channel is closed after them.
func requireSetWatchClosed(t *testing.T, w *SetWatch) []SetWatchEvent {
	t.Helper()
	var rest []SetWatchEvent
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

func awaitSetWatchDone(t *testing.T, w *SetWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

func TestSetWatchBaselineIsBufferedBeforeWatchReturns(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Err != nil || event.State.State != "QUIESCED" || event.State.Version != 12 {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("Watch returned before the baseline was buffered")
	}
}

func TestSetWatchUnchangedStateProducesNoEvent(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(8), source)
	nextSetEvent(t, w)
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("identical reads produced %d events", n)
	}
}

// Expiration is the loss of the quiesced condition that a checkpoint must
// notice: it is the last event and an outcome, not a watch error.
func TestSetWatchReportsTransitionsAndEndsAfterTheTerminalState(t *testing.T) {
	for _, terminal := range []string{"EXPIRED", "TIMEDOUT", "CANCELED", "FAILED", "RELEASED"} {
		t.Run(terminal, func(t *testing.T) {
			source := newSetWatchSource()
			source.state.State, source.state.Version = "QUIESCING", 11
			w, err := startSetWatch(t.Context(), fastSetWatch(8), "tc-set", 0, source.read)
			if err != nil {
				t.Fatal(err)
			}
			if baseline := nextSetEvent(t, w); baseline.State.State != "QUIESCING" {
				t.Fatalf("baseline = %+v", baseline.State)
			}
			source.transition("QUIESCED")
			if quiesced := nextSetEvent(t, w); quiesced.Kind != ceph.WatchChanged || quiesced.State.State != "QUIESCED" || quiesced.State.Version != 12 || quiesced.Sequence != 2 {
				t.Fatalf("quiesced = %+v", quiesced)
			}
			source.transition(terminal)
			ended := nextSetEvent(t, w)
			if ended.Kind != ceph.WatchChanged || ended.State.State != terminal || ended.State.Version != 13 || ended.Err != nil {
				t.Fatalf("terminal = %+v", ended)
			}
			awaitSetWatchDone(t, w)
			if err := w.Stop(); err != nil {
				t.Fatalf("Stop after %s = %v", terminal, err)
			}
			if rest := requireSetWatchClosed(t, w); len(rest) != 0 {
				t.Fatalf("events after the terminal state: %+v", rest)
			}
		})
	}
}

// An outside edit of the timer is a new version of the same set: a change,
// not the end of the watch.
func TestSetWatchReportsAnOutsideTimerEdit(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(8), source)
	nextSetEvent(t, w)
	source.update(func(s *setWatchSource) { s.state.Expiration, s.state.Version = 40, 13 })
	edited := nextSetEvent(t, w)
	if edited.Kind != ceph.WatchChanged || edited.State.State != "QUIESCED" || edited.State.Expiration != 40 || edited.State.Version != 13 {
		t.Fatalf("edited = %+v", edited)
	}
}

func TestSetWatchTerminalBaselineIsTheOnlyEvent(t *testing.T) {
	source := newSetWatchSource()
	source.transition("EXPIRED")
	w := startTestSetWatch(t, t.Context(), fastSetWatch(1), source)
	if rest := requireSetWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline || rest[0].State.State != "EXPIRED" {
		t.Fatalf("events = %+v", rest)
	}
	if err := w.Stop(); err != nil || source.readCount() != 1 {
		t.Fatalf("Stop = %v after %d reads", err, source.readCount())
	}
}

func TestSetWatchFailedBaselineStartsNothing(t *testing.T) {
	for name, fail := range map[string]error{
		"transient": errors.New("transient"),
		"absent":    ownershipError("native quiesce set is absent"),
	} {
		t.Run(name, func(t *testing.T) {
			source := newSetWatchSource()
			source.fail = fail
			w, err := startSetWatch(t.Context(), fastSetWatch(1), "tc-set", 12, source.read)
			if w != nil || !errors.Is(err, fail) {
				t.Fatalf("Watch = %v, %v", w, err)
			}
			time.Sleep(10 * time.Millisecond)
			if source.readCount() != 1 {
				t.Fatalf("a failed baseline kept reading: %d", source.readCount())
			}
		})
	}
}

func TestSetWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(64), source)
	baseline := nextSetEvent(t, w)
	boom := errors.New("transient")
	source.update(func(s *setWatchSource) { s.fail = boom })
	gap := nextSetEvent(t, w)
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, boom) || gap.State != nil || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *setWatchSource) { s.fail = nil })
	var resumed SetWatchEvent
	for resumed = nextSetEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextSetEvent(t, w) {
	}
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || resumed.State.Version != baseline.State.Version {
		t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
	}
	source.transition("EXPIRED")
	if expired := nextSetEvent(t, w); expired.Kind != ceph.WatchChanged || expired.Sequence != resumed.Sequence+1 || expired.State.State != "EXPIRED" {
		t.Fatalf("expired = %+v", expired)
	}
	awaitSetWatchDone(t, w)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestSetWatchOwnershipLossEndsTheWatch(t *testing.T) {
	for name, edit := range map[string]func(*setWatchSource){
		"absent":              func(s *setWatchSource) { s.fail = ownershipError("native quiesce set is absent") },
		"members changed":     func(s *setWatchSource) { s.fail = ownershipError("native quiesce members changed; refusing ownership") },
		"filesystem replaced": func(s *setWatchSource) { s.fail = fsbase.IdentityRefusal("owned filesystem is absent from FSMap") },
		"version went back":   func(s *setWatchSource) { s.state.Version = 3; s.state.State = "QUIESCED" },
	} {
		t.Run(name, func(t *testing.T) {
			source := newSetWatchSource()
			w := startTestSetWatch(t, t.Context(), fastSetWatch(4), source)
			nextSetEvent(t, w)
			source.update(edit)
			awaitSetWatchDone(t, w)
			err := w.Stop()
			if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), `watch quiesce set "tc-set"`) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireSetWatchClosed(t, w); len(rest) != 0 {
				t.Fatalf("a refused read produced events: %+v", rest)
			}
		})
	}
	// A baseline below the version Start confirmed is another set.
	source := newSetWatchSource()
	source.state.Version = 5
	if w, err := startSetWatch(t.Context(), fastSetWatch(1), "tc-set", 12, source.read); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "went back from 12 to 5") {
		t.Fatalf("recreated baseline = %v, %v", w, err)
	}
}

func TestSetWatchEventsAreIndependent(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(8), source)
	baseline := nextSetEvent(t, w)
	baseline.State.State = "EXPIRED"
	baseline.State.Members[0] = "file:/edited"
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.update(func(s *setWatchSource) { s.state.Version = 13 })
	changed := nextSetEvent(t, w)
	if changed.State.State != "QUIESCED" || changed.State.Members[0] != "file:/volumes/group/volume/unique-id" {
		t.Fatalf("a later event shared a delivered state: %+v", changed.State)
	}
}

func TestSetWatchStalledConsumerOverflowsWithAnAttributableCause(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(2), source)
	source.update(func(s *setWatchSource) { s.state.Version = 13 })
	source.awaitReads(t, 2)
	source.update(func(s *setWatchSource) { s.state.Version = 14 })
	awaitSetWatchDone(t, w)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), `watch quiesce set "tc-set": event 3 (changed)`) {
		t.Fatalf("Stop = %v", err)
	}
}

func TestSetWatchStopAndContextEndDuringQuery(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(1), source)
	source.update(func(s *setWatchSource) { s.block = true })
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
	requireSetWatchClosed(t, w)

	source = newSetWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestSetWatch(t, ctx, fastSetWatch(1), source)
	source.update(func(s *setWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	awaitSetWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
}

func TestSetWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	source := newSetWatchSource()
	w := startTestSetWatch(t, t.Context(), fastSetWatch(1), source)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireSetWatchClosed(t, w)
	var zero *SetWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&SetWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

// lockedQuiesceControl serializes the quiesce fake, so the test can edit the
// native set while the watch goroutine queries it.
type lockedQuiesceControl struct {
	mu sync.Mutex
	*quiesceTestControl
}

func (c *lockedQuiesceControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.quiesceTestControl.Exec(ctx, args, opts...)
}

func (c *lockedQuiesceControl) edit(change func(*quiesceTestControl)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	change(c.quiesceTestControl)
}

func (c *lockedQuiesceControl) queries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, call := range c.Calls {
		if slices.Contains(call, "--query") {
			n++
		}
	}
	return n
}

// The watch reads the set without a mutation, so it never renews the native
// timer, and the consumer releases the set while it runs: RELEASED is the
// last event.
func TestSetWatchObservesWithoutMutatingAndEndsOnRelease(t *testing.T) {
	fs, fake, volume := quiesceFixture(t)
	control := &lockedQuiesceControl{quiesceTestControl: fake}
	fs.Core.Cluster().Container = control
	q, err := start(t.Context(), fs.Core, []*volbase.Volume{volume}, Config{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w, err := q.Watch(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	if baseline := nextSetEvent(t, w); baseline.State.State != "QUIESCED" || baseline.State.Version != 12 || baseline.State.ID != q.ID() {
		t.Fatalf("baseline = %+v", baseline.State)
	}
	for start := control.queries(); control.queries() < start+3; {
		time.Sleep(10 * time.Millisecond)
	}
	control.edit(func(c *quiesceTestControl) {
		if c.mutations != 1 {
			t.Errorf("the watch ran %d mutations", c.mutations-1)
		}
	})
	if err := q.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if released := nextSetEvent(t, w); released.State.State != "RELEASED" || released.State.Version != 13 {
		t.Fatalf("released = %+v", released.State)
	}
	awaitSetWatchDone(t, w)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestSetWatchEndsWhenTheMembersChange(t *testing.T) {
	fs, fake, volume := quiesceFixture(t)
	control := &lockedQuiesceControl{quiesceTestControl: fake}
	fs.Core.Cluster().Container = control
	q, err := start(t.Context(), fs.Core, []*volbase.Volume{volume}, Config{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w, err := q.Watch(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	nextSetEvent(t, w)
	control.edit(func(c *quiesceTestControl) {
		c.members["file:/volumes/foreign"] = map[string]any{"excluded": false}
		c.version++
	})
	awaitSetWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "members changed") {
		t.Fatalf("Stop = %v", err)
	}
}

func TestSetWatchRefusesInvalidConfigAndUnstartedSets(t *testing.T) {
	fs, fake, volume := quiesceFixture(t)
	q, err := start(t.Context(), fs.Core, []*volbase.Volume{volume}, Config{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	calls := len(fake.Calls)
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := q.Watch(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch quiesce set") {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if len(fake.Calls) != calls {
		t.Fatalf("an invalid config ran commands: %v", fake.Calls[calls:])
	}
	for name, unstarted := range map[string]*Set{"nil": nil, "zero": {}} {
		if w, err := unstarted.Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	got, err := watch.Validate(0, 0, 0, setWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
}
