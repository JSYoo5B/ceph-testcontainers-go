package ceph

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

const osdUUID2 = "33333333-3333-4333-8333-333333333333"

// watchSource is a scripted osdSampleRead: the native map, the owned
// registrations and the failure of the next reads, all changed by the test
// while the watch goroutine reads them.
type watchSource struct {
	mu    sync.Mutex
	epoch uint64
	osds  map[int]OSDState
	owned map[int]*osdRegistration
	fail  error
	block bool
	delay time.Duration
	reads int
}

func newWatchSource() *watchSource {
	return &watchSource{
		epoch: 10,
		osds:  map[int]OSDState{0: {ID: 0, UUID: osdUUID0, Up: true, In: true, Weight: 1}, 1: {ID: 1, UUID: osdUUID1, Up: true, In: true, Weight: 1}},
		owned: map[int]*osdRegistration{0: {nativeUUID: osdUUID0}, 1: {nativeUUID: osdUUID1}},
	}
}

func (s *watchSource) read(ctx context.Context) (osdPolicySnapshot, map[int]*osdRegistration, error) {
	s.mu.Lock()
	s.reads++
	block, fail, delay := s.block, s.fail, s.delay
	dump := osdPolicySnapshot{epoch: s.epoch, osds: maps.Clone(s.osds)}
	owned := make(map[int]*osdRegistration, len(s.owned))
	for id, reg := range s.owned {
		copied := *reg
		owned[id] = &copied
	}
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return osdPolicySnapshot{}, nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if block {
		<-ctx.Done()
		return osdPolicySnapshot{}, nil, ctx.Err()
	}
	if fail != nil {
		return osdPolicySnapshot{}, nil, fail
	}
	return dump, owned, nil
}

func (s *watchSource) update(change func(*watchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
	s.epoch++
}

func (s *watchSource) setUp(id int, up bool) {
	s.update(func(s *watchSource) {
		state := s.osds[id]
		state.Up = up
		s.osds[id] = state
	})
}

func (s *watchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// awaitReads waits until the source has started n more reads, each of which
// sampled the state as it is now or later.
func (s *watchSource) awaitReads(t *testing.T, n int) {
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

func fastWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

func startTestWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *watchSource) *OSDWatch {
	t.Helper()
	w, err := startOSDWatch(ctx, settings, source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func nextEvent(t *testing.T, w *OSDWatch) OSDWatchEvent {
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
	return OSDWatchEvent{}
}

// requireClosed drains the buffered events and fails unless the channel is
// closed after them.
func requireClosed(t *testing.T, w *OSDWatch) []OSDWatchEvent {
	t.Helper()
	var rest []OSDWatchEvent
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

func awaitDone(t *testing.T, w *OSDWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

func TestWatchBaselineIsBufferedBeforeWatchOSDsReturns(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		want := []OSDState{source.osds[0], source.osds[1]}
		if event.Sequence != 1 || event.Kind != WatchBaseline || event.Epoch != 10 || event.Err != nil || !reflect.DeepEqual(event.States, want) {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchOSDs returned before the baseline was buffered")
	}
}

func TestOSDWatchUnchangedStateProducesNoEvent(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(4), source)
	nextEvent(t, w)
	source.awaitReads(t, 5)
	// An epoch change alone is not a change of the compared fields.
	source.update(func(*watchSource) {})
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged state", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := requireClosed(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged state: %+v", rest)
	}
}

func TestOSDWatchReportsChangesInOrderWithoutOverwriting(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(8), source)
	// healthy -> down -> healthy, each kept for several reads and unread by
	// the consumer until the end: none may be replaced by a later one.
	for _, up := range []bool{false, true, false, true} {
		source.setUp(1, up)
		source.awaitReads(t, 3)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := requireClosed(t, w)
	if len(events) != 5 {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("event %d has sequence %d", i, event.Sequence)
		}
		if i == 0 {
			continue
		}
		if event.Kind != WatchChanged || event.States[1].Up != (i%2 == 0) || !event.States[0].Up {
			t.Fatalf("event %d = %+v", i, event)
		}
		if event.Epoch <= events[i-1].Epoch {
			t.Fatalf("epoch did not advance: %d after %d", event.Epoch, events[i-1].Epoch)
		}
	}
}

func TestOSDWatchAlreadyCanceledContextStartsNothing(t *testing.T) {
	source := newWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := startOSDWatch(ctx, fastWatch(1), source.read); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	if source.readCount() != 0 {
		t.Fatal("a canceled context read the cluster")
	}
	cluster, ctr := newWatchCluster()
	observer, _ := Observe(cluster)
	if w, err := observer.WatchOSDs(ctx, WatchConfig{}); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("observer watch = %v, %v", w, err)
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("a canceled context ran commands: %q", got)
	}
}

func TestOSDWatchFailedBaselineStartsNothing(t *testing.T) {
	source := newWatchSource()
	boom := errors.New("boom")
	source.fail = boom
	if w, err := startOSDWatch(t.Context(), fastWatch(1), source.read); w != nil || !errors.Is(err, boom) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	source = newWatchSource()
	source.owned = map[int]*osdRegistration{}
	if w, err := startOSDWatch(t.Context(), fastWatch(1), source.read); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("watch of no OSD = %v, %v", w, err)
	}
	source = newWatchSource()
	source.owned[0].nativeUUID = osdUUID2
	if w, err := startOSDWatch(t.Context(), fastWatch(1), source.read); w != nil || err == nil {
		t.Fatalf("watch of a replaced baseline = %v, %v", w, err)
	}
}

func TestOSDWatchStopDuringQuery(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(1), source)
	source.update(func(s *watchSource) { s.block = true })
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
	if rest := requireClosed(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
}

// A read that takes at least one interval makes the next read overdue, so
// the watch goes straight to it without a timed wait. Stop must still be
// recognized as the cause there.
func TestOSDWatchStopWhenTheNextReadIsOverdue(t *testing.T) {
	for range 5 {
		source := newWatchSource()
		source.delay = 120 * time.Millisecond
		w, err := startOSDWatch(t.Context(), watch.Settings{Interval: 100 * time.Millisecond, Timeout: time.Second, Buffer: 16}, source.read)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Stop(); err != nil {
			t.Fatalf("Stop with an overdue read = %v", err)
		}
		requireClosed(t, w)
	}
}

func TestOSDWatchContextEndDuringQuery(t *testing.T) {
	source := newWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w := startTestWatch(t, ctx, fastWatch(1), source)
	source.update(func(s *watchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	awaitDone(t, w)
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	requireClosed(t, w)
}

func TestOSDWatchStopDuringBlockedDelivery(t *testing.T) {
	source := newWatchSource()
	// One slot holds the unread baseline; the change then waits for space
	// for up to one interval.
	w := startTestWatch(t, t.Context(), watch.Settings{Interval: 400 * time.Millisecond, Timeout: time.Second, Buffer: 1}, source)
	source.setUp(0, false)
	source.awaitReads(t, 1)
	time.Sleep(50 * time.Millisecond)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop during delivery = %v", err)
	}
	if rest := requireClosed(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
}

func TestOSDWatchStalledConsumerOverflowsWithAnAttributableCause(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(2), source)
	source.setUp(1, false)
	source.awaitReads(t, 2)
	source.setUp(1, true)
	awaitDone(t, w)
	err := w.Stop()
	if !errors.Is(err, ErrWatchOverflow) || !strings.Contains(err.Error(), "event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if err2 := w.Stop(); err2 != err {
		t.Fatalf("Stop is not stable: %v then %v", err, err2)
	}
	rest := requireClosed(t, w)
	if len(rest) != 2 || rest[0].Kind != WatchBaseline || rest[1].Sequence != 2 || rest[1].States[1].Up {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestOSDWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(64), source)
	baseline := nextEvent(t, w)
	boom := errors.New("transient")
	source.update(func(s *watchSource) { s.fail = boom })
	gap := nextEvent(t, w)
	if gap.Kind != WatchGap || !errors.Is(gap.Err, boom) || gap.States != nil || gap.Epoch != 0 || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *watchSource) { s.fail = nil })
	var resumed OSDWatchEvent
	for resumed = nextEvent(t, w); resumed.Kind == WatchGap; resumed = nextEvent(t, w) {
		if !errors.Is(resumed.Err, boom) {
			t.Fatalf("gap = %+v", resumed)
		}
	}
	if resumed.Kind != WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.States, baseline.States) {
		t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
	}
	source.setUp(0, false)
	changed := nextEvent(t, w)
	if changed.Kind != WatchChanged || changed.Sequence != resumed.Sequence+1 || changed.States[0].Up {
		t.Fatalf("changed = %+v", changed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestOSDWatchIdentityDriftEndsTheWatch(t *testing.T) {
	cases := map[string]func(*watchSource){
		"native UUID replaced":       func(s *watchSource) { s.osds[1] = OSDState{ID: 1, UUID: osdUUID2, Up: true, In: true, Weight: 1} },
		"absent from the native map": func(s *watchSource) { delete(s.osds, 1) },
		"no longer owned":            func(s *watchSource) { delete(s.owned, 1) },
		"purged registration":        func(s *watchSource) { s.owned[1].purged = true },
		"re-registered under the same ID": func(s *watchSource) {
			s.owned[1] = &osdRegistration{nativeUUID: osdUUID2}
			s.osds[1] = OSDState{ID: 1, UUID: osdUUID2, Up: true, In: true, Weight: 1}
		},
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			source := newWatchSource()
			w := startTestWatch(t, t.Context(), fastWatch(4), source)
			source.update(drift)
			awaitDone(t, w)
			err := w.Stop()
			if err == nil || !strings.Contains(err.Error(), "watched osd.1") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireClosed(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
				t.Fatalf("a drifted read produced events: %+v", rest)
			}
		})
	}
}

func TestOSDWatchKeepsTheBaselineCohort(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(4), source)
	nextEvent(t, w)
	source.update(func(s *watchSource) {
		s.owned[5] = &osdRegistration{nativeUUID: osdUUID2}
		s.osds[5] = OSDState{ID: 5, UUID: osdUUID2, Up: true, In: true, Weight: 1}
	})
	source.awaitReads(t, 3)
	source.setUp(0, false)
	changed := nextEvent(t, w)
	if len(changed.States) != 2 || changed.States[0].ID != 0 || changed.States[1].ID != 1 {
		t.Fatalf("an OSD added after the baseline is watched: %+v", changed)
	}
}

func TestOSDWatchTerminationEndsTheWatch(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(4), source)
	source.update(func(s *watchSource) { s.fail = errClusterTerminated })
	awaitDone(t, w)
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Stop = %v", err)
	}
	if rest := requireClosed(t, w); len(rest) != 1 {
		t.Fatalf("a terminated cluster produced events: %+v", rest)
	}
}

func TestOSDWatchSnapshotsAreIndependent(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(8), source)
	baseline := nextEvent(t, w)
	// A consumer that edits a delivered snapshot changes neither the
	// comparison state nor a later event.
	baseline.States[0].Up = false
	baseline.States[1].UUID = osdUUID2
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.setUp(1, false)
	first := nextEvent(t, w)
	source.setUp(1, true)
	second := nextEvent(t, w)
	if !first.States[0].Up || first.States[1].UUID != osdUUID1 || first.States[1].Up {
		t.Fatalf("first change = %+v", first)
	}
	if &first.States[0] == &second.States[0] || !second.States[1].Up || first.States[1].Up {
		t.Fatalf("a later poll changed a delivered snapshot: %+v then %+v", first, second)
	}
}

func TestOSDWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	source := newWatchSource()
	w := startTestWatch(t, t.Context(), fastWatch(1), source)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireClosed(t, w)
	var zero *OSDWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ErrInvalidRequest) || !errors.Is((&OSDWatch{}).Stop(), ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

func TestWatchConfigValidation(t *testing.T) {
	got, err := watchSettings("watch OSDs", WatchConfig{}, osdWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 2 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	for name, cfg := range map[string]WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if _, err := watchSettings("watch OSDs", cfg, osdWatchDefaultInterval); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	cluster, ctr := newWatchCluster()
	observer, _ := Observe(cluster)
	if w, err := observer.WatchOSDs(t.Context(), WatchConfig{Buffer: -1}); w != nil || !errors.Is(err, ErrInvalidRequest) || len(ctr.commands()) != 0 {
		t.Fatalf("invalid config = %v, %v, %q", w, err, ctr.commands())
	}
	if w, err := (&Observer{}).WatchOSDs(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unconstructed observer = %v, %v", w, err)
	}
}

// newWatchCluster is an Observer cluster that owns osd.0 and osd.1, both up
// and in.
func newWatchCluster() (*Container, *observerControl) {
	cluster, ctr := newObserverCluster()
	cluster.fleet.osdHandles.register(0, &OSDContainer{ID: 0, nativeUUID: osdUUID0})
	cluster.fleet.osdHandles.register(1, &OSDContainer{ID: 1, nativeUUID: osdUUID1})
	ctr.replies[observerOSDDumpKey] = watchDump(1, 1)
	return cluster, ctr
}

func watchDump(epoch int, up1 int) string {
	return `{"epoch":` + itoa(epoch) + `,"flags":"","osds":[` + osdEntry("0", osdUUID0, 1, 1) + `,` + osdEntry("1", osdUUID1, up1, 1) + `]}`
}

func TestOSDWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	cluster, ctr := newWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchOSDs(t.Context(), WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireUnlocked := func(phase string) {
		t.Helper()
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if !cluster.mu.TryLock() {
				t.Fatalf("the watch kept the topology lock %s", phase)
			}
			cluster.mu.Unlock()
		}
	}
	requireUnlocked("between reads")
	// The unread baseline fills the buffer, so the change waits in delivery:
	// the consumer can still take the topology lock and query the fixture.
	ctr.set(observerOSDDumpKey, watchDump(2, 0))
	for deadline := time.Now().Add(5 * time.Second); len(ctr.commands()) < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if states, err := observer.OSDStates(t.Context()); err != nil || states[1].Up {
		t.Fatalf("consumer query during the watch = %+v, %v", states, err)
	}
	if baseline := nextEvent(t, w); baseline.Kind != WatchBaseline || baseline.Epoch != 1 || !baseline.States[1].Up {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextEvent(t, w); changed.Kind != WatchChanged || changed.Epoch != 2 || changed.States[1].Up {
		t.Fatalf("changed = %+v", changed)
	}
	// A consumer that holds the lock longer than QueryTimeout gets a gap
	// rather than a blocked watch, and the watch resumes after it.
	cluster.mu.Lock()
	gap := nextEvent(t, w)
	cluster.mu.Unlock()
	if gap.Kind != WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextEvent(t, w); event.Kind != WatchResumed; event = nextEvent(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("event after the lock was released = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	requireClosed(t, w)
}

func TestOSDWatchEndsWhenTheClusterTerminates(t *testing.T) {
	cluster, ctr := newWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchOSDs(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	cluster.mu.Lock()
	cluster.closed = true
	before := len(ctr.commands())
	cluster.mu.Unlock()
	awaitDone(t, w)
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("Stop = %v", err)
	}
	if got := ctr.commands(); len(got) != before {
		t.Fatalf("a terminated cluster ran commands: %q", got[before:])
	}
	if rest := requireClosed(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
}
