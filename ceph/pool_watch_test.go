package ceph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// watchHandle is the public shape of every watch of this package.
type watchHandle[E any] interface {
	Events() <-chan E
	Stop() error
}

func nextOf[E any](t *testing.T, w watchHandle[E]) E {
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
	var zero E
	return zero
}

// closedOf drains the buffered events and fails unless the channel is closed
// after them.
func closedOf[E any](t *testing.T, w watchHandle[E]) []E {
	t.Helper()
	var rest []E
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

func awaitEnded(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

// requireNoFixtureLock fails if the topology lock of cluster is held at any
// moment of the next 100 milliseconds.
func requireNoFixtureLock(t *testing.T, cluster *Container, phase string) {
	t.Helper()
	for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if !cluster.mu.TryLock() {
			t.Fatalf("the watch kept the topology lock %s", phase)
		}
		cluster.mu.Unlock()
	}
}

func awaitCommands(t *testing.T, ctr *observerControl, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); len(ctr.commands()) < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d commands ran, want %d", len(ctr.commands()), n)
		}
	}
}

// pgWatchSource is a scripted poolPGRead: the reported snapshot and the
// failure of the next reads, changed by the test while the watch reads them.
type pgWatchSource struct {
	mu       sync.Mutex
	snapshot PoolPGSnapshot
	fail     error
	block    bool
	reads    int
}

func watchPGRow(id PGID) PGState {
	return PGState{PGID: id, State: "active+clean", Up: []int{0, 1}, Acting: []int{0, 1}, ReportedEpoch: 20, ReportedSequence: 1, MappingEpoch: 18}
}

func newPGWatchSource() *pgWatchSource {
	pool := PoolState{ID: 7, Name: "fixture", Type: "replicated", Size: 2, MinSize: 1, PGNum: 3, AutoscaleMode: "off"}
	// Reported out of PGID order: the watch sorts by numeric seed.
	return &pgWatchSource{snapshot: PoolPGSnapshot{FSID: poolPGFixtureFSID, PoolBefore: pool, PoolAfter: pool,
		OSDMapEpochBefore: 20, OSDMapEpochAfter: 20, PGReady: true, PGs: []PGState{watchPGRow("7.10"), watchPGRow("7.2"), watchPGRow("7.a")}}}
}

func (s *pgWatchSource) read(ctx context.Context) (PoolPGSnapshot, error) {
	s.mu.Lock()
	s.reads++
	snapshot, fail, block := clonePoolPGSnapshot(s.snapshot), s.fail, s.block
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return PoolPGSnapshot{}, ctx.Err()
	}
	if fail != nil {
		return PoolPGSnapshot{}, fail
	}
	return snapshot, nil
}

// update changes the reported snapshot; every update also advances the
// OSDMap epochs, which alone is no event.
func (s *pgWatchSource) update(change func(*PoolPGSnapshot)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if change != nil {
		change(&s.snapshot)
	}
	s.snapshot.OSDMapEpochBefore++
	s.snapshot.OSDMapEpochAfter++
}

func (s *pgWatchSource) setState(id PGID, state string) {
	s.update(func(p *PoolPGSnapshot) { watchPGOf(p, id).State = state })
}

func (s *pgWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *pgWatchSource) awaitReads(t *testing.T, n int) {
	t.Helper()
	want := s.readCount() + n
	for deadline := time.Now().Add(5 * time.Second); s.readCount() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

func watchPGOf(p *PoolPGSnapshot, id PGID) *PGState {
	for i := range p.PGs {
		if p.PGs[i].PGID == id {
			return &p.PGs[i]
		}
	}
	return nil
}

func startTestPGWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *pgWatchSource) *PoolPGWatch {
	t.Helper()
	w, err := startPoolPGWatch(ctx, settings, "fixture", source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func pgIDs(s PoolPGSnapshot) []PGID {
	ids := make([]PGID, len(s.PGs))
	for i, pg := range s.PGs {
		ids[i] = pg.PGID
	}
	return ids
}

func TestPoolPGWatchBaselineIsBufferedAndSortedBeforeWatchPGsReturns(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != WatchBaseline || event.Err != nil || event.Snapshot.FSID != poolPGFixtureFSID ||
			event.Snapshot.PoolBefore.ID != 7 || event.Snapshot.OSDMapEpochBefore != 20 || !event.Snapshot.PGReady {
			t.Fatalf("baseline = %+v", event)
		}
		if got := pgIDs(event.Snapshot); !reflect.DeepEqual(got, []PGID{"7.2", "7.a", "7.10"}) {
			t.Fatalf("baseline PGs are not in numeric seed order: %v", got)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchPGs returned before the baseline was buffered")
	}
}

func TestPoolPGWatchIgnoresEpochsCountersAndRowOrder(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(4), source)
	nextOf(t, w)
	source.awaitReads(t, 3)
	// Freshness metadata and statistics change on every report; reported
	// row order is irrelevant. None of these is a semantic change.
	source.update(func(p *PoolPGSnapshot) {
		for i := range p.PGs {
			p.PGs[i].ReportedEpoch += 5
			p.PGs[i].ReportedSequence += 9
			p.PGs[i].MappingEpoch++
			p.PGs[i].LastEpochClean++
			p.PGs[i].StatsInvalid = !p.PGs[i].StatsInvalid
			p.PGs[i].Stats.Objects += 3
			p.PGs[i].Stats.ObjectsDegraded++
		}
		p.PGs[0], p.PGs[2] = p.PGs[2], p.PGs[0]
	})
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged PG placement", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := closedOf(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged state: %+v", rest)
	}
}

func TestPoolPGWatchReportsStatePlacementAndMembershipChanges(t *testing.T) {
	cases := map[string]func(*PoolPGSnapshot){
		"state":          func(p *PoolPGSnapshot) { watchPGOf(p, "7.a").State = "active+undersized+degraded" },
		"up order":       func(p *PoolPGSnapshot) { watchPGOf(p, "7.a").Up = []int{1, 0} },
		"acting":         func(p *PoolPGSnapshot) { watchPGOf(p, "7.a").Acting = []int{0, 2147483647} },
		"up primary":     func(p *PoolPGSnapshot) { watchPGOf(p, "7.a").UpPrimary = 1 },
		"acting primary": func(p *PoolPGSnapshot) { watchPGOf(p, "7.a").ActingPrimary = -1 },
		"pg ready":       func(p *PoolPGSnapshot) { p.PGReady = false },
		"pool policy":    func(p *PoolPGSnapshot) { p.PoolAfter.PGNumTarget = 4 },
		"split":          func(p *PoolPGSnapshot) { p.PGs = append(p.PGs, watchPGRow("7.12")) },
		"merge":          func(p *PoolPGSnapshot) { p.PGs = p.PGs[:2] },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			source := newPGWatchSource()
			w := startTestPGWatch(t, t.Context(), fastWatch(4), source)
			baseline := nextOf(t, w)
			source.update(change)
			changed := nextOf(t, w)
			if changed.Kind != WatchChanged || changed.Sequence != 2 || changed.Err != nil || reflect.DeepEqual(changed.Snapshot, baseline.Snapshot) {
				t.Fatalf("changed = %+v", changed)
			}
			if changed.Snapshot.OSDMapEpochAfter <= baseline.Snapshot.OSDMapEpochAfter {
				t.Fatalf("the event lost the native epochs of its read: %+v", changed.Snapshot)
			}
		})
	}
}

func TestPoolPGWatchReportsChangesInOrderWithoutOverwriting(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(8), source)
	states := []string{"active+undersized+degraded", "active+recovering+degraded", "active+clean"}
	for _, state := range states {
		source.setState("7.a", state)
		source.awaitReads(t, 3)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := closedOf(t, w)
	if len(events) != 4 {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events[1:] {
		if event.Sequence != uint64(i+2) || event.Kind != WatchChanged || watchPGOf(&event.Snapshot, "7.a").State != states[i] {
			t.Fatalf("event %d = %+v", i+2, event)
		}
	}
}

func TestPoolPGWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(64), source)
	baseline := nextOf(t, w)
	boom := errors.New("transient")
	source.update(func(*PoolPGSnapshot) {})
	source.mu.Lock()
	source.fail = boom
	source.mu.Unlock()
	gap := nextOf(t, w)
	if gap.Kind != WatchGap || !errors.Is(gap.Err, boom) || !reflect.DeepEqual(gap.Snapshot, PoolPGSnapshot{}) || gap.Sequence != 2 {
		t.Fatalf("gap = %+v (a failed read is never an empty PG report)", gap)
	}
	source.mu.Lock()
	source.fail = nil
	source.mu.Unlock()
	var resumed PoolPGWatchEvent
	for resumed = nextOf(t, w); resumed.Kind == WatchGap; resumed = nextOf(t, w) {
	}
	if resumed.Kind != WatchResumed || resumed.Err != nil || !equalPoolPGSamples(resumed.Snapshot, baseline.Snapshot) {
		t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
	}
	source.setState("7.2", "active+clean+scrubbing")
	if changed := nextOf(t, w); changed.Kind != WatchChanged || changed.Sequence != resumed.Sequence+1 {
		t.Fatalf("changed = %+v", changed)
	}
}

func TestPoolPGWatchStalledConsumerOverflows(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(2), source)
	source.setState("7.a", "peering")
	source.awaitReads(t, 2)
	source.setState("7.a", "active+clean")
	awaitEnded(t, w.stream.Done())
	err := w.Stop()
	if !errors.Is(err, ErrWatchOverflow) || !strings.Contains(err.Error(), `watch PGs of pool "fixture": event 3 (changed)`) {
		t.Fatalf("Stop = %v", err)
	}
	if rest := closedOf(t, w); len(rest) != 2 || watchPGOf(&rest[1].Snapshot, "7.a").State != "peering" {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestPoolPGWatchStopAndContextEndJoinTheQuery(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(1), source)
	source.mu.Lock()
	source.block = true
	source.mu.Unlock()
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
	if rest := closedOf(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}

	source = newPGWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestPGWatch(t, ctx, fastWatch(1), source)
	source.mu.Lock()
	source.block = true
	source.mu.Unlock()
	source.awaitReads(t, 1)
	cancel()
	awaitEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	closedOf(t, w)
}

func TestPoolPGWatchSnapshotsAreIndependent(t *testing.T) {
	source := newPGWatchSource()
	w := startTestPGWatch(t, t.Context(), fastWatch(8), source)
	baseline := nextOf(t, w)
	// Editing a delivered snapshot, nested placement vectors included,
	// changes neither the comparison state nor a later event.
	baseline.Snapshot.PGs[0].State = "edited"
	baseline.Snapshot.PGs[1].Up[0] = 99
	baseline.Snapshot.PGs = baseline.Snapshot.PGs[:1]
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.setState("7.10", "active+clean+scrubbing")
	first := nextOf(t, w)
	first.Snapshot.PGs[1].Acting[1] = 42
	source.setState("7.10", "active+clean")
	second := nextOf(t, w)
	if len(first.Snapshot.PGs) != 3 || first.Snapshot.PGs[0].State != "active+clean" || first.Snapshot.PGs[1].Up[0] != 0 {
		t.Fatalf("first change = %+v", first.Snapshot)
	}
	if second.Snapshot.PGs[1].Acting[1] != 1 || watchPGOf(&first.Snapshot, "7.10").State != "active+clean+scrubbing" {
		t.Fatalf("a later poll or an edit changed a delivered snapshot: %+v then %+v", first.Snapshot, second.Snapshot)
	}
}

func TestPoolPGWatchEndsWhenAReadIdentifiesAnotherPool(t *testing.T) {
	for name, drift := range map[string]func(*PoolPGSnapshot){
		"pool ID":         func(p *PoolPGSnapshot) { p.PoolBefore.ID, p.PoolAfter.ID = 9, 9 },
		"after bracket":   func(p *PoolPGSnapshot) { p.PoolAfter.ID = 9 },
		"cluster FSID":    func(p *PoolPGSnapshot) { p.FSID = "61098f7f-474e-4403-ad31-a976f7c4e731" },
		"marked end read": nil,
	} {
		t.Run(name, func(t *testing.T) {
			source := newPGWatchSource()
			w := startTestPGWatch(t, t.Context(), fastWatch(4), source)
			if drift != nil {
				source.update(drift)
			} else {
				source.mu.Lock()
				source.fail = endObservation(errors.New("one-shot text"), invalid(errPoolPGAbsent))
				source.mu.Unlock()
			}
			awaitEnded(t, w.stream.Done())
			err := w.Stop()
			if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), `watch PGs of pool "fixture"`) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := closedOf(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
				t.Fatalf("a drifted read produced events: %+v", rest)
			}
		})
	}
}

func TestPoolPGWatchFailedBaselineStartsNothing(t *testing.T) {
	source := newPGWatchSource()
	boom := errors.New("boom")
	source.fail = boom
	if w, err := startPoolPGWatch(t.Context(), fastWatch(1), "fixture", source.read); w != nil || !errors.Is(err, boom) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	source.fail = endObservation(errors.New("one-shot text"), errClusterTerminated)
	if w, err := startPoolPGWatch(t.Context(), fastWatch(1), "fixture", source.read); w != nil || !errors.Is(err, errClusterTerminated) {
		t.Fatalf("watch of a terminated cluster = %v, %v", w, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source = newPGWatchSource()
	if w, err := startPoolPGWatch(ctx, fastWatch(1), "fixture", source.read); w != nil || !errors.Is(err, context.Canceled) || source.readCount() != 0 {
		t.Fatalf("watch under a canceled context = %v, %v", w, err)
	}
}

func TestPoolPGWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	w := startTestPGWatch(t, t.Context(), fastWatch(1), newPGWatchSource())
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	closedOf(t, w)
	var zero *PoolPGWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ErrInvalidRequest) || !errors.Is((&PoolPGWatch{}).Stop(), ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

// The scripted CLI of a cluster that owns pool "fixture" (native ID 7) with
// two PGs.
const (
	pgWatchFSIDKey   = "fsid"
	pgWatchMapKey    = "osd dump --format json"
	pgWatchReportKey = "pg ls-by-pool fixture --format json"
	pgWatchRowClean  = `{"pgid":"7.1","state":"active+clean","up":[0,1],"acting":[0,1],"up_primary":0,"acting_primary":0,"reported_epoch":20,"reported_seq":5,"mapping_epoch":18,"last_epoch_clean":19,"stats_invalid":false,"stat_sum":{"num_objects":1,"num_bytes":4096,"num_object_copies":2,"num_objects_degraded":0,"num_objects_misplaced":0,"num_objects_unfound":0}}`
	pgWatchRowOther  = `{"pgid":"7.0","state":"active+clean","up":[1,0],"acting":[1,0],"up_primary":1,"acting_primary":1,"reported_epoch":20,"reported_seq":5,"mapping_epoch":18,"last_epoch_clean":19,"stats_invalid":false,"stat_sum":{"num_objects":0,"num_bytes":0,"num_object_copies":0,"num_objects_degraded":0,"num_objects_misplaced":0,"num_objects_unfound":0}}`
)

func pgWatchReports(row string) string {
	return `{"pg_ready":true,"pg_stats":[` + row + `,` + pgWatchRowOther + `]}`
}

func newPGWatchCluster(t *testing.T) (*Container, *observerControl, *Pool) {
	cluster, ctr := newObserverCluster()
	cluster.bootstrap = bootstrapRecord{config: []byte("[global]\nfsid = " + poolPGFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	ctr.set(pgWatchFSIDKey, poolPGFixtureFSID+"\n")
	ctr.set(pgWatchMapKey, poolPGFixtureMap(poolPGFixturePool, 20))
	ctr.set(pgWatchReportKey, pgWatchReports(pgWatchRowClean))
	return cluster, ctr, poolHandle(t, t.Context(), cluster, "fixture")
}

func TestPoolPGWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	cluster, ctr, pool := newPGWatchCluster(t)
	w, err := pool.WatchPGs(t.Context(), WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireNoFixtureLock(t, cluster, "between reads")
	// The unread baseline fills the buffer, so the change waits in delivery:
	// the consumer can still take the topology lock and query the fixture.
	degraded := strings.Replace(pgWatchRowClean, `"active+clean"`, `"active+undersized+degraded"`, 1)
	ctr.set(pgWatchReportKey, pgWatchReports(degraded))
	awaitCommands(t, ctr, 10)
	time.Sleep(20 * time.Millisecond)
	requireNoFixtureLock(t, cluster, "during delivery")
	if snapshot, err := pool.PGs(t.Context()); err != nil || watchPGOf(&snapshot, "7.1").State != "active+undersized+degraded" {
		t.Fatalf("consumer query during the watch = %+v, %v", snapshot, err)
	}
	baseline := nextOf(t, w)
	if baseline.Kind != WatchBaseline || !reflect.DeepEqual(pgIDs(baseline.Snapshot), []PGID{"7.0", "7.1"}) || baseline.Snapshot.PGs[1].Stats.Bytes != 4096 {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextOf(t, w); changed.Kind != WatchChanged || changed.Snapshot.PGs[1].State != "active+undersized+degraded" {
		t.Fatalf("changed = %+v", changed)
	}
	// A consumer that holds the lock longer than QueryTimeout gets a gap
	// rather than a blocked watch, and the watch resumes after it.
	cluster.mu.Lock()
	gap := nextOf(t, w)
	cluster.mu.Unlock()
	if gap.Kind != WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextOf(t, w); event.Kind != WatchResumed; event = nextOf(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("event after the lock was released = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	closedOf(t, w)
}

func TestPoolPGWatchEndsOnNativeIdentityReplacement(t *testing.T) {
	cases := map[string]struct {
		key, reply string
	}{
		"recreated under the same name": {pgWatchMapKey, poolPGFixtureMap(strings.Replace(poolPGFixturePool, `"pool":7`, `"pool":9`, 1), 21)},
		"removed":                       {pgWatchMapKey, poolPGFixtureMap(`{"pool":9,"pool_name":"elsewhere","type":1}`, 21)},
		"renamed":                       {pgWatchMapKey, poolPGFixtureMap(strings.Replace(poolPGFixturePool, `"pool_name":"fixture"`, `"pool_name":"renamed"`, 1), 21)},
		"OSDMap of another cluster":     {pgWatchMapKey, strings.Replace(poolPGFixtureMap(poolPGFixturePool, 21), poolPGFixtureFSID, "61098f7f-474e-4403-ad31-a976f7c4e731", 1)},
		"FSID of another cluster":       {pgWatchFSIDKey, "61098f7f-474e-4403-ad31-a976f7c4e731\n"},
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			_, ctr, pool := newPGWatchCluster(t)
			w, err := pool.WatchPGs(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
			if err != nil {
				t.Fatal(err)
			}
			ctr.set(drift.key, drift.reply)
			awaitEnded(t, w.stream.Done())
			err = w.Stop()
			if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), `watch PGs of pool "fixture"`) || strings.Contains(err.Error(), "61098f7f") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := closedOf(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
				t.Fatalf("a replaced pool produced events: %+v", rest)
			}
			// The one-shot query keeps its unmarked error.
			if _, err := pool.PGs(t.Context()); err == nil {
				t.Fatal("PGs accepted the replaced identity")
			} else if _, marked := err.(*observationEnd); marked {
				t.Fatalf("PGs returned the watch mark: %v", err)
			}
		})
	}
	// A malformed FSID or report is a gap that a later read can repair.
	_, ctr, pool := newPGWatchCluster(t)
	w, err := pool.WatchPGs(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	nextOf(t, w)
	ctr.set(pgWatchFSIDKey, "SECRET_GARBAGE\n")
	if gap := nextOf(t, w); gap.Kind != WatchGap || strings.Contains(gap.Err.Error(), "SECRET_GARBAGE") {
		t.Fatalf("malformed FSID = %+v", gap)
	}
	ctr.set(pgWatchFSIDKey, poolPGFixtureFSID+"\n")
	for event := nextOf(t, w); event.Kind != WatchResumed; event = nextOf(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("event = %+v", event)
		}
	}
}

func TestPoolPGWatchEndsWhenTheClusterTerminates(t *testing.T) {
	cluster, ctr, pool := newPGWatchCluster(t)
	w, err := pool.WatchPGs(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	cluster.mu.Lock()
	cluster.closed = true
	before := len(ctr.commands())
	cluster.mu.Unlock()
	awaitEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Stop = %v", err)
	}
	if got := ctr.commands(); len(got) != before {
		t.Fatalf("a terminated cluster ran commands: %q", got[before:])
	}
	if rest := closedOf(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
}

func TestPoolPGWatchConfigValidation(t *testing.T) {
	got, err := watchSettings("watch pool PGs", WatchConfig{}, poolPGWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	_, ctr, pool := newPGWatchCluster(t)
	for name, cfg := range map[string]WatchConfig{
		"negative interval": {Interval: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := pool.WatchPGs(t.Context(), cfg); w != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("an invalid config ran commands: %q", got)
	}
	if w, err := (&Pool{PoolConfig: PoolConfig{Name: "fixture"}}).WatchPGs(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a pool without a cluster = %v, %v", w, err)
	}
	var nilPool *Pool
	if w, err := nilPool.WatchPGs(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a nil pool = %v, %v", w, err)
	}
}
