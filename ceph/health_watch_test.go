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

// healthWatchSource is a scripted healthRead: the reported snapshot and the
// failure of the next reads, changed by the test while the watch reads them.
type healthWatchSource struct {
	mu       sync.Mutex
	snapshot HealthSnapshot
	fail     error
	block    bool
	reads    int
}

func newHealthWatchSource() *healthWatchSource {
	return &healthWatchSource{snapshot: HealthSnapshot{FSID: healthDetailsFixtureFSID, Status: "HEALTH_WARN",
		Checks: map[string]HealthCheck{
			"OSD_DOWN":      {Severity: "HEALTH_WARN", Summary: "1 osds down", Count: 1, Details: []string{"osd.1 is down"}},
			"FUTURE_Code_x": {Severity: "HEALTH_ERR", Summary: "future", Count: 2, Details: []string{"a", "b"}, Muted: true},
		},
		// Reported out of code order: the watch sorts the mutes.
		Mutes: []HealthMute{{Code: "FUTURE_Code_x", Count: 2, Sticky: true}, {Code: "AUTH_INSECURE", ExpiresAt: "2026-11-04T10:00:00.000000+0000"}}}}
}

func (s *healthWatchSource) read(ctx context.Context) (HealthSnapshot, error) {
	s.mu.Lock()
	s.reads++
	snapshot, fail, block := cloneHealthSnapshot(s.snapshot), s.fail, s.block
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return HealthSnapshot{}, ctx.Err()
	}
	if fail != nil {
		return HealthSnapshot{}, fail
	}
	return snapshot, nil
}

func (s *healthWatchSource) update(change func(*HealthSnapshot)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(&s.snapshot)
}

func (s *healthWatchSource) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

func (s *healthWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *healthWatchSource) awaitReads(t *testing.T, n int) {
	t.Helper()
	want := s.readCount() + n
	for deadline := time.Now().Add(5 * time.Second); s.readCount() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

func startTestHealthWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *healthWatchSource) *HealthWatch {
	t.Helper()
	w, err := startHealthWatch(ctx, settings, source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func TestHealthWatchBaselineIsBufferedBeforeWatchHealthReturns(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != WatchBaseline || event.Err != nil || event.Health.FSID != healthDetailsFixtureFSID ||
			event.Health.Status != "HEALTH_WARN" || len(event.Health.Checks) != 2 || !event.Health.Checks["FUTURE_Code_x"].Muted {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Health.Mutes[0].Code != "AUTH_INSECURE" || event.Health.Mutes[1].Code != "FUTURE_Code_x" {
			t.Fatalf("mutes are not sorted by code: %+v", event.Health.Mutes)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchHealth returned before the baseline was buffered")
	}
}

func TestHealthWatchIgnoresMessagesAndOrder(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(4), source)
	nextOf(t, w)
	source.awaitReads(t, 3)
	// Countdown text, rewritten summaries, reordered details and reordered
	// mutes are not semantic changes.
	source.update(func(h *HealthSnapshot) {
		check := h.Checks["OSD_DOWN"]
		check.Summary, check.Details = "1 osds down (for 25s)", []string{"host b", "osd.1 is down"}
		h.Checks["OSD_DOWN"] = check
		check = h.Checks["FUTURE_Code_x"]
		check.Details = []string{"b", "a"}
		h.Checks["FUTURE_Code_x"] = check
		h.Mutes[0], h.Mutes[1] = h.Mutes[1], h.Mutes[0]
		h.Mutes[0].Summary = "rewritten"
	})
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for unchanged health", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := closedOf(t, w); len(rest) != 0 {
		t.Fatalf("events after unchanged health: %+v", rest)
	}
}

func TestHealthWatchReportsCheckAndMuteChanges(t *testing.T) {
	cases := map[string]func(*HealthSnapshot){
		"check appears": func(h *HealthSnapshot) {
			h.Checks["POOL_APP_NOT_ENABLED"] = HealthCheck{Severity: "HEALTH_WARN", Count: 1}
		},
		"check clears": func(h *HealthSnapshot) { delete(h.Checks, "OSD_DOWN"); h.Status = "HEALTH_OK" },
		"severity": func(h *HealthSnapshot) {
			c := h.Checks["OSD_DOWN"]
			c.Severity = "HEALTH_ERR"
			h.Checks["OSD_DOWN"] = c
		},
		"count":        func(h *HealthSnapshot) { c := h.Checks["OSD_DOWN"]; c.Count = 2; h.Checks["OSD_DOWN"] = c },
		"muted":        func(h *HealthSnapshot) { c := h.Checks["OSD_DOWN"]; c.Muted = true; h.Checks["OSD_DOWN"] = c },
		"mute added":   func(h *HealthSnapshot) { h.Mutes = append(h.Mutes, HealthMute{Code: "OSD_DOWN"}) },
		"mute removed": func(h *HealthSnapshot) { h.Mutes = h.Mutes[:1] },
		"mute sticky":  func(h *HealthSnapshot) { h.Mutes[1].Sticky = true },
		"mute expiry":  func(h *HealthSnapshot) { h.Mutes[1].ExpiresAt = "2026-11-04T11:00:00.000000+0000" },
		"mute count":   func(h *HealthSnapshot) { h.Mutes[0].Count = 3 },
		"status":       func(h *HealthSnapshot) { h.Status = "HEALTH_ERR" },
		"code case": func(h *HealthSnapshot) {
			h.Checks["future_code_x"] = h.Checks["FUTURE_Code_x"]
			delete(h.Checks, "FUTURE_Code_x")
		},
		"replaced code": func(h *HealthSnapshot) {
			h.Checks["OSD_HOST_DOWN"] = h.Checks["OSD_DOWN"]
			delete(h.Checks, "OSD_DOWN")
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			source := newHealthWatchSource()
			w := startTestHealthWatch(t, t.Context(), fastWatch(4), source)
			baseline := nextOf(t, w)
			source.update(change)
			changed := nextOf(t, w)
			if changed.Kind != WatchChanged || changed.Sequence != 2 || changed.Err != nil || reflect.DeepEqual(changed.Health, baseline.Health) {
				t.Fatalf("changed = %+v", changed)
			}
		})
	}
}

func TestHealthWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(64), source)
	baseline := nextOf(t, w)
	boom := errors.New("transient")
	source.setFail(boom)
	gap := nextOf(t, w)
	if gap.Kind != WatchGap || !errors.Is(gap.Err, boom) || !reflect.DeepEqual(gap.Health, HealthSnapshot{}) || gap.Sequence != 2 {
		t.Fatalf("gap = %+v (a failed read is never a healthy or empty report)", gap)
	}
	source.setFail(nil)
	var resumed HealthWatchEvent
	for resumed = nextOf(t, w); resumed.Kind == WatchGap; resumed = nextOf(t, w) {
	}
	if resumed.Kind != WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Health, baseline.Health) {
		t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
	}
	source.update(func(h *HealthSnapshot) { delete(h.Checks, "OSD_DOWN") })
	if changed := nextOf(t, w); changed.Kind != WatchChanged || changed.Sequence != resumed.Sequence+1 {
		t.Fatalf("changed = %+v", changed)
	}
}

func TestHealthWatchStalledConsumerOverflows(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(2), source)
	source.update(func(h *HealthSnapshot) { delete(h.Checks, "OSD_DOWN") })
	source.awaitReads(t, 2)
	source.update(func(h *HealthSnapshot) { h.Checks["OSD_DOWN"] = HealthCheck{Severity: "HEALTH_WARN", Count: 1} })
	awaitEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, ErrWatchOverflow) || !strings.Contains(err.Error(), "watch health: event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := closedOf(t, w); len(rest) != 2 {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestHealthWatchStopAndContextEndJoinTheQuery(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(1), source)
	source.update(func(*HealthSnapshot) {})
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

	source = newHealthWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestHealthWatch(t, ctx, fastWatch(1), source)
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

func TestHealthWatchSnapshotsAreIndependent(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(8), source)
	baseline := nextOf(t, w)
	// Editing a delivered snapshot, its check map, detail slices and mutes
	// included, changes neither the comparison state nor a later event.
	delete(baseline.Health.Checks, "OSD_DOWN")
	baseline.Health.Checks["FUTURE_Code_x"].Details[0] = "edited"
	baseline.Health.Mutes[0].Count = 77
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.update(func(h *HealthSnapshot) { h.Mutes = h.Mutes[:1] })
	first := nextOf(t, w)
	first.Health.Checks["FUTURE_Code_x"].Details[1] = "edited"
	source.update(func(h *HealthSnapshot) { delete(h.Checks, "OSD_DOWN") })
	second := nextOf(t, w)
	if _, ok := first.Health.Checks["OSD_DOWN"]; !ok || first.Health.Checks["FUTURE_Code_x"].Details[0] != "a" || len(first.Health.Mutes) != 1 {
		t.Fatalf("first change = %+v", first.Health)
	}
	if second.Health.Checks["FUTURE_Code_x"].Details[1] != "b" || second.Health.Mutes[0].Count != 2 {
		t.Fatalf("a later poll or an edit changed a delivered snapshot: %+v then %+v", first.Health, second.Health)
	}
}

func TestHealthWatchFailedBaselineStartsNothing(t *testing.T) {
	source := newHealthWatchSource()
	boom := errors.New("boom")
	source.fail = boom
	if w, err := startHealthWatch(t.Context(), fastWatch(1), source.read); w != nil || !errors.Is(err, boom) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	source.fail = endObservation(errClusterTerminated, errClusterTerminated)
	if w, err := startHealthWatch(t.Context(), fastWatch(1), source.read); w != nil || !errors.Is(err, errClusterTerminated) {
		t.Fatalf("watch of a terminated cluster = %v, %v", w, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source = newHealthWatchSource()
	if w, err := startHealthWatch(ctx, fastWatch(1), source.read); w != nil || !errors.Is(err, context.Canceled) || source.readCount() != 0 {
		t.Fatalf("watch under a canceled context = %v, %v", w, err)
	}
}

func TestHealthWatchEndsWhenAReadIdentifiesAnotherCluster(t *testing.T) {
	source := newHealthWatchSource()
	w := startTestHealthWatch(t, t.Context(), fastWatch(4), source)
	source.update(func(h *HealthSnapshot) { h.FSID = "61098f7f-474e-4403-ad31-a976f7c4e731" })
	awaitEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "watch health") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := closedOf(t, w); len(rest) != 1 {
		t.Fatalf("events = %+v", rest)
	}
}

func TestHealthWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	w := startTestHealthWatch(t, t.Context(), fastWatch(1), newHealthWatchSource())
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
	var zero *HealthWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ErrInvalidRequest) || !errors.Is((&HealthWatch{}).Stop(), ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

const healthWatchKey = "health detail --format json"

func newHealthWatchCluster() (*Container, *observerControl) {
	cluster, ctr := newObserverCluster()
	cluster.bootstrap = bootstrapRecord{config: []byte("[global]\nfsid = " + healthDetailsFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	ctr.set("fsid", healthDetailsFixtureFSID+"\n")
	ctr.set(healthWatchKey, healthDetailsFixtureHealthy)
	return cluster, ctr
}

func TestHealthWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	cluster, ctr := newHealthWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchHealth(t.Context(), WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireNoFixtureLock(t, cluster, "between reads")
	// The unread baseline fills the buffer, so the change waits in delivery:
	// the consumer can still take the topology lock and query the fixture.
	ctr.set(healthWatchKey, healthDetailsFixtureWarning)
	awaitCommands(t, ctr, 6)
	time.Sleep(20 * time.Millisecond)
	requireNoFixtureLock(t, cluster, "during delivery")
	if health, err := observer.HealthDetails(t.Context()); err != nil || health.Status != "HEALTH_WARN" {
		t.Fatalf("consumer query during the watch = %+v, %v", health, err)
	}
	if baseline := nextOf(t, w); baseline.Kind != WatchBaseline || baseline.Health.Status != "HEALTH_OK" || len(baseline.Health.Checks) != 0 {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextOf(t, w); changed.Kind != WatchChanged || changed.Health.Checks["FUTURE_CODE"].Count != 9007199254740993 {
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

func TestHealthWatchEndsOnAForeignNativeFSID(t *testing.T) {
	cluster, ctr := newHealthWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchHealth(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	nextOf(t, w)
	// A malformed FSID is a gap; a valid FSID of another cluster ends the
	// watch, and neither leaks the native output.
	ctr.set("fsid", "SECRET_GARBAGE\n")
	if gap := nextOf(t, w); gap.Kind != WatchGap || strings.Contains(gap.Err.Error(), "SECRET_GARBAGE") {
		t.Fatalf("malformed FSID = %+v", gap)
	}
	ctr.set("fsid", "61098f7f-474e-4403-ad31-a976f7c4e731\n")
	awaitEnded(t, w.stream.Done())
	err = w.Stop()
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "watch health") || strings.Contains(err.Error(), "61098f7f") {
		t.Fatalf("Stop = %v", err)
	}
	for _, event := range closedOf(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("a foreign cluster produced an observation: %+v", event)
		}
	}
	if _, err := observer.HealthDetails(t.Context()); err == nil {
		t.Fatal("HealthDetails accepted a foreign FSID")
	} else if _, marked := err.(*observationEnd); marked {
		t.Fatalf("HealthDetails returned the watch mark: %v", err)
	}
}

func TestHealthWatchEndsWhenTheClusterTerminates(t *testing.T) {
	cluster, ctr := newHealthWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchHealth(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
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
	if _, err := observer.HealthDetails(t.Context()); err != errClusterTerminated {
		t.Fatalf("HealthDetails of a terminated cluster = %v", err)
	}
}

func TestHealthWatchConfigValidation(t *testing.T) {
	got, err := watchSettings("watch health", WatchConfig{}, healthWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	cluster, ctr := newHealthWatchCluster()
	observer, _ := Observe(cluster)
	for name, cfg := range map[string]WatchConfig{
		"negative timeout": {QueryTimeout: -1},
		"short interval":   {Interval: 99 * time.Millisecond},
		"negative buffer":  {Buffer: -1},
	} {
		if w, err := observer.WatchHealth(t.Context(), cfg); w != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("an invalid config ran commands: %q", got)
	}
	if w, err := (&Observer{}).WatchHealth(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unconstructed observer = %v, %v", w, err)
	}
}
