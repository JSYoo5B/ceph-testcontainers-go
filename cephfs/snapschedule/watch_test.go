package snapschedule

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

func nextLeaseEvent(t *testing.T, w *LeaseWatch) LeaseWatchEvent {
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
	return LeaseWatchEvent{}
}

// requireLeaseWatchClosed drains the buffered events and fails unless the
// channel is closed after them.
func requireLeaseWatchClosed(t *testing.T, w *LeaseWatch) []LeaseWatchEvent {
	t.Helper()
	var rest []LeaseWatchEvent
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

func awaitLeaseWatchDone(t *testing.T, w *LeaseWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

// awaitCommands waits until the fake has answered n more commands.
func (f *fakeSnapSchedule) awaitCommands(t *testing.T, n int) {
	t.Helper()
	count := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.commands
	}
	want := count() + n
	deadline := time.Now().Add(5 * time.Second)
	for count() < want {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
		time.Sleep(time.Millisecond)
	}
}

const fastLeaseInterval = 100 * time.Millisecond

func watchedLease(t *testing.T) (*fakeSnapSchedule, *Lease, *LeaseWatch) {
	t.Helper()
	fake := newFakeSnapSchedule()
	lease, err := add(t.Context(), snapScheduleFilesystem(fake), leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	w, err := lease.Watch(t.Context(), ceph.WatchConfig{Interval: fastLeaseInterval, Buffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return fake, lease, w
}

// The module's counters and times are aggregate progress: a change of them is
// one event, however many runs it covers, and an equal read is none.
func TestLeaseWatchReportsAggregateProgressOnlyWhenItChanges(t *testing.T) {
	fake, _, w := watchedLease(t)
	baseline := nextLeaseEvent(t, w)
	if baseline.Kind != ceph.WatchBaseline || baseline.Sequence != 1 || baseline.Status == nil || !baseline.Status.Active ||
		baseline.Status.Created != 0 || !baseline.Status.First.IsZero() || !maps.Equal(baseline.Status.Retention, leaseSchedule.Retention) {
		t.Fatalf("baseline = %+v", baseline)
	}
	fake.awaitCommands(t, 6)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("identical reads produced %d events", n)
	}
	fake.report("/", "2h", map[string]any{"created_count": 1, "first": "2030-03-01T00:00:00", "last": "2030-03-01T00:00:00"})
	first := nextLeaseEvent(t, w)
	if first.Kind != ceph.WatchChanged || first.Status.Created != 1 || first.Status.Pruned != 0 || !first.Status.Last.Equal(time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("first = %+v", first.Status)
	}
	// Two runs and a pruning between reads are one event.
	fake.report("/", "2h", map[string]any{"created_count": 3, "pruned_count": 1, "last": "2030-03-01T04:00:00"})
	second := nextLeaseEvent(t, w)
	if second.Sequence != 3 || second.Status.Created != 3 || second.Status.Pruned != 1 || !second.Status.Last.Equal(time.Date(2030, 3, 1, 4, 0, 0, 0, time.UTC)) {
		t.Fatalf("second = %+v", second.Status)
	}
	// A deactivated schedule can be activated again: a change, not the end.
	fake.report("/", "2h", map[string]any{"active": false})
	if deactivated := nextLeaseEvent(t, w); deactivated.Kind != ceph.WatchChanged || deactivated.Status.Active {
		t.Fatalf("deactivated = %+v", deactivated.Status)
	}
	fake.report("/", "2h", map[string]any{"active": true})
	if activated := nextLeaseEvent(t, w); !activated.Status.Active || activated.Status.Created != 3 {
		t.Fatalf("activated = %+v", activated.Status)
	}
	// Retention is the directory's and changes the compared state.
	fake.native(t, "fs snap-schedule retention add / h 4")
	if retention := nextLeaseEvent(t, w); retention.Status.Retention["h"] != 4 {
		t.Fatalf("retention = %+v", retention.Status)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	fake, _, w := watchedLease(t)
	baseline := nextLeaseEvent(t, w)
	fake.mu.Lock()
	fake.refused["fs snap-schedule status"] = true
	fake.mu.Unlock()
	gap := nextLeaseEvent(t, w)
	if gap.Kind != ceph.WatchGap || gap.Err == nil || gap.Status != nil || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	resumed := nextLeaseEvent(t, w)
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || resumed.Status.Created != baseline.Status.Created {
		t.Fatalf("resumed = %+v (an unchanged status after a gap is still reported)", resumed)
	}
}

func TestLeaseWatchEndsWhenTheScheduleIsNoLongerTheLeases(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*testing.T, *fakeSnapSchedule, *Lease)
		want   string
	}{
		"restored": {func(t *testing.T, _ *fakeSnapSchedule, lease *Lease) {
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
		}, "the lease was restored"},
		"removed natively": {func(t *testing.T, fake *fakeSnapSchedule, _ *Lease) {
			fake.native(t, "fs snap-schedule remove / 2h")
		}, "no longer lists"},
		"replaced natively": {func(t *testing.T, fake *fakeSnapSchedule, _ *Lease) {
			fake.native(t, "fs snap-schedule remove / 2h")
			fake.native(t, "fs snap-schedule add / 2h 2030-03-01T00:00:00")
		}, "lease's"},
		"filesystem unregistered": {func(_ *testing.T, fake *fakeSnapSchedule, _ *Lease) {
			fake.mu.Lock()
			fake.unregistered = true
			fake.mu.Unlock()
		}, "active filesystem created by this cluster"},
		"filesystem replaced": {func(_ *testing.T, fake *fakeSnapSchedule, _ *Lease) {
			fake.mu.Lock()
			fake.fsID = 42
			fake.mu.Unlock()
		}, "replaced"},
	} {
		t.Run(name, func(t *testing.T) {
			fake, lease, w := watchedLease(t)
			nextLeaseEvent(t, w)
			tc.change(t, fake, lease)
			awaitLeaseWatchDone(t, w)
			err := w.Stop()
			if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch snapshot schedule 2h of /") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireLeaseWatchClosed(t, w); len(rest) != 0 {
				t.Fatalf("a refused read produced events: %+v", rest)
			}
		})
	}
}

func TestLeaseWatchEventsAreIndependent(t *testing.T) {
	fake, _, w := watchedLease(t)
	baseline := nextLeaseEvent(t, w)
	baseline.Status.Retention["n"] = 99
	baseline.Status.Raw[0] = 'x'
	baseline.Status.Created = 7
	fake.awaitCommands(t, 6)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	fake.report("/", "2h", map[string]any{"created_count": 1})
	changed := nextLeaseEvent(t, w)
	if changed.Status.Retention["n"] != 2 || changed.Status.Raw[0] != '{' || changed.Status.Created != 1 {
		t.Fatalf("a later event shared a delivered status: %+v", changed.Status)
	}
}

func TestLeaseWatchRefusesUnboundLeasesAndInvalidConfig(t *testing.T) {
	fake := newFakeSnapSchedule()
	fs := snapScheduleFilesystem(fake)
	fake.lost["fs snap-schedule add"] = true
	unbound, err := add(t.Context(), fs, Config{Path: "/", Repeat: "1h"})
	if err == nil || unbound == nil {
		t.Fatalf("lost add = %v, %v", unbound, err)
	}
	if w, err := unbound.Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "never confirmed") {
		t.Fatalf("Watch of an unconfirmed lease = %v, %v", w, err)
	}
	if err := unbound.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if w, err := unbound.Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "restored") {
		t.Fatalf("Watch of a restored lease = %v, %v", w, err)
	}
	lease, err := add(t.Context(), fs, leaseSchedule)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	commands := fake.commands
	fake.mu.Unlock()
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := lease.Watch(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch snapshot schedule") {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	fake.mu.Lock()
	ran := fake.commands - commands
	fake.mu.Unlock()
	if ran != 0 {
		t.Fatalf("an invalid config ran %d commands", ran)
	}
	for name, l := range map[string]*Lease{"nil": nil, "zero": {}} {
		if w, err := l.Watch(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	got, err := watch.Validate(0, 0, 0, leaseWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
}

func TestLeaseWatchStalledConsumerOverflowsWithAnAttributableCause(t *testing.T) {
	var mu sync.Mutex
	created := 0
	read := func(context.Context) (Status, error) {
		mu.Lock()
		defer mu.Unlock()
		return Status{Path: "/", Schedule: "1m", Active: true, Created: created, Retention: map[string]int{}}, nil
	}
	w, err := startLeaseWatch(t.Context(), watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: 2}, "watch snapshot schedule 1m of /", read)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		mu.Lock()
		created = i
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	awaitLeaseWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), "watch snapshot schedule 1m of /: event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
}

func TestLeaseWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	_, _, w := watchedLease(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireLeaseWatchClosed(t, w)
	var zero *LeaseWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&LeaseWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}
