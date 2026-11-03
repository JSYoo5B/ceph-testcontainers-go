package mirror

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/testcontainers/testcontainers-go"
)

// directoryWatchFixture is the scripted mirror of the DirectoryStatus tests,
// observed by a watch. Every change of the scripted native state happens
// under Mirror.mu, which each read holds for its whole observation.
type directoryWatchFixture struct {
	mirror              *Mirror
	source, destination *cephFSObserverDraftControl
	a, b                *cephFSObserverDraftDaemon
	reads               atomic.Int64
}

func newDirectoryWatchFixture() *directoryWatchFixture {
	f := &directoryWatchFixture{}
	f.mirror, f.source, f.destination, f.a, f.b = newCephFSObserverDraft()
	f.mirror.source = &ceph.Container{Container: terminableDirectoryControl{f.source}}
	f.mirror.destination = &ceph.Container{Container: terminableDirectoryControl{f.destination}}
	return f
}

// terminableDirectoryControl lets the scripted control container of a
// cluster be terminated, so that a test can end the cluster under a watch.
type terminableDirectoryControl struct{ *cephFSObserverDraftControl }

func (terminableDirectoryControl) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return nil
}

func (f *directoryWatchFixture) start(t *testing.T, ctx context.Context, settings watch.Settings) *DirectoryWatch {
	t.Helper()
	w, err := f.startErr(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func (f *directoryWatchFixture) startErr(ctx context.Context, settings watch.Settings) (*DirectoryWatch, error) {
	read := f.mirror.directoryWatchRead("/owned")
	return startDirectoryWatch(ctx, settings, "/owned", func(ctx context.Context) (DirectoryStatus, error) {
		f.reads.Add(1)
		return read(ctx)
	})
}

func (f *directoryWatchFixture) change(edit func(*directoryWatchFixture)) {
	f.mirror.mu.Lock()
	defer f.mirror.mu.Unlock()
	edit(f)
}

// awaitReads waits until n more reads have started, each of which observed
// the state as it is now or later.
func (f *directoryWatchFixture) awaitReads(t *testing.T, n int64) {
	t.Helper()
	want := f.reads.Load() + n
	deadline := time.Now().Add(5 * time.Second)
	for f.reads.Load() < want {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
		time.Sleep(time.Millisecond)
	}
}

func directoryReplay(state string, last uint64, synced int) string {
	current := ""
	if state == "syncing" {
		current = fmt.Sprintf(`"current_syncing_snap":{"id":%d,"name":"next"},`, last+1)
	}
	return fmt.Sprintf(`{"/owned":{"state":%q,%s"last_synced_snap":{"id":%d,"name":"checkpoint-%d"},"snaps_synced":%d,"snaps_deleted":0,"snaps_renamed":0}}`, state, current, last, last, synced)
}

func fastDirectoryWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

func nextDirectoryEvent(t *testing.T, w *DirectoryWatch) DirectoryWatchEvent {
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
	return DirectoryWatchEvent{}
}

func requireDirectoryWatchClosed(t *testing.T, w *DirectoryWatch) []DirectoryWatchEvent {
	t.Helper()
	var rest []DirectoryWatchEvent
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

func awaitDirectoryWatchDone(t *testing.T, w *DirectoryWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

func TestDirectoryWatchBaselineIsBufferedBeforeItReturns(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1})
	select {
	case event := <-w.Events():
		s := event.Status
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Err != nil || !s.Ready || s.Directory != "/owned" || s.PeerID != cephFSObserverDraftPeer ||
			s.SourceFilesystemID != 1 || s.DestinationFilesystemID != 2 || s.MappingState != "mapped" || s.InstanceID != "4262" || s.DaemonName != "a" ||
			s.State != "idle" || !reflect.DeepEqual(s.LastSyncedSnapshot, &Snapshot{ID: 120, Name: "checkpoint"}) || len(s.DaemonProblems) != 0 {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("the watch returned before the baseline was buffered")
	}
	if len(f.source.mutations) != 0 {
		t.Fatalf("the watch mutated native policy: %q", f.source.mutations)
	}
}

func TestDirectoryWatchUnchangedStateProducesNoEvent(t *testing.T) {
	f := newDirectoryWatchFixture()
	f.change(func(f *directoryWatchFixture) {
		f.a.replay = `{"/owned":{"state":"failed","failure_reason":"first text","snaps_synced":0,"snaps_deleted":0,"snaps_renamed":0}}`
	})
	w := f.start(t, t.Context(), fastDirectoryWatch(4))
	if baseline := nextDirectoryEvent(t, w); baseline.Status.State != "failed" || baseline.Status.Ready || baseline.Status.FailureReason != "first text" {
		t.Fatalf("baseline = %+v", baseline)
	}
	f.awaitReads(t, 5)
	// Free failure text and a restarted unchanged report are not changes.
	f.change(func(f *directoryWatchFixture) {
		f.a.replay = strings.Replace(f.a.replay, "first text", "second text", 1)
	})
	f.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged state", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := requireDirectoryWatchClosed(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged state: %+v", rest)
	}
}

func TestDirectoryWatchReportsReplayAndReassignmentInOrder(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(16))
	steps := []func(*directoryWatchFixture){
		// The selected owner starts syncing the next source snapshot.
		func(f *directoryWatchFixture) { f.a.replay = directoryReplay("syncing", 120, 1) },
		// It reports that snapshot as the last synced one.
		func(f *directoryWatchFixture) { f.a.replay = directoryReplay("idle", 121, 2) },
		// The owner is lost while the MGR still maps the directory to it.
		func(f *directoryWatchFixture) { f.a.running = false },
		// The MGR reassigns it; the new owner's counters start again.
		func(f *directoryWatchFixture) {
			f.source.mapping = `{"state":"mapped","instance_id":"4266"}`
			f.b.replay = directoryReplay("idle", 121, 0)
		},
	}
	for _, step := range steps {
		f.change(step)
		f.awaitReads(t, 3)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := requireDirectoryWatchClosed(t, w)
	if len(events) != 5 {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) || (i > 0 && event.Kind != ceph.WatchChanged) || event.Err != nil {
			t.Fatalf("event %d = %+v", i, event)
		}
	}
	syncing, synced, lost, reassigned := events[1].Status, events[2].Status, events[3].Status, events[4].Status
	if syncing.State != "syncing" || !syncing.Ready || !reflect.DeepEqual(syncing.CurrentSnapshot, &Snapshot{ID: 121, Name: "next"}) || syncing.LastSyncedSnapshot.ID != 120 {
		t.Fatalf("syncing = %+v", syncing)
	}
	if synced.State != "idle" || synced.CurrentSnapshot != nil || !reflect.DeepEqual(synced.LastSyncedSnapshot, &Snapshot{ID: 121, Name: "checkpoint-121"}) || synced.SnapshotsSynced != 2 {
		t.Fatalf("synced = %+v", synced)
	}
	// The lost owner is evidence in DaemonProblems, not a gap or an empty
	// observation of the directory.
	if lost.Ready || lost.DaemonName != "" || lost.InstanceID != "4262" || lost.DaemonProblems["a"] == "" || lost.LastSyncedSnapshot != nil {
		t.Fatalf("owner lost = %+v", lost)
	}
	if !reassigned.Ready || reassigned.DaemonName != "b" || reassigned.InstanceID != "4266" || reassigned.SnapshotsSynced != 0 ||
		!reflect.DeepEqual(reassigned.LastSyncedSnapshot, &Snapshot{ID: 121, Name: "checkpoint-121"}) || reassigned.DaemonProblems["a"] == "" {
		t.Fatalf("reassigned = %+v", reassigned)
	}
}

func TestDirectoryWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(64))
	baseline := nextDirectoryEvent(t, w)
	boom := errors.New("transient")
	f.change(func(f *directoryWatchFixture) { f.source.peerQueryError = boom })
	gap := nextDirectoryEvent(t, w)
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, boom) || gap.Status.Ready || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	f.change(func(f *directoryWatchFixture) { f.source.peerQueryError = nil })
	var resumed DirectoryWatchEvent
	for resumed = nextDirectoryEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextDirectoryEvent(t, w) {
		if !errors.Is(resumed.Err, boom) {
			t.Fatalf("gap = %+v", resumed)
		}
	}
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Status, baseline.Status) {
		t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
	}
	f.change(func(f *directoryWatchFixture) { f.a.replay = directoryReplay("idle", 121, 2) })
	changed := nextDirectoryEvent(t, w)
	if changed.Kind != ceph.WatchChanged || changed.Sequence != resumed.Sequence+1 || changed.Status.LastSyncedSnapshot.ID != 121 {
		t.Fatalf("changed = %+v", changed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestDirectoryWatchStalledConsumerOverflows(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(2))
	f.change(func(f *directoryWatchFixture) { f.a.replay = directoryReplay("idle", 121, 2) })
	f.awaitReads(t, 2)
	f.change(func(f *directoryWatchFixture) { f.a.replay = directoryReplay("idle", 122, 3) })
	awaitDirectoryWatchDone(t, w)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), "event 3 (changed)") || !strings.Contains(err.Error(), "/owned") {
		t.Fatalf("Stop = %v", err)
	}
	rest := requireDirectoryWatchClosed(t, w)
	if len(rest) != 2 || rest[0].Kind != ceph.WatchBaseline || rest[1].Status.LastSyncedSnapshot.ID != 121 {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestDirectoryWatchStopDuringBlockedReadJoins(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(1))
	// A consumer that holds the mirror lock blocks the next read in its lock
	// wait; Stop must interrupt that wait and join.
	f.mirror.mu.Lock()
	f.awaitReads(t, 1)
	stopped := make(chan error, 1)
	go func() { stopped <- w.Stop() }()
	select {
	case err := <-stopped:
		f.mirror.mu.Unlock()
		if err != nil {
			t.Fatalf("Stop = %v", err)
		}
	case <-time.After(5 * time.Second):
		f.mirror.mu.Unlock()
		t.Fatal("Stop did not interrupt the read")
	}
	if rest := requireDirectoryWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
}

func TestDirectoryWatchContextEndStopsTheWatch(t *testing.T) {
	f := newDirectoryWatchFixture()
	ctx, cancel := context.WithCancel(t.Context())
	w := f.start(t, ctx, fastDirectoryWatch(1))
	cancel()
	awaitDirectoryWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	requireDirectoryWatchClosed(t, w)
}

func TestDirectoryWatchEventsAreIndependent(t *testing.T) {
	f := newDirectoryWatchFixture()
	f.change(func(f *directoryWatchFixture) {
		f.a.running = false
		f.source.mapping = `{"state":"mapped","instance_id":"4266"}`
	})
	w := f.start(t, t.Context(), fastDirectoryWatch(8))
	baseline := nextDirectoryEvent(t, w)
	if baseline.Status.DaemonProblems["a"] == "" || baseline.Status.DaemonName != "b" {
		t.Fatalf("baseline = %+v", baseline)
	}
	// A consumer that edits a delivered status changes neither the compared
	// state nor a later event.
	baseline.Status.LastSyncedSnapshot.ID = 999
	baseline.Status.DaemonProblems["a"] = "edited"
	baseline.Status.DaemonProblems["c"] = "added"
	f.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	f.change(func(f *directoryWatchFixture) { f.b.replay = directoryReplay("idle", 121, 2) })
	first := nextDirectoryEvent(t, w)
	f.change(func(f *directoryWatchFixture) { f.b.replay = directoryReplay("idle", 122, 3) })
	second := nextDirectoryEvent(t, w)
	if first.Status.LastSyncedSnapshot.ID != 121 || first.Status.DaemonProblems["a"] == "edited" || len(first.Status.DaemonProblems) != 1 {
		t.Fatalf("first change = %+v", first)
	}
	if first.Status.LastSyncedSnapshot == second.Status.LastSyncedSnapshot || second.Status.LastSyncedSnapshot.ID != 122 {
		t.Fatalf("a later read changed a delivered status: %+v then %+v", first, second)
	}
	second.Status.DaemonProblems["a"] = "edited"
	if first.Status.DaemonProblems["a"] == "edited" {
		t.Fatal("two events share one DaemonProblems map")
	}
}

func TestDirectoryWatchIdentitySubstitutionEndsTheWatch(t *testing.T) {
	const otherPeer = "e8dded8c-93a6-4c12-ab99-5b429cdba6c5"
	cases := map[string]struct {
		change func(*directoryWatchFixture)
		want   string
	}{
		"policy removed": {func(f *directoryWatchFixture) { delete(f.mirror.ownedDirectories, "/owned") }, "does not own the watched directory policy"},
		"policy registered again under the same path": {func(f *directoryWatchFixture) {
			f.mirror.ops.advanceDirectoryGeneration("/owned")
		}, "registered again"},
		"peer replaced by the fixture": {func(f *directoryWatchFixture) {
			f.mirror.peerID = otherPeer
			f.mirror.ops.peerGeneration++
		}, "peer was removed or replaced"},
		"peer removed": {func(f *directoryWatchFixture) { f.mirror.peerID = "" }, "peer was removed or replaced"},
		"native peer substituted": {func(f *directoryWatchFixture) {
			f.source.peer = fmt.Sprintf(`{%q:{"client_name":"client.peer","site_name":"destination-site","fs_name":"destination"}}`, otherPeer)
		}, "peer policy destination differs"},
		"destination filesystem recreated": {func(f *directoryWatchFixture) { f.destination.filesystemID++ }, "filesystem or metadata pool identity differs"},
		"source metadata pool replaced":    {func(f *directoryWatchFixture) { f.source.metadataPool++ }, "filesystem or metadata pool identity differs"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newDirectoryWatchFixture()
			w := f.start(t, t.Context(), fastDirectoryWatch(4))
			f.change(c.change)
			awaitDirectoryWatchDone(t, w)
			err := w.Stop()
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.HasPrefix(err.Error(), "watch CephFS mirror directory /owned: ") || !errors.Is(err, errCephFSObservationGuard) {
				t.Fatalf("Stop = %v", err)
			}
			if strings.Contains(err.Error(), "client.peer") || strings.Contains(err.Error(), "AQ") {
				t.Fatalf("identity error leaks native detail: %v", err)
			}
			if rest := requireDirectoryWatchClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("a substituted read produced events: %+v", rest)
			}
		})
	}
}

func TestDirectoryWatchEndsWhenTheMirrorTerminates(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(4))
	nextDirectoryEvent(t, w)
	if err := f.mirror.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	awaitDirectoryWatchDone(t, w)
	if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "mirror has been terminated") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := requireDirectoryWatchClosed(t, w); len(rest) != 0 {
		t.Fatalf("a terminated mirror produced events: %+v", rest)
	}
}

func TestDirectoryWatchEndsWhenAClusterTerminates(t *testing.T) {
	for _, site := range []string{"source", "destination"} {
		t.Run(site, func(t *testing.T) {
			f := newDirectoryWatchFixture()
			w := f.start(t, t.Context(), fastDirectoryWatch(4))
			nextDirectoryEvent(t, w)
			cluster := f.mirror.source
			if site == "destination" {
				cluster = f.mirror.destination
			}
			if err := cluster.Terminate(t.Context()); err != nil {
				t.Fatal(err)
			}
			awaitDirectoryWatchDone(t, w)
			if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), site+" cluster has been terminated") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireDirectoryWatchClosed(t, w); len(rest) != 0 {
				t.Fatalf("a terminated cluster produced events: %+v", rest)
			}
		})
	}
}

func TestDirectoryWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	f := newDirectoryWatchFixture()
	w, err := f.mirror.WatchDirectory(t.Context(), ceph.WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1}, "/owned")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireUnlocked := func(phase string) {
		t.Helper()
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if !f.mirror.mu.TryLock() {
				t.Fatalf("the watch kept the mirror lock %s", phase)
			}
			f.mirror.mu.Unlock()
			for _, cluster := range []*ceph.Container{f.mirror.source, f.mirror.destination} {
				if !servicehook.TryLock(cluster) {
					t.Fatalf("the watch kept a cluster topology lock %s", phase)
				}
				servicehook.Unlock(cluster)
			}
		}
	}
	requireUnlocked("between reads")
	// The unread baseline fills the buffer, so the change waits in delivery:
	// the consumer can still take the locks and query the fixture.
	f.change(func(f *directoryWatchFixture) { f.a.replay = directoryReplay("idle", 121, 2) })
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		f.mirror.mu.Lock()
		calls := len(f.a.calls)
		f.mirror.mu.Unlock()
		if calls >= 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if status, err := f.mirror.DirectoryStatus(t.Context(), "/owned"); err != nil || status.LastSyncedSnapshot.ID != 121 {
		t.Fatalf("consumer query during the watch = %+v, %v", status, err)
	}
	if baseline := nextDirectoryEvent(t, w); baseline.Kind != ceph.WatchBaseline || baseline.Status.LastSyncedSnapshot.ID != 120 {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextDirectoryEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.LastSyncedSnapshot.ID != 121 {
		t.Fatalf("changed = %+v", changed)
	}
	// A consumer that holds the mirror lock longer than QueryTimeout gets a
	// gap rather than a blocked watch, and the watch resumes after it.
	f.mirror.mu.Lock()
	gap := nextDirectoryEvent(t, w)
	f.mirror.mu.Unlock()
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextDirectoryEvent(t, w); event.Kind != ceph.WatchResumed; event = nextDirectoryEvent(t, w) {
		if event.Kind != ceph.WatchGap {
			t.Fatalf("event after the lock was released = %+v", event)
		}
	}
	// The same holds for a cluster operation that keeps the topology lock.
	servicehook.Lock(f.mirror.source)
	gap = nextDirectoryEvent(t, w)
	servicehook.Unlock(f.mirror.source)
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	requireDirectoryWatchClosed(t, w)
}

func TestDirectoryWatchRefusalsStartNothing(t *testing.T) {
	f := newDirectoryWatchFixture()
	commands := func() int {
		f.mirror.mu.Lock()
		defer f.mirror.mu.Unlock()
		return len(f.source.calls) + len(f.destination.calls) + len(f.a.calls) + len(f.b.calls)
	}
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := f.mirror.WatchDirectory(t.Context(), cfg, "/owned"); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if w, err := f.mirror.WatchDirectory(t.Context(), ceph.WatchConfig{}, "owned"); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("relative path = %v, %v", w, err)
	}
	var none *Mirror
	if w, err := none.WatchDirectory(t.Context(), ceph.WatchConfig{}, "/owned"); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil mirror = %v, %v", w, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := f.mirror.WatchDirectory(ctx, ceph.WatchConfig{}, "/owned"); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context = %v, %v", w, err)
	}
	if n := commands(); n != 0 {
		t.Fatalf("a refused watch ran %d commands", n)
	}
	if w, err := f.mirror.WatchDirectory(t.Context(), ceph.WatchConfig{}, "/foreign"); w != nil || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("a directory the mirror does not own = %v, %v", w, err)
	}
	f.change(func(f *directoryWatchFixture) { f.source.peerQueryError = errors.New("boom") })
	if w, err := f.startErr(t.Context(), fastDirectoryWatch(1)); w != nil || err == nil {
		t.Fatalf("failed baseline = %v, %v", w, err)
	}
	got, err := watch.Validate(0, 0, 0, directoryWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
}

func TestDirectoryWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	f := newDirectoryWatchFixture()
	w := f.start(t, t.Context(), fastDirectoryWatch(1))
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireDirectoryWatchClosed(t, w)
	var zero *DirectoryWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&DirectoryWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}
