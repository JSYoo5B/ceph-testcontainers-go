package cephfs

import (
	"context"
	"encoding/json"
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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// mdsWatchSource is a scripted mdsWatchRead: the owner state and the FSMap of
// filesystem "tenant", and the failure of the next reads, all changed by the
// test while the watch goroutine reads them. The FSMap is encoded as fs dump
// JSON, so every read goes through the production decoder.
type mdsWatchSource struct {
	mu                 sync.Mutex
	epoch, mdsMapEpoch uint64
	id, metadataPool   int64
	dataPools          []int64
	maxMDS             int
	info               map[string]fsbase.MDSInfo
	standbys           []fsbase.MDSInfo
	absent, garbled    bool
	raw                []byte // replaces the encoded FSMap when set
	open, registered   bool
	native             *fsbase.NativeIdentity
	daemons            []*MDS
	fail               error
	block              bool
	reads              int
}

// newMDSWatchSource owns tenant-0 as the active rank 0 and tenant-1 as an
// ordinary standby.
func newMDSWatchSource() *mdsWatchSource {
	return &mdsWatchSource{
		epoch: 20, mdsMapEpoch: 12, id: 41, metadataPool: 1, dataPools: []int64{2, 3}, maxMDS: 1,
		info:     map[string]fsbase.MDSInfo{"gid_101": {Name: "tenant-0", Rank: 0, GID: 101, State: "up:active", JoinFSCID: 41}},
		standbys: []fsbase.MDSInfo{{Name: "tenant-1", Rank: -1, GID: 102, State: "up:standby", JoinFSCID: 41}},
		open:     true, registered: true,
		native:  &fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2},
		daemons: []*MDS{{ID: "tenant-0", FilesystemName: "tenant"}, {ID: "tenant-1", FilesystemName: "tenant"}},
	}
}

func (s *mdsWatchSource) dump() []byte {
	if s.garbled {
		return []byte(`{"filesystems":`)
	}
	if s.raw != nil {
		return s.raw
	}
	filesystems := []any{map[string]any{"id": 8, "mdsmap": map[string]any{"epoch": 3, "fs_name": "other", "max_mds": 1, "metadata_pool": 9, "data_pools": []int64{10}, "info": map[string]any{}}}}
	if !s.absent {
		filesystems = append(filesystems, map[string]any{"id": s.id, "mdsmap": map[string]any{
			"epoch": s.mdsMapEpoch, "fs_name": "tenant", "max_mds": s.maxMDS, "metadata_pool": s.metadataPool,
			"data_pools": s.dataPools, "info": s.info,
		}})
	}
	data, err := json.Marshal(map[string]any{"epoch": s.epoch, "standbys": s.standbys, "filesystems": filesystems})
	if err != nil {
		panic(err)
	}
	return data
}

func (s *mdsWatchSource) read(ctx context.Context) (mdsOwnerSample, []byte, error) {
	s.mu.Lock()
	s.reads++
	block, fail := s.block, s.fail
	owner := mdsOwnerSample{daemons: slices.Clone(s.daemons), open: s.open, registered: s.registered}
	if s.native != nil {
		native := *s.native
		owner.native = &native
	}
	data := s.dump()
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return owner, nil, ctx.Err()
	}
	if fail != nil {
		return owner, nil, fail
	}
	if !owner.open || !owner.registered {
		return owner, nil, nil
	}
	return owner, data, nil
}

// update changes the native state and advances both epochs.
func (s *mdsWatchSource) update(change func(*mdsWatchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
	s.epoch++
	s.mdsMapEpoch++
}

// failActive removes the active tenant-0 and lets the standby tenant-1 take
// rank 0 in state.
func (s *mdsWatchSource) failActive(state string) {
	s.update(func(s *mdsWatchSource) {
		s.info = map[string]fsbase.MDSInfo{"gid_102": {Name: "tenant-1", Rank: 0, GID: 102, State: state, JoinFSCID: 41}}
		s.standbys = nil
	})
}

func (s *mdsWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// awaitReads waits until the source has started n more reads, each of which
// sampled the state as it is now or later.
func (s *mdsWatchSource) awaitReads(t *testing.T, n int) {
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

func fastMDSWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

func startTestMDSWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *mdsWatchSource) *MDSWatch {
	t.Helper()
	w, err := startMDSWatch(ctx, settings, "tenant", source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func nextMDSEvent(t *testing.T, w *MDSWatch) MDSWatchEvent {
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
	return MDSWatchEvent{}
}

// requireMDSClosed drains the buffered events and fails unless the channel is
// closed after them.
func requireMDSClosed(t *testing.T, w *MDSWatch) []MDSWatchEvent {
	t.Helper()
	var rest []MDSWatchEvent
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

func awaitMDSDone(t *testing.T, w *MDSWatch) {
	t.Helper()
	select {
	case <-w.stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

var (
	mdsWatchActive0  = MDSStatus{Name: "tenant-0", Rank: 0, GID: 101, State: "up:active", Owned: true}
	mdsWatchStandby1 = MDSStatus{Name: "tenant-1", Rank: -1, GID: 102, State: "up:standby", Owned: true}
)

func TestMDSWatchBaselineIsBufferedBeforeWatchMDSsReturns(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		want := FilesystemStatus{FilesystemName: "tenant", FilesystemID: 41, MaxMDS: 1, Active: []MDSStatus{mdsWatchActive0}, Standby: []MDSStatus{mdsWatchStandby1}}
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Epoch != 20 || event.MDSMapEpoch != 12 || event.Err != nil || !reflect.DeepEqual(event.Status, want) {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchMDSs returned before the baseline was buffered")
	}
}

func TestMDSWatchUnchangedStateProducesNoEvent(t *testing.T) {
	source := newMDSWatchSource()
	source.daemons = append(source.daemons, &MDS{ID: "tenant-2", FilesystemName: "tenant"})
	source.standbys = append(source.standbys, fsbase.MDSInfo{Name: "tenant-2", Rank: -1, GID: 103, State: "up:standby", JoinFSCID: 41})
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(4), source)
	if baseline := nextMDSEvent(t, w); len(baseline.Status.Standby) != 2 || baseline.Status.Standby[0].Name != "tenant-1" {
		t.Fatalf("baseline = %+v", baseline)
	}
	source.awaitReads(t, 5)
	// Epoch changes alone, a reordered native list and a standby of another
	// deployment are not changes of the compared fields.
	source.update(func(s *mdsWatchSource) {
		s.standbys = append(s.standbys, fsbase.MDSInfo{Name: "outsider", Rank: -1, GID: 300, State: "up:standby", JoinFSCID: 41})
		slices.Reverse(s.standbys)
	})
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged state", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := requireMDSClosed(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged state: %+v", rest)
	}
}

// A takeover and the failed daemon's return are ordered changes; none is
// replaced by a later one while the consumer leaves them unread.
func TestMDSWatchReportsTakeoverAndRestoredStandbyInOrder(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(8), source)
	source.failActive("up:replay")
	source.awaitReads(t, 3)
	source.failActive("up:active")
	source.awaitReads(t, 3)
	source.update(func(s *mdsWatchSource) {
		s.standbys = []fsbase.MDSInfo{{Name: "tenant-0", Rank: -1, GID: 201, State: "up:standby", JoinFSCID: 41}}
	})
	source.awaitReads(t, 3)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := requireMDSClosed(t, w)
	if len(events) != 4 {
		t.Fatalf("events = %+v", events)
	}
	// A rank in replay is not active, so MDSStatus lists it in no list.
	took := MDSStatus{Name: "tenant-1", Rank: 0, GID: 102, State: "up:active", Owned: true}
	want := []struct {
		active  []MDSStatus
		standby []MDSStatus
	}{
		{[]MDSStatus{mdsWatchActive0}, []MDSStatus{mdsWatchStandby1}},
		{nil, nil},
		{[]MDSStatus{took}, nil},
		{[]MDSStatus{took}, []MDSStatus{{Name: "tenant-0", Rank: -1, GID: 201, State: "up:standby", Owned: true}}},
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) || (i > 0 && event.Kind != ceph.WatchChanged) {
			t.Fatalf("event %d = %+v", i, event)
		}
		if !slices.Equal(event.Status.Active, want[i].active) || !slices.Equal(event.Status.Standby, want[i].standby) || event.Status.FilesystemID != 41 {
			t.Fatalf("event %d status = %+v", i, event.Status)
		}
		if i > 0 && (event.Epoch <= events[i-1].Epoch || event.MDSMapEpoch <= events[i-1].MDSMapEpoch) {
			t.Fatalf("epochs did not advance: %+v after %+v", event, events[i-1])
		}
	}
}

func TestMDSWatchReportsMaxMDSAndUnownedRanks(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(8), source)
	nextMDSEvent(t, w)
	source.update(func(s *mdsWatchSource) { s.maxMDS = 2 })
	if changed := nextMDSEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.MaxMDS != 2 {
		t.Fatalf("max_mds change = %+v", changed)
	}
	// An external daemon that takes a rank is a change of ownership, not of
	// the watched filesystem.
	source.update(func(s *mdsWatchSource) {
		s.info["gid_300"] = fsbase.MDSInfo{Name: "outsider", Rank: 1, GID: 300, State: "up:active", JoinFSCID: 41}
	})
	changed := nextMDSEvent(t, w)
	if len(changed.Status.Active) != 2 || changed.Status.Active[1] != (MDSStatus{Name: "outsider", Rank: 1, GID: 300, State: "up:active"}) {
		t.Fatalf("unowned rank = %+v", changed.Status)
	}
}

func TestMDSWatchAlreadyCanceledContextStartsNothing(t *testing.T) {
	source := newMDSWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := startMDSWatch(ctx, fastMDSWatch(1), "tenant", source.read); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("watch = %v, %v", w, err)
	}
	if source.readCount() != 0 {
		t.Fatal("a canceled context read the cluster")
	}
	fs, control := newMDSWatchFixture()
	if w, err := fs.WatchMDSs(ctx, ceph.WatchConfig{}); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("filesystem watch = %v, %v", w, err)
	}
	if n := control.dumpCount(); n != 0 {
		t.Fatalf("a canceled context ran %d FSMap reads", n)
	}
}

func TestMDSWatchFailedBaselineStartsNothing(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]struct {
		change  func(*mdsWatchSource)
		invalid bool
	}{
		"failed read":        {change: func(s *mdsWatchSource) { s.fail = boom }},
		"undecodable FSMap":  {change: func(s *mdsWatchSource) { s.garbled = true }},
		"terminated cluster": {change: func(s *mdsWatchSource) { s.open = false }, invalid: true},
		"unregistered":       {change: func(s *mdsWatchSource) { s.registered = false }, invalid: true},
		"no native identity": {change: func(s *mdsWatchSource) { s.native = nil }, invalid: true},
		"absent filesystem":  {change: func(s *mdsWatchSource) { s.absent = true }, invalid: true},
		"recreated":          {change: func(s *mdsWatchSource) { s.id = 42 }, invalid: true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			source := newMDSWatchSource()
			c.change(source)
			w, err := startMDSWatch(t.Context(), fastMDSWatch(1), "tenant", source.read)
			if w != nil || err == nil || errors.Is(err, ceph.ErrInvalidRequest) != c.invalid {
				t.Fatalf("watch = %v, %v", w, err)
			}
			if source.readCount() != 1 {
				t.Fatalf("a failed baseline made %d reads", source.readCount())
			}
		})
	}
}

func TestMDSWatchStopDuringQuery(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(1), source)
	source.update(func(s *mdsWatchSource) { s.block = true })
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
	if rest := requireMDSClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
}

func TestMDSWatchContextEndDuringQuery(t *testing.T) {
	source := newMDSWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w := startTestMDSWatch(t, ctx, fastMDSWatch(1), source)
	source.update(func(s *mdsWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	awaitMDSDone(t, w)
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	requireMDSClosed(t, w)
}

func TestMDSWatchStalledConsumerOverflowsWithAnAttributableCause(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(2), source)
	source.failActive("up:replay")
	source.awaitReads(t, 2)
	source.failActive("up:active")
	awaitMDSDone(t, w)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), "watch MDSs: event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if err2 := w.Stop(); err2 != err {
		t.Fatalf("Stop is not stable: %v then %v", err, err2)
	}
	rest := requireMDSClosed(t, w)
	if len(rest) != 2 || rest[0].Kind != ceph.WatchBaseline || rest[1].Sequence != 2 || len(rest[1].Status.Active) != 0 || len(rest[1].Status.Standby) != 0 {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestMDSWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	for name, fault := range map[string]func(*mdsWatchSource, error){
		"failed read":       func(s *mdsWatchSource, err error) { s.fail = err },
		"undecodable FSMap": func(s *mdsWatchSource, _ error) { s.garbled = true },
		// Valid JSON that is not a complete FSMap must not pass for a
		// removed or recreated filesystem.
		"empty object":     func(s *mdsWatchSource, _ error) { s.raw = []byte(`{}`) },
		"null":             func(s *mdsWatchSource, _ error) { s.raw = []byte(`null`) },
		"null filesystems": func(s *mdsWatchSource, _ error) { s.raw = []byte(`{"epoch":21,"standbys":[],"filesystems":null}`) },
		"unnamed entry": func(s *mdsWatchSource, _ error) {
			s.raw = []byte(`{"epoch":21,"standbys":[],"filesystems":[{"id":41}]}`)
		},
		"entry without its ID": func(s *mdsWatchSource, _ error) {
			s.raw = []byte(`{"epoch":21,"standbys":[],"filesystems":[{"mdsmap":{"fs_name":"tenant","metadata_pool":1,"data_pools":[2]}}]}`)
		},
		"entry without pools": func(s *mdsWatchSource, _ error) {
			s.raw = []byte(`{"epoch":21,"standbys":[],"filesystems":[{"id":41,"mdsmap":{"fs_name":"tenant"}}]}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			source := newMDSWatchSource()
			w := startTestMDSWatch(t, t.Context(), fastMDSWatch(64), source)
			baseline := nextMDSEvent(t, w)
			boom := errors.New("transient")
			source.update(func(s *mdsWatchSource) { fault(s, boom) })
			gap := nextMDSEvent(t, w)
			if gap.Kind != ceph.WatchGap || gap.Err == nil || !reflect.DeepEqual(gap.Status, FilesystemStatus{}) || gap.Epoch != 0 || gap.MDSMapEpoch != 0 || gap.Sequence != 2 {
				t.Fatalf("gap = %+v", gap)
			}
			source.update(func(s *mdsWatchSource) { s.fail, s.garbled, s.raw = nil, false, nil })
			var resumed MDSWatchEvent
			for resumed = nextMDSEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextMDSEvent(t, w) {
			}
			if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Status, baseline.Status) {
				t.Fatalf("resumed = %+v (an unchanged state after a gap is still reported)", resumed)
			}
			source.failActive("up:active")
			changed := nextMDSEvent(t, w)
			if changed.Kind != ceph.WatchChanged || changed.Sequence != resumed.Sequence+1 || changed.Status.Active[0].Name != "tenant-1" {
				t.Fatalf("changed = %+v", changed)
			}
			if err := w.Stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMDSWatchIdentityDriftEndsTheWatch(t *testing.T) {
	cases := map[string]struct {
		drift func(*mdsWatchSource)
		cause string
	}{
		"recreated under the same name": {func(s *mdsWatchSource) { s.id = 42 }, "now has FSMap ID 42"},
		"replaced metadata pool":        {func(s *mdsWatchSource) { s.metadataPool = 7 }, "recreated under the same name"},
		"replaced default data pool":    {func(s *mdsWatchSource) { s.dataPools = []int64{8} }, "recreated under the same name"},
		"absent from the FSMap":         {func(s *mdsWatchSource) { s.absent = true }, "is absent from the FSMap"},
		"removed from the fixture":      {func(s *mdsWatchSource) { s.registered = false }, "was removed"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			source := newMDSWatchSource()
			w := startTestMDSWatch(t, t.Context(), fastMDSWatch(4), source)
			source.update(c.drift)
			awaitMDSDone(t, w)
			err := w.Stop()
			if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), c.cause) || !strings.Contains(err.Error(), `cephfs "tenant"`) {
				t.Fatalf("Stop = %v", err)
			}
			if rest := requireMDSClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("a drifted read produced events: %+v", rest)
			}
		})
	}
}

func TestMDSWatchTerminationEndsTheWatch(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(4), source)
	source.update(func(s *mdsWatchSource) { s.open = false })
	awaitMDSDone(t, w)
	if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "terminated") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := requireMDSClosed(t, w); len(rest) != 1 {
		t.Fatalf("a terminated cluster produced events: %+v", rest)
	}
}

func TestMDSWatchSnapshotsAreIndependent(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(8), source)
	baseline := nextMDSEvent(t, w)
	// A consumer that edits a delivered snapshot changes neither the
	// comparison state nor a later event.
	baseline.Status.Active[0].GID = 999
	baseline.Status.Standby[0].State = "edited"
	baseline.Status.Standby = append(baseline.Status.Standby, MDSStatus{Name: "appended"})
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.update(func(s *mdsWatchSource) { s.maxMDS = 2 })
	first := nextMDSEvent(t, w)
	source.failActive("up:active")
	second := nextMDSEvent(t, w)
	if first.Status.Active[0] != mdsWatchActive0 || !slices.Equal(first.Status.Standby, []MDSStatus{mdsWatchStandby1}) {
		t.Fatalf("first change = %+v", first.Status)
	}
	first.Status.Active[0].Name = "edited"
	if &first.Status.Active[0] == &second.Status.Active[0] || second.Status.Active[0].Name != "tenant-1" || len(second.Status.Standby) != 0 {
		t.Fatalf("a later poll shares or changed a delivered snapshot: %+v then %+v", first.Status, second.Status)
	}
}

func TestMDSWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	source := newMDSWatchSource()
	w := startTestMDSWatch(t, t.Context(), fastMDSWatch(1), source)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	requireMDSClosed(t, w)
	var zero *MDSWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&MDSWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

func TestMDSWatchConfigValidation(t *testing.T) {
	got, err := watch.Validate(0, 0, 0, mdsWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 2 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	fs, control := newMDSWatchFixture()
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := fs.WatchMDSs(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "watch MDSs") {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if n := control.dumpCount(); n != 0 {
		t.Fatalf("an invalid config ran %d FSMap reads", n)
	}
	var unconstructed *Filesystem
	if w, err := unconstructed.WatchMDSs(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil filesystem = %v, %v", w, err)
	}
	if w, err := (&Filesystem{}).WatchMDSs(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("unconstructed filesystem = %v, %v", w, err)
	}
}

// mdsWatchControl is the fixture's control container, serialized so that the
// test can change the FSMap while the watch goroutine reads it.
type mdsWatchControl struct {
	testcontainers.Container
	mu     sync.Mutex
	native *poolFixtureContainer
	dumps  int
}

const mdsWatchDumpKey = "fs dump --format json"

func (c *mdsWatchControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.Join(args[3:], " ") == mdsWatchDumpKey {
		c.dumps++
	}
	return c.native.Exec(ctx, args, opts...)
}

func (c *mdsWatchControl) set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.native.Output = maps.Clone(c.native.Output)
	c.native.Output[key] = value
}

func (c *mdsWatchControl) dumpCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dumps
}

func (c *mdsWatchControl) awaitDumps(t *testing.T, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); c.dumpCount() < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("no FSMap read %d", n)
		}
	}
}

// newMDSWatchFixture is the registered filesystem "fixture" with mds.a active
// on rank 0 at FSMap epoch 5, read through a serialized control container.
func newMDSWatchFixture() (*Filesystem, *mdsWatchControl) {
	fs, native, _, _ := cephFSContextAdmissionFixture()
	control := &mdsWatchControl{native: native}
	control.set(mdsWatchDumpKey, mdsWatchFixtureDump(5, 41, 101))
	fs.base().Cluster().Container = control
	return fs, control
}

func mdsWatchFixtureDump(epoch, id, gid int) string {
	data, err := json.Marshal(map[string]any{"epoch": epoch, "standbys": []any{}, "filesystems": []any{map[string]any{"id": id, "mdsmap": map[string]any{
		"epoch": epoch, "fs_name": "fixture", "max_mds": 1, "metadata_pool": 1, "data_pools": []int{2, 3},
		"info": map[string]any{"a": map[string]any{"name": "a", "rank": 0, "gid": gid, "state": "up:active", "join_fscid": id}},
	}}}})
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestMDSWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	fs, control := newMDSWatchFixture()
	cluster := fs.base().Cluster()
	state := testhelper.TestState(cluster)
	w, err := fs.WatchMDSs(t.Context(), ceph.WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireUnlocked := func(phase string) {
		t.Helper()
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			for name, mutex := range map[string]sync.Locker{"topology": state.Mutex, "control": state.ControlMutex, "setup": &filesystems(cluster).setupMu} {
				if !mutex.(interface{ TryLock() bool }).TryLock() {
					t.Fatalf("the watch kept the %s lock %s", name, phase)
				}
				mutex.Unlock()
			}
		}
	}
	requireUnlocked("between reads")
	// The unread baseline fills the buffer, so the restart waits in
	// delivery: the consumer can still take the locks and query the fixture.
	control.set(mdsWatchDumpKey, mdsWatchFixtureDump(6, 41, 202))
	control.awaitDumps(t, 2)
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if status, err := fs.MDSStatus(t.Context()); err != nil || status.Active[0].GID != 202 {
		t.Fatalf("consumer query during the watch = %+v, %v", status, err)
	}
	if baseline := nextMDSEvent(t, w); baseline.Kind != ceph.WatchBaseline || baseline.Epoch != 5 || baseline.Status.Active[0] != (MDSStatus{Name: "a", Rank: 0, GID: 101, State: "up:active", Owned: true}) {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextMDSEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Epoch != 6 || changed.MDSMapEpoch != 6 || changed.Status.Active[0].GID != 202 {
		t.Fatalf("changed = %+v", changed)
	}
	// A consumer that holds the topology lock longer than QueryTimeout gets a
	// gap rather than a blocked watch, and the watch resumes after it.
	state.Mutex.Lock()
	gap := nextMDSEvent(t, w)
	state.Mutex.Unlock()
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextMDSEvent(t, w); event.Kind != ceph.WatchResumed; event = nextMDSEvent(t, w) {
		if event.Kind != ceph.WatchGap {
			t.Fatalf("event after the lock was released = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	requireMDSClosed(t, w)
}

// A long setup operation such as ScaleMDS holds the setup gate; the watch
// does not take it, so it keeps reading.
func TestMDSWatchReadsWhileTheSetupGateIsHeld(t *testing.T) {
	fs, control := newMDSWatchFixture()
	gate := &filesystems(fs.base().Cluster()).setupMu
	gate.Lock()
	defer gate.Unlock()
	w, err := fs.WatchMDSs(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	control.awaitDumps(t, 3)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMDSWatchEndsWhenTheFixtureEnds(t *testing.T) {
	cases := map[string]struct {
		end   func(*Filesystem, *mdsWatchControl)
		cause string
		reads bool
	}{
		"terminated cluster": {func(fs *Filesystem, _ *mdsWatchControl) { testhelper.TestSetClosed(fs.base().Cluster(), true) }, "terminated", false},
		"removed filesystem": {func(fs *Filesystem, _ *mdsWatchControl) { delete(filesystems(fs.base().Cluster()).byName, "fixture") }, "was removed", false},
		"recreated under the same name": {func(_ *Filesystem, c *mdsWatchControl) {
			c.native.Output[mdsWatchDumpKey] = mdsWatchFixtureDump(9, 42, 303)
		}, "now has FSMap ID 42", true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			fs, control := newMDSWatchFixture()
			state := testhelper.TestState(fs.base().Cluster())
			w, err := fs.WatchMDSs(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Stop() })
			state.Mutex.Lock()
			control.mu.Lock()
			c.end(fs, control)
			before := control.dumps
			control.mu.Unlock()
			state.Mutex.Unlock()
			awaitMDSDone(t, w)
			if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), c.cause) {
				t.Fatalf("Stop = %v", err)
			}
			if after := control.dumpCount(); (after > before) != c.reads || after > before+1 {
				t.Fatalf("FSMap reads after the end: %d then %d", before, after)
			}
			if rest := requireMDSClosed(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("events = %+v", rest)
			}
		})
	}
}
