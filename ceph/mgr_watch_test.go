package ceph

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

type mgrStandby = struct {
	Name string `json:"name"`
	GID  uint64 `json:"gid"`
}

// mgrWatchSource is a scripted managerMapRead and originalFSIDRead.
type mgrWatchSource struct {
	mu            sync.Mutex
	before, after string
	sample        managerSample
	fail          error
	block         bool
	reads         int
}

func newMgrWatchSource() *mgrWatchSource {
	return &mgrWatchSource{before: daemonWatchFSID, after: daemonWatchFSID, sample: managerSample{
		epoch:    20,
		status:   ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 4100, Standbys: []mgrStandby{{Name: "b", GID: 4200}}},
		services: map[string]string{"prometheus": "http://172.20.0.5:9283/"},
	}}
}

func (s *mgrWatchSource) originalFSID(context.Context) (string, error) { return daemonWatchFSID, nil }

func (s *mgrWatchSource) read(ctx context.Context) (managerMapReport, error) {
	s.mu.Lock()
	s.reads++
	block, fail := s.block, s.fail
	report := managerMapReport{before: s.before, after: s.after, sample: s.sample}
	report.sample.status.Standbys = slices.Clone(s.sample.status.Standbys)
	report.sample.services = maps.Clone(s.sample.services)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return managerMapReport{}, ctx.Err()
	}
	if fail != nil {
		return managerMapReport{}, fail
	}
	return report, nil
}

func (s *mgrWatchSource) update(change func(*mgrWatchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
	s.sample.epoch++
}

func (s *mgrWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *mgrWatchSource) awaitReads(t *testing.T, n int) {
	t.Helper()
	want := s.readCount() + n
	for deadline := time.Now().Add(5 * time.Second); s.readCount() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

func startTestMgrWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *mgrWatchSource) *ManagerWatch {
	t.Helper()
	w, err := startManagerWatch(ctx, settings, source.originalFSID, source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func TestManagerWatchBaselineIsBufferedBeforeWatchManagersReturns(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != WatchBaseline || event.Err != nil || event.Epoch != 20 ||
			!reflect.DeepEqual(event.Status, source.sample.status) || !maps.Equal(event.Services, source.sample.services) {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchManagers returned before the baseline was buffered")
	}
}

func TestManagerWatchUnchangedStateProducesNoEvent(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(4), source)
	daemonWatchNext(t, w)
	source.awaitReads(t, 5)
	// An epoch change alone is not a change of the compared fields.
	source.update(func(*mgrWatchSource) {})
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged map", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged map: %+v", rest)
	}
}

func TestManagerWatchReportsAFailoverInOrder(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(8), source)
	steps := []func(*mgrWatchSource){
		// The active manager is dropped: no active manager, no services.
		func(s *mgrWatchSource) {
			s.sample.status = ManagerStatus{Standbys: []mgrStandby{{Name: "b", GID: 4200}}}
			s.sample.services = map[string]string{}
		},
		// b is promoted but not yet available.
		func(s *mgrWatchSource) { s.sample.status = ManagerStatus{ActiveName: "b", ActiveGID: 4200} },
		// b becomes available and advertises its service.
		func(s *mgrWatchSource) {
			s.sample.status.Available = true
			s.sample.services = map[string]string{"prometheus": "http://172.20.0.6:9283/"}
		},
		// a restarts as a standby with a new GID.
		func(s *mgrWatchSource) { s.sample.status.Standbys = []mgrStandby{{Name: "a", GID: 4300}} },
		// The same name registers again with another GID.
		func(s *mgrWatchSource) { s.sample.status.Standbys = []mgrStandby{{Name: "a", GID: 4400}} },
	}
	for _, step := range steps {
		source.update(step)
		source.awaitReads(t, 3)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := daemonWatchDrain(t, w)
	if len(events) != 6 {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) || (i > 0 && (event.Kind != WatchChanged || event.Epoch <= events[i-1].Epoch)) {
			t.Fatalf("event %d = %+v", i, event)
		}
	}
	if e := events[1]; e.Status.Available || e.Status.ActiveName != "" || e.Status.ActiveGID != 0 || len(e.Services) != 0 || e.Err != nil {
		t.Fatalf("a map without an active manager is a successful observation: %+v", e)
	}
	if e := events[2]; e.Status.Available || e.Status.ActiveName != "b" || e.Status.ActiveGID != 4200 {
		t.Fatalf("promotion = %+v", e)
	}
	if e := events[3]; !e.Status.Available || e.Services["prometheus"] != "http://172.20.0.6:9283/" {
		t.Fatalf("availability and service = %+v", e)
	}
	if e := events[5]; e.Status.Standbys[0] != (mgrStandby{Name: "a", GID: 4400}) || events[4].Status.Standbys[0].GID != 4300 {
		t.Fatalf("same name with a new GID = %+v after %+v", e, events[4])
	}
}

func TestManagerWatchFailedBaselineStartsNothing(t *testing.T) {
	boom := errors.New("boom")
	for name, change := range map[string]func(*mgrWatchSource){
		"failed read":                 func(s *mgrWatchSource) { s.fail = boom },
		"another cluster before":      func(s *mgrWatchSource) { s.before = daemonWatchOtherFSID },
		"another cluster after":       func(s *mgrWatchSource) { s.after = daemonWatchOtherFSID },
		"terminated cluster":          func(s *mgrWatchSource) { s.fail = errClusterTerminated },
		"malformed FSID in the read":  func(s *mgrWatchSource) { s.after = "not-a-uuid" },
		"nil FSID in the read":        func(s *mgrWatchSource) { s.before = "00000000-0000-0000-0000-000000000000" },
		"upper-case FSID in the read": func(s *mgrWatchSource) { s.before = strings.ToUpper(daemonWatchFSID) },
	} {
		t.Run(name, func(t *testing.T) {
			source := newMgrWatchSource()
			change(source)
			if w, err := startManagerWatch(t.Context(), fastWatch(1), source.originalFSID, source.read); w != nil || err == nil {
				t.Fatalf("watch = %v, %v", w, err)
			}
		})
	}
}

func TestManagerWatchTransientFailureIsAGapThenResumed(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(64), source)
	baseline := daemonWatchNext(t, w)
	boom := errors.New("transient")
	source.update(func(s *mgrWatchSource) { s.fail = boom })
	gap := daemonWatchNext(t, w)
	if gap.Kind != WatchGap || !errors.Is(gap.Err, boom) || gap.Services != nil || gap.Status.ActiveName != "" || gap.Epoch != 0 || gap.Sequence != 2 {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *mgrWatchSource) { s.fail = nil })
	var resumed ManagerWatchEvent
	for resumed = daemonWatchNext(t, w); resumed.Kind == WatchGap; resumed = daemonWatchNext(t, w) {
	}
	if resumed.Kind != WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Status, baseline.Status) {
		t.Fatalf("resumed = %+v (an unchanged map after a gap is still reported)", resumed)
	}
	source.update(func(s *mgrWatchSource) { s.sample.status.Standbys = nil })
	if changed := daemonWatchNext(t, w); changed.Kind != WatchChanged || changed.Sequence != resumed.Sequence+1 || len(changed.Status.Standbys) != 0 {
		t.Fatalf("changed = %+v", changed)
	}
}

func TestManagerWatchAnotherClusterEndsTheWatch(t *testing.T) {
	for name, change := range map[string]func(*mgrWatchSource){
		"before the dump": func(s *mgrWatchSource) { s.before = daemonWatchOtherFSID },
		"after the dump":  func(s *mgrWatchSource) { s.after = daemonWatchOtherFSID },
	} {
		t.Run(name, func(t *testing.T) {
			source := newMgrWatchSource()
			w := startTestMgrWatch(t, t.Context(), fastWatch(4), source)
			source.update(change)
			daemonWatchEnded(t, w.stream.Done())
			if err := w.Stop(); err == nil || !strings.Contains(err.Error(), "watch managers: the native FSID differs from the original cluster") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
				t.Fatalf("another cluster produced events: %+v", rest)
			}
		})
	}
}

func TestManagerWatchTerminationEndsTheWatch(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(4), source)
	source.update(func(s *mgrWatchSource) { s.fail = errClusterTerminated })
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 {
		t.Fatalf("a terminated cluster produced events: %+v", rest)
	}
}

func TestManagerWatchStalledConsumerOverflows(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(2), source)
	source.update(func(s *mgrWatchSource) { s.sample.status.Available = false })
	source.awaitReads(t, 2)
	source.update(func(s *mgrWatchSource) { s.sample.status.Available = true })
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, ErrWatchOverflow) || !strings.Contains(err.Error(), "watch managers: event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 2 || rest[1].Status.Available {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestManagerWatchStopAndContextEndDuringQuery(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(1), source)
	source.update(func(s *mgrWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}

	source = newMgrWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestMgrWatch(t, ctx, fastWatch(1), source)
	source.update(func(s *mgrWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	daemonWatchDrain(t, w)
	var zero *ManagerWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ErrInvalidRequest) || !errors.Is((&ManagerWatch{}).Stop(), ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

func TestManagerWatchSnapshotsAreIndependent(t *testing.T) {
	source := newMgrWatchSource()
	w := startTestMgrWatch(t, t.Context(), fastWatch(8), source)
	baseline := daemonWatchNext(t, w)
	baseline.Status.Standbys[0].GID = 1
	baseline.Services["prometheus"] = "edited"
	baseline.Services["extra"] = "edited"
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.update(func(s *mgrWatchSource) { s.sample.status.Available = false })
	first := daemonWatchNext(t, w)
	source.update(func(s *mgrWatchSource) { s.sample.status.Available = true })
	second := daemonWatchNext(t, w)
	if first.Status.Standbys[0].GID != 4200 || first.Services["prometheus"] != "http://172.20.0.5:9283/" || len(first.Services) != 1 {
		t.Fatalf("first change = %+v", first)
	}
	first.Services["prometheus"] = "edited again"
	if &first.Status.Standbys[0] == &second.Status.Standbys[0] || second.Services["prometheus"] != "http://172.20.0.5:9283/" {
		t.Fatalf("a later poll shares a delivered snapshot: %+v then %+v", first, second)
	}
}

func TestManagerWatchConfigValidation(t *testing.T) {
	got, err := watchSettings("watch managers", WatchConfig{}, managerWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 3 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	cluster, ctr := newMgrWatchCluster()
	observer, _ := Observe(cluster)
	for name, cfg := range map[string]WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := observer.WatchManagers(t.Context(), cfg); w != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("an invalid config ran commands: %q", got)
	}
	if w, err := (&Observer{}).WatchManagers(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unconstructed observer = %v, %v", w, err)
	}
}

const mgrWatchDumpKey = "mgr dump --format json"

func mgrWatchDump(epoch int, active string, gid int, available bool, services string, standbys ...string) string {
	entries := make([]string, 0, len(standbys))
	for i, name := range standbys {
		entries = append(entries, `{"name":"`+name+`","gid":`+strconv.Itoa(5000+i)+`,"available_modules":[]}`)
	}
	return `{"epoch":` + strconv.Itoa(epoch) + `,"active_gid":` + strconv.Itoa(gid) + `,"active_name":"` + active + `","active_change":"2026-11-04T00:00:0` + strconv.Itoa(epoch%10) + `Z","available":` + strconv.FormatBool(available) +
		`,"standbys":[` + strings.Join(entries, ",") + `],"modules":["iostat"],"services":` + services + `}`
}

// newMgrWatchCluster is an Observer cluster whose MGRMap has active a and
// standby b, with the original bootstrap FSID.
func newMgrWatchCluster() (*Container, *observerControl) {
	cluster, ctr := newObserverCluster()
	cluster.bootstrap = bootstrapRecord{config: []byte("[global]\nfsid = " + daemonWatchFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	ctr.replies["fsid"] = daemonWatchFSID + "\n"
	ctr.replies[mgrWatchDumpKey] = mgrWatchDump(1, "a", 4100, true, `{}`, "b")
	ctr.replies["mgr services --format json"] = `{}`
	return cluster, ctr
}

func TestDecodeManagerMap(t *testing.T) {
	sample, err := decodeManagerMap([]byte(mgrWatchDump(7, "a", 4100, true, `{"prometheus":"http://172.20.0.5:9283/"}`, "d", "b", "c")))
	if err != nil {
		t.Fatal(err)
	}
	want := managerSample{epoch: 7, status: ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 4100,
		Standbys: []mgrStandby{{Name: "b", GID: 5001}, {Name: "c", GID: 5002}, {Name: "d", GID: 5000}}},
		services: map[string]string{"prometheus": "http://172.20.0.5:9283/"}}
	if !reflect.DeepEqual(sample, want) {
		t.Fatalf("sample = %+v", sample)
	}
	if sample, err := decodeManagerMap([]byte(mgrWatchDump(8, "", 0, false, `{}`))); err != nil || sample.status.ActiveName != "" || sample.status.Standbys == nil || len(sample.services) != 0 {
		t.Fatalf("a map without managers = %+v, %v", sample, err)
	}
	valid := mgrWatchDump(7, "a", 4100, true, `{}`, "b")
	for name, data := range map[string]string{
		"no epoch":          strings.Replace(valid, `"epoch":7,`, "", 1),
		"no availability":   strings.Replace(valid, `,"available":true`, "", 1),
		"no active GID":     strings.Replace(valid, `"active_gid":4100,`, "", 1),
		"no standbys":       strings.Replace(valid, `"standbys":[{"name":"b","gid":5000,"available_modules":[]}],`, "", 1),
		"no services":       strings.Replace(valid, `,"services":{}`, "", 1),
		"name without GID":  mgrWatchDump(7, "a", 0, true, `{}`),
		"unnamed standby":   strings.Replace(valid, `"name":"b"`, `"name":""`, 1),
		"invalid URL":       mgrWatchDump(7, "a", 4100, true, `{"prometheus":"not a url"}`),
		"not a JSON object": `[]`,
	} {
		if _, err := decodeManagerMap([]byte(data)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

func TestManagerWatchReadsBracketTheDumpWithTheFSID(t *testing.T) {
	cluster, ctr := newMgrWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchManagers(t.Context(), WatchConfig{Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()
	if got := ctr.commands(); !slices.Equal(got, []string{"fsid", mgrWatchDumpKey, "fsid"}) {
		t.Fatalf("baseline commands = %q", got)
	}
	baseline := daemonWatchNext(t, w)
	if baseline.Epoch != 1 || baseline.Status.ActiveName != "a" || baseline.Status.Standbys[0].Name != "b" || baseline.Services == nil {
		t.Fatalf("baseline = %+v", baseline)
	}
}

func TestManagerWatchHoldsNoLockAndReadsWhileTheConsumerHoldsOne(t *testing.T) {
	cluster, ctr := newMgrWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchManagers(t.Context(), WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: time.Second, Buffer: 1})
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
			if !cluster.bootstrap.mu.TryLock() {
				t.Fatalf("the watch kept the bootstrap record lock %s", phase)
			}
			cluster.bootstrap.mu.Unlock()
		}
	}
	requireUnlocked("between reads")
	ctr.set(mgrWatchDumpKey, mgrWatchDump(2, "b", 5000, false, `{}`))
	for deadline := time.Now().Add(5 * time.Second); len(ctr.commands()) < 6; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if services, err := observer.ManagerServices(t.Context()); err != nil || len(services) != 0 {
		t.Fatalf("consumer query during the watch = %v, %v", services, err)
	}
	if baseline := daemonWatchNext(t, w); baseline.Kind != WatchBaseline || baseline.Status.ActiveName != "a" {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := daemonWatchNext(t, w); changed.Kind != WatchChanged || changed.Status.ActiveName != "b" || changed.Status.Available || len(changed.Status.Standbys) != 0 {
		t.Fatalf("changed = %+v", changed)
	}
	// Like ManagerStatus, the watch takes no topology lock.
	cluster.mu.Lock()
	ctr.set(mgrWatchDumpKey, mgrWatchDump(3, "b", 5000, true, `{"prometheus":"http://172.20.0.6:9283/"}`))
	changed := daemonWatchNext(t, w)
	cluster.mu.Unlock()
	if changed.Kind != WatchChanged || !changed.Status.Available || changed.Services["prometheus"] == "" {
		t.Fatalf("change while the consumer holds the topology lock = %+v", changed)
	}
	// A failed command is a gap, then the watch resumes.
	ctr.mu.Lock()
	ctr.err = errors.New("exec failed")
	ctr.mu.Unlock()
	if gap := daemonWatchNext(t, w); gap.Kind != WatchGap || gap.Err == nil {
		t.Fatalf("gap = %+v", gap)
	}
	ctr.mu.Lock()
	ctr.err = nil
	ctr.mu.Unlock()
	for event := daemonWatchNext(t, w); event.Kind != WatchResumed; event = daemonWatchNext(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("event after the failure = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	daemonWatchDrain(t, w)
}

func TestManagerWatchEndsWhenTheClusterTerminates(t *testing.T) {
	cluster, ctr := newMgrWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchManagers(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	cluster.native.cancelAll()
	before := len(ctr.commands())
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("Stop = %v", err)
	}
	if got := ctr.commands(); len(got) > before+3 {
		t.Fatalf("a terminated cluster ran commands: %q", got[before:])
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
}

func TestManagerWatchPinsTheOriginalBootstrapFSID(t *testing.T) {
	cluster, ctr := newMgrWatchCluster()
	ctr.replies["fsid"] = daemonWatchOtherFSID + "\n"
	observer, _ := Observe(cluster)
	if w, err := observer.WatchManagers(t.Context(), WatchConfig{}); w != nil || err == nil || !strings.Contains(err.Error(), "differs from the original cluster") {
		t.Fatalf("watch of another cluster = %v, %v", w, err)
	}
}
