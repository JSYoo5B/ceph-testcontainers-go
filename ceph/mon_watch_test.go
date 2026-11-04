package ceph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	daemonWatchFSID      = "9b9b782d-a094-4b2a-b883-87ea6ea6c5f8"
	daemonWatchOtherFSID = "0f0e0d0c-0b0a-4908-8706-050403020100"
)

// daemonWatch is the part of MonitorWatch and ManagerWatch that the shared
// test helpers use.
type daemonWatch[E any] interface {
	Events() <-chan E
	Stop() error
}

func daemonWatchNext[E any](t *testing.T, w daemonWatch[E]) E {
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

// daemonWatchDrain reads the buffered events and fails unless the channel
// closes after them.
func daemonWatchDrain[E any](t *testing.T, w daemonWatch[E]) []E {
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

func daemonWatchEnded(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

// monWatchSource is a scripted monitorQuorumRead and originalFSIDRead: the
// reported quorum, the FSIDs and the failure of the next reads, all changed by
// the test while the watch goroutine reads them.
type monWatchSource struct {
	mu          sync.Mutex
	original    string
	originalErr error
	fsid        string
	sample      monitorSample
	fail        error
	block       bool
	reads       int
}

func monWatchMembers(names ...string) []DaemonAddress {
	members := make([]DaemonAddress, 0, len(names))
	for i, name := range names {
		port := strconv.Itoa(3300 + i)
		members = append(members, DaemonAddress{Name: name, Addrs: []EntityAddress{{Type: "v2", Addr: "172.20.0." + strconv.Itoa(2+i) + ":" + port}}})
	}
	return members
}

func newMonWatchSource() *monWatchSource {
	return &monWatchSource{original: daemonWatchFSID, fsid: daemonWatchFSID, sample: monitorSample{
		monMapEpoch: 3, electionEpoch: 8, leader: "a", monmap: monWatchMembers("a", "b", "c"), quorum: []string{"a", "b", "c"},
	}}
}

func (s *monWatchSource) originalFSID(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.original, s.originalErr
}

func (s *monWatchSource) read(ctx context.Context) (monitorQuorumReport, error) {
	s.mu.Lock()
	s.reads++
	block, fail := s.block, s.fail
	report := monitorQuorumReport{fsid: s.fsid, sample: s.sample}
	report.sample.monmap, report.sample.quorum = cloneDaemonAddresses(s.sample.monmap), slices.Clone(s.sample.quorum)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return monitorQuorumReport{}, ctx.Err()
	}
	if fail != nil {
		return monitorQuorumReport{}, fail
	}
	return report, nil
}

func (s *monWatchSource) update(change func(*monWatchSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
	s.sample.electionEpoch++
}

func (s *monWatchSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *monWatchSource) awaitReads(t *testing.T, n int) {
	t.Helper()
	want := s.readCount() + n
	for deadline := time.Now().Add(5 * time.Second); s.readCount() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

func startTestMonWatch(t *testing.T, ctx context.Context, settings watch.Settings, source *monWatchSource) *MonitorWatch {
	t.Helper()
	w, err := startMonitorWatch(ctx, settings, source.originalFSID, source.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

func TestMonitorWatchBaselineIsBufferedBeforeWatchMonitorsReturns(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), watch.Settings{Interval: time.Hour, Timeout: time.Second, Buffer: 1}, source)
	select {
	case event := <-w.Events():
		if event.Sequence != 1 || event.Kind != WatchBaseline || event.Err != nil || event.MonMapEpoch != 3 || event.ElectionEpoch != 8 || event.Leader != "a" ||
			!reflect.DeepEqual(event.Members, monWatchMembers("a", "b", "c")) || !slices.Equal(event.Quorum, []string{"a", "b", "c"}) {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchMonitors returned before the baseline was buffered")
	}
}

func TestMonitorWatchUnchangedStateProducesNoEvent(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(4), source)
	daemonWatchNext(t, w)
	source.awaitReads(t, 5)
	// Epochs alone are not compared fields.
	source.update(func(s *monWatchSource) { s.sample.monMapEpoch++ })
	source.awaitReads(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged quorum", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged quorum: %+v", rest)
	}
}

func TestMonitorWatchReportsQuorumMembershipAndLeaderChangesInOrder(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(8), source)
	steps := []func(*monWatchSource){
		// c stops: it stays a member and leaves the quorum.
		func(s *monWatchSource) { s.sample.quorum = []string{"a", "b"} },
		// A new election makes b the leader.
		func(s *monWatchSource) { s.sample.leader = "b" },
		// d joins, c is removed.
		func(s *monWatchSource) {
			s.sample.monMapEpoch += 2
			s.sample.monmap = monWatchMembers("a", "b", "d")
			s.sample.quorum = []string{"a", "b", "d"}
		},
		// d moves to another address.
		func(s *monWatchSource) {
			s.sample.monmap[2].Addrs = []EntityAddress{{Type: "v2", Addr: "172.20.0.9:3300"}}
		},
	}
	for _, step := range steps {
		source.update(step)
		source.awaitReads(t, 3)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	events := daemonWatchDrain(t, w)
	if len(events) != 5 {
		t.Fatalf("events = %+v", events)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) || (i > 0 && event.Kind != WatchChanged) {
			t.Fatalf("event %d = %+v", i, event)
		}
	}
	if q := events[1]; len(q.Members) != 3 || q.Members[2].Name != "c" || !slices.Equal(q.Quorum, []string{"a", "b"}) || q.Leader != "a" {
		t.Fatalf("quorum loss of a member = %+v", q)
	}
	if l := events[2]; l.Leader != "b" || l.ElectionEpoch <= events[1].ElectionEpoch {
		t.Fatalf("leader change = %+v", l)
	}
	if m := events[3]; m.Members[2].Name != "d" || m.MonMapEpoch != 5 || !slices.Equal(m.Quorum, []string{"a", "b", "d"}) {
		t.Fatalf("membership change = %+v", m)
	}
	if a := events[4]; a.Members[2].Addrs[0].Addr != "172.20.0.9:3300" {
		t.Fatalf("address change = %+v", a)
	}
}

func TestMonitorWatchFailedBaselineStartsNothing(t *testing.T) {
	boom := errors.New("boom")
	for name, change := range map[string]func(*monWatchSource){
		"failed read":               func(s *monWatchSource) { s.fail = boom },
		"unavailable original FSID": func(s *monWatchSource) { s.originalErr = boom },
		"another cluster":           func(s *monWatchSource) { s.fsid = daemonWatchOtherFSID },
	} {
		t.Run(name, func(t *testing.T) {
			source := newMonWatchSource()
			change(source)
			if w, err := startMonitorWatch(t.Context(), fastWatch(1), source.originalFSID, source.read); w != nil || err == nil {
				t.Fatalf("watch = %v, %v", w, err)
			}
		})
	}
	source := newMonWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := startMonitorWatch(ctx, fastWatch(1), source.originalFSID, source.read); w != nil || !errors.Is(err, context.Canceled) || source.readCount() != 0 {
		t.Fatalf("watch with a canceled context = %v, %v", w, err)
	}
}

func TestMonitorWatchUnreachableQuorumIsAGapThenResumed(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(64), source)
	baseline := daemonWatchNext(t, w)
	// Without a majority the bounded query times out: a gap, not an empty
	// quorum.
	source.update(func(s *monWatchSource) { s.block = true })
	gap := daemonWatchNext(t, w)
	if gap.Kind != WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) || gap.Members != nil || gap.Quorum != nil || gap.Leader != "" || gap.MonMapEpoch != 0 {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *monWatchSource) { s.block = false })
	var resumed MonitorWatchEvent
	for resumed = daemonWatchNext(t, w); resumed.Kind == WatchGap; resumed = daemonWatchNext(t, w) {
	}
	if resumed.Kind != WatchResumed || resumed.Err != nil || !reflect.DeepEqual(resumed.Members, baseline.Members) || !slices.Equal(resumed.Quorum, baseline.Quorum) {
		t.Fatalf("resumed = %+v (an unchanged quorum after a gap is still reported)", resumed)
	}
	source.update(func(s *monWatchSource) { s.sample.quorum = []string{"a", "c"} })
	if changed := daemonWatchNext(t, w); changed.Kind != WatchChanged || changed.Sequence != resumed.Sequence+1 {
		t.Fatalf("changed = %+v", changed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorWatchMalformedFSIDIsAGap(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(64), source)
	daemonWatchNext(t, w)
	source.update(func(s *monWatchSource) { s.fsid = strings.ToUpper(daemonWatchFSID) })
	if gap := daemonWatchNext(t, w); gap.Kind != WatchGap || !strings.Contains(gap.Err.Error(), "not a canonical UUID") {
		t.Fatalf("gap = %+v", gap)
	}
	source.update(func(s *monWatchSource) { s.fsid = daemonWatchFSID })
	for event := daemonWatchNext(t, w); event.Kind != WatchResumed; event = daemonWatchNext(t, w) {
		if event.Kind != WatchGap {
			t.Fatalf("event = %+v", event)
		}
	}
}

func TestMonitorWatchAnotherClusterEndsTheWatch(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(4), source)
	// The same member names and quorum under another FSID are not the
	// watched cluster.
	source.update(func(s *monWatchSource) { s.fsid = daemonWatchOtherFSID })
	daemonWatchEnded(t, w.stream.Done())
	err := w.Stop()
	if err == nil || !strings.Contains(err.Error(), "monmap FSID differs from the original cluster") || strings.Contains(err.Error(), daemonWatchOtherFSID) {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("another cluster produced events: %+v", rest)
	}
}

func TestMonitorWatchTerminationEndsTheWatch(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(4), source)
	source.update(func(s *monWatchSource) { s.fail = errClusterTerminated })
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 {
		t.Fatalf("a terminated cluster produced events: %+v", rest)
	}
}

func TestMonitorWatchStalledConsumerOverflows(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(2), source)
	source.update(func(s *monWatchSource) { s.sample.quorum = []string{"a", "b"} })
	source.awaitReads(t, 2)
	source.update(func(s *monWatchSource) { s.sample.quorum = []string{"a", "b", "c"} })
	daemonWatchEnded(t, w.stream.Done())
	err := w.Stop()
	if !errors.Is(err, ErrWatchOverflow) || !strings.Contains(err.Error(), "watch monitors: event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 2 || rest[1].Sequence != 2 || len(rest[1].Quorum) != 2 {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestMonitorWatchStopAndContextEndDuringQuery(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(1), source)
	source.update(func(s *monWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("second Stop = %v", err)
	}

	source = newMonWatchSource()
	ctx, cancel := context.WithCancel(t.Context())
	w = startTestMonWatch(t, ctx, fastWatch(1), source)
	source.update(func(s *monWatchSource) { s.block = true })
	source.awaitReads(t, 1)
	cancel()
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop after the context ended = %v", err)
	}
	daemonWatchDrain(t, w)
}

func TestMonitorWatchSnapshotsAreIndependent(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(8), source)
	baseline := daemonWatchNext(t, w)
	baseline.Members[0].Name = "edited"
	baseline.Members[1].Addrs[0].Addr = "edited"
	baseline.Quorum[0] = "edited"
	source.awaitReads(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	source.update(func(s *monWatchSource) { s.sample.quorum = []string{"a", "b"} })
	first := daemonWatchNext(t, w)
	source.update(func(s *monWatchSource) { s.sample.quorum = []string{"a", "b", "c"} })
	second := daemonWatchNext(t, w)
	if first.Members[0].Name != "a" || first.Members[1].Addrs[0].Addr != "172.20.0.3:3301" || first.Quorum[0] != "a" || len(first.Quorum) != 2 {
		t.Fatalf("first change = %+v", first)
	}
	if &first.Members[1].Addrs[0] == &second.Members[1].Addrs[0] || &first.Quorum[0] == &second.Quorum[0] || len(second.Quorum) != 3 {
		t.Fatalf("a later poll shares a delivered snapshot: %+v then %+v", first, second)
	}
}

func TestMonitorWatchStopIsSafeConcurrentlyAndOnZeroValue(t *testing.T) {
	source := newMonWatchSource()
	w := startTestMonWatch(t, t.Context(), fastWatch(1), source)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	daemonWatchDrain(t, w)
	var zero *MonitorWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ErrInvalidRequest) || !errors.Is((&MonitorWatch{}).Stop(), ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

func TestMonitorWatchConfigValidation(t *testing.T) {
	got, err := watchSettings("watch monitors", WatchConfig{}, monitorWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	cluster, ctr := newMonWatchCluster()
	observer, _ := Observe(cluster)
	for name, cfg := range map[string]WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := observer.WatchMonitors(t.Context(), cfg); w != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
	}
	if n := ctr.quorumReads(); n != 0 {
		t.Fatalf("an invalid config ran %d queries", n)
	}
	if w, err := (&Observer{}).WatchMonitors(t.Context(), WatchConfig{}); w != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unconstructed observer = %v, %v", w, err)
	}
}

// monWatchControl answers the supervised quorum_status command with its
// current reply and other `ceph` commands from a table.
type monWatchControl struct {
	testcontainers.Container
	mu      sync.Mutex
	quorum  string
	replies map[string]string
	reads   int
}

func (ctr *monWatchControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	if monitorQuorumTestCommand(args) {
		ctr.reads++
		return monitorQuorumTestReader(args, 0, []byte(ctr.quorum))
	}
	return 0, healthDetailsFixtureStream(ctr.replies[strings.Join(args[3:], " ")]), nil
}

func (ctr *monWatchControl) setQuorum(reply string) {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	ctr.quorum = reply
}

func (ctr *monWatchControl) quorumReads() int {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	return ctr.reads
}

func monWatchQuorumJSON(fsid string, epoch int, leader string, quorum ...string) string {
	return `{"election_epoch":` + strconv.Itoa(10+epoch) + `,"quorum_names":["` + strings.Join(quorum, `","`) + `"],"quorum_leader_name":"` + leader + `","quorum_age":` + strconv.Itoa(epoch*7) + `,
"monmap":{"epoch":` + strconv.Itoa(epoch) + `,"fsid":"` + fsid + `","modified":"2026-11-04T00:00:0` + strconv.Itoa(epoch) + `Z","mons":[
{"rank":0,"name":"c","public_addrs":{"addrvec":[{"type":"v2","addr":"172.20.0.4:3300","nonce":0},{"type":"v1","addr":"172.20.0.4:6789","nonce":0}]}},
{"rank":1,"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"172.20.0.2:3300","nonce":0}]}},
{"rank":2,"name":"b","public_addrs":{"addrvec":[{"type":"v2","addr":"172.20.0.3:3300","nonce":0}]}}]}}`
}

// newMonWatchCluster is an Observer cluster with three MONs in quorum and the
// original bootstrap FSID.
func newMonWatchCluster() (*Container, *monWatchControl) {
	ctr := &monWatchControl{quorum: monWatchQuorumJSON(daemonWatchFSID, 1, "a", "c", "a", "b"), replies: map[string]string{"mgr services --format json": `{}`}}
	return &Container{Container: ctr, bootstrap: bootstrapRecord{config: []byte("[global]\nfsid = " + daemonWatchFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}}, ctr
}

func TestDecodeMonitorQuorumReport(t *testing.T) {
	data := []byte(monWatchQuorumJSON(daemonWatchFSID, 4, "b", "c", "b"))
	status, err := decodeMonitorQuorum(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	report, err := decodeMonitorQuorumReport(data, status)
	if err != nil {
		t.Fatal(err)
	}
	want := monitorSample{monMapEpoch: 4, electionEpoch: 14, leader: "b", quorum: []string{"b", "c"}, monmap: []DaemonAddress{
		{Name: "a", Addrs: []EntityAddress{{Type: "v2", Addr: "172.20.0.2:3300"}}},
		{Name: "b", Addrs: []EntityAddress{{Type: "v2", Addr: "172.20.0.3:3300"}}},
		{Name: "c", Addrs: []EntityAddress{{Type: "v2", Addr: "172.20.0.4:3300"}, {Type: "v1", Addr: "172.20.0.4:6789"}}},
	}}
	if report.fsid != daemonWatchFSID || !reflect.DeepEqual(report.sample, want) {
		t.Fatalf("report = %+v", report)
	}
	for name, edit := range map[string]func(string) string{
		"no election epoch": func(s string) string { return strings.Replace(s, `"election_epoch":14,`, "", 1) },
		"no leader":         func(s string) string { return strings.Replace(s, `"quorum_leader_name":"b",`, "", 1) },
		"leader outside quorum": func(s string) string {
			return strings.Replace(s, `"quorum_leader_name":"b"`, `"quorum_leader_name":"a"`, 1)
		},
		"no monmap epoch": func(s string) string { return strings.Replace(s, `"epoch":4,`, "", 1) },
		"no FSID":         func(s string) string { return strings.Replace(s, `"fsid":"`+daemonWatchFSID+`",`, "", 1) },
		"address without a nonce": func(s string) string {
			return strings.Replace(s, `"addr":"172.20.0.2:3300","nonce":0`, `"addr":"172.20.0.2:3300"`, 1)
		},
	} {
		edited := []byte(edit(string(data)))
		status, err := decodeMonitorQuorum(t.Context(), edited)
		if err == nil {
			_, err = decodeMonitorQuorumReport(edited, status)
		}
		if err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

func TestMonitorWatchHoldsNoLockAndReadsWhileTheConsumerHoldsOne(t *testing.T) {
	cluster, ctr := newMonWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchMonitors(t.Context(), WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: time.Second, Buffer: 1})
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
	// The unread baseline fills the buffer, so the change waits in delivery
	// while the consumer queries the fixture under the topology lock.
	ctr.setQuorum(monWatchQuorumJSON(daemonWatchFSID, 1, "a", "a", "b"))
	before := ctr.quorumReads()
	for deadline := time.Now().Add(5 * time.Second); ctr.quorumReads() < before+1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireUnlocked("during delivery")
	if services, err := observer.ManagerServices(t.Context()); err != nil || len(services) != 0 {
		t.Fatalf("consumer query during the watch = %v, %v", services, err)
	}
	if baseline := daemonWatchNext(t, w); baseline.Kind != WatchBaseline || len(baseline.Quorum) != 3 || baseline.MonMapEpoch != 1 {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := daemonWatchNext(t, w); changed.Kind != WatchChanged || !slices.Equal(changed.Quorum, []string{"a", "b"}) || len(changed.Members) != 3 {
		t.Fatalf("changed = %+v", changed)
	}
	// Like QuorumStatus, the watch takes no topology lock: a consumer that
	// holds it, as a long MON operation does, does not stop the observation.
	cluster.mu.Lock()
	ctr.setQuorum(monWatchQuorumJSON(daemonWatchFSID, 2, "a", "a", "b", "c"))
	changed := daemonWatchNext(t, w)
	cluster.mu.Unlock()
	if changed.Kind != WatchChanged || len(changed.Quorum) != 3 || changed.MonMapEpoch != 2 {
		t.Fatalf("change while the consumer holds the topology lock = %+v", changed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	daemonWatchDrain(t, w)
}

func TestMonitorWatchEndsWhenTheClusterTerminates(t *testing.T) {
	cluster, ctr := newMonWatchCluster()
	observer, _ := Observe(cluster)
	w, err := observer.WatchMonitors(t.Context(), WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	// Terminate closes native admission before it waits for the topology
	// lock.
	cluster.native.cancelAll()
	before := ctr.quorumReads()
	daemonWatchEnded(t, w.stream.Done())
	if err := w.Stop(); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("Stop = %v", err)
	}
	if n := ctr.quorumReads(); n > before+1 {
		t.Fatalf("a terminated cluster ran %d more queries", n-before)
	}
	if rest := daemonWatchDrain(t, w); len(rest) != 1 || rest[0].Kind != WatchBaseline {
		t.Fatalf("events = %+v", rest)
	}
}

func TestMonitorWatchPinsTheOriginalBootstrapFSID(t *testing.T) {
	cluster, ctr := newMonWatchCluster()
	ctr.setQuorum(monWatchQuorumJSON(daemonWatchOtherFSID, 1, "a", "a", "b", "c"))
	observer, _ := Observe(cluster)
	if w, err := observer.WatchMonitors(t.Context(), WatchConfig{}); w != nil || err == nil || !strings.Contains(err.Error(), "differs from the original cluster") {
		t.Fatalf("watch of another cluster = %v, %v", w, err)
	}
	cluster, _ = newMonWatchCluster()
	cluster.bootstrap.keyring = nil
	observer, _ = Observe(cluster)
	if w, err := observer.WatchMonitors(t.Context(), WatchConfig{}); w != nil || err == nil {
		t.Fatalf("watch without a confirmed bootstrap identity = %v, %v", w, err)
	}
}
