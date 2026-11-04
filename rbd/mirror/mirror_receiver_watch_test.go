package mirror

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/moby/moby/api/types/container"
)

func fastRBDWatch(buffer int) watch.Settings {
	return watch.Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: buffer}
}

// rbdWatchReads counts the completed reads of a watch under test. A read that
// completes after a change made under the owner gate may have started before
// it, so a test awaits two reads to know that one sampled the change.
type rbdWatchReads struct{ n atomic.Int64 }

func (r *rbdWatchReads) await(t *testing.T, n int) {
	t.Helper()
	want := r.n.Load() + int64(n)
	for deadline := time.Now().Add(5 * time.Second); r.n.Load() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the watch stopped reading")
		}
	}
}

// change edits the scripted native state under the owner gate, which every
// read of the mirror holds while it uses the test doubles.
func (h *receiverHarness) change(edit func()) {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	edit()
}

// elect rewrites every daemon's native report: each reports instances[i],
// daemon leader leads with all running members, and every running member
// follows it.
func (h *receiverHarness) elect(t *testing.T, instances []string, leader int) {
	t.Helper()
	members := slices.Clone(instances)
	slices.Sort(members)
	for i, d := range h.daemons {
		d.status = receiverPatchStatus(t, d.status, func(row map[string]any) {
			row["instance_id"], row["leader_instance_id"], row["leader"] = instances[i], instances[leader], i == leader
			delete(row, "instances")
			if i == leader {
				row["instances"] = members
			}
		})
	}
}

func startTestReceiverWatch(t *testing.T, ctx context.Context, h *receiverHarness, settings watch.Settings, scope *rbdReceiverScope, names ...string) (*ReceiverWatch, *rbdWatchReads) {
	t.Helper()
	reads := &rbdWatchReads{}
	w, err := startReceiverWatch(ctx, settings, h.m.checkWatchOpen, func(ctx context.Context, previous *rbdReceiverWitness) (ReceiverStatus, *rbdReceiverWitness, error) {
		defer reads.n.Add(1)
		return h.m.receiverStatusForScope(ctx, scope, names, previous)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w, reads
}

func nextReceiverEvent(t *testing.T, w *ReceiverWatch) ReceiverWatchEvent {
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
	return ReceiverWatchEvent{}
}

func drainReceiverWatch(t *testing.T, w *ReceiverWatch) []ReceiverWatchEvent {
	t.Helper()
	var rest []ReceiverWatchEvent
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

func awaitRBDWatchDone[E any](t *testing.T, s *watch.Stream[E]) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the watch did not end by itself")
	}
}

func TestRBDReceiverWatchBaselineIsBufferedBeforeReturn(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, err := h.m.WatchReceivers(t.Context(), ceph.WatchConfig{Interval: time.Hour, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	select {
	case event := <-w.Events():
		s := event.Status
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Err != nil || !s.Ready || s.LeaderDaemonName != "a" || s.LeaderInstanceID != "100" || !slices.Equal(s.ExpectedDaemons, []string{"a", "b"}) || !slices.Equal(s.Instances, []string{"100", "101"}) || s.PeerID != receiverPeerUUID {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchReceivers returned before the baseline was buffered")
	}
}

func TestRBDReceiverWatchUnchangedStateProducesNoEvent(t *testing.T) {
	h := newReceiverHarness(t, "ns-a", "ns-b", 2)
	w, reads := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(4), nil)
	nextReceiverEvent(t, w)
	reads.await(t, 5)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for an unchanged cohort", n)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if rest := drainReceiverWatch(t, w); len(rest) != 0 {
		t.Fatalf("events after an unchanged cohort: %+v", rest)
	}
}

// A stopped leader, the survivor's election and the same container's restart
// with a new native instance are each one event of the same pinned cohort.
func TestRBDReceiverWatchReportsElectionAndSameContainerRestart(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, _ := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(8), nil)
	baseline := nextReceiverEvent(t, w)
	h.change(func() {
		h.daemons[0].state.Running, h.daemons[0].state.Status = false, container.StateExited
		h.elect(t, []string{"100", "101"}, 1)
		h.daemons[1].status = receiverPatchStatus(t, h.daemons[1].status, func(row map[string]any) { row["instances"] = []string{"101"} })
	})
	survivor := nextReceiverEvent(t, w)
	if s := survivor.Status; survivor.Kind != ceph.WatchChanged || survivor.Err != nil || s.Ready || s.LeaderDaemonName != "b" || s.Daemons["a"].Running || !s.Daemons["b"].Leader || !slices.Equal(s.ExpectedDaemons, []string{"a", "b"}) || s.DaemonProblems["a"] == "" {
		t.Fatalf("survivor election = %+v", survivor)
	}
	h.change(func() {
		h.daemons[0].state = container.State{Running: true, Status: container.StateRunning, Pid: 77, StartedAt: "2026-10-07T09:05:00Z"}
		h.elect(t, []string{"200", "101"}, 1)
	})
	restarted := nextReceiverEvent(t, w)
	a := restarted.Status.Daemons["a"]
	if restarted.Kind != ceph.WatchChanged || !restarted.Status.Ready || a.InstanceID != "200" || a.ContainerID != baseline.Status.Daemons["a"].ContainerID || a.StartedAt == baseline.Status.Daemons["a"].StartedAt || restarted.Status.LeaderDaemonName != "b" || !slices.Equal(restarted.Status.Instances, []string{"101", "200"}) {
		t.Fatalf("same-container restart = %+v", restarted)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestRBDReceiverWatchKeepsForeignAndConvergingStatesVisible(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, _ := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(8), nil)
	nextReceiverEvent(t, w)
	h.change(func() {
		h.elect(t, []string{"100", "101"}, 0)
		h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["instances"] = []string{"100", "101", "999"} })
	})
	foreign := nextReceiverEvent(t, w)
	if foreign.Kind != ceph.WatchChanged || foreign.Err != nil || foreign.Status.Ready || !slices.Equal(foreign.Status.ForeignInstances, []string{"999"}) {
		t.Fatalf("foreign member = %+v", foreign)
	}
	h.change(func() {
		h.elect(t, []string{"100", "101"}, 0)
		h.daemons[1].status = receiverPatchStatus(t, h.daemons[1].status, func(row map[string]any) { row["leader_instance_id"] = "101" })
	})
	disagreement := nextReceiverEvent(t, w)
	if disagreement.Kind != ceph.WatchChanged || disagreement.Status.Ready || len(disagreement.Status.ForeignInstances) != 0 || disagreement.Status.DaemonProblems["b"] != "receiver-leader-converging" {
		t.Fatalf("leader disagreement = %+v", disagreement)
	}
	h.change(func() {
		h.daemons[1].status = receiverPatchStatus(t, h.daemons[1].status, func(row map[string]any) {
			row["leader_instance_id"] = "100"
			row["namespace_replayers"] = []any{}
		})
	})
	undiscovered := nextReceiverEvent(t, w)
	if undiscovered.Kind != ceph.WatchChanged || undiscovered.Status.Ready || undiscovered.Status.Daemons["b"].NamespaceDiscovered || undiscovered.Status.DaemonProblems["b"] == "" {
		t.Fatalf("undiscovered scope = %+v", undiscovered)
	}
}

func TestRBDReceiverWatchGapKeepsPartialEvidenceThenResumes(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, _ := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(64), nil)
	baseline := nextReceiverEvent(t, w)
	h.change(func() { h.daemons[1].stateErr = errors.New("private docker detail") })
	gap := nextReceiverEvent(t, w)
	if gap.Kind != ceph.WatchGap || gap.Err == nil || gap.Sequence != 2 || gap.Status.Ready || gap.Status.PeerID != receiverPeerUUID || gap.Status.DaemonProblems["b"] == "" || gap.Status.Daemons["b"].Running || !gap.Status.Daemons["a"].Running {
		t.Fatalf("gap = %+v", gap)
	}
	if strings.Contains(gap.Err.Error(), "private") {
		t.Fatalf("gap error leaks native detail: %v", gap.Err)
	}
	h.change(func() { h.daemons[1].stateErr = nil })
	var resumed ReceiverWatchEvent
	for resumed = nextReceiverEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextReceiverEvent(t, w) {
	}
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || !resumed.Status.Ready || resumed.Status.LeaderDaemonName != baseline.Status.LeaderDaemonName {
		t.Fatalf("resumed = %+v (an unchanged cohort after a gap is still reported)", resumed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestRBDReceiverWatchStalledConsumerOverflows(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, reads := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(2), nil)
	h.change(func() { h.elect(t, []string{"100", "101"}, 1) })
	reads.await(t, 2)
	h.change(func() { h.elect(t, []string{"100", "101"}, 0) })
	awaitRBDWatchDone(t, w.stream)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), "event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := drainReceiverWatch(t, w); len(rest) != 2 || rest[1].Status.LeaderDaemonName != "b" {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestRBDReceiverWatchStopAndCancelDuringQuery(t *testing.T) {
	for _, cancelCtx := range []bool{false, true} {
		h := newReceiverHarness(t, "", "", 1)
		ctx, cancel := context.WithCancel(t.Context())
		w, _ := startTestReceiverWatch(t, ctx, h, fastRBDWatch(1), nil)
		blocked := make(chan struct{})
		var once sync.Once
		h.change(func() {
			h.daemons[0].hook = func(ctx context.Context, _ int) {
				once.Do(func() { close(blocked) })
				<-ctx.Done()
			}
		})
		select {
		case <-blocked:
		case <-time.After(5 * time.Second):
			t.Fatal("no blocked read")
		}
		ended := make(chan error, 1)
		if cancelCtx {
			cancel()
			awaitRBDWatchDone(t, w.stream)
			go func() { ended <- w.Stop() }()
		} else {
			go func() { ended <- w.Stop() }()
		}
		select {
		case err := <-ended:
			if cancelCtx && !errors.Is(err, context.Canceled) || !cancelCtx && err != nil {
				t.Fatalf("cancel=%v: Stop = %v", cancelCtx, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not interrupt the query")
		}
		if rest := drainReceiverWatch(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
			t.Fatalf("events = %+v", rest)
		}
		cancel()
	}
}

func TestRBDReceiverWatchSnapshotsAreIndependent(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, reads := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(8), nil)
	baseline := nextReceiverEvent(t, w)
	baseline.Status.ExpectedDaemons[0] = "outside"
	baseline.Status.Instances[0] = "outside"
	baseline.Status.Daemons["a"] = ReceiverDaemonStatus{}
	baseline.Status.DaemonProblems["a"] = "outside"
	reads.await(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	h.change(func() { h.elect(t, []string{"100", "101"}, 1) })
	first := nextReceiverEvent(t, w)
	h.change(func() { h.elect(t, []string{"100", "101"}, 0) })
	second := nextReceiverEvent(t, w)
	if first.Status.ExpectedDaemons[0] != "a" || first.Status.Instances[0] != "100" || !first.Status.Daemons["a"].Running || len(first.Status.DaemonProblems) != 0 || first.Status.LeaderDaemonName != "b" {
		t.Fatalf("first change = %+v", first)
	}
	first.Status.Daemons["b"] = ReceiverDaemonStatus{}
	if !second.Status.Daemons["b"].Running || second.Status.LeaderDaemonName != "a" || &first.Status.Instances[0] == &second.Status.Instances[0] {
		t.Fatalf("a delivered snapshot is shared: %+v then %+v", first, second)
	}
}

func TestRBDReceiverWatchIdentitySubstitutionEndsTheWatch(t *testing.T) {
	cases := map[string]func(*receiverHarness){
		"replaced container ID": func(h *receiverHarness) { h.m.roster.daemons[1].receiverContainerID = strings.Repeat("c", 64) },
		"replaced handle": func(h *receiverHarness) {
			d := &receiverTestDaemon{cid: h.daemons[1].cid, state: h.daemons[1].state, status: h.daemons[1].status}
			h.m.roster.daemons[1].Container, h.m.roster.daemons[1].receiverHandle = d, d
		},
		"added owned daemon": func(h *receiverHarness) {
			d := &receiverTestDaemon{cid: strings.Repeat("c", 64), state: h.daemons[1].state, status: h.daemons[1].status}
			h.m.roster.daemons = append(h.m.roster.daemons, &Daemon{Container: d, DaemonName: "c", ClientName: "client.rbd-mirror.tc-c", receiverHandle: d, receiverContainerID: d.cid, receiverClientName: "client.rbd-mirror.tc-c", receiverDaemonName: "c", receiverStartupConfirmed: true})
		},
		"removed owned daemon": func(h *receiverHarness) { h.m.roster.daemons = h.m.roster.daemons[:1] },
		"new peer generation":  func(h *receiverHarness) { h.m.receiver.generation++ },
		"replaced native peer": func(h *receiverHarness) { h.destination.peers[0].UUID = receiverReplacementUUID },
		"changed cluster FSID": func(h *receiverHarness) { h.source.fsid = receiverReplacementUUID },
		"changed policy": func(h *receiverHarness) {
			policy := h.destination.policies["images"]
			policy.MirrorUUID = receiverReplacementUUID
			h.destination.policies["images"] = policy
		},
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 2)
			w, _ := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(4), nil)
			h.change(func() { drift(h) })
			awaitRBDWatchDone(t, w.stream)
			err := w.Stop()
			if err == nil || !rbdReceiverPermanent(err) || !strings.HasPrefix(err.Error(), "watch RBD receivers: ") || strings.Contains(err.Error(), "private") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := drainReceiverWatch(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("a substituted read produced events: %+v", rest)
			}
		})
	}
}

func TestRBDReceiverWatchExplicitCohortIgnoresOtherOwnedDaemons(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, reads := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(4), nil, "b")
	baseline := nextReceiverEvent(t, w)
	if !slices.Equal(baseline.Status.ExpectedDaemons, []string{"b"}) || len(baseline.Status.Daemons) != 1 {
		t.Fatalf("baseline = %+v", baseline)
	}
	h.change(func() { h.m.roster.daemons = h.m.roster.daemons[1:] })
	reads.await(t, 3)
	if err := w.Stop(); err != nil {
		t.Fatalf("removing a daemon outside the explicit cohort ended the watch: %v", err)
	}
}

func TestRBDReceiverWatchTerminationEndsTheWatch(t *testing.T) {
	for _, target := range []string{"mirror", "source", "destination"} {
		t.Run(target, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			// The change lands well before the first poll, so no read that
			// passed the termination check is still in flight.
			w, _ := startTestReceiverWatch(t, t.Context(), h, watch.Settings{Interval: 50 * time.Millisecond, Timeout: time.Second, Buffer: 4}, nil)
			var before int
			h.change(func() {
				switch target {
				case "mirror":
					h.m.closed = true
				default:
					cluster := h.m.pair.config.Source
					if target == "destination" {
						cluster = h.m.pair.config.Destination
					}
					servicehook.Lock(cluster)
					testhelper.TestSetClosed(cluster, true)
					servicehook.Unlock(cluster)
				}
				before = len(h.source.calls) + len(h.destination.calls) + h.daemons[0].statusCalls
			})
			awaitRBDWatchDone(t, w.stream)
			err := w.Stop()
			if !errors.Is(err, errRBDWatchTerminated) || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), target) {
				t.Fatalf("Stop = %v", err)
			}
			if after := len(h.source.calls) + len(h.destination.calls) + h.daemons[0].statusCalls; after != before {
				t.Fatalf("a terminated fixture ran %d native reads", after-before)
			}
			if rest := drainReceiverWatch(t, w); len(rest) != 1 {
				t.Fatalf("a terminated fixture produced events: %+v", rest)
			}
		})
	}
}

func TestRBDReceiverWatchConfigAndUnstartedRefusals(t *testing.T) {
	got, err := rbdWatchSettings(receiverWatchOperation, ceph.WatchConfig{}, receiverWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		h := newReceiverHarness(t, "", "", 1)
		if w, err := h.m.WatchReceivers(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(h.source.calls)+len(h.destination.calls) != 0 {
			t.Errorf("%s: %v, %v", name, w, err)
		}
		view := &Namespace{owner: h.m, scope: rbdReceiverScope{binding: &rbdNamespaceBindingIdentity{}}}
		if w, err := view.WatchReceivers(t.Context(), cfg); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("namespace %s: %v, %v", name, w, err)
		}
	}
	h := newReceiverHarness(t, "", "", 0)
	if w, err := h.m.WatchReceivers(t.Context(), ceph.WatchConfig{}); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("watch of no daemon = %v, %v", w, err)
	}
	h = newReceiverHarness(t, "", "", 1)
	if w, err := h.m.WatchReceivers(t.Context(), ceph.WatchConfig{}, "missing"); w != nil || !rbdReceiverPermanent(err) {
		t.Fatalf("watch of an unowned name = %v, %v", w, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := h.m.WatchReceivers(ctx, ceph.WatchConfig{}); w != nil || !errors.Is(err, context.Canceled) || len(h.source.calls)+len(h.destination.calls) != 0 {
		t.Fatalf("canceled context = %v, %v", w, err)
	}
	var nilMirror *Mirror
	var nilView *Namespace
	if w, err := nilMirror.WatchReceivers(t.Context(), ceph.WatchConfig{}); w != nil || err == nil {
		t.Fatal("a nil mirror is not refused")
	}
	if w, err := nilView.WatchReceivers(t.Context(), ceph.WatchConfig{}); w != nil || err == nil {
		t.Fatal("a nil namespace is not refused")
	}
	var zero *ReceiverWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&ReceiverWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

// The bound namespace runs the same lifecycle with its own scope and pinned
// selected policies.
func TestRBDReceiverWatchBoundNamespace(t *testing.T) {
	h := newNamespaceBindingHarness(t, 2)
	view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	w, err := view.WatchReceivers(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	baseline := nextReceiverEvent(t, w)
	if baseline.Kind != ceph.WatchBaseline || !baseline.Status.Ready || baseline.Status.SourceNamespace != "ns-c" || baseline.Status.DestinationNamespace != "ns-d" {
		t.Fatalf("baseline = %+v", baseline)
	}
	h.change(func() { h.elect(t, []string{"100", "101"}, 1) })
	if changed := nextReceiverEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.LeaderDaemonName != "b" || changed.Status.DestinationNamespace != "ns-d" {
		t.Fatalf("changed = %+v", changed)
	}
	h.change(func() {
		policy := h.destination.policies["images/ns-d"]
		policy.MirrorUUID = receiverReplacementUUID
		h.destination.policies["images/ns-d"] = policy
	})
	awaitRBDWatchDone(t, w.stream)
	if err := w.Stop(); !rbdReceiverPermanent(err) {
		t.Fatalf("Stop after selected policy drift = %v", err)
	}
	namespaceBindingAssertReadOnly(t, h)
}

func requireRBDWatchUnlocked(t *testing.T, m *Mirror, phase string) {
	t.Helper()
	for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if !m.mu.TryLock() {
			t.Fatalf("the watch kept the owner gate %s", phase)
		}
		for _, d := range m.roster.daemons {
			if !d.mu.TryLock() {
				m.mu.Unlock()
				t.Fatalf("the watch kept a member gate %s", phase)
			}
			d.mu.Unlock()
		}
		for _, cluster := range []*ceph.Container{m.pair.config.Source, m.pair.config.Destination} {
			if !servicehook.TryLock(cluster) {
				m.mu.Unlock()
				t.Fatalf("the watch kept a cluster topology lock %s", phase)
			}
			servicehook.Unlock(cluster)
		}
		m.mu.Unlock()
	}
}

func TestRBDReceiverWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	h := newReceiverHarness(t, "", "", 2)
	w, err := h.m.WatchReceivers(t.Context(), ceph.WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireRBDWatchUnlocked(t, h.m, "between reads")
	// The unread baseline fills the buffer, so the change waits in delivery:
	// the consumer can still take every gate and query the fixture.
	var calls int
	h.change(func() { h.elect(t, []string{"100", "101"}, 1); calls = h.daemons[0].statusCalls })
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		h.m.mu.Lock()
		read := h.daemons[0].statusCalls >= calls+2
		h.m.mu.Unlock()
		if read {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireRBDWatchUnlocked(t, h.m, "during delivery")
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || status.LeaderDaemonName != "b" {
		t.Fatalf("consumer query during the watch = %+v, %v", status, err)
	}
	if baseline := nextReceiverEvent(t, w); baseline.Kind != ceph.WatchBaseline || baseline.Status.LeaderDaemonName != "a" {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextReceiverEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.LeaderDaemonName != "b" {
		t.Fatalf("changed = %+v", changed)
	}
	// A consumer that holds the owner gate longer than QueryTimeout gets a
	// gap rather than a blocked watch, and the watch resumes after it.
	h.m.mu.Lock()
	gap := nextReceiverEvent(t, w)
	h.m.mu.Unlock()
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextReceiverEvent(t, w); event.Kind != ceph.WatchResumed; event = nextReceiverEvent(t, w) {
		if event.Kind != ceph.WatchGap {
			t.Fatalf("event after the gate was released = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	drainReceiverWatch(t, w)
}

func TestRBDReceiverWatchStopIsSafeConcurrently(t *testing.T) {
	h := newReceiverHarness(t, "", "", 1)
	w, _ := startTestReceiverWatch(t, t.Context(), h, fastRBDWatch(1), nil)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := w.Stop(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	drainReceiverWatch(t, w)
}
