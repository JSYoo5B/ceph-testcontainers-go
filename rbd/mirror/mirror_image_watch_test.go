package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/moby/moby/api/types/container"
)

// changeMirror edits the scripted native state under the owner gate, which
// every image read holds while it uses the test doubles.
func changeMirror(m *Mirror, edit func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	edit()
}

func rbdWatchImageStatus(instance, daemon, state, lastUpdate string) string {
	return fmt.Sprintf(`{"name":"volume","global_id":"global-image","state":%q,"description":"replaying, bytes_per_second: %s","last_update":%q,"daemon_service":{"service_id":"22","instance_id":%q,"daemon_id":%q}}`, state, lastUpdate, lastUpdate, instance, daemon)
}

// startTestImageWatch watches the unscoped image "volume" of m. A set block
// makes the next reads wait for their context.
func startTestImageWatch(t *testing.T, ctx context.Context, m *Mirror, settings watch.Settings, block *atomic.Bool) (*ImageWatch, *rbdWatchReads) {
	t.Helper()
	reads := &rbdWatchReads{}
	w, err := startImageWatch(ctx, settings, m.checkWatchOpen, func(ctx context.Context) (ImageStatus, error) {
		defer reads.n.Add(1)
		if block != nil && block.Load() {
			<-ctx.Done()
			return ImageStatus{}, ctx.Err()
		}
		return m.ImageStatus(ctx, "volume")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	return w, reads
}

func nextImageEvent(t *testing.T, w *ImageWatch) ImageWatchEvent {
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
	return ImageWatchEvent{}
}

func drainImageWatch(t *testing.T, w *ImageWatch) []ImageWatchEvent {
	t.Helper()
	var rest []ImageWatchEvent
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

func TestRBDImageWatchBaselineIsBufferedBeforeReturn(t *testing.T) {
	link, _, _, _ := newRBDImageStatusFixture(t, ModeSnapshot, "ns-a", "ns-b")
	w, err := link.WatchImage(t.Context(), ceph.WatchConfig{Interval: time.Hour, Buffer: 1}, "volume")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	select {
	case event := <-w.Events():
		s := event.Status
		if event.Sequence != 1 || event.Kind != ceph.WatchBaseline || event.Err != nil || !s.ReplayReady || s.DaemonName != "a" || s.InstanceID != "123" || s.SourceImageID != "source-local" || s.DestinationImageID != "destination-local" || s.GlobalID != "global-image" || s.SourceNamespace != "ns-a" || s.DestinationNamespace != "ns-b" {
			t.Fatalf("baseline = %+v", event)
		}
		if event.Started.IsZero() || event.Finished.Before(event.Started) {
			t.Fatalf("baseline timing = %v .. %v", event.Started, event.Finished)
		}
	default:
		t.Fatal("WatchImage returned before the baseline was buffered")
	}
}

// Changing last_update and description text alone is no event; the native
// state and assignment are.
func TestRBDImageWatchIgnoresStatusProseAndReportsAssignment(t *testing.T) {
	link, _, destination, daemon := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	w, reads := startTestImageWatch(t, t.Context(), link, fastRBDWatch(8), nil)
	nextImageEvent(t, w)
	for i := range 5 {
		changeMirror(link, func() {
			destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+replaying", fmt.Sprintf("2026-10-07 00:00:%02d", i))
		})
		reads.await(t, 2)
	}
	if n := len(w.Events()); n != 0 {
		t.Fatalf("%d events for status prose churn", n)
	}
	changeMirror(link, func() {
		destination.imageStatus = rbdWatchImageStatus("124", "tc-owned", "up+replaying", "2026-10-07 00:01:00")
		daemon.status = strings.ReplaceAll(daemon.status, `"123"`, `"124"`)
	})
	instance := nextImageEvent(t, w)
	if instance.Kind != ceph.WatchChanged || instance.Err != nil || !instance.Status.ReplayReady || instance.Status.InstanceID != "124" || instance.Status.LastUpdate != "2026-10-07 00:01:00" {
		t.Fatalf("new native instance = %+v", instance)
	}
	other := &rbdImageStatusDaemon{running: true, status: `{"pool_replayers":[{"pool":"images","peer":"peer","state":"running","instance_id":"300"}]}`}
	changeMirror(link, func() {
		link.roster.daemons = append(link.roster.daemons, &Daemon{Container: other, DaemonName: "b", ClientName: "client.rbd-mirror.tc-other"})
		destination.imageStatus = rbdWatchImageStatus("300", "tc-other", "up+replaying", "2026-10-07 00:02:00")
	})
	moved := nextImageEvent(t, w)
	if moved.Kind != ceph.WatchChanged || !moved.Status.ReplayReady || moved.Status.DaemonName != "b" || moved.Status.InstanceID != "300" {
		t.Fatalf("moved assignment = %+v", moved)
	}
	changeMirror(link, func() { other.running = false })
	lost := nextImageEvent(t, w)
	if lost.Kind != ceph.WatchChanged || lost.Err != nil || lost.Status.ReplayReady || lost.Status.DaemonName != "b" || lost.Status.State != "up+replaying" {
		t.Fatalf("lost reporting process = %+v", lost)
	}
	changeMirror(link, func() {
		destination.imageStatus = rbdWatchImageStatus("300", "tc-other", "down+stopped", "2026-10-07 00:03:00")
	})
	if down := nextImageEvent(t, w); down.Kind != ceph.WatchChanged || down.Status.State != "down+stopped" || down.Status.ReplayReady {
		t.Fatalf("native state change = %+v", down)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

// A destination that rbd-mirror created but has not enrolled is a state of
// the baseline; its identity is pinned, and a later disable ends the watch.
func TestRBDImageWatchDestinationEnrollment(t *testing.T) {
	for _, mode := range []Mode{ModeSnapshot, ModeJournal} {
		t.Run(string(mode), func(t *testing.T) {
			link, _, destination, _ := newRBDImageStatusFixture(t, mode, "", "")
			destination.imageInfo = rbdImageEnrolling("destination-local", mode)
			w, _ := startTestImageWatch(t, t.Context(), link, fastRBDWatch(8), nil)
			baseline := nextImageEvent(t, w)
			if baseline.Kind != ceph.WatchBaseline || baseline.Err != nil || baseline.Status.ReplayReady || baseline.Status.DestinationMirrorState != "disabled" || baseline.Status.DestinationImageID != "destination-local" || baseline.Status.SourceImageID != "source-local" {
				t.Fatalf("enrolling baseline = %+v", baseline)
			}
			changeMirror(link, func() {
				destination.imageInfo = rbdImageStatusInfo("destination-local", string(mode), "global-image", false)
			})
			enrolled := nextImageEvent(t, w)
			if enrolled.Kind != ceph.WatchChanged || !enrolled.Status.ReplayReady || enrolled.Status.DestinationMirrorState != "enabled" {
				t.Fatalf("enrolled = %+v", enrolled)
			}
			changeMirror(link, func() { destination.imageInfo = rbdImageEnrolling("destination-local", mode) })
			awaitRBDWatchDone(t, w.stream)
			if err := w.Stop(); !scopedRBDImagePermanent(err) || !strings.Contains(err.Error(), "disabled while watching") {
				t.Fatalf("Stop after a disabled destination = %v", err)
			}
		})
	}
}

func TestRBDImageWatchGapKeepsPartialEvidenceThenResumes(t *testing.T) {
	link, _, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	w, _ := startTestImageWatch(t, t.Context(), link, fastRBDWatch(64), nil)
	baseline := nextImageEvent(t, w)
	changeMirror(link, func() {
		destination.execError = func(args []string) error {
			if len(args) > 1 && args[0] == "rbd" && args[1] == "info" {
				return errors.New("private native detail")
			}
			return nil
		}
	})
	gap := nextImageEvent(t, w)
	if gap.Kind != ceph.WatchGap || gap.Err == nil || gap.Sequence != 2 || gap.Status.ReplayReady || gap.Status.SourceImageID != "source-local" || gap.Status.DestinationImageID != "" {
		t.Fatalf("gap = %+v", gap)
	}
	if strings.Contains(gap.Err.Error(), "private") {
		t.Fatalf("gap error leaks native detail: %v", gap.Err)
	}
	changeMirror(link, func() { destination.execError = nil })
	var resumed ImageWatchEvent
	for resumed = nextImageEvent(t, w); resumed.Kind == ceph.WatchGap; resumed = nextImageEvent(t, w) {
	}
	if resumed.Kind != ceph.WatchResumed || resumed.Err != nil || resumed.Status != baseline.Status {
		t.Fatalf("resumed = %+v (an unchanged pair after a gap is still reported)", resumed)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestRBDImageWatchStalledConsumerOverflows(t *testing.T) {
	link, _, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	w, reads := startTestImageWatch(t, t.Context(), link, fastRBDWatch(2), nil)
	changeMirror(link, func() { destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+syncing", "x") })
	reads.await(t, 2)
	changeMirror(link, func() { destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+replaying", "y") })
	awaitRBDWatchDone(t, w.stream)
	err := w.Stop()
	if !errors.Is(err, ceph.ErrWatchOverflow) || !strings.Contains(err.Error(), "event 3 (changed)") {
		t.Fatalf("Stop = %v", err)
	}
	if rest := drainImageWatch(t, w); len(rest) != 2 || rest[1].Status.State != "up+syncing" {
		t.Fatalf("the buffered events before the overflow = %+v", rest)
	}
}

func TestRBDImageWatchStopAndCancelDuringQuery(t *testing.T) {
	for _, cancelCtx := range []bool{false, true} {
		link, _, _, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
		ctx, cancel := context.WithCancel(t.Context())
		var block atomic.Bool
		w, reads := startTestImageWatch(t, ctx, link, fastRBDWatch(1), &block)
		block.Store(true)
		reads.await(t, 1) // a read that started after the block is now waiting
		ended := make(chan error, 1)
		if cancelCtx {
			cancel()
			awaitRBDWatchDone(t, w.stream)
		}
		go func() { ended <- w.Stop() }()
		select {
		case err := <-ended:
			if cancelCtx && !errors.Is(err, context.Canceled) || !cancelCtx && err != nil {
				t.Fatalf("cancel=%v: Stop = %v", cancelCtx, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not interrupt the query")
		}
		if rest := drainImageWatch(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
			t.Fatalf("events = %+v", rest)
		}
		cancel()
	}
}

func TestRBDImageWatchSnapshotsAreIndependent(t *testing.T) {
	link, _, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	w, reads := startTestImageWatch(t, t.Context(), link, fastRBDWatch(8), nil)
	baseline := nextImageEvent(t, w)
	baseline.Status.SourceImageID, baseline.Status.ReplayReady, baseline.Status.State = "outside", false, "outside"
	reads.await(t, 3)
	if n := len(w.Events()); n != 0 {
		t.Fatalf("a consumer edit produced %d events", n)
	}
	changeMirror(link, func() { destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+syncing", "x") })
	first := nextImageEvent(t, w)
	first.Status.State = "edited"
	changeMirror(link, func() { destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+replaying", "y") })
	second := nextImageEvent(t, w)
	if first.Status.SourceImageID != "source-local" || second.Status.State != "up+replaying" || !second.Status.ReplayReady || second.Status.SourceImageID != "source-local" {
		t.Fatalf("a delivered snapshot is shared: %+v then %+v", first, second)
	}
}

func TestRBDImageWatchIdentitySubstitutionEndsTheWatch(t *testing.T) {
	cases := map[string]func(source, destination *rbdImageStatusFixture){
		"source recreated": func(source, _ *rbdImageStatusFixture) {
			source.imageInfo = rbdImageStatusInfo("source-other", "snapshot", "global-image", true)
		},
		"destination recreated": func(_, destination *rbdImageStatusFixture) {
			destination.imageInfo = rbdImageStatusInfo("destination-other", "snapshot", "global-image", false)
		},
		"global ID replaced": func(source, destination *rbdImageStatusFixture) {
			source.imageInfo = rbdImageStatusInfo("source-local", "snapshot", "global-other", true)
			destination.imageInfo = rbdImageStatusInfo("destination-local", "snapshot", "global-other", false)
			destination.imageStatus = strings.ReplaceAll(destination.imageStatus, "global-image", "global-other")
		},
		"source policy replaced": func(source, _ *rbdImageStatusFixture) {
			policy := source.policies["images"]
			policy.MirrorUUID = testMirrorUUID("outside-replacement")
			source.policies["images"] = policy
		},
		"destination pool replaced": func(_, destination *rbdImageStatusFixture) { destination.poolID++ },
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
			w, _ := startTestImageWatch(t, t.Context(), link, fastRBDWatch(4), nil)
			changeMirror(link, func() { drift(source, destination) })
			awaitRBDWatchDone(t, w.stream)
			err := w.Stop()
			if err == nil || !scopedRBDImagePermanent(err) || !strings.HasPrefix(err.Error(), "watch RBD mirror image: ") {
				t.Fatalf("Stop = %v", err)
			}
			if rest := drainImageWatch(t, w); len(rest) != 1 || rest[0].Kind != ceph.WatchBaseline {
				t.Fatalf("a substituted read produced events: %+v", rest)
			}
		})
	}
}

func TestRBDImageWatchTerminationEndsTheWatch(t *testing.T) {
	for _, target := range []string{"mirror", "source", "destination"} {
		t.Run(target, func(t *testing.T) {
			link, source, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
			w, _ := startTestImageWatch(t, t.Context(), link, watch.Settings{Interval: 50 * time.Millisecond, Timeout: time.Second, Buffer: 4}, nil)
			var before int
			changeMirror(link, func() {
				switch target {
				case "mirror":
					link.closed = true
				default:
					cluster := link.pair.config.Source
					if target == "destination" {
						cluster = link.pair.config.Destination
					}
					servicehook.Lock(cluster)
					testhelper.TestSetClosed(cluster, true)
					servicehook.Unlock(cluster)
				}
				before = len(source.imageCalls) + len(destination.imageCalls)
			})
			awaitRBDWatchDone(t, w.stream)
			err := w.Stop()
			if !errors.Is(err, errRBDWatchTerminated) || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), target) {
				t.Fatalf("Stop = %v", err)
			}
			if after := len(source.imageCalls) + len(destination.imageCalls); after != before {
				t.Fatalf("a terminated fixture ran %d image reads", after-before)
			}
			if rest := drainImageWatch(t, w); len(rest) != 1 {
				t.Fatalf("a terminated fixture produced events: %+v", rest)
			}
		})
	}
}

func TestRBDImageWatchConfigAndUnstartedRefusals(t *testing.T) {
	got, err := rbdWatchSettings(imageWatchOperation, ceph.WatchConfig{}, imageWatchDefaultInterval)
	if err != nil || got != (watch.Settings{Interval: 5 * time.Second, Timeout: 30 * time.Second, Buffer: 16}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	link, source, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	for name, cfg := range map[string]ceph.WatchConfig{
		"negative interval": {Interval: -1},
		"negative timeout":  {QueryTimeout: -1},
		"negative buffer":   {Buffer: -1},
		"short interval":    {Interval: 99 * time.Millisecond},
		"large buffer":      {Buffer: 1025},
	} {
		if w, err := link.WatchImage(t.Context(), cfg, "volume"); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v, %v", name, w, err)
		}
		view := &Namespace{owner: link, scope: rbdReceiverScope{binding: &rbdNamespaceBindingIdentity{}}}
		if w, err := view.WatchImage(t.Context(), cfg, "volume"); w != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("namespace %s: %v, %v", name, w, err)
		}
	}
	for _, name := range []string{"", "pool/volume", "volume@snap", "-volume"} {
		if w, err := link.WatchImage(t.Context(), ceph.WatchConfig{}, name); w != nil || !scopedRBDImagePermanent(err) {
			t.Errorf("image name %q: %v, %v", name, w, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if w, err := link.WatchImage(ctx, ceph.WatchConfig{}, "volume"); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context = %v, %v", w, err)
	}
	if len(source.imageCalls)+len(destination.imageCalls) != 0 {
		t.Fatal("a refused watch read the image")
	}
	changeMirror(link, func() {
		destination.execError = func(args []string) error {
			if len(args) > 1 && args[1] == "info" {
				return errors.New("absent")
			}
			return nil
		}
	})
	if w, err := link.WatchImage(t.Context(), ceph.WatchConfig{}, "volume"); w != nil || err == nil {
		t.Fatalf("a baseline without a destination = %v, %v", w, err)
	}
	var nilMirror *Mirror
	var nilView *Namespace
	if w, err := nilMirror.WatchImage(t.Context(), ceph.WatchConfig{}, "volume"); w != nil || err == nil {
		t.Fatal("a nil mirror is not refused")
	}
	if w, err := nilView.WatchImage(t.Context(), ceph.WatchConfig{}, "volume"); w != nil || err == nil {
		t.Fatal("a nil namespace is not refused")
	}
	var zero *ImageWatch
	if zero.Events() != nil || !errors.Is(zero.Stop(), ceph.ErrInvalidRequest) || !errors.Is((&ImageWatch{}).Stop(), ceph.ErrInvalidRequest) {
		t.Fatal("an unstarted watch is not refused")
	}
}

func TestRBDImageWatchHoldsNoLockBetweenReadsOrDuringDelivery(t *testing.T) {
	link, _, destination, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
	w, err := link.WatchImage(t.Context(), ceph.WatchConfig{Interval: 500 * time.Millisecond, QueryTimeout: 50 * time.Millisecond, Buffer: 1}, "volume")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop() })
	requireRBDWatchUnlocked(t, link, "between reads")
	var calls int
	changeMirror(link, func() {
		destination.imageStatus = rbdWatchImageStatus("123", "tc-owned", "up+syncing", "x")
		calls = len(destination.imageCalls)
	})
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		link.mu.Lock()
		read := len(destination.imageCalls) >= calls+3
		link.mu.Unlock()
		if read {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no second read")
		}
	}
	time.Sleep(20 * time.Millisecond)
	requireRBDWatchUnlocked(t, link, "during delivery")
	if status, err := link.ImageStatus(t.Context(), "volume"); err != nil || status.State != "up+syncing" {
		t.Fatalf("consumer query during the watch = %+v, %v", status, err)
	}
	if baseline := nextImageEvent(t, w); baseline.Kind != ceph.WatchBaseline || baseline.Status.State != "up+replaying" {
		t.Fatalf("baseline = %+v", baseline)
	}
	if changed := nextImageEvent(t, w); changed.Kind != ceph.WatchChanged || changed.Status.State != "up+syncing" {
		t.Fatalf("changed = %+v", changed)
	}
	link.mu.Lock()
	gap := nextImageEvent(t, w)
	link.mu.Unlock()
	if gap.Kind != ceph.WatchGap || !errors.Is(gap.Err, context.DeadlineExceeded) {
		t.Fatalf("gap = %+v", gap)
	}
	for event := nextImageEvent(t, w); event.Kind != ceph.WatchResumed; event = nextImageEvent(t, w) {
		if event.Kind != ceph.WatchGap {
			t.Fatalf("event after the gate was released = %+v", event)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	drainImageWatch(t, w)
}

// The bound namespace runs the same lifecycle with the cohort pinned at the
// baseline: an original daemon's restart is an event, a replacement ends it.
func TestRBDImageWatchBoundNamespace(t *testing.T) {
	for _, change := range []string{"restart", "replacement", "mode"} {
		t.Run(change, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			w, err := view.WatchImage(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 4}, "volume")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Stop() })
			baseline := nextImageEvent(t, w)
			if baseline.Kind != ceph.WatchBaseline || !baseline.Status.ReplayReady || baseline.Status.DaemonName != "a" || baseline.Status.InstanceID != "100" || baseline.Status.DestinationNamespace != "ns-d" {
				t.Fatalf("baseline = %+v", baseline)
			}
			h.change(func() {
				switch change {
				case "restart":
					h.daemons[0].state = container.State{Running: true, Status: container.StateRunning, Pid: 52, StartedAt: "2026-10-07T11:00:00Z"}
					h.daemons[0].status = strings.ReplaceAll(h.daemons[0].status, `"100"`, `"200"`)
					h.statuses["images/ns-d/volume"] = strings.ReplaceAll(h.statuses["images/ns-d/volume"], `"instance_id":"100"`, `"instance_id":"200"`)
				case "replacement":
					h.m.roster.daemons = newReceiverHarness(t, "ns-a", "ns-b", 1).m.roster.daemons
				case "mode":
					h.setImages([2]string{"ns-c", "ns-d"}, ModeJournal, "1")
				}
			})
			if change == "restart" {
				restarted := nextImageEvent(t, w)
				if restarted.Kind != ceph.WatchChanged || !restarted.Status.ReplayReady || restarted.Status.InstanceID != "200" || restarted.Status.DaemonName != "a" {
					t.Fatalf("same-container restart = %+v", restarted)
				}
				if err := w.Stop(); err != nil {
					t.Fatal(err)
				}
			} else {
				awaitRBDWatchDone(t, w.stream)
				if err := w.Stop(); !scopedRBDImagePermanent(err) {
					t.Fatalf("Stop after %s = %v", change, err)
				}
				if rest := drainImageWatch(t, w); len(rest) != 0 {
					t.Fatalf("a substituted read produced events: %+v", rest)
				}
			}
			namespaceImageReadOnly(t, h)
		})
	}
}
