package finalize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// trackedDebugWindow registers a fake window with a fixed clock, so that two
// windows on the same daemon, option and generation have identical receipts
// apart from their registration order.
func trackedDebugWindow(fx *finalizationFixture, level string) *fakeDebugWindow {
	w := newFakeDebugWindow(level)
	fx.TrackDebugLevel(w)
	return w
}

func restoreStep(fx *finalizationFixture, w *fakeDebugWindow, name string) {
	fx.Cleanup(name, w.restore)
}

// debugWindowProblems is the oracle of the controls: it lists every rule
// that the manifest's debug windows break, given the wanted registrations.
type wantedDebugWindow struct {
	archiveState, finalState override.DebugLevelState
	absent                   bool
}

func debugWindowProblems(m finalizationManifest, want []wantedDebugWindow) []string {
	var problems []string
	if len(m.DebugWindows) != len(want) {
		return []string{fmt.Sprintf("%d windows, want %d", len(m.DebugWindows), len(want))}
	}
	for i, w := range want {
		got := m.DebugWindows[i]
		if got.Sequence != i+1 {
			problems = append(problems, fmt.Sprintf("window %d has sequence %d", i+1, got.Sequence))
		}
		for _, c := range []struct {
			kind  string
			snap  *finalizationDebugSnapshot
			state override.DebugLevelState
		}{{"archive", got.Archive, w.archiveState}, {"final", got.Final, w.finalState}} {
			switch {
			case c.snap == nil:
				if c.state != "" {
					problems = append(problems, fmt.Sprintf("window %d has no %s copy", i+1, c.kind))
				}
			case w.absent && (c.snap.Present || c.snap.Receipt != nil):
				problems = append(problems, fmt.Sprintf("window %d %s copy of a missing handle is present", i+1, c.kind))
			case !w.absent && (!c.snap.Present || c.snap.Receipt == nil || c.snap.Receipt.State != c.state):
				problems = append(problems, fmt.Sprintf("window %d %s copy is not %s: %+v", i+1, c.kind, c.state, c.snap))
			}
		}
	}
	return problems
}

func TestFinalizationSnapshotsDebugWindowsAroundCleanup(t *testing.T) {
	t.Run("workload failure then successful restore", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		h := trackedDebugWindow(fx, "20")
		restoreStep(fx, h, "restore debug_osd")
		fx.tb.Errorf("workload failed")
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		if problems := debugWindowProblems(manifest, []wantedDebugWindow{{override.DebugLevelApplied, override.DebugLevelRestored, false}}); len(problems) != 0 {
			t.Fatalf("%v", problems)
		}
		w := manifest.DebugWindows[0]
		if w.Archive.Phase != "before-cleanup" || w.Final.Phase != "after-cleanup" || manifest.Phase != "before-cleanup" {
			t.Fatalf("phases %q %q manifest %q", w.Archive.Phase, w.Final.Phase, manifest.Phase)
		}
		before, after := w.Archive.Receipt, w.Final.Receipt
		if before.Applied != "20/20" || before.Restored != "" || before.Prior != "1/5" || len(before.Transitions) != 3 {
			t.Fatalf("pre-restore copy %+v", before)
		}
		if after.Restored != "1/5" || after.Applied != "20/20" || len(after.Transitions) <= len(before.Transitions) || h.Receipt().State != override.DebugLevelRestored {
			t.Fatalf("final copy %+v", after)
		}
		// The copy only reads local state: the cleanup's restore is the only
		// one, and taking the snapshots restored nothing.
		if h.restoreCalls() != 1 {
			t.Fatalf("%d restores", h.restoreCalls())
		}
	})
	t.Run("restoration as the first failure", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		h := trackedDebugWindow(fx, "20")
		restoreStep(fx, h, "restore debug_osd")
		h.paused = true
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		if problems := debugWindowProblems(manifest, []wantedDebugWindow{{override.DebugLevelUnconfirmed, override.DebugLevelUnconfirmed, false}}); len(problems) != 0 {
			t.Fatalf("%v", problems)
		}
		w := manifest.DebugWindows[0]
		if manifest.Phase != "after-cleanup" || w.Archive.Phase != "after-cleanup" || w.Final.Receipt.Restored != "" || h.Receipt().Restored != "" {
			t.Fatalf("manifest %q window %+v", manifest.Phase, w)
		}
	})
	t.Run("two sequential windows on one daemon option and generation", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		first := trackedDebugWindow(fx, "20")
		if err := first.restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		second := trackedDebugWindow(fx, "20")
		restoreStep(fx, second, "restore second")
		fx.tb.Errorf("workload failed")
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		want := []wantedDebugWindow{{override.DebugLevelRestored, override.DebugLevelRestored, false}, {override.DebugLevelApplied, override.DebugLevelRestored, false}}
		if problems := debugWindowProblems(manifest, want); len(problems) != 0 {
			t.Fatalf("%v", problems)
		}
		a, b := manifest.DebugWindows[0].Final.Receipt, manifest.DebugWindows[1].Final.Receipt
		if a.Daemon != b.Daemon || a.Name != b.Name || a.Generation != b.Generation || a.State != b.State || !a.Transitions[0].At.Equal(b.Transitions[0].At) {
			t.Fatalf("the windows are not the same target and state: %+v %+v", a, b)
		}
		if manifest.DebugWindows[0].Sequence == manifest.DebugWindows[1].Sequence {
			t.Fatal("sequential windows share an identity")
		}
	})
	t.Run("missing and apply-error handles", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		uncertain := newUncertainFakeDebugWindow(errors.New("reply lost"))
		fx.TrackDebugLevel(nil)
		fx.TrackDebugLevel(&fakeDebugWindow{})
		fx.TrackDebugLevel(uncertain)
		fx.tb.Errorf("workload failed")
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		want := []wantedDebugWindow{{absent: true}, {absent: true}, {override.DebugLevelApplyUncertain, override.DebugLevelApplyUncertain, false}}
		want[0].archiveState, want[0].finalState = "-", "-"
		want[1].archiveState, want[1].finalState = "-", "-"
		if problems := debugWindowProblems(manifest, want); len(problems) != 0 {
			t.Fatalf("%v", problems)
		}
		if got := manifest.DebugWindows[2].Final.Receipt.Transitions[2].Error; !strings.Contains(got, "reply lost") {
			t.Fatalf("apply error %q", got)
		}
	})
	t.Run("passing run archives nothing and registration is local", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		h := trackedDebugWindow(fx, "20")
		restoreStep(fx, h, "restore")
		fx.run()
		if len(fx.archives(t)) != 0 || h.Receipt().Restored != "1/5" {
			t.Fatalf("archives %v receipt %+v", fx.archives(t), h.Receipt())
		}
	})
	t.Run("no registration leaves an empty list", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.tb.Errorf("workload failed")
		fx.run()
		data, err := os.ReadFile(filepath.Join(fx.archives(t)[0], "manifest.json"))
		if err != nil || !strings.Contains(string(data), `"debug_windows": []`) {
			t.Fatalf("%s %v", data, err)
		}
	})
	t.Run("late registration fails the test", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		fx.Check("late", func(context.Context) error { fx.TrackDebugLevel(nil); return nil })
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		if len(manifest.DebugWindows) != 0 || len(fx.tb.errors) == 0 || !strings.Contains(fx.tb.errors[0], "after finalization started") {
			t.Fatalf("windows %v errors %v", manifest.DebugWindows, fx.tb.errors)
		}
	})
}

func TestFinalizationDebugWindowBoundsAndRedaction(t *testing.T) {
	t.Run("transitions are bounded and keep the last", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		h := trackedDebugWindow(fx, "20")
		h.paused = true
		for range 60 {
			_ = h.restore(t.Context())
		}
		total := len(h.Receipt().Transitions)
		fx.tb.Errorf("workload failed")
		fx.run()
		receipt := readManifest(t, fx.archives(t)[0]).DebugWindows[0].Archive.Receipt
		if len(receipt.Transitions) != maxFinalizationDebugTransitions || receipt.TransitionsOmitted != total-maxFinalizationDebugTransitions ||
			receipt.Transitions[0].State != override.DebugLevelPriorRead || receipt.State != override.DebugLevelUnconfirmed {
			t.Fatalf("kept %d omitted %d of %d", len(receipt.Transitions), receipt.TransitionsOmitted, total)
		}
	})
	t.Run("registrations are bounded", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		for range maxFinalizationDebugWindows + 3 {
			fx.TrackDebugLevel(nil)
		}
		fx.tb.Errorf("workload failed")
		fx.run()
		manifest := readManifest(t, fx.archives(t)[0])
		if len(manifest.DebugWindows) != maxFinalizationDebugWindows || manifest.DebugWindowsOmitted != 3 {
			t.Fatalf("kept %d omitted %d", len(manifest.DebugWindows), manifest.DebugWindowsOmitted)
		}
	})
	t.Run("errors are redacted and clipped", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true, Diagnostics: diagnostics.Config{RedactValues: []string{"s3cr3t"}}})
		h := newUncertainFakeDebugWindow(errors.New("token s3cr3t rejected " + strings.Repeat("x", 4*maxFinalizationDebugError)))
		fx.TrackDebugLevel(h)
		fx.tb.Errorf("workload failed")
		fx.run()
		data, err := os.ReadFile(filepath.Join(fx.archives(t)[0], "manifest.json"))
		if err != nil || strings.Contains(string(data), "s3cr3t") {
			t.Fatalf("secret kept: %v", err)
		}
		manifest := readManifest(t, fx.archives(t)[0])
		message := manifest.DebugWindows[0].Archive.Receipt.Transitions[2].Error
		if len(message) > maxFinalizationDebugError+len(" [truncated]") || !strings.HasSuffix(message, " [truncated]") {
			t.Fatalf("error of %d bytes", len(message))
		}
	})
}

// TestFinalizationDebugWindowNegativeControls feeds the oracle manifests
// that break one rule each, and rebuilds the broken designs the issue warns
// about from the same inputs.
func TestFinalizationDebugWindowNegativeControls(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	first := trackedDebugWindow(fx, "20")
	restoreStep(fx, first, "restore first")
	fx.TrackDebugLevel(nil)
	fx.tb.Errorf("workload failed")
	fx.run()
	good := readManifest(t, fx.archives(t)[0])
	want := []wantedDebugWindow{{override.DebugLevelApplied, override.DebugLevelRestored, false}, {absent: true, archiveState: "-", finalState: "-"}}
	if problems := debugWindowProblems(good, want); len(problems) != 0 {
		t.Fatalf("the good manifest breaks a rule: %v", problems)
	}
	clone := func() finalizationManifest {
		m := good
		m.DebugWindows = slices.Clone(good.DebugWindows)
		for i := range m.DebugWindows {
			w := m.DebugWindows[i]
			if w.Archive != nil {
				c := *w.Archive
				w.Archive = &c
			}
			if w.Final != nil {
				c := *w.Final
				w.Final = &c
			}
			m.DebugWindows[i] = w
		}
		return m
	}
	mutations := map[string]func(*finalizationManifest){
		"aliased: the archive copy follows the final state": func(m *finalizationManifest) { m.DebugWindows[0].Archive = m.DebugWindows[0].Final },
		"the final copy is dropped":                         func(m *finalizationManifest) { m.DebugWindows[0].Final = nil },
		"the archive copy is dropped":                       func(m *finalizationManifest) { m.DebugWindows[0].Archive = nil },
		"a missing handle is recorded as restored": func(m *finalizationManifest) {
			r := *m.DebugWindows[0].Final.Receipt
			m.DebugWindows[1].Final = &finalizationDebugSnapshot{Phase: "after-cleanup", Present: true, Receipt: &r}
		},
		"success is inferred from a cleanup success": func(m *finalizationManifest) {
			m.DebugWindows[0].Archive.Receipt.State = override.DebugLevelRestored
		},
		"sequential windows are merged by target": func(m *finalizationManifest) { m.DebugWindows = m.DebugWindows[:1] },
		"the sequence is not the registration order": func(m *finalizationManifest) {
			m.DebugWindows[1].Sequence = 1
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			m := clone()
			mutate(&m)
			if problems := debugWindowProblems(m, want); len(problems) == 0 {
				t.Fatal("the oracle accepted the broken manifest")
			}
		})
	}
	t.Run("a map keyed by target and time keeps one of two sequential windows", func(t *testing.T) {
		fx2 := newFinalizationFixture(t, Config{})
		a := trackedDebugWindow(fx2, "20")
		if err := a.restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		b := trackedDebugWindow(fx2, "20")
		if err := b.restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		byTarget := map[string]override.DebugLevelReceipt{}
		for _, h := range []*fakeDebugWindow{a, b} {
			r := h.Receipt()
			byTarget[r.Daemon+"/"+r.Name+"/"+r.Generation+"/"+r.Transitions[0].At.Format(time.RFC3339Nano)] = r
		}
		if len(byTarget) != 1 {
			t.Fatalf("the fixed clock should collapse the broken key, got %d", len(byTarget))
		}
		if len(fx2.debugWindows) != 2 {
			t.Fatalf("the registration list has %d entries", len(fx2.debugWindows))
		}
	})
}

// TestFinalizationRestoreOrder pins the order in which Finalization runs a
// window's restore among the other cleanups and the failure archive.
func TestFinalizationRestoreOrder(t *testing.T) {
	t.Run("known failure is archived before restoration", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		w := trackedDebugWindow(fx, "20")
		fx.Cleanup("remove client", func(context.Context) error { fx.event("remove client"); return nil })
		fx.Cleanup("restore debug_osd", func(ctx context.Context) error {
			fx.event("restore at " + w.Receipt().Applied)
			return w.restore(ctx)
		})
		fx.tb.Errorf("workload failed")
		fx.run()
		want := []string{"diagnostics", "crash", "restore at 20/20", "remove client", "terminate"}
		if events := debugLevelFinalizationEvents(fx); !slices.Equal(events, want) {
			t.Fatalf("events %v, want %v", events, want)
		}
		manifest := readManifest(t, fx.archives(t)[0])
		if manifest.Phase != "before-cleanup" || manifest.Cleanups[0].Name != "restore debug_osd" || manifest.Cleanups[0].Error != "" || w.Receipt().State != override.DebugLevelRestored {
			t.Fatalf("manifest %+v receipt %+v", manifest.Cleanups, w.Receipt())
		}
	})
	t.Run("restoration as the first failure", func(t *testing.T) {
		fx := newFinalizationFixture(t, Config{Archive: true})
		w := trackedDebugWindow(fx, "20")
		fx.Cleanup("remove client", func(context.Context) error { fx.event("remove client"); return nil })
		fx.Cleanup("restore debug_osd", func(ctx context.Context) error {
			fx.event("restore")
			return w.restore(ctx)
		})
		w.paused = true
		fx.run()
		want := []string{"restore", "remove client", "diagnostics", "crash", "terminate"}
		if events := debugLevelFinalizationEvents(fx); !slices.Equal(events, want) {
			t.Fatalf("events %v, want %v", events, want)
		}
		manifest := readManifest(t, fx.archives(t)[0])
		if manifest.Phase != "after-cleanup" || !manifest.Trigger.CleanupFailed || !strings.Contains(manifest.Cleanups[0].Error, "paused") ||
			manifest.Termination == nil || !manifest.Final || w.Receipt().State != override.DebugLevelUnconfirmed {
			t.Fatalf("manifest %+v", manifest)
		}
	})
}

// debugLevelFinalizationEvents merges the two collectors' events, which run
// concurrently, into a fixed order.
func debugLevelFinalizationEvents(fx *finalizationFixture) []string {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	events := slices.Clone(fx.events)
	for i := 0; i+1 < len(events); i++ {
		if events[i] == "crash" && events[i+1] == "diagnostics" {
			events[i], events[i+1] = events[i+1], events[i]
		}
	}
	return events
}
