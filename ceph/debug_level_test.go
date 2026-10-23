package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// debugLevelFake is one daemon generation's runtime level. A set stores the
// normalized value as Ceph does, unless a fault says otherwise.
type debugLevelFake struct {
	mu     sync.Mutex
	value  string
	gen    debugLevelGeneration
	genErr error
	// setFaults are consumed in order by the sets: applies says whether the
	// value changes before err is returned.
	setFaults []debugLevelSetFault
	// getErrs are consumed in order by the gets; nil reads normally.
	getErrs []error
	// afterSet runs after a set, for example to edit or restart the target,
	// and afterGet after a read.
	afterSet, afterGet func(*debugLevelFake)
	// live holds the values of sets that may still run; settleErrs are
	// consumed in order by their settling, and a nil one stops them.
	live       []string
	settleErrs []error
	sets       []string
	gets       int
}

type debugLevelSetFault struct {
	applies bool
	// live leaves the set running unconfirmed: it applies only on complete.
	live bool
	err  error
}

func newDebugLevelFake(value string) *debugLevelFake {
	return &debugLevelFake{value: value, gen: debugLevelGeneration{owned: true, identity: "osd0@start-1", running: true}}
}

func (f *debugLevelFake) generation(ctx context.Context) (debugLevelGeneration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return debugLevelGeneration{}, err
	}
	return f.gen, f.genErr
}

func (f *debugLevelFake) get(ctx context.Context, name string) (string, error) {
	value, err := f.read(ctx)
	if f.afterGet != nil {
		f.afterGet(f)
	}
	return value, err
}

func (f *debugLevelFake) read(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(f.getErrs) > 0 {
		err := f.getErrs[0]
		f.getErrs = f.getErrs[1:]
		if err != nil {
			return "", err
		}
	}
	return f.value, nil
}

// set admits the set like osdDebugTarget: only on the expected generation.
func (f *debugLevelFake) set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	f.mu.Lock()
	if changed, err := checkDebugLevelGeneration(f.gen, f.genErr, identity, "osd.0"); err != nil {
		f.mu.Unlock()
		return nil, &debugLevelRefusal{changed: changed, err: err}
	}
	f.sets = append(f.sets, name+"="+value)
	fault := debugLevelSetFault{applies: true}
	if len(f.setFaults) > 0 {
		fault, f.setFaults = f.setFaults[0], f.setFaults[1:]
	}
	var settle func(context.Context) error
	switch {
	case fault.live:
		f.live = append(f.live, value)
		settle = f.settle
	case fault.applies:
		f.value, _ = normalizeDebugLevel(value)
	}
	after := f.afterSet
	f.mu.Unlock()
	if after != nil {
		after(f)
	}
	return settle, fault.err
}

func (f *debugLevelFake) settle(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.settleErrs) > 0 {
		err := f.settleErrs[0]
		f.settleErrs = f.settleErrs[1:]
		if err != nil {
			return err
		}
	}
	f.live = nil
	return nil
}

// complete lets the sets that still run apply, as a delayed command does.
func (f *debugLevelFake) complete() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, value := range f.live {
		f.value, _ = normalizeDebugLevel(value)
	}
	f.live = nil
}

func (f *debugLevelFake) edit(edit func(*debugLevelFake)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	edit(f)
}

func (f *debugLevelFake) setCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sets)
}

func (f *debugLevelFake) current() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.value
}

func openDebugLevelFake(ctx context.Context, registry *debugLevelRegistry, fake *debugLevelFake, level string) (*DebugLevelOverrideHandle, error) {
	setting, owned, err := normalizeDebugLevelSetting(DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: level})
	if err != nil {
		return nil, err
	}
	return openDebugLevel(ctx, registry, fake, setting, owned, time.Second, func() time.Time { return time.Unix(0, 0).UTC() })
}

func debugLevelStates(h *DebugLevelOverrideHandle) []DebugLevelState {
	var states []DebugLevelState
	for _, transition := range h.Receipt().Transitions {
		states = append(states, transition.State)
	}
	return states
}

func wantDebugLevelStates(t *testing.T, h *DebugLevelOverrideHandle, want ...DebugLevelState) {
	t.Helper()
	if got := debugLevelStates(h); !slices.Equal(got, want) {
		t.Fatalf("transitions %v, want %v", got, want)
	}
	if want != nil && h.Receipt().State != want[len(want)-1] {
		t.Fatalf("state %s, want %s", h.Receipt().State, want[len(want)-1])
	}
}

func TestDebugLevelNormalization(t *testing.T) {
	for level, want := range map[string]string{"20": "20/20", "1/5": "1/5", "0": "0/0", "05/07": "5/7", "99/0": "99/0"} {
		if got, ok := normalizeDebugLevel(level); !ok || got != want {
			t.Errorf("%q normalizes to %q %v, want %q", level, got, ok, want)
		}
	}
	for _, level := range []string{"", "a", "100", "1/100", "-1", "1/", "/1", "1/2/3", " 1", "1 "} {
		if got, ok := normalizeDebugLevel(level); ok {
			t.Errorf("%q accepted as %q", level, got)
		}
	}
	if !sameDebugLevel("20", "20/20") || sameDebugLevel("20", "20/5") || !sameDebugLevel("odd", "odd") || sameDebugLevel("odd", "1/1") {
		t.Fatal("readback comparison")
	}
	for _, name := range []string{"debug_mon", "debug-osd", "DEBUG_OSD", ""} {
		if _, _, err := normalizeDebugLevelSetting(DebugLevelSetting{Daemon: "osd.0", Name: name, Level: "1"}); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("option %q: %v", name, err)
		}
	}
}

func TestDebugLevelDecodesConfigGet(t *testing.T) {
	if value, err := decodeDebugLevel([]byte(`{"debug_osd": "1/5"}`), "debug_osd"); err != nil || value != "1/5" {
		t.Fatalf("%q %v", value, err)
	}
	for _, out := range []string{``, `{}`, `{"debug_ms": "1/5"}`, `{"debug_osd": ""}`, `["1/5"]`} {
		if value, err := decodeDebugLevel([]byte(out), "debug_osd"); err == nil {
			t.Errorf("%q decoded as %q", out, value)
		}
	}
}

func TestDebugLevelAppliesNormalizedLevelAndRestoresPrior(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	receipt := h.Receipt()
	if receipt.Prior != "1/5" || receipt.Requested != "20" || receipt.Applied != "20/20" || receipt.Generation != "osd0@start-1" || fake.current() != "20/20" {
		t.Fatalf("receipt %+v value %q", receipt, fake.current())
	}
	wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplied)
	if err := h.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplied, DebugLevelRestoreAttempted, DebugLevelRestored)
	if fake.current() != "1/5" || h.Receipt().Restored != "1/5" || !slices.Equal(fake.setCalls(), []string{"debug_osd=20", "debug_osd=1/5"}) {
		t.Fatalf("value %q sets %v", fake.current(), fake.setCalls())
	}
	// A second Restore is idempotent and issues nothing.
	if err := h.Restore(t.Context()); err != nil || len(fake.setCalls()) != 2 {
		t.Fatalf("second restore: %v %v", err, fake.setCalls())
	}
	if !strings.Contains(h.String(), "osd.0 debug_osd") || strings.Contains(h.GoString(), "1/5") {
		t.Fatalf("String %q", h.String())
	}
}

func TestDebugLevelApplyFailuresKeepRestorationState(t *testing.T) {
	lost := errors.New("reply lost")
	for name, test := range map[string]struct {
		fault       debugLevelSetFault
		getErrs     []error
		readback    string
		restoreSets int
	}{
		// The set applied, but its reply was lost.
		"lost reply": {debugLevelSetFault{applies: true, err: lost}, nil, "20/20", 1},
		// The daemon refused the set: Restore finds the prior level.
		"refused": {debugLevelSetFault{applies: false, err: lost}, nil, "1/5", 0},
		// Nothing is known about the set: Restore reconciles first.
		"unknown": {debugLevelSetFault{applies: true, err: lost}, []error{nil, errors.New("socket gone")}, "", 1},
		// The set reported success, but the readback failed.
		"unread success": {debugLevelSetFault{applies: true}, []error{nil, errors.New("socket gone")}, "", 1},
	} {
		t.Run(name, func(t *testing.T) {
			var registry debugLevelRegistry
			fake := newDebugLevelFake("1/5")
			fake.setFaults = []debugLevelSetFault{test.fault}
			fake.getErrs = test.getErrs
			h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
			if h == nil || err == nil {
				t.Fatalf("handle %v err %v", h, err)
			}
			wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplyUncertain)
			if h.Receipt().Applied != test.readback {
				t.Fatalf("applied readback %q, want %q", h.Receipt().Applied, test.readback)
			}
			// The admission is kept until the window is settled.
			if _, err := openDebugLevelFake(t.Context(), &registry, newDebugLevelFake("1/5"), "5"); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("overlapping window admitted: %v", err)
			}
			if err := h.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if fake.current() != "1/5" || len(fake.setCalls()) != 1+test.restoreSets || h.Receipt().State != DebugLevelRestored {
				t.Fatalf("value %q sets %v state %s", fake.current(), fake.setCalls(), h.Receipt().State)
			}
			if _, err := openDebugLevelFake(t.Context(), &registry, newDebugLevelFake("1/5"), "5"); err != nil {
				t.Fatalf("admission not released: %v", err)
			}
		})
	}
}

func TestDebugLevelRefusesBeforeSetting(t *testing.T) {
	for name, edit := range map[string]func(*debugLevelFake){
		"stopped":     func(f *debugLevelFake) { f.gen.running = false },
		"paused":      func(f *debugLevelFake) { f.gen.paused = true },
		"not owned":   func(f *debugLevelFake) { f.gen.owned = false },
		"no identity": func(f *debugLevelFake) { f.gen.identity = "" },
		"inspect":     func(f *debugLevelFake) { f.genErr = errors.New("inspect failed") },
		"prior read":  func(f *debugLevelFake) { f.getErrs = []error{errors.New("socket gone")} },
	} {
		t.Run(name, func(t *testing.T) {
			var registry debugLevelRegistry
			fake := newDebugLevelFake("1/5")
			fake.edit(edit)
			if h, err := openDebugLevelFake(t.Context(), &registry, fake, "20"); h != nil || err == nil || len(fake.setCalls()) != 0 {
				t.Fatalf("handle %v err %v sets %v", h, err, fake.setCalls())
			}
			if len(registry.active) != 0 {
				t.Fatal("a refused window kept its admission")
			}
		})
	}
}

func TestDebugLevelRejectsDuplicateWindow(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	other := newDebugLevelFake("1/5")
	if second, err := openDebugLevelFake(t.Context(), &registry, other, "10"); second != nil || !errors.Is(err, ErrInvalidRequest) || other.gets != 0 {
		t.Fatalf("duplicate window: %v %v gets=%d", second, err, other.gets)
	}
	// Another option of the same daemon is a separate window.
	setting := DebugLevelSetting{Daemon: "osd.0", Name: "debug_ms", Level: "1"}
	if ms, err := openDebugLevel(t.Context(), &registry, newDebugLevelFake("0/5"), setting, "1/1", time.Second, time.Now); err != nil || ms == nil {
		t.Fatalf("debug_ms window: %v", err)
	}
	if err := h.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if again, err := openDebugLevelFake(t.Context(), &registry, other, "10"); err != nil || again == nil {
		t.Fatalf("window after restoration: %v", err)
	}
	// The restored handle does not touch the new window's level.
	if err := h.Restore(t.Context()); err != nil || other.current() != "10/10" {
		t.Fatalf("old handle: %v value %q", err, other.current())
	}
}

func TestDebugLevelRestorePreservesOutsideEdits(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	fake.edit(func(f *debugLevelFake) { f.value = "5/5" })
	err = h.Restore(t.Context())
	if err == nil || !strings.Contains(err.Error(), "outside") || fake.current() != "5/5" || len(fake.setCalls()) != 1 {
		t.Fatalf("restore over an outside edit: %v value %q sets %v", err, fake.current(), fake.setCalls())
	}
	wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplied, DebugLevelRestoreAttempted, DebugLevelConflict)
	// Conflict is terminal: the same error, no further reads or sets.
	gets := fake.gets
	if again := h.Restore(t.Context()); again == nil || again.Error() != err.Error() || fake.gets != gets {
		t.Fatalf("second restore: %v", again)
	}
	if len(registry.active) != 0 {
		t.Fatal("a conflicted window kept its admission")
	}
}

func TestDebugLevelRestoreAcceptsAlreadyRestoredLevel(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	fake.edit(func(f *debugLevelFake) { f.value = "1/5" })
	if err := h.Restore(t.Context()); err != nil || len(fake.setCalls()) != 1 || h.Receipt().State != DebugLevelRestored {
		t.Fatalf("already restored: %v sets %v", err, fake.setCalls())
	}
}

func TestDebugLevelRestoreRefusesChangedGenerations(t *testing.T) {
	for name, edit := range map[string]func(*debugLevelFake){
		"stopped":    func(f *debugLevelFake) { f.gen.running = false },
		"restarted":  func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" },
		"replaced":   func(f *debugLevelFake) { f.gen.identity = "osd0-new@start-1" },
		"removed":    func(f *debugLevelFake) { f.gen = debugLevelGeneration{} },
		"terminated": func(f *debugLevelFake) { f.gen.owned = false },
	} {
		t.Run(name, func(t *testing.T) {
			var registry debugLevelRegistry
			fake := newDebugLevelFake("1/5")
			h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
			if err != nil {
				t.Fatal(err)
			}
			fake.edit(edit)
			gets := fake.gets
			err = h.Restore(t.Context())
			if err == nil || len(fake.setCalls()) != 1 || fake.gets != gets || h.Receipt().State != DebugLevelTargetChanged {
				t.Fatalf("restore: %v sets %v gets %d->%d state %s", err, fake.setCalls(), gets, fake.gets, h.Receipt().State)
			}
			if len(registry.active) != 0 {
				t.Fatal("a changed target kept its admission")
			}
		})
	}
}

func TestDebugLevelApplyDuringGenerationChange(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	fake.afterSet = func(f *debugLevelFake) { f.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" }) }
	h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
	if h == nil || err == nil || h.Receipt().State != DebugLevelTargetChanged {
		t.Fatalf("handle %v err %v", h, err)
	}
	fake.afterSet = nil
	if again := h.Restore(t.Context()); again == nil || len(fake.setCalls()) != 1 {
		t.Fatalf("restore of a changed target: %v sets %v", again, fake.setCalls())
	}
}

func TestDebugLevelRestoreRetriesUnconfirmedTargets(t *testing.T) {
	for name, test := range map[string]struct {
		edit, fix func(*debugLevelFake)
	}{
		"paused":         {func(f *debugLevelFake) { f.gen.paused = true }, func(f *debugLevelFake) { f.gen.paused = false }},
		"socket":         {func(f *debugLevelFake) { f.getErrs = []error{errors.New("socket gone")} }, func(*debugLevelFake) {}},
		"inspect failed": {func(f *debugLevelFake) { f.genErr = errors.New("inspect failed") }, func(f *debugLevelFake) { f.genErr = nil }},
	} {
		t.Run(name, func(t *testing.T) {
			var registry debugLevelRegistry
			fake := newDebugLevelFake("1/5")
			h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
			if err != nil {
				t.Fatal(err)
			}
			fake.edit(test.edit)
			if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || len(fake.setCalls()) != 1 || fake.current() != "20/20" {
				t.Fatalf("restore: %v state %s sets %v", err, h.Receipt().State, fake.setCalls())
			}
			if len(registry.active) != 1 {
				t.Fatal("an unconfirmed window lost its admission")
			}
			fake.edit(test.fix)
			if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" {
				t.Fatalf("retry: %v value %q", err, fake.current())
			}
		})
	}
}

func TestDebugLevelRestoreReplies(t *testing.T) {
	lost := errors.New("reply lost")
	t.Run("applied with lost reply", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) { f.setFaults = []debugLevelSetFault{{applies: true, err: lost}} })
		if err := h.Restore(t.Context()); err != nil || h.Receipt().State != DebugLevelRestored {
			t.Fatalf("restore: %v", err)
		}
		last := h.Receipt().Transitions[len(h.Receipt().Transitions)-1]
		if !strings.Contains(last.Error, "reply lost") || last.Readback != "1/5" {
			t.Fatalf("restored transition %+v", last)
		}
	})
	t.Run("lost reply and readback", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) {
			f.setFaults = []debugLevelSetFault{{applies: true, err: lost}}
			f.getErrs = []error{nil, errors.New("socket gone")}
		})
		err = h.Restore(t.Context())
		if err == nil || !strings.Contains(err.Error(), "reply lost") || !strings.Contains(err.Error(), "socket gone") || h.Receipt().State != DebugLevelUnconfirmed {
			t.Fatalf("restore: %v", err)
		}
		// The retry reconciles: the prior level is in place, so nothing is set.
		if err := h.Restore(t.Context()); err != nil || len(fake.setCalls()) != 2 {
			t.Fatalf("retry: %v sets %v", err, fake.setCalls())
		}
	})
	t.Run("refused", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) { f.setFaults = []debugLevelSetFault{{applies: false, err: lost}} })
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || fake.current() != "20/20" {
			t.Fatalf("restore: %v", err)
		}
		if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" {
			t.Fatalf("retry: %v", err)
		}
	})
	t.Run("outside edit during restore", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.afterSet = func(f *debugLevelFake) { f.edit(func(f *debugLevelFake) { f.value = "7/7" }) }
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelConflict || fake.current() != "7/7" {
			t.Fatalf("restore: %v", err)
		}
	})
}

func TestDebugLevelSettlesUnconfirmedSets(t *testing.T) {
	stillRunning := errors.New("command not stopped")
	t.Run("apply", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		fake.settleErrs = []error{stillRunning, stillRunning}
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if h == nil || err == nil || !strings.Contains(err.Error(), "may still run") {
			t.Fatalf("handle %v err %v", h, err)
		}
		wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplyUncertain)
		// The prior level still reads back, but the set may still apply:
		// Restore neither reports restoration nor releases the admission.
		err = h.Restore(t.Context())
		if err == nil || !strings.Contains(err.Error(), "may still run") || h.Receipt().State != DebugLevelUnconfirmed || len(fake.setCalls()) != 1 {
			t.Fatalf("restore: %v state %s sets %v", err, h.Receipt().State, fake.setCalls())
		}
		if len(registry.active) != 1 {
			t.Fatal("a window with a running set lost its admission")
		}
		// The delayed set applies; once it is confirmed ended, Restore
		// reconciles the level it left.
		fake.complete()
		if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" || h.Receipt().State != DebugLevelRestored {
			t.Fatalf("retry: %v value %q state %s", err, fake.current(), h.Receipt().State)
		}
		if len(registry.active) != 0 {
			t.Fatal("admission not released")
		}
	})
	t.Run("apply stopped before it applied", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if h == nil || err == nil || h.Receipt().State != DebugLevelApplyUncertain || h.Receipt().Applied != "1/5" {
			t.Fatalf("handle %v err %v", h, err)
		}
		if err := h.Restore(t.Context()); err != nil || len(fake.setCalls()) != 1 {
			t.Fatalf("restore: %v sets %v", err, fake.setCalls())
		}
		fake.complete()
		if fake.current() != "1/5" {
			t.Fatalf("a stopped set applied: %q", fake.current())
		}
	})
	t.Run("restore", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) {
			f.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
			f.settleErrs = []error{stillRunning}
		})
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || len(registry.active) != 1 {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		fake.complete()
		if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" || len(fake.setCalls()) != 2 {
			t.Fatalf("retry: %v value %q sets %v", err, fake.current(), fake.setCalls())
		}
	})
	t.Run("restore during a replacement", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) {
			f.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
			f.settleErrs = []error{stillRunning}
			f.afterSet = func(f *debugLevelFake) { f.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" }) }
		})
		// The restore set may still reach the new generation.
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || len(registry.active) != 1 {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		fake.edit(func(f *debugLevelFake) { f.afterSet = nil })
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || len(fake.setCalls()) != 2 {
			t.Fatalf("retry: %v state %s sets %v", err, h.Receipt().State, fake.setCalls())
		}
	})
	t.Run("changed target", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		fake.settleErrs = []error{stillRunning, stillRunning}
		h, _ := openDebugLevelFake(t.Context(), &registry, fake, "20")
		fake.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" })
		// The running set could reach the new generation: the window is not
		// released as target-changed until the set is confirmed ended.
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || len(registry.active) != 1 {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || len(registry.active) != 0 {
			t.Fatalf("retry: %v state %s", err, h.Receipt().State)
		}
	})
}

func TestDebugLevelRevalidatesAfterReads(t *testing.T) {
	// replace makes the next read return before a replacement completes.
	replace := func(f *debugLevelFake) {
		f.afterGet = func(f *debugLevelFake) {
			f.edit(func(f *debugLevelFake) { f.gen.identity, f.value = "osd0@start-2", "9/9" })
			f.afterGet = nil
		}
	}
	t.Run("restore", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		replace(fake)
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || fake.current() != "9/9" || len(fake.setCalls()) != 1 {
			t.Fatalf("restore: %v state %s value %q sets %v", err, h.Receipt().State, fake.current(), fake.setCalls())
		}
	})
	t.Run("prior already in place", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) { f.value = "1/5" })
		replace(fake)
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || h.Receipt().Restored != "" {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
	})
	t.Run("apply", func(t *testing.T) {
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		replace(fake)
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if h != nil || err == nil || fake.current() != "9/9" || len(fake.setCalls()) != 0 || len(registry.active) != 0 {
			t.Fatalf("handle %v err %v value %q sets %v", h, err, fake.current(), fake.setCalls())
		}
	})
}

func TestDebugLevelEndedWorkloadContext(t *testing.T) {
	var registry debugLevelRegistry
	fake := newDebugLevelFake("1/5")
	workload, cancel := context.WithCancel(t.Context())
	// The workload ends while the set runs: the set applied, and the
	// readback still runs with a fresh context.
	fake.afterSet = func(*debugLevelFake) { cancel() }
	fake.setFaults = []debugLevelSetFault{{applies: true, err: context.Canceled}}
	h, err := openDebugLevelFake(workload, &registry, fake, "20")
	if h == nil || !errors.Is(err, context.Canceled) || h.Receipt().Applied != "20/20" {
		t.Fatalf("handle %v err %v", h, err)
	}
	fake.afterSet = nil
	if err := h.Restore(workload); err == nil || fake.current() != "20/20" {
		t.Fatalf("restore with the ended workload context: %v", err)
	}
	if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" {
		t.Fatalf("restore with a fresh context: %v", err)
	}
}

func TestDebugLevelRestoreDuringFinalization(t *testing.T) {
	t.Run("known failure is archived before restoration", func(t *testing.T) {
		fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fx.Cleanup("remove client", func(context.Context) error { fx.event("remove client"); return nil })
		fx.Cleanup("restore debug_osd", func(ctx context.Context) error {
			fx.event("restore at " + fake.current())
			return h.Restore(ctx)
		})
		fx.tb.Errorf("workload failed")
		fx.run()
		want := []string{"diagnostics", "crash", "restore at 20/20", "remove client", "terminate"}
		if events := debugLevelFinalizationEvents(fx); !slices.Equal(events, want) {
			t.Fatalf("events %v, want %v", events, want)
		}
		manifest := readManifest(t, fx.archives(t)[0])
		if manifest.Phase != "before-cleanup" || manifest.Cleanups[0].Name != "restore debug_osd" || manifest.Cleanups[0].Error != "" || fake.current() != "1/5" {
			t.Fatalf("manifest %+v value %q", manifest.Cleanups, fake.current())
		}
	})
	t.Run("restoration as the first failure", func(t *testing.T) {
		fx := newFinalizationFixture(t, FinalizationConfig{Archive: true})
		var registry debugLevelRegistry
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), &registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fx.Cleanup("remove client", func(context.Context) error { fx.event("remove client"); return nil })
		fx.Cleanup("restore debug_osd", func(ctx context.Context) error {
			fx.event("restore")
			return h.Restore(ctx)
		})
		fake.edit(func(f *debugLevelFake) { f.gen.paused = true })
		fx.run()
		want := []string{"restore", "remove client", "diagnostics", "crash", "terminate"}
		if events := debugLevelFinalizationEvents(fx); !slices.Equal(events, want) {
			t.Fatalf("events %v, want %v", events, want)
		}
		manifest := readManifest(t, fx.archives(t)[0])
		if manifest.Phase != "after-cleanup" || !manifest.Trigger.CleanupFailed || !strings.Contains(manifest.Cleanups[0].Error, "paused") ||
			manifest.Termination == nil || !manifest.Final || h.Receipt().State != DebugLevelUnconfirmed {
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

func TestOverrideDebugLevelRefusesBeforeCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit    func(*Container)
		setting DebugLevelSetting
		want    string
	}{
		"MON":            {nil, DebugLevelSetting{Daemon: "mon.a", Name: "debug_ms", Level: "1"}, "must be an OSD"},
		"MGR":            {nil, DebugLevelSetting{Daemon: "mgr.a", Name: "debug_ms", Level: "1"}, "must be an OSD"},
		"malformed":      {nil, DebugLevelSetting{Daemon: "osd.01", Name: "debug_ms", Level: "1"}, "must be an OSD"},
		"option":         {nil, DebugLevelSetting{Daemon: "osd.0", Name: "debug_mon", Level: "1"}, "not supported"},
		"level":          {nil, DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "100"}, "N or N/M"},
		"unknown OSD":    {nil, DebugLevelSetting{Daemon: "osd.7", Name: "debug_osd", Level: "1"}, "not a running-capable OSD"},
		"purged OSD":     {nil, DebugLevelSetting{Daemon: "osd.1", Name: "debug_osd", Level: "1"}, "not a running-capable OSD"},
		"detaching OSD":  {func(c *Container) { c.osds[0].detaching = true }, DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "1"}, "not a running-capable OSD"},
		"closed cluster": {func(c *Container) { c.closed = true }, DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "1"}, "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			h, err := c.OverrideDebugLevel(t.Context(), test.setting)
			if h != nil || err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 || len(c.debugLevels.active) != 0 {
				t.Fatalf("handle %v err %v calls %v", h, err, control.Calls)
			}
		})
	}
	var nilCluster *Container
	if _, err := nilCluster.OverrideDebugLevel(t.Context(), DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: "1"}); err == nil {
		t.Fatal("nil cluster accepted")
	}
}

// debugLevelOSD is an OSD container whose generation the test sets.
type debugLevelOSD struct {
	testcontainers.Container
	id    string
	state container.State
}

func (o *debugLevelOSD) GetContainerID() string { return o.id }
func (o *debugLevelOSD) State(context.Context) (*container.State, error) {
	state := o.state
	return &state, nil
}

func TestOSDDebugTargetGeneration(t *testing.T) {
	c, _ := adminSocketCluster()
	ctr := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
	c.osds[0] = &OSDContainer{ID: 0, Container: ctr}
	target := &osdDebugTarget{c: c, id: 0, daemon: "osd.0", osd: c.osds[0]}
	gen, err := target.generation(t.Context())
	if err != nil || gen != (debugLevelGeneration{owned: true, identity: "osd-container@start-1", running: true}) {
		t.Fatalf("generation %+v %v", gen, err)
	}
	ctr.state = container.State{Running: true, Paused: true, StartedAt: "start-1"}
	if gen, _ := target.generation(t.Context()); !gen.paused {
		t.Fatalf("paused generation %+v", gen)
	}
	for name, edit := range map[string]func(){
		"reopened":   func() { c.osds[0] = &OSDContainer{ID: 0, Container: ctr} },
		"purged":     func() { c.osds[0].purged = true },
		"purging":    func() { c.osds[0].purgeIssued = true },
		"detaching":  func() { c.osds[0].detaching = true },
		"terminated": func() { c.closed = true },
	} {
		t.Run(name, func(t *testing.T) {
			saved, closed := *c.osds[0], c.closed
			original := c.osds[0]
			defer func() { *original, c.osds[0], c.closed = saved, original, closed }()
			edit()
			if gen, err := target.generation(t.Context()); err != nil || gen.owned {
				t.Fatalf("generation %+v %v", gen, err)
			}
		})
	}
}

func TestOSDDebugTargetSetAdmission(t *testing.T) {
	newTarget := func() (*Container, *poolFixtureContainer, *debugLevelOSD, *osdDebugTarget) {
		c, control := adminSocketCluster()
		ctr := &debugLevelOSD{id: "osd-container", state: container.State{Running: true, StartedAt: "start-1"}}
		c.osds[0] = &OSDContainer{ID: 0, Container: ctr}
		return c, control, ctr, &osdDebugTarget{c: c, id: 0, daemon: "osd.0", osd: c.osds[0]}
	}
	for name, test := range map[string]struct {
		edit    func(*Container, *debugLevelOSD)
		changed bool
	}{
		"restarted":  {func(_ *Container, o *debugLevelOSD) { o.state.StartedAt = "start-2" }, true},
		"replaced":   {func(c *Container, o *debugLevelOSD) { c.osds[0] = &OSDContainer{ID: 0, Container: o} }, true},
		"stopped":    {func(_ *Container, o *debugLevelOSD) { o.state.Running = false }, true},
		"terminated": {func(c *Container, _ *debugLevelOSD) { c.closed = true }, true},
		"paused":     {func(_ *Container, o *debugLevelOSD) { o.state.Paused = true }, false},
		"no run dir": {func(c *Container, _ *debugLevelOSD) { c.run.removed = true }, false},
	} {
		t.Run(name, func(t *testing.T) {
			c, control, ctr, target := newTarget()
			test.edit(c, ctr)
			settle, err := target.set(t.Context(), "osd-container@start-1", "debug_osd", "20")
			var refusal *debugLevelRefusal
			if settle != nil || !errors.As(err, &refusal) || refusal.changed != test.changed || len(control.Calls) != 0 {
				t.Fatalf("settle %v err %v calls %v", settle != nil, err, control.Calls)
			}
		})
	}
	t.Run("topology busy", func(t *testing.T) {
		c, control, _, target := newTarget()
		c.mu.Lock()
		defer c.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		var refusal *debugLevelRefusal
		if _, err := target.set(ctx, "osd-container@start-1", "debug_osd", "20"); !errors.As(err, &refusal) || refusal.changed || len(control.Calls) != 0 {
			t.Fatalf("err %v calls %v", err, control.Calls)
		}
	})
}
