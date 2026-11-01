package override

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
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

// debugLevelGeneration is what the fake target reports for its daemon.
type debugLevelGeneration struct {
	owned           bool
	identity        string
	running, paused bool
}

// fakeRefusal is a set that issued nothing, as the cluster's target reports.
type fakeRefusal struct {
	changed bool
	err     error
}

func (e *fakeRefusal) Error() string { return e.err.Error() }
func (e *fakeRefusal) Unwrap() error { return e.err }
func (e *fakeRefusal) Changed() bool { return e.changed }

type debugLevelSetFault struct {
	applies bool
	// live leaves the set running unconfirmed: it applies only on complete.
	live bool
	err  error
}

func newDebugLevelFake(value string) *debugLevelFake {
	return &debugLevelFake{value: value, gen: debugLevelGeneration{owned: true, identity: "osd0@start-1", running: true}}
}

func (f *debugLevelFake) Generation(ctx context.Context) (bool, string, bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, "", false, false, err
	}
	return f.gen.owned, f.gen.identity, f.gen.running, f.gen.paused, f.genErr
}

func (f *debugLevelFake) Get(ctx context.Context, name string) (string, error) {
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

// Set admits the set like the cluster's target: only on the expected
// generation, and a refusal issued nothing.
func (f *debugLevelFake) Set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	f.mu.Lock()
	if changed, err := overridehook.CheckDebugGeneration(f.gen.owned, f.gen.identity, f.gen.running, f.gen.paused, f.genErr, identity, "osd.0"); err != nil {
		f.mu.Unlock()
		return nil, &fakeRefusal{changed: changed, err: err}
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

// newDebugWindows returns the registry of a cluster of the test's own, which
// is the registry that SetDebugLevel opens windows in.
func newDebugWindows(t *testing.T) overridehook.DebugWindows {
	t.Helper()
	return overridehook.DebugWindowRegistry(testhelper.TestCluster(nil, testhelper.TestConfig{}))
}

// slotHeld reports whether a window of osd.0 debug_osd is open in registry.
func slotHeld(registry overridehook.DebugWindows) bool {
	probe := new(int)
	if err := registry.Reserve("osd.0/debug_osd", probe); err != nil {
		return true
	}
	registry.Release("osd.0/debug_osd", probe)
	return false
}

func openDebugLevelFake(ctx context.Context, registry overridehook.DebugWindows, fake *debugLevelFake, level string) (*DebugLevelHandle, error) {
	setting, owned, err := normalizeDebugLevelSetting(DebugLevelSetting{Daemon: "osd.0", Name: "debug_osd", Level: level})
	if err != nil {
		return nil, err
	}
	return openDebugLevel(ctx, registry, fake, setting, owned, time.Second, func() time.Time { return time.Unix(0, 0).UTC() })
}

func debugLevelStates(h *DebugLevelHandle) []DebugLevelState {
	var states []DebugLevelState
	for _, transition := range h.Receipt().Transitions {
		states = append(states, transition.State)
	}
	return states
}

func wantDebugLevelStates(t *testing.T, h *DebugLevelHandle, want ...DebugLevelState) {
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
		if _, _, err := normalizeDebugLevelSetting(DebugLevelSetting{Daemon: "osd.0", Name: name, Level: "1"}); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("option %q: %v", name, err)
		}
	}
}

func TestDebugLevelAppliesNormalizedLevelAndRestoresPrior(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
			registry := newDebugWindows(t)
			fake := newDebugLevelFake("1/5")
			fake.setFaults = []debugLevelSetFault{test.fault}
			fake.getErrs = test.getErrs
			h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
			if h == nil || err == nil {
				t.Fatalf("handle %v err %v", h, err)
			}
			wantDebugLevelStates(t, h, DebugLevelPriorRead, DebugLevelApplyAttempted, DebugLevelApplyUncertain)
			if h.Receipt().Applied != test.readback {
				t.Fatalf("applied readback %q, want %q", h.Receipt().Applied, test.readback)
			}
			// The admission is kept until the window is settled.
			if _, err := openDebugLevelFake(t.Context(), registry, newDebugLevelFake("1/5"), "5"); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("overlapping window admitted: %v", err)
			}
			if err := h.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if fake.current() != "1/5" || len(fake.setCalls()) != 1+test.restoreSets || h.Receipt().State != DebugLevelRestored {
				t.Fatalf("value %q sets %v state %s", fake.current(), fake.setCalls(), h.Receipt().State)
			}
			if _, err := openDebugLevelFake(t.Context(), registry, newDebugLevelFake("1/5"), "5"); err != nil {
				t.Fatalf("admission not released: %v", err)
			}
		})
	}
}

// restoreWithin runs Restore and fails the test, instead of hanging it, when
// the call waits for a lock that a not-applied handle must not take.
func restoreWithin(ctx context.Context, t *testing.T, h *DebugLevelHandle) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.Restore(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Restore waited for a lock")
		return nil
	}
}

// wantNotApplied pins the contract of a refused window: a non-nil handle whose
// receipt says not-applied with the refusal as its reason, no applied or
// restored value, and a Restore that returns nil without taking the window's
// lock (it is held here and the context has ended), without reading the
// daemon and without changing the receipt.
func wantNotApplied(t *testing.T, h *DebugLevelHandle, err error, fake *debugLevelFake) {
	t.Helper()
	if h == nil || err == nil {
		t.Fatalf("handle %v err %v", h, err)
	}
	receipt := h.Receipt()
	last := receipt.Transitions[len(receipt.Transitions)-1]
	if receipt.State != DebugLevelNotApplied || receipt.Reason != err.Error() || receipt.Applied != "" || receipt.Restored != "" ||
		last.State != DebugLevelNotApplied || last.Error != err.Error() {
		t.Fatalf("receipt %+v for %v", receipt, err)
	}
	if fake != nil && len(fake.setCalls()) != 0 {
		t.Fatalf("a refused window set %v", fake.setCalls())
	}
	select {
	case h.state.lock <- struct{}{}:
		defer func() { <-h.state.lock }()
	default:
	}
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	gets := 0
	if fake != nil {
		gets = fake.gets
	}
	if restoreErr := restoreWithin(ended, t, h); restoreErr != nil {
		t.Fatalf("Restore of a not-applied window: %v", restoreErr)
	}
	if fake != nil && (fake.gets != gets || len(fake.setCalls()) != 0) {
		t.Fatalf("Restore of a not-applied window reached the daemon: gets %d->%d sets %v", gets, fake.gets, fake.setCalls())
	}
	if after := h.Receipt(); !reflect.DeepEqual(after, receipt) {
		t.Fatalf("Restore changed the receipt: %+v -> %+v", receipt, after)
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
			registry := newDebugWindows(t)
			fake := newDebugLevelFake("1/5")
			fake.edit(edit)
			h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
			wantNotApplied(t, h, err, fake)
			if slotHeld(registry) {
				t.Fatal("a refused window kept its admission")
			}
		})
	}
}

func TestDebugLevelRejectsDuplicateWindow(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
	if err != nil {
		t.Fatal(err)
	}
	other := newDebugLevelFake("1/5")
	second, err := openDebugLevelFake(t.Context(), registry, other, "10")
	if !errors.Is(err, ceph.ErrInvalidRequest) || other.gets != 0 {
		t.Fatalf("duplicate window: %v gets=%d", err, other.gets)
	}
	wantNotApplied(t, second, err, other)
	// The refusal did not release the first window's slot, and its Restore
	// does not release it either.
	if err := second.Restore(t.Context()); err != nil || !slotHeld(registry) {
		t.Fatalf("restore of the refused window: %v, first window's slot held: %v", err, slotHeld(registry))
	}
	// Another option of the same daemon is a separate window.
	setting := DebugLevelSetting{Daemon: "osd.0", Name: "debug_ms", Level: "1"}
	if ms, err := openDebugLevel(t.Context(), registry, newDebugLevelFake("0/5"), setting, "1/1", time.Second, time.Now); err != nil || ms == nil {
		t.Fatalf("debug_ms window: %v", err)
	}
	if err := h.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if again, err := openDebugLevelFake(t.Context(), registry, other, "10"); err != nil || again == nil {
		t.Fatalf("window after restoration: %v", err)
	}
	// The restored handle does not touch the new window's level.
	if err := h.Restore(t.Context()); err != nil || other.current() != "10/10" {
		t.Fatalf("old handle: %v value %q", err, other.current())
	}
}

func TestDebugLevelRestorePreservesOutsideEdits(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
	if slotHeld(registry) {
		t.Fatal("a conflicted window kept its admission")
	}
}

func TestDebugLevelRestoreAcceptsAlreadyRestoredLevel(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
			registry := newDebugWindows(t)
			fake := newDebugLevelFake("1/5")
			h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
			if err != nil {
				t.Fatal(err)
			}
			fake.edit(edit)
			gets := fake.gets
			err = h.Restore(t.Context())
			if err == nil || len(fake.setCalls()) != 1 || fake.gets != gets || h.Receipt().State != DebugLevelTargetChanged {
				t.Fatalf("restore: %v sets %v gets %d->%d state %s", err, fake.setCalls(), gets, fake.gets, h.Receipt().State)
			}
			if slotHeld(registry) {
				t.Fatal("a changed target kept its admission")
			}
		})
	}
}

func TestDebugLevelApplyDuringGenerationChange(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	fake.afterSet = func(f *debugLevelFake) { f.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" }) }
	h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
			registry := newDebugWindows(t)
			fake := newDebugLevelFake("1/5")
			h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
			if err != nil {
				t.Fatal(err)
			}
			fake.edit(test.edit)
			if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || len(fake.setCalls()) != 1 || fake.current() != "20/20" {
				t.Fatalf("restore: %v state %s sets %v", err, h.Receipt().State, fake.setCalls())
			}
			if !slotHeld(registry) {
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		fake.settleErrs = []error{stillRunning, stillRunning}
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		if !slotHeld(registry) {
			t.Fatal("a window with a running set lost its admission")
		}
		// The delayed set applies; once it is confirmed ended, Restore
		// reconciles the level it left.
		fake.complete()
		if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" || h.Receipt().State != DebugLevelRestored {
			t.Fatalf("retry: %v value %q state %s", err, fake.current(), h.Receipt().State)
		}
		if slotHeld(registry) {
			t.Fatal("admission not released")
		}
	})
	t.Run("apply stopped before it applied", func(t *testing.T) {
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) {
			f.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
			f.settleErrs = []error{stillRunning}
		})
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || !slotHeld(registry) {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		fake.complete()
		if err := h.Restore(t.Context()); err != nil || fake.current() != "1/5" || len(fake.setCalls()) != 2 {
			t.Fatalf("retry: %v value %q sets %v", err, fake.current(), fake.setCalls())
		}
	})
	t.Run("restore during a replacement", func(t *testing.T) {
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		fake.edit(func(f *debugLevelFake) {
			f.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
			f.settleErrs = []error{stillRunning}
			f.afterSet = func(f *debugLevelFake) { f.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" }) }
		})
		// The restore set may still reach the new generation.
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || !slotHeld(registry) {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		fake.edit(func(f *debugLevelFake) { f.afterSet = nil })
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || len(fake.setCalls()) != 2 {
			t.Fatalf("retry: %v state %s sets %v", err, h.Receipt().State, fake.setCalls())
		}
	})
	t.Run("changed target", func(t *testing.T) {
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		fake.setFaults = []debugLevelSetFault{{live: true, err: stillRunning}}
		fake.settleErrs = []error{stillRunning, stillRunning}
		h, _ := openDebugLevelFake(t.Context(), registry, fake, "20")
		fake.edit(func(f *debugLevelFake) { f.gen.identity = "osd0@start-2" })
		// The running set could reach the new generation: the window is not
		// released as target-changed until the set is confirmed ended.
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelUnconfirmed || !slotHeld(registry) {
			t.Fatalf("restore: %v state %s", err, h.Receipt().State)
		}
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || slotHeld(registry) {
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
		if err != nil {
			t.Fatal(err)
		}
		replace(fake)
		if err := h.Restore(t.Context()); err == nil || h.Receipt().State != DebugLevelTargetChanged || fake.current() != "9/9" || len(fake.setCalls()) != 1 {
			t.Fatalf("restore: %v state %s value %q sets %v", err, h.Receipt().State, fake.current(), fake.setCalls())
		}
	})
	t.Run("prior already in place", func(t *testing.T) {
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
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
		registry := newDebugWindows(t)
		fake := newDebugLevelFake("1/5")
		replace(fake)
		h, err := openDebugLevelFake(t.Context(), registry, fake, "20")
		wantNotApplied(t, h, err, fake)
		if fake.current() != "9/9" || slotHeld(registry) {
			t.Fatalf("value %q slot held %v", fake.current(), slotHeld(registry))
		}
	})
}

func TestDebugLevelEndedWorkloadContext(t *testing.T) {
	registry := newDebugWindows(t)
	fake := newDebugLevelFake("1/5")
	workload, cancel := context.WithCancel(t.Context())
	// The workload ends while the set runs: the set applied, and the
	// readback still runs with a fresh context.
	fake.afterSet = func(*debugLevelFake) { cancel() }
	fake.setFaults = []debugLevelSetFault{{applies: true, err: context.Canceled}}
	h, err := openDebugLevelFake(workload, registry, fake, "20")
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
