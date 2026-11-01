package finalize

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// fakeDebugWindow is a DebugLevelReceipter that records the receipt of a
// window the way the override package does, with a fixed clock so that two
// windows on one daemon, option and generation have identical receipts apart
// from their registration order. Package ceph cannot build the real handle,
// because the override package imports ceph, so the manifest tests use this
// stand-in. Pairing a real override.DebugLevelHandle with a real Finalization
// is covered only by the native debug level scenario.
type fakeDebugWindow struct {
	mu      sync.Mutex
	receipt override.DebugLevelReceipt
	// paused makes restore end unconfirmed, as a paused daemon does.
	paused   bool
	restores int
}

var fakeDebugClock = time.Unix(0, 0).UTC()

func (w *fakeDebugWindow) Receipt() override.DebugLevelReceipt {
	w.mu.Lock()
	defer w.mu.Unlock()
	receipt := w.receipt
	receipt.Transitions = slices.Clone(w.receipt.Transitions)
	return receipt
}

func (w *fakeDebugWindow) step(state override.DebugLevelState, readback string, err error) {
	w.receipt.State = state
	transition := override.DebugLevelTransition{State: state, At: fakeDebugClock, Readback: readback}
	if err != nil {
		transition.Error = err.Error()
	}
	w.receipt.Transitions = append(w.receipt.Transitions, transition)
}

// newFakeDebugWindow returns a window that was applied and verified.
func newFakeDebugWindow(level string) *fakeDebugWindow {
	w := &fakeDebugWindow{receipt: override.DebugLevelReceipt{Daemon: "osd.0", Name: "debug_osd", Generation: "osd0@start-1",
		Requested: level, Prior: "1/5", Transitions: []override.DebugLevelTransition{}}}
	w.step(override.DebugLevelPriorRead, "1/5", nil)
	w.step(override.DebugLevelApplyAttempted, "", nil)
	w.receipt.Applied = "20/20"
	w.step(override.DebugLevelApplied, "20/20", nil)
	return w
}

// newUncertainFakeDebugWindow returns a window whose set failed with err.
func newUncertainFakeDebugWindow(err error) *fakeDebugWindow {
	w := newFakeDebugWindow("20")
	w.receipt.Transitions = w.receipt.Transitions[:2]
	w.step(override.DebugLevelApplyUncertain, "20/20", err)
	return w
}

// restore is the cleanup step of a window: it restores the prior level, or
// ends unconfirmed with an error while the daemon is paused.
func (w *fakeDebugWindow) restore(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.restores++
	if w.receipt.State == override.DebugLevelRestored {
		return nil
	}
	w.step(override.DebugLevelRestoreAttempted, "", nil)
	if w.paused {
		err := errors.New("osd.0 is paused")
		w.step(override.DebugLevelUnconfirmed, "", err)
		return err
	}
	w.receipt.Restored = w.receipt.Prior
	w.step(override.DebugLevelRestored, w.receipt.Prior, nil)
	return nil
}

func (w *fakeDebugWindow) restoreCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.restores
}
