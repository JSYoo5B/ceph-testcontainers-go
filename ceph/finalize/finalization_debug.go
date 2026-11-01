package finalize

import (
	"reflect"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

const (
	// maxFinalizationDebugWindows bounds the registrations that the manifest
	// keeps; later ones are counted in DebugWindowsOmitted.
	maxFinalizationDebugWindows = 32
	// maxFinalizationDebugTransitions bounds the transitions kept per
	// snapshot: the first half and the last half, with the rest counted.
	maxFinalizationDebugTransitions = 32
	maxFinalizationDebugField       = 256
	maxFinalizationDebugError       = 2048
)

// DebugLevelReceipter is a source of one debug level window's receipt. A
// *override.DebugLevelHandle implements it. A registration is absent when it
// is a nil interface, when its dynamic value is a nil pointer, map, slice,
// func, chan or interface (Receipt is then never called, so a method that is
// not nil-safe, including a value-receiver method reached through a nil
// pointer, cannot panic), or when Receipt returns the zero receipt (State is
// empty). Absence is not decided by a nil comparison of the interface alone.
type DebugLevelReceipter interface {
	Receipt() override.DebugLevelReceipt
}

// TrackDebugLevel registers a debug level window for the manifest. Every
// call is a separate window, in registration order, even for the same
// daemon, option and generation: windows are never merged or deduplicated.
// Register the handle right after override.SetDebugLevel returns, including the
// non-nil handle that comes with an apply error. A nil interface, a typed nil
// (a nil pointer, map, slice, func, chan or interface dynamic value), a
// zero-value handle, or any receipter whose receipt has no State is recorded
// as absent; Receipt is not called on a typed nil. Other panics in a foreign
// Receipt are not recovered, never as a restored window. A receipt in
// state DebugLevelNotApplied is recorded as present, with its Reason.
//
// The manifest keeps a copy of the window's receipt when the failure archive
// is taken, labeled with the archive's phase, and a second copy after the
// cleanups, so that a restoration run by Finalization.Cleanup is a separate,
// later record. Taking a copy reads only the handle's local state; it never
// applies or restores a level and never queries the daemon. Passing runs
// archive nothing. Registering after finalization started fails the test.
//
//purpose:evidence
func (f *Finalization) TrackDebugLevel(handle DebugLevelReceipter) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started {
		f.t.Errorf("ceph finalization: a debug level window was registered after finalization started")
		return
	}
	if isNilReceipter(handle) {
		handle = nil
	}
	f.debugWindows = append(f.debugWindows, handle)
}

// finalizationDebugWindow is one registration. Archive is the copy taken
// with the failure archive and Final the copy taken after the cleanups; a nil
// one was not taken, for example Archive in a run that archived nothing.
type finalizationDebugWindow struct {
	// Sequence is the 1-based registration order, the window's identity.
	Sequence int                        `json:"sequence"`
	Archive  *finalizationDebugSnapshot `json:"archive,omitempty"`
	Final    *finalizationDebugSnapshot `json:"final,omitempty"`
}

// finalizationDebugSnapshot is a copy of a window's receipt at one phase.
// Present is false when the registration had no usable handle, and Receipt
// is nil then.
type finalizationDebugSnapshot struct {
	Phase   string                    `json:"phase"`
	Present bool                      `json:"present"`
	Receipt *finalizationDebugReceipt `json:"receipt,omitempty"`
}

type finalizationDebugReceipt struct {
	Daemon     string                   `json:"daemon"`
	Name       string                   `json:"name"`
	Generation string                   `json:"generation"`
	Requested  string                   `json:"requested"`
	Prior      string                   `json:"prior"`
	Applied    string                   `json:"applied,omitempty"`
	Restored   string                   `json:"restored,omitempty"`
	State      override.DebugLevelState `json:"state"`
	// Reason is the redacted, bounded reason of a not-applied window.
	Reason string `json:"reason,omitempty"`
	// Transitions keeps the first and last transitions of a long record;
	// TransitionsOmitted counts the ones between.
	Transitions        []finalizationDebugTransition `json:"transitions"`
	TransitionsOmitted int                           `json:"transitions_omitted,omitempty"`
}

type finalizationDebugTransition struct {
	State    override.DebugLevelState `json:"state"`
	At       time.Time                `json:"at"`
	Readback string                   `json:"readback,omitempty"`
	Error    string                   `json:"error,omitempty"`
}

// snapshotDebugWindows adds one copy per registered window to the manifest.
// Each archive-phase or final copy is taken once, into its own field, so
// the later copy never replaces the earlier one.
func (f *Finalization) snapshotDebugWindows(m *finalizationManifest, phase string, final bool) {
	f.mu.Lock()
	handles := append([]DebugLevelReceipter(nil), f.debugWindows...)
	f.mu.Unlock()
	for i, handle := range handles {
		if i >= maxFinalizationDebugWindows {
			m.DebugWindowsOmitted = len(handles) - i
			break
		}
		for len(m.DebugWindows) <= i {
			m.DebugWindows = append(m.DebugWindows, finalizationDebugWindow{Sequence: len(m.DebugWindows) + 1})
		}
		window := &m.DebugWindows[i]
		slot := &window.Archive
		if final {
			slot = &window.Final
		}
		snapshot := f.debugSnapshot(handle, phase)
		*slot = &snapshot
	}
}

func (f *Finalization) debugSnapshot(handle DebugLevelReceipter, phase string) finalizationDebugSnapshot {
	snapshot := finalizationDebugSnapshot{Phase: phase}
	if isNilReceipter(handle) {
		return snapshot
	}
	receipt := handle.Receipt()
	if receipt.State == "" {
		return snapshot
	}
	field := func(s string) string { return f.debugField(s, maxFinalizationDebugField) }
	out := &finalizationDebugReceipt{Daemon: field(receipt.Daemon), Name: field(receipt.Name), Generation: field(receipt.Generation),
		Requested: field(receipt.Requested), Prior: field(receipt.Prior), Applied: field(receipt.Applied), Restored: field(receipt.Restored),
		State: override.DebugLevelState(field(string(receipt.State))), Reason: f.debugField(receipt.Reason, maxFinalizationDebugError), Transitions: []finalizationDebugTransition{}}
	transitions := receipt.Transitions
	if len(transitions) > maxFinalizationDebugTransitions {
		half := maxFinalizationDebugTransitions / 2
		out.TransitionsOmitted = len(transitions) - 2*half
		transitions = append(append([]override.DebugLevelTransition{}, transitions[:half]...), transitions[len(transitions)-half:]...)
	}
	for _, transition := range transitions {
		out.Transitions = append(out.Transitions, finalizationDebugTransition{State: override.DebugLevelState(field(string(transition.State))),
			At: transition.At, Readback: field(transition.Readback), Error: f.debugField(transition.Error, maxFinalizationDebugError)})
	}
	snapshot.Present, snapshot.Receipt = true, out
	return snapshot
}

// debugField redacts a retained string through the archive policy and then
// bounds it; an empty string stays empty.
func (f *Finalization) debugField(s string, limit int) string {
	if s == "" {
		return ""
	}
	s, clipped := evidence.BoundText(f.redact(s), limit)
	if clipped {
		s += " [truncated]"
	}
	return s
}

// isNilReceipter reports a nil interface or a typed nil: a dynamic value of
// a nilable kind that is nil. Receipt is never called on such a value.
func isNilReceipter(r DebugLevelReceipter) bool {
	if r == nil {
		return true
	}
	v := reflect.ValueOf(r)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
