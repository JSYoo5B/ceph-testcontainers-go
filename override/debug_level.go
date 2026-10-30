package override

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// DebugLevelHandle manages one runtime debug level window and its
// restoration. Copies share state.
//
// SetDebugLevel returns a non-nil handle for every outcome. A handle of a call
// that was refused or never applied owns nothing: its Restore returns nil,
// takes no lock and never reports a restoration, and its Receipt has the state
// ceph.DebugLevelNotApplied with the refusal as Reason. A handle returned
// together with an error for an uncertain outcome is real, may own a changed
// level and must be restored as well; its receipt never reports not-applied.
// The zero value is unavailable: its Restore returns an error and its Receipt
// has no state.
type DebugLevelHandle struct {
	state *debugLevelState
}

// String names the window's daemon and option without its values.
func (h DebugLevelHandle) String() string {
	if h.state == nil {
		return "Ceph debug level override"
	}
	return "Ceph debug level override " + h.state.setting.Daemon + " " + h.state.setting.Name
}

// GoString formats the window like String.
func (h DebugLevelHandle) GoString() string { return h.String() }

// Receipt returns a copy of the window's record. It reads only local state and
// never queries the daemon. A handle that has no window (a nil handle or the
// zero value) returns a receipt without a state.
func (h *DebugLevelHandle) Receipt() ceph.DebugLevelReceipt {
	if h == nil || h.state == nil {
		return ceph.DebugLevelReceipt{Transitions: []ceph.DebugLevelTransition{}}
	}
	s := h.state
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt := s.receipt
	receipt.Transitions = append([]ceph.DebugLevelTransition{}, s.receipt.Transitions...)
	return receipt
}

// SetDebugLevel changes one runtime debug level of one running owned OSD
// through its admin socket, as ceph.Container.AdminSocket reaches it, until
// Restore. It reads the daemon's effective prior level, sets the requested
// level and verifies the normalized effective readback, so "20" reads back as
// "20/20". It changes the daemon's runtime value, never the central
// configuration database, and a daemon restart drops it; see SetConfig for
// stored entries. The window is bound to the daemon container's generation.
// Only one window per daemon and option may be open; a second one is refused
// until the first is restored or reaches a terminal state.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied
// handle whose Restore is a no-op (see DebugLevelHandle). A nil cluster is
// refused with ceph.ErrInvalidRequest and Op "override.SetDebugLevel". When the
// set or its readback fails, the returned handle keeps the prior level for
// Restore, which reconciles first: a lost reply is not evidence that nothing
// changed. The readback after a set uses a fresh context bounded by the
// cluster's startup timeout, as does every call.
//
// Restore with a context of its own, not the workload's, so that an ended
// workload still restores. With TerminateOnCleanup, register the removal of
// clients first and then Finalization.Cleanup with Restore: the cleanups run
// in reverse order after the failure archive, so a known failure is archived
// while the level is still raised, the restoration runs before the clients
// are removed, and a failed restoration triggers an after-cleanup archive and
// still lets Terminate run. Register the handle with
// Finalization.TrackDebugLevel right after this call returns, whatever it
// returned.
//
// Each set is admitted only while the daemon still runs the window's
// generation, and the cluster's own topology operations, such as
// OSDContainer.Remove, OSDContainer.Detach and DetachedOSD.Reopen, wait until
// the set has ended. A set whose end cannot be confirmed may still run: the
// window keeps it, and no later set or terminal state is taken until it is
// confirmed stopped. Until then OSDContainer.Remove and OSDContainer.Detach
// first stop it, or refuse to replace the OSD.
//
// The readbacks around each set are not an atomic compare-and-swap: another
// writer of the same daemon and option must not race the window, and an
// outside write of the window's own value cannot be told apart from it. A
// restart of the daemon's container from outside the cluster after the last
// check is not held off either.
//
// There is no generic logging default: no level or duration is a universal
// diagnostic setting, and 0/0 is a control rather than a silence guarantee.
// Pick the daemon and option relevant to one concrete operation, open the
// window right before it and restore right after it returns. Verbose logging
// can change the CPU use and timing of the operation it observes.
//
// Outcomes, as the handle it returns and the error:
//   - Window applied and read back: real handle (applied-verified); nil.
//   - Nil cluster: not applied handle; ceph.ErrInvalidRequest, Op
//     override.SetDebugLevel.
//   - Context already ended: not applied handle; the context error.
//   - Option or level not supported: not applied handle;
//     ceph.ErrInvalidRequest.
//   - Daemon is not an OSD such as osd.0: not applied handle;
//     ceph.ErrInvalidRequest.
//   - OSD unknown, purged, being purged or detaching: not applied handle;
//     ceph.ErrInvalidRequest.
//   - Terminated cluster: not applied handle; the error of an operation on a
//     terminated cluster.
//   - A window for this daemon and option is open: not applied handle;
//     ceph.ErrInvalidRequest.
//   - The daemon is not an owned running one, or the prior level cannot be
//     read: not applied handle; the native or inspection error.
//   - The generation changed before the set was issued: not applied handle;
//     the refusal.
//   - The set, its settling or its readback fails or differs: real handle
//     (apply-uncertain); the joined errors.
//   - The generation changed during the set: real handle (target-changed); the
//     joined errors.
func SetDebugLevel(ctx context.Context, c *ceph.Container, setting ceph.DebugLevelSetting) (*DebugLevelHandle, error) {
	const op = "override.SetDebugLevel"
	if c == nil {
		return debugNotApplied(setting, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}

	if err := ctx.Err(); err != nil {
		return debugNotApplied(setting, err)
	}
	setting, owned, err := normalizeDebugLevelSetting(setting)
	if err != nil {
		return debugNotApplied(setting, err)
	}
	bound := overridehook.StartupTimeout(c)
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	target, err := overridehook.OpenDebugTarget(ctx, c, setting.Daemon)
	if err != nil {
		return debugNotApplied(setting, err)
	}
	return openDebugLevel(ctx, overridehook.DebugWindowRegistry(c), target, setting, owned, bound, time.Now)
}

var debugLevelPattern = regexp.MustCompile(`^([0-9]{1,2})(?:/([0-9]{1,2}))?$`)

// normalizeDebugLevel returns Ceph's effective form "N/M" of a level; a
// single number sets both the log and the memory level.
func normalizeDebugLevel(level string) (string, bool) {
	match := debugLevelPattern.FindStringSubmatch(level)
	if match == nil {
		return "", false
	}
	log, _ := strconv.Atoi(match[1])
	gather := log
	if match[2] != "" {
		gather, _ = strconv.Atoi(match[2])
	}
	return strconv.Itoa(log) + "/" + strconv.Itoa(gather), true
}

// sameDebugLevel compares two readbacks by their normalized form, and
// unparseable ones exactly.
func sameDebugLevel(a, b string) bool {
	na, okA := normalizeDebugLevel(a)
	nb, okB := normalizeDebugLevel(b)
	if okA && okB {
		return na == nb
	}
	return a == b
}

func normalizeDebugLevelSetting(setting ceph.DebugLevelSetting) (ceph.DebugLevelSetting, string, error) {
	if setting.Name != "debug_ms" && setting.Name != "debug_osd" {
		return setting, "", overridehook.Invalid(fmt.Errorf("debug level option %q is not supported; select debug_ms or debug_osd", setting.Name))
	}
	owned, ok := normalizeDebugLevel(setting.Level)
	if !ok {
		return setting, "", overridehook.Invalid(fmt.Errorf("debug level %q must be N or N/M with numbers from 0 to 99", setting.Level))
	}
	return setting, owned, nil
}

type debugLevelState struct {
	registry overridehook.DebugWindows
	key      string
	target   overridehook.DebugTarget
	setting  ceph.DebugLevelSetting
	// owned is the normalized requested level, the only value the window
	// restores from.
	owned    string
	identity string
	bound    time.Duration
	now      func() time.Time
	// lock serializes the window's operations and honors contexts.
	lock chan struct{}
	// pending settles a set whose end was not confirmed; lock guards it.
	pending func(context.Context) error
	// refused is why the call was not applied. It is set once, before the
	// handle is returned, and a handle with it owns nothing: Restore reads it
	// without a lock.
	refused error

	mu      sync.Mutex
	receipt ceph.DebugLevelReceipt
	// terminal is the error that a terminal state other than restored
	// returns again.
	terminal error
}

// debugNotApplied returns the handle of a call that was refused before a window
// was opened. Nothing was applied, so no registry slot is held.
func debugNotApplied(setting ceph.DebugLevelSetting, cause error) (*DebugLevelHandle, error) {
	s := &debugLevelState{setting: setting, now: time.Now,
		receipt: ceph.DebugLevelReceipt{Daemon: setting.Daemon, Name: setting.Name, Requested: setting.Level, Transitions: []ceph.DebugLevelTransition{}}}
	return s.refuse(cause)
}

// refuse ends the window as not applied: nothing was set, so the slot is
// released and Restore is a no-op. The caller holds the window's lock, or the
// state is not shared yet.
func (s *debugLevelState) refuse(cause error) (*DebugLevelHandle, error) {
	s.refused = cause
	s.mu.Lock()
	s.receipt.Reason = cause.Error()
	s.mu.Unlock()
	s.record(ceph.DebugLevelNotApplied, "", cause)
	if s.registry != nil {
		s.registry.Release(s.key, s)
	}
	return &DebugLevelHandle{state: s}, cause
}

func (s *debugLevelState) record(state ceph.DebugLevelState, readback string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipt.State = state
	transition := ceph.DebugLevelTransition{State: state, At: s.now(), Readback: readback}
	if err != nil {
		transition.Error = err.Error()
	}
	s.receipt.Transitions = append(s.receipt.Transitions, transition)
}

func (s *debugLevelState) current() ceph.DebugLevelState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipt.State
}

// finish records a terminal state and releases the admission.
func (s *debugLevelState) finish(state ceph.DebugLevelState, readback string, err error) error {
	s.record(state, readback, err)
	s.mu.Lock()
	s.terminal = err
	s.mu.Unlock()
	s.registry.Release(s.key, s)
	return err
}

// sameGeneration checks the target against the window's generation: changed
// is a terminal mismatch, and err is a check that could not be completed.
func (s *debugLevelState) sameGeneration(ctx context.Context) (changed bool, err error) {
	owned, identity, running, paused, err := s.target.Generation(ctx)
	return overridehook.CheckDebugGeneration(owned, identity, running, paused, err, s.identity, s.setting.Daemon)
}

// settle confirms under a fresh bound that the window's unconfirmed set can
// no longer run, and forgets it then.
func (s *debugLevelState) settle(ctx context.Context) error {
	if s.pending == nil {
		return nil
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.bound)
	defer cancel()
	if err := s.pending(settleCtx); err != nil {
		return fmt.Errorf("an earlier set of %s of %s may still run: %w", s.setting.Name, s.setting.Daemon, err)
	}
	s.pending = nil
	return nil
}

// confirmTarget checks the generation during Restore: a changed target ends
// the window, and a check that could not be completed leaves it unconfirmed.
func (s *debugLevelState) confirmTarget(ctx context.Context) error {
	changed, err := s.sameGeneration(ctx)
	switch {
	case changed:
		return s.finish(ceph.DebugLevelTargetChanged, "", err)
	case err != nil:
		s.record(ceph.DebugLevelUnconfirmed, "", err)
	}
	return err
}

// setRefusal is a set that was not issued because the target was not the
// window's running generation. Changed marks a terminal mismatch.
type setRefusal interface {
	error
	Changed() bool
}

func openDebugLevel(ctx context.Context, registry overridehook.DebugWindows, target overridehook.DebugTarget, setting ceph.DebugLevelSetting, owned string, bound time.Duration, now func() time.Time) (*DebugLevelHandle, error) {
	s := &debugLevelState{registry: registry, key: setting.Daemon + "/" + setting.Name, target: target, setting: setting,
		owned: owned, bound: bound, now: now, lock: make(chan struct{}, 1),
		receipt: ceph.DebugLevelReceipt{Daemon: setting.Daemon, Name: setting.Name, Requested: setting.Level, Transitions: []ceph.DebugLevelTransition{}}}
	if err := registry.Reserve(s.key, s); err != nil {
		return s.refuse(err)
	}
	s.lock <- struct{}{}
	defer func() { <-s.lock }()
	owner, identity, running, paused, err := target.Generation(ctx)
	if err == nil && (!owner || !running || paused || identity == "") {
		err = fmt.Errorf("%s must be an owned running daemon", setting.Daemon)
	}
	if err != nil {
		return s.refuse(err)
	}
	s.identity = identity
	s.mu.Lock()
	s.receipt.Generation = identity
	s.mu.Unlock()
	prior, err := target.Get(ctx, setting.Name)
	if err != nil {
		// Nothing was set: the window ends as not applied.
		return s.refuse(fmt.Errorf("read the prior %s of %s: %w", setting.Name, setting.Daemon, err))
	}
	s.mu.Lock()
	s.receipt.Prior = prior
	s.mu.Unlock()
	s.record(ceph.DebugLevelPriorRead, prior, nil)
	handle := &DebugLevelHandle{state: s}

	s.record(ceph.DebugLevelApplyAttempted, "", nil)
	settle, setErr := target.Set(ctx, s.identity, setting.Name, setting.Level)
	if setErr != nil {
		setErr = fmt.Errorf("set %s of %s: %w", setting.Name, setting.Daemon, setErr)
	}
	if refusal := setRefusal(nil); errors.As(setErr, &refusal) {
		// The target changed during the prior read and nothing was set.
		return s.refuse(setErr)
	}
	s.pending = settle
	// A lost reply or cancellation does not prove that the set did not
	// apply; read back with a fresh bounded context, after a set that may
	// still run is stopped.
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()
	settleErr := s.settle(readCtx)
	value, readErr := target.Get(readCtx, setting.Name)
	changed, genErr := s.sameGeneration(readCtx)
	if readErr != nil {
		readErr = fmt.Errorf("read back %s of %s: %w", setting.Name, setting.Daemon, readErr)
		value = ""
	}
	if value != "" {
		s.mu.Lock()
		s.receipt.Applied = value
		s.mu.Unlock()
	}
	switch {
	case settleErr != nil:
		// The window keeps the set, which Restore settles first.
		err = errors.Join(setErr, settleErr, readErr, genErr)
		s.record(ceph.DebugLevelApplyUncertain, value, err)
		return handle, err
	case changed:
		return handle, s.finish(ceph.DebugLevelTargetChanged, value, errors.Join(setErr, genErr))
	case setErr == nil && readErr == nil && genErr == nil && sameDebugLevel(value, owned):
		s.record(ceph.DebugLevelApplied, value, nil)
		return handle, nil
	}
	err = errors.Join(setErr, readErr, genErr)
	if readErr == nil && !sameDebugLevel(value, owned) {
		err = errors.Join(err, fmt.Errorf("%s of %s reads back %q, not the requested %q", setting.Name, setting.Daemon, value, setting.Level))
	}
	s.record(ceph.DebugLevelApplyUncertain, value, err)
	return handle, err
}

// Restore returns the daemon's runtime level to the prior value after it
// confirms the same container generation and that the daemon still shows
// the window's own value; it then verifies the restored readback. A prior
// value already in place counts as restored. Another value is an outside
// edit, which Restore leaves as it is. A stopped, restarted or replaced
// daemon is not restored, and neither is one of a cluster that has been
// terminated. These outcomes are terminal: Restore returns nil once restored
// and the same error again otherwise. A paused daemon, an unavailable socket,
// an unverified readback or an earlier set of the window that may still run
// leave the window open, and a later Restore settles and reconciles it first.
// The generation is checked again after each read. The readback after the set
// and the settling of a set use fresh contexts bounded by the cluster's
// startup timeout. A handle of a call that was not applied returns nil without
// taking a lock.
func (h *DebugLevelHandle) Restore(ctx context.Context) error {
	if h == nil || h.state == nil {
		return errors.New("debug level override is unavailable")
	}
	s := h.state
	if s.refused != nil {
		return nil
	}
	select {
	case s.lock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.lock }()
	switch s.current() {
	case ceph.DebugLevelRestored:
		return nil
	case ceph.DebugLevelConflict, ceph.DebugLevelTargetChanged:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.terminal
	}
	ctx, cancel := context.WithTimeout(ctx, s.bound)
	defer cancel()
	name, daemon := s.setting.Name, s.setting.Daemon
	s.record(ceph.DebugLevelRestoreAttempted, "", nil)
	// A set that may still run could change the level after any check.
	if err := s.settle(ctx); err != nil {
		s.record(ceph.DebugLevelUnconfirmed, "", err)
		return err
	}
	if err := s.confirmTarget(ctx); err != nil {
		return err
	}
	current, err := s.target.Get(ctx, name)
	if err != nil {
		err = fmt.Errorf("read the current %s of %s: %w", name, daemon, err)
		s.record(ceph.DebugLevelUnconfirmed, "", err)
		return err
	}
	// A replacement during the read would make current another generation's.
	if err := s.confirmTarget(ctx); err != nil {
		return err
	}
	prior := s.receipt.Prior
	if sameDebugLevel(current, prior) {
		s.setRestored(current)
		return s.finish(ceph.DebugLevelRestored, current, nil)
	}
	if !sameDebugLevel(current, s.owned) {
		return s.finish(ceph.DebugLevelConflict, current, fmt.Errorf("%s of %s changed outside this window to %q; it was not restored", name, daemon, current))
	}
	settle, setErr := s.target.Set(ctx, s.identity, name, prior)
	if setErr != nil {
		setErr = fmt.Errorf("restore %s of %s: %w", name, daemon, setErr)
	}
	if refusal := setRefusal(nil); errors.As(setErr, &refusal) {
		if refusal.Changed() {
			return s.finish(ceph.DebugLevelTargetChanged, "", setErr)
		}
		s.record(ceph.DebugLevelUnconfirmed, "", setErr)
		return setErr
	}
	s.pending = settle
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), s.bound)
	defer readCancel()
	settleErr := s.settle(readCtx)
	value, readErr := s.target.Get(readCtx, name)
	changed, genErr := s.sameGeneration(readCtx)
	if readErr != nil {
		readErr = fmt.Errorf("read back %s of %s: %w", name, daemon, readErr)
		value = ""
	}
	switch {
	case settleErr != nil:
		// No terminal state releases the window while its set may run.
		err := errors.Join(setErr, settleErr, readErr, genErr)
		s.record(ceph.DebugLevelUnconfirmed, value, err)
		return err
	case changed:
		return s.finish(ceph.DebugLevelTargetChanged, value, errors.Join(setErr, genErr))
	case readErr != nil || genErr != nil:
		err := errors.Join(setErr, readErr, genErr)
		s.record(ceph.DebugLevelUnconfirmed, value, err)
		return err
	case sameDebugLevel(value, prior):
		// The readback proves restoration even after a lost reply, whose
		// error the transition keeps.
		s.setRestored(value)
		s.record(ceph.DebugLevelRestored, value, setErr)
		s.registry.Release(s.key, s)
		return nil
	case sameDebugLevel(value, s.owned):
		err := errors.Join(setErr, fmt.Errorf("%s of %s still reads back the window's %q", name, daemon, value))
		s.record(ceph.DebugLevelUnconfirmed, value, err)
		return err
	}
	return s.finish(ceph.DebugLevelConflict, value, errors.Join(setErr, fmt.Errorf("%s of %s reads back %q, neither the prior nor the window's value; it was not restored", name, daemon, value)))
}

func (s *debugLevelState) setRestored(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipt.Restored = value
}
