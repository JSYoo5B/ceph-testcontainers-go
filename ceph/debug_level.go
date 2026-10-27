package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// DebugLevelSetting selects one runtime debug level of one running daemon.
// Daemon is an owned OSD such as "osd.0"; Name is "debug_ms" or "debug_osd";
// Level is "N" or "N/M", each from 0 to 99, as Ceph's config set accepts it.
// Other roles and options are not supported yet.
type DebugLevelSetting struct {
	Daemon, Name, Level string
}

// DebugLevelState is the progress of a DebugLevelOverrideHandle. A window
// moves from DebugLevelPriorRead through DebugLevelApplyAttempted to
// DebugLevelApplied or DebugLevelApplyUncertain, and Restore moves it through
// DebugLevelRestoreAttempted to DebugLevelRestored, DebugLevelConflict,
// DebugLevelTargetChanged or DebugLevelUnconfirmed.
type DebugLevelState string

const (
	// DebugLevelPriorRead: the daemon's effective prior level was read.
	DebugLevelPriorRead DebugLevelState = "prior-read"
	// DebugLevelApplyAttempted: the set was issued; its outcome is not
	// known yet.
	DebugLevelApplyAttempted DebugLevelState = "apply-attempted"
	// DebugLevelApplied: the effective readback equals the requested level.
	DebugLevelApplied DebugLevelState = "applied-verified"
	// DebugLevelApplyUncertain: the set or its readback failed or differs;
	// the level may have changed, and Restore reconciles it.
	DebugLevelApplyUncertain DebugLevelState = "apply-uncertain"
	// DebugLevelRestoreAttempted: Restore has started and not settled yet.
	DebugLevelRestoreAttempted DebugLevelState = "restore-attempted"
	// DebugLevelRestored: the effective readback equals the prior level.
	// Terminal.
	DebugLevelRestored DebugLevelState = "restored-verified"
	// DebugLevelConflict: the daemon showed a level that is neither the
	// prior nor the window's own, so it was left as it is. Terminal.
	DebugLevelConflict DebugLevelState = "conflict"
	// DebugLevelTargetChanged: the daemon's container generation was
	// stopped, restarted, replaced or removed, or the cluster terminated, so
	// the window's process is gone and nothing was restored. Terminal.
	DebugLevelTargetChanged DebugLevelState = "target-changed"
	// DebugLevelUnconfirmed: restoration could not be verified, for example
	// with a paused daemon or an unavailable socket; Restore may be retried.
	DebugLevelUnconfirmed DebugLevelState = "unconfirmed"
)

// DebugLevelReceipt is the JSON-serializable record of one window. Requested
// is the caller's text and Applied, Restored and each transition's Readback
// are the daemon's normalized effective values; an empty value was not read.
type DebugLevelReceipt struct {
	Daemon string `json:"daemon"`
	Name   string `json:"name"`
	// Generation is the daemon container's ID and start time that the
	// window was opened on.
	Generation  string                 `json:"generation"`
	Requested   string                 `json:"requested"`
	Prior       string                 `json:"prior"`
	Applied     string                 `json:"applied,omitempty"`
	Restored    string                 `json:"restored,omitempty"`
	State       DebugLevelState        `json:"state"`
	Transitions []DebugLevelTransition `json:"transitions"`
}

// DebugLevelTransition is one recorded state change of a window.
type DebugLevelTransition struct {
	State    DebugLevelState `json:"state"`
	At       time.Time       `json:"at"`
	Readback string          `json:"readback,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// DebugLevelOverrideHandle manages one runtime debug level window and its
// restoration. Copies share state. A non-nil handle returned with an error
// may still own a changed level: Restore it as well.
type DebugLevelOverrideHandle struct {
	state *debugLevelState
}

// String names the window's daemon and option without its values.
func (h DebugLevelOverrideHandle) String() string {
	if h.state == nil {
		return "Ceph debug level override"
	}
	return "Ceph debug level override " + h.state.setting.Daemon + " " + h.state.setting.Name
}

// GoString formats the window like String.
func (h DebugLevelOverrideHandle) GoString() string { return h.String() }

// Receipt returns a copy of the window's record.
func (h *DebugLevelOverrideHandle) Receipt() DebugLevelReceipt {
	if h == nil || h.state == nil {
		return DebugLevelReceipt{Transitions: []DebugLevelTransition{}}
	}
	s := h.state
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt := s.receipt
	receipt.Transitions = append([]DebugLevelTransition{}, s.receipt.Transitions...)
	return receipt
}

// OverrideDebugLevel changes one runtime debug level of one running owned
// OSD through its admin socket, as AdminSocket reaches it, until Restore. It
// reads the daemon's effective prior level, sets the requested level and
// verifies the normalized effective readback, so "20" reads back as
// "20/20". It changes the daemon's runtime value, never the central
// configuration database, and a daemon restart drops it; see OverrideConfig
// for stored entries. The window is bound to the daemon container's
// generation. Only one window per daemon and option may be open; a second
// one is refused until the first is restored or reaches a terminal state.
//
// When the set or its readback fails, the returned non-nil handle keeps the
// prior level for Restore, which reconciles first: a lost reply is not
// evidence that nothing changed. The readback after a set uses a fresh
// context bounded by the cluster's startup timeout, as does every call.
//
// Restore with a context of its own, not the workload's, so that an ended
// workload still restores. With TerminateOnCleanup, register the removal of
// clients first and then Finalization.Cleanup with Restore: the cleanups run
// in reverse order after the failure archive, so a known failure is archived
// while the level is still raised, the restoration runs before the clients
// are removed, and a failed restoration triggers an after-cleanup archive and
// still lets Terminate run.
//
// Each set is admitted only while the daemon still runs the window's
// generation, and the cluster's own topology operations, such as OSDContainer.Remove,
// OSDContainer.Detach and DetachedOSD.Reopen, wait until the set has ended. A set whose end
// cannot be confirmed may still run: the window keeps it, and no later set or
// terminal state is taken until it is confirmed stopped. Until then OSDContainer.Remove
// and OSDContainer.Detach first stop it, or refuse to replace the OSD.
//
// The readbacks around each set are not an atomic compare-and-swap: another
// writer of the same daemon and option must not race the window, and an
// outside write of the window's own value cannot be told apart from it. A
// restart of the daemon's container from outside the cluster after the last
// check is not held off either.
func (c *Container) OverrideDebugLevel(ctx context.Context, setting DebugLevelSetting) (*DebugLevelOverrideHandle, error) {
	if gateErr := c.sharedGate("OverrideDebugLevel"); gateErr != nil {
		return nil, gateErr
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	setting, owned, err := normalizeDebugLevelSetting(setting)
	if err != nil {
		return nil, err
	}
	kind, id, err := parseAdminSocketDaemon(setting.Daemon)
	if err != nil || kind != "osd" {
		return nil, invalid(fmt.Errorf("debug level daemon %q must be an OSD such as osd.0", setting.Daemon))
	}
	n, _ := strconv.Atoi(id)
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	closed, osd := c.closed, c.osds[n]
	owns := c.ownsDaemon(kind, id) && osd != nil && !osd.purgeIssued && !osd.detaching
	c.mu.Unlock()
	switch {
	case closed:
		return nil, errClusterTerminated
	case !owns:
		return nil, invalid(fmt.Errorf("daemon %s is not a running-capable OSD owned by this cluster", setting.Daemon))
	}
	target := &osdDebugTarget{c: c, id: n, daemon: setting.Daemon, osd: osd}
	return openDebugLevel(ctx, &c.debugLevels, target, setting, owned, c.settings.startupTimeout, time.Now)
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

func normalizeDebugLevelSetting(setting DebugLevelSetting) (DebugLevelSetting, string, error) {
	if setting.Name != "debug_ms" && setting.Name != "debug_osd" {
		return setting, "", invalid(fmt.Errorf("debug level option %q is not supported; select debug_ms or debug_osd", setting.Name))
	}
	owned, ok := normalizeDebugLevel(setting.Level)
	if !ok {
		return setting, "", invalid(fmt.Errorf("debug level %q must be N or N/M with numbers from 0 to 99", setting.Level))
	}
	return setting, owned, nil
}

// debugLevelTarget is the access to one daemon generation's runtime level.
type debugLevelTarget interface {
	// generation reports the target's current container generation.
	generation(ctx context.Context) (debugLevelGeneration, error)
	get(ctx context.Context, name string) (string, error)
	// set issues the set only while the target runs generation identity,
	// and a *debugLevelRefusal means that it issued nothing. A non-nil
	// settle means that the set may still run; settle returns nil once it
	// can no longer.
	set(ctx context.Context, identity, name, value string) (settle func(context.Context) error, err error)
}

// debugLevelRefusal is a set that was not issued because the target was not
// the window's running generation; changed marks a terminal mismatch.
type debugLevelRefusal struct {
	changed bool
	err     error
}

func (e *debugLevelRefusal) Error() string { return e.err.Error() }

func (e *debugLevelRefusal) Unwrap() error { return e.err }

type debugLevelGeneration struct {
	// owned is false when the cluster no longer owns the same daemon
	// descriptor, or has terminated.
	owned bool
	// identity is the container ID and start time.
	identity        string
	running, paused bool
}

// debugLevelRegistry admits one window per daemon and option.
type debugLevelRegistry struct {
	mu     sync.Mutex
	active map[string]*debugLevelState
}

func (r *debugLevelRegistry) reserve(key string, s *debugLevelState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] != nil {
		return invalid(errors.New("a debug level window for this daemon and option is open; restore it first"))
	}
	if r.active == nil {
		r.active = make(map[string]*debugLevelState)
	}
	r.active[key] = s
	return nil
}

func (r *debugLevelRegistry) release(key string, s *debugLevelState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] == s {
		delete(r.active, key)
	}
}

type debugLevelState struct {
	registry *debugLevelRegistry
	key      string
	target   debugLevelTarget
	setting  DebugLevelSetting
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

	mu      sync.Mutex
	receipt DebugLevelReceipt
	// terminal is the error that a terminal state other than restored
	// returns again.
	terminal error
}

func (s *debugLevelState) record(state DebugLevelState, readback string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipt.State = state
	transition := DebugLevelTransition{State: state, At: s.now(), Readback: readback}
	if err != nil {
		transition.Error = err.Error()
	}
	s.receipt.Transitions = append(s.receipt.Transitions, transition)
}

func (s *debugLevelState) current() DebugLevelState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipt.State
}

// finish records a terminal state and releases the admission.
func (s *debugLevelState) finish(state DebugLevelState, readback string, err error) error {
	s.record(state, readback, err)
	s.mu.Lock()
	s.terminal = err
	s.mu.Unlock()
	s.registry.release(s.key, s)
	return err
}

// sameGeneration checks the target against the window's generation: changed
// is a terminal mismatch, and err is a check that could not be completed.
func (s *debugLevelState) sameGeneration(ctx context.Context) (changed bool, err error) {
	gen, err := s.target.generation(ctx)
	return checkDebugLevelGeneration(gen, err, s.identity, s.setting.Daemon)
}

func checkDebugLevelGeneration(gen debugLevelGeneration, err error, identity, daemon string) (changed bool, _ error) {
	switch {
	case err != nil:
		return false, fmt.Errorf("inspect %s: %w", daemon, err)
	case !gen.owned:
		return true, fmt.Errorf("%s is no longer owned by this cluster", daemon)
	case gen.identity == "" || gen.identity != identity:
		return true, fmt.Errorf("%s runs in another container generation", daemon)
	case !gen.running:
		return true, fmt.Errorf("%s is stopped; its runtime level is gone", daemon)
	case gen.paused:
		return false, fmt.Errorf("%s is paused", daemon)
	}
	return false, nil
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
		return s.finish(DebugLevelTargetChanged, "", err)
	case err != nil:
		s.record(DebugLevelUnconfirmed, "", err)
	}
	return err
}

func openDebugLevel(ctx context.Context, registry *debugLevelRegistry, target debugLevelTarget, setting DebugLevelSetting, owned string, bound time.Duration, now func() time.Time) (*DebugLevelOverrideHandle, error) {
	s := &debugLevelState{registry: registry, key: setting.Daemon + "/" + setting.Name, target: target, setting: setting,
		owned: owned, bound: bound, now: now, lock: make(chan struct{}, 1),
		receipt: DebugLevelReceipt{Daemon: setting.Daemon, Name: setting.Name, Requested: setting.Level, Transitions: []DebugLevelTransition{}}}
	if err := registry.reserve(s.key, s); err != nil {
		return nil, err
	}
	s.lock <- struct{}{}
	defer func() { <-s.lock }()
	gen, err := target.generation(ctx)
	if err == nil && (!gen.owned || !gen.running || gen.paused || gen.identity == "") {
		err = fmt.Errorf("%s must be an owned running daemon", setting.Daemon)
	}
	if err != nil {
		registry.release(s.key, s)
		return nil, err
	}
	s.identity, s.receipt.Generation = gen.identity, gen.identity
	prior, err := target.get(ctx, setting.Name)
	if err != nil {
		// Nothing was set: no handle is needed.
		registry.release(s.key, s)
		return nil, fmt.Errorf("read the prior %s of %s: %w", setting.Name, setting.Daemon, err)
	}
	s.receipt.Prior = prior
	s.record(DebugLevelPriorRead, prior, nil)
	handle := &DebugLevelOverrideHandle{state: s}

	s.record(DebugLevelApplyAttempted, "", nil)
	settle, setErr := target.set(ctx, s.identity, setting.Name, setting.Level)
	if setErr != nil {
		setErr = fmt.Errorf("set %s of %s: %w", setting.Name, setting.Daemon, setErr)
	}
	if refusal := (*debugLevelRefusal)(nil); errors.As(setErr, &refusal) {
		// The target changed during the prior read and nothing was set.
		registry.release(s.key, s)
		return nil, setErr
	}
	s.pending = settle
	// A lost reply or cancellation does not prove that the set did not
	// apply; read back with a fresh bounded context, after a set that may
	// still run is stopped.
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()
	settleErr := s.settle(readCtx)
	value, readErr := target.get(readCtx, setting.Name)
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
		s.record(DebugLevelApplyUncertain, value, err)
		return handle, err
	case changed:
		return handle, s.finish(DebugLevelTargetChanged, value, errors.Join(setErr, genErr))
	case setErr == nil && readErr == nil && genErr == nil && sameDebugLevel(value, owned):
		s.record(DebugLevelApplied, value, nil)
		return handle, nil
	}
	err = errors.Join(setErr, readErr, genErr)
	if readErr == nil && !sameDebugLevel(value, owned) {
		err = errors.Join(err, fmt.Errorf("%s of %s reads back %q, not the requested %q", setting.Name, setting.Daemon, value, setting.Level))
	}
	s.record(DebugLevelApplyUncertain, value, err)
	return handle, err
}

// Restore returns the daemon's runtime level to the prior value after it
// confirms the same container generation and that the daemon still shows
// the window's own value; it then verifies the restored readback. A prior
// value already in place counts as restored. Another value is an outside
// edit, which Restore leaves as it is. A stopped, restarted or replaced
// daemon is not restored. These outcomes are terminal: Restore returns nil
// once restored and the same error again otherwise. A paused daemon, an
// unavailable socket, an unverified readback or an earlier set of the window
// that may still run leave the window open, and a later Restore settles and
// reconciles it first. The generation is checked again after each read. The
// readback after the set and the settling of a set use fresh contexts bounded
// by the cluster's startup timeout.
func (h *DebugLevelOverrideHandle) Restore(ctx context.Context) error {
	if h == nil || h.state == nil {
		return errors.New("debug level override is unavailable")
	}
	s := h.state
	select {
	case s.lock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.lock }()
	switch s.current() {
	case DebugLevelRestored:
		return nil
	case DebugLevelConflict, DebugLevelTargetChanged:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.terminal
	}
	ctx, cancel := context.WithTimeout(ctx, s.bound)
	defer cancel()
	name, daemon := s.setting.Name, s.setting.Daemon
	s.record(DebugLevelRestoreAttempted, "", nil)
	// A set that may still run could change the level after any check.
	if err := s.settle(ctx); err != nil {
		s.record(DebugLevelUnconfirmed, "", err)
		return err
	}
	if err := s.confirmTarget(ctx); err != nil {
		return err
	}
	current, err := s.target.get(ctx, name)
	if err != nil {
		err = fmt.Errorf("read the current %s of %s: %w", name, daemon, err)
		s.record(DebugLevelUnconfirmed, "", err)
		return err
	}
	// A replacement during the read would make current another generation's.
	if err := s.confirmTarget(ctx); err != nil {
		return err
	}
	prior := s.receipt.Prior
	if sameDebugLevel(current, prior) {
		s.setRestored(current)
		return s.finish(DebugLevelRestored, current, nil)
	}
	if !sameDebugLevel(current, s.owned) {
		return s.finish(DebugLevelConflict, current, fmt.Errorf("%s of %s changed outside this window to %q; it was not restored", name, daemon, current))
	}
	settle, setErr := s.target.set(ctx, s.identity, name, prior)
	if setErr != nil {
		setErr = fmt.Errorf("restore %s of %s: %w", name, daemon, setErr)
	}
	if refusal := (*debugLevelRefusal)(nil); errors.As(setErr, &refusal) {
		if refusal.changed {
			return s.finish(DebugLevelTargetChanged, "", setErr)
		}
		s.record(DebugLevelUnconfirmed, "", setErr)
		return setErr
	}
	s.pending = settle
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), s.bound)
	defer readCancel()
	settleErr := s.settle(readCtx)
	value, readErr := s.target.get(readCtx, name)
	changed, genErr := s.sameGeneration(readCtx)
	if readErr != nil {
		readErr = fmt.Errorf("read back %s of %s: %w", name, daemon, readErr)
		value = ""
	}
	switch {
	case settleErr != nil:
		// No terminal state releases the window while its set may run.
		err := errors.Join(setErr, settleErr, readErr, genErr)
		s.record(DebugLevelUnconfirmed, value, err)
		return err
	case changed:
		return s.finish(DebugLevelTargetChanged, value, errors.Join(setErr, genErr))
	case readErr != nil || genErr != nil:
		err := errors.Join(setErr, readErr, genErr)
		s.record(DebugLevelUnconfirmed, value, err)
		return err
	case sameDebugLevel(value, prior):
		// The readback proves restoration even after a lost reply, whose
		// error the transition keeps.
		s.setRestored(value)
		s.record(DebugLevelRestored, value, setErr)
		s.registry.release(s.key, s)
		return nil
	case sameDebugLevel(value, s.owned):
		err := errors.Join(setErr, fmt.Errorf("%s of %s still reads back the window's %q", name, daemon, value))
		s.record(DebugLevelUnconfirmed, value, err)
		return err
	}
	return s.finish(DebugLevelConflict, value, errors.Join(setErr, fmt.Errorf("%s of %s reads back %q, neither the prior nor the window's value; it was not restored", name, daemon, value)))
}

func (s *debugLevelState) setRestored(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipt.Restored = value
}

// osdDebugTarget reaches one OSD descriptor's admin socket.
type osdDebugTarget struct {
	c      *Container
	id     int
	daemon string
	osd    *OSDContainer
}

func (t *osdDebugTarget) generation(ctx context.Context) (debugLevelGeneration, error) {
	if err := t.c.lockTopology(ctx); err != nil {
		return debugLevelGeneration{}, err
	}
	defer t.c.mu.Unlock()
	return t.generationLocked(ctx)
}

// generationLocked is generation for a caller that holds c.mu.
func (t *osdDebugTarget) generationLocked(ctx context.Context) (debugLevelGeneration, error) {
	c := t.c
	osd := c.osds[t.id]
	owned := !c.closed && osd == t.osd && osd != nil && osd.Container != nil && !osd.purged && !osd.purgeIssued && !osd.detaching
	if !owned {
		return debugLevelGeneration{}, nil
	}
	state, err := osd.State(ctx)
	if err != nil {
		return debugLevelGeneration{}, err
	}
	return debugLevelGeneration{owned: true, identity: daemonIdentity(osd.Container, state), running: state.Running, paused: state.Paused}, nil
}

func (t *osdDebugTarget) get(ctx context.Context, name string) (string, error) {
	out, err := t.osd.AdminSocket(ctx, "config", "get", name)
	if err != nil {
		return "", err
	}
	return decodeDebugLevel(out, name)
}

// set holds c.mu, which the cluster's topology operations hold, from the
// generation check until the command has ended, so that the cluster cannot
// replace the OSD in between. The command is registered as a native
// invocation of the OSD's container instance, as RunNativeCommand's are, and
// one whose end is unconfirmed stays registered until settle confirms it: the
// fence of OSDContainer.Remove and OSDContainer.Detach drains it first or refuses, and Terminate
// stops it.
func (t *osdDebugTarget) set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	c := t.c
	if err := c.lockTopology(ctx); err != nil {
		return nil, &debugLevelRefusal{err: err}
	}
	defer c.mu.Unlock()
	gen, err := t.generationLocked(ctx)
	if changed, err := checkDebugLevelGeneration(gen, err, identity, t.daemon); err != nil {
		return nil, &debugLevelRefusal{changed: changed, err: err}
	}
	if c.run == nil || c.run.removed {
		return nil, &debugLevelRefusal{err: errors.New("this cluster has no shared run directory for admin sockets")}
	}
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return nil, &debugLevelRefusal{err: fmt.Errorf("select admin socket control: %w", err)}
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// The invocation runs in control but addresses the OSD instance, which
	// its fence names.
	inv := &nativeInvocation{owner: c, target: control, targetID: t.osd.GetContainerID(), label: "admin socket",
		cancel: cancel, stopCommand: supervised.Cancel, done: make(chan struct{})}
	switch c.nativeCmds.admit(inv, nil) {
	case nativeRefusedClosed:
		return nil, &debugLevelRefusal{changed: true, err: errClusterTerminated}
	case nativeRefusedFenced:
		return nil, &debugLevelRefusal{changed: true, err: fmt.Errorf("%s is fenced for removal", t.daemon)}
	}

	_, err = c.adminSocketCommand(runCtx, control, t.daemon, "config", "set", name, value)
	marker, unconfirmed := supervised.Unconfirmed(err)
	c.nativeCmds.finish(inv, marker, unconfirmed)
	close(inv.done)
	if !unconfirmed {
		return nil, err
	}
	return func(ctx context.Context) error {
		if err := inv.stop(ctx); err != nil {
			return err
		}
		c.nativeCmds.forget(inv)
		return nil
	}, err
}

// decodeDebugLevel reads the reply of config get, {"<name>": "<value>"}.
func decodeDebugLevel(out []byte, name string) (string, error) {
	var reply map[string]string
	if err := json.Unmarshal(out, &reply); err != nil {
		return "", fmt.Errorf("decode config get %s", name)
	}
	value, ok := reply[name]
	if !ok || value == "" {
		return "", fmt.Errorf("config get %s returned no value", name)
	}
	return value, nil
}
