package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// DebugLevelState is the progress of an override.DebugLevelHandle. A window
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
	// DebugLevelNotApplied: the request was refused before any native
	// step, so nothing was applied and there is nothing to restore. Reason
	// holds the refusal. A window that may have been applied never reports
	// this state.
	DebugLevelNotApplied DebugLevelState = "not-applied"
)

// DebugLevelReceipt is the JSON-serializable record of one window. Requested
// is the caller's text and Applied, Restored and each transition's Readback
// are the daemon's normalized effective values; an empty value was not read.
type DebugLevelReceipt struct {
	Daemon string `json:"daemon"`
	Name   string `json:"name"`
	// Generation is the daemon container's ID and start time that the
	// window was opened on.
	Generation string          `json:"generation"`
	Requested  string          `json:"requested"`
	Prior      string          `json:"prior"`
	Applied    string          `json:"applied,omitempty"`
	Restored   string          `json:"restored,omitempty"`
	State      DebugLevelState `json:"state"`
	// Reason is the refusal text of a DebugLevelNotApplied window and is
	// empty for every other state.
	Reason      string                 `json:"reason,omitempty"`
	Transitions []DebugLevelTransition `json:"transitions"`
}

// DebugLevelTransition is one recorded state change of a window.
type DebugLevelTransition struct {
	State    DebugLevelState `json:"state"`
	At       time.Time       `json:"at"`
	Readback string          `json:"readback,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// debugLevelRefusal is a set that was not issued because the target was not
// the window's running generation; changed marks a terminal mismatch.
type debugLevelRefusal struct {
	changed bool
	err     error
}

func (e *debugLevelRefusal) Error() string { return e.err.Error() }

func (e *debugLevelRefusal) Unwrap() error { return e.err }

// Changed reports a terminal mismatch. The override package reads it through
// an interface, because it cannot name this type.
func (e *debugLevelRefusal) Changed() bool { return e.changed }

type debugLevelGeneration struct {
	// owned is false when the cluster no longer owns the same daemon
	// descriptor, or has terminated.
	owned bool
	// identity is the container ID and start time.
	identity        string
	running, paused bool
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

// debugLevelRegistry admits one window per daemon and option. It has its own
// leaf lock on purpose: releasing a window must not wait behind a long
// topology operation that holds the cluster mutex. The owner of a slot is
// opaque; the override package passes its window state.
type debugLevelRegistry struct {
	mu     sync.Mutex
	active map[string]any
}

// Reserve admits the window owner for key, or refuses an open window.
func (r *debugLevelRegistry) Reserve(key string, owner any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] != nil {
		return invalid(errors.New("a debug level window for this daemon and option is open; restore it first"))
	}
	if r.active == nil {
		r.active = make(map[string]any)
	}
	r.active[key] = owner
	return nil
}

// Release frees key while owner still holds it.
func (r *debugLevelRegistry) Release(key string, owner any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] == owner {
		delete(r.active, key)
	}
}

// openDebugTarget checks that daemon is an OSD that this cluster owns and can
// run, and returns the target of its admin socket. It waits for the topology
// lock under ctx and releases it before it returns.
func (c *Container) openDebugTarget(ctx context.Context, daemon string) (*osdDebugTarget, error) {
	kind, id, err := parseAdminSocketDaemon(daemon)
	if err != nil || kind != "osd" {
		return nil, invalid(fmt.Errorf("debug level daemon %q must be an OSD such as osd.0", daemon))
	}
	n, _ := strconv.Atoi(id)
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
		return nil, invalid(fmt.Errorf("daemon %s is not a running-capable OSD owned by this cluster", daemon))
	}
	return &osdDebugTarget{c: c, id: n, daemon: daemon, osd: osd}, nil
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

// Generation reports the target's current container generation as plain
// values, for the override package.
func (t *osdDebugTarget) Generation(ctx context.Context) (owned bool, identity string, running, paused bool, err error) {
	gen, err := t.generation(ctx)
	return gen.owned, gen.identity, gen.running, gen.paused, err
}

// Get reads the daemon's effective level of option name.
func (t *osdDebugTarget) Get(ctx context.Context, name string) (string, error) {
	return t.get(ctx, name)
}

// Set issues the set of option name only while the daemon runs generation
// identity. A refusal reports Changed() and issued nothing.
func (t *osdDebugTarget) Set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	return t.set(ctx, identity, name, value)
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
