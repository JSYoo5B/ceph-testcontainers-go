package mirror

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/moby/moby/api/types/container"
)

// cephFSRemovalCohort is the live daemon cohort that a peer removal and a
// directory removal capture before their native remove, and the checks whose
// rules the two removals share: every owned daemon runs normally, witnesses
// the owned peer in a filesystem session that is a native watcher and lists
// the exact peer command, the watcher set holds exactly the cohort, and an
// optional process observer is bound to each original run. Capture and every
// later observation confirm each witness the same way (confirmWitness); only
// the read between the two session reads belongs to the removal: a peer drain
// waits for the peer command to unregister, a directory release for the path
// to leave the peer stats. Its methods run while Container.mu is held.
type cephFSRemovalCohort struct {
	scope     *cephFSOriginalScope
	witnesses []cephFSRemovalWitness
}

// cephFSRemovalWitness is one original daemon as the cohort captured it.
type cephFSRemovalWitness struct {
	daemon                                            *Daemon
	name, containerID, startedAt, address, instanceID string
	originalProcess                                   *cephFSBoundOriginalProcess
}

func (c *cephFSRemovalCohort) capture(ctx context.Context) error {
	watchers, err := c.scope.watchers(ctx)
	if err != nil {
		return err
	}
	ids, names := make(map[string]bool), make(map[string]bool)
	for _, daemon := range c.scope.mirror.roster.daemons {
		if daemon == nil {
			return cephFSObserveGuard("original CephFS peer cohort contains an unavailable daemon")
		}
		if err := clusterutil.LockMutex(ctx, &daemon.mu); err != nil {
			return err
		}
		w, err := c.captureDaemon(ctx, daemon, watchers)
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
		if ids[w.containerID] || names[w.name] {
			return cephFSObserveGuard("original CephFS peer cohort identity is ambiguous")
		}
		ids[w.containerID], names[w.name] = true, true
		c.witnesses = append(c.witnesses, w)
	}
	if c.scope.mirror.roster.observesProcesses() {
		if err := c.validateProcesses(ctx); err != nil {
			return err
		}
	}
	if err := c.checkWatchers(watchers); err != nil {
		return err
	}
	after, err := c.scope.watchers(ctx)
	if err != nil {
		return err
	}
	if !maps.Equal(watchers, after) {
		return cephFSObserveGuard("CephFS original filesystem watchers changed during removal preflight")
	}
	return c.checkWatchers(after)
}

// captureDaemon records daemon as a witness through the confirmation that
// every later observation repeats, in its capturing form, then binds its
// original process when the fixture observes processes.
func (c *cephFSRemovalCohort) captureDaemon(ctx context.Context, daemon *Daemon, watchers map[string]string) (cephFSRemovalWitness, error) {
	w := cephFSRemovalWitness{daemon: daemon, name: daemon.DaemonName}
	if daemon.removed || daemon.Container == nil || !validCephFSContainerID(daemon.GetContainerID()) || w.name == "" {
		return w, cephFSObserveGuard("original CephFS daemon ownership is unavailable")
	}
	w.containerID = daemon.GetContainerID()
	if _, err := c.confirmWitness(ctx, &w, watchers, cephFSWitnessCheck{
		subject: "CephFS removal preflight",
		capture: true,
		read: func(ctx context.Context, w *cephFSRemovalWitness, before cephFSDaemonSession) (string, error) {
			if !before.peerPresent {
				return "", cephFSObserveGuard("original CephFS daemon did not witness the owned peer")
			}
			w.instanceID = watchers[before.address]
			if gid, err := strconv.ParseUint(w.instanceID, 10, 64); err != nil || gid == 0 {
				return "", cephFSObserveGuard("original CephFS filesystem watcher is unavailable")
			}
			registered, err := c.scope.catalog(ctx, w.daemon)
			if err != nil {
				return "", err
			}
			if !registered {
				return "", cephFSObserveGuard("original exact CephFS peer command was not witnessed before removal")
			}
			return "", nil
		},
	}); err != nil {
		return w, err
	}
	if c.scope.mirror.roster.observesProcesses() {
		if daemon.originalProcess == nil {
			return w, cephFSObserveGuard("original raw process observer binding is unavailable")
		}
		w.originalProcess = daemon.originalProcess
		if err := confirmCephFSOriginalRun(ctx, w); err != nil {
			return w, err
		}
	}
	return w, ctx.Err()
}

// cephFSWitnessCheck is one removal's use of the witness confirmation: the
// subject its refusals name, the read it makes between the two session
// reads, and how a change of peer presence during that read is reported.
type cephFSWitnessCheck struct {
	subject string
	// capture records the process start and the session address that the
	// confirmation reads instead of comparing them, and refuses every
	// deviation as a guard, so that only a normal live process becomes a
	// witness.
	capture bool
	// peerMayLeave reports a session whose peer presence changed during
	// read as pending, because the peer drain waits for exactly that;
	// otherwise the change is a changed session.
	peerMayLeave bool
	// read is the per-removal read. It gets the session read before it and
	// returns the witness state.
	read func(ctx context.Context, w *cephFSRemovalWitness, before cephFSDaemonSession) (string, error)
}

// confirmWitness reads one original daemon in its original process and
// filesystem session around check.read: the container identity, a normal
// process with the original start, the session and its native watcher, the
// read, the session again and the process again. It returns the read's state,
// or the state of the first step that failed: retired (the daemon left the
// fixture), stopped or unavailable (retryable query errors), or
// session-changed (a guard error). The caller holds the daemon's lock.
func (c *cephFSRemovalCohort) confirmWitness(ctx context.Context, w *cephFSRemovalWitness, watchers map[string]string, check cephFSWitnessCheck) (string, error) {
	if !check.capture {
		if w.daemon == nil || w.daemon.Container == nil || w.daemon.removed {
			return "retired", cephFSObserveQuery(check.subject+" original daemon is no longer owned", nil)
		}
		if w.daemon.DaemonName != w.name || w.daemon.GetContainerID() != w.containerID {
			return "session-changed", cephFSObserveGuard(check.subject + " original container identity changed")
		}
	}
	if state, err := c.confirmProcess(ctx, w, check, "inspect"); err != nil {
		return state, err
	}
	before, err := c.scope.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if check.capture {
		w.address = before.address
	} else if before.address != w.address || watchers[w.address] != w.instanceID {
		return "session-changed", cephFSObserveGuard(check.subject + " original filesystem session changed")
	}
	state, err := check.read(ctx, w, before)
	if err != nil {
		return state, err
	}
	after, err := c.scope.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if after.address != before.address {
		return "session-changed", cephFSObserveGuard(check.subject + " original filesystem session changed during observation")
	}
	if after.peerPresent != before.peerPresent {
		if check.peerMayLeave {
			return "pending", cephFSObserveQuery(check.subject+" peer changed during observation", nil)
		}
		return "session-changed", cephFSObserveGuard(check.subject + " original peer changed during observation")
	}
	if state, err := c.confirmProcess(ctx, w, check, "confirm"); err != nil {
		return state, err
	}
	if err := ctx.Err(); err != nil {
		return "unavailable", err
	}
	return state, nil
}

// confirmProcess reads the witness's container state: a normal process
// with the original start in the original container. A capturing first
// read records the start instead.
func (c *cephFSRemovalCohort) confirmProcess(ctx context.Context, w *cephFSRemovalWitness, check cephFSWitnessCheck, step string) (string, error) {
	state, err := w.daemon.State(ctx)
	if err != nil {
		return "unavailable", cephFSObserveQuery(step+" "+check.subject+" original process", err)
	}
	if !normalCephFSMirrorProcess(state) {
		if check.capture {
			return "", cephFSObserveGuard(check.subject + " requires every daemon running normally")
		}
		return "stopped", cephFSObserveQuery(check.subject+" original process is not running normally", nil)
	}
	if check.capture && w.startedAt == "" {
		if !validProcessTime(state.StartedAt) {
			return "", cephFSObserveGuard(check.subject + " original process generation is unavailable")
		}
		w.startedAt = state.StartedAt
		return "", nil
	}
	if !sameCephFSProcessStart(state.StartedAt, w.startedAt) || w.daemon.GetContainerID() != w.containerID {
		return "session-changed", cephFSObserveGuard(check.subject + " original process generation changed")
	}
	return "", nil
}

// observe confirms every witness with check, each under its own lock, and
// records its state and problem through record. Both removals read the
// watcher set and check it against the cohort before any witness: a foreign
// or retired watcher means the session that a witness reports is not proven
// to be the only one, so no witness state can be release or drain evidence.
// The set is read again after the witnesses and must be unchanged. It returns
// whether every witness reached done, the joined observation errors, and
// whether a guard ended the observation before its last step.
func (c *cephFSRemovalCohort) observe(ctx context.Context, check cephFSWitnessCheck, done string, record func(name, state, problem string)) (bool, error, bool) {
	before, err := c.scope.watchers(ctx)
	if err != nil {
		return false, err, true
	}
	if err := c.checkWatchers(before); err != nil {
		return false, err, true
	}
	all := true
	var errs []error
	for i := range c.witnesses {
		w := &c.witnesses[i]
		if err := clusterutil.LockMutex(ctx, &w.daemon.mu); err != nil {
			all = false
			errs = append(errs, err)
			break
		}
		state, err := c.confirmWitness(ctx, w, before, check)
		w.daemon.mu.Unlock()
		problem := ""
		if err != nil {
			all = false
			problem = err.Error()
			errs = append(errs, err)
		}
		if state != done {
			all = false
		}
		record(w.name, state, problem)
		if errors.Is(err, errCephFSObservationGuard) {
			return false, errors.Join(errs...), true
		}
	}
	after, err := c.scope.watchers(ctx)
	if err != nil {
		return false, errors.Join(append(errs, err)...), true
	}
	if !maps.Equal(before, after) {
		all = false
		errs = append(errs, cephFSObserveGuard(check.subject+" original watcher set changed during observation"))
	}
	if err := c.checkWatchers(after); err != nil {
		all = false
		errs = append(errs, err)
	}
	return all && len(errs) == 0, errors.Join(errs...), false
}

func (c *cephFSRemovalCohort) checkWatchers(watchers map[string]string) error {
	if len(watchers) != len(c.witnesses) {
		return cephFSObserveGuard("CephFS removal has foreign or retired filesystem watchers")
	}
	seen := make(map[string]bool)
	for _, w := range c.witnesses {
		if w.instanceID == "" || watchers[w.address] != w.instanceID || seen[w.instanceID] {
			return cephFSObserveGuard("CephFS removal original watcher cohort differs")
		}
		seen[w.instanceID] = true
	}
	return nil
}

func (c *cephFSRemovalCohort) validateProcesses(ctx context.Context) error {
	if c == nil || c.scope == nil || c.scope.mirror == nil || len(c.witnesses) == 0 {
		return cephFSObserveGuard("original process cohort is empty")
	}
	owned := make(map[*Daemon]bool)
	names, ids, gids := make(map[string]bool), make(map[string]bool), make(map[uint64]bool)
	for _, w := range c.witnesses {
		if w.daemon == nil {
			return cephFSObserveGuard("original daemon identity is unavailable")
		}
		if err := clusterutil.LockMutex(ctx, &w.daemon.mu); err != nil {
			return err
		}
		sameOwner := w.originalProcess != nil && w.originalProcess.originalHandle == w.daemon.Container && w.daemon.originalProcess == w.originalProcess && w.daemon.DaemonName == w.name
		w.daemon.mu.Unlock()
		gid, err := strconv.ParseUint(w.instanceID, 10, 64)
		if !sameOwner || w.originalProcess.containerID != w.containerID || w.address == "" || !validProcessTime(w.startedAt) || err != nil || gid == 0 || names[w.name] || ids[w.containerID] || gids[gid] || owned[w.daemon] {
			return cephFSObserveGuard("original process cohort binding or numeric watcher identity differs")
		}
		owned[w.daemon], names[w.name], ids[w.containerID], gids[gid] = true, true, true, true
	}
	for _, d := range c.scope.mirror.roster.daemons {
		if !owned[d] {
			return cephFSObserveGuard("current process membership is outside the original cohort")
		}
	}
	return nil
}

func validCephFSContainerID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, r := range id {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

// validProcessTime reports a parseable, non-zero container start or finish
// time; Docker reports the zero time 0001-01-01 for a container that never
// ran.
func validProcessTime(value string) bool {
	v, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && !v.IsZero()
}

// sameCephFSProcessStart reports two valid start times of one process run.
func sameCephFSProcessStart(a, b string) bool {
	left, e1 := time.Parse(time.RFC3339Nano, a)
	right, e2 := time.Parse(time.RFC3339Nano, b)
	return e1 == nil && e2 == nil && !left.IsZero() && !right.IsZero() && left.Equal(right)
}

// normalCephFSMirrorProcess reports a running process that is not paused,
// restarting or dead.
func normalCephFSMirrorProcess(s *container.State) bool {
	return s != nil && s.Running && !s.Paused && !s.Restarting && !s.Dead
}
