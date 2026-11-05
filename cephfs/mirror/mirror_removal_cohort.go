package mirror

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/moby/moby/api/types/container"
)

// cephFSRemovalCohort is the live daemon cohort that a peer removal and a
// directory removal capture before their native remove, and the checks whose
// rules the two removals share: every owned daemon runs normally, witnesses
// the owned peer in a filesystem session that is a native watcher and lists
// the exact peer command, the watcher set holds exactly the cohort, and an
// optional process observer is bound to each original run. The per-daemon
// completion observations differ (a peer drain waits for the peer command to
// unregister, a directory release for the path to leave the peer stats), so
// each removal keeps its own. Its methods run while Container.mu is held.
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
		if err := multicluster.LockObservation(ctx, &daemon.mu); err != nil {
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
	if !equalCephFSRemovalWatchers(watchers, after) {
		return cephFSObserveGuard("CephFS original filesystem watchers changed during removal preflight")
	}
	return c.checkWatchers(after)
}

func (c *cephFSRemovalCohort) captureDaemon(ctx context.Context, daemon *Daemon, watchers map[string]string) (cephFSRemovalWitness, error) {
	w := cephFSRemovalWitness{daemon: daemon, name: daemon.DaemonName}
	if daemon.removed || daemon.Container == nil || !validCephFSRemovalContainerID(daemon.GetContainerID()) || w.name == "" {
		return w, cephFSObserveGuard("original CephFS daemon ownership is unavailable")
	}
	w.containerID = daemon.GetContainerID()
	state, err := daemon.State(ctx)
	if err != nil {
		return w, cephFSObserveQuery("inspect original CephFS mirror process", err)
	}
	if !normalCephFSRemovalProcess(state) {
		return w, cephFSObserveGuard("strict CephFS peer removal requires every daemon running normally")
	}
	if _, err := time.Parse(time.RFC3339Nano, state.StartedAt); err != nil || strings.HasPrefix(state.StartedAt, "0001-") {
		return w, cephFSObserveGuard("original CephFS process generation is unavailable")
	}
	w.startedAt = state.StartedAt
	session, err := c.scope.session(ctx, daemon)
	if err != nil {
		return w, err
	}
	if !session.peerPresent {
		return w, cephFSObserveGuard("original CephFS daemon did not witness the owned peer")
	}
	w.address, w.instanceID = session.address, watchers[session.address]
	if gid, err := strconv.ParseUint(w.instanceID, 10, 64); err != nil || gid == 0 {
		return w, cephFSObserveGuard("original CephFS filesystem watcher is unavailable")
	}
	registered, err := c.scope.catalog(ctx, daemon)
	if err != nil {
		return w, err
	}
	if !registered {
		return w, cephFSObserveGuard("original exact CephFS peer command was not witnessed before removal")
	}
	after, err := c.scope.session(ctx, daemon)
	if err != nil {
		return w, err
	}
	if after != session {
		return w, cephFSObserveGuard("original CephFS filesystem session changed during removal preflight")
	}
	state, err = daemon.State(ctx)
	if err != nil {
		return w, cephFSObserveQuery("confirm original CephFS mirror process", err)
	}
	if !normalCephFSRemovalProcess(state) || !sameCephFSRemovalStartedAt(state.StartedAt, w.startedAt) || daemon.GetContainerID() != w.containerID {
		return w, cephFSObserveGuard("original CephFS process changed during removal preflight")
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
		gid, err := strconv.ParseUint(w.instanceID, 10, 64)
		if w.daemon == nil {
			return cephFSObserveGuard("original daemon identity is unavailable")
		}
		if err := multicluster.LockObservation(ctx, &w.daemon.mu); err != nil {
			return err
		}
		sameOwner := w.originalProcess != nil && w.originalProcess.originalHandle == w.daemon.Container && w.daemon.originalProcess == w.originalProcess && w.daemon.DaemonName == w.name
		w.daemon.mu.Unlock()
		if !sameOwner || w.daemon == nil || w.originalProcess == nil || w.daemon.originalProcess != w.originalProcess || w.daemon.DaemonName != w.name || w.originalProcess.containerID != w.containerID || w.address == "" || !validProcessTime(w.startedAt) || err != nil || gid == 0 || names[w.name] || ids[w.containerID] || gids[gid] || owned[w.daemon] {
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

func validCephFSRemovalContainerID(id string) bool {
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

func sameCephFSRemovalStartedAt(a, b string) bool {
	left, e1 := time.Parse(time.RFC3339Nano, a)
	right, e2 := time.Parse(time.RFC3339Nano, b)
	return e1 == nil && e2 == nil && !left.IsZero() && !right.IsZero() && left.Equal(right)
}

func normalCephFSRemovalProcess(s *container.State) bool {
	return s != nil && s.Running && !s.Paused && !s.Restarting && !s.Dead
}

func equalCephFSRemovalWatchers(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
