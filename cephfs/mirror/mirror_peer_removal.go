package mirror

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/google/uuid"
)

// PeerRemoval retains one explicit removal, its original scope and its
// original live daemon cohort. It owns no clusters, containers, policy, data or cleanup hooks.
// A non-nil handle returned with an error retains an uncertain native request;
// retry BeginPeerRemoval with the same UUID and a fresh context to reconcile it.
type PeerRemoval struct {
	scope                                                         *cephFSOriginalScope
	cohort                                                        cephFSRemovalCohort
	completed, requestAcknowledged, processQuiescenceAcknowledged bool
}

// PeerRemovalStatus describes one observation of the original UUID.
// Drained means every witnessed original live replayer finished native teardown
// in the same process/filesystem session. It does not establish snapshot
// completion, remote lock release, or unchanged destination metadata.
type PeerRemovalStatus struct {
	PeerID                                      string
	SourceFilesystem, DestinationFilesystem     string
	SourceFilesystemID, DestinationFilesystemID int
	PolicyRemoved, Drained                      bool
	Daemons                                     map[string]PeerRemovalDaemonStatus
}

// PeerRemovalDaemonStatus is one original daemon's view of a peer removal.
// State is unobserved, pending, awaiting-peer-removal,
// awaiting-command-unregister, drained, stopped, unavailable, retired or
// session-changed; drained means the daemon, in its original process and
// session, unregistered the peer's admin command. Problem explains a state
// other than drained.
type PeerRemovalDaemonStatus struct {
	ContainerID, InstanceID string
	State, Problem          string
}

// BeginPeerRemoval validates every current owned daemon's original running
// process, filesystem watcher and exact peer command before issuing removal.
// Stopped/zero-daemon and partial cohorts are refused before mutation; RemovePeer
// retains its existing policy-request contract for those topologies.
// The same UUID can be retried after an uncertain response. Rebootstrap and
// expansion are refused while this explicit receipt lacks a drain proof.
// External native policy and direct container lifecycle writers must not race
// this operation or its observers. The first version requires the same session;
// stopping/restarting/removing its cohort later can prevent a drain observation.
//
// Drained means every original daemon, in its original process and session,
// unregistered the peer's admin command, which Ceph does only after joining the
// replayer's workers and unmounting the remote filesystem; it does not prove a
// completed snapshot, a released remote lock or unchanged destination metadata.
// After Drained, call RebootstrapPeer; the new peer supersedes the receipt,
// whose Status then returns a guard error. See the package documentation for an
// interrupted removal.
//
//purpose:condition,lifetime
func (mirror *Mirror) BeginPeerRemoval(ctx context.Context, id string) (*PeerRemoval, error) {
	return mirror.beginPeerRemoval(ctx, id, mirror.peerRemovalClusterIdentities)
}

func (mirror *Mirror) beginPeerRemoval(ctx context.Context, id string, readClusters func(context.Context) ([2]string, error)) (*PeerRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &mirror.mu); err != nil {
		return nil, err
	}
	defer mirror.mu.Unlock()
	if err := mirror.ops.guardDirectoryAdditionOverlap(); err != nil {
		return nil, err
	}
	if r := mirror.ops.peerRemoval; r != nil && r.scope.peerID == id {
		return r, r.resumeRetained(ctx)
	}
	if err := mirror.ops.guardPeerRemovalOverlap(); err != nil {
		return nil, err
	}
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || parsed == uuid.Nil || id != mirror.peerID {
		return nil, cephFSObserveGuard("CephFS mirror does not own this original peer")
	}
	if len(mirror.daemons) == 0 {
		return nil, cephFSObserveGuard("strict CephFS peer removal requires a live owned daemon cohort")
	}
	scope, err := mirror.captureOriginalScope(ctx, "CephFS peer removal", id, readClusters)
	if err != nil {
		return nil, err
	}
	r := &PeerRemoval{scope: scope, cohort: cephFSRemovalCohort{scope: scope}}
	if err := r.preflight(ctx); err != nil {
		return nil, err
	}
	// Register BEFORE mutation; a lost CLI response cannot erase the intent.
	mirror.ops.supersedePeerDirectoryAddition(id)
	mirror.ops.peerRemoval = r
	return r, r.resume(ctx)
}

// resumeRetained continues the peer removal already registered for this peer:
// an acknowledged process policy is only confirmed, otherwise the handle is
// checked and the removal resumed. Mirror.mu is held.
func (r *PeerRemoval) resumeRetained(ctx context.Context) error {
	if r.processQuiescenceAcknowledged {
		return r.confirmAcknowledgedProcessPolicy(ctx)
	}
	if err := r.checkHandle(); err != nil {
		return err
	}
	return r.resume(ctx)
}

// preflight is the read-only admission of a new peer removal: the peer and
// filesystem policies are present, the daemon cohort is captured and a second
// read finds both policies unchanged. Mirror.mu is held.
func (r *PeerRemoval) preflight(ctx context.Context) error {
	present, fsPresent, err := r.scope.policy(ctx)
	if err != nil {
		return err
	}
	if !present || !fsPresent {
		return cephFSObserveGuard("original CephFS peer policy is not confirmed before removal")
	}
	if err := r.cohort.capture(ctx); err != nil {
		return err
	}
	present, fsPresent, err = r.scope.policy(ctx)
	if err != nil {
		return err
	}
	if !present || !fsPresent {
		return cephFSObserveGuard("original CephFS peer changed during removal preflight")
	}
	return ctx.Err()
}

// Called under mirror.mu. Legacy policy-only RemovePeer has no explicit receipt.
func (ops *mirrorOperations) guardPeerRemovalOverlap() error {
	if err := ops.guardDirectoryAdditionOverlap(); err != nil {
		return err
	}
	return ops.guardCephFSRemovalOverlap()
}

// Addition resumes its own pending intent using only the removal admission gate.
func (ops *mirrorOperations) guardCephFSRemovalOverlap() error {
	if ops.peerRemoval != nil && !ops.peerRemoval.terminal() {
		return cephFSObserveGuard("explicit CephFS peer drain is incomplete")
	}
	return ops.guardDirectoryRemovalOverlap()
}

func (r *PeerRemoval) checkHandle() error {
	if r == nil || !r.scope.usable() || len(r.cohort.witnesses) == 0 {
		return cephFSObserveGuard("CephFS peer removal original identity is unavailable")
	}
	m := r.scope.mirror
	if m.closed || m.ops.peerRemoval != r || m.ops.peerGeneration != r.scope.generation {
		return cephFSObserveGuard("CephFS peer removal owner is closed or superseded")
	}
	// The removal's own request clears the fixture's peer.
	if !r.scope.sameFixture(true) {
		return cephFSObserveGuard("CephFS peer removal original fixture identity changed")
	}
	return nil
}

// Called under mirror.mu; may mutate only the original owned UUID, never a
// substituted native peer. A retry observes authoritative MON absence first.
func (r *PeerRemoval) resume(ctx context.Context) error {
	present, fsPresent, err := r.scope.policy(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !fsPresent {
		r.scope.mirror.peerID = ""
		if present {
			return cephFSObserveQuery("wait for CephFS manager peer policy convergence", nil)
		}
		return nil
	}
	if !present {
		return cephFSObserveQuery("wait for original CephFS manager peer policy", nil)
	}
	if r.requestAcknowledged {
		return cephFSObserveQuery("wait for acknowledged CephFS peer removal policy convergence", nil)
	}
	if _, err := r.scope.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_remove", r.scope.sourceFilesystem, r.scope.peerID); err != nil {
		return cephFSObserveQuery("remove original CephFS mirror peer", err)
	}
	r.requestAcknowledged = true
	r.scope.mirror.peerID = ""
	return ctx.Err()
}

func (r *PeerRemoval) initialStatus() PeerRemovalStatus {
	result := PeerRemovalStatus{}
	if r == nil || r.scope == nil {
		return result
	}
	s := r.scope
	result = PeerRemovalStatus{PeerID: s.peerID, SourceFilesystem: s.sourceFilesystem, DestinationFilesystem: s.destinationFilesystem, SourceFilesystemID: s.sourceID, DestinationFilesystemID: s.destinationID, Daemons: make(map[string]PeerRemovalDaemonStatus)}
	for _, w := range r.cohort.witnesses {
		result.Daemons[w.name] = PeerRemovalDaemonStatus{ContainerID: w.containerID, InstanceID: w.instanceID, State: "unobserved"}
	}
	return result
}

// Status observes policy absence and exact command unregister on the original
// live cohort. It never removes policy, changes a daemon's lifetime, or borrows
// a new session as evidence for the original one. Native query errors remain
// retryable; schema/identity/generation changes are permanent guard failures.
// Common MON attestation has no typed error categories: its non-context errors
// conservatively guard this observation, retaining the intent for a fresh retry.
//
//purpose:observation
func (r *PeerRemoval) Status(ctx context.Context) (PeerRemovalStatus, error) {
	return r.statusWithAdmission(ctx, nil)
}

// Private wait metadata distinguishes a queued attempt from a newer native
// observation while leaving public Status values and error contracts unchanged.
func (r *PeerRemoval) statusWithAdmission(ctx context.Context, admitted *bool) (PeerRemovalStatus, error) {
	result := r.initialStatus()
	if r == nil || r.scope == nil || r.scope.mirror == nil {
		return result, cephFSObserveGuard("CephFS peer removal handle is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &r.scope.mirror.mu); err != nil {
		return result, err
	}
	defer r.scope.mirror.mu.Unlock()
	if admitted != nil {
		*admitted = true
	}
	if err := r.checkHandle(); err != nil {
		return result, err
	}
	present, fsPresent, err := r.scope.policy(ctx)
	if err != nil {
		return result, err
	}
	result.PolicyRemoved = !present && !fsPresent
	if !result.PolicyRemoved {
		return result, ctx.Err()
	}
	watchers, err := r.scope.watchers(ctx)
	if err != nil {
		return result, err
	}
	all, errs, guarded := r.observeCohort(ctx, watchers, result.Daemons)
	if guarded {
		return result, errors.Join(errs...)
	}
	after, err := r.scope.watchers(ctx)
	if err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if !equalCephFSRemovalWatchers(watchers, after) {
		all = false
		errs = append(errs, cephFSObserveGuard("original CephFS filesystem watcher set changed during drain observation"))
	}
	if err := r.cohort.checkWatchers(after); err != nil {
		all = false
		errs = append(errs, err)
	}
	present, fsPresent, err = r.scope.policy(ctx)
	if err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if present || fsPresent {
		all = false
		result.PolicyRemoved = false
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	result.Drained = all && len(errs) == 0
	if result.Drained {
		r.completed = true
		r.scope.mirror.peerID = ""
	}
	return result, errors.Join(errs...)
}

// observeCohort observes each original daemon under its own lock and records
// its state and problem in daemons. It reports whether every daemon has
// drained the peer, the observation errors, and whether a guard refusal ended
// the observation early. Mirror.mu is held.
func (r *PeerRemoval) observeCohort(ctx context.Context, watchers map[string]string, daemons map[string]PeerRemovalDaemonStatus) (bool, []error, bool) {
	all := true
	var errs []error
	for _, w := range r.cohort.witnesses {
		entry := daemons[w.name]
		if err := multicluster.LockObservation(ctx, &w.daemon.mu); err != nil {
			all = false
			errs = append(errs, err)
			break
		}
		state, entryError := r.observeDaemon(ctx, w, watchers)
		w.daemon.mu.Unlock()
		entry.State = state
		if entryError != nil {
			all = false
			entry.Problem = entryError.Error()
			errs = append(errs, entryError)
		}
		if state != "drained" {
			all = false
		}
		daemons[w.name] = entry
		if errors.Is(entryError, errCephFSObservationGuard) {
			return all, errs, true
		}
	}
	return all, errs, false
}

// WaitDrained releases the fixture lock between observations. Deadline/cancel
// returns the last report with Drained=false and preserves the last query cause.
//
//purpose:observation
func (r *PeerRemoval) WaitDrained(ctx context.Context) (PeerRemovalStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return waitCephFSPeerDrainWithAdmission(ctx, 500*time.Millisecond, func(attempt context.Context) (PeerRemovalStatus, bool, error) {
		admitted := false
		status, err := r.statusWithAdmission(attempt, &admitted)
		return status, admitted, err
	})
}

func waitCephFSPeerDrainWithAdmission(ctx context.Context, interval time.Duration, observe func(context.Context) (PeerRemovalStatus, bool, error)) (PeerRemovalStatus, error) {
	return waitCephFSRemovalCompletion(ctx, interval, observe,
		func(status PeerRemovalStatus) bool { return status.Drained },
		func(status *PeerRemovalStatus) { status.Drained = false })
}

func (r *PeerRemoval) observeDaemon(ctx context.Context, w cephFSRemovalWitness, watchers map[string]string) (string, error) {
	if w.daemon == nil || w.daemon.Container == nil || w.daemon.removed {
		return "retired", cephFSObserveQuery("original CephFS daemon no longer has a live drain witness", nil)
	}
	if w.daemon.DaemonName != w.name {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain daemon name changed")
	}
	if w.daemon.GetContainerID() != w.containerID {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain container was replaced")
	}
	state, err := w.daemon.State(ctx)
	if err != nil {
		return "unavailable", cephFSObserveQuery("inspect original CephFS drain process", err)
	}
	if !normalCephFSRemovalProcess(state) {
		return "stopped", cephFSObserveQuery("original CephFS peer drain process is not running normally", nil)
	}
	if !sameCephFSRemovalStartedAt(state.StartedAt, w.startedAt) {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain process generation changed")
	}
	before, err := r.scope.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if before.address != w.address || watchers[w.address] != w.instanceID {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain filesystem session changed")
	}
	registered, err := r.scope.catalog(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	after, err := r.scope.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if before.address != after.address {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain filesystem session changed during observation")
	}
	if before.peerPresent != after.peerPresent {
		return "pending", cephFSObserveQuery("CephFS peer drain peer changed during observation", nil)
	}
	state, err = w.daemon.State(ctx)
	if err != nil {
		return "unavailable", cephFSObserveQuery("confirm original CephFS drain process", err)
	}
	if !normalCephFSRemovalProcess(state) {
		return "stopped", cephFSObserveQuery("original CephFS peer drain process stopped during observation", nil)
	}
	if !sameCephFSRemovalStartedAt(state.StartedAt, w.startedAt) || w.daemon.GetContainerID() != w.containerID {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain process changed during observation")
	}
	if before.peerPresent {
		return "awaiting-peer-removal", nil
	}
	if registered {
		return "awaiting-command-unregister", nil
	}
	return "drained", ctx.Err()
}
