package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/moby/moby/api/types/container"
)

// CephFSMirrorPeerRemoval retains one explicit removal and its original live
// daemon cohort. It owns no clusters, containers, policy, data or cleanup hooks.
// A non-nil handle returned with an error retains an uncertain native request;
// retry BeginPeerRemoval with the same UUID and a fresh context to reconcile it.
type CephFSMirrorPeerRemoval struct {
	mirror                                                              *CephFSMirror
	source, destination                                                 *ceph.Container
	sourceFilesystem, destinationFilesystem, sourceClient, metadataPool string
	sourceID, destinationID                                             int
	sourceMetadata, destinationMetadata                                 int64
	peerID                                                              string
	peer                                                                cephFSPeerIdentity
	generation                                                          uint64
	clusters                                                            [2]string
	readClusters                                                        func(context.Context) ([2]string, error)
	cohort                                                              []cephFSPeerRemovalWitness
	completed                                                           bool
	requestAcknowledged                                                 bool
	processQuiescenceAcknowledged                                       bool
}

// CephFSMirrorPeerRemovalStatus describes one observation of the original UUID.
// Drained means every witnessed original live replayer finished native teardown
// in the same process/filesystem session. It does not establish snapshot
// completion, remote lock release, or unchanged destination metadata.
type CephFSMirrorPeerRemovalStatus struct {
	PeerID                                      string
	SourceFilesystem, DestinationFilesystem     string
	SourceFilesystemID, DestinationFilesystemID int
	PolicyRemoved, Drained                      bool
	Daemons                                     map[string]CephFSMirrorPeerRemovalDaemonStatus
}

type CephFSMirrorPeerRemovalDaemonStatus struct {
	ContainerID, InstanceID string
	State, Problem          string
}

type cephFSPeerRemovalWitness struct {
	daemon                                            *CephFSMirrorDaemon
	name, containerID, startedAt, address, instanceID string
	originalProcess                                   *cephFSBoundOriginalProcess
}

type cephFSPeerRemovalSession struct {
	address     string
	peerPresent bool
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
func (mirror *CephFSMirror) BeginPeerRemoval(ctx context.Context, id string) (*CephFSMirrorPeerRemoval, error) {
	return mirror.beginPeerRemoval(ctx, id, mirror.peerRemovalClusterIdentities)
}

func (mirror *CephFSMirror) beginPeerRemoval(ctx context.Context, id string, readClusters func(context.Context) ([2]string, error)) (*CephFSMirrorPeerRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return nil, err
	}
	defer mirror.mu.Unlock()
	if err := mirror.guardDirectoryAdditionOverlap(); err != nil {
		return nil, err
	}
	if mirror.peerRemoval != nil && mirror.peerRemoval.peerID == id {
		r := mirror.peerRemoval
		if r.processQuiescenceAcknowledged {
			return r, r.confirmAcknowledgedProcessPolicy(ctx)
		}
		if err := r.checkHandle(); err != nil {
			return r, err
		}
		return r, r.resume(ctx)
	}
	if err := mirror.guardPeerRemovalOverlap(); err != nil {
		return nil, err
	}
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || parsed == uuid.Nil || id != mirror.peerID {
		return nil, cephFSObserveGuard("CephFS mirror does not own this original peer")
	}
	if mirror.metadataPoolID <= 0 || mirror.destinationMetadataPoolID <= 0 || mirror.SourceClientEntity == "" || mirror.metadataPool == "" {
		return nil, cephFSObserveGuard("CephFS original filesystem metadata identity is unavailable")
	}
	if len(mirror.daemons) == 0 {
		return nil, cephFSObserveGuard("strict CephFS peer removal requires a live owned daemon cohort")
	}
	if readClusters == nil {
		return nil, cephFSObserveGuard("CephFS original cluster identity reader is unavailable")
	}
	clusters, err := readClusters(ctx)
	if err != nil {
		return nil, err
	}
	if !validCephFSRemovalClusters(clusters) {
		return nil, cephFSObserveGuard("CephFS original cluster identities are unavailable or equal")
	}
	r := &CephFSMirrorPeerRemoval{mirror: mirror, source: mirror.source, destination: mirror.destination,
		sourceFilesystem: mirror.SourceFilesystem, destinationFilesystem: mirror.DestinationFilesystem, sourceClient: mirror.SourceClientEntity, metadataPool: mirror.metadataPool,
		sourceID: mirror.filesystemID, destinationID: mirror.destinationFilesystemID, sourceMetadata: mirror.metadataPoolID, destinationMetadata: mirror.destinationMetadataPoolID,
		peerID: id, peer: cephFSPeerIdentity{mirror.DestinationClientEntity, mirror.destinationSite, mirror.DestinationFilesystem}, generation: mirror.peerGeneration, clusters: clusters, readClusters: readClusters}
	present, fsPresent, err := r.policy(ctx)
	if err != nil {
		return nil, err
	}
	if !present || !fsPresent {
		return nil, cephFSObserveGuard("original CephFS peer policy is not confirmed before removal")
	}
	if err := r.captureCohort(ctx); err != nil {
		return nil, err
	}
	present, fsPresent, err = r.policy(ctx)
	if err != nil {
		return nil, err
	}
	if !present || !fsPresent {
		return nil, cephFSObserveGuard("original CephFS peer changed during removal preflight")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Register BEFORE mutation; a lost CLI response cannot erase the intent.
	mirror.supersedePeerDirectoryAddition(id)
	mirror.peerRemoval = r
	return r, r.resume(ctx)
}

// Called under mirror.mu. Legacy policy-only RemovePeer has no explicit receipt.
func (mirror *CephFSMirror) guardPeerRemovalOverlap() error {
	if err := mirror.guardDirectoryAdditionOverlap(); err != nil {
		return err
	}
	return mirror.guardCephFSRemovalOverlap()
}

// Addition resumes its own pending intent using only the removal admission gate.
func (mirror *CephFSMirror) guardCephFSRemovalOverlap() error {
	if mirror.peerRemoval != nil && !mirror.peerRemoval.terminal() {
		return cephFSObserveGuard("explicit CephFS peer drain is incomplete")
	}
	return mirror.guardDirectoryRemovalOverlap()
}

func (r *CephFSMirrorPeerRemoval) checkHandle() error {
	if r == nil || r.mirror == nil || r.source == nil || r.destination == nil || r.readClusters == nil || len(r.cohort) == 0 {
		return cephFSObserveGuard("CephFS peer removal original identity is unavailable")
	}
	m := r.mirror
	if m.closed || m.peerRemoval != r || m.peerGeneration != r.generation {
		return cephFSObserveGuard("CephFS peer removal owner is closed or superseded")
	}
	if m.source != r.source || m.destination != r.destination || m.SourceFilesystem != r.sourceFilesystem || m.DestinationFilesystem != r.destinationFilesystem || m.SourceClientEntity != r.sourceClient || m.filesystemID != r.sourceID || m.destinationFilesystemID != r.destinationID || m.metadataPoolID != r.sourceMetadata || m.destinationMetadataPoolID != r.destinationMetadata || m.metadataPool != r.metadataPool || m.DestinationClientEntity != r.peer.ClientName || m.destinationSite != r.peer.SiteName || (m.peerID != "" && m.peerID != r.peerID) {
		return cephFSObserveGuard("CephFS peer removal original fixture identity changed")
	}
	return nil
}

// Called under mirror.mu; may mutate only the original owned UUID, never a
// substituted native peer. A retry observes authoritative MON absence first.
func (r *CephFSMirrorPeerRemoval) resume(ctx context.Context) error {
	present, fsPresent, err := r.policy(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !fsPresent {
		r.mirror.peerID = ""
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
	if _, err := r.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_remove", r.sourceFilesystem, r.peerID); err != nil {
		return cephFSObserveQuery("remove original CephFS mirror peer", err)
	}
	r.requestAcknowledged = true
	r.mirror.peerID = ""
	return ctx.Err()
}

func (r *CephFSMirrorPeerRemoval) initialStatus() CephFSMirrorPeerRemovalStatus {
	result := CephFSMirrorPeerRemovalStatus{}
	if r == nil {
		return result
	}
	result = CephFSMirrorPeerRemovalStatus{PeerID: r.peerID, SourceFilesystem: r.sourceFilesystem, DestinationFilesystem: r.destinationFilesystem, SourceFilesystemID: r.sourceID, DestinationFilesystemID: r.destinationID, Daemons: make(map[string]CephFSMirrorPeerRemovalDaemonStatus)}
	for _, w := range r.cohort {
		result.Daemons[w.name] = CephFSMirrorPeerRemovalDaemonStatus{ContainerID: w.containerID, InstanceID: w.instanceID, State: "unobserved"}
	}
	return result
}

// Status observes policy absence and exact command unregister on the original
// live cohort. It never removes policy, changes a daemon's lifetime, or borrows
// a new session as evidence for the original one. Native query errors remain
// retryable; schema/identity/generation changes are permanent guard failures.
// Common MON attestation has no typed error categories: its non-context errors
// conservatively guard this observation, retaining the intent for a fresh retry.
func (r *CephFSMirrorPeerRemoval) Status(ctx context.Context) (CephFSMirrorPeerRemovalStatus, error) {
	return r.statusWithAdmission(ctx, nil)
}

// Private wait metadata distinguishes a queued attempt from a newer native
// observation while leaving public Status values and error contracts unchanged.
func (r *CephFSMirrorPeerRemoval) statusWithAdmission(ctx context.Context, admitted *bool) (CephFSMirrorPeerRemovalStatus, error) {
	result := r.initialStatus()
	if r == nil || r.mirror == nil {
		return result, cephFSObserveGuard("CephFS peer removal handle is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &r.mirror.mu); err != nil {
		return result, err
	}
	defer r.mirror.mu.Unlock()
	if admitted != nil {
		*admitted = true
	}
	if err := r.checkHandle(); err != nil {
		return result, err
	}
	present, fsPresent, err := r.policy(ctx)
	if err != nil {
		return result, err
	}
	result.PolicyRemoved = !present && !fsPresent
	if !result.PolicyRemoved {
		return result, ctx.Err()
	}
	watchers, err := r.watchers(ctx)
	if err != nil {
		return result, err
	}
	all := true
	var errs []error
	for _, w := range r.cohort {
		entry := result.Daemons[w.name]
		if err := lockRGWSyncObservation(ctx, &w.daemon.mu); err != nil {
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
		result.Daemons[w.name] = entry
		if errors.Is(entryError, errCephFSObservationGuard) {
			return result, errors.Join(errs...)
		}
	}
	after, err := r.watchers(ctx)
	if err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if !equalCephFSRemovalWatchers(watchers, after) {
		all = false
		errs = append(errs, cephFSObserveGuard("original CephFS filesystem watcher set changed during drain observation"))
	}
	if err := r.checkWatcherCohort(after); err != nil {
		all = false
		errs = append(errs, err)
	}
	present, fsPresent, err = r.policy(ctx)
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
		r.mirror.peerID = ""
	}
	return result, errors.Join(errs...)
}

// WaitDrained releases the fixture lock between observations. Deadline/cancel
// returns the last report with Drained=false and preserves the last query cause.
func (r *CephFSMirrorPeerRemoval) WaitDrained(ctx context.Context) (CephFSMirrorPeerRemovalStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return waitCephFSPeerDrainWithAdmission(ctx, 500*time.Millisecond, func(attempt context.Context) (CephFSMirrorPeerRemovalStatus, bool, error) {
		admitted := false
		status, err := r.statusWithAdmission(attempt, &admitted)
		return status, admitted, err
	})
}

func waitCephFSPeerDrain(ctx context.Context, interval time.Duration, observe func(context.Context) (CephFSMirrorPeerRemovalStatus, error)) (CephFSMirrorPeerRemovalStatus, error) {
	return waitCephFSPeerDrainWithAdmission(ctx, interval, func(attempt context.Context) (CephFSMirrorPeerRemovalStatus, bool, error) {
		status, err := observe(attempt)
		return status, true, err
	})
}

func waitCephFSPeerDrainWithAdmission(ctx context.Context, interval time.Duration, observe func(context.Context) (CephFSMirrorPeerRemovalStatus, bool, error)) (CephFSMirrorPeerRemovalStatus, error) {
	var last CephFSMirrorPeerRemovalStatus
	var lastError error
	var haveObservation bool
	for {
		if err := ctx.Err(); err != nil {
			last.Drained = false
			return last, errors.Join(lastError, err)
		}
		current, admitted, err := observe(ctx)
		if admitted || !haveObservation {
			last = current
		}
		haveObservation = haveObservation || admitted
		if err != nil {
			lastError = retainWaitQueryCause(ctx, lastError, err)
		}
		if canceled := ctx.Err(); canceled != nil {
			last.Drained = false
			return last, errors.Join(lastError, canceled)
		}
		if errors.Is(err, errCephFSObservationGuard) {
			last.Drained = false
			return last, err
		}
		if err == nil && last.Drained {
			return last, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			last.Drained = false
			return last, errors.Join(lastError, ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *CephFSMirrorPeerRemoval) captureCohort(ctx context.Context) error {
	watchers, err := r.watchers(ctx)
	if err != nil {
		return err
	}
	ids, names := make(map[string]bool), make(map[string]bool)
	for _, daemon := range r.mirror.daemons {
		if daemon == nil {
			return cephFSObserveGuard("original CephFS peer cohort contains an unavailable daemon")
		}
		if err := lockRGWSyncObservation(ctx, &daemon.mu); err != nil {
			return err
		}
		w, err := r.captureDaemon(ctx, daemon, watchers)
		daemon.mu.Unlock()
		if err != nil {
			return err
		}
		if ids[w.containerID] || names[w.name] {
			return cephFSObserveGuard("original CephFS peer cohort identity is ambiguous")
		}
		ids[w.containerID], names[w.name] = true, true
		r.cohort = append(r.cohort, w)
	}
	if r.mirror.originalProcessClientFactory != nil {
		if err := validateCephFSProcessCohort(ctx, r); err != nil {
			return err
		}
	}
	if err := r.checkWatcherCohort(watchers); err != nil {
		return err
	}
	after, err := r.watchers(ctx)
	if err != nil {
		return err
	}
	if !equalCephFSRemovalWatchers(watchers, after) {
		return cephFSObserveGuard("CephFS original filesystem watchers changed during removal preflight")
	}
	return r.checkWatcherCohort(after)
}

func (r *CephFSMirrorPeerRemoval) captureDaemon(ctx context.Context, daemon *CephFSMirrorDaemon, watchers map[string]string) (cephFSPeerRemovalWitness, error) {
	w := cephFSPeerRemovalWitness{daemon: daemon, name: daemon.DaemonName}
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
	session, err := r.session(ctx, daemon)
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
	registered, err := r.catalog(ctx, daemon)
	if err != nil {
		return w, err
	}
	if !registered {
		return w, cephFSObserveGuard("original exact CephFS peer command was not witnessed before removal")
	}
	after, err := r.session(ctx, daemon)
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
	if r.mirror.originalProcessClientFactory != nil {
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

func (r *CephFSMirrorPeerRemoval) observeDaemon(ctx context.Context, w cephFSPeerRemovalWitness, watchers map[string]string) (string, error) {
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
	before, err := r.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if before.address != w.address || watchers[w.address] != w.instanceID {
		return "session-changed", cephFSObserveGuard("original CephFS peer drain filesystem session changed")
	}
	registered, err := r.catalog(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	after, err := r.session(ctx, w.daemon)
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

func (r *CephFSMirrorPeerRemoval) watchers(ctx context.Context) (map[string]string, error) {
	control, err := r.source.ControlContainerContext(ctx)
	if err != nil {
		return nil, cephFSObserveQuery("select CephFS peer drain control container", err)
	}
	if control == nil {
		return nil, cephFSObserveGuard("CephFS peer drain control container is unavailable")
	}
	data, err := exec(ctx, control, "rados", "--pool", r.metadataPool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, cephFSObserveQuery("read original CephFS peer drain filesystem watchers", err)
	}
	value, err := parseCephFSMirrorWatchers(data)
	if err != nil {
		return nil, cephFSObserveGuard("decode original CephFS peer drain filesystem watchers")
	}
	return value, ctx.Err()
}

func (r *CephFSMirrorPeerRemoval) checkWatcherCohort(watchers map[string]string) error {
	if len(watchers) != len(r.cohort) {
		return cephFSObserveGuard("CephFS peer drain has foreign or retired filesystem watchers")
	}
	seen := make(map[string]bool)
	for _, w := range r.cohort {
		if w.instanceID == "" || watchers[w.address] != w.instanceID || seen[w.instanceID] {
			return cephFSObserveGuard("CephFS peer drain original watcher cohort differs")
		}
		seen[w.instanceID] = true
	}
	return nil
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

func (r *CephFSMirrorPeerRemoval) session(ctx context.Context, daemon *CephFSMirrorDaemon) (cephFSPeerRemovalSession, error) {
	data, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", r.sourceFilesystem, r.sourceID))
	if err != nil {
		return cephFSPeerRemovalSession{}, cephFSObserveQuery("read original CephFS peer drain filesystem session", err)
	}
	return decodeCephFSRemovalSession(data, r.peerID, r.peer)
}

func (r *CephFSMirrorPeerRemoval) catalog(ctx context.Context, daemon *CephFSMirrorDaemon) (bool, error) {
	data, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help")
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS peer drain command catalog", err)
	}
	return decodeCephFSRemovalCatalog(data, fmt.Sprintf("fs mirror status %s@%d", r.sourceFilesystem, r.sourceID), fmt.Sprintf("fs mirror peer status %s@%d %s", r.sourceFilesystem, r.sourceID, r.peerID))
}

func decodeCephFSRemovalCatalog(data []byte, fsCommand, peerCommand string) (bool, error) {
	var raw map[string]json.RawMessage
	if json.Unmarshal(data, &raw) != nil || raw == nil {
		return false, cephFSObserveGuard("decode CephFS peer drain command catalog object")
	}
	commands := make(map[string]string)
	for command, value := range raw {
		var text *string
		if json.Unmarshal(value, &text) != nil || text == nil || *text == "" {
			return false, cephFSObserveGuard("decode CephFS peer drain command catalog help text")
		}
		commands[command] = *text
	}
	if commands[fsCommand] == "" || commands["help"] == "" || commands["get_command_descriptions"] == "" {
		return false, cephFSObserveGuard("original CephFS filesystem command catalog is incomplete")
	}
	_, present := commands[peerCommand]
	return present, nil
}

func decodeCephFSRemovalSession(data []byte, id string, expected cephFSPeerIdentity) (cephFSPeerRemovalSession, error) {
	var result cephFSPeerRemovalSession
	var value struct {
		State    json.RawMessage            `json:"state"`
		Address  *string                    `json:"rados_inst"`
		Peers    map[string]json.RawMessage `json:"peers"`
		SnapDirs *struct {
			Count *int `json:"dir_count"`
		} `json:"snap_dirs"`
	}
	if json.Unmarshal(data, &value) != nil {
		return result, cephFSObserveGuard("decode CephFS peer drain filesystem session")
	}
	if len(value.State) != 0 {
		var state string
		if json.Unmarshal(value.State, &state) != nil || !slices.Contains([]string{"failed", "blocklisted"}, state) {
			return result, cephFSObserveGuard("decode CephFS peer drain filesystem failure state")
		}
		return result, cephFSObserveQuery("original CephFS peer drain filesystem session is unavailable", nil)
	}
	if value.Address == nil || *value.Address == "" || value.Peers == nil || value.SnapDirs == nil || value.SnapDirs.Count == nil || *value.SnapDirs.Count < 0 {
		return result, cephFSObserveGuard("decode normal CephFS peer drain filesystem session")
	}
	result.address = *value.Address
	present, err := decodeCephFSRemovalPeers(value.Peers, id, expected, true)
	result.peerPresent = present
	return result, err
}

// peer_list uses site_name, while FSMap/admin session use remote.cluster_name.
func decodeCephFSRemovalPeers(peers map[string]json.RawMessage, id string, expected cephFSPeerIdentity, remote bool) (bool, error) {
	if peers == nil {
		return false, cephFSObserveGuard("decode CephFS peer drain peer object")
	}
	for key := range peers {
		parsed, err := uuid.Parse(key)
		if err != nil || parsed.String() != key || parsed == uuid.Nil {
			return false, cephFSObserveGuard("decode CephFS peer drain peer UUID")
		}
	}
	if len(peers) == 0 {
		return false, nil
	}
	entry, exists := peers[id]
	if len(peers) != 1 || !exists {
		return false, cephFSObserveGuard("CephFS peer drain has a substituted peer UUID")
	}
	if remote {
		var wrapper *struct {
			Remote json.RawMessage `json:"remote"`
		}
		if json.Unmarshal(entry, &wrapper) != nil || wrapper == nil || len(wrapper.Remote) == 0 {
			return false, cephFSObserveGuard("decode CephFS peer drain remote identity")
		}
		entry = wrapper.Remote
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(entry, &fields) != nil || fields == nil {
		return false, cephFSObserveGuard("decode CephFS peer drain destination tuple")
	}
	keys := []string{"client_name", "site_name", "fs_name"}
	if remote {
		keys[1] = "cluster_name"
	}
	values := make([]string, 3)
	for i, key := range keys {
		var text *string
		if json.Unmarshal(fields[key], &text) != nil || text == nil || *text == "" {
			return false, cephFSObserveGuard("decode CephFS peer drain destination tuple field")
		}
		values[i] = *text
	}
	if (cephFSPeerIdentity{values[0], values[1], values[2]}) != expected {
		return false, cephFSObserveGuard("CephFS peer drain original destination tuple differs")
	}
	return true, nil
}

// Validates immutable names/FS/metadata/cluster identities before and after every
// observer operation. Source FSMap is a MON view; peer_list is MGR's cached view.
func (r *CephFSMirrorPeerRemoval) policy(ctx context.Context) (bool, bool, error) {
	clusters, err := r.readClusters(ctx)
	if err != nil {
		return false, false, err
	}
	if clusters != r.clusters {
		return false, false, cephFSObserveGuard("CephFS peer drain original cluster identity differs")
	}
	var fsPeers map[string]json.RawMessage
	for _, site := range []struct {
		cluster *ceph.Container
		name    string
		id      int
		pool    int64
		source  bool
	}{{r.source, r.sourceFilesystem, r.sourceID, r.sourceMetadata, true}, {r.destination, r.destinationFilesystem, r.destinationID, r.destinationMetadata, false}} {
		data, err := site.cluster.Ceph(ctx, "fs", "get", site.name, "--format", "json")
		if err != nil {
			return false, false, cephFSObserveQuery("read original CephFS peer drain filesystem identity", err)
		}
		var value struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &value) != nil || value.ID == nil || *value.ID != site.id || value.MDSMap == nil || value.MDSMap.Name == nil || *value.MDSMap.Name != site.name || value.MDSMap.Pool == nil || *value.MDSMap.Pool != site.pool {
			return false, false, cephFSObserveGuard("CephFS peer drain original filesystem or metadata pool differs")
		}
		if site.source {
			if value.MirrorInfo == nil || value.MirrorInfo.Peers == nil {
				return false, false, cephFSObserveGuard("CephFS peer drain source mirroring policy is unavailable")
			}
			fsPeers = value.MirrorInfo.Peers
		}
	}
	fsPresent, err := decodeCephFSRemovalPeers(fsPeers, r.peerID, r.peer, true)
	if err != nil {
		return false, false, err
	}
	data, err := r.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", r.sourceFilesystem)
	if err != nil {
		return false, false, cephFSObserveQuery("read original CephFS peer drain manager policy", err)
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil {
		return false, false, cephFSObserveGuard("decode original CephFS peer drain manager policy")
	}
	present, err := decodeCephFSRemovalPeers(peers, r.peerID, r.peer, false)
	return present, fsPresent, errors.Join(err, ctx.Err())
}

func validCephFSRemovalClusters(values [2]string) bool {
	if values[0] == values[1] {
		return false
	}
	for _, value := range values {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed.String() != value || parsed == uuid.Nil {
			return false
		}
	}
	return true
}

func (mirror *CephFSMirror) peerRemovalClusterIdentities(ctx context.Context) ([2]string, error) {
	var result [2]string
	for i, cluster := range []*ceph.Container{mirror.source, mirror.destination} {
		if cluster == nil {
			return result, cephFSObserveGuard("CephFS peer drain original cluster is unavailable")
		}
		original, err := readCephFSRemovalClusterIdentity(ctx, cluster.MonitorBootstrapAddresses,
			func(queryCtx context.Context) (string, error) {
				status, err := cluster.QuorumStatus(queryCtx)
				return status.MonMap.FSID, err
			},
			func(queryCtx context.Context) (string, error) {
				data, err := cluster.Ceph(queryCtx, "fsid")
				return strings.TrimSpace(string(data)), err
			})
		if err != nil {
			return result, err
		}
		result[i] = original
	}
	return result, ctx.Err()
}

// Common validation attests the fixture's private original template before and
// after the native identity observations. Native FSID alone is never adopted.
// The common API does not type query versus schema errors, so non-context
// failures conservatively guard this observation. The retained intent survives:
// retry Status/Wait with a fresh context after quorum availability is restored.
func readCephFSRemovalClusterIdentity(ctx context.Context, bootstrap, quorum, native func(context.Context) (string, error)) (string, error) {
	validate := func() error {
		addresses, err := bootstrap(ctx)
		if err != nil {
			cause := cephFSObserveQuery("validate original CephFS peer drain monitor bootstrap", err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return cause
			}
			return errors.Join(cephFSObserveGuard("original CephFS peer drain monitor identity is not confirmed"), cause)
		}
		if addresses == "" || strings.ContainsAny(addresses, "\x00\r\n") {
			return cephFSObserveGuard("original CephFS peer drain monitor bootstrap is unavailable")
		}
		return ctx.Err()
	}
	if err := validate(); err != nil {
		return "", err
	}
	original, err := quorum(ctx)
	if err != nil {
		cause := cephFSObserveQuery("read original CephFS peer drain monmap FSID", err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", cause
		}
		return "", errors.Join(cephFSObserveGuard("original CephFS peer drain monmap identity is not confirmed"), cause)
	}
	parsed, err := uuid.Parse(original)
	if err != nil || parsed.String() != original || parsed == uuid.Nil {
		return "", cephFSObserveGuard("decode original CephFS peer drain monmap FSID")
	}
	current, err := native(ctx)
	if err != nil {
		return "", cephFSObserveQuery("read original CephFS peer drain native FSID", err)
	}
	if current != original {
		return "", cephFSObserveGuard("CephFS peer drain native FSID differs from its confirmed monmap")
	}
	if err := validate(); err != nil {
		return "", err
	}
	return original, ctx.Err()
}
