package mirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// DirectoryRemoval retains an explicit path removal and its original
// live daemon/peer cohort. It owns no cluster, container, directory or cleanup.
// A non-nil receipt returned with an error retains an uncertain native request;
// retry BeginDirectoryRemoval for the same path with a fresh context.
type DirectoryRemoval struct {
	original                                   *PeerRemoval
	directory, ownerID                         string
	generation                                 uint64
	requestAcknowledged, completed, superseded bool
	processQuiescenceAcknowledged              bool
	// requestAttempted records that a native remove was sent, whatever its
	// reply; a remove without an acknowledgement is uncertain and never
	// sent again.
	requestAttempted bool
}

// errCephFSUncertainRemoval is the cause of a refusal to send a remove again
// after one whose reply was lost: a policy that still shows the path mapped
// does not prove that the MGR dropped the first, which can still wait behind
// the mirroring module's policy lock.
var errCephFSUncertainRemoval = errors.New("an uncertain native CephFS mirror directory remove is not sent again")

// uncertainRemoval reports the native mapping of a path whose earlier remove
// is uncertain, without sending anything. It is retryable: a later call that
// finds the path gone from the policy completes the request.
func uncertainRemoval(ctx context.Context, m *Mirror, directory string) error {
	state := "unread"
	if mapping, err := m.readCephFSObservedMapping(ctx, directory); err == nil {
		state = mapping.State
	}
	return &cephFSObservationError{message: fmt.Sprintf("%v: the native policy still tracks %s, %s", errCephFSUncertainRemoval, directory, state), cause: errCephFSUncertainRemoval}
}

// DirectoryRemovalStatus observes the original path registration.
// Released proves every original replayer has no registered sync cycle for the
// directory in its original normal session. It does not establish worker thread
// exit, successful remote unlock, checkpoint completion or unchanged metadata.
type DirectoryRemovalStatus struct {
	Directory, PeerID                           string
	SourceFilesystem, DestinationFilesystem     string
	SourceFilesystemID, DestinationFilesystemID int
	PolicyRemoved, Released                     bool
	Daemons                                     map[string]DirectoryRemovalDaemonStatus
}

// DirectoryRemovalDaemonStatus is one original daemon's view of a directory
// removal. State is unobserved (not observed yet in this call), tracked (the
// daemon still has a sync cycle registered for the path), released (no sync
// cycle for the path in its original session), stopped or unavailable (not
// running or not queryable; retryable, except a malformed native reply, which
// is a guard error), retired (removed from the fixture) or
// session-changed (the process, filesystem session or peer command is not
// the original one; a guard error). Problem explains a state other than
// released.
type DirectoryRemovalDaemonStatus struct {
	ContainerID, InstanceID string
	State, Problem          string
}

// BeginDirectoryRemoval requires a privately owned path, stable mapped owner,
// positive path witness and every owned daemon's original normal live process,
// filesystem watcher and exact peer command before removing native policy.
// RemoveDirectory keeps its policy-request-only behavior for other topologies.
// This first version requires the same process/session throughout observation;
// stopping, restarting or removing the cohort can prevent a release proof.
// External policy, filesystem and direct container writers must not race it.
//
// A remove that was sent and whose reply was lost is never sent again: a retry
// that finds the policy gone continues to the release proof, and one that
// still finds the path tracked returns a retryable error naming its native
// state, even when that state is mapped, because the first remove can still
// wait in the MGR. The caller decides whether to keep retrying. Before the
// first remove the original assigned daemon and cohort are checked again,
// waiting while the assignment moves and refusing if another daemon took
// over. Like RemoveDirectory, the
// native remove waits until every directory that the native policy tracks is
// mapped or stalled. While the removal is not
// Released, AddDirectory, AddDaemon, RebalanceDirectories, peer removal,
// RebootstrapPeer and removal of other paths are refused. Released means the
// policy is gone and every original daemon reports released in its original
// process and session; it does not prove that worker threads exited, the remote
// lock was released, a checkpoint completed or destination metadata is
// unchanged. See the package documentation for an interrupted removal.
//
//purpose:condition,lifetime
func (mirror *Mirror) BeginDirectoryRemoval(ctx context.Context, directory string) (*DirectoryRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory removal fixture is unavailable")
	}
	return mirror.beginDirectoryRemoval(ctx, directory, mirror.peerRemovalClusterIdentities)
}

func (mirror *Mirror) beginDirectoryRemoval(ctx context.Context, directory string, readClusters func(context.Context) ([2]string, error)) (*DirectoryRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory removal fixture is unavailable")
	}
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return nil, err
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
	if r := mirror.ops.directoryRemoval; r != nil && r.directory == directory && r.generation == mirror.ops.directoryGenerations[directory] && !r.superseded {
		return r, r.resumeRetained(ctx)
	}
	if err := mirror.ops.guardPeerRemovalOverlap(); err != nil {
		return nil, err
	}
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return nil, err
	}
	if !mirror.ownedDirectories[directory] || mirror.ops.directoryGenerations[directory] == 0 {
		return nil, cephFSObserveGuard("CephFS fixture does not own this original directory registration")
	}
	if len(mirror.daemons) == 0 || mirror.metadataPoolID <= 0 || mirror.destinationMetadataPoolID <= 0 || mirror.SourceClientEntity == "" || mirror.metadataPool == "" || readClusters == nil {
		return nil, cephFSObserveGuard("strict CephFS directory removal original live identity is unavailable")
	}
	clusters, err := readClusters(ctx)
	if err != nil {
		return nil, err
	}
	if !validCephFSRemovalClusters(clusters) {
		return nil, cephFSObserveGuard("CephFS directory removal original clusters are unavailable or equal")
	}
	original := mirror.originalPeerRemoval(mirror.peerID, clusters, readClusters)
	r := &DirectoryRemoval{original: original, directory: directory, generation: mirror.ops.directoryGenerations[directory]}
	if err := r.preflight(ctx); err != nil {
		return nil, err
	}
	// Store the immutable identity and live witnesses BEFORE native mutation.
	mirror.ops.supersedeDirectoryAddition(directory)
	mirror.ops.directoryRemoval = r
	return r, r.resume(ctx)
}

// resumeRetained continues the directory removal already registered for this
// directory and generation: an acknowledged process policy is only confirmed,
// otherwise the handle is checked and the removal resumed. Mirror.mu is held.
func (r *DirectoryRemoval) resumeRetained(ctx context.Context) error {
	if r.processQuiescenceAcknowledged {
		return r.confirmAcknowledgedProcessPolicy(ctx)
	}
	if err := r.checkHandle(); err != nil {
		return err
	}
	return r.resume(ctx)
}

// preflight is the read-only admission of a new directory removal: the peer
// and directory policies are present, the directory has a stable mapped
// owner, the daemon cohort is captured and confirmed, and a second read finds
// the same policy and owner. It records the owner. Mirror.mu is held.
func (r *DirectoryRemoval) preflight(ctx context.Context) error {
	mirror := r.original.mirror
	if err := r.peerPolicy(ctx); err != nil {
		return err
	}
	present, err := r.policy(ctx)
	if err != nil {
		return err
	}
	if !present {
		return cephFSObserveGuard("original CephFS directory policy is absent before removal")
	}
	mapping, err := mirror.readCephFSObservedMapping(ctx, r.directory)
	if err != nil {
		return err
	}
	if mapping.State != "mapped" {
		return cephFSObserveGuard("strict CephFS directory removal requires a stable mapped owner")
	}
	r.ownerID = mapping.InstanceID
	if err := r.original.captureCohort(ctx); err != nil {
		return err
	}
	if err := r.confirmCohort(ctx, true); err != nil {
		return err
	}
	if err := r.peerPolicy(ctx); err != nil {
		return err
	}
	present, err = r.policy(ctx)
	if err != nil {
		return err
	}
	mapping, err = mirror.readCephFSObservedMapping(ctx, r.directory)
	if err != nil {
		return err
	}
	if !present || mapping.State != "mapped" || mapping.InstanceID != r.ownerID {
		return cephFSObserveGuard("original CephFS directory owner changed during removal preflight")
	}
	return ctx.Err()
}

// Called under mirror.mu or before the constructor publishes its handle. Only
// acknowledged owned registrations advance this generation: constructor add,
// public additions and successful rebalance add.
func (ops *mirrorOperations) advanceDirectoryGeneration(directory string) {
	ops.supersedeDirectoryAddition(directory)
	if ops.directoryGenerations == nil {
		ops.directoryGenerations = make(map[string]uint64)
	}
	ops.directoryGenerations[directory]++
}

func (ops *mirrorOperations) guardDirectoryRemovalOverlap() error {
	if ops.directoryRemoval != nil && !ops.directoryRemoval.terminal() {
		return cephFSObserveGuard("explicit CephFS directory release is incomplete")
	}
	return nil
}

// checkHandle is called only under mirror.mu. This composition reuses the peer
// receipt's attestation/session parsers, never its peer teardown completion.
func (r *DirectoryRemoval) checkHandle() error {
	return r.checkDirectoryRemovalHandle(false)
}

// allowRetired belongs only to the separate original-process observation.
func (r *DirectoryRemoval) checkDirectoryRemovalHandle(allowRetired bool) error {
	if r == nil || r.original == nil || r.original.mirror == nil || r.original.source == nil || r.original.destination == nil || r.original.readClusters == nil || len(r.original.cohort) == 0 {
		return cephFSObserveGuard("CephFS directory removal original identity is unavailable")
	}
	o, m := r.original, r.original.mirror
	if m.closed || r.superseded || m.ops.directoryRemoval != r || m.ops.directoryGenerations[r.directory] != r.generation || r.generation == 0 || m.ops.peerGeneration != o.generation {
		return cephFSObserveGuard("CephFS directory removal owner is closed or superseded")
	}
	if m.source != o.source || m.destination != o.destination || m.SourceFilesystem != o.sourceFilesystem || m.DestinationFilesystem != o.destinationFilesystem || m.SourceClientEntity != o.sourceClient || m.filesystemID != o.sourceID || m.destinationFilesystemID != o.destinationID || m.metadataPoolID != o.sourceMetadata || m.destinationMetadataPoolID != o.destinationMetadata || m.metadataPool != o.metadataPool || m.DestinationClientEntity != o.peer.ClientName || m.destinationSite != o.peer.SiteName || m.peerID != o.peerID {
		return cephFSObserveGuard("CephFS directory removal original fixture identity changed")
	}
	if !allowRetired && len(m.daemons) != len(o.cohort) {
		return cephFSObserveGuard("CephFS directory removal original owned cohort changed")
	}
	seen := make(map[*Daemon]bool)
	for _, daemon := range m.daemons {
		seen[daemon] = true
	}
	for _, w := range o.cohort {
		if !allowRetired && !seen[w.daemon] {
			return cephFSObserveGuard("CephFS directory removal original daemon ownership changed")
		}
	}
	return nil
}

func (r *DirectoryRemoval) peerPolicy(ctx context.Context) error {
	present, fsPresent, err := r.original.policy(ctx)
	if err != nil {
		return err
	}
	if !present || !fsPresent {
		return cephFSObserveGuard("CephFS directory release original MON or MGR peer disappeared")
	}
	return ctx.Err()
}

func (r *DirectoryRemoval) policy(ctx context.Context) (bool, error) {
	data, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", r.original.sourceFilesystem)
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS directory removal policy", err)
	}
	seen, err := decodeCephFSDirectoryPolicy(data)
	if err != nil {
		return false, err
	}
	return seen[r.directory], ctx.Err()
}

// Shared strict native ls authority for retained path registration/removal.
func decodeCephFSDirectoryPolicy(data []byte) (map[string]bool, error) {
	var paths []string
	if json.Unmarshal(data, &paths) != nil || paths == nil {
		return nil, cephFSObserveGuard("decode CephFS directory policy array")
	}
	seen := make(map[string]bool)
	for _, directory := range paths {
		canonical, err := normalizeMirrorDirectory(directory)
		if err != nil || canonical != directory || seen[directory] {
			return nil, cephFSObserveGuard("CephFS directory policy path identity is ambiguous")
		}
		seen[directory] = true
	}
	return seen, nil
}

func (r *DirectoryRemoval) forgetDesiredPolicy() {
	m := r.original.mirror
	m.Directories = slices.DeleteFunc(m.Directories, func(path string) bool { return path == r.directory })
	delete(m.ownedDirectories, r.directory)
	delete(m.pendingDirectoryRelease, r.directory)
}

// Called under mirror.mu. Only the exact retained original path is retried.
// The native remove is sent at most once per receipt; no error-text parsing is
// used.
func (r *DirectoryRemoval) resume(ctx context.Context) error {
	if err := r.checkHandle(); err != nil {
		return err
	}
	if err := r.peerPolicy(ctx); err != nil {
		return err
	}
	present, err := r.policy(ctx)
	if err != nil {
		return err
	}
	if !present {
		r.forgetDesiredPolicy()
		return ctx.Err()
	}
	if r.requestAcknowledged {
		return cephFSObserveQuery("wait for acknowledged CephFS directory removal policy", nil)
	}
	if r.requestAttempted {
		return uncertainRemoval(ctx, r.original.mirror, r.directory)
	}
	mapping, err := r.original.mirror.readCephFSObservedMapping(ctx, r.directory)
	if err != nil {
		return err
	}
	if mapping.State != "mapped" {
		return cephFSObserveQuery("wait for original CephFS directory removal transition", nil)
	}
	if mapping.InstanceID != r.ownerID {
		return cephFSObserveGuard("CephFS directory removal original mapped owner changed")
	}
	if err := r.confirmCohort(ctx, true); err != nil {
		return err
	}
	if err := r.original.mirror.waitNativePolicyIdle(ctx); err != nil {
		return cephFSObserveQuery("wait for an idle native CephFS mirror policy before removal", err)
	}
	if err := r.peerPolicy(ctx); err != nil {
		return err
	}
	present, err = r.policy(ctx)
	if err != nil {
		return err
	}
	if !present {
		r.forgetDesiredPolicy()
		return ctx.Err()
	}
	mapping, err = r.original.mirror.readCephFSObservedMapping(ctx, r.directory)
	if err != nil {
		return err
	}
	if mapping.State != "mapped" {
		return cephFSObserveQuery("wait for original CephFS directory removal transition", nil)
	}
	if mapping.InstanceID != r.ownerID {
		return cephFSObserveGuard("CephFS directory removal original mapped owner changed before native removal")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.requestAttempted = true
	if _, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "remove", r.original.sourceFilesystem, r.directory); err != nil {
		return cephFSObserveQuery("remove original CephFS mirror directory", err)
	}
	r.requestAcknowledged = true
	r.forgetDesiredPolicy()
	return ctx.Err()
}

func (r *DirectoryRemoval) confirmCohort(ctx context.Context, requireOwnerPath bool) error {
	watchers, err := r.original.watchers(ctx)
	if err != nil {
		return err
	}
	if err := r.original.checkWatcherCohort(watchers); err != nil {
		return err
	}
	owner := false
	for _, w := range r.original.cohort {
		if err := multicluster.LockObservation(ctx, &w.daemon.mu); err != nil {
			return err
		}
		state, err := r.observeDaemon(ctx, w, watchers)
		w.daemon.mu.Unlock()
		if err != nil {
			return err
		}
		if w.instanceID == r.ownerID {
			owner = true
			if requireOwnerPath && state == "released" {
				return cephFSObserveGuard("CephFS mapped owner lacked the original directory path witness")
			}
		}
	}
	if !owner {
		return cephFSObserveGuard("CephFS directory owner is outside the original owned cohort")
	}
	after, err := r.original.watchers(ctx)
	if err != nil {
		return err
	}
	if !equalCephFSRemovalWatchers(watchers, after) {
		return cephFSObserveGuard("CephFS directory removal watchers changed during preflight")
	}
	return errors.Join(r.original.checkWatcherCohort(after), ctx.Err())
}

func (r *DirectoryRemoval) initialStatus() DirectoryRemovalStatus {
	result := DirectoryRemovalStatus{}
	if r == nil || r.original == nil {
		return result
	}
	o := r.original
	result = DirectoryRemovalStatus{Directory: r.directory, PeerID: o.peerID, SourceFilesystem: o.sourceFilesystem, DestinationFilesystem: o.destinationFilesystem, SourceFilesystemID: o.sourceID, DestinationFilesystemID: o.destinationID, Daemons: make(map[string]DirectoryRemovalDaemonStatus)}
	for _, w := range o.cohort {
		result.Daemons[w.name] = DirectoryRemovalDaemonStatus{ContainerID: w.containerID, InstanceID: w.instanceID, State: "unobserved"}
	}
	return result
}

// Status never changes native policy/processes. It checks original MON/MGR peer,
// complete watcher/cohort and normal exact peer command/session around native
// whole-map path stats observations. Schema/identity changes permanently guard;
// native query failures remain retryable. Common attestation conservatively
// guards non-context failures while retaining the intent for fresh-context retry.
//
//purpose:observation
func (r *DirectoryRemoval) Status(ctx context.Context) (DirectoryRemovalStatus, error) {
	return r.statusWithAdmission(ctx, nil)
}

// Admission is private observation metadata, not a public proof field. A fresh
// admitted policy or daemon observation must never be replaced by stale data.
func (r *DirectoryRemoval) statusWithAdmission(ctx context.Context, admitted *bool) (DirectoryRemovalStatus, error) {
	result := r.initialStatus()
	if r == nil || r.original == nil || r.original.mirror == nil {
		return result, cephFSObserveGuard("CephFS directory removal handle is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &r.original.mirror.mu); err != nil {
		return result, err
	}
	defer r.original.mirror.mu.Unlock()
	if admitted != nil {
		*admitted = true
	}
	if err := r.checkHandle(); err != nil {
		return result, err
	}
	if err := r.peerPolicy(ctx); err != nil {
		return result, err
	}
	present, err := r.policy(ctx)
	if err != nil {
		return result, err
	}
	if present {
		return result, ctx.Err()
	}
	watchers, err := r.original.watchers(ctx)
	if err != nil {
		return result, err
	}
	if err := r.original.checkWatcherCohort(watchers); err != nil {
		return result, err
	}
	all, errs, guarded := r.observeCohort(ctx, watchers, result.Daemons)
	if guarded {
		return result, errors.Join(errs...)
	}
	after, err := r.original.watchers(ctx)
	if err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if !equalCephFSRemovalWatchers(watchers, after) {
		all = false
		errs = append(errs, cephFSObserveGuard("CephFS directory release original watcher set changed"))
	}
	if err := r.original.checkWatcherCohort(after); err != nil {
		all = false
		errs = append(errs, err)
	}
	if err := r.peerPolicy(ctx); err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	present, err = r.policy(ctx)
	if err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if present {
		all = false
	} else {
		result.PolicyRemoved = true
	}
	if err := r.checkHandle(); err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(append(errs, err)...)
	}
	result.Released = result.PolicyRemoved && all && len(errs) == 0
	if result.Released {
		r.completed = true
		r.forgetDesiredPolicy()
	}
	return result, errors.Join(errs...)
}

// observeCohort observes each original daemon under its own lock and records
// its state and problem in daemons. It reports whether every daemon has
// released the directory, the observation errors, and whether a guard
// refusal ended the observation early. Mirror.mu is held.
func (r *DirectoryRemoval) observeCohort(ctx context.Context, watchers map[string]string, daemons map[string]DirectoryRemovalDaemonStatus) (bool, []error, bool) {
	all := true
	var errs []error
	for _, w := range r.original.cohort {
		entry := daemons[w.name]
		if err := multicluster.LockObservation(ctx, &w.daemon.mu); err != nil {
			all = false
			errs = append(errs, err)
			break
		}
		state, err := r.observeDaemon(ctx, w, watchers)
		w.daemon.mu.Unlock()
		entry.State = state
		if err != nil {
			all = false
			entry.Problem = err.Error()
			errs = append(errs, err)
		}
		if state != "released" {
			all = false
		}
		daemons[w.name] = entry
		if errors.Is(err, errCephFSObservationGuard) {
			return all, errs, true
		}
	}
	return all, errs, false
}

// WaitReleased drops fixture/member locks after each bounded observation and
// between polls. Deadline/cancel returns the last partial report, Released=false,
// preserving both the caller cause and last native observation error.
//
//purpose:observation
func (r *DirectoryRemoval) WaitReleased(ctx context.Context) (DirectoryRemovalStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return waitCephFSDirectoryReleasedWithAdmission(ctx, 500*time.Millisecond, func(attempt context.Context) (DirectoryRemovalStatus, bool, error) {
		admitted := false
		status, err := r.statusWithAdmission(attempt, &admitted)
		return status, admitted, err
	})
}

func waitCephFSDirectoryReleasedWithAdmission(ctx context.Context, interval time.Duration, observe func(context.Context) (DirectoryRemovalStatus, bool, error)) (DirectoryRemovalStatus, error) {
	return waitCephFSRemovalCompletion(ctx, interval, observe,
		func(status DirectoryRemovalStatus) bool { return status.Released },
		func(status *DirectoryRemovalStatus) { status.Released = false })
}

func (r *DirectoryRemoval) observeDaemon(ctx context.Context, w cephFSPeerRemovalWitness, watchers map[string]string) (string, error) {
	if w.daemon == nil || w.daemon.Container == nil || w.daemon.removed {
		return "retired", cephFSObserveQuery("CephFS directory release original daemon is unavailable", nil)
	}
	if w.daemon.DaemonName != w.name || w.daemon.GetContainerID() != w.containerID {
		return "session-changed", cephFSObserveGuard("CephFS directory release original container identity changed")
	}
	state, err := w.daemon.State(ctx)
	if err != nil {
		return "unavailable", cephFSObserveQuery("inspect original CephFS directory release process", err)
	}
	if !normalCephFSRemovalProcess(state) {
		return "stopped", cephFSObserveQuery("CephFS directory release original process is not normal", nil)
	}
	if !sameCephFSRemovalStartedAt(state.StartedAt, w.startedAt) {
		return "session-changed", cephFSObserveGuard("CephFS directory release original process generation changed")
	}
	before, err := r.original.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if before.address != w.address || watchers[w.address] != w.instanceID || !before.peerPresent {
		return "session-changed", cephFSObserveGuard("CephFS directory release original peer filesystem session changed")
	}
	registered, err := r.original.catalog(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if !registered {
		return "session-changed", cephFSObserveGuard("CephFS directory release original peer command disappeared")
	}
	data, err := multicluster.Exec(ctx, w.daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", r.original.sourceFilesystem, r.original.sourceID), r.original.peerID)
	if err != nil {
		return "unavailable", cephFSObserveQuery("read original CephFS directory release peer stats", err)
	}
	present, err := decodeCephFSDirectoryRemovalStats(data, r.directory)
	if err != nil {
		return "unavailable", err
	}
	registered, err = r.original.catalog(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if !registered {
		return "session-changed", cephFSObserveGuard("CephFS directory release peer command disappeared during observation")
	}
	after, err := r.original.session(ctx, w.daemon)
	if err != nil {
		return "unavailable", err
	}
	if after != before {
		return "session-changed", cephFSObserveGuard("CephFS directory release original session changed during observation")
	}
	state, err = w.daemon.State(ctx)
	if err != nil {
		return "unavailable", cephFSObserveQuery("confirm original CephFS directory release process", err)
	}
	if !normalCephFSRemovalProcess(state) {
		return "stopped", cephFSObserveQuery("CephFS directory release original process stopped during observation", nil)
	}
	if !sameCephFSRemovalStartedAt(state.StartedAt, w.startedAt) || w.daemon.GetContainerID() != w.containerID {
		return "session-changed", cephFSObserveGuard("CephFS directory release original process changed during observation")
	}
	if err := ctx.Err(); err != nil {
		return "unavailable", err
	}
	if present {
		return "tracked", nil
	}
	return "released", nil
}

// Native peer_status is a flat object. Refuse a duplicate key at any depth
// before map decoding can overwrite one, stream the paths to require each to be
// canonical, and validate every entry with the existing production replay
// decoder before treating selected path absence as cycle-unregister evidence.
// Empty {} requires separate normal peer guards.
func decodeCephFSDirectoryRemovalStats(data []byte, selected string) (bool, error) {
	if err := strictjson.Check(data, strictjson.Options{}); errors.Is(err, strictjson.ErrDuplicateKey) {
		return false, cephFSObserveGuard("CephFS directory release peer stats path identity is ambiguous")
	} else if errors.Is(err, strictjson.ErrTrailing) {
		return false, cephFSObserveGuard("decode CephFS directory release peer stats trailing data")
	} else if err != nil {
		return false, cephFSObserveGuard("decode CephFS directory release peer stats object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false, cephFSObserveGuard("decode CephFS directory release peer stats object")
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false, cephFSObserveGuard("decode CephFS directory release peer stats key")
		}
		directory, ok := token.(string)
		canonical, pathErr := normalizeMirrorDirectory(directory)
		if !ok || pathErr != nil || canonical != directory || seen[directory] {
			return false, cephFSObserveGuard("CephFS directory release peer stats path identity is ambiguous")
		}
		seen[directory] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false, cephFSObserveGuard("decode CephFS directory release peer stats value")
		}
		entry, err := json.Marshal(map[string]json.RawMessage{directory: value})
		if err != nil {
			return false, cephFSObserveGuard("decode CephFS directory release peer stats entry")
		}
		_, present, err := decodeCephFSObservedReplay(entry, directory)
		if err != nil || !present {
			return false, errors.Join(cephFSObserveGuard("decode CephFS directory release peer stats entry"), err)
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return false, cephFSObserveGuard("decode CephFS directory release peer stats object end")
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return false, cephFSObserveGuard("decode CephFS directory release peer stats trailing data")
	}
	return seen[selected], nil
}
