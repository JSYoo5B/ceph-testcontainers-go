package multicluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

// CephFSMirrorDirectoryRemoval retains an explicit path removal and its original
// live daemon/peer cohort. It owns no cluster, container, directory or cleanup.
// A non-nil receipt returned with an error retains an uncertain native request;
// retry BeginDirectoryRemoval for the same path with a fresh context.
type CephFSMirrorDirectoryRemoval struct {
	original                                   *CephFSMirrorPeerRemoval
	directory, ownerID                         string
	generation                                 uint64
	requestAcknowledged, completed, superseded bool
}

// CephFSMirrorDirectoryRemovalStatus observes the original path registration.
// Released proves every original replayer has no registered sync cycle for the
// directory in its original normal session. It does not establish worker thread
// exit, successful remote unlock, checkpoint completion or unchanged metadata.
type CephFSMirrorDirectoryRemovalStatus struct {
	Directory, PeerID                           string
	SourceFilesystem, DestinationFilesystem     string
	SourceFilesystemID, DestinationFilesystemID int
	PolicyRemoved, Released                     bool
	Daemons                                     map[string]CephFSMirrorDirectoryRemovalDaemonStatus
}

type CephFSMirrorDirectoryRemovalDaemonStatus struct {
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
func (mirror *CephFSMirror) BeginDirectoryRemoval(ctx context.Context, directory string) (*CephFSMirrorDirectoryRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory removal fixture is unavailable")
	}
	return mirror.beginDirectoryRemoval(ctx, directory, mirror.peerRemovalClusterIdentities)
}

func (mirror *CephFSMirror) beginDirectoryRemoval(ctx context.Context, directory string, readClusters func(context.Context) ([2]string, error)) (*CephFSMirrorDirectoryRemoval, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory removal fixture is unavailable")
	}
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return nil, err
	}
	defer mirror.mu.Unlock()
	if r := mirror.directoryRemoval; r != nil && r.directory == directory && r.generation == mirror.directoryGenerations[directory] && !r.superseded {
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
	if !mirror.ownedDirectories[directory] || mirror.directoryGenerations[directory] == 0 {
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
	original := &CephFSMirrorPeerRemoval{mirror: mirror, source: mirror.source, destination: mirror.destination,
		sourceFilesystem: mirror.SourceFilesystem, destinationFilesystem: mirror.DestinationFilesystem, sourceClient: mirror.SourceClientEntity, metadataPool: mirror.metadataPool,
		sourceID: mirror.filesystemID, destinationID: mirror.destinationFilesystemID, sourceMetadata: mirror.metadataPoolID, destinationMetadata: mirror.destinationMetadataPoolID,
		peerID: mirror.peerID, peer: cephFSPeerIdentity{mirror.DestinationClientEntity, mirror.destinationSite, mirror.DestinationFilesystem}, generation: mirror.peerGeneration, clusters: clusters, readClusters: readClusters}
	r := &CephFSMirrorDirectoryRemoval{original: original, directory: directory, generation: mirror.directoryGenerations[directory]}
	if err := r.peerPolicy(ctx); err != nil {
		return nil, err
	}
	present, err := r.policy(ctx)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, cephFSObserveGuard("original CephFS directory policy is absent before removal")
	}
	mapping, err := mirror.readCephFSObservedMapping(ctx, directory)
	if err != nil {
		return nil, err
	}
	if mapping.State != "mapped" {
		return nil, cephFSObserveGuard("strict CephFS directory removal requires a stable mapped owner")
	}
	r.ownerID = mapping.InstanceID
	if err := original.captureCohort(ctx); err != nil {
		return nil, err
	}
	if err := r.confirmCohort(ctx, true); err != nil {
		return nil, err
	}
	if err := r.peerPolicy(ctx); err != nil {
		return nil, err
	}
	present, err = r.policy(ctx)
	if err != nil {
		return nil, err
	}
	mapping, err = mirror.readCephFSObservedMapping(ctx, directory)
	if err != nil {
		return nil, err
	}
	if !present || mapping.State != "mapped" || mapping.InstanceID != r.ownerID {
		return nil, cephFSObserveGuard("original CephFS directory owner changed during removal preflight")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Store the immutable identity and live witnesses BEFORE native mutation.
	mirror.directoryRemoval = r
	return r, r.resume(ctx)
}

// Called under mirror.mu or before the constructor publishes its handle. Only
// acknowledged owned registrations advance this generation: constructor add,
// public AddDirectory and successful rebalance add.
func (mirror *CephFSMirror) advanceDirectoryGeneration(directory string) {
	if mirror.directoryGenerations == nil {
		mirror.directoryGenerations = make(map[string]uint64)
	}
	mirror.directoryGenerations[directory]++
}

func (mirror *CephFSMirror) guardDirectoryRemovalOverlap() error {
	if mirror.directoryRemoval != nil && !mirror.directoryRemoval.completed {
		return cephFSObserveGuard("explicit CephFS directory release is incomplete")
	}
	return nil
}

// checkHandle is called only under mirror.mu. This composition reuses the peer
// receipt's attestation/session parsers, never its peer teardown completion.
func (r *CephFSMirrorDirectoryRemoval) checkHandle() error {
	return r.checkDirectoryRemovalHandle(false)
}

// allowRetired belongs only to the separate original-process observation.
func (r *CephFSMirrorDirectoryRemoval) checkDirectoryRemovalHandle(allowRetired bool) error {
	if r == nil || r.original == nil || r.original.mirror == nil || r.original.source == nil || r.original.destination == nil || r.original.readClusters == nil || len(r.original.cohort) == 0 {
		return cephFSObserveGuard("CephFS directory removal original identity is unavailable")
	}
	o, m := r.original, r.original.mirror
	if m.closed || r.superseded || m.directoryRemoval != r || m.directoryGenerations[r.directory] != r.generation || r.generation == 0 || m.peerGeneration != o.generation {
		return cephFSObserveGuard("CephFS directory removal owner is closed or superseded")
	}
	if m.source != o.source || m.destination != o.destination || m.SourceFilesystem != o.sourceFilesystem || m.DestinationFilesystem != o.destinationFilesystem || m.SourceClientEntity != o.sourceClient || m.filesystemID != o.sourceID || m.destinationFilesystemID != o.destinationID || m.metadataPoolID != o.sourceMetadata || m.destinationMetadataPoolID != o.destinationMetadata || m.metadataPool != o.metadataPool || m.DestinationClientEntity != o.peer.ClientName || m.destinationSite != o.peer.SiteName || m.peerID != o.peerID {
		return cephFSObserveGuard("CephFS directory removal original fixture identity changed")
	}
	if !allowRetired && len(m.daemons) != len(o.cohort) {
		return cephFSObserveGuard("CephFS directory removal original owned cohort changed")
	}
	seen := make(map[*CephFSMirrorDaemon]bool)
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

func (r *CephFSMirrorDirectoryRemoval) peerPolicy(ctx context.Context) error {
	present, fsPresent, err := r.original.policy(ctx)
	if err != nil {
		return err
	}
	if !present || !fsPresent {
		return cephFSObserveGuard("CephFS directory release original MON or MGR peer disappeared")
	}
	return ctx.Err()
}

func (r *CephFSMirrorDirectoryRemoval) policy(ctx context.Context) (bool, error) {
	data, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", r.original.sourceFilesystem)
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS directory removal policy", err)
	}
	var paths []string
	if json.Unmarshal(data, &paths) != nil || paths == nil {
		return false, cephFSObserveGuard("decode CephFS directory removal policy array")
	}
	seen := make(map[string]bool)
	for _, directory := range paths {
		canonical, err := normalizeMirrorDirectory(directory)
		if err != nil || canonical != directory || seen[directory] {
			return false, cephFSObserveGuard("CephFS directory removal policy path identity is ambiguous")
		}
		seen[directory] = true
	}
	return seen[r.directory], ctx.Err()
}

func (r *CephFSMirrorDirectoryRemoval) forgetDesiredPolicy() {
	m := r.original.mirror
	m.Directories = slices.DeleteFunc(m.Directories, func(path string) bool { return path == r.directory })
	delete(m.ownedDirectories, r.directory)
	delete(m.pendingDirectoryRelease, r.directory)
}

// Called under mirror.mu. Only the exact retained original path is retried.
// Native MGR serializes remove/purging. An uncertain transport may cause more
// than one CLI attempt; no exactly-once invocation or error-text parsing is used.
func (r *CephFSMirrorDirectoryRemoval) resume(ctx context.Context) error {
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
	if _, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "remove", r.original.sourceFilesystem, r.directory); err != nil {
		return cephFSObserveQuery("remove original CephFS mirror directory", err)
	}
	r.requestAcknowledged = true
	r.forgetDesiredPolicy()
	return ctx.Err()
}

func (r *CephFSMirrorDirectoryRemoval) confirmCohort(ctx context.Context, requireOwnerPath bool) error {
	watchers, err := r.original.watchers(ctx)
	if err != nil {
		return err
	}
	if err := r.original.checkWatcherCohort(watchers); err != nil {
		return err
	}
	owner := false
	for _, w := range r.original.cohort {
		if err := lockRGWSyncObservation(ctx, &w.daemon.mu); err != nil {
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

func (r *CephFSMirrorDirectoryRemoval) initialStatus() CephFSMirrorDirectoryRemovalStatus {
	result := CephFSMirrorDirectoryRemovalStatus{}
	if r == nil || r.original == nil {
		return result
	}
	o := r.original
	result = CephFSMirrorDirectoryRemovalStatus{Directory: r.directory, PeerID: o.peerID, SourceFilesystem: o.sourceFilesystem, DestinationFilesystem: o.destinationFilesystem, SourceFilesystemID: o.sourceID, DestinationFilesystemID: o.destinationID, Daemons: make(map[string]CephFSMirrorDirectoryRemovalDaemonStatus)}
	for _, w := range o.cohort {
		result.Daemons[w.name] = CephFSMirrorDirectoryRemovalDaemonStatus{ContainerID: w.containerID, InstanceID: w.instanceID, State: "unobserved"}
	}
	return result
}

// Status never changes native policy/processes. It checks original MON/MGR peer,
// complete watcher/cohort and normal exact peer command/session around native
// whole-map path stats observations. Schema/identity changes permanently guard;
// native query failures remain retryable. Common attestation conservatively
// guards non-context failures while retaining the intent for fresh-context retry.
func (r *CephFSMirrorDirectoryRemoval) Status(ctx context.Context) (CephFSMirrorDirectoryRemovalStatus, error) {
	result := r.initialStatus()
	if r == nil || r.original == nil || r.original.mirror == nil {
		return result, cephFSObserveGuard("CephFS directory removal handle is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &r.original.mirror.mu); err != nil {
		return result, err
	}
	defer r.original.mirror.mu.Unlock()
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
	all := true
	var errs []error
	for _, w := range r.original.cohort {
		entry := result.Daemons[w.name]
		if err := lockRGWSyncObservation(ctx, &w.daemon.mu); err != nil {
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
		result.Daemons[w.name] = entry
		if errors.Is(err, errCephFSObservationGuard) {
			return result, errors.Join(errs...)
		}
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

// WaitReleased drops fixture/member locks after each bounded observation and
// between polls. Deadline/cancel returns the last partial report, Released=false,
// preserving both the caller cause and last native observation error.
func (r *CephFSMirrorDirectoryRemoval) WaitReleased(ctx context.Context) (CephFSMirrorDirectoryRemovalStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return waitCephFSDirectoryReleased(ctx, 500*time.Millisecond, r.Status)
}

func waitCephFSDirectoryReleased(ctx context.Context, interval time.Duration, observe func(context.Context) (CephFSMirrorDirectoryRemovalStatus, error)) (CephFSMirrorDirectoryRemovalStatus, error) {
	var last CephFSMirrorDirectoryRemovalStatus
	var lastError error
	for {
		if err := ctx.Err(); err != nil {
			last.Released = false
			return last, errors.Join(lastError, err)
		}
		current, err := observe(ctx)
		last = current
		if err != nil {
			lastError = err
		}
		if canceled := ctx.Err(); canceled != nil {
			last.Released = false
			return last, errors.Join(lastError, canceled)
		}
		if errors.Is(err, errCephFSObservationGuard) {
			last.Released = false
			return last, err
		}
		if err == nil && last.Released {
			return last, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			last.Released = false
			return last, errors.Join(lastError, ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *CephFSMirrorDirectoryRemoval) observeDaemon(ctx context.Context, w cephFSPeerRemovalWitness, watchers map[string]string) (string, error) {
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
	data, err := exec(ctx, w.daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", r.original.sourceFilesystem, r.original.sourceID), r.original.peerID)
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

// Native peer_status is a flat object. Stream keys to reject duplicate canonical
// paths before map decoding can overwrite them, and validate every entry with
// the existing production replay decoder before treating selected path absence
// as cycle-unregister evidence. Empty {} requires separate normal peer guards.
func decodeCephFSDirectoryRemovalStats(data []byte, selected string) (bool, error) {
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
