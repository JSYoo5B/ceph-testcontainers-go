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
)

// CephFSMirrorSnapshot identifies a source snapshot reported by the native
// receiving peer. ID is the original source snap ID, not the destination ID.
type CephFSMirrorSnapshot struct {
	ID   uint64
	Name string
}

// Native CEPH_NOSNAP and CEPH_SNAPDIR occupy the two largest uint64 values.
const cephFSObservationMaxSnapshotID = ^uint64(0) - 2

// CephFSMirrorDirectoryStatus observes one current owned directory policy.
// MappingState is MGR assignment state; State is the selected daemon's native
// idle/syncing/failed replay state. Ready proves a live owned assignment and
// active peer for this directory, not whole-fixture health, data equivalence or
// an application checkpoint. DaemonProblems reports other unavailable members.
// Native counters reset on daemon restart/directory reassignment.
type CephFSMirrorDirectoryStatus struct {
	SourceFilesystem, DestinationFilesystem         string
	SourceFilesystemID, DestinationFilesystemID     int
	Directory, PeerID, MappingState, InstanceID     string
	DaemonName, State, FailureReason                string
	CurrentSnapshot, LastSyncedSnapshot             *CephFSMirrorSnapshot
	SnapshotsSynced, SnapshotsDeleted, SnapsRenamed uint64
	DaemonProblems                                  map[string]string
	Ready                                           bool
}

// DirectoryStatus uses current owned processes and exact native filesystem,
// peer, directory and watcher identities. It never rebalances or edits policy.
// An unassigned owner is non-ready. Stopped or unavailable owned members return
// a partial error and DaemonProblems even if this directory's owner is ready.
// This method accepts only currently owned directory policies; removal-drain and
// peer-removal observation require a separate retained policy handle.
func (mirror *CephFSMirror) DirectoryStatus(ctx context.Context, directory string) (CephFSMirrorDirectoryStatus, error) {
	var result CephFSMirrorDirectoryStatus
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return result, cephFSObserveGuard("CephFS mirror directory must be an absolute path")
	}
	if mirror == nil {
		return result, cephFSObserveGuard("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &mirror.mu); err != nil {
		return result, err
	}
	defer mirror.mu.Unlock()
	if mirror.closed || mirror.source == nil || mirror.destination == nil || mirror.pendingPeerImport != nil || mirror.peerID == "" || mirror.filesystemID <= 0 || mirror.destinationFilesystemID <= 0 {
		return result, cephFSObserveGuard("CephFS mirror filesystem and peer identities are not confirmed")
	}
	if !mirror.ownedDirectories[directory] {
		return result, cephFSObserveGuard("CephFS mirror does not own this current directory policy")
	}
	result = CephFSMirrorDirectoryStatus{
		SourceFilesystem: mirror.SourceFilesystem, DestinationFilesystem: mirror.DestinationFilesystem,
		SourceFilesystemID: mirror.filesystemID, DestinationFilesystemID: mirror.destinationFilesystemID,
		Directory: directory, PeerID: mirror.peerID,
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return result, err
	}
	mapping, err := mirror.readCephFSObservedMapping(ctx, directory)
	if err != nil {
		return result, err
	}
	result.MappingState, result.InstanceID = mapping.State, mapping.InstanceID
	if mapping.State != "mapped" {
		if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
			return result, err
		}
		return result, ctx.Err()
	}
	control, err := mirror.source.ControlContainerContext(ctx)
	if err != nil {
		return result, cephFSObserveQuery("read CephFS filesystem watchers", err)
	}
	if control == nil {
		return result, cephFSObserveQuery("CephFS source control container is unavailable", nil)
	}
	data, err := exec(ctx, control, "rados", "--pool", mirror.metadataPool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return result, cephFSObserveQuery("read CephFS filesystem watchers", err)
	}
	watchers, err := parseCephFSMirrorWatchers(data)
	if err != nil {
		return result, cephFSObserveGuard("decode CephFS filesystem watchers")
	}
	var selected *CephFSMirrorDaemon
	var observationErrors []error
	problem := func(name string, err error) {
		if result.DaemonProblems == nil {
			result.DaemonProblems = make(map[string]string)
		}
		result.DaemonProblems[name] = err.Error()
		observationErrors = append(observationErrors, err)
	}
	for _, daemon := range mirror.daemons {
		if daemon == nil {
			problem("<nil>", cephFSObserveQuery("owned CephFS mirror process is unavailable", nil))
			continue
		}
		if daemon.Container == nil {
			problem(daemon.DaemonName, cephFSObserveQuery("owned CephFS mirror process is unavailable", nil))
			continue
		}
		state, err := daemon.State(ctx)
		if err != nil {
			problem(daemon.DaemonName, cephFSObserveQuery("inspect CephFS mirror process", err))
			continue
		}
		if state == nil || !state.Running || state.Paused || state.Restarting || state.Dead {
			problem(daemon.DaemonName, cephFSObserveQuery("owned CephFS mirror process is not running normally", nil))
			continue
		}
		data, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", mirror.SourceFilesystem, mirror.filesystemID))
		if err != nil {
			problem(daemon.DaemonName, cephFSObserveQuery("read CephFS mirror filesystem session", err))
			continue
		}
		address, peer, failed, err := decodeCephFSObservedSession(data, mirror.peerID)
		if err != nil {
			return result, err
		}
		if failed {
			problem(daemon.DaemonName, cephFSObserveQuery("owned CephFS filesystem session or peer is unavailable", nil))
			continue
		}
		if watchers[address] != mapping.InstanceID {
			continue
		}
		if peer != (cephFSPeerIdentity{ClientName: mirror.DestinationClientEntity, SiteName: mirror.destinationSite, FilesystemName: mirror.DestinationFilesystem}) {
			return result, cephFSObserveGuard("CephFS selected daemon peer destination differs")
		}
		if selected != nil {
			return result, cephFSObserveGuard("CephFS directory maps to more than one owned filesystem session")
		}
		selected = daemon
	}
	if selected == nil {
		if len(observationErrors) != 0 {
			return result, &cephFSObservationPartialError{problems: observationErrors}
		}
		return result, cephFSObserveQuery("CephFS directory has no current owned filesystem session", nil)
	}
	result.DaemonName = selected.DaemonName
	data, err = exec(ctx, selected, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", mirror.SourceFilesystem, mirror.filesystemID), mirror.peerID)
	if err != nil {
		return result, cephFSObserveQuery("read CephFS mirror directory replay", err)
	}
	replay, present, err := decodeCephFSObservedReplay(data, directory)
	if err != nil {
		return result, err
	}
	if present {
		result.State, result.FailureReason = replay.State, replay.FailureReason
		result.CurrentSnapshot, result.LastSyncedSnapshot = replay.Current, replay.Last
		result.SnapshotsSynced, result.SnapshotsDeleted, result.SnapsRenamed = replay.Synced, replay.Deleted, replay.Renamed
		result.Ready = replay.State == "idle" || replay.State == "syncing"
	}
	after, err := mirror.readCephFSObservedMapping(ctx, directory)
	if err != nil {
		result.Ready = false
		return result, err
	}
	if after != mapping {
		result.Ready = false
		return result, cephFSObserveQuery("CephFS directory assignment changed during observation", nil)
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		result.Ready = false
		return result, err
	}
	if err := ctx.Err(); err != nil {
		result.Ready = false
		return result, err
	}
	if len(observationErrors) != 0 {
		return result, &cephFSObservationPartialError{problems: observationErrors}
	}
	return result, nil
}

// WaitDirectoryReady waits for a current owned directory to be assigned to a
// live owned process with the captured destination peer. Unrelated stopped
// members remain in DaemonProblems but do not prevent this directory's readiness.
// Snapshot completion and payload equivalence are separate conditions.
func (mirror *CephFSMirror) WaitDirectoryReady(ctx context.Context, directory string) (CephFSMirrorDirectoryStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitCephFSObservedDirectory(ctx, time.Second, func(attempt context.Context) (CephFSMirrorDirectoryStatus, error) {
		return mirror.DirectoryStatus(attempt, directory)
	}, func(status CephFSMirrorDirectoryStatus) bool { return status.Ready })
}

// WaitSnapshotSynced waits for the selected live process to report exactly the
// caller's source snapshot ID and name as last_synced_snap. ID must come from
// the source native client; reusing a name with another ID cannot satisfy this
// wait. It is a point-in-time native report, not destination snapshot retention
// or byte equality. If another newer snapshot becomes last before observation,
// this conservative exact-checkpoint wait can time out. Quiesce checkpoint
// creation and verify the intended destination snapshot bytes separately.
func (mirror *CephFSMirror) WaitSnapshotSynced(ctx context.Context, directory string, checkpoint CephFSMirrorSnapshot) (CephFSMirrorDirectoryStatus, error) {
	if checkpoint.ID == 0 || checkpoint.ID > cephFSObservationMaxSnapshotID || checkpoint.Name == "" || checkpoint.Name == "." || checkpoint.Name == ".." || strings.ContainsAny(checkpoint.Name, "/\x00\r\n") {
		return CephFSMirrorDirectoryStatus{}, cephFSObserveGuard("CephFS mirror checkpoint requires a source snapshot ID and valid name")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return waitCephFSObservedDirectory(ctx, time.Second, func(attempt context.Context) (CephFSMirrorDirectoryStatus, error) {
		return mirror.DirectoryStatus(attempt, directory)
	}, func(status CephFSMirrorDirectoryStatus) bool {
		return status.Ready && status.LastSyncedSnapshot != nil && *status.LastSyncedSnapshot == checkpoint
	})
}

func waitCephFSObservedDirectory(ctx context.Context, interval time.Duration, observe func(context.Context) (CephFSMirrorDirectoryStatus, error), ready func(CephFSMirrorDirectoryStatus) bool) (CephFSMirrorDirectoryStatus, error) {
	var last CephFSMirrorDirectoryStatus
	var lastErr error
	var sourceID, destinationID int
	var peerID, directory string
	for {
		if err := ctx.Err(); err != nil {
			last.Ready = false
			return last, fmt.Errorf("wait CephFS mirror directory observation: %w", errors.Join(err, lastErr))
		}
		current, err := observe(ctx)
		if current.SourceFilesystemID > 0 {
			last = current
			if sourceID != 0 && (sourceID != current.SourceFilesystemID || destinationID != current.DestinationFilesystemID || peerID != current.PeerID || directory != current.Directory) {
				last.Ready = false
				return last, cephFSObserveGuard("CephFS filesystem, peer or directory identity changed during wait")
			}
			sourceID, destinationID, peerID = current.SourceFilesystemID, current.DestinationFilesystemID, current.PeerID
			directory = current.Directory
		}
		lastErr = retainWaitQueryCause(ctx, lastErr, err)
		if ctxErr := ctx.Err(); ctxErr != nil {
			last.Ready = false
			return last, fmt.Errorf("wait CephFS mirror directory observation: %w", errors.Join(ctxErr, lastErr))
		}
		if errors.Is(err, errCephFSObservationGuard) {
			last.Ready = false
			return last, err
		}
		_, partial := err.(*cephFSObservationPartialError)
		if (err == nil || partial && current.Ready) && ready(current) {
			return current, nil
		}
		select {
		case <-ctx.Done():
			last.Ready = false
			return last, fmt.Errorf("wait CephFS mirror directory observation: %w", errors.Join(ctx.Err(), lastErr))
		case <-time.After(interval):
		}
	}
}

type cephFSObservationError struct {
	message   string
	cause     error
	permanent bool
}

// A caller-context-only failure has no new independent native query cause.
// Retain an earlier exposed cause without changing inner-attempt timeouts,
// mixed errors, permanent guards, or newer successful observations.
func retainWaitQueryCause(ctx context.Context, previous, current error) error {
	if ctx.Err() != nil && previous != nil && waitContextOnlyError(current) {
		return errors.Join(previous, current)
	}
	return current
}

func waitContextOnlyError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		found := false
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			found = true
			if !waitContextOnlyError(child) {
				return false
			}
		}
		return found
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok && wrapped.Unwrap() != nil {
		return waitContextOnlyError(wrapped.Unwrap())
	}
	return err == context.Canceled || err == context.DeadlineExceeded
}

var errCephFSObservationGuard = errors.New("permanent CephFS observation guard")

// Only problems from non-selected owned members can become a successful
// per-directory wait. Identity/schema/deadline and selected peer errors never
// use this type. DaemonProblems remains in the successful result.
type cephFSObservationPartialError struct{ problems []error }

func (e *cephFSObservationPartialError) Error() string {
	return "some owned CephFS mirror members are unavailable"
}
func (e *cephFSObservationPartialError) Unwrap() []error { return e.problems }

func (e *cephFSObservationError) Error() string { return e.message }
func (e *cephFSObservationError) Unwrap() error { return e.cause }
func (e *cephFSObservationError) Is(target error) bool {
	return e.permanent && target == errCephFSObservationGuard
}
func cephFSObserveGuard(message string) error {
	return &cephFSObservationError{message: message, permanent: true}
}
func cephFSObserveQuery(operation string, cause error) error {
	return &cephFSObservationError{message: operation + " failed", cause: cause}
}

func (mirror *CephFSMirror) checkCephFSObservedIdentities(ctx context.Context) error {
	for _, site := range []struct {
		name   string
		id     int
		poolID int64
		read   func(context.Context, ...string) ([]byte, error)
	}{
		{mirror.SourceFilesystem, mirror.filesystemID, mirror.metadataPoolID, mirror.source.Ceph},
		{mirror.DestinationFilesystem, mirror.destinationFilesystemID, mirror.destinationMetadataPoolID, mirror.destination.Ceph},
	} {
		data, err := site.read(ctx, "fs", "get", site.name, "--format", "json")
		if err != nil {
			return cephFSObserveQuery("read CephFS filesystem identity", err)
		}
		var native struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				MetadataPool *int64 `json:"metadata_pool"`
			} `json:"mdsmap"`
		}
		if json.Unmarshal(data, &native) != nil || native.ID == nil || *native.ID != site.id || native.MDSMap == nil || native.MDSMap.MetadataPool == nil || *native.MDSMap.MetadataPool != site.poolID {
			return cephFSObserveGuard("CephFS original filesystem or metadata pool identity differs")
		}
	}
	data, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", mirror.SourceFilesystem)
	if err != nil {
		return cephFSObserveQuery("read CephFS mirror peer policy", err)
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil || peers == nil {
		return cephFSObserveGuard("decode CephFS mirror peer policy object")
	}
	for id := range peers {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id {
			return cephFSObserveGuard("decode CephFS mirror peer policy UUID")
		}
	}
	if len(peers) != 1 {
		return cephFSObserveGuard("CephFS owned peer policy changed")
	}
	entry, exists := peers[mirror.peerID]
	if !exists {
		return cephFSObserveGuard("CephFS owned peer policy destination differs")
	}
	var peer *struct {
		ClientName     *string `json:"client_name"`
		SiteName       *string `json:"site_name"`
		FilesystemName *string `json:"fs_name"`
	}
	if json.Unmarshal(entry, &peer) != nil || peer == nil || peer.ClientName == nil || peer.SiteName == nil || peer.FilesystemName == nil || *peer.ClientName == "" || *peer.SiteName == "" || *peer.FilesystemName == "" {
		return cephFSObserveGuard("decode CephFS mirror peer policy destination identity")
	}
	if (cephFSPeerIdentity{ClientName: *peer.ClientName, SiteName: *peer.SiteName, FilesystemName: *peer.FilesystemName}) != (cephFSPeerIdentity{ClientName: mirror.DestinationClientEntity, SiteName: mirror.destinationSite, FilesystemName: mirror.DestinationFilesystem}) {
		return cephFSObserveGuard("CephFS owned peer policy destination differs")
	}
	return nil
}

type cephFSObservedMapping struct{ State, InstanceID string }

func (mirror *CephFSMirror) readCephFSObservedMapping(ctx context.Context, directory string) (cephFSObservedMapping, error) {
	var result cephFSObservedMapping
	data, err := mirror.source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", mirror.SourceFilesystem, directory)
	if err != nil {
		return result, cephFSObserveQuery("read CephFS directory mapping", err)
	}
	var native struct {
		State      json.RawMessage `json:"state"`
		InstanceID *string         `json:"instance_id"`
	}
	if json.Unmarshal(data, &native) != nil || len(native.State) == 0 {
		return result, cephFSObserveGuard("decode CephFS directory mapping state")
	}
	// v20.2.4 emits null for a transient UNASSOCIATED state.
	if string(native.State) == "null" {
		result.State = "unassociated"
	} else if json.Unmarshal(native.State, &result.State) != nil || !slices.Contains([]string{"mapped", "mapping", "unmapping", "shuffling", "resolving", "stalled"}, result.State) {
		return result, cephFSObserveGuard("decode CephFS directory mapping state")
	}
	if native.InstanceID != nil {
		result.InstanceID = *native.InstanceID
	}
	if result.State == "mapped" {
		if id, err := strconv.ParseUint(result.InstanceID, 10, 64); err != nil || id == 0 {
			return result, cephFSObserveGuard("decode CephFS mapped filesystem watcher identity")
		}
	}
	return result, nil
}

func decodeCephFSObservedSession(data []byte, peerID string) (string, cephFSPeerIdentity, bool, error) {
	var peer cephFSPeerIdentity
	// Explicit JSON tags for fields with underscores.
	var tagged struct {
		State         json.RawMessage `json:"state"`
		RadosInstance *string         `json:"rados_inst"`
		Peers         map[string]struct {
			Remote *struct {
				ClientName     *string `json:"client_name"`
				ClusterName    *string `json:"cluster_name"`
				FilesystemName *string `json:"fs_name"`
			} `json:"remote"`
		} `json:"peers"`
	}
	if json.Unmarshal(data, &tagged) != nil {
		return "", peer, false, cephFSObserveGuard("decode CephFS daemon filesystem session")
	}
	if len(tagged.State) != 0 {
		var state string
		if json.Unmarshal(tagged.State, &state) != nil {
			return "", peer, false, cephFSObserveGuard("decode CephFS daemon filesystem failure state")
		}
		if state == "failed" || state == "blocklisted" {
			return "", peer, true, nil
		}
		return "", peer, false, cephFSObserveGuard("decode CephFS daemon filesystem failure state")
	}
	if tagged.RadosInstance == nil || *tagged.RadosInstance == "" || tagged.Peers == nil {
		return "", peer, false, cephFSObserveGuard("decode CephFS daemon filesystem session identity")
	}
	entry, present := tagged.Peers[peerID]
	if !present {
		return *tagged.RadosInstance, peer, true, nil
	}
	remote := entry.Remote
	if remote == nil || remote.ClientName == nil || remote.ClusterName == nil || remote.FilesystemName == nil || *remote.ClientName == "" || *remote.ClusterName == "" || *remote.FilesystemName == "" {
		return "", peer, false, cephFSObserveGuard("decode CephFS daemon peer destination identity")
	}
	peer = cephFSPeerIdentity{ClientName: *remote.ClientName, SiteName: *remote.ClusterName, FilesystemName: *remote.FilesystemName}
	return *tagged.RadosInstance, peer, false, nil
}

type cephFSObservedReplay struct {
	State, FailureReason     string
	Current, Last            *CephFSMirrorSnapshot
	Synced, Deleted, Renamed uint64
}

func decodeCephFSObservedReplay(data []byte, directory string) (cephFSObservedReplay, bool, error) {
	var result cephFSObservedReplay
	var raw map[string]json.RawMessage
	if json.Unmarshal(data, &raw) != nil || raw == nil {
		return result, false, cephFSObserveGuard("decode CephFS directory peer status map")
	}
	entry, present := raw[directory]
	if !present {
		return result, false, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(entry, &fields) != nil || fields == nil {
		return result, false, cephFSObserveGuard("decode CephFS directory peer replay entry")
	}
	for _, key := range []string{"current_syncing_snap", "last_synced_snap"} {
		if value, present := fields[key]; present && string(value) == "null" {
			return result, false, cephFSObserveGuard("decode CephFS mirror source snapshot identity")
		}
	}
	type snapshot struct {
		ID   *uint64 `json:"id"`
		Name *string `json:"name"`
	}
	var native struct {
		State         *string   `json:"state"`
		FailureReason string    `json:"failure_reason"`
		Current       *snapshot `json:"current_syncing_snap"`
		Last          *snapshot `json:"last_synced_snap"`
		Synced        *uint64   `json:"snaps_synced"`
		Deleted       *uint64   `json:"snaps_deleted"`
		Renamed       *uint64   `json:"snaps_renamed"`
	}
	if json.Unmarshal(entry, &native) != nil || native.State == nil || !slices.Contains([]string{"idle", "syncing", "failed"}, *native.State) || native.Synced == nil || native.Deleted == nil || native.Renamed == nil {
		return result, false, cephFSObserveGuard("decode CephFS directory peer replay state/counters")
	}
	for _, value := range []*snapshot{native.Current, native.Last} {
		if value != nil && (value.ID == nil || value.Name == nil || *value.ID == 0 || *value.ID > cephFSObservationMaxSnapshotID || *value.Name == "") {
			return result, false, cephFSObserveGuard("decode CephFS mirror source snapshot identity")
		}
	}
	if *native.State == "syncing" && native.Current == nil {
		return result, false, cephFSObserveGuard("CephFS syncing directory has no current source snapshot")
	}
	if *native.State == "idle" && native.Current != nil {
		return result, false, cephFSObserveGuard("CephFS idle directory reports a current syncing snapshot")
	}
	result.State, result.FailureReason = *native.State, native.FailureReason
	result.Synced, result.Deleted, result.Renamed = *native.Synced, *native.Deleted, *native.Renamed
	if native.Current != nil {
		result.Current = &CephFSMirrorSnapshot{ID: *native.Current.ID, Name: *native.Current.Name}
	}
	if native.Last != nil {
		result.Last = &CephFSMirrorSnapshot{ID: *native.Last.ID, Name: *native.Last.Name}
	}
	return result, true, nil
}
