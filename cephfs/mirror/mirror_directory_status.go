package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Snapshot identifies a source snapshot reported by the native
// receiving peer. ID is the original source snap ID, not the destination ID.
type Snapshot struct {
	ID   uint64
	Name string
}

// Native CEPH_NOSNAP and CEPH_SNAPDIR occupy the two largest uint64 values.
const cephFSObservationMaxSnapshotID = ^uint64(0) - 2

// DirectoryStatus observes one current owned directory policy.
// MappingState is MGR assignment state; State is the selected daemon's native
// idle/syncing/failed replay state. Ready proves a live owned assignment and
// active peer for this directory, not whole-fixture health, data equivalence or
// an application checkpoint. DaemonProblems reports other unavailable members.
// Native counters reset on daemon restart/directory reassignment.
type DirectoryStatus struct {
	SourceFilesystem, DestinationFilesystem         string
	SourceFilesystemID, DestinationFilesystemID     int
	Directory, PeerID, MappingState, InstanceID     string
	DaemonName, State, FailureReason                string
	CurrentSnapshot, LastSyncedSnapshot             *Snapshot
	SnapshotsSynced, SnapshotsDeleted, SnapsRenamed uint64
	DaemonProblems                                  map[string]string
	Ready                                           bool
}

// DirectoryStatus uses current owned processes and exact native filesystem,
// peer, directory and watcher identities. It confirms the selected session and
// watcher association after reading replay data; a change makes the observation
// non-ready with a retryable query error and no replay checkpoint. These reads
// do not form a transaction with subsequent native changes. It never rebalances
// or edits policy.
// An unassigned owner is non-ready. Stopped or unavailable owned members return
// a partial error and DaemonProblems even if this directory's owner is ready.
// This method accepts only currently owned directory policies; removal-drain and
// peer-removal observation require a separate retained policy handle.
//
//purpose:observation
func (mirror *Container) DirectoryStatus(ctx context.Context, directory string) (DirectoryStatus, error) {
	var result DirectoryStatus
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return result, cephFSObserveGuard("CephFS mirror directory must be an absolute path")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return mirror.directoryStatus(ctx, directory, nil)
}

// directoryStatus is one DirectoryStatus observation of a normalized
// directory under Container.mu, bounded by ctx including the lock wait. admit,
// when set, runs first under the lock and refuses the observation with its
// error; a watch or wait checks its pinned identities there. Every lock is
// released before it returns.
func (mirror *Container) directoryStatus(ctx context.Context, directory string, admit func(context.Context) error) (DirectoryStatus, error) {
	var result DirectoryStatus
	if err := mirror.lock(ctx); err != nil {
		return result, err
	}
	defer mirror.mu.Unlock()
	if admit != nil {
		if err := admit(ctx); err != nil {
			return result, err
		}
	}
	if mirror.pair.source == nil || mirror.pair.destination == nil || mirror.peer.pendingImport != nil || mirror.peer.id == "" || mirror.pair.sourceFilesystemID <= 0 || mirror.pair.destinationFilesystemID <= 0 || !mirror.pair.identityConfirmed || mirror.pair.sourceMetadataPoolID < 0 || mirror.pair.destinationMetadataPoolID < 0 {
		return result, cephFSObserveGuard("CephFS mirror filesystem and peer identities are not confirmed")
	}
	if !mirror.directories.owns(directory) {
		return result, cephFSObserveGuard("CephFS mirror does not own this current directory policy")
	}
	result = DirectoryStatus{
		SourceFilesystem: mirror.pair.sourceFilesystem, DestinationFilesystem: mirror.pair.destinationFilesystem,
		SourceFilesystemID: mirror.pair.sourceFilesystemID, DestinationFilesystemID: mirror.pair.destinationFilesystemID,
		Directory: directory, PeerID: mirror.peer.id,
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return result, err
	}
	mapping, err := readDirectoryMapping(ctx, mirror.pair, directory)
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
	watchers, err := readWatchers(ctx, mirror.pair, "read CephFS filesystem watchers")
	if err != nil {
		return result, err
	}
	selected, observationErrors, err := mirror.selectDirectoryDaemon(ctx, mapping.InstanceID, watchers, &result)
	if err != nil {
		return result, err
	}
	if selected == nil {
		if len(observationErrors) != 0 {
			return result, &cephFSObservationPartialError{problems: observationErrors}
		}
		return result, cephFSObserveQuery("CephFS directory has no current owned filesystem session", nil)
	}
	result.DaemonName = selected.daemon.DaemonName
	confirmed := result
	if err := mirror.readDirectoryReplay(ctx, selected.daemon, directory, &confirmed); err != nil {
		return result, err
	}
	if err := mirror.confirmDirectoryUnchanged(ctx, directory, mapping, *selected); err != nil {
		result.Ready = false
		return result, err
	}
	result = confirmed
	if len(observationErrors) != 0 {
		return result, &cephFSObservationPartialError{problems: observationErrors}
	}
	return result, nil
}

type cephFSDirectorySession struct {
	daemon  *Daemon
	address string
}

// selectDirectoryDaemon finds the one owned daemon whose filesystem session
// watches the mapped instance and checks its peer. A daemon that cannot be
// observed is recorded in result.DaemonProblems and returned among the
// observation errors; a contradiction is returned as err. Container.mu is held.
func (mirror *Container) selectDirectoryDaemon(ctx context.Context, instanceID string, watchers map[string]string, result *DirectoryStatus) (*cephFSDirectorySession, []error, error) {
	var selected *cephFSDirectorySession
	var observationErrors []error
	problem := func(name string, err error) {
		if result.DaemonProblems == nil {
			result.DaemonProblems = make(map[string]string)
		}
		result.DaemonProblems[name] = err.Error()
		observationErrors = append(observationErrors, err)
	}
	for _, daemon := range mirror.roster.daemons {
		if daemon == nil {
			problem("<nil>", cephFSObserveQuery("owned CephFS mirror process is unavailable", nil))
			continue
		}
		address, peer, unavailable, err := mirror.observeDaemonSession(ctx, daemon)
		if err != nil {
			return nil, nil, err
		}
		if unavailable != nil {
			problem(daemon.DaemonName, unavailable)
			continue
		}
		if watchers[address] != instanceID {
			continue
		}
		if peer != mirror.pair.peerIdentity() {
			return nil, nil, cephFSObserveGuard("CephFS selected daemon peer destination differs")
		}
		if selected != nil {
			return nil, nil, cephFSObserveGuard("CephFS directory maps to more than one owned filesystem session")
		}
		selected = &cephFSDirectorySession{daemon: daemon, address: address}
	}
	return selected, observationErrors, nil
}

// observeDaemonSession reads one owned daemon's filesystem session: its
// watcher address and peer. A daemon that is missing, not running normally or
// whose session cannot be read or is failed is reported as unavailable, the
// problem recorded for it; a malformed session is returned as err.
func (mirror *Container) observeDaemonSession(ctx context.Context, daemon *Daemon) (address string, peer cephFSPeerIdentity, unavailable, err error) {
	if daemon.Container == nil {
		return "", cephFSPeerIdentity{}, cephFSObserveQuery("owned CephFS mirror process is unavailable", nil), nil
	}
	state, stateErr := daemon.State(ctx)
	if stateErr != nil {
		return "", cephFSPeerIdentity{}, cephFSObserveQuery("inspect CephFS mirror process", stateErr), nil
	}
	if !normalCephFSMirrorProcess(state) {
		return "", cephFSPeerIdentity{}, cephFSObserveQuery("owned CephFS mirror process is not running normally", nil), nil
	}
	session, err := readDaemonSession(ctx, daemon, mirror.pair, "read CephFS mirror filesystem session")
	if err != nil {
		if errors.Is(err, errCephFSObservationGuard) {
			return "", cephFSPeerIdentity{}, nil, err
		}
		return "", cephFSPeerIdentity{}, err, nil
	}
	if session.failed {
		return "", cephFSPeerIdentity{}, cephFSObserveQuery("owned CephFS filesystem session or peer is unavailable", nil), nil
	}
	peer, present := session.peers[mirror.peer.id]
	if !present {
		return "", cephFSPeerIdentity{}, cephFSObserveQuery("owned CephFS filesystem session or peer is unavailable", nil), nil
	}
	return session.address, peer, nil, nil
}

// readDirectoryReplay reads the selected daemon's replay state of directory
// into result.
func (mirror *Container) readDirectoryReplay(ctx context.Context, selected *Daemon, directory string, result *DirectoryStatus) error {
	data, err := adminSocket(ctx, selected, "fs", "mirror", "peer", "status", mirror.pair.sessionName(), mirror.peer.id)
	if err != nil {
		return cephFSObserveQuery("read CephFS mirror directory replay", err)
	}
	if data, err = cephFSPeerStatusDirectories(data, mirror.peer.id); err != nil {
		return err
	}
	replay, present, err := decodeCephFSObservedReplay(data, directory)
	if err != nil {
		return err
	}
	if present {
		result.State, result.FailureReason = replay.State, replay.FailureReason
		result.CurrentSnapshot, result.LastSyncedSnapshot = replay.Current, replay.Last
		result.SnapshotsSynced, result.SnapshotsDeleted, result.SnapsRenamed = replay.Synced, replay.Deleted, replay.Renamed
		result.Ready = replay.State == "idle" || replay.State == "syncing"
	}
	return nil
}

// confirmDirectoryUnchanged rereads the directory mapping and the filesystem
// and peer identities, then the selected session and watcher association.
// Replay is published only after those identities agree with the selection.
func (mirror *Container) confirmDirectoryUnchanged(ctx context.Context, directory string, mapping cephFSObservedMapping, selected cephFSDirectorySession) error {
	after, err := readDirectoryMapping(ctx, mirror.pair, directory)
	if err != nil {
		return err
	}
	if after != mapping {
		return cephFSObserveQuery("CephFS directory assignment changed during observation", nil)
	}
	if err := mirror.checkCephFSObservedIdentities(ctx); err != nil {
		return err
	}
	address, peer, unavailable, err := mirror.observeDaemonSession(ctx, selected.daemon)
	if err != nil {
		return err
	}
	if unavailable != nil {
		return unavailable
	}
	if address != selected.address || peer != mirror.pair.peerIdentity() {
		return cephFSObserveQuery("CephFS selected filesystem session changed during observation", nil)
	}
	watchers, err := readWatchers(ctx, mirror.pair, "read CephFS filesystem watchers")
	if err != nil {
		return err
	}
	if watchers[address] != mapping.InstanceID {
		return cephFSObserveQuery("CephFS selected watcher association changed during observation", nil)
	}
	return ctx.Err()
}

// WaitDirectoryReady waits for a current owned directory to be assigned to a
// live owned process with the captured destination peer. Unrelated stopped
// members remain in DaemonProblems but do not prevent this directory's readiness.
// Snapshot completion and payload equivalence are separate conditions.
//
// It polls for at most four minutes or the caller's deadline and returns the
// last status with the last query error on timeout (errors.Is(err,
// context.DeadlineExceeded) holds). A changed filesystem, peer or directory
// registration fails at once. Start a new wait after re-registering the path
// or recreating the peer.
//
//purpose:observation
func (mirror *Container) WaitDirectoryReady(ctx context.Context, directory string) (DirectoryStatus, error) {
	return mirror.waitDirectory(ctx, directory, func(status DirectoryStatus) bool { return status.Ready })
}

// WaitSnapshotSynced waits for the selected live process to report exactly the
// caller's source snapshot ID and name as last_synced_snap. ID must come from
// the source native client; reusing a name with another ID cannot satisfy this
// wait. It is a point-in-time native report, not destination snapshot retention
// or byte equality. If another newer snapshot becomes last before observation,
// this conservative exact-checkpoint wait can time out. Quiesce checkpoint
// creation and verify the intended destination snapshot bytes separately.
//
// Read the source ID with libcephfs, such as
// snap_info('/data/.snap/<name>')['id']; the destination's own snapshot ID is a
// different number, and taking the expected value from LastSyncedSnapshot tests
// nothing. It waits at most four minutes and fails if the directory
// registration, filesystem or peer is replaced during the wait.
//
//purpose:observation
func (mirror *Container) WaitSnapshotSynced(ctx context.Context, directory string, checkpoint Snapshot) (DirectoryStatus, error) {
	if checkpoint.ID == 0 || checkpoint.ID > cephFSObservationMaxSnapshotID || checkpoint.Name == "" || checkpoint.Name == "." || checkpoint.Name == ".." || strings.ContainsAny(checkpoint.Name, "/\x00\r\n") {
		return DirectoryStatus{}, cephFSObserveGuard("CephFS mirror checkpoint requires a source snapshot ID and valid name")
	}
	return mirror.waitDirectory(ctx, directory, func(status DirectoryStatus) bool {
		return status.Ready && status.LastSyncedSnapshot != nil && *status.LastSyncedSnapshot == checkpoint
	})
}

func (mirror *Container) waitDirectory(ctx context.Context, directory string, ready func(DirectoryStatus) bool) (DirectoryStatus, error) {
	directory, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return DirectoryStatus{}, cephFSObserveGuard("CephFS mirror directory must be an absolute path")
	}
	if mirror == nil {
		return DirectoryStatus{}, cephFSRefusal("CephFS mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	pin := directoryObservationPin{mirror: mirror, directory: directory}
	return waitCephFSObservedDirectory(ctx, time.Second, func(attempt context.Context) (DirectoryStatus, error) {
		attempt, cancel := context.WithTimeout(attempt, 30*time.Second)
		defer cancel()
		if err := pin.running(attempt); err != nil {
			return DirectoryStatus{}, err
		}
		return mirror.directoryStatus(attempt, directory, pin.admit)
	}, ready)
}

func waitCephFSObservedDirectory(ctx context.Context, interval time.Duration, observe func(context.Context) (DirectoryStatus, error), ready func(DirectoryStatus) bool) (DirectoryStatus, error) {
	var last DirectoryStatus
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
		lastErr = multicluster.RetainWaitQueryCause(ctx, lastErr, err)
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

// cephFSRefusal refuses a request that the fixture can never serve: a nil
// or terminated mirror, or a terminated cluster. It matches
// ceph.ErrInvalidRequest, and it is a permanent guard, so a wait or a watch
// ends on it at once.
func cephFSRefusal(message string) error {
	return &cephFSObservationError{message: message, cause: servicehook.Invalid(errors.New(message)), permanent: true}
}

// errCephFSMirrorTerminated refuses every request to a terminated mirror.
func errCephFSMirrorTerminated() error { return cephFSRefusal("CephFS mirror has been terminated") }
func cephFSObserveQuery(operation string, cause error) error {
	return &cephFSObservationError{message: operation + " failed", cause: cause}
}

func (mirror *Container) checkCephFSObservedIdentities(ctx context.Context) error {
	if err := mirror.checkCephFSOriginalFilesystemIdentities(ctx, "read CephFS filesystem identity", "CephFS original filesystem or metadata pool identity differs"); err != nil {
		return err
	}
	peers, err := readPeerList(ctx, mirror.pair, "read CephFS mirror peer policy")
	if err != nil {
		return err
	}
	if len(peers) != 1 {
		return cephFSObserveGuard("CephFS owned peer policy changed")
	}
	if peer, exists := peers[mirror.peer.id]; !exists || peer != mirror.pair.peerIdentity() {
		return cephFSObserveGuard("CephFS owned peer policy destination differs")
	}
	return nil
}

type cephFSObservedReplay struct {
	State, FailureReason     string
	Current, Last            *Snapshot
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
	for _, key := range []string{"current_syncing_snap", "current_sycning_snap", "last_synced_snap"} {
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
		// Squid's cephfs-mirror names the syncing snapshot
		// current_sycning_snap; Tentacle spells it correctly.
		CurrentMisspelled *snapshot `json:"current_sycning_snap"`
		Last              *snapshot `json:"last_synced_snap"`
		Synced            *uint64   `json:"snaps_synced"`
		Deleted           *uint64   `json:"snaps_deleted"`
		Renamed           *uint64   `json:"snaps_renamed"`
	}
	if json.Unmarshal(entry, &native) != nil || native.State == nil || !slices.Contains([]string{"idle", "syncing", "failed"}, *native.State) || native.Synced == nil || native.Deleted == nil || native.Renamed == nil {
		return result, false, cephFSObserveGuard("decode CephFS directory peer replay state/counters")
	}
	if native.Current == nil {
		native.Current = native.CurrentMisspelled
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
		result.Current = &Snapshot{ID: *native.Current.ID, Name: *native.Current.Name}
	}
	if native.Last != nil {
		result.Last = &Snapshot{ID: *native.Last.ID, Name: *native.Last.Name}
	}
	return result, true, nil
}

// checkCephFSOriginalFilesystemIdentities reads the source and then the
// destination filesystem and requires the recorded filesystem ID and metadata
// pool ID of each. A failed read is a transient query error named by
// readOperation and wraps the cause. A missing, null, mistyped or different
// identity is a permanent guard error with the exact mismatch text. Callers
// own both texts so each caller's diagnostics stay its own; the source is
// always checked first and a failure stops before the destination is read.
func (mirror *Container) checkCephFSOriginalFilesystemIdentities(ctx context.Context, readOperation, mismatch string) error {
	if !mirror.originalDirectoryFilesystemNames() {
		return cephFSObserveGuard(mismatch)
	}
	for _, site := range []struct {
		cluster *ceph.Container
		name    string
		id      int
		poolID  int64
	}{
		{mirror.pair.source, mirror.pair.sourceFilesystem, mirror.pair.sourceFilesystemID, mirror.pair.sourceMetadataPoolID},
		{mirror.pair.destination, mirror.pair.destinationFilesystem, mirror.pair.destinationFilesystemID, mirror.pair.destinationMetadataPoolID},
	} {
		identity, err := readFilesystemIdentity(ctx, site.cluster, site.name, readOperation)
		if err != nil && !errors.Is(err, errCephFSObservationGuard) {
			return err
		}
		if err != nil || identity.id != site.id || identity.metadataPool != site.poolID {
			return cephFSObserveGuard(mismatch)
		}
	}
	return nil
}
