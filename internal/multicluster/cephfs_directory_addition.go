package multicluster

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CephFSMirrorDirectoryAddition retains one explicit addition intent. It owns
// no cluster, daemon, directory, data or cleanup hook. A nonnil receipt returned
// with an error retains an uncertain request; repeat BeginDirectoryAddition with
// the same path and a fresh context. Only one pending intent is admitted.
type CephFSMirrorDirectoryAddition struct {
	original                              *CephFSMirrorPeerRemoval
	directory                             string
	baseGeneration, generation            uint64
	requestAttempted, requestAcknowledged bool
	registered, superseded                bool
}

// CephFSMirrorDirectoryAdditionStatus reports native policy presence in the
// retained original scope. Registered also requires this receipt's published
// owned generation. Neither flag proves daemon assignment or snapshot replay.
type CephFSMirrorDirectoryAdditionStatus struct {
	Directory, PeerID                           string
	SourceFilesystem, DestinationFilesystem     string
	SourceFilesystemID, DestinationFilesystemID int
	PolicyPresent, Registered                   bool
}

// BeginDirectoryAddition records a positively absent, privately unowned path
// before native add. A retry may reconcile policy applied after a lost response;
// Status cannot publish ownership. A prior legacy add or preexisting native path
// cannot be adopted. Native policy writers must not race this operation: Ceph's
// path policy has no registration UUID or compare-and-swap identity.
// No live daemon is required on an already initialized fixture. Constructor and
// AddDirectory keep their existing contracts. A new completed-other-path intent
// supersedes the previous addition receipt. Add/reconcile is bounded to two
// minutes, or the caller's earlier deadline.
func (mirror *CephFSMirror) BeginDirectoryAddition(ctx context.Context, directory string) (*CephFSMirrorDirectoryAddition, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory addition fixture is unavailable")
	}
	return mirror.beginDirectoryAddition(ctx, directory, mirror.peerRemovalClusterIdentities)
}

func (mirror *CephFSMirror) beginDirectoryAddition(ctx context.Context, directory string, readClusters func(context.Context) ([2]string, error)) (*CephFSMirrorDirectoryAddition, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory addition fixture is unavailable")
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
	if err := mirror.guardCephFSRemovalOverlap(); err != nil {
		return nil, err
	}
	if r := mirror.directoryAddition; r != nil && r.directory == directory && !r.superseded {
		return r, r.resume(ctx)
	}
	if err := mirror.guardDirectoryAdditionOverlap(); err != nil {
		return nil, err
	}
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(mirror.peerID)
	if err != nil || parsed.String() != mirror.peerID || parsed == uuid.Nil || mirror.peerGeneration == 0 || mirror.metadataPoolID <= 0 || mirror.destinationMetadataPoolID <= 0 || mirror.metadataPool == "" || mirror.SourceClientEntity == "" || readClusters == nil {
		return nil, cephFSObserveGuard("CephFS directory addition original identity is unavailable")
	}
	if mirror.ownedDirectories[directory] || mirror.pendingDirectoryRelease[directory] || mirror.directoryGenerations[directory] == ^uint64(0) {
		return nil, cephFSObserveGuard("CephFS directory addition path already has owned intent")
	}
	clusters, err := readClusters(ctx)
	if err != nil {
		return nil, err
	}
	if !validCephFSRemovalClusters(clusters) {
		return nil, cephFSObserveGuard("CephFS directory addition original clusters are unavailable or equal")
	}
	original := &CephFSMirrorPeerRemoval{mirror: mirror, source: mirror.source, destination: mirror.destination,
		sourceFilesystem: mirror.SourceFilesystem, destinationFilesystem: mirror.DestinationFilesystem, sourceClient: mirror.SourceClientEntity, metadataPool: mirror.metadataPool,
		sourceID: mirror.filesystemID, destinationID: mirror.destinationFilesystemID, sourceMetadata: mirror.metadataPoolID, destinationMetadata: mirror.destinationMetadataPoolID,
		peerID: mirror.peerID, peer: cephFSPeerIdentity{mirror.DestinationClientEntity, mirror.destinationSite, mirror.DestinationFilesystem}, generation: mirror.peerGeneration, clusters: clusters, readClusters: readClusters}
	r := &CephFSMirrorDirectoryAddition{original: original, directory: directory, baseGeneration: mirror.directoryGenerations[directory]}
	present, err := r.observe(ctx, false)
	if err != nil {
		return nil, err
	}
	if present {
		return nil, cephFSObserveGuard("CephFS directory addition path existed before this intent")
	}
	if previous := mirror.directoryAddition; previous != nil {
		previous.superseded = true
	}
	// Register BEFORE mutation; an uncertain native response cannot erase intent.
	mirror.directoryAddition = r
	return r, r.attempt(ctx)
}

func (mirror *CephFSMirror) guardDirectoryAdditionOverlap() error {
	if r := mirror.directoryAddition; r != nil && !r.registered {
		return cephFSObserveGuard("explicit CephFS directory addition is incomplete")
	}
	return nil
}

// Called before a selected-path mutation, including an uncertain native attempt.
func (mirror *CephFSMirror) supersedeDirectoryAddition(directory string) {
	if r := mirror.directoryAddition; r != nil && r.registered && r.directory == directory {
		r.superseded = true
	}
}

// Peer removal invalidates only a completed addition from that exact owned peer
// generation. Read-only/stale receipt retries do not retire a newer addition.
func (mirror *CephFSMirror) supersedePeerDirectoryAddition(id string) {
	if r := mirror.directoryAddition; r != nil && r.registered && r.original != nil && r.original.peerID == id && r.original.generation == mirror.peerGeneration {
		r.superseded = true
	}
}

func (r *CephFSMirrorDirectoryAddition) checkHandle(retained bool) error {
	if r == nil || r.original == nil || r.original.mirror == nil || r.original.source == nil || r.original.destination == nil || r.original.readClusters == nil {
		return cephFSObserveGuard("CephFS directory addition original identity is unavailable")
	}
	o, m := r.original, r.original.mirror
	if m.closed || r.superseded || (retained && m.directoryAddition != r) || m.peerGeneration != o.generation {
		return cephFSObserveGuard("CephFS directory addition owner is closed or superseded")
	}
	if m.source != o.source || m.destination != o.destination || m.SourceFilesystem != o.sourceFilesystem || m.DestinationFilesystem != o.destinationFilesystem || m.SourceClientEntity != o.sourceClient || m.filesystemID != o.sourceID || m.destinationFilesystemID != o.destinationID || m.metadataPoolID != o.sourceMetadata || m.destinationMetadataPoolID != o.destinationMetadata || m.metadataPool != o.metadataPool || m.DestinationClientEntity != o.peer.ClientName || m.destinationSite != o.peer.SiteName || m.peerID != o.peerID || m.pendingPeerImport != nil {
		return cephFSObserveGuard("CephFS directory addition original fixture identity changed")
	}
	want := r.baseGeneration
	if r.registered {
		want = r.generation
	}
	if m.directoryGenerations[r.directory] != want || m.ownedDirectories[r.directory] != r.registered || m.pendingDirectoryRelease[r.directory] {
		return cephFSObserveGuard("CephFS directory addition original path generation changed")
	}
	return nil
}

// Observe only one strict native ls authority, bracketed with original peer/FS
// identities. No watcher or daemon cohort is needed for policy registration.
func (r *CephFSMirrorDirectoryAddition) observe(ctx context.Context, retained bool) (bool, error) {
	var before bool
	for pass := 0; pass < 2; pass++ {
		if err := r.checkHandle(retained); err != nil {
			return false, err
		}
		present, fsPresent, err := r.original.policy(ctx)
		if err != nil {
			return false, err
		}
		if !present || !fsPresent {
			return false, cephFSObserveGuard("CephFS directory addition original MON or MGR peer disappeared")
		}
		current, err := r.pathPolicy(ctx)
		if err != nil {
			return false, err
		}
		if pass == 0 {
			before = current
		} else if current != before {
			return false, cephFSObserveQuery("CephFS directory addition policy changed during observation", nil)
		}
	}
	// The final path read may invoke user transport code; confirm local authority
	// and original native scope again before reporting or publishing success.
	if err := r.checkHandle(retained); err != nil {
		return false, err
	}
	present, fsPresent, err := r.original.policy(ctx)
	if err != nil {
		return false, err
	}
	if !present || !fsPresent {
		return false, cephFSObserveGuard("CephFS directory addition original peer changed during observation")
	}
	after, err := r.pathPolicy(ctx)
	if err != nil {
		return false, err
	}
	if after != before {
		return false, cephFSObserveQuery("CephFS directory addition policy changed during observation", nil)
	}
	if err := r.checkHandle(retained); err != nil {
		return false, err
	}
	return before, ctx.Err()
}

func (r *CephFSMirrorDirectoryAddition) pathPolicy(ctx context.Context) (bool, error) {
	data, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", r.original.sourceFilesystem)
	if err != nil {
		return false, cephFSObserveQuery("read original CephFS directory addition policy", err)
	}
	paths, err := decodeCephFSDirectoryPolicy(data)
	if err != nil {
		return false, err
	}
	for path := range paths {
		if path != r.directory && (path == "/" || r.directory == "/" || strings.HasPrefix(path, r.directory+"/") || strings.HasPrefix(r.directory, path+"/")) {
			return false, cephFSObserveGuard("CephFS directory addition overlaps another native path")
		}
	}
	return paths[r.directory], ctx.Err()
}

func (r *CephFSMirrorDirectoryAddition) resume(ctx context.Context) error {
	present, err := r.observe(ctx, true)
	if err != nil {
		return err
	}
	if r.registered {
		if !present {
			return cephFSObserveGuard("CephFS directory addition registered policy disappeared")
		}
		return nil
	}
	if present {
		if !r.requestAttempted {
			return cephFSObserveGuard("CephFS directory addition has no prior attempted intent")
		}
		return r.publish(ctx)
	}
	if r.requestAcknowledged {
		return cephFSObserveQuery("wait for acknowledged CephFS directory addition policy convergence", nil)
	}
	return r.attempt(ctx)
}

func (r *CephFSMirrorDirectoryAddition) attempt(ctx context.Context) error {
	if err := r.checkHandle(true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := r.original.mirror
	if old := m.directoryRemoval; old != nil && old.terminal() && old.directory == r.directory {
		old.superseded = true
	}
	r.requestAttempted = true
	if _, err := r.original.source.Ceph(ctx, "fs", "snapshot", "mirror", "add", r.original.sourceFilesystem, r.directory); err != nil {
		return cephFSObserveQuery("add original CephFS mirror directory", err)
	}
	r.requestAcknowledged = true
	return r.resume(ctx)
}

func (r *CephFSMirrorDirectoryAddition) publish(ctx context.Context) error {
	if err := r.checkHandle(true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := r.original.mirror
	if !slices.Contains(m.Directories, r.directory) {
		m.Directories = append(m.Directories, r.directory)
	}
	if m.ownedDirectories == nil {
		m.ownedDirectories = make(map[string]bool)
	}
	if m.directoryGenerations == nil {
		m.directoryGenerations = make(map[string]uint64)
	}
	m.ownedDirectories[r.directory] = true
	m.directoryGenerations[r.directory] = r.baseGeneration + 1
	r.generation = m.directoryGenerations[r.directory]
	r.registered = true
	return nil
}

// Status is read-only. A visible policy after a lost reply can be reported while
// Registered remains false until an explicit fresh Begin reconciles the intent.
// Each observation is bounded to 30 seconds, or the caller's earlier deadline.
func (r *CephFSMirrorDirectoryAddition) Status(ctx context.Context) (CephFSMirrorDirectoryAdditionStatus, error) {
	result := CephFSMirrorDirectoryAdditionStatus{}
	if r == nil || r.original == nil || r.original.mirror == nil {
		return result, cephFSObserveGuard("CephFS directory addition handle is unavailable")
	}
	o := r.original
	result = CephFSMirrorDirectoryAdditionStatus{Directory: r.directory, PeerID: o.peerID, SourceFilesystem: o.sourceFilesystem, DestinationFilesystem: o.destinationFilesystem, SourceFilesystemID: o.sourceID, DestinationFilesystemID: o.destinationID}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &o.mirror.mu); err != nil {
		return result, err
	}
	defer o.mirror.mu.Unlock()
	present, err := r.observe(ctx, true)
	if err != nil {
		return result, err
	}
	if r.registered && !present {
		return result, cephFSObserveGuard("CephFS directory addition registered policy disappeared")
	}
	result.PolicyPresent, result.Registered = present, present && r.registered
	return result, nil
}
