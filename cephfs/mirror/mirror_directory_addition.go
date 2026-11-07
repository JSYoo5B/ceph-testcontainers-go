package mirror

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/google/uuid"
)

// DirectoryAddition retains one explicit addition intent and its original
// scope; it captures no daemon cohort. It owns
// no cluster, daemon, directory, data or cleanup hook. A nonnil receipt returned
// with an error retains an uncertain request; repeat BeginDirectoryAddition with
// the same path and a fresh context. Only one pending intent is admitted.
type DirectoryAddition struct {
	scope                                 *cephFSOriginalScope
	directory                             string
	baseGeneration, generation            uint64
	requestAttempted, requestAcknowledged bool
	registered, superseded                bool
	// Receipts displaced by this pending add, restored only if the original
	// scope confirms absence after a native refusal.
	priorAddition *DirectoryAddition
	priorRemoval  *DirectoryRemoval
}

// DirectoryAdditionStatus reports native policy presence in the
// retained original scope. Registered also requires this receipt's published
// owned generation. Neither flag proves daemon assignment or snapshot replay.
type DirectoryAdditionStatus struct {
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
//
// Only one addition can be pending; while it is, AddDaemon, AddDirectory,
// RebalanceDirectories, directory and peer removal and RebootstrapPeer are
// refused. DirectoryAddition.Status reports PolicyPresent (the path is in
// Ceph's policy) and Registered (the fixture also owns it); after a lost reply
// the policy can be present while Registered stays false, and only a repeated
// BeginDirectoryAddition confirms ownership. Neither flag says that a daemon
// picked up the path. When the native add exits unsuccessfully and a fresh
// observation confirms absence in the original scope, the fixture retires the
// intent and returns a nil receipt. Transport failures retain the pending receipt.
//
//purpose:construction,condition
func (mirror *Container) BeginDirectoryAddition(ctx context.Context, directory string) (*DirectoryAddition, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory addition fixture is unavailable")
	}
	return mirror.beginDirectoryAddition(ctx, directory, mirror.peerRemovalClusterIdentities)
}

func (mirror *Container) beginDirectoryAddition(ctx context.Context, directory string, readClusters func(context.Context) ([2]string, error)) (*DirectoryAddition, error) {
	if mirror == nil {
		return nil, cephFSObserveGuard("CephFS directory addition fixture is unavailable")
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
	if err := mirror.ops.guardCephFSRemovalOverlap(); err != nil {
		return nil, err
	}
	if r := mirror.ops.directoryAddition; r != nil && r.directory == directory && !r.superseded {
		return r.result(r.resume(ctx))
	}
	if err := mirror.ops.guardDirectoryAdditionOverlap(); err != nil {
		return nil, err
	}
	if err := mirror.confirmCephFSRefreshHandle(true); err != nil {
		return nil, err
	}
	parsed, err := uuid.Parse(mirror.peerID)
	if err != nil || parsed.String() != mirror.peerID || parsed == uuid.Nil || mirror.ops.peerGeneration == 0 {
		return nil, cephFSObserveGuard("CephFS directory addition original identity is unavailable")
	}
	if mirror.directories.hasIntent(directory) {
		return nil, cephFSObserveGuard("CephFS directory addition path already has owned intent")
	}
	scope, err := mirror.captureOriginalScope(ctx, "CephFS directory addition", mirror.peerID, readClusters)
	if err != nil {
		return nil, err
	}
	r := &DirectoryAddition{scope: scope, directory: directory, baseGeneration: mirror.directories.generation(directory)}
	present, err := r.observe(ctx, false)
	if err != nil {
		return nil, err
	}
	if present {
		return nil, cephFSObserveGuard("CephFS directory addition path existed before this intent")
	}
	if previous := mirror.ops.directoryAddition; previous != nil && !previous.superseded {
		r.priorAddition = previous
		previous.superseded = true
	}
	// Register BEFORE mutation; an uncertain native response cannot erase intent.
	mirror.ops.directoryAddition = r
	return r.result(r.attempt(ctx))
}

// A settled refusal retires the attempted receipt. A caller retrying the
// attempt must never receive the earlier receipt it displaced.
func (r *DirectoryAddition) result(err error) (*DirectoryAddition, error) {
	if r.superseded {
		return nil, err
	}
	return r, err
}

func (ops *mirrorOperations) guardDirectoryAdditionOverlap() error {
	if r := ops.directoryAddition; r != nil && !r.registered {
		return cephFSObserveGuard("explicit CephFS directory addition is incomplete")
	}
	return nil
}

// Called before a selected-path mutation, including an uncertain native attempt.
func (ops *mirrorOperations) supersedeDirectoryAddition(directory string) {
	if r := ops.directoryAddition; r != nil && r.registered && r.directory == directory {
		r.superseded = true
	}
}

// Peer removal invalidates only a completed addition from that exact owned peer
// generation. Read-only/stale receipt retries do not retire a newer addition.
func (ops *mirrorOperations) supersedePeerDirectoryAddition(id string) {
	if r := ops.directoryAddition; r != nil && r.registered && r.scope != nil && r.scope.peerID == id && r.scope.generation == ops.peerGeneration {
		r.superseded = true
	}
}

func (r *DirectoryAddition) checkHandle(retained bool) error {
	if r == nil || !r.scope.usable() {
		return cephFSObserveGuard("CephFS directory addition original identity is unavailable")
	}
	m := r.scope.mirror
	if m.closed || r.superseded || (retained && m.ops.directoryAddition != r) || m.ops.peerGeneration != r.scope.generation {
		return cephFSObserveGuard("CephFS directory addition owner is closed or superseded")
	}
	if !r.scope.sameFixture(false) || m.pendingPeerImport != nil {
		return cephFSObserveGuard("CephFS directory addition original fixture identity changed")
	}
	want := r.baseGeneration
	if r.registered {
		want = r.generation
	}
	if m.directories.generation(r.directory) != want || m.directories.owns(r.directory) != r.registered || m.directories.releasePending(r.directory) {
		return cephFSObserveGuard("CephFS directory addition original path generation changed")
	}
	return nil
}

// Observe only one strict native ls authority, bracketed with original peer/FS
// identities. No watcher or daemon cohort is needed for policy registration.
func (r *DirectoryAddition) observe(ctx context.Context, retained bool) (bool, error) {
	var before bool
	for pass := 0; pass < 2; pass++ {
		if err := r.checkHandle(retained); err != nil {
			return false, err
		}
		present, fsPresent, err := r.scope.policy(ctx)
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
	present, fsPresent, err := r.scope.policy(ctx)
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

func (r *DirectoryAddition) pathPolicy(ctx context.Context) (bool, error) {
	paths, err := r.scope.directoryPolicy(ctx, "directory addition")
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

func (r *DirectoryAddition) resume(ctx context.Context) error {
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
		if err := r.checkHandle(true); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		m := r.scope.mirror
		// checkHandle confirmed the base generation, so the registration
		// publishes base+1.
		m.registerDirectory(r.directory, true)
		r.generation = m.directories.generation(r.directory)
		r.registered = true
		r.priorAddition, r.priorRemoval = nil, nil
		return nil
	}
	if r.requestAcknowledged {
		return cephFSObserveQuery("wait for acknowledged CephFS directory addition policy convergence", nil)
	}
	return r.attempt(ctx)
}

func (r *DirectoryAddition) attempt(ctx context.Context) error {
	if err := r.checkHandle(true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := r.scope.mirror
	if old := m.ops.directoryRemoval; old != nil && !old.superseded && old.terminal() && old.directory == r.directory {
		r.priorRemoval = old
		old.superseded = true
	}
	r.requestAttempted = true
	if _, err := r.scope.source.Ceph(ctx, "fs", "snapshot", "mirror", "add", r.scope.sourceFilesystem, r.directory); err != nil {
		var failure *ceph.NativeFailure
		if errors.As(err, &failure) {
			present, readErr := r.observe(ctx, true)
			if readErr == nil && !present {
				r.superseded = true
				m.ops.directoryAddition = r.priorAddition
				if r.priorAddition != nil {
					r.priorAddition.superseded = false
				}
				if r.priorRemoval != nil {
					r.priorRemoval.superseded = false
				}
				r.priorAddition, r.priorRemoval = nil, nil
			}
			err = errors.Join(err, readErr)
		}
		return cephFSObserveQuery("add original CephFS mirror directory", err)
	}
	r.requestAcknowledged = true
	return r.resume(ctx)
}

// Status is read-only. A visible policy after a lost reply can be reported while
// Registered remains false until an explicit fresh Begin reconciles the intent.
// Each observation is bounded to 30 seconds, or the caller's earlier deadline.
//
//purpose:observation
func (r *DirectoryAddition) Status(ctx context.Context) (DirectoryAdditionStatus, error) {
	result := DirectoryAdditionStatus{}
	if r == nil || r.scope == nil || r.scope.mirror == nil {
		return result, cephFSObserveGuard("CephFS directory addition handle is unavailable")
	}
	o := r.scope
	result = DirectoryAdditionStatus{Directory: r.directory, PeerID: o.peerID, SourceFilesystem: o.sourceFilesystem, DestinationFilesystem: o.destinationFilesystem, SourceFilesystemID: o.sourceID, DestinationFilesystemID: o.destinationID}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &o.mirror.mu); err != nil {
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
