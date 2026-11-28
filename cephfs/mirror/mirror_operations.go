package mirror

// mirrorOperations is the ledger of a mirror's explicit operations in flight:
// the peer drain, the directory release and the directory addition, with the
// peer generation that lets a completed addition be superseded. Its guards
// keep two of these operations from running against the same native state.
// Every method requires the caller to hold Container.mu.
//
// Which operation runs which guard:
//
//	operation                  guards, in order
//	AddDaemon                  guardAnyPending
//	AddDirectory               guardAnyPending
//	RebalanceDirectories       guardAnyPending
//	RebootstrapPeer            guardAnyPending
//	RemoveDirectory            guardPendingAddition, then guardAnyPending
//	RemovePeer                 guardPendingAddition, then guardAnyPending
//	BeginDirectoryRemoval      guardPendingAddition, then guardAnyPending
//	BeginPeerRemoval           guardPendingAddition, then guardAnyPending
//	BeginDirectoryAddition     guardPendingRemoval, then guardPendingAddition
//
// Where two guards are listed, the operation first resumes its own pending
// request of the same peer or path, which the first guard admits, and runs
// the second guard only before a new request.
type mirrorOperations struct {
	peerRemoval       *PeerRemoval
	directoryRemoval  *DirectoryRemoval
	directoryAddition *DirectoryAddition
	peerGeneration    uint64
}

// guardAnyPending refuses while an explicit addition, peer removal or
// directory removal is pending.
func (ops *mirrorOperations) guardAnyPending() error {
	if err := ops.guardPendingAddition(); err != nil {
		return err
	}
	return ops.guardPendingRemoval()
}

// guardPendingRemoval refuses while an explicit peer removal or directory
// removal has neither completed nor been acknowledged.
func (ops *mirrorOperations) guardPendingRemoval() error {
	if ops.peerRemoval != nil && !ops.peerRemoval.terminal() {
		return cephFSObserveGuard("explicit CephFS peer drain is incomplete")
	}
	if ops.directoryRemoval != nil && !ops.directoryRemoval.terminal() {
		return cephFSObserveGuard("explicit CephFS directory release is incomplete")
	}
	return nil
}

// guardPendingAddition refuses while an explicit directory addition has not
// registered its path.
func (ops *mirrorOperations) guardPendingAddition() error {
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
