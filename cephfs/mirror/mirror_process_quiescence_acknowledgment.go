package mirror

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

// ProcessQuiescenceAcknowledgment is the result of one explicit
// acceptance attempt. Acknowledged is true only when this invocation freshly
// proved every original container removed and committed the separate terminal
// decision. Observation is never cached evidence. A failed retry does not undo
// an earlier private decision, but returns Acknowledged=false for that attempt.
type ProcessQuiescenceAcknowledgment struct {
	Acknowledged bool
	Observation  ProcessQuiescenceStatus
}

// AcknowledgeProcessQuiescence accepts interrupted removal only after callers
// explicitly Remove every owned original daemon. It requires fresh bound
// full-CID absence and native original watcher retirement, and opens continued
// fixture-use gates without claiming native Drained or replaying peer deletion.
//
//purpose:condition
func (r *PeerRemoval) AcknowledgeProcessQuiescence(ctx context.Context) (ProcessQuiescenceAcknowledgment, error) {
	if r == nil || r.scope == nil {
		return ProcessQuiescenceAcknowledgment{}, cephFSObserveGuard("original peer acknowledgment receipt is unavailable")
	}
	return acknowledgeCephFSProcessQuiescence(ctx, r)
}

// AcknowledgeProcessQuiescence accepts the exact interrupted path removal after
// explicit owned-daemon removal and fresh original process proof. It reconciles
// only that path's desired policy and never claims native Released.
//
//purpose:condition
func (r *DirectoryRemoval) AcknowledgeProcessQuiescence(ctx context.Context) (ProcessQuiescenceAcknowledgment, error) {
	if r == nil || r.scope == nil {
		return ProcessQuiescenceAcknowledgment{}, cephFSObserveGuard("original directory acknowledgment receipt is unavailable")
	}
	return acknowledgeCephFSProcessQuiescence(ctx, r)
}

func acknowledgeCephFSProcessQuiescence(ctx context.Context, receipt cephFSQuiescenceReceipt) (ProcessQuiescenceAcknowledgment, error) {
	c := receipt.quiescenceCohort()
	result := ProcessQuiescenceAcknowledgment{Observation: initialCephFSProcessQuiescenceStatus(c, receipt.quiescenceDirectory())}
	if c.scope == nil || c.scope.mirror == nil {
		return result, cephFSObserveGuard("original process acknowledgment owner is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &c.scope.mirror.mu); err != nil {
		return result, err
	}
	defer c.scope.mirror.mu.Unlock()
	if err := receipt.quiescenceOwner(); err != nil {
		return result, err
	}
	if receipt.nativelyCompleted() {
		return result, cephFSObserveGuard("original removal already has native graceful completion")
	}
	if err := confirmCephFSAcknowledgmentRetiredInventory(ctx, c); err != nil {
		return result, err
	}
	observation, err := observeCephFSReceiptProcessQuiescenceLocked(ctx, receipt)
	result.Observation = observation
	if err != nil {
		return result, err
	}
	if !observation.PolicyRemoved || !observation.OriginalQuiescent || len(observation.Daemons) != len(c.witnesses) {
		return result, cephFSObserveQuery("original process acknowledgment proof is incomplete", nil)
	}
	for _, entry := range observation.Daemons {
		if !entry.OriginalQuiescent || entry.Evidence != "container-removed" {
			return result, cephFSObserveQuery("original process acknowledgment requires removed original containers", nil)
		}
	}
	if err := receipt.quiescenceOwner(); err != nil {
		return result, err
	}
	if err := confirmCephFSAcknowledgmentRetiredInventory(ctx, c); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// No native mutation or unlock occurs between fresh proof and local decision.
	receipt.acknowledge()
	result.Acknowledged = true
	return result, nil
}

func confirmCephFSAcknowledgmentRetiredInventory(ctx context.Context, c *cephFSRemovalCohort) error {
	if len(c.scope.mirror.roster.daemons) != 0 {
		return cephFSObserveQuery("original process acknowledgment requires explicit owned daemon removal", nil)
	}
	for _, w := range c.witnesses {
		if w.daemon == nil {
			return cephFSObserveGuard("original process acknowledgment daemon identity is unavailable")
		}
		if err := clusterutil.LockMutex(ctx, &w.daemon.mu); err != nil {
			return err
		}
		removed := w.daemon.removed
		w.daemon.mu.Unlock()
		if !removed {
			return cephFSObserveGuard("original process acknowledgment daemon retirement is unconfirmed")
		}
	}
	return ctx.Err()
}

// terminal belongs only to overlap bookkeeping. completed continues to mean
// native graceful teardown and is never derived from process acknowledgment.
func (r *PeerRemoval) terminal() bool {
	return r != nil && (r.completed || r.processQuiescenceAcknowledged)
}

func (r *DirectoryRemoval) terminal() bool {
	return r != nil && (r.completed || r.processQuiescenceAcknowledged)
}

// Called under mirror.mu for a same-generation accepted receipt. Re-attest
// original native authority, without process adoption, cleanup or delete replay.
func (r *PeerRemoval) confirmAcknowledgedProcessPolicy(ctx context.Context) error {
	if err := r.checkHandle(); err != nil {
		return err
	}
	if !r.processQuiescenceAcknowledged {
		return cephFSObserveGuard("original peer process acknowledgment is unavailable")
	}
	present, fsPresent, err := r.scope.policy(ctx)
	if err != nil {
		return err
	}
	if present || fsPresent {
		return cephFSObserveGuard("acknowledged original peer policy reappeared")
	}
	if err := r.checkHandle(); err != nil {
		return err
	}
	return ctx.Err()
}

func (r *DirectoryRemoval) confirmAcknowledgedProcessPolicy(ctx context.Context) error {
	if err := r.checkDirectoryRemovalHandle(true); err != nil {
		return err
	}
	if !r.processQuiescenceAcknowledged {
		return cephFSObserveGuard("original directory process acknowledgment is unavailable")
	}
	if err := r.peerPolicy(ctx); err != nil {
		return err
	}
	present, err := r.policy(ctx)
	if err != nil {
		return err
	}
	if present {
		return cephFSObserveGuard("acknowledged original directory policy reappeared")
	}
	if err := r.checkDirectoryRemovalHandle(true); err != nil {
		return err
	}
	return ctx.Err()
}
