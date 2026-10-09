package multicluster

import (
	"context"
	"time"
)

// CephFSMirrorProcessQuiescenceAcknowledgment is the result of one explicit
// acceptance attempt. Acknowledged is true only when this invocation freshly
// proved every original container removed and committed the separate terminal
// decision. Observation is never cached evidence. A failed retry does not undo
// an earlier private decision, but returns Acknowledged=false for that attempt.
type CephFSMirrorProcessQuiescenceAcknowledgment struct {
	Acknowledged bool
	Observation  CephFSMirrorProcessQuiescenceStatus
}

// AcknowledgeProcessQuiescence accepts interrupted removal only after callers
// explicitly RemoveDaemon every owned original member. It requires fresh bound
// full-CID absence and native original watcher retirement, and opens continued
// fixture-use gates without claiming native Drained or replaying peer deletion.
func (r *CephFSMirrorPeerRemoval) AcknowledgeProcessQuiescence(ctx context.Context) (CephFSMirrorProcessQuiescenceAcknowledgment, error) {
	if r == nil {
		return CephFSMirrorProcessQuiescenceAcknowledgment{}, cephFSObserveGuard("original peer acknowledgment receipt is unavailable")
	}
	return acknowledgeCephFSProcessQuiescence(ctx, r, "", r.checkHandle, func(ctx context.Context) (bool, error) {
		present, fsPresent, err := r.policy(ctx)
		return !present && !fsPresent && err == nil, err
	}, func() bool { return r.completed }, func() {
		r.mirror.peerID = ""
		r.processQuiescenceAcknowledged = true
	})
}

// AcknowledgeProcessQuiescence accepts the exact interrupted path removal after
// explicit owned-daemon removal and fresh original process proof. It reconciles
// only that path's desired policy and never claims native Released.
func (r *CephFSMirrorDirectoryRemoval) AcknowledgeProcessQuiescence(ctx context.Context) (CephFSMirrorProcessQuiescenceAcknowledgment, error) {
	if r == nil || r.original == nil {
		return CephFSMirrorProcessQuiescenceAcknowledgment{}, cephFSObserveGuard("original directory acknowledgment receipt is unavailable")
	}
	return acknowledgeCephFSProcessQuiescence(ctx, r.original, r.directory, func() error { return r.checkDirectoryRemovalHandle(true) }, func(ctx context.Context) (bool, error) {
		if err := r.peerPolicy(ctx); err != nil {
			return false, err
		}
		present, err := r.policy(ctx)
		return !present && err == nil, err
	}, func() bool { return r.completed }, func() {
		r.forgetDesiredPolicy()
		r.processQuiescenceAcknowledged = true
	})
}

func acknowledgeCephFSProcessQuiescence(ctx context.Context, o *CephFSMirrorPeerRemoval, directory string, owner func() error, authority func(context.Context) (bool, error), completed func() bool, commit func()) (CephFSMirrorProcessQuiescenceAcknowledgment, error) {
	result := CephFSMirrorProcessQuiescenceAcknowledgment{Observation: initialCephFSProcessQuiescenceStatus(o, directory)}
	if o == nil || o.mirror == nil || owner == nil || authority == nil || completed == nil || commit == nil {
		return result, cephFSObserveGuard("original process acknowledgment owner is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &o.mirror.mu); err != nil {
		return result, err
	}
	defer o.mirror.mu.Unlock()
	if err := owner(); err != nil {
		return result, err
	}
	if completed() {
		return result, cephFSObserveGuard("original removal already has native graceful completion")
	}
	if err := confirmCephFSAcknowledgmentRetiredInventory(ctx, o); err != nil {
		return result, err
	}
	observation, err := observeCephFSReceiptProcessQuiescenceLocked(ctx, o, directory, owner, authority)
	result.Observation = observation
	if err != nil {
		return result, err
	}
	if !observation.PolicyRemoved || !observation.OriginalQuiescent || len(observation.Daemons) != len(o.cohort) {
		return result, cephFSObserveQuery("original process acknowledgment proof is incomplete", nil)
	}
	for _, entry := range observation.Daemons {
		if !entry.OriginalQuiescent || entry.Evidence != "container-removed" {
			return result, cephFSObserveQuery("original process acknowledgment requires removed original containers", nil)
		}
	}
	if err := owner(); err != nil {
		return result, err
	}
	if err := confirmCephFSAcknowledgmentRetiredInventory(ctx, o); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// No native mutation or unlock occurs between fresh proof and local decision.
	commit()
	result.Acknowledged = true
	return result, nil
}

func confirmCephFSAcknowledgmentRetiredInventory(ctx context.Context, o *CephFSMirrorPeerRemoval) error {
	if len(o.mirror.daemons) != 0 {
		return cephFSObserveQuery("original process acknowledgment requires explicit owned daemon removal", nil)
	}
	for _, w := range o.cohort {
		if w.daemon == nil {
			return cephFSObserveGuard("original process acknowledgment daemon identity is unavailable")
		}
		if err := lockRGWSyncObservation(ctx, &w.daemon.mu); err != nil {
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
func (r *CephFSMirrorPeerRemoval) terminal() bool {
	return r != nil && (r.completed || r.processQuiescenceAcknowledged)
}

func (r *CephFSMirrorDirectoryRemoval) terminal() bool {
	return r != nil && (r.completed || r.processQuiescenceAcknowledged)
}

// Called under mirror.mu for a same-generation accepted receipt. Re-attest
// original native authority, without process adoption, cleanup or delete replay.
func (r *CephFSMirrorPeerRemoval) confirmAcknowledgedProcessPolicy(ctx context.Context) error {
	if err := r.checkHandle(); err != nil {
		return err
	}
	if !r.processQuiescenceAcknowledged {
		return cephFSObserveGuard("original peer process acknowledgment is unavailable")
	}
	present, fsPresent, err := r.policy(ctx)
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

func (r *CephFSMirrorDirectoryRemoval) confirmAcknowledgedProcessPolicy(ctx context.Context) error {
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
