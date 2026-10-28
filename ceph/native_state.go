package ceph

import (
	"context"
	"maps"
	"slices"
	"sync"
)

// nativeAdmission is the result of nativeState.admit.
type nativeAdmission int

const (
	// nativeAdmitted: the invocation is registered.
	nativeAdmitted nativeAdmission = iota
	// nativeRefusedClosed: Terminate has closed native admission.
	nativeRefusedClosed
	// nativeRefusedFenced: the target instance is fenced for OSD removal.
	nativeRefusedFenced
	// nativeRefusedClaim: the claim hook refused the invocation.
	nativeRefusedClaim
)

// nativeState is the native-command state of one cluster: the invocations that
// run or whose command is not confirmed as stopped, whether Terminate refuses
// new ones, the OSD instances fenced against new ones, and the cancel function
// of a client-image startup. It is the second component of the internal state
// split, recorded in docs/design/container-decomposition/native-command-state.md.
//
// mu is the former Container.nativeMu, moved with the fields it always
// guarded, so the split adds no lock. It is a leaf lock: no method here calls
// out or takes another lock while holding it, and every caller invokes
// cancellations and stops after the method returned. Terminate therefore
// calls cancelNativeInvocations before it waits for Container.mu, and
// featureClientState.mu (the former clientMu) is held around the short
// critical sections of the client startup (ceph/images.go), never the other
// way round.
//
// Every field is read and written only by the methods of this type, which take
// mu and call nothing else while they hold it. The guards in
// native_state_test.go enforce that, and that the split adds no mutex.
type nativeState struct {
	mu          sync.Mutex
	invocations map[*nativeInvocation]bool
	closed      bool
	// fenced permanently refuses commands for an OSD instance once removal
	// starts; a failed removal retains its fence until cleanup is retried.
	fenced map[string]bool
	// clientStartupCancel cancels a running client-image startup.
	clientStartupCancel context.CancelFunc
}

// admit registers inv unless native admission is closed, its target instance is
// fenced, or claim (when not nil) refuses it. Checks run in that order, under
// one critical section, and claim must not block or take a lock.
func (n *nativeState) admit(inv *nativeInvocation, claim func() bool) nativeAdmission {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch {
	case n.closed:
		return nativeRefusedClosed
	case n.fenced[inv.targetID]:
		return nativeRefusedFenced
	case claim != nil && !claim():
		return nativeRefusedClaim
	}
	if n.invocations == nil {
		n.invocations = make(map[*nativeInvocation]bool)
	}
	n.invocations[inv] = true
	return nativeAdmitted
}

// finish ends the registration of inv after its call returned. When the end is
// unconfirmed, inv stays registered with marker, so a later stop can retry it.
func (n *nativeState) finish(inv *nativeInvocation, marker string, unconfirmed bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if unconfirmed {
		inv.marker = marker
	} else {
		delete(n.invocations, inv)
	}
}

// forget drops the registration of inv once its command is confirmed stopped.
func (n *nativeState) forget(inv *nativeInvocation) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.invocations, inv)
}

// registered returns the number of tracked invocations.
func (n *nativeState) registered() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.invocations)
}

// isClosed reports whether Terminate has closed native admission.
func (n *nativeState) isClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// close closes admission for good and returns every tracked invocation and the
// cancel function of a running client startup (nil when none runs). The
// caller cancels them after the lock is released.
func (n *nativeState) close() ([]*nativeInvocation, context.CancelFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	return slices.Collect(maps.Keys(n.invocations)), n.clientStartupCancel
}

// fence refuses new commands for the instance id and returns the tracked
// invocations that target it. It never closes admission for other targets.
func (n *nativeState) fence(id string) []*nativeInvocation {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fenced == nil {
		n.fenced = make(map[string]bool)
	}
	n.fenced[id] = true
	var pending []*nativeInvocation
	for inv := range n.invocations {
		if inv.targetID == id {
			pending = append(pending, inv)
		}
	}
	return pending
}

// beginClientStartup records cancel as the cancel function of the client
// startup, unless admission is closed.
func (n *nativeState) beginClientStartup(cancel context.CancelFunc) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return false
	}
	n.clientStartupCancel = cancel
	return true
}

// endClientStartup clears the cancel function of the client startup.
func (n *nativeState) endClientStartup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clientStartupCancel = nil
}
