package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// Exclusive lends t the whole shared cluster and ends the lease in
// t.Cleanup. It waits until no tenant holds a lease, and new leases wait
// while it waits and while it holds the cluster, so a stream of leases
// cannot starve it. Leases that asked before it still go first.
//
// The returned *Container allows every method except Terminate, which only
// Close does. The cluster is checked against its baseline before it is lent,
// like a lease's admission. The release does not try to restore it: a raw
// admin path, a service package's functions or a daemon fault can change
// state that no comparison with the baseline sees, such as MGR modules,
// network shaping, daemon keys or data. The release therefore retires the
// cluster and terminates it, and the next Lease or Exclusive starts a fresh
// one. These restarts do not count against WithMaxRecycles.
//
// A test that holds a tenant lease, or whose parent test does, cannot take
// the exclusive lease, which would wait for that lease forever: Exclusive
// fails it at once. So does a second Exclusive of the same test. Exclusive
// fails t when the cluster is closed, cannot start, or t's context ends
// while it waits.
func (s *SharedCluster) Exclusive(t testing.TB) *Container {
	t.Helper()
	inst, err := s.exclusiveLease(t.Context(), t.Name(), t.Logf)
	if err != nil {
		t.Fatalf("ceph: exclusive lease of shared cluster: %v", err)
		return nil
	}
	t.Cleanup(func() { s.releaseExclusive(inst, t.Logf) })
	ops := inst.backend.ops()
	if ops == nil {
		return nil
	}
	return ops.raw()
}

func (s *SharedCluster) exclusiveLease(ctx context.Context, holder string, logf func(string, ...any)) (*sharedInstance, error) {
	if s.settings.err != nil {
		return nil, classify(ErrInvalidRequest, s.settings.err)
	}
	waitStart := time.Now()
	if err := s.acquire(ctx, true, holder); err != nil {
		return nil, err
	}
	granted := false
	defer func() {
		s.mu.Lock()
		s.stats.ExclusiveWait += time.Since(waitStart)
		s.mu.Unlock()
		if !granted {
			s.endExclusive()
		}
	}()
	for {
		inst, err := s.instance(ctx, logf)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		usable := !s.closed && inst.poisoned == "" && !inst.retired && s.current == inst
		s.mu.Unlock()
		if !usable {
			continue
		}
		if err := s.admit(ctx, inst); err != nil {
			if ctx.Err() != nil {
				return nil, errors.Join(err, ctx.Err())
			}
			// A cluster that drifted from its baseline is replaced as for a
			// lease; the test gets the fresh one.
			s.poison(inst, "admission: "+err.Error())
			s.mu.Lock()
			drained := len(inst.live) == 0 && !inst.retired
			s.mu.Unlock()
			if drained {
				_ = s.terminateDetached(inst)
			}
			logf("ceph: shared cluster poisoned before an exclusive lease: %v", err)
			continue
		}
		s.mu.Lock()
		// Close may have retired inst during the admission.
		usable = !s.closed && inst.poisoned == "" && !inst.retired && s.current == inst
		if usable {
			// No later lease may find inst: the release retires it.
			inst.poisoned = "lent to an exclusive lease"
			s.current = nil
			s.stats.Exclusives++
		}
		s.mu.Unlock()
		if !usable {
			continue
		}
		inst.backend.setSharing(sharedExclusively)
		granted = true
		return inst, nil
	}
}

// releaseExclusive retires the cluster of an exclusive lease and then lets
// the waiting leases in. A cluster that cannot be terminated stays owned by
// s, so Close tries again and reports it.
func (s *SharedCluster) releaseExclusive(inst *sharedInstance, logf func(string, ...any)) {
	defer s.endExclusive()
	if err := s.terminateDetached(inst); err != nil {
		logf("ceph: the cluster of an exclusive lease could not be terminated; Close tries again: %v", err)
	}
}

// terminateDetached terminates inst with the release timeout, after the
// test's context ended.
func (s *SharedCluster) terminateDetached(inst *sharedInstance) error {
	ctx, cancel := context.WithTimeout(context.Background(), sharedReleaseTimeout)
	defer cancel()
	return s.terminate(ctx, inst)
}

// sharedWaiter is a Lease or an Exclusive waiting for admission.
type sharedWaiter struct {
	exclusive bool
	holder    string
}

// acquire waits until s admits a lease, or an exclusive lease, of holder: a
// tenant slot, or the cluster without tenants. Waiters are admitted in
// arrival order across both kinds, except that a lease of a test that
// already holds one does not wait behind an Exclusive, which waits for that
// test's leases. A request that would wait for its own test fails at once.
func (s *SharedCluster) acquire(ctx context.Context, exclusive bool, holder string) error {
	s.mu.Lock()
	if err := s.selfWaitLocked(exclusive, holder); err != nil {
		s.mu.Unlock()
		return err
	}
	waiter := &sharedWaiter{exclusive: exclusive, holder: holder}
	s.waiters = append(s.waiters, waiter)
	for {
		if s.closed {
			s.dropWaiterLocked(waiter)
			s.mu.Unlock()
			return invalidRequest("shared cluster is closed")
		}
		if s.admissibleLocked(waiter) {
			s.dropWaiterLocked(waiter)
			if exclusive {
				s.exclusiveHeld, s.exclusive = true, holder
			} else {
				s.slots++
				s.holds[holder]++
			}
			s.mu.Unlock()
			return nil
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			// A canceled Exclusive no longer holds back the leases behind it.
			s.mu.Lock()
			s.dropWaiterLocked(waiter)
			s.mu.Unlock()
			return ctx.Err()
		}
		s.mu.Lock()
	}
}

func (s *SharedCluster) admissibleLocked(waiter *sharedWaiter) bool {
	if s.exclusiveHeld {
		return false
	}
	if waiter.exclusive {
		return s.slots == 0 && s.waiters[0] == waiter
	}
	if s.slots >= s.settings.maxTenants {
		return false
	}
	// A queued Exclusive waits for every tenant lease, so it cannot go
	// before a lease that its waiting releases depend on: one of a test
	// that holds a lease, or of a parent whose paused parallel subtest
	// holds one and releases it only after the parent returns.
	if s.leaseBlocksExclusiveLocked(waiter.holder) {
		return true
	}
	for _, ahead := range s.waiters {
		if ahead == waiter {
			break
		}
		if ahead.exclusive {
			return false
		}
	}
	return true
}

// selfWaitLocked fails a request that its own test would block forever.
func (s *SharedCluster) selfWaitLocked(exclusive bool, holder string) error {
	kind := "lease"
	if exclusive {
		kind = "exclusive lease"
	}
	if s.exclusiveHeld && related(holder, s.exclusive) {
		return invalidRequest("test %s holds the exclusive lease of this shared cluster, so a %s of %s would wait for itself", s.exclusive, kind, holder)
	}
	// A subtest that still holds a lease while its parent runs has called
	// t.Parallel: it is paused until the parent returns, so the parent
	// would wait for a release that waits for the parent.
	if s.exclusiveHeld && subtestOf(s.exclusive, holder) {
		return invalidRequest("test %s, a parallel subtest of %s, holds the exclusive lease of this shared cluster and releases it only after %s returns, so a %s of %s would wait forever", s.exclusive, holder, holder, kind, holder)
	}
	if exclusive {
		for name, count := range s.holds {
			if count > 0 && related(holder, name) {
				return invalidRequest("test %s holds a tenant lease of this shared cluster, which an exclusive lease of %s would wait for forever; take the exclusive lease in a test without tenant leases", name, holder)
			}
			if count > 0 && subtestOf(name, holder) {
				return invalidRequest("test %s, a parallel subtest of %s, holds a tenant lease of this shared cluster and releases it only after %s returns, so an exclusive lease of %s would wait forever", name, holder, holder, holder)
			}
		}
		for _, waiting := range s.waiters {
			if waiting.exclusive && related(holder, waiting.holder) {
				return invalidRequest("test %s already waits for an exclusive lease of this shared cluster", waiting.holder)
			}
		}
	}
	return nil
}

// leaseBlocksExclusiveLocked reports whether a tenant lease that a queued
// Exclusive waits for is released only after a lease of holder returns:
// holder's own test or one of its ancestors holds a lease, or one of its
// subtests does, which is then a paused parallel subtest.
func (s *SharedCluster) leaseBlocksExclusiveLocked(holder string) bool {
	for name, count := range s.holds {
		if count > 0 && (related(holder, name) || subtestOf(name, holder)) {
			return true
		}
	}
	return false
}

// related reports whether test caller is test holder or one of its
// subtests, which the holder's cleanup waits for.
func related(caller, holder string) bool {
	return holder != "" && (caller == holder || strings.HasPrefix(caller, holder+"/"))
}

// subtestOf reports whether test name is a subtest, at any depth, of test
// parent.
func subtestOf(name, parent string) bool {
	return strings.HasPrefix(name, parent+"/")
}

func (s *SharedCluster) dropWaiterLocked(waiter *sharedWaiter) {
	if i := slices.Index(s.waiters, waiter); i >= 0 {
		s.waiters = slices.Delete(s.waiters, i, i+1)
	}
	s.signalLocked()
}

// releaseSlot returns a tenant slot of holder.
func (s *SharedCluster) releaseSlot(holder string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slots--
	if s.holds[holder]--; s.holds[holder] <= 0 {
		delete(s.holds, holder)
	}
	s.signalLocked()
}

// endExclusive ends the exclusive lease and lets the waiters in.
func (s *SharedCluster) endExclusive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exclusiveHeld, s.exclusive = false, ""
	s.signalLocked()
}

// signalLocked wakes every waiter to check its admission again.
func (s *SharedCluster) signalLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}
