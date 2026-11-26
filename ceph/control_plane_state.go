package ceph

import (
	"context"
	"fmt"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

// controlPlaneState is the separate control container of one cluster and the
// read-write lock that guards its handle: the independent CLI container that
// keeps topology operations available while the first MON is stopped. It is
// the control-plane lock domain of the membership and control group.
//
// The lock is a leaf
// below Container.mu: ensureControlPlane and
// terminate take it while the topology lock is held, and the readers
// take it either after releasing the topology lock (evidenceInventory,
// admitNativeTarget) or may be called with it held
// (cephConn.cliContainer and cephConn.controlContainer). No method of the type takes another lock or calls
// into Container, so a reader or writer that holds this lock never waits for
// Container.mu: the only wait inside it is the Docker call that terminate makes
// for the handle itself, as before.
//
// The ready handle is used for commands. A handle returned with a start
// error is retained separately until the retry or cluster termination can
// remove it. The topology lock serializes ensure and terminate.
type controlPlaneState struct {
	mu      sync.RWMutex
	handle  testcontainers.Container
	pending testcontainers.Container
}

// current returns the control container, or nil without one. It takes the
// read lock.
func (s *controlPlaneState) current() testcontainers.Container {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.handle
}

// currentContext is current that gives up when ctx ends while waiting for the
// lock.
func (s *controlPlaneState) currentContext(ctx context.Context) (testcontainers.Container, error) {
	if err := lockTopologyReadMutex(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	return s.handle, nil
}

// owns reports whether id is the container ID of the control container,
// waiting for the lock no longer than ctx allows.
func (s *controlPlaneState) owns(ctx context.Context, id string) (bool, error) {
	ctr, err := s.currentContext(ctx)
	if err != nil {
		return false, err
	}
	return ctr != nil && ctr.GetContainerID() == id, nil
}

// diagnosticHandles records the CLI and any incomplete startup separately
// without ever blocking past ctx.
func (s *controlPlaneState) diagnosticHandles(ctx context.Context) (ready, pending testcontainers.Container, err error) {
	if err := diagnosticLock(ctx, s.mu.TryRLock); err != nil {
		return nil, nil, err
	}
	defer s.mu.RUnlock()
	return s.handle, s.pending, nil
}

// ensure is called while the topology holds c.mu. Retry removal of any failed
// start before trying to create another helper.
func (s *controlPlaneState) ensure(ctx context.Context, start func() (testcontainers.Container, error)) error {
	if s.current() != nil {
		return nil
	}
	if err := s.terminate(ctx, testcontainers.StopTimeout(terminationStopTimeout)); err != nil {
		return fmt.Errorf("remove incomplete control container: %w", err)
	}
	ctr, err := start()
	s.publish(ctr, err)
	return err
}

// publish stores a ready handle or retains an incomplete one for removal.
func (s *controlPlaneState) publish(ctr testcontainers.Container, startErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if startErr != nil {
		s.pending = ctr
		return
	}
	s.handle = ctr
}

// terminate removes the control container and forgets it when the removal
// succeeded or the container was already gone. It holds the write lock for the
// removal, as terminate always did, and returns the error that is not a
// missing host resource.
func (s *controlPlaneState) terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctr := s.handle
	if ctr == nil {
		ctr = s.pending
	}
	if ctr == nil {
		return nil
	}
	if err := ctr.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
		return err
	}
	s.handle = nil
	s.pending = nil
	return nil
}

// mutex is the lock identity that UnitState publishes for the lock queueing
// tests of the service packages. Nothing else calls it.
func (s *controlPlaneState) mutex() *sync.RWMutex { return &s.mu }
