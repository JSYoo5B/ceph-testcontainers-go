package ceph

import (
	"context"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

// controlPlaneState is the separate control container of one cluster and the
// read-write lock that guards its handle: the independent CLI container that
// keeps topology operations available while the first MON is stopped. It is
// a component of the internal state split, recorded in
// docs/design/container-decomposition/control-plane-state.md, the control-plane
// lock domain of the membership and control group.
//
// The lock is the former Container.controlMu, moved with the handle it
// guards; no mutex was added and no lock order changed. controlMu is a leaf
// below Container.mu: ensureControlPlane and
// terminate take it while the topology lock is held, and the readers
// take it either after releasing the topology lock (diagnosticsSnapshot,
// admitNativeTarget) or may be called with it held
// (cliContainer and controlContainerContext). No method of the type takes another lock or calls
// into Container, so a reader or writer that holds this lock never waits for
// Container.mu: the only wait inside it is the Docker call that terminate makes
// for the handle itself, as before.
//
// The handle is nil until ensureControlPlane publishes it and after a
// successful removal in terminate. The guards in control_plane_state_test.go
// enforce that the field and the lock are used only by this type and the one
// published lock identity, that the handle has the single production writers
// publish and terminate, that the methods call only their pinned
// collaborators, and that the split adds no mutex.
type controlPlaneState struct {
	mu     sync.RWMutex
	handle testcontainers.Container
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

// diagnosticHandle returns the control container without ever blocking past
// ctx, polling the read lock the way the diagnostics snapshot does.
func (s *controlPlaneState) diagnosticHandle(ctx context.Context) (testcontainers.Container, error) {
	if err := diagnosticLock(ctx, s.mu.TryRLock); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	return s.handle, nil
}

// publish stores the control container under the write lock.
func (s *controlPlaneState) publish(ctr testcontainers.Container) {
	s.mu.Lock()
	s.handle = ctr
	s.mu.Unlock()
}

// terminate removes the control container and forgets it when the removal
// succeeded or the container was already gone. It holds the write lock for the
// removal, as terminate always did, and returns the error that is not a
// missing host resource.
func (s *controlPlaneState) terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return nil
	}
	if err := s.handle.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
		return err
	}
	s.handle = nil
	return nil
}

// mutex is the lock identity that UnitState publishes for the lock queueing
// tests of the service packages. Nothing else calls it.
func (s *controlPlaneState) mutex() *sync.RWMutex { return &s.mu }
