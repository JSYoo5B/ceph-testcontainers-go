package ceph

import (
	"maps"
	"slices"
	"sync"
)

// extensionStore is the per-cluster state that service packages keep under a
// kind string (the CephFS filesystem registry, the RGW gateway registry), and
// the mutex that guards the map. It is a component of the internal state split
// recorded in docs/design/container-decomposition/extension-store.md, the
// extensions lock domain of the membership and control group.
//
// The lock is the former Container.extensionsMu, moved with the data it guards;
// no mutex was added and no lock order changed. mu is a leaf: no method of the
// type takes another lock or calls into Container, so a holder of mu never
// waits for Container.mu or any other lock. The one callback it runs is the
// create function of getOrCreate, which runs with mu held exactly as it did
// when the method body lived in serviceHooks.Extension; the two callbacks in
// the module (cephfs.filesystems and rgw.gatewaysFrom) only allocate a registry.
// Callers may or may not hold Container.mu, which is why the lock is
// separate: the order, where both are held, is Container.mu then mu.
//
// The stored states are handed to callers as they are. The service packages
// guard their own contents with Container.mu, as their docs say; the store
// guards only the map. inKindOrder returns the states in kind order and
// releases mu before the caller looks at any of them, so a state method such as
// Secrets or ConfigTargets never runs under mu. The guards in
// extension_store_test.go enforce that the map and the lock are used only by
// this type, that the methods call only their pinned collaborators, and that
// the split adds no mutex.
type extensionStore struct {
	mu     sync.Mutex
	states map[string]any
}

// getOrCreate returns the state kept under kind, storing the result of create
// on first use. create runs with mu held; it must not call back into the store.
func (s *extensionStore) getOrCreate(kind string, create func() any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.states[kind]; ok {
		return state
	}
	if s.states == nil {
		s.states = make(map[string]any)
	}
	state := create()
	s.states[kind] = state
	return state
}

// inKindOrder returns the stored states ordered by kind, as a snapshot taken
// under mu.
func (s *extensionStore) inKindOrder() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	kinds := slices.Sorted(maps.Keys(s.states))
	states := make([]any, 0, len(kinds))
	for _, kind := range kinds {
		states = append(states, s.states[kind])
	}
	return states
}
