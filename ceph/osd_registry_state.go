package ceph

import (
	"iter"
	"slices"
)

// osdRegistry is the handle registry of one cluster: the OSD descriptors it
// owns, keyed by OSD ID, and the detached OSDs that hold a retained store and
// registration without a container until DetachedOSD.Reopen or Terminate.
//
// The type has no mutex. Container.mu guards both maps: every method of
// the type is called with it held (the caller table in
// osd_registry_state_test.go pins each caller and how it holds the lock), apart
// from the single-threaded constructors, which run before the cluster is
// shared: Run's composition planner and the unit-test cluster builder. Because
// the lock is Container.mu, the registry sits under the topology lock and
// before the bootstrap-record lock, the control-plane lock and the leaf
// locks, and a method never takes a lock or
// calls into Container.
//
// park and unpark are the only methods that touch both maps. A handle is in
// exactly one of them (or in neither after a purge), so each pair is a single
// method and the invariant cannot be broken by a caller that forgets half of
// it. The zero value is an empty registry.
type osdRegistry struct {
	// owned is the descriptors the cluster owns, including one whose container
	// was not created yet (a registration that Terminate must still own) and
	// one that a purge marked.
	owned map[int]*OSDContainer
	// detached is the retained stores and registrations without a container.
	detached map[int]*DetachedOSD
}

// get returns the owned descriptor of id, or nil.
func (r *osdRegistry) get(id int) *OSDContainer { return r.owned[id] }

// count is the number of owned descriptors.
func (r *osdRegistry) count() int { return len(r.owned) }

// each iterates the owned descriptors in map order. The loop body may call
// forget on the key it is visiting, as Terminate does.
func (r *osdRegistry) each() iter.Seq2[int, *OSDContainer] {
	return func(yield func(int, *OSDContainer) bool) {
		for id, osd := range r.owned {
			if !yield(id, osd) {
				return
			}
		}
	}
}

// sorted lists the owned descriptors by ID.
func (r *osdRegistry) sorted() []*OSDContainer {
	result := make([]*OSDContainer, 0, len(r.owned))
	for _, osd := range r.owned {
		result = append(result, osd)
	}
	slices.SortFunc(result, func(a, b *OSDContainer) int { return a.ID - b.ID })
	return result
}

// register records osd as the owned descriptor of id.
func (r *osdRegistry) register(id int, osd *OSDContainer) {
	if r.owned == nil {
		r.owned = make(map[int]*OSDContainer)
	}
	r.owned[id] = osd
}

// forget drops the owned descriptor of id.
func (r *osdRegistry) forget(id int) { delete(r.owned, id) }

// detachedOf returns the detached OSD of id, or nil.
func (r *osdRegistry) detachedOf(id int) *DetachedOSD { return r.detached[id] }

// park records detached under id and drops the owned descriptor of id in one
// step: the detach transition.
func (r *osdRegistry) park(id int, detached *DetachedOSD) {
	if r.detached == nil {
		r.detached = make(map[int]*DetachedOSD)
	}
	r.detached[id] = detached
	delete(r.owned, id)
}

// unpark drops the detached OSD of id and records osd as the owned descriptor
// of id in one step: the reopen transition.
func (r *osdRegistry) unpark(id int, osd *OSDContainer) {
	delete(r.detached, id)
	r.register(id, osd)
}

// replace swaps the owned descriptors for owned. Only the unit-test hook
// unitAccess.UnitReplaceOSDs calls it, on a cluster no one else uses yet.
func (r *osdRegistry) replace(owned map[int]*OSDContainer) { r.owned = owned }
