package ceph

import (
	"iter"
	"maps"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// memberRoster is the roster of the daemons that a cluster owns beside its
// first MON and its OSDs: the added MONs, the MGR candidates, the first MGR's
// container and the optional services. It is a component of the internal
// state split, recorded in
// docs/design/container-decomposition/member-roster.md.
//
// The type has no mutex. Container.mu guards every field, as it guarded
// Container.manager, monitors, managers and services before the move: each
// method is called with it held (the caller table in member_roster_test.go
// pins every caller and how it holds the lock), apart from the
// single-threaded constructors that run before the cluster is shared. Because
// the lock is Container.mu, the roster sits at the position of the fields it
// replaced: after the topology gate, before
// configMu, controlMu and the leaf locks, and a method never takes a lock or
// calls into Container.
//
// What stays on Container, with the reason, is in the fourteenth slice of the
// design note: the first MON (the embedded container, which the roster does
// not own), monitorTerminated (it describes that embedded container), closed
// (read by the terminate gate) and mu.
//
// The first MGR's container (firstMgr) mirrors the entry "a" of managers.
// dropManager is therefore the only way to forget a manager entry: it clears
// the mirror together with the entry "a", so the two cannot disagree after a
// removal. The zero value is an empty roster.
type memberRoster struct {
	// firstMgr is the container of the initial MGR a, nil before it starts and
	// after it is removed or terminated. ManagerContainer returns it.
	firstMgr testcontainers.Container
	// monitors holds the added MONs by name. The first MON is not in it.
	monitors map[string]*MonitorContainer
	// managers holds every owned MGR candidate by name, including the initial
	// MGR a.
	managers map[string]*ManagerContainer
	// services holds the optional services (RGW, MDS and the like) by name.
	services map[string]testcontainers.Container
}

// monitor returns the added MON of name, or nil.
func (r *memberRoster) monitor(name string) *MonitorContainer { return r.monitors[name] }

// monitorCount is the number of added MONs.
func (r *memberRoster) monitorCount() int { return len(r.monitors) }

// eachMonitor iterates the added MONs in map order. The loop body may call
// dropMonitor on the key it is visiting, as Terminate does.
func (r *memberRoster) eachMonitor() iter.Seq2[string, *MonitorContainer] {
	return func(yield func(string, *MonitorContainer) bool) {
		for name, mon := range r.monitors {
			if !yield(name, mon) {
				return
			}
		}
	}
}

// addMonitor records mon as the added MON of name.
func (r *memberRoster) addMonitor(name string, mon *MonitorContainer) {
	if r.monitors == nil {
		r.monitors = make(map[string]*MonitorContainer)
	}
	r.monitors[name] = mon
}

// dropMonitor forgets the added MON of name.
func (r *memberRoster) dropMonitor(name string) { delete(r.monitors, name) }

// manager returns the MGR candidate of name, or nil.
func (r *memberRoster) manager(name string) *ManagerContainer { return r.managers[name] }

// managerCount is the number of owned MGR candidates.
func (r *memberRoster) managerCount() int { return len(r.managers) }

// eachManager iterates the MGR candidates in map order. The loop body may call
// dropManager on the key it is visiting, as Terminate does.
func (r *memberRoster) eachManager() iter.Seq2[string, *ManagerContainer] {
	return func(yield func(string, *ManagerContainer) bool) {
		for name, mgr := range r.managers {
			if !yield(name, mgr) {
				return
			}
		}
	}
}

// addManager records mgr as the MGR candidate of name.
func (r *memberRoster) addManager(name string, mgr *ManagerContainer) {
	if r.managers == nil {
		r.managers = make(map[string]*ManagerContainer)
	}
	r.managers[name] = mgr
}

// dropManager forgets the MGR candidate of name. Dropping a clears the first
// MGR's container with it.
func (r *memberRoster) dropManager(name string) {
	delete(r.managers, name)
	if name == "a" {
		r.firstMgr = nil
	}
}

// initialManager returns the first MGR's container, or nil.
func (r *memberRoster) initialManager() testcontainers.Container { return r.firstMgr }

// recordInitialManager records ctr as the first MGR's container.
func (r *memberRoster) recordInitialManager(ctr testcontainers.Container) { r.firstMgr = ctr }

// clearInitialManager forgets the first MGR's container once Terminate removed
// it. The entry "a" of managers stays until the loop that follows drops it.
func (r *memberRoster) clearInitialManager() { r.firstMgr = nil }

// service returns the service container of name, or nil.
func (r *memberRoster) service(name string) testcontainers.Container { return r.services[name] }

// hasService reports whether a service is recorded under name, also one whose
// container is nil.
func (r *memberRoster) hasService(name string) bool {
	_, exists := r.services[name]
	return exists
}

// serviceCount is the number of services.
func (r *memberRoster) serviceCount() int { return len(r.services) }

// eachService iterates the services in map order. The loop body may call
// dropService on the key it is visiting, as Terminate does.
func (r *memberRoster) eachService() iter.Seq2[string, testcontainers.Container] {
	return func(yield func(string, testcontainers.Container) bool) {
		for name, ctr := range r.services {
			if !yield(name, ctr) {
				return
			}
		}
	}
}

// sortedServices lists the service containers by service name.
func (r *memberRoster) sortedServices() []testcontainers.Container {
	names := slices.Sorted(maps.Keys(r.services))
	result := make([]testcontainers.Container, 0, len(names))
	for _, name := range names {
		result = append(result, r.services[name])
	}
	return result
}

// serviceCopy returns a copy of the services by name.
func (r *memberRoster) serviceCopy() map[string]testcontainers.Container {
	return maps.Clone(r.services)
}

// setService records ctr as the service container of name.
func (r *memberRoster) setService(name string, ctr testcontainers.Container) {
	if r.services == nil {
		r.services = make(map[string]testcontainers.Container)
	}
	r.services[name] = ctr
}

// dropService forgets the service of name.
func (r *memberRoster) dropService(name string) { delete(r.services, name) }
