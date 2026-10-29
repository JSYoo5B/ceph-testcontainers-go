package cephfs

import (
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// mdsRoster is the list of the metadata daemons one Filesystem owns, with the
// customizers every one of them is started with and the index of the next
// daemon name.
//
// The roster owns no lock and adds none. Its fields are guarded by the
// two-key invariant of the type:
//
//   - A reader holds the cluster mutex (servicehook.Lock or LockTopology,
//     ceph.Container.mu) or, for the two cold-start reads that run between the
//     two cluster-mutex sections of scaleColdMDS, the filesystem registry's
//     setupMu. Both are enough because every writer holds both.
//   - A writer holds setupMu and the cluster mutex. The writers are
//     admitMDSStart (reserveIndex), publishStartedMDS (publish), retireMDS,
//     removeFilesystem and removeStoppedMDS (remove). The constructor
//     newMDSRoster runs before the Filesystem is published.
//
// setupMu is taken before the cluster mutex, as everywhere in this package, and
// nothing in this type takes either lock. The methods marked "c.mu held"
// require the caller to hold the cluster mutex; members, snapshot and the
// others read the slice as it is, so a caller must not retain the result of
// members across an unlock. The guards in mds_roster_test.go enforce that the
// fields are reached only through these methods, that only the pinned writers
// call the mutating ones, that every caller holds the lock it needs, and that
// the split adds no mutex.
//
// The embedded Filesystem.Container is not part of the roster: its four
// re-seat sites stay where they are.
type mdsRoster struct {
	// opts are the customizers for every daemon. Immutable after newMDSRoster.
	opts []testcontainers.ContainerCustomizer
	// daemons are the owned daemons in creation order. A partial daemon with a
	// real container handle stays published until it is retired or removed.
	daemons []*MDS
	// nextIndex is the index of the next daemon name; it only grows.
	nextIndex int
}

// newMDSRoster returns an empty roster that starts daemons with a copy of opts.
func newMDSRoster(opts []testcontainers.ContainerCustomizer) mdsRoster {
	return mdsRoster{opts: slices.Clone(opts)}
}

// members returns the owned daemons in creation order without copying them. The
// caller holds c.mu (or setupMu, see the type) and does not edit or retain it.
func (r *mdsRoster) members() []*MDS { return r.daemons }

// snapshot returns a copy of the owned daemons. It keeps descriptor identity
// and creation order. c.mu held.
func (r *mdsRoster) snapshot() []*MDS { return slices.Clone(r.daemons) }

// count returns the number of owned daemons. c.mu held.
func (r *mdsRoster) count() int { return len(r.daemons) }

// at returns the i-th owned daemon in creation order and panics out of range,
// like indexing the slice. c.mu held.
func (r *mdsRoster) at(i int) *MDS { return r.daemons[i] }

// customizers returns a copy of the customizers for a daemon start. c.mu held.
func (r *mdsRoster) customizers() []testcontainers.ContainerCustomizer {
	return slices.Clone(r.opts)
}

// issued returns how many daemon name indexes were reserved. c.mu held, or
// setupMu held by a caller that does not publish.
func (r *mdsRoster) issued() int { return r.nextIndex }

// nextID returns the daemon name of the next reserved index for the named
// filesystem. c.mu held, or setupMu held by a caller that does not publish.
func (r *mdsRoster) nextID(filesystem string) string {
	return cephFSMDSID(filesystem, r.nextIndex)
}

// reserveIndex consumes the current name index. setupMu and c.mu held.
func (r *mdsRoster) reserveIndex() { r.nextIndex++ }

// publish appends a daemon, also a partial one that has a real container
// handle. setupMu and c.mu held by the caller chain.
func (r *mdsRoster) publish(daemon *MDS) { r.daemons = append(r.daemons, daemon) }

// remove drops a daemon by descriptor identity. setupMu and c.mu held.
func (r *mdsRoster) remove(daemon *MDS) {
	r.daemons = slices.DeleteFunc(r.daemons, func(owned *MDS) bool { return owned == daemon })
}
