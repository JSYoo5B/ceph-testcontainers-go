package mirror

import (
	"slices"
)

// cephFSDirectoryOwnership is the fixture's directory registrations: the paths
// it registered and still owns, the paths whose native remove was sent without
// an acknowledged reply, and each path's registration generation. Only an
// acknowledged owned registration advances a generation, and a release keeps
// it, so a receipt that recorded one detects any later registration of the
// same path. A path whose remove is pending is never removed again until it
// has left the native policy. It is guarded by Container.mu. In-flight receipts
// are not part of it; mirrorOperations keeps them.
type cephFSDirectoryOwnership struct {
	owned, pendingRelease map[string]bool
	// A rebalance unregisters temporarily and restores the path on retry.
	// An explicit removal instead forgets the path once Ceph confirms absence.
	rebalanceRelease map[string]bool
	generations      map[string]uint64
}

// owns reports whether the fixture registered directory and still owns it.
func (o *cephFSDirectoryOwnership) owns(directory string) bool { return o.owned[directory] }

// generation is directory's registration generation; zero before its first
// acknowledged owned registration.
func (o *cephFSDirectoryOwnership) generation(directory string) uint64 {
	return o.generations[directory]
}

// releasePending reports whether a remove of directory was sent without an
// acknowledged reply.
func (o *cephFSDirectoryOwnership) releasePending(directory string) bool {
	return o.pendingRelease[directory] || o.rebalanceRelease[directory]
}

// hasIntent reports whether directory is owned, has a pending release or has
// exhausted its generations, any of which refuses a new addition intent.
func (o *cephFSDirectoryOwnership) hasIntent(directory string) bool {
	return o.owned[directory] || o.releasePending(directory) || o.generations[directory] == ^uint64(0)
}

// register records an acknowledged owned registration of directory and
// advances its generation.
func (o *cephFSDirectoryOwnership) register(directory string) {
	if o.owned == nil {
		o.owned = make(map[string]bool)
	}
	if o.generations == nil {
		o.generations = make(map[string]uint64)
	}
	o.owned[directory] = true
	o.generations[directory]++
}

// markReleasePending records an explicit removal, including a caller asking
// to keep the path absent after an uncertain temporary rebalance removal.
func (o *cephFSDirectoryOwnership) markReleasePending(directory string) {
	if o.pendingRelease == nil {
		o.pendingRelease = make(map[string]bool)
	}
	o.pendingRelease[directory] = true
	delete(o.rebalanceRelease, directory)
}

// rebalanceReleaseSet records temporary removes between rebalance attempts.
func (o *cephFSDirectoryOwnership) rebalanceReleaseSet() map[string]bool {
	if o.rebalanceRelease == nil {
		o.rebalanceRelease = make(map[string]bool)
	}
	return o.rebalanceRelease
}

// forget drops ownership and the pending release of directory once its
// removal is acknowledged or the path has left the native policy. Its
// generation is kept.
func (o *cephFSDirectoryOwnership) forget(directory string) {
	delete(o.owned, directory)
	delete(o.pendingRelease, directory)
	delete(o.rebalanceRelease, directory)
}

// ownedPaths lists the owned paths in order.
func (o *cephFSDirectoryOwnership) ownedPaths() []string {
	directories := make([]string, 0, len(o.owned))
	for directory, owned := range o.owned {
		if owned {
			directories = append(directories, directory)
		}
	}
	slices.Sort(directories)
	return directories
}

// registerDirectory records an acknowledged owned registration of directory:
// a completed addition receipt of the path is superseded, the path's
// generation advances and, with publish, Directories lists the path.
// Directories is a published descriptor only; ownership never reads it back.
// Container.mu is held, or Run has not shared the mirror yet.
func (mirror *Container) registerDirectory(directory string, publish bool) {
	mirror.ops.supersedeDirectoryAddition(directory)
	mirror.directories.register(directory)
	if publish && !slices.Contains(mirror.Directories, directory) {
		mirror.Directories = append(mirror.Directories, directory)
	}
}

// forgetDirectory drops the ownership of directory and its Directories entry.
// Container.mu is held.
func (mirror *Container) forgetDirectory(directory string) {
	mirror.Directories = slices.DeleteFunc(mirror.Directories, func(item string) bool { return item == directory })
	mirror.directories.forget(directory)
}
