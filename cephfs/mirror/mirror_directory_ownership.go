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
// has left the native policy. It is guarded by Mirror.mu. In-flight receipts
// are not part of it; mirrorOperations keeps them.
type cephFSDirectoryOwnership struct {
	owned, pendingRelease map[string]bool
	generations           map[string]uint64
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
	return o.pendingRelease[directory]
}

// hasIntent reports whether directory is owned, has a pending release or has
// exhausted its generations, any of which refuses a new addition intent.
func (o *cephFSDirectoryOwnership) hasIntent(directory string) bool {
	return o.owned[directory] || o.pendingRelease[directory] || o.generations[directory] == ^uint64(0)
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

// markReleasePending records that a remove of directory is about to be sent;
// an error cannot prove that the MGR did not apply it.
func (o *cephFSDirectoryOwnership) markReleasePending(directory string) {
	o.releaseSet()[directory] = true
}

// releaseSet is the pending-release set that a rebalance updates while it
// removes and restores the owned paths.
func (o *cephFSDirectoryOwnership) releaseSet() map[string]bool {
	if o.pendingRelease == nil {
		o.pendingRelease = make(map[string]bool)
	}
	return o.pendingRelease
}

// forget drops ownership and the pending release of directory once its
// removal is acknowledged or the path has left the native policy. Its
// generation is kept.
func (o *cephFSDirectoryOwnership) forget(directory string) {
	delete(o.owned, directory)
	delete(o.pendingRelease, directory)
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
// Mirror.mu is held, or Run has not shared the mirror yet.
func (mirror *Mirror) registerDirectory(directory string, publish bool) {
	mirror.ops.supersedeDirectoryAddition(directory)
	mirror.directories.register(directory)
	if publish && !slices.Contains(mirror.Directories, directory) {
		mirror.Directories = append(mirror.Directories, directory)
	}
}

// forgetDirectory drops the ownership of directory and its Directories entry.
// Mirror.mu is held.
func (mirror *Mirror) forgetDirectory(directory string) {
	mirror.Directories = slices.DeleteFunc(mirror.Directories, func(item string) bool { return item == directory })
	mirror.directories.forget(directory)
}
