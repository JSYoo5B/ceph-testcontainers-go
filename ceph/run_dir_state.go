package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
)

// runDirState is the shared admin-socket run directory of one cluster: the
// Docker volume that every MON, MGR, OSD and control container mounts at
// /var/run/ceph, so that a command in the control container can reach the
// admin socket of a daemon. It is the sixth component of the internal state
// split described in docs/design/container-decomposition.md.
//
// The type has no mutex and Container.mu was not given a new job. The parts
// have two different guards, which the methods name:
//
//   - The volume pointer is written once, by create, which only Run calls on a
//     cluster that no other goroutine can reach yet. After that it is never
//     assigned again, so published, volumeName and mounts read only that
//     pointer and the immutable name of the volume, and need no lock. The
//     feature-client startup relies on this: it mounts the volume while it
//     holds only the feature-client startup lock (ceph/images.go).
//   - The removed mark of the volume is written by release, which terminate
//     calls with Container.mu held, and read by shared under the same lock
//     (adminSocket and osdDebugTarget.set take lockTopology first).
//
// The guards in run_dir_state_test.go enforce that nothing touches the field
// outside this type, that the methods call only their pinned collaborators,
// that create has a single caller, that the callers of the mu-guarded methods
// take the topology lock, that terminate removes the volume after the storage
// retry and before the bridges, and that the split adds no mutex.
type runDirState struct {
	// volume is nil for a cluster value that was not made by Run, and is
	// published before the Docker create so that a lost create reply stays
	// owned for removal by Terminate.
	volume *runVolume
}

// create allocates the volume record, publishes it and then creates the
// volume. A failed create keeps the record. Run only, before the cluster is
// shared.
func (d *runDirState) create(ctx context.Context) error {
	d.volume = newRunVolume()
	return d.volume.create(ctx)
}

// published reports whether a volume record exists, removed or not. No lock.
func (d *runDirState) published() bool { return d.volume != nil }

// volumeName returns the name of the volume, or "" without one. No lock.
func (d *runDirState) volumeName() string {
	if d.volume == nil {
		return ""
	}
	return d.volume.name
}

// mounts returns the mount of the volume for a new daemon or control
// container, or nothing for a cluster value without a run volume. No lock.
func (d *runDirState) mounts() []testcontainers.ContainerCustomizer {
	if d.volume == nil {
		return nil
	}
	return []testcontainers.ContainerCustomizer{d.volume.mount()}
}

// shared reports whether admin sockets can be reached: a volume exists and was
// not removed. mu held.
func (d *runDirState) shared() bool { return d.volume != nil && !d.volume.removed }

// release removes the volume when one exists, and marks it removed only when
// the removal succeeded or the volume was already gone. mu held.
func (d *runDirState) release(ctx context.Context) error {
	if d.volume == nil {
		return nil
	}
	return d.volume.remove(ctx)
}
