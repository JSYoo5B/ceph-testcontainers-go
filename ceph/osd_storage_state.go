package ceph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
)

// osdStorageState is the owned OSD storage of one cluster: the tmpfs volume
// that OSDStorage.WithInMemory selects, the ordinary volume that
// OSDStorage.WithVolume selects, and the test seams that replace the Docker
// collaborators of their allocation and of the OSD containers. It is the
// fourth component of the internal state split, recorded in
// docs/design/container-decomposition/osd-storage-state.md.
//
// The type has no mutex. Container.mu guards every field, as it guarded
// Container.osdMemory and Container.osdVolume before the move: registerOSD,
// removeOSD, detachOSD, reopenOSD, startInitialOSDs and terminate run with it
// held, and the ensure helpers run inside registerOSD. The seams are written
// only by tests, before a cluster is used; startOSD, which runs without
// Container.mu so that Run can start several OSDs at once, only reads runner.
//
// Terminate order, unchanged by the move: releaseMemory and releaseVolume run
// after the OSD containers are removed and before the manager, the monitors and
// the control container; retryMemory and retryVolume run once more after the
// labeled containers are removed, because one of those can be a storage user
// whose create reply was lost. The guards in osd_storage_state_test.go enforce
// that no field is touched outside this type, that the methods call only
// their pinned collaborators, that callers hold Container.mu, that a seam has
// no production writer, that the release stages keep their order, and that the
// split adds no mutex.
type osdStorageState struct {
	// memory and volume are published before allocation, so that a lost create
	// reply stays owned for cleanup, and cleared only by a successful release.
	memory *osdMemoryStorage
	volume *osdVolumeStorage

	// runner replaces runContainer for OSD containers in tests.
	runner func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
	// volumeDocker and volumeRunner replace owned-volume allocation in tests.
	volumeDocker func(context.Context) (osdMemoryVolumeClient, error)
	volumeRunner func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
}

// memoryStorage returns the published tmpfs storage, or nil.
func (s *osdStorageState) memoryStorage() *osdMemoryStorage { return s.memory }

// volumeStorage returns the published volume storage, or nil.
func (s *osdStorageState) volumeStorage() *osdVolumeStorage { return s.volume }

// hasVolume reports whether volume storage was published.
func (s *osdStorageState) hasVolume() bool { return s.volume != nil }

// beginMemory allocates and publishes the tmpfs storage record before its
// volume exists.
func (s *osdStorageState) beginMemory(size int64) *osdMemoryStorage {
	m := newOSDMemoryStorage(size)
	s.memory = m
	return m
}

// beginVolume allocates and publishes the volume storage record before its
// volume exists, wired to the Docker seam.
func (s *osdStorageState) beginVolume() *osdVolumeStorage {
	v := newOSDVolumeStorage()
	v.newClient = s.volumeDocker
	s.volume = v
	return v
}

// runOSD starts an OSD container through the seam or runContainer.
func (s *osdStorageState) runOSD(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	run := s.runner
	if run == nil {
		run = runContainer
	}
	return run(ctx, image, opts...)
}

// runVolumeHelper starts the volume helper through the seam or runContainer.
func (s *osdStorageState) runVolumeHelper(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	run := s.volumeRunner
	if run == nil {
		run = runContainer
	}
	return run(ctx, image, opts...)
}

// launchVolumes returns the names of the memory and data volumes that an OSD
// container mounts, empty for storage that was not published.
func (s *osdStorageState) launchVolumes() (memory, data string) {
	if s.memory != nil {
		memory = s.memory.name
	}
	if s.volume != nil {
		data = s.volume.name
	}
	return memory, data
}

// requireFresh checks, memory first and then volume, that osd.id has no
// directory in the published storage.
func (s *osdStorageState) requireFresh(ctx context.Context, id int) error {
	if s.memory != nil {
		if err := s.memory.requireFreshOSD(ctx, id); err != nil {
			return err
		}
	}
	if s.volume != nil {
		if err := s.volume.requireFreshOSD(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// discard removes the directory of a purged osd.id, memory first and then
// volume; the first failure stops it.
func (s *osdStorageState) discard(ctx context.Context, id int) error {
	if s.memory != nil {
		if err := s.memory.discardOSD(ctx, id); err != nil {
			return err
		}
	}
	if s.volume != nil {
		if err := s.volume.discardOSD(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// retainedStore reads the identity of osd.id's directory in the volume storage.
// The caller checked hasVolume.
func (s *osdStorageState) retainedStore(ctx context.Context, id int) (osdStoreIdentity, error) {
	return s.volume.retainedStore(ctx, id)
}

// releaseMemory is the first Terminate stage for the tmpfs storage: it
// releases it when osdsGone, and forgets it only when the release succeeded.
func (s *osdStorageState) releaseMemory(ctx context.Context, osdsGone bool) error {
	if s.memory == nil || !osdsGone {
		return nil
	}
	err := s.memory.release(ctx)
	if err == nil {
		s.memory = nil
	}
	return err
}

// releaseVolume is releaseMemory for the volume storage.
func (s *osdStorageState) releaseVolume(ctx context.Context, osdsGone bool) error {
	if s.volume == nil || !osdsGone {
		return nil
	}
	err := s.volume.release(ctx)
	if err == nil {
		s.volume = nil
	}
	return err
}

// retryMemory is the second Terminate stage: it repeats the release only when
// the first one failed (previous), and returns previous otherwise.
func (s *osdStorageState) retryMemory(ctx context.Context, previous error, osdsGone bool) error {
	if previous == nil || s.memory == nil || !osdsGone {
		return previous
	}
	return s.releaseMemory(ctx, osdsGone)
}

// retryVolume is retryMemory for the volume storage.
func (s *osdStorageState) retryVolume(ctx context.Context, previous error, osdsGone bool) error {
	if previous == nil || s.volume == nil || !osdsGone {
		return previous
	}
	return s.releaseVolume(ctx, osdsGone)
}
