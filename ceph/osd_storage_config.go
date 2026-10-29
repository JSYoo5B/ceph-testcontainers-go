package ceph

import "fmt"

// osdStorageField names one setting of OSDStorage.
type osdStorageField uint8

const (
	osdStorageBlockSize osdStorageField = 1 << iota
	osdStorageObjectStore
	osdStorageInMemory
	osdStorageVolume
)

// OSDStorage is the OSD storage group of the Ceph configuration: where and how
// every OSD of the cluster keeps its data, including OSDs added later. It is a
// plain value with chainable, value-returning methods, not a customizer; pass it
// to Run through its one grouped option function, WithOSDStorage. The zero value
// changes nothing, and each field records whether it was set, so an explicit
// setting is different from an unspecified one.
//
//	storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
//	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
//
// Within one value a later call of the same method replaces the earlier one.
// Run refuses a field that two WithOSDStorage options both set, even when they
// agree, so argument order never decides a conflict. Incompatible combinations
// (MemStore with in-memory or volume storage, in-memory with volume storage) are
// refused before any resource is created. The composition rules are in
// docs/contributing/configuration-composition.md.
type OSDStorage struct {
	set        osdStorageField
	blockSize  int64
	store      OSDObjectStore
	memoryMax  int64
	volumeKept bool
}

// WithBlockSize sets each sparse BlueStore file's logical size in bytes. The
// minimum is 64 MiB and the default remains 1 GiB. These are disposable files,
// not real disks. It is also the capacity of a MemStore OSD. Small functional
// fixtures can use ClusterConfig.WithNoInitialOSDs and override.SetConfig to skip mClock's
// startup capacity benchmark before AddOSD; the benchmark can exhaust a small
// file even without client data. A size below the minimum, including zero, is an
// explicit invalid setting, not an unspecified one.
func (o OSDStorage) WithBlockSize(size int64) OSDStorage {
	o.set |= osdStorageBlockSize
	o.blockSize = size
	return o
}

// WithObjectStore selects the object store of every OSD in the cluster,
// including OSDs added later. WithBlockSize sets the capacity of a MemStore OSD.
// MemStore cannot be combined with WithInMemory or WithVolume, which place
// BlueStore files in shared volumes. fault.InjectObjectDataError needs
// BlueStore. An unknown store, including the empty one, is refused.
func (o OSDStorage) WithObjectStore(store OSDObjectStore) OSDStorage {
	o.set |= osdStorageObjectStore
	o.store = store
	return o
}

// WithInMemory keeps all OSD stores in one Docker-managed tmpfs volume. maxBytes
// is a positive cluster-wide filesystem allocation ceiling, independent of each
// sparse file's logical WithBlockSize. It does not reserve RAM or limit daemon
// RSS; tmpfs may swap, and a full volume fails I/O. An owned helper using the
// essential image preserves data across OSD Stop/Start. Cluster termination
// discards the volume. Helper loss or Docker host/VM restart can lose data.
// Requires a Linux Docker engine with local tmpfs volume support, including
// compatible Docker Desktop engines. No host mount, privileged container or
// additional image is required. The default remains container-local sparse
// files. ClusterConfig.WithNoInitialOSDs defers allocation until AddOSD. A non-positive
// maxBytes is refused.
func (o OSDStorage) WithInMemory(maxBytes int64) OSDStorage {
	o.set |= osdStorageInMemory
	o.memoryMax = maxBytes
	return o
}

// WithVolume keeps classic BlueStore OSD directories in one ordinary
// Docker-managed local named volume owned by the cluster. Allocation is lazy
// until the first OSD registration. Unlike tmpfs it needs no live keeper to
// preserve bytes; an isolated essential-image helper manages fresh/purged paths.
// Terminate removes the volume after its users; a busy/failed removal is
// retryable. It cannot be combined with MemStore or WithInMemory. No
// caller-owned volumes, host mounts or devices are adopted. This does not
// provide retained detach/reopen, host-reboot durability or a public
// replacement-container API.
func (o OSDStorage) WithVolume() OSDStorage {
	o.set |= osdStorageVolume
	o.volumeKept = true
	return o
}

// WithOSDStorage is the grouped option function that carries an OSDStorage to
// Run. Like every Option it changes the cluster settings only and never reaches
// a container request.
func WithOSDStorage(storage OSDStorage) Option {
	return func(o *options) error { return storage.apply(o) }
}

// apply validates the fields storage set and writes them to the settings. It
// refuses a field that an earlier WithOSDStorage already set.
func (s OSDStorage) apply(o *options) error {
	if clash := s.set & o.osdStorageSet; clash != 0 {
		return fmt.Errorf("OSD storage %s set by more than one WithOSDStorage", osdStorageFieldName(clash))
	}
	if s.set&osdStorageBlockSize != 0 {
		if s.blockSize < 64<<20 {
			return fmt.Errorf("OSD block size must be at least 64 MiB")
		}
		o.blockSize = s.blockSize
	}
	if s.set&osdStorageObjectStore != 0 {
		if s.store != OSDBlueStore && s.store != OSDMemStore {
			return fmt.Errorf("unknown OSD object store %q", s.store)
		}
		o.osdObjectStore = s.store
	}
	if s.set&osdStorageInMemory != 0 {
		if s.memoryMax <= 0 {
			return fmt.Errorf("OSD in-memory storage size must be positive")
		}
		o.osdMemorySize = s.memoryMax
	}
	if s.set&osdStorageVolume != 0 {
		o.osdVolumeStorage = s.volumeKept
	}
	o.osdStorageSet |= s.set
	return nil
}

func osdStorageFieldName(f osdStorageField) string {
	switch {
	case f&osdStorageBlockSize != 0:
		return "block size"
	case f&osdStorageObjectStore != 0:
		return "object store"
	case f&osdStorageInMemory != 0:
		return "in-memory size"
	default:
		return "volume"
	}
}
