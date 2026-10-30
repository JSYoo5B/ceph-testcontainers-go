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
//
//purpose:construction
func (o OSDStorage) WithBlockSize(size int64) OSDStorage {
	o.set |= osdStorageBlockSize
	o.blockSize = size
	return o
}

// WithObjectStore selects the object store of every OSD in the cluster,
// including OSDs added later. WithBlockSize sets the capacity of a MemStore OSD.
// MemStore cannot be combined with WithInMemory or WithVolume, which place
// BlueStore files in shared volumes. fault.InjectObjectDataError needs
// BlueStore. An unknown store, including the empty one, is refused. MemStore
// keeps data in the OSD process's memory and saves it to the data directory
// when the store is unmounted, so the fixture disables Ceph's fast OSD shutdown
// for MemStore OSDs: a stopped OSD keeps its data across Stop and Start, while
// a killed OSD or a removed container loses it. ceph osd metadata reports
// osd_objectstore as memstore.
//
//purpose:construction
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
// maxBytes is refused. The volume shares the Docker daemon's memory (the Linux
// VM's on Docker Desktop, the remote host's on a remote engine) with every
// daemon; size the cap for replicas, metadata, recovery and mClock's startup
// benchmark, and note that deleting Ceph objects does not necessarily release
// tmpfs pages. The helper is an idle container without network, created before
// the first OSD; the cluster refuses to continue with a restarted helper.
// OSDContainer.Remove discards the purged OSD's directory. Rootless or managed
// engines may refuse tmpfs volumes.
//
//purpose:construction
func (o OSDStorage) WithInMemory(maxBytes int64) OSDStorage {
	o.set |= osdStorageInMemory
	o.memoryMax = maxBytes
	return o
}

// WithVolume keeps classic BlueStore OSD directories in one ordinary
// Docker-managed local named volume owned by the cluster. Allocation is lazy
// until the first OSD registration. Unlike tmpfs it needs no live keeper to
// preserve bytes; an isolated essential-image helper manages fresh/purged paths.
// Each OSD still gets its own sparse file with WithBlockSize as its logical
// capacity. The helper has no network and no cluster credentials; a stopped or
// restarted helper refuses further storage operations before a new OSD is
// registered and is not replaced automatically, while existing OSDs can still
// stop and start with their containers. OSDContainer.Remove drains and purges
// the native registration, removes the container and discards only its
// directory. Terminate removes every owned OSD and the helper, then deletes the
// volume without force; a busy or failed deletion keeps ownership for another
// Terminate. Changed volume metadata is refused before directory operations or
// deletion, so never modify or attach the volume externally. It cannot be
// combined with MemStore or WithInMemory. No caller-owned volumes, host mounts
// or devices are adopted. This is the only backend on which
// OSDContainer.Detach retains a store and DetachedOSD.Reopen restarts it in a
// new container; it gives no host-reboot durability, and Terminate also
// discards detached stores.
//
//purpose:construction
func (o OSDStorage) WithVolume() OSDStorage {
	o.set |= osdStorageVolume
	o.volumeKept = true
	return o
}

// WithOSDStorage is the grouped option function that carries an OSDStorage to
// Run. Like every Option it changes the cluster settings only and never reaches
// a container request.
//
//purpose:construction
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
