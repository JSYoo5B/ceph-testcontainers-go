package cephfs

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/mdsmap"
)

// The MDS lifecycle reads the native global MDS map through the decoders of
// cephfs/internal/mdsmap. The expectations below are what the readers take
// from the filesystem and the daemon identity records under the owner lock;
// each decode hands the decoder only their plain values.

// coldMDSMapExpectation is every value the cold-start decoder compares the
// native global map with. verifyColdMDS reads them from the filesystem and the
// confirmed cold capability; the decoder keeps no reference to either.
type coldMDSMapExpectation struct {
	// filesystem is the configured name of the owned filesystem.
	filesystem string
	// reservedName is the name the first MDS would take, which must be absent.
	reservedName string
	// native is the identity captured at creation; the decoders require it.
	native *cephFSNativeIdentity
	// dataPools are the data pool IDs attached when the filesystem was created.
	dataPools []int64
}

// stoppedMDSMapExpectation is every value the stopped-MDS decoder compares the
// native global map with. The caller reads them from the filesystem and the
// target's identity record under the owner lock; the decoder keeps no reference
// to either.
type stoppedMDSMapExpectation struct {
	// targetName is the stopped daemon's immutable name, which must be absent.
	targetName string
	// filesystem is the configured name of the owned filesystem.
	filesystem string
	// native is the identity captured at creation; the decoders require it.
	native *cephFSNativeIdentity
	// pools is the native pool identity read for this decode.
	pools *cephFSNativePools
	// activeMDS, standbyMDS and standbyReplay are the configured policy.
	activeMDS, standbyMDS int
	standbyReplay         bool
}

// replacementMDSMapExpectation is every value the replacement decoder compares
// the native global map with. The caller reads them from the filesystem, the
// stopped target's identity record and the replacement record under the
// topology lock; the decoder keeps no reference to any of them.
type replacementMDSMapExpectation struct {
	// oldName is the stopped daemon's immutable name, which must be absent.
	oldName string
	// filesystem is the configured name of the owned filesystem.
	filesystem string
	// reservedName is the name reserved for the replacement daemon.
	reservedName string
	// workerExists reports whether the replacement daemon was already created.
	workerExists bool
	// native is the identity captured at creation; the decoders require it.
	native *cephFSNativeIdentity
	// pools is the native pool identity read for this decode.
	pools *cephFSNativePools
}

// decodeColdMDSMap checks that the owned filesystem is still cold; see
// mdsmap.DecodeCold.
func decodeColdMDSMap(data []byte, want coldMDSMapExpectation) error {
	return mdsmap.DecodeCold(data, mdsmap.ColdExpectation{Filesystem: want.filesystem, ReservedName: want.reservedName,
		Native: mdsMapNative(want.native), DataPools: want.dataPools})
}

// decodeStoppedMDSMap checks that a stopped MDS left the map; see
// mdsmap.DecodeStopped.
func decodeStoppedMDSMap(data []byte, want stoppedMDSMapExpectation) (*mdsmap.StoppedState, error) {
	return mdsmap.DecodeStopped(data, mdsmap.StoppedExpectation{TargetName: want.targetName, Filesystem: want.filesystem,
		Native: mdsMapNative(want.native), Pools: mdsMapPools(want.pools), ActiveMDS: want.activeMDS, StandbyMDS: want.standbyMDS,
		StandbyReplay: want.standbyReplay})
}

// decodeMDSReplacementMap reads how far an MDS replacement got; see
// mdsmap.DecodeReplacement.
func decodeMDSReplacementMap(data []byte, want replacementMDSMapExpectation) (*mdsmap.ReplacementState, error) {
	return mdsmap.DecodeReplacement(data, mdsmap.ReplacementExpectation{OldName: want.oldName, Filesystem: want.filesystem,
		ReservedName: want.reservedName, WorkerExists: want.workerExists, Native: mdsMapNative(want.native), Pools: mdsMapPools(want.pools)})
}

// mdsMapNative is the plain form of a captured identity; nil stays nil.
func mdsMapNative(native *cephFSNativeIdentity) *mdsmap.Native {
	if native == nil {
		return nil
	}
	return &mdsmap.Native{ID: native.id, MetadataPool: native.metadataPool}
}

// mdsMapPools is the plain form of a pool identity read; nil stays nil.
func mdsMapPools(pools *cephFSNativePools) *mdsmap.Pools {
	if pools == nil {
		return nil
	}
	names := make(map[int64]string, len(pools.pools))
	for _, pool := range pools.pools {
		names[pool.ID] = pool.Name
	}
	return &mdsmap.Pools{DataPools: pools.dataPools, Names: names}
}
