package cephfs

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/mdsmap"
)

// The MDS lifecycle reads the native global MDS map through the decoders of
// cephfs/internal/mdsmap. Each reader builds the decoder's expectation from
// the filesystem and the daemon identity records under the owner lock, with
// the plain identity and pool forms below.

// mdsMapNative is the plain form of a captured identity; nil stays nil.
func mdsMapNative(native *fsbase.NativeIdentity) *mdsmap.Native {
	if native == nil {
		return nil
	}
	return &mdsmap.Native{ID: native.ID, MetadataPool: native.MetadataPool}
}

// mdsMapPools is the plain form of a pool identity read; nil stays nil.
func mdsMapPools(pools *fsbase.NativePools) *mdsmap.Pools {
	if pools == nil {
		return nil
	}
	names := make(map[int64]string, len(pools.Pools))
	for _, pool := range pools.Pools {
		names[pool.ID] = pool.Name
	}
	return &mdsmap.Pools{DataPools: pools.DataPools, Names: names}
}
