package cephfs

import (
	"context"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// lockSetupThenTopology takes the cluster's setup gate (context aware) and then
// its topology mutex, in the order of the package: setupMu before the cluster
// mutex (cephfs/entry.go:100-102). It is the acquisition prefix that
// removeFilesystem, ScaleMDS, removeStoppedMDS and addMDSReplacement each wrote
// out. It owns acquisition and the setup-gate release only.
//
// On error no lock is held. On success both are held: the caller releases the
// topology mutex with servicehook.Unlock at the point its own flow requires
// (some release it early, some defer it) and calls the returned release exactly
// once, normally deferred, to release the setup gate. Each caller keeps its own
// ownership predicate, refusal text and the timing of the topology release.
// This is not an admission predicate and adds no mutex.
func lockSetupThenTopology(ctx context.Context, c *ceph.Container) (release func(), err error) {
	if err := clusterutil.LockMutex(ctx, &filesystems(c).setupMu); err != nil {
		return nil, err
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		filesystems(c).setupMu.Unlock()
		return nil, err
	}
	return func() { filesystems(c).setupMu.Unlock() }, nil
}
