// Package cephfs prepares CephFS server fixtures in disposable Ceph clusters:
// filesystems, their MDS topology and data pools, client sessions and their
// timeouts, and snapshot schedules. Subvolume groups, subvolumes, snapshots,
// clones, subvolume principals and pins are in the cephfs/subvolume package,
// quiesce sets in cephfs/quiesce, and snapshot mirroring between two clusters
// in cephfs/mirror.
//
// Run starts a cluster with at least one filesystem. Start adds a filesystem
// to a cluster created by ceph.Run or another service package's Run, and
// WithFilesystems selects filesystems for any of those Run functions.
package cephfs
