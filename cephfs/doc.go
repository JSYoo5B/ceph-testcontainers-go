// Package cephfs prepares CephFS server fixtures in disposable Ceph clusters:
// filesystems and their MDS topology, subvolume groups, subvolumes, snapshots,
// clones, pins, quiesce sets and client authorization, and snapshot mirroring
// between two clusters.
//
// Run starts a cluster with at least one filesystem. Start adds a filesystem
// to a cluster created by ceph.Run or another service package's Run, and
// WithFilesystems selects filesystems for any of those Run functions.
package cephfs
