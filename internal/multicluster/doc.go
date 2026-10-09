// Package multicluster implements relationships between independent Ceph test
// clusters: RGW multisite topology and policies, RBD and CephFS mirroring, and
// RBD full/incremental backup transport and restore. The public rgw, rbd and
// cephfs packages re-export it. It owns the additional containers and network
// attachments, not the clusters; Ceph-side replication configuration and data
// remain in the disposable clusters after Terminate.
package multicluster
