// Package multicluster provides helpers for relationships between independent
// Ceph test clusters: RGW multisite topology and policies, RBD/CephFS mirroring,
// and RBD full/incremental backup transport and restore.
// It owns the additional containers and network attachments, not the clusters.
// Terminate links before terminating either cluster. Ceph-side replication
// configuration and data remain in the disposable clusters after Terminate.
package multicluster
