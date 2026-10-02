// Package federation connects existing, independent Ceph test clusters using
// native RGW multisite, RBD snapshot mirroring and CephFS snapshot mirroring.
// It owns the additional containers and network attachments, not the clusters.
// Terminate links before terminating either cluster. Ceph-side replication
// configuration and data remain in the disposable clusters after Terminate.
package federation
