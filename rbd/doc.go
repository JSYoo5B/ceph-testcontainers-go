// Package rbd prepares RBD server fixtures in disposable Ceph clusters:
// initialized image pools, namespaces, pool and image mirroring between two
// clusters, and full or incremental backup transport.
//
// Run starts a cluster with at least one initialized RBD pool. InitPool
// prepares a pool on a cluster created by ceph.Run or another service
// package's Run, and WithPools selects RBD pools for any of those Run
// functions.
package rbd
