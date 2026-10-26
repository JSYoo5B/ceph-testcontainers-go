// Package rbd prepares RBD server fixtures in disposable Ceph clusters:
// initialized image pools, namespaces and full or incremental backup transport.
// Mirroring between two clusters is in the rbd/mirror package.
//
// Run starts a cluster with at least one initialized RBD pool. InitPool
// prepares a pool on a cluster created by ceph.Run or another service
// package's Run, and WithPools selects RBD pools for any of those Run
// functions.
package rbd
