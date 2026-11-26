// Package rbd prepares RBD server fixtures in disposable Ceph clusters:
// initialized image pools, namespaces and full or incremental backup transport.
// Mirroring between two clusters is in the rbd/mirror package.
//
// Run creates one initialized RBD pool named rbd by default. WithPools
// replaces that selection, and an explicitly empty selection starts the
// cluster without provisioning an RBD pool. InitPool prepares an existing
// pool on a cluster created by ceph.Run or another service package's Run.
package rbd
