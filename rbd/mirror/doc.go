// Package mirror connects two Ceph clusters with native RBD mirroring: setup
// CLI containers, one-way pool or image mirroring in snapshot or journal mode,
// any number of rbd-mirror receiver daemons, and typed readiness observation.
//
// Run starts the fixture for two existing clusters. The pool and namespaces
// are prepared with the rbd package. CephFS snapshot mirroring is a separate
// fixture in the cephfs package; the two services share no public interface.
package mirror
