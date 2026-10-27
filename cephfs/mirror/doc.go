// Package mirror mirrors CephFS snapshots of selected directories from a
// filesystem in one Ceph cluster to a filesystem in another, with native
// cephfs-mirror daemons: directory enrollment, peer management, MGR
// reassignment, process quiescence and typed readiness observation.
//
// Run starts the fixture for two existing clusters. Filesystems and data are
// prepared with the cephfs package. RBD mirroring is a separate fixture in the
// rbd/mirror package; the two services share no public interface.
package mirror
