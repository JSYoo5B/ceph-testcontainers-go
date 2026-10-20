//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_mds_path_fault))))

//ci: timeout=35m job-timeout=45

package integration_test

import "testing"

// One direction of the path between a libcephfs client and its MDS is cut
// while the client holds an open file: the MDS's packets to the client, and
// in the reverse variant the client's packets to the MDS. A metadata request
// stays pending while data writes to the open file reach the OSDs. It needs
// the netem helper (CEPH_TEST_NETEM_IMAGE, by default ceph.NetemHelperImage)
// and an engine that grants it NET_ADMIN.
func TestCephFSClientMDSPathCut(t *testing.T) {
	t.Run("mds-to-client", func(t *testing.T) { testCephFSClientMDSPathCut(t, mdsToClient) })
	t.Run("client-to-mds", func(t *testing.T) { testCephFSClientMDSPathCut(t, clientToMDS) })
}

// Only the MDS's packets to the filesystem's one OSD are dropped while two
// live clients reach that OSD as before: a data write and a data sync
// complete, while a mkdir and a full fsync wait on the MDS's journal writes,
// without an eviction, and both complete after the restore. It needs the
// netem helper like TestCephFSClientMDSPathCut.
func TestCephFSMDSOSDPathCut(t *testing.T) {
	testCephFSMDSOSDPath(t, 1)
}

// The same with two OSDs, each with a copy of every object: one lease with
// a filter per OSD drops the MDS's packets to both, while the clients reach
// both, and the MDS waits on its journal writes in the same way.
func TestCephFSMDSOSDsPathCut(t *testing.T) {
	testCephFSMDSOSDPath(t, 2)
}
