//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_osd_peer_path))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

// One direction of one OSD pair on the cluster network is cut: the primary's
// packets to its replica. It needs the netem helper (CEPH_TEST_NETEM_IMAGE,
// by default ceph.NetemHelperImage) and an engine that grants it NET_ADMIN.
func TestOSDReplicationPeerPathCut(t *testing.T) {
	testOSDReplicationPeerPathCut(t)
}
