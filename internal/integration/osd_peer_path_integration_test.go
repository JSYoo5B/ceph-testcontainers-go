//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_operations))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// One direction of one OSD pair on the cluster network is cut: the primary's
// packets to its replica. It needs the netem helper (CEPH_TEST_NETEM_IMAGE,
// by default fault.NetemHelperImage) and an engine that grants it NET_ADMIN.
func TestOSDReplicationPeerPathCut(t *testing.T) {
	harness.RunOSDReplicationPeerPathCut(t)
}
