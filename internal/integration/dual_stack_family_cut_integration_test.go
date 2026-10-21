//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_dual_stack_family_cut))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs,rbd-aux,rgw

package integration_test

import "testing"

// TestDualStackFamilyCut cuts one address family of one path of an
// AddressDualStack cluster, the client's packets to one daemon's IPv6
// address, while the same daemon over IPv4 and the client's other paths keep
// working, and checks that the blocked operation completes after Restore. It
// does this for a MON and for an OSD, then restarts the OSD's container and
// checks that the same client reaches the new process over IPv6. It needs the netem helper
// (CEPH_TEST_NETEM_IMAGE, by default fault.NetemHelperImage) and an engine
// that grants it NET_ADMIN.
func TestDualStackFamilyCut(t *testing.T) {
	parallelWhenEnabled(t)
	testDualStackFamilyCut(t)
}
