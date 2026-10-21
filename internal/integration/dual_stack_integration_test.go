//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_dual_stack))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client,cephfs,rbd-aux,rgw

package integration_test

import "testing"

// TestDualStackAddressFamily starts a cluster with AddressDualStack and checks
// that MONs, MGRs and OSDs advertise a concrete IPv4 and IPv6 address, and
// that native clients pinned to the MON addresses of either family complete
// MON, MGR and object operations.
func TestDualStackAddressFamily(t *testing.T) {
	parallelWhenEnabled(t)
	testDualStackAddressFamily(t)
}

// TestDualStackSeparateClusterNetwork combines AddressDualStack with
// WithSeparateClusterNetwork.
func TestDualStackSeparateClusterNetwork(t *testing.T) {
	parallelWhenEnabled(t)
	testDualStackSeparateClusterNetwork(t)
}

// TestDualStackServices runs CephFS and RGW on an AddressDualStack cluster
// whose mgr.b, osd.1 and second MDS advertise their IPv6 address first, and
// checks that native clients complete MGR commands, object I/O and CephFS I/O
// over both families and that the gateway serves S3 over both families.
func TestDualStackServices(t *testing.T) {
	parallelWhenEnabled(t)
	testDualStackServices(t)
}
