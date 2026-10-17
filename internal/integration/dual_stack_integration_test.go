//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_dual_stack))))

//ci: timeout=25m job-timeout=35

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
