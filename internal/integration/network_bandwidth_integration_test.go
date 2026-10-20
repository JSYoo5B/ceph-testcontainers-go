//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_network_bandwidth))))

//ci: timeout=30m job-timeout=40

package integration_test

import "testing"

// It needs the netem helper (CEPH_TEST_NETEM_IMAGE, by default
// fault.NetemHelperImage) and an engine that grants it NET_ADMIN; a missing
// one fails the test.
func TestNetworkBandwidth(t *testing.T) {
	testNetworkBandwidth(t)
}

// The IPv6 filter of a peer rate limit, on an IPv6 cluster.
func TestNetworkBandwidthIPv6(t *testing.T) {
	testNetworkBandwidthIPv6(t)
}

// Severe limits: a client operation timeout, an extreme slowdown that still
// completes, and an overflowing netem queue.
func TestNetworkBandwidthSevere(t *testing.T) {
	testNetworkBandwidthSevere(t)
}
