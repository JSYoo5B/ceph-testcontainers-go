//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_network_packet_faults))))

//ci: timeout=30m job-timeout=40

package integration_test

import "testing"

// It needs the netem helper (CEPH_TEST_NETEM_IMAGE, by default
// ceph.NetemHelperImage) and an engine that grants it NET_ADMIN; a missing
// one fails the test.
func TestNetworkPacketFaults(t *testing.T) {
	testNetworkPacketFaults(t, false)
}

// The same profiles through the IPv6 filter of a peer, on an IPv6 cluster.
func TestNetworkPacketFaultsIPv6(t *testing.T) {
	testNetworkPacketFaults(t, true)
}
