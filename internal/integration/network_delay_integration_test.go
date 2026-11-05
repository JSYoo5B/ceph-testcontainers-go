//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_network_faults))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client architecture=arm64

package integration_test

import (
	"testing"
)

// It needs the netem helper (CEPH_TEST_NETEM_IMAGE, by default
// fault.NetemHelperImage) and an engine that grants it NET_ADMIN; a missing
// one fails the test.
func TestNetworkDelay(t *testing.T) {
	runNetworkDelay(t)
}

// The IPv6 filter of a peer delay, on an IPv6 cluster.
func TestNetworkDelayPeerIPv6(t *testing.T) {
	runNetworkDelayPeerIPv6(t)
}

// The Direction and Phase selectors: connection setup cut toward one OSD
// while sessions that exist keep answering, and one OSD's answers cut at its
// own endpoint through an Inbound profile.
func TestNetworkDirectionAndPhase(t *testing.T) {
	runNetworkDirectionAndPhase(t)
}
