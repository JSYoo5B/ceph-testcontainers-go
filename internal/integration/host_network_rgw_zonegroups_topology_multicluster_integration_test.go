//go:build integration && hostnetwork && multicluster

package integration_test

import "testing"

func TestHostNetworkRGWZonegroupsAndRemovalTopology(t *testing.T) {
	testRGWZonegroupsAndRemovalTopology(t, true)
}
func TestHostNetworkRGWInitialZonegroupsTopology(t *testing.T) {
	testRGWInitialZonegroupsTopology(t, true)
}
