//go:build integration && hostnetwork && multicluster

package integration_test

import "testing"

func TestHostNetworkCephFSManagerTopology(t *testing.T) {
	testCephFSManagerTopology(t, true)
}
