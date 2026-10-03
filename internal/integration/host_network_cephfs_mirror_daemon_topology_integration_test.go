//go:build integration && hostnetwork && multicluster

package integration_test

import "testing"

func TestHostNetworkCephFSMirrorDaemonTopology(t *testing.T) {
	requireCephFSNativeShuffleDiagnostic(t)
	testCephFSMirrorDaemonTopology(t, true)
}

func TestHostNetworkCephFSMirrorDaemonRebalanceTopology(t *testing.T) {
	testCephFSMirrorDaemonTopology(t, true, true)
}
