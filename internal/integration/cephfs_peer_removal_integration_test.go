//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_peer_drain))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package integration_test

import (
	"testing"
)

// Actual PUBLIC BeginPeerRemoval is required. No private attestation callback or
// experimental stopped-process quiescence helper participates in this parent.
// The wrapper runs real peer_remove to completion before losing one response.
func TestMultiClusterCephFSPeerRemovalDrain(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testCephFSPeerRemovalDrain(t, host) })
	}
}
