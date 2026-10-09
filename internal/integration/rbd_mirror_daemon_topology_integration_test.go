//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rbd_daemons))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

// TestMultiClusterRBDMirrorDaemonTopology proves Ceph's native pool leader
// election, rather than counting containers. Two processes share one receiving
// pool/peer and retain replicated image identities across process membership
// changes. Native clients compare bytes; only rbd-mirror copies image data.
func TestMultiClusterRBDMirrorDaemonTopology(t *testing.T) {
	testMultiClusterRBDMirrorDaemonTopology(t)
}
