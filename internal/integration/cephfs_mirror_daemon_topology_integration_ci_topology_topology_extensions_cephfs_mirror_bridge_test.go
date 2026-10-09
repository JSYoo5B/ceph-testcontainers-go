//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_cephfs_mirror_bridge))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

// This variant keeps native dead-owner failover unchanged, but requests an
// explicit fixture rebalance after expanding the surviving set with AddDaemon.
func TestMultiClusterCephFSMirrorDaemonRebalanceTopology(t *testing.T) {
	testCephFSMirrorDaemonTopology(t, false, true)
}
