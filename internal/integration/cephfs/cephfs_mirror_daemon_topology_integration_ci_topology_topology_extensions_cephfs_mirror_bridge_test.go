//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_cephfs_mirror_bridge))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// This variant keeps native dead-owner failover unchanged, but requests an
// explicit fixture rebalance after expanding the surviving set with AddDaemon.
func TestMultiClusterCephFSMirrorDaemonRebalanceTopology(t *testing.T) {
	harness.RunCephFSMirrorDaemonTopology(t, false, true)
}
