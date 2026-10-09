//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// The mirroring module executes peer bootstrap import in the active source
// MGR. Each standby must retain peer-network access when it becomes active;
// source.ManagerContainer() may already be nil after removing initial MGR a.
func TestMultiClusterCephFSManagerTopology(t *testing.T) {
	testCephFSManagerTopology(t, false)
}
