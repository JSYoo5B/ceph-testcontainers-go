//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

func TestHostNetworkCephFSManagerTopology(t *testing.T) {
	testCephFSManagerTopology(t, true)
}
