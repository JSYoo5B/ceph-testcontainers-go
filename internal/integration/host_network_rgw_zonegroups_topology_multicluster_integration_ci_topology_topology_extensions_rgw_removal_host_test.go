//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_removal_host))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestHostNetworkRGWZonegroupsAndRemovalTopology(t *testing.T) {
	testRGWZonegroupsAndRemovalTopology(t, true)
}
