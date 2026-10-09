//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_removal_bridge))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestMultiClusterRGWZonegroupsAndRemovalTopology(t *testing.T) {
	testRGWZonegroupsAndRemovalTopology(t, false)
}
