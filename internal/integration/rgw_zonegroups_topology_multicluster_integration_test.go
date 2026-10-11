//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_initial_bridge))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestMultiClusterRGWInitialZonegroupsTopology(t *testing.T) {
	testRGWInitialZonegroupsTopology(t, false)
}
