//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_three_zone_bridge))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

func TestMultiClusterRGWThreeZoneTopology(t *testing.T) {
	testRGWThreeZoneTopology(t, false)
}
