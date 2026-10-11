//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_three_zone_host))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

func TestHostNetworkRGWThreeZoneTopology(t *testing.T) {
	testRGWThreeZoneTopology(t, true)
}
