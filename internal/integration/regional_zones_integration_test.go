//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_regional_zones))))

//ci: timeout=45m job-timeout=60 image-roles=essential,client

package integration_test

import "testing"

func TestRegionalZoneTopology(t *testing.T) {
	testRegionalZoneTopology(t)
}

func TestRegionalZoneFailedSetup(t *testing.T) {
	testRegionalZoneFailedSetup(t)
}
