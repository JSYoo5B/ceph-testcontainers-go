//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_rbd_zone_locality))))

//ci: timeout=30m job-timeout=45 image-roles=essential,client

package rbd_test

import "testing"

func TestRBDZoneLocality(t *testing.T) {
	testRBDZoneLocality(t)
}
