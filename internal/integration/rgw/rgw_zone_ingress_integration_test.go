//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_rgw_zone_ingress))))

//ci: timeout=30m job-timeout=45 image-roles=essential,client,rgw

package rgw_test

import "testing"

func TestRGWZoneIngress(t *testing.T) {
	testRGWZoneIngress(t)
}
