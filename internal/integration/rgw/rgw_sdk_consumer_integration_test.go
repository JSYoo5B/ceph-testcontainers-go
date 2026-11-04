//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import "testing"

func TestRGWPublicSDKConsumer(t *testing.T) {
	testRGWPublicSDKConsumer(t)
}
