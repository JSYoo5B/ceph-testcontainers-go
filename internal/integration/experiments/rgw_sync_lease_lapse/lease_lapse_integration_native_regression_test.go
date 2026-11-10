//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_sync_lease_lapse)))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_sync_lease_lapse_test

import "testing"

func TestMultiClusterRGWDataSyncAfterSyncLeaseLapse(t *testing.T) {
	testSyncLeaseLapse(t)
}
