//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_bridge))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"
)

func TestMultiClusterRGWOwnedSyncPolicy(t *testing.T) {
	testRGWOwnedSyncPolicy(t)
}
