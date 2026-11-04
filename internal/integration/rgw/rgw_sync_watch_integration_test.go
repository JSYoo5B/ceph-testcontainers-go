//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_bridge))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"
)

// A zone watch and a bucket watch report the destination falling behind
// while its gateway is stopped and catching up after it starts, without
// holding the topology lock between reads; the destination then serves the
// exact bytes, and recreating the watched bucket ends the bucket watch.
func TestMultiClusterRGWSyncWatch(t *testing.T) {
	testRGWSyncWatch(t)
}
