//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_ec_eligible_fragments))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// A K=2, M=1 pool with an explicit min_size of 2 acknowledges an overwrite
// under a snapshot context while one non-primary fragment holder is stopped.
// The primary's own reports then show which distinct fragment indices can
// supply the version that each object requires: while degraded, while the
// holder is back but recovery is held, and after recovery.
func TestECDegradedOverwriteEligibleFragments(t *testing.T) {
	parallelWhenEnabled(t)
	testECEligibleFragments(t)
}
