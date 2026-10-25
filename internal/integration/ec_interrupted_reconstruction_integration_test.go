//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_ec_reconstruction))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// TestECInterruptedReconstruction reuses the compound-fault orchestration with
// an explicit 2+1 EC pool. A backfill destination's advanced, unfinished cursor
// is required before its retained instance is stopped.
func TestECInterruptedReconstruction(t *testing.T) { testOSDSecondFaultPool(t, true) }
