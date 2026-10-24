//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_fullness_barrier))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// TestOSDFullnessBarrierDuringBackfill makes the backfill destination refuse
// backfill through Ceph's own backfillfull policy (ratio overrides, no disk is
// filled), once as the revocation of a reservation that had been granted and
// once as a refusal at admission, and shows that lifting the policy lets the
// recovery finish with every acknowledged object intact. It does not cover
// recovery_toofull (the full ratio), physical capacity exhaustion, a second
// OSD fault, or client writes during the barrier.
func TestOSDFullnessBarrierDuringBackfill(t *testing.T) { testOSDFullnessBarrier(t) }
