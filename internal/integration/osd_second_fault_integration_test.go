//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_second_fault))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// TestOSDSecondFaultDuringBackfill stops a second OSD while the backfill to a
// spare OSD is observably under way, with min_size 2 held by the two remaining
// replicas. It does not cover a fullness policy barrier or a below-min_size
// outage.
func TestOSDSecondFaultDuringBackfill(t *testing.T) { testOSDSecondFault(t) }
