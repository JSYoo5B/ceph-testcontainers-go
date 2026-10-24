//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_min_size_boundary))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// TestOSDMinSizeBoundaryDuringRecovery crosses a pool's min_size while a
// backfill is pending and a retained live writer is running: the writer is
// acknowledged with min_size OSDs acting, is not acknowledged while fewer act,
// and continues, without any replay, once a second OSD returns. It does not
// cover recovery_toofull, a transfer in progress when min_size is crossed,
// log-based recovery, erasure-coded pools or physical capacity exhaustion.
func TestOSDMinSizeBoundaryDuringRecovery(t *testing.T) { testOSDMinSizeBoundary(t) }
