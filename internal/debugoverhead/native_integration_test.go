//go:build (all || integration) && ((debug_overhead && !ci) || (ci_optional && (!ci_batch || ci_batch_debug_overhead)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package debugoverhead_test

import "testing"

// TestOSDDebugOverhead retains the explicit debug_osd measurement cohort.
func TestOSDDebugOverhead(t *testing.T) { runDebugOverhead(t, "debug_osd") }
