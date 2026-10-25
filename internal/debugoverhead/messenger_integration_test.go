//go:build (all || integration) && ((debug_ms_window && !ci) || (ci_optional && (!ci_batch || ci_batch_debug_ms_window)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package debugoverhead_test

import "testing"

// TestOSDMessengerDebugOverhead measures one OSD's debug_ms operation windows.
// It is an explicit optional cohort, separate from debug_osd and required CI.
func TestOSDMessengerDebugOverhead(t *testing.T) { runDebugOverhead(t, "debug_ms") }
