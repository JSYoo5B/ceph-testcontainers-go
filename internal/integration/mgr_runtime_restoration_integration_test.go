//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import "testing"

// TestMGRRuntimeRestoration checks the running managers, not only the stored
// state, after configuration and module membership restorations.
func TestMGRRuntimeRestoration(t *testing.T) {
	testMGRRuntimeRestoration(t)
}
