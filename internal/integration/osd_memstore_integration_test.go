//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_osd_memstore))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

func TestOSDMemStoreLegacyPlacement(t *testing.T) {
	parallelWhenEnabled(t)
	testOSDMemStoreLegacyPlacement(t)
}
