//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_osd_memstore))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestOSDMemStoreLegacyPlacement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testOSDMemStoreLegacyPlacement(t)
}
