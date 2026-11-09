//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestManagerWatchReportsFailoverAndServiceAdvertisement(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testManagerWatch(t)
}
