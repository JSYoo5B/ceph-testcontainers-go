//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestPoolPGWatchReportsDegradedAndCleanAcrossAnOSDStop(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testPoolPGWatch(t)
}
