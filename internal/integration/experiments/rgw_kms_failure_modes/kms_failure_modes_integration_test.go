//go:build all || (integration && features && (!ci || (ci_recovery && (!ci_batch || ci_batch_rgw_kms_failure_modes))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_kms_failure_modes_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestRGWKMSFailureModes records how RGW behaves when its external KMS fails:
// a revoked token, a KMS cut off the network, and SSE-S3 bucket deletion with
// and without permission to delete the bucket's Transit key. Docker cannot
// disconnect a host-network endpoint, so it runs on a bridge only.
func TestRGWKMSFailureModes(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, rgwKMSFailureModes)
	}
}
