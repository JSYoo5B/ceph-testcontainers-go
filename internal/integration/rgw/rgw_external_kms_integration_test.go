//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_kms))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestRGWExternalKMS runs the external-KMS recipe of the rgw README: SSE-KMS
// and SSE-S3 configured against a caller-owned Vault over verified HTTPS on
// both legs, with a per-gateway trust file. The native KMS failure modes are
// the rgw_kms_failure_modes experiment case.
func TestRGWExternalKMS(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, rgwExternalKMS)
	}
}
