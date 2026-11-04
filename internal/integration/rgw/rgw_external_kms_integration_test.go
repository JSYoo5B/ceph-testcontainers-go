//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_kms))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestRGWExternalKMS runs SSE-KMS and SSE-S3 against a caller-owned Vault over
// verified HTTPS on both legs. Docker cannot disconnect a host-network
// endpoint, so the KMS reachability fault runs on a bridge only.
func TestRGWExternalKMS(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "bridge") {
		t.Run(mode.Name, rgwExternalKMS)
	}
}
