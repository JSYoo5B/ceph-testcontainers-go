//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A scoped RGW rate limit as a SUT test condition: User.OverrideRateLimit on
// a bucket the SUT created and on the user itself, against a real gateway.
func TestRGWRateLimitOverride(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWRateLimitOverride(t)
}
