//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestRGWUserPlacementPolicy(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWUserPlacementPolicy(t)
}

func TestHostNetworkRGWUserPlacementPolicy(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWUserPlacementPolicy(t, ceph.WithHostNetwork())
}
