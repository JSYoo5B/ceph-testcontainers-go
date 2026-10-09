//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWUserPlacementPolicy(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWUserPlacementPolicy(t)
}

func TestHostNetworkRGWUserPlacementPolicy(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWUserPlacementPolicy(t, ceph.WithHostNetwork())
}
