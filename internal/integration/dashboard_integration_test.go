//go:build all || (integration && features && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_dashboard))))

//ci: timeout=45m job-timeout=60 image-roles=essential,client,rgw

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

func TestDashboardOwnedHTTPS(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		t.Run(mode.Name, func(t *testing.T) {
			var opts []testcontainers.ContainerCustomizer
			if mode.Host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			testDashboardOwnedHTTPS(t, opts...)
		})
	}
}

func TestDashboardEssentialMGRRefused(t *testing.T) {
	testDashboardEssentialMGRRefused(t)
}

func TestDashboardRGWGateway(t *testing.T) {
	testDashboardRGWGateway(t)
}
