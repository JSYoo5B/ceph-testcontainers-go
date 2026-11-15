//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The active manager serves Prometheus metrics to WithClient containers. Host
// networking needs a bindable address and one port per manager, because the
// active and standby managers share the host network.
func TestManagerPrometheusExporter(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) { testManagerPrometheusExporter(t, mode.Host) })
	}
}

// ManagerMetrics returns the active manager's samples: a pool's write counter
// grows with the writes to it and its object count matches, every OSD is up,
// and after a failover the new active manager answers. It never enables the
// module.
func TestManagerMetrics(t *testing.T) {
	testManagerMetrics(t)
}
