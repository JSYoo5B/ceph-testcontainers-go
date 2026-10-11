//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_multisite_bridge))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// This exercises RGW's native HTTP multisite replication. It never copies an
// object through the host or shares an OSD between the independent clusters.
func TestMultiClusterRGWMultisite(t *testing.T) {
	testMultiClusterRGWMultisite(t)
}
