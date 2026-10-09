//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_multisite_host))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostNetworkRGWMultisite(t *testing.T) {
	testMultiClusterRGWMultisite(t, ceph.WithHostNetwork())
}
