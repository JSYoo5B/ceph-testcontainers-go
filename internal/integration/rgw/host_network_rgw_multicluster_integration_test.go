//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_multisite_host))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestHostNetworkRGWMultisite(t *testing.T) {
	testMultiClusterRGWMultisite(t, ceph.WithHostNetwork())
}
