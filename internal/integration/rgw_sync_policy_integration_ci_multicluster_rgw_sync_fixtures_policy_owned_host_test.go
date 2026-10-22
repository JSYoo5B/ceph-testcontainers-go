//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_host))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package integration_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestHostNetworkMultiClusterRGWOwnedSyncPolicy(t *testing.T) {
	testRGWOwnedSyncPolicy(t, ceph.WithHostNetwork())
}
