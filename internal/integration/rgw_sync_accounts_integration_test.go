//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_account))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestMultiClusterRGWAccountRootSync(t *testing.T) {
	testRGWAccountRootSync(t)
}

func TestHostNetworkMultiClusterRGWAccountRootSync(t *testing.T) {
	testRGWAccountRootSync(t, ceph.WithHostNetwork())
}
