//go:build all || (integration && hostnetwork && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data_layout))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestHostNetworkCephFSFilesystem(t *testing.T) {
	parallelWhenEnabled(t)
	testCephFSFilesystem(t, ceph.WithHostNetwork())
}
