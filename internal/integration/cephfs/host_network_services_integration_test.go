//go:build all || (integration && hostnetwork && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestHostNetworkCephFSFilesystem(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSFilesystem(t, ceph.WithHostNetwork())
}
