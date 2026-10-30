//go:build all || (integration && hostnetwork && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestHostNetworkCephFSSnapshotMirrorAndBackup(t *testing.T) {
	harness.RunMultiClusterCephFSSnapshotMirrorAndBackup(t, ceph.WithHostNetwork())
}
