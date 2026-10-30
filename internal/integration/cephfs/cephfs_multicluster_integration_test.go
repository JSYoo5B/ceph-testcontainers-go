//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// This exercises two separate mechanisms: an application snapshot archive and
// the native cephfs-mirror daemon. The archive is deliberately a small fixture
// format, not a general-purpose filesystem backup utility.
func TestMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T) {
	harness.RunMultiClusterCephFSSnapshotMirrorAndBackup(t)
}
