//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// This exercises two separate mechanisms: an application snapshot archive and
// the native cephfs-mirror daemon. The archive is deliberately a small fixture
// format, not a general-purpose filesystem backup utility.
func TestMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T) {
	testMultiClusterCephFSSnapshotMirrorAndBackup(t)
}
