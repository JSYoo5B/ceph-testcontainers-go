//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_cephfs))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"
)

// The watch of one mirrored directory advances the test: each source
// checkpoint and the receiver's loss are acted on when the watch reports
// them, and the destination bytes are verified independently of the watch.
func TestMultiClusterCephFSMirrorDirectoryWatch(t *testing.T) {
	runCephFSMirrorDirectoryWatch(t)
}
