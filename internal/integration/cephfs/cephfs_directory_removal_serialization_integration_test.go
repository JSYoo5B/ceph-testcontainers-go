//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_directory_serialization))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// Removals sent back to back, and a removal right after an addition, while
// the source MGR's writes to its OSD take a second: each RemoveDirectory
// waits until the native policy is idle, so the MGR's mirroring module keeps
// answering and its policy drains. It needs the netem helper like
// TestCephFSClientMDSPathCut, and a bridge network.
func TestMultiClusterCephFSDirectoryRemovalSerialization(t *testing.T) {
	testCephFSDirectoryRemovalSerialization(t)
}
