//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_live_quota))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// A single libcephfs mount retains a file descriptor while an independent
// administrator limits its directory to its current recursive entry count, then relaxes
// the limit. Read-only observation gates each single create attempt: EDQUOT
// at the observed entry limit, successful creation with one more slot. Independent reads
// verify both files, and quota restoration precedes client/cluster teardown.
func TestCephFSLiveQuota(t *testing.T) {
	testCephFSLiveQuota(t)
}
