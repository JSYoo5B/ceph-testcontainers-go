//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// Two existing FUSE mounts and POSIX record-lock descriptors survive one
// rank-0 standby takeover. The contender must still observe the first
// holder's overlapping write lock before the holder makes another request.
func TestCephFSFcntlStandbyTakeover(t *testing.T) {
	testCephFSFcntlStandbyTakeover(t)
}
