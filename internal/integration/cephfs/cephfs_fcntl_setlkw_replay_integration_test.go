//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// A POSIX record lock blocked in F_SETLKW is replayed across an MDS standby
// takeover: the contender's request waits in the active MDS, survives the
// failover still waiting on the new MDS, and is granted only after the
// holder unlocks. See testCephFSFcntlSetlkwReplay.
func TestCephFSFcntlSetlkwReplay(t *testing.T) {
	testCephFSFcntlSetlkwReplay(t)
}
