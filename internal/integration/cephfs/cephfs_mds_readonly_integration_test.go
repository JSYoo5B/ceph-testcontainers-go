//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// The owned active MDS is forced read-only through MDS.ForceReadOnly: a fresh
// client still reads the seeded bytes and its write fails with EROFS. The
// daemon is then replaced by the owned standby, WaitReplaced reports the new
// GID on rank 0, and a newly mounted client reads the same bytes and writes.
func TestCephFSMDSReadOnlyFaultAndReplacement(t *testing.T) {
	testCephFSMDSReadOnlyFault(t)
}
