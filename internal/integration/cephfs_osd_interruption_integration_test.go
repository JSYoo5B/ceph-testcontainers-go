//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_osd_interruption))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package integration_test

import "testing"

// The OSD that holds a live client's file freezes while the client fsyncs a
// buffered write and a second client makes a directory: the client's write
// and the MDS's journal writes wait at the OSD, both calls wait without an
// eviction, and both return 0 when the OSD resumes.
func TestCephFSOSDInterruption(t *testing.T) {
	testCephFSOSDInterruption(t)
}

// The OSD that holds a live client's file stops and comes back with its
// recovery held, then released: the client's appends and fsyncs return 0
// while the PG is degraded and while it waits for recovery, the pools
// recover to active+clean, and the client and another client read every
// byte, without an eviction.
func TestCephFSOSDRecovery(t *testing.T) {
	testCephFSOSDRecovery(t)
}
