//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// A client holds a flock while its active MDS is SIGKILLed and the standby
// takes the rank: the new MDS learns the lock from the clients' reconnects,
// refuses a contender and grants it after the unlock, without an eviction.
func TestCephFSFlockAcrossMDSFailover(t *testing.T) {
	testCephFSFlockAcrossMDSFailover(t)
}

// The MDS is SIGKILLed after one rename reached its journal and before it
// reached the directory object; the standby replays the journal and the
// rename survives for the same client and an independent one.
func TestCephFSJournaledRenameAcrossMDSCrash(t *testing.T) {
	testCephFSJournaledRenameAcrossMDSCrash(t)
}
