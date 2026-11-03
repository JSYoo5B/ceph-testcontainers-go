//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// The active MDS is SIGKILLed after the watch baseline; the watched takeover
// starts a fresh client session that must find the data written before the
// fault, and the watched return of the killed daemon as a standby precedes
// WaitReady and a third client session.
func TestCephFSMDSWatchReportsTakeoverAndRestoredStandby(t *testing.T) {
	testCephFSMDSWatch(t)
}
