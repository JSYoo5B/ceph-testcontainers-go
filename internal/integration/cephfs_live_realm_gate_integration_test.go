//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_live_realm_gate))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client,cephfs

package integration_test

import (
	"testing"
)

// A retained libcephfs client keeps a file open while an independent client
// snapshots the file's directory. The client's own dump_cache is parsed into
// a derived snapshot context; only when that context lists the new snapshot
// ID does the retained fd overwrite the file, and the snapshot and HEAD bytes
// are then checked. An incomplete or invalid observation fails the test
// without releasing the overwrite.
func TestCephFSLiveSnapRealmGate(t *testing.T) {
	testCephFSLiveSnapRealmGate(t)
}
