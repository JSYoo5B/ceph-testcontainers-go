//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_clients))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,cephfs,rbd-aux,rgw

package cephfs_test

import (
	"testing"
)

// A retained libcephfs client keeps a file open while an independent client
// snapshots and then removes the snapshot of the file's directory. The
// client's own dump_cache is parsed into a derived snapshot context twice:
// first until it lists the new snapshot ID (the addition gate of
// TestCephFSLiveSnapRealmGate), then, after the removal, until a complete
// observation no longer lists it and carries a newer sequence. Only then does
// the retained fd overwrite the file; HEAD must hold the new bytes and the
// removed snapshot must no longer be readable.
func TestCephFSLiveSnapRealmRemoval(t *testing.T) {
	testCephFSLiveSnapRealmRemoval(t)
}
