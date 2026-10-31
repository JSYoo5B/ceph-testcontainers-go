//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_snap_messages))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// A retained libcephfs client holds a directory open at debug_client 10
// while an independent administrator snapshots it. Its dump_cache, polled
// until it shows the snapshot, gates the read of its log, where the
// client_snap message that delivered the realm's SnapContext is decoded and
// required to hold the created snapshot's MDS ID.
func TestCephFSSnapMessages(t *testing.T) {
	testCephFSSnapMessages(t)
}
