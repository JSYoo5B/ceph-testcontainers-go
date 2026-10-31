//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_keepalive))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import "testing"

// A libcephfs client mounted at debug_ms 20 writes, fsyncs and idles in
// cycles. Its log shows it sending KEEPALIVE2 frames, receiving
// KEEPALIVE2_ACK and exchanging ACK frames with the MDS, and the MDS's log
// shows it receiving those keepalives and acknowledging the client's
// messages.
func TestCephFSMessengerKeepalive(t *testing.T) {
	testCephFSMessengerKeepalive(t)
}
