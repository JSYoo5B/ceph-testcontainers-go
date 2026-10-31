//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_clients))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,cephfs,rbd-aux,rgw

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
