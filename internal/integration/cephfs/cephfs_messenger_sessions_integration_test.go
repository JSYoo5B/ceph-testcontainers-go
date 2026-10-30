//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_messenger_sessions))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A CephFS cluster without Messenger v1: every MON, MGR, OSD and the MDS
// advertise and bind v2 only, and every msgr2 session each of them logged
// as READY at debug_ms 1, the MDS's with a mounted client among them, is
// msgr2.1. Where the release has messenger dump, every READY connection it
// reports is msgr2.1 too and every listening messenger advertises v2 only.
func TestCephFSMessengerV2Sessions(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSMessengerV2Sessions(t)
}

// A libcephfs client injects socket failures into its own messenger for 2 s
// while it writes and fsyncs records. Its lossless session with the MDS
// reconnects as the same session, keeping its cookies and acknowledged
// sequence numbers, while its lossy OSD sessions are replaced by new ones.
// The MDS session stays open and an independent client reads every
// acknowledged record. The verdicts are the Docker-free READY-line judges'.
func TestCephFSMessengerReconnect(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSMessengerReconnect(t)
}
