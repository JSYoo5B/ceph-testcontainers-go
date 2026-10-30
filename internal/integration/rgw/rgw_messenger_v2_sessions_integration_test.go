//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_messenger_sessions))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs,rgw

package rgw_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// An RGW cluster without Messenger v1: the gateway's container listens on
// its HTTP port only, and every msgr2 session that a MON, MGR or OSD logged
// as READY at debug_ms 1 with one of radosgw's messengers, the ones the MONs
// hold client sessions with from that container, is msgr2.1. radosgw-admin's
// sessions from the same container are left out. Where the release has
// messenger dump, radosgw's own messengers are exactly those, listen on no
// socket and their READY connections to MONs and OSDs are msgr2.1.
func TestRGWMessengerV2Sessions(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testRGWMessengerV2Sessions(t)
}
