//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_osd_backoff))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A 32 MiB RADOS read is held behind a frozen consumer: the OSD's send queue
// on that one connection stays full while the fixture's MON commands and the
// OSD's other sessions keep answering. Thawing the consumer delivers the exact
// bytes, and a fresh client reads them back.
func TestHeldNativeReply(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testHeldReply(t, false)
}

// The same held reply is abandoned by killing the frozen consumer: the OSD
// drops the connection without delivering it, and a fresh client reads the
// unchanged bytes back.
func TestAbandonedHeldNativeReply(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testHeldReply(t, true)
}
