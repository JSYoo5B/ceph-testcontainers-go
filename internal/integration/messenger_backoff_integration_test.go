//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_osd_backoff))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A primary whose PG peered without going active, because a replica of a
// min_size 3 pool stopped, holds a native client's read with a PG-wide
// backoff; the client's and the primary's messenger logs join BLOCK,
// ACK_BLOCK and UNBLOCK, and the original read completes once the replica
// returns.
func TestOSDBackoffReceipt(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testOSDBackoffReceipt(t)
}

// A write to an object that is degraded, missing on the replica of an
// active PG whose recovery is held by norecover, is held by its primary
// running with osd_backoff_on_degraded with a backoff of that object alone;
// the client's and the primary's messenger logs join BLOCK, ACK_BLOCK and
// UNBLOCK of the object, the write completes after the UNBLOCK and a fresh
// client reads its bytes back.
func TestOSDObjectBackoffReceipt(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testOSDObjectBackoffReceipt(t)
}
