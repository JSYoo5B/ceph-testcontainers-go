//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_osd_backoff))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// A primary whose PG peered without going active, because a replica of a
// min_size 3 pool stopped, holds a native client's read with a PG-wide
// backoff; the client's and the primary's messenger logs join BLOCK,
// ACK_BLOCK and UNBLOCK, and the original read completes once the replica
// returns.
func TestOSDBackoffReceipt(t *testing.T) {
	parallelWhenEnabled(t)
	testOSDBackoffReceipt(t)
}
