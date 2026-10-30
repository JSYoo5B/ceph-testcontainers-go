//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_ec_recovered_contribution))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A K=2, M=1 pool with an explicit min_size of 2 acknowledges an overwrite
// under a snapshot context while one non-primary fragment holder is stopped.
// After that holder has recovered and the PG is clean, the other non-primary
// holder is stopped, so that only the primary's fragment and the restored one
// remain, and fresh clients must read the head and the snapshot from them.
func TestECRecoveredIndexContribution(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testECRecoveredContribution(t)
}
