//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_osd_peering_messages))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A new pool's one PG on two OSDs is created, peered, leased and deep
// scrubbed while the MON and the OSDs log every message at debug_ms 1 and
// the OSDs their peering and scrubber states at debug_osd 10. Each of
// pg_create2, pg_query2, pg_notify2, PGlog, pg_info2, pg_lease,
// pg_lease_ack, replica_scrub and rep_scrubmap about that PG is logged as
// sent by one daemon and received by the other in its direction, and falls
// between the states it drives in each OSD's own log.
func TestOSDPeeringMessages(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testOSDPeeringMessages(t)
}
