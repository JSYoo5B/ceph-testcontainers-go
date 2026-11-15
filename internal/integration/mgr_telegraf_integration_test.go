//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The telegraf module sends line protocol to a receiver the test owns over
// UDP and TCP: every line carries the cluster's fsid, a new pool's writes
// appear under its native pool ID, an outage is only logged and loses its
// passes, a failover moves the sender, and disabling stops it.
func TestManagerTelegrafDelivery(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testManagerTelegrafDelivery(t)
}
