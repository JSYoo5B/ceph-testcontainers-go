//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_mgr_image))))

//ci: timeout=40m job-timeout=55 image-roles=essential,client,mgr-modules

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The alerts module mails health changes to an SMTP receiver the test owns:
// a baseline on every start, a transport control, a change and its
// clearance, a change lost while the receiver is down, an authentication
// refusal and its restoration, and the promoted manager after a failover.
func TestManagerAlertsSMTPDelivery(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testManagerAlertsDelivery(t)
}
