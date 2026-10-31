//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_operations))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestDaemonKeyRotation(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunDaemonKeyRotation(t)
}
