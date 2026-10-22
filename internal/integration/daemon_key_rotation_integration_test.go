//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_key_rotation))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import "testing"

func TestDaemonKeyRotation(t *testing.T) {
	parallelWhenEnabled(t)
	testDaemonKeyRotation(t)
}
