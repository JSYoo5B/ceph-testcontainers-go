//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_admin_socket))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import "testing"

func TestDaemonAdminSocket(t *testing.T) {
	parallelWhenEnabled(t)
	testDaemonAdminSocket(t)
}
