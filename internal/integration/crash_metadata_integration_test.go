//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_daemon_basics))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,rbd-aux

package integration_test

import "testing"

// It counts every container and volume of the Docker engine, so it never
// runs in parallel with other tests.
func TestCrashMetadata(t *testing.T) {
	testCrashMetadata(t)
}
