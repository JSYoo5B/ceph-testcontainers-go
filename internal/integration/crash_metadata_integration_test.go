//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_crash_metadata))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

// It counts every container and volume of the Docker engine, so it never
// runs in parallel with other tests.
func TestCrashMetadata(t *testing.T) {
	testCrashMetadata(t)
}
