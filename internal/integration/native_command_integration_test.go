//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_native_command))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client

package integration_test

import "testing"

func TestNativeCommands(t *testing.T) {
	parallelWhenEnabled(t)
	testNativeCommands(t)
}
