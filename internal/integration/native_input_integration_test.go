//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_native_input))))

//ci: timeout=15m job-timeout=25 image-roles=essential,client

package integration_test

import "testing"

// A worker under RunNativeCommand gets binary records on its standard input
// in separate phases, and then the end of input.
func TestNativeCommandInput(t *testing.T) {
	testNativeCommandInput(t)
}

// Canceling a call while it writes to a command that does not read stops the
// command and ends the writes.
func TestNativeCommandInputCancellation(t *testing.T) {
	testNativeCommandInputCancellation(t)
}
