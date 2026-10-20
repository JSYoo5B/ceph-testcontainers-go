//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_client_auth))))

//ci: timeout=25m job-timeout=35 representative=auth

package integration_test

import "testing"

func TestClientKeyReplacement(t *testing.T) {
	parallelWhenEnabled(t)
	testClientKeyReplacement(t)
}
