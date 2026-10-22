//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_client_auth))))

//ci: timeout=25m job-timeout=35 representative=auth image-roles=essential,client

package integration_test

import "testing"

func TestClientRevocation(t *testing.T) {
	parallelWhenEnabled(t)
	testClientRevocation(t)
}
