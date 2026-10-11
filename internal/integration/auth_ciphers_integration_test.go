//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_auth_ciphers))))

//ci: timeout=25m job-timeout=35

package integration_test

import "testing"

func TestAuthCiphersAndTicketLifetimes(t *testing.T) {
	parallelWhenEnabled(t)
	testAuthCiphers(t)
}
