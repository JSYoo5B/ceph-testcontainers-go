//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_messenger_policy))))

//ci: timeout=20m job-timeout=30 representative=flaky

package integration_test

import "testing"

func TestMessengerPolicy(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerPolicy(t)
}

func TestClientMessengerPolicyOverridesSecureOnly(t *testing.T) {
	parallelWhenEnabled(t)
	testClientPolicyOverridesSecureOnly(t)
}
