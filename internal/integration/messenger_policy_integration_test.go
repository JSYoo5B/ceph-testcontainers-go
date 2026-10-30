//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_messenger_policy))))

//ci: timeout=20m job-timeout=30 representative=flaky image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestMessengerPolicy(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMessengerPolicy(t)
}

func TestClientMessengerPolicyOverridesSecureOnly(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testClientPolicyOverridesSecureOnly(t)
}
