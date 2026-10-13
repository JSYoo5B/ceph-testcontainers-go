//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_messenger_policy))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The policy of TestMessengerPolicy on a cluster without Messenger v1.
func TestMessengerPolicyV2Only(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerPolicy(t, ceph.WithMessengerMode(ceph.MessengerV2Only))
}

func TestMessengerV2OnlyCRC(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerV2OnlyCRC(t)
}
