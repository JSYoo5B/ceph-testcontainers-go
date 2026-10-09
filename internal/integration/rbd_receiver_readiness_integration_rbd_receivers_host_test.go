//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_rbd_receivers_host

//ci: timeout=90m job-timeout=100 checker=receivers case=host

package integration_test

import (
	"testing"
)

func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	t.Run("host", func(t *testing.T) { testRBDReceiverReadiness(t, true) })
}
