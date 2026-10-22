//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_rbd_receivers

//ci: timeout=90m job-timeout=100 checker=receivers image-roles=essential,client,rbd-aux

package integration_test

import (
	"testing"
)

func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) { testRBDReceiverReadiness(t, mode.Host) })
	}
}
