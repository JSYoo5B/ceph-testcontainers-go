//go:build (all || (integration && multicluster)) && (!ci || (ci_recovery && !ci_batch))

package integration_test

import (
	"testing"
)

// TestMultiClusterRBDReceiverReadiness verifies image-free scope discovery,
// exact owned election, topology changes and independent resumed payload I/O.
func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) { testRBDReceiverReadiness(t, host) })
	}
}
