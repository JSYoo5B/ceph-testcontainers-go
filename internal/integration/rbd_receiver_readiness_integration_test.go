//go:build (all || (integration && multicluster)) && (!ci || (ci_recovery && !ci_batch))

package integration_test

import (
	"testing"
)

// TestMultiClusterRBDReceiverReadiness verifies image-free scope discovery,
// exact owned election, topology changes and independent resumed payload I/O.
func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) { testRBDReceiverReadiness(t, mode.Host) })
	}
}
