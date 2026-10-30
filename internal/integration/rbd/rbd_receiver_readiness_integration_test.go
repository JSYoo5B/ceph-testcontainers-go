//go:build (all || (integration && multicluster)) && (!ci || (ci_recovery && !ci_batch))

package rbd_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// TestMultiClusterRBDReceiverReadiness verifies image-free scope discovery,
// exact owned election, topology changes and independent resumed payload I/O.
func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) { harness.RunRBDReceiverReadiness(t, mode.Host) })
	}
}
