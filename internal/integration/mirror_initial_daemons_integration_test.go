//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_mirror_initial_daemons))))

//ci: timeout=150m job-timeout=160

package integration_test

import (
	"testing"
)

// Fresh pairs prove policy-only construction and explicit delayed process
// ownership, followed by independent source/destination client payload checks.
func TestMultiClusterNoInitialMirrorDaemons(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			t.Run("rbd-snapshot-normal-first", func(t *testing.T) { testMirrorInitialRBD(t, host, false) })
			t.Run("rbd-journal-partial-first", func(t *testing.T) { testMirrorInitialRBD(t, host, true) })
			t.Run("cephfs-normal-first", func(t *testing.T) { testMirrorInitialCephFS(t, host) })
		})
	}
}
