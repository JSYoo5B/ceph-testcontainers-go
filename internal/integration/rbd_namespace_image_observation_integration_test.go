//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_rbd_namespace_observation))))

//ci: timeout=90m job-timeout=100

package integration_test

import (
	"testing"
)

// Scoped image observations share one original owner across mixed native modes.
// Data/checkpoint delivery is independently checked after readiness; no image
// observer creates a policy, process, checkpoint or new cleanup owner.
func TestMultiClusterRBDNamespaceImageObservation(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		if !t.Run(network, func(t *testing.T) { testRBDNamespaceImageObservation(t, host) }) {
			return // A failed fixture must not start the next network.
		}
	}
}
