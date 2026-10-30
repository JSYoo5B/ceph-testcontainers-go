//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_rbd_namespace_observation))))

//ci: timeout=90m job-timeout=100 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// Scoped image observations share one original owner across mixed native modes.
// Data/checkpoint delivery is independently checked after readiness; no image
// observer creates a policy, process, checkpoint or new cleanup owner.
func TestMultiClusterRBDNamespaceImageObservation(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		if !t.Run(network, func(t *testing.T) { testRBDNamespaceImageObservation(t, host) }) {
			return // A failed fixture must not start the next network.
		}
	}
}
