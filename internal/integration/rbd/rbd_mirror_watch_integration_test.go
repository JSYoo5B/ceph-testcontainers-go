//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_rbd_namespace_observation))))

//ci: timeout=90m job-timeout=100 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The receiver and image watches observe the elected receiver's stop, the
// survivor's election and replay, and the same container's restart, while
// the destination data independently proves the replay that followed.
func TestMultiClusterRBDMirrorWatch(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		if !t.Run(network, func(t *testing.T) { testRBDMirrorWatch(t, host) }) {
			return // A failed fixture must not start the next network.
		}
	}
}
