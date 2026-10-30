//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_mirror_initial_daemons))))

//ci: timeout=150m job-timeout=160 image-roles=essential,client,cephfs,rbd-aux

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// Fresh pairs prove policy-only construction and explicit delayed process
// ownership, followed by independent source/destination client payload checks.
func TestMultiClusterNoInitialMirrorDaemons(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		t.Run(network, func(t *testing.T) {
			t.Run("rbd-snapshot-normal-first", func(t *testing.T) { harness.RunMirrorInitialRBD(t, host, false) })
			t.Run("rbd-journal-partial-first", func(t *testing.T) { harness.RunMirrorInitialRBD(t, host, true) })
			t.Run("cephfs-normal-first", func(t *testing.T) { harness.RunMirrorInitialCephFS(t, host) })
		})
	}
}
