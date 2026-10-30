//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90 image-roles=essential,client,cephfs,rgw

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A cold filesystem retains its original storage and first-start ownership;
// the ordinary sibling is an independent live client/identity control.
func TestNoInitialMDSTopology(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		if !t.Run(network, func(t *testing.T) { testNoInitialMDS(t, host) }) {
			return
		}
	}
}
