//go:build all || (integration && topology && (!ci || (ci_recovery && (!ci_batch || ci_batch_mds_replacements))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Docker task ownership and logical native name/GID authority stay separate.
// This fixture never runs CLI/Python/socket/process observers in the MDS role.
func TestLastMDSReplacementTopology(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		if !t.Run(network, func(t *testing.T) { testLastMDSReplacement(t, host) }) {
			return
		}
	}
}
