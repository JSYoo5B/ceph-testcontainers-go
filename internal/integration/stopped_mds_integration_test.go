//go:build all || (integration && topology && (!ci || (ci_recovery && (!ci_batch || ci_batch_mds_replacements))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs

package integration_test

import (
	"testing"
)

// Docker CID ownership and native name/GID observations are independent. This
// fixture does not infer a native GID from an MDS-role process or socket.
func TestStoppedMDSRetirementTopology(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		if !t.Run(network, func(t *testing.T) { testStoppedMDSRetirement(t, host) }) {
			return
		}
	}
}
