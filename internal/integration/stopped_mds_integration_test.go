//go:build all || (integration && topology && (!ci || (ci_recovery && (!ci_batch || ci_batch_mds_replacements))))

//ci: timeout=50m job-timeout=60

package integration_test

import (
	"testing"
)

// Docker CID ownership and native name/GID observations are independent. This
// fixture does not infer a native GID from an MDS-role process or socket.
func TestStoppedMDSRetirementTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		if !t.Run(network, func(t *testing.T) { testStoppedMDSRetirement(t, host) }) {
			return
		}
	}
}
