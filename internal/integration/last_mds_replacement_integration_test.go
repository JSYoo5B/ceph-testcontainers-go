//go:build all || (integration && topology && (!ci || (ci_recovery && (!ci_batch || ci_batch_mds_replacements))))

//ci: timeout=50m job-timeout=60

package integration_test

import (
	"testing"
)

// Docker task ownership and logical native name/GID authority stay separate.
// This fixture never runs CLI/Python/socket/process observers in the MDS role.
func TestLastMDSReplacementTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		if !t.Run(network, func(t *testing.T) { testLastMDSReplacement(t, host) }) {
			return
		}
	}
}
