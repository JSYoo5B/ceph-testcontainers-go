//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90

package integration_test

import (
	"testing"
)

// A cold filesystem retains its original storage and first-start ownership;
// the ordinary sibling is an independent live client/identity control.
func TestNoInitialMDSTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		if !t.Run(network, func(t *testing.T) { testNoInitialMDS(t, host) }) {
			return
		}
	}
}
