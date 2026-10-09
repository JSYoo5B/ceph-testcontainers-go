//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90

package integration_test

import (
	"testing"
)

// Independent cold-MGR and combined MON-only fixtures exercise first management
// provisioning on the original cluster, including real client data before MGR.
func TestNoInitialManagerTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		if !t.Run(name, func(t *testing.T) {
			if !t.Run("storage-before-first-manager", func(t *testing.T) { testNoInitialManager(t, host, false) }) {
				return
			}
			t.Run("mon-before-management-and-storage", func(t *testing.T) { testNoInitialManager(t, host, true) })
		}) {
			return
		}
	}
}
