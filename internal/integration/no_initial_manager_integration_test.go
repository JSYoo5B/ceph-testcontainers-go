//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Independent cold-MGR and combined MON-only fixtures exercise first management
// provisioning on the original cluster, including real client data before MGR.
func TestNoInitialManagerTopology(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
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
