//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// Fresh bridge/host fixtures verify MON/MGR bootstrap with no storage launches,
// followed by explicit storage provisioning. A separate partial first Add uses
// a generated-host/root collision to exercise existing registered ownership.
func TestNoInitialOSDTopology(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		if !t.Run(name, func(t *testing.T) {
			if !t.Run("bootstrap-and-storage", func(t *testing.T) { harness.RunNoInitialOSDStorage(t, host) }) {
				return
			}
			t.Run("partial-first-add-cleanup", func(t *testing.T) { harness.RunNoInitialOSDPartial(t, host) })
		}) {
			return
		}
	}
}
