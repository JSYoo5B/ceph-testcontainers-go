//go:build all || (integration && multicluster && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_namespaces))))

//ci: timeout=90m job-timeout=100

package integration_test

import (
	"testing"
)

// One owner and one receiving pool share their actual cohort across two named
// snapshot mappings and a named automatic-journal mapping, including topology
// changes and independent same-image-name client isolation controls.
func TestMultiClusterRBDNamespaceBinding(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, network := mode.Host, mode.Name
		t.Run(network, func(t *testing.T) { testRBDNamespaceBinding(t, host) })
	}
}
