//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// Tentacle describes multiple mirror instances but still labels that deployment
// untested. This PoC checks native MGR assignment and userspace CephFS snapshot
// bytes instead of treating two running containers as proof of native HA.
func TestMultiClusterCephFSMirrorDaemonTopology(t *testing.T) {
	testCephFSMirrorDaemonTopology(t, false)
}
