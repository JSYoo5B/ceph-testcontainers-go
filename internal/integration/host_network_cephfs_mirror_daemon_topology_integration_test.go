//go:build (all || (integration && hostnetwork && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

func TestHostNetworkCephFSMirrorDaemonTopology(t *testing.T) {
	testCephFSMirrorDaemonTopology(t, true)
}
