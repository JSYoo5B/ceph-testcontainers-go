//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestCephFSMDSScaleTopology(t *testing.T) {
	testCephFSMDSScaleTopology(t, false)
}

func TestCephFSMDSScaleStandbyReplayTopology(t *testing.T) {
	testCephFSMDSScaleTopology(t, true)
}
