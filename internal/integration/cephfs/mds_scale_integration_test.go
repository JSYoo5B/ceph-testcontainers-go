//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestCephFSMDSScaleTopology(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSMDSScaleTopology(t, false)
}

func TestCephFSMDSScaleStandbyReplayTopology(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSMDSScaleTopology(t, true)
}
