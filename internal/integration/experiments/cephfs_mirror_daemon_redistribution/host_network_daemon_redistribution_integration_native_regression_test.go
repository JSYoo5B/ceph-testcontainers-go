//go:build (all || (integration && hostnetwork && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_mirror_daemon_redistribution_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestHostNetworkCephFSMirrorDaemonTopology(t *testing.T) {
	harness.RunCephFSMirrorDaemonRedistribution(t, true, harness.CephFSMirrorRedistribution{
		Reproduction: harness.StartReproduction(t, "cephfs_mirror_daemon_redistribution/host-network", redistributionObservation).Network("host"),
		Observation:  redistributionObservation, Judge: judgeRedistribution,
	})
}
