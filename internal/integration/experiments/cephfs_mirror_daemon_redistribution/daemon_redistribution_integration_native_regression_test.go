//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_mirror_daemon_redistribution_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Tentacle describes multiple mirror instances but still labels that deployment
// untested. This PoC checks native MGR assignment and userspace CephFS snapshot
// bytes instead of treating two running containers as proof of native HA.
func TestMultiClusterCephFSMirrorDaemonTopology(t *testing.T) {
	harness.RunCephFSMirrorDaemonRedistribution(t, false, harness.CephFSMirrorRedistribution{
		Reproduction: harness.StartReproduction(t, "cephfs_mirror_daemon_redistribution", redistributionObservation),
		Observation:  redistributionObservation, Judge: judgeRedistribution,
	})
}
