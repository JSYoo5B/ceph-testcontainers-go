//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_recovery_bridge_peer

//ci: timeout=60m job-timeout=70 checker=recovery case=process-recovery-bridge-peer

package integration_test

import (
	"testing"
)

func TestMultiClusterCephFSOriginalProcessQuiescenceRecovery(t *testing.T) {
	t.Run("bridge", func(t *testing.T) {
		t.Run("peer", func(t *testing.T) { testCephFSOriginalProcessQuiescenceRecovery(t, false, "peer") })
	})
}
