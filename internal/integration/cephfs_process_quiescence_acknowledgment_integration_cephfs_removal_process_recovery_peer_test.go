//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_recovery_peer

//ci: timeout=60m job-timeout=70 checker=recovery case=peer

package integration_test

import (
	"testing"
)

func TestMultiClusterCephFSOriginalProcessQuiescenceRecovery(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) {
			t.Run("peer", func(t *testing.T) { testCephFSOriginalProcessQuiescenceRecovery(t, mode.Host, "peer") })
		})
	}
}
