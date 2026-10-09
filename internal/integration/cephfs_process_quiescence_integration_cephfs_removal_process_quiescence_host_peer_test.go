//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_quiescence_host_peer

//ci: timeout=60m job-timeout=70 checker=quiescence case=process-quiescence-host-peer

package integration_test

import (
	"testing"
)

func TestMultiClusterCephFSOriginalProcessQuiescence(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		t.Run("peer", func(t *testing.T) { testCephFSOriginalProcessQuiescence(t, true, "peer") })
	})
}
