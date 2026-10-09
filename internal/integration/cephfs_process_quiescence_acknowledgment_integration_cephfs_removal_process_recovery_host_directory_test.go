//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_recovery_host_directory

//ci: timeout=60m job-timeout=70 checker=recovery case=process-recovery-host-directory

package integration_test

import (
	"testing"
)

func TestMultiClusterCephFSOriginalProcessQuiescenceRecovery(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		t.Run("directory", func(t *testing.T) { testCephFSOriginalProcessQuiescenceRecovery(t, true, "directory") })
	})
}
