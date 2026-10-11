//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_quiescence_bridge_directory

//ci: timeout=60m job-timeout=70 checker=quiescence case=process-quiescence-bridge-directory

package integration_test

import (
	"testing"
)

func TestMultiClusterCephFSOriginalProcessQuiescence(t *testing.T) {
	t.Run("bridge", func(t *testing.T) {
		t.Run("directory", func(t *testing.T) { testCephFSOriginalProcessQuiescence(t, false, "directory") })
	})
}
