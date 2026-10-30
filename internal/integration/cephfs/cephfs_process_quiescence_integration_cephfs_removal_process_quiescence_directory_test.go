//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_cephfs_removal_process_quiescence_directory

//ci: timeout=60m job-timeout=70 checker=quiescence case=directory image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestMultiClusterCephFSOriginalProcessQuiescence(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) {
			t.Run("directory", func(t *testing.T) { harness.RunCephFSOriginalProcessQuiescence(t, mode.Host, "directory") })
		})
	}
}
