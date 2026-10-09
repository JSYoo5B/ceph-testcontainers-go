//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_directory_release))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// Exercise actual public admission, native removal and original live sessions.
// A real successful remove loses one reply; no private identity seam is used.
func TestMultiClusterCephFSDirectoryRemovalRelease(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSDirectoryRemovalRelease(t, host) })
	}
}
