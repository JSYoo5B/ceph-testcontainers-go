//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_directory_intent))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"testing"
)

// Two independent pairs cover explicit zero-daemon policy construction, actual
// applied response loss, separate unapplied dispatch loss, and real client I/O.
func TestMultiClusterCephFSDirectoryAdditionIntent(t *testing.T) {
	for _, host := range []bool{false, true} {
		mode := "bridge"
		if host {
			mode = "host"
		}
		t.Run(mode, func(t *testing.T) { testCephFSDirectoryAdditionIntent(t, host) })
	}
}
