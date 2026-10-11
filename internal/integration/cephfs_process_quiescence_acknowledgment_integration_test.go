//go:build (all || (integration && multicluster)) && (!ci || (ci_recovery && !ci_batch))

package integration_test

import (
	"testing"
)

// Uses the original-process evidence helpers with independent fresh pairs.
func TestMultiClusterCephFSOriginalProcessQuiescenceRecovery(t *testing.T) {
	for _, host := range []bool{false, true} {
		mode := "bridge"
		if host {
			mode = "host"
		}
		t.Run(mode, func(t *testing.T) {
			for _, kind := range []string{"peer", "directory"} {
				t.Run(kind, func(t *testing.T) { testCephFSOriginalProcessQuiescenceRecovery(t, host, kind) })
			}
		})
	}
}
