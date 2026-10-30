//go:build (all || (integration && multicluster)) && (!ci || (ci_recovery && !ci_batch))

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Each receipt kind/network mode gets a fresh
// cluster pair, one path and one daemon. The same public receipt survives all
// three observations; no later peer/path generation contaminates its authority.
func TestMultiClusterCephFSOriginalProcessQuiescence(t *testing.T) {
	for _, host := range []bool{false, true} {
		mode := "bridge"
		if host {
			mode = "host"
		}
		t.Run(mode, func(t *testing.T) {
			for _, kind := range []string{"peer", "directory"} {
				t.Run(kind, func(t *testing.T) { harness.RunCephFSOriginalProcessQuiescence(t, host, kind) })
			}
		})
	}
}
