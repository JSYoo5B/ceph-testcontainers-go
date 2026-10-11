//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestCephFSSubvolumeSnapshotsAndClones(t *testing.T) {
	parallelWhenEnabled(t)
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSSubvolumeSnapshotsAndClones(t, host) })
	}
}
