//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_clone_cancellation))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestCephFSCloneCancellationAndPartialCleanup(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSCloneCancellationAndPartialCleanup(t, host) })
	}
}
