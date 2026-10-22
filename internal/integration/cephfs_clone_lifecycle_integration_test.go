//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_clone_cancellation))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package integration_test

import (
	"testing"
)

func TestCephFSCloneCancellationAndPartialCleanup(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testCephFSCloneCancellationAndPartialCleanup(t, host) })
	}
}
