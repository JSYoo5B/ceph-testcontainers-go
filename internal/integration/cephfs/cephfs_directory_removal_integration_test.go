//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_directory_release))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Exercise actual public admission, native removal and original live sessions.
// A real successful remove loses one reply; no private identity seam is used.
func TestMultiClusterCephFSDirectoryRemovalRelease(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testCephFSDirectoryRemovalRelease(t, host) })
	}
}
