//go:build all || (integration && multicluster && (!ci || (ci_recovery && (!ci_batch || ci_batch_cephfs_removal_directory_intent))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// Two independent pairs cover explicit zero-daemon policy construction, actual
// applied response loss, separate unapplied dispatch loss, and real client I/O.
func TestMultiClusterCephFSDirectoryAdditionIntent(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, mode := mode.Host, mode.Name
		t.Run(mode, func(t *testing.T) { harness.RunCephFSDirectoryAdditionIntent(t, host) })
	}
}
