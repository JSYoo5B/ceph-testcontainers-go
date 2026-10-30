//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_retained_snapshot))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// This executable recipe intentionally composes public Ceph commands for the
// native snapshot-retained state and custom volumes metadata. These are server
// provisioning operations; file I/O below is performed only by a Linux native
// client to verify the fixture. Raw commands must be fenced from outside users.
// After a raw retention/recreation, old typed handles never adopt a new UUID.
func TestCephFSRetainedSnapshotAndMetadataRecipe(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testCephFSRetainedSnapshotAndMetadataRecipe(t, host) })
	}
}
