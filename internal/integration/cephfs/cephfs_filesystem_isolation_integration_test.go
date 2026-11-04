//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Two filesystems on one cluster keep their authorization, data namespaces
// and MDS topology apart. A principal with r on iso-a and rw on iso-b reads
// both but creates only in iso-b, in either clause order and with the access
// swapped; a principal without an iso-b clause cannot mount it. A principal
// restricted to /dir1 that deletes a child still reads the child's bytes
// through an earlier /dir1 snapshot. Same-named namespace-isolated subvolumes
// in two groups get distinct RADOS namespaces, and each one's reader is
// refused the other's objects. Removing iso-a leaves iso-b with its FSMap ID,
// active and standby-replay GIDs and their affinity when Remove returns, and a
// fresh client reads iso-b's bytes.
func TestCephFSMultiFilesystemIsolation(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSMultiFilesystemIsolation(t)
}
