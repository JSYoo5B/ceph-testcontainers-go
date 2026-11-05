//go:build all || (integration && features && (!ci || (ci_topology && (!ci_batch || ci_batch_cephfs_authorization_isolation))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs

package cephfs_authorization_isolation_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Two filesystems on one cluster keep their authorization and data
// namespaces apart. A principal with r on iso-a and rw on iso-b reads both but
// creates only in iso-b, in either clause order and with the access swapped;
// a principal without an iso-b clause cannot mount it. A principal restricted
// to /dir1 that deletes a child still reads the child's bytes through an
// earlier /dir1 snapshot. Same-named namespace-isolated subvolumes in two
// groups get distinct RADOS namespaces, and each one's reader is refused the
// other's objects.
func TestCephFSAuthorizationIsolation(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSAuthorizationIsolation(t)
}
