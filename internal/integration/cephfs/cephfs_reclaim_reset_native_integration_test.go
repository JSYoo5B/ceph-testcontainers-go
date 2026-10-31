//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_clients))))

//ci: timeout=45m job-timeout=55 image-roles=essential,client,cephfs,rbd-aux,rgw

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A killed client's session and lock still stand; a fresh handle's RESET of a
// UUID that no session carries returns -ENOENT and touches nothing; a second
// fresh handle's state-preserving request (flags 0) for the killed client's
// UUID is refused by the MDS with -EINVAL and leaves its session and lock; a
// third fresh handle's RESET of that UUID removes the session and frees its
// lock before any replacement mounts, while an unrelated live client keeps
// its session and lock; the replacement, a new native client, reads the
// original bytes. The verdict is the Docker-free RESET judge's. No capability
// or lock is adopted: the MDS supports RESET only.
func TestCephFSReclaimReset(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunCephFSReclaimReset(t)
}
