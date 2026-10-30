//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_reclaim_reset))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
)

// A killed client's session and lock still stand; a fresh handle's RESET of a
// UUID that no session carries returns -ENOENT and touches nothing; a second
// fresh handle's RESET of the killed client's UUID removes that session and
// frees its lock before any replacement mounts, while an unrelated live
// client keeps its session and lock; the replacement, a new native client,
// reads the original bytes. The verdict is the Docker-free RESET judge's.
// This is RESET and eviction, not state-preserving reclaim.
func TestCephFSReclaimReset(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	harness.RunCephFSReclaimReset(t)
}
