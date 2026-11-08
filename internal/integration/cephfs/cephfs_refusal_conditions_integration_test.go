//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_sessions))))

//ci: timeout=25m job-timeout=35 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// A real libcephfs client plays the SUT against both filesystem refusal
// conditions on one filesystem. With max_file_size lowered, a write or
// truncate past the limit fails with EFBIG while smaller operations succeed;
// with new sessions refused, a new mount fails with EPERM while a session
// opened before keeps writing, but one that must reconnect to a failed-over
// MDS is dropped and blocklisted. The FSMap ID stays the original one, an
// outside edit is refused, and after Restore a new session mounts. A second
// holder keeps its session address and can write across failover without the
// refusal setting.
func TestCephFSRefusalConditions(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSRefusalConditions(t)
}
