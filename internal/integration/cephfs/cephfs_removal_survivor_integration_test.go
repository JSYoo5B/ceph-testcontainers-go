//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_admin))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// When Remove of iso-a returns, iso-b's daemons carry its FSMap ID as
// affinity again; its active and standby-replay GIDs are unchanged unless the
// monitor cleared that affinity during the removal, in which case iso-b
// settles to the same topology; a fresh client reads its bytes.
func TestCephFSRemovalKeepsSurvivorAffinity(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testCephFSRemovalSurvivor(t)
}
