//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_sessions))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestCephFSRetainedClientAcrossMDSRestart(t *testing.T) {
	t.Run("bridge", func(t *testing.T) { testCephFSRetainedClient(t, cephFSRestart) })
	t.Run("host", func(t *testing.T) { testCephFSRetainedClient(t, cephFSRestart, ceph.WithHostNetwork()) })
}

// A standby or the journal follower takes rank 0 while the client keeps its
// mount, session and open file.
func TestCephFSRetainedClientAcrossStandbyTakeover(t *testing.T) {
	t.Run("standby", func(t *testing.T) { testCephFSRetainedClient(t, cephFSStandbyTakeover) })
	t.Run("standby-replay", func(t *testing.T) { testCephFSRetainedClient(t, cephFSReplayTakeover) })
}
