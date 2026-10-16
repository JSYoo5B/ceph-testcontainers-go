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

// With two active ranks, the standby takes over rank 1, where the client's
// file lives in a pinned directory, while rank 0 keeps its daemon.
func TestCephFSRetainedClientAcrossRankOneTakeover(t *testing.T) {
	testCephFSRetainedClient(t, cephFSRankOneTakeover)
}

// A second client writes a file that a live client holds; the MDS revokes the
// holder's capabilities without evicting it.
func TestCephFSCapabilityHandoff(t *testing.T) {
	testCephFSCapabilityHandoff(t)
}

// A client that is frozen while its MDS fails over misses the reconnect window
// and is evicted.
func TestCephFSLateReconnect(t *testing.T) {
	testCephFSLateReconnect(t)
}

// With two active ranks, a directory moves from rank 0 to rank 1 and back
// while a client streams fsynced records into a file in it.
func TestCephFSLiveRankMigration(t *testing.T) {
	testCephFSLiveRankMigration(t)
}

// One rename is held between its unsafe and safe replies, and observed from
// the MDS and from the client.
func TestCephFSUnsafeRequestWindow(t *testing.T) {
	testCephFSUnsafeRequestWindow(t)
}

// The MDS is killed while one rename is held unsafe; after its restart the
// client replays the request and it becomes safe.
func TestCephFSUnsafeRequestReplay(t *testing.T) {
	testCephFSUnsafeRequestReplay(t)
}
