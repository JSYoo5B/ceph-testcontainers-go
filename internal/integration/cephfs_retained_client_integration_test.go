//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_sessions))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestCephFSRetainedClientAcrossMDSRestart(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		var opts []testcontainers.ContainerCustomizer
		if mode.Host {
			opts = append(opts, ceph.WithHostNetwork())
		}
		t.Run(mode.Name, func(t *testing.T) { testCephFSRetainedClient(t, cephFSRestart, opts...) })
	}
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

// A client looks up a directory that a second, mounted client created; the
// MDS leases the dentry to it and recalls the lease when the second client
// renames and then removes the directory.
func TestCephFSDentryLeaseRecall(t *testing.T) {
	testCephFSDentryLeaseRecall(t)
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

// The MDS is killed while one cap flush waits for its ack; after its restart
// the client's flush is acknowledged and its size and bytes survive.
func TestCephFSFlushingCapReplay(t *testing.T) {
	testCephFSFlushingCapReplay(t)
}

// The client is killed while one rename is held unsafe; after the journal is
// released the MDS commits the request, evicts and blocklists the dead
// session, and another client finds the new name.
func TestCephFSClientKilledDuringUnsafeRequest(t *testing.T) {
	testCephFSClientKilledDuringUnsafeRequest(t)
}
