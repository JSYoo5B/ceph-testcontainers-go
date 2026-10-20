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

// One native client runs a fixed sequence of metadata operations, with the
// errno and native values of each, and a second client checks the result.
func TestCephFSMetadataRecipe(t *testing.T) {
	testCephFSMetadataRecipe(t)
}

// One client's discovery, session open, renewals and close, a path-restricted
// identity admitted and refused, and a required client feature that evicts
// and refuses a client without it.
func TestCephFSSessionAdmission(t *testing.T) {
	testCephFSSessionAdmission(t)
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

// A live client with a buffered write is evicted: the MDS's OSDMap epoch
// barrier is the epoch that blocklists the client, the client drops its
// dirty data and fails with ESHUTDOWN, and a second live client receives the
// barrier and reads only the fsynced bytes.
func TestCephFSBlocklistEpochBarrier(t *testing.T) {
	testCephFSBlocklistEpochBarrier(t)
}

// The OSDs of a live client's file fill up: its write fails with ENOSPC
// while metadata calls succeed, and after the full ratios are restored the
// same fd writes again and another client reads every byte.
func TestCephFSOSDFullRetainedClient(t *testing.T) {
	testCephFSOSDFullRetainedClient(t)
}

// Two live clients contend for one flock: the second gets EWOULDBLOCK while
// the first holds it, waits in the MDS with a blocking lock, and gets the
// lock when the first unlocks, without an eviction.
func TestCephFSFlockContention(t *testing.T) {
	testCephFSFlockContention(t)
}
