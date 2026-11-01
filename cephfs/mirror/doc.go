// Package mirror mirrors CephFS snapshots of selected directories from a
// filesystem in one Ceph cluster to a filesystem in another, with native
// cephfs-mirror daemons: directory enrollment, peer management, MGR
// reassignment, process quiescence and typed readiness observation.
//
// Run starts the fixture for two existing clusters. Filesystems and data are
// prepared with the cephfs package. RBD mirroring is a separate fixture in the
// rbd/mirror package; the two services share no public interface.
//
// # Tracked changes
//
// AddDirectory, RemoveDirectory and RemovePeer only change Ceph's policy,
// which is enough when a test checks the effect on data. BeginDirectoryAddition,
// BeginDirectoryRemoval (then DirectoryRemoval.WaitReleased) and
// BeginPeerRemoval (then PeerRemoval.WaitDrained) return a receipt that keeps
// the original state and proves what happened, even after a lost reply. The
// receipts share these rules:
//   - Begin* returns (nil, err) when it refused during admission, before it
//     recorded a receipt. Once a receipt is recorded, every error returns it
//     with the error, whether or not the native request was sent: call the
//     same Begin* again with the same path or peer UUID and a fresh context;
//     the retry returns the same receipt, checks Ceph first
//     and does not resend an applied request. A directory remove that was
//     sent without an acknowledged reply is never sent again, by the receipt,
//     RemoveDirectory or RebalanceDirectories: a policy that still shows the
//     path mapped does not prove that the MGR dropped it. The retry then only
//     reports the native state, and completes once the path has left the
//     policy.
//   - While a receipt is pending, the fixture refuses changes that would
//     disturb it; Daemon.Remove and Terminate are always allowed.
//   - Query errors from Status and the waits are retryable. A changed
//     identity (cluster FSID, filesystem or metadata pool ID, peer UUID,
//     daemon process or session) is a permanent guard error for the receipt.
//   - Each Begin* is bounded to two minutes, each Status to 30 seconds and
//     each wait to two minutes, or the caller's earlier deadline. Receipts own
//     no cluster, container, directory, data or cleanup.
//
// A removal can finish only while the original daemons keep running. When a
// test stops, restarts or removes them first, the receipt stays pending and
// blocks the fixture until it is acknowledged: set
// Config.OriginalProcessClientFactory, check that every daemon's
// ProcessObserverBindingStatus is bound before Begin*, remove every owned
// daemon with Daemon.Remove (Stop or a restart is not enough), then call the
// receipt's AcknowledgeProcessQuiescence and continue with RebootstrapPeer or
// AddDirectory and AddDaemon. ProcessQuiescence observes the same evidence
// read-only. An acknowledgement does not set Drained or Released and resends
// no removal; a later failed attempt does not undo an earlier acceptance, and a
// receipt that already drained or released refuses one. The observer assumes a
// trusted Docker endpoint.
package mirror
