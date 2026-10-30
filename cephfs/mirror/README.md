# CephFS mirror

Import `github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror` to mirror
snapshots of selected directories from a filesystem in one disposable cluster
to a filesystem in another with native `cephfs-mirror` daemons, so that a test
can check how its application behaves with snapshot replication: a checkpoint
arriving on the other side, directory reassignment when a daemon is lost, peer
replacement. The clusters, filesystems and data stay in the
[cephfs package](../README.md). See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

Import it with an alias, because the RBD family also uses the package name
`mirror`:

```golang
import cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
```

## Order of use

1. **Start two clusters with filesystems** (`cephfs.Run`, or `ceph.Run` with
   `cephfs.WithFilesystems`).
2. **Start the mirror** with `cephfsmirror.Run(ctx, img,
   cephfsmirror.WithConfig(cfg))`. `img` is the daemon image (usually
   `FactsOf(source).Images.CephFS`); `cfg` names the two clusters (the embedded
   `*ceph.Container`), the two filesystems and the `Directories` to mirror.
   Mirroring is one way with one peer per source filesystem.
3. **Run the SUT** on the source and take a snapshot of a mirrored directory
   (`mkdir <dir>/.snap/<name>` through the client) as a checkpoint.
4. **Observe and verify**: `WaitDirectoryReady` (a live daemon owns the
   directory), `WaitSnapshotSynced` with the source snapshot's ID and name, then
   compare the destination data. `DirectoryStatus`, `PeerIDs` and the daemon
   handles show the current state.
5. **Change the topology** when the scenario needs it: `AddDaemon`,
   `Daemon.Remove`, `RebalanceDirectories`, `Daemon.InterruptPeerLink`,
   `RemovePeer` / `RebootstrapPeer`, or the tracked calls `BeginDirectoryAddition`,
   `BeginDirectoryRemoval` and `BeginPeerRemoval`, which prove what the daemons
   did even after a lost reply.
6. **Clean up**: register the clusters' cleanup first and the mirror's after,
   so the mirror is terminated first; terminate a non-nil `*Mirror` even after
   an error.

```golang
facts, err := ceph.FactsOf(source.Container)
if err != nil {
	t.Fatal(err)
}
mirror, err := cephfsmirror.Run(ctx, facts.Images.CephFS, cephfsmirror.WithConfig(cephfsmirror.Config{
	Source:                source.Container,
	Destination:           destination.Container,
	SourceFilesystem:      sourceFS.FilesystemName,
	DestinationFilesystem: destinationFS.FilesystemName,
	Directories:           []string{"/data"},
}))
testcontainers.CleanupContainer(t, mirror)
if err != nil {
	t.Fatal(err)
}
if _, err := mirror.WaitDirectoryReady(ctx, "/data"); err != nil {
	t.Fatal(err)
}
```

## Concepts and limits

- The mirror owns only its daemon containers and the network attachments it
  adds. `Terminate` leaves the Ceph principals, the peer, directory policies and
  all data in the clusters.
- One way, one peer per source filesystem: several daemons share one peer and
  do not fan out. Active-active writes and automatic site failover are not
  provided. Both clusters must use the same network mode;
  `Daemon.InterruptPeerLink` needs bridge networking.
- Ceph's MGR assigns directories to daemons. On Ceph 20.2.4 it does not move
  assigned directories when a daemon joins: call `RebalanceDirectories`. The
  MGR's mirroring module can hang for good when a directory is removed while
  another assignment is changing; `RebalanceDirectories` waits for an idle
  policy, plain removals do not, and `ceph mgr fail` recovers.
- Plain calls only change Ceph's policy; tracked calls return receipts whose
  `Released` or `Drained` proves the daemons' side. Neither proves remote
  unlock, checkpoint completion or unchanged destination metadata. A tracked
  removal interrupted by stopping the original daemons needs the process
  observer (`Config.OriginalProcessClientFactory`) to be acknowledged; the
  procedure is in the package documentation.
- `WaitSnapshotSynced` waits for one exact snapshot and times out if a newer
  one is synced first. Native mirroring is not guaranteed to reproduce
  `user.*` extended attributes.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror`.
- Mirror policy and daemon admin-socket commands:
  [Native commands](../../docs/guides/native-commands.md#cephfs-state-and-settings);
  [Fault injection](../../docs/guides/fault-injection.md);
  [Releases and images](../../docs/guides/releases-and-images.md).
