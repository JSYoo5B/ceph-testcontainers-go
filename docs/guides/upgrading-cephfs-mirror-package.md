# Upgrading CephFS mirror names

The CephFS mirror fixture moved from package `cephfs` to the service-specific
package `cephfs/mirror`. This source break targets the next minor release.
Published tags keep their original APIs; main adds no deprecated alias and no
forwarding layer. The RBD family moved earlier to `rbd/mirror` (see
[Upgrading RBD mirror names](upgrading-rbd-mirror-package.md)); the two share no
public type.

Import the new package with an alias, because the RBD mirror family is also
named `mirror`:

```golang
import cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
```

| Previous name | Current name |
| --- | --- |
| `cephfs.MirrorProcessQuiescenceAcknowledgment` | `cephfsmirror.ProcessQuiescenceAcknowledgment` |
| `cephfs.MirrorProcessQuiescenceStatus` | `cephfsmirror.ProcessQuiescenceStatus` |
| `cephfs.MirrorOriginalProcessStatus` | `cephfsmirror.OriginalProcessStatus` |
| `cephfs.MirrorProcessBindingStatus` | `cephfsmirror.ProcessBindingStatus` |
| `cephfs.MirrorDirectoryRemovalDaemonStatus` | `cephfsmirror.DirectoryRemovalDaemonStatus` |
| `cephfs.MirrorDirectoryRemovalStatus` | `cephfsmirror.DirectoryRemovalStatus` |
| `cephfs.MirrorDirectoryRemoval` | `cephfsmirror.DirectoryRemoval` |
| `cephfs.MirrorDirectoryAdditionStatus` | `cephfsmirror.DirectoryAdditionStatus` |
| `cephfs.MirrorDirectoryAddition` | `cephfsmirror.DirectoryAddition` |
| `cephfs.MirrorDirectoryStatus` | `cephfsmirror.DirectoryStatus` |
| `cephfs.MirrorSnapshot` | `cephfsmirror.Snapshot` |
| `cephfs.MirrorPeerRemovalDaemonStatus` | `cephfsmirror.PeerRemovalDaemonStatus` |
| `cephfs.MirrorPeerRemovalStatus` | `cephfsmirror.PeerRemovalStatus` |
| `cephfs.MirrorPeerRemoval` | `cephfsmirror.PeerRemoval` |
| `cephfs.MirrorConfig` | `cephfsmirror.Config` |
| `cephfs.MirrorDaemon` | `cephfsmirror.Daemon` |
| `cephfs.RunMirrorWithImages` | `cephfsmirror.RunWithImages` |
| `cephfs.RunMirror` | `cephfsmirror.Run` |
| `cephfs.Mirror` | `cephfsmirror.Mirror` |

All methods keep their names and receivers on the moved types: the
`Mirror` methods (`AddDirectory`, `RemoveDirectory`, `RebalanceDirectories`,
`DirectoryStatus`, `WaitDirectoryReady`, `WaitSnapshotSynced`, `BeginDirectoryAddition`,
`BeginDirectoryRemoval`, `BeginPeerRemoval`, `PeerIDs`, `RemovePeer`,
`RebootstrapPeer`, `AttachManagers`, `AddDaemon`, `RemoveDaemon`, `Daemons`,
`InterruptPeerLink`, `RefreshMonitorConfig`, `RefreshPeerMonitorConfig`,
`Terminate`), the removal and addition handles' `Status`, `WaitReleased`,
`WaitDrained`, `ProcessQuiescence` and `AcknowledgeProcessQuiescence`, and
`Daemon.Terminate` and `Daemon.ProcessObserverBindingStatus`. The embedded
container, field values, error kinds, filesystem and path identity checks,
generation and lifecycle ownership, locking, cancellation, original-process
observation, uncertain outcomes and retryable cleanup are unchanged, and so
are the test names and `//ci:` batch names of native scenarios.

Filesystem preparation (`cephfs.Run`, `cephfs.Start`, `cephfs.WithFilesystems`,
`cephfs.Filesystem` and the rest of the package) stays in `cephfs`. Change the
package qualifier and the names above; no call needs a different argument.

See the [CephFS mirror reference](../cephfs-mirror.md) and the
[package boundary record](../design/mirror-package-boundaries.md).
