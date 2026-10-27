# Upgrading RBD mirror names

The RBD mirror fixture moved from package `rbd` to the service-specific package
`rbd/mirror`. This source break targets the next minor release. Published tags
keep their original APIs; main adds no deprecated alias and no forwarding layer.
CephFS mirroring stays in `cephfs` for now and is not a shared type of this
package.

Import the new package with an alias, because the CephFS mirror family is also
named `mirror`:

```golang
import rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
```

| Previous name | Current name |
| --- | --- |
| `rbd.Mirror` | `rbdmirror.Mirror` |
| `rbd.MirrorConfig` | `rbdmirror.Config` |
| `rbd.MirrorMode` | `rbdmirror.Mode` |
| `rbd.MirrorModeJournal` | `rbdmirror.ModeJournal` |
| `rbd.MirrorModeSnapshot` | `rbdmirror.ModeSnapshot` |
| `rbd.MirrorScope` | `rbdmirror.Scope` |
| `rbd.MirrorScopeImage` | `rbdmirror.ScopeImage` |
| `rbd.MirrorScopePool` | `rbdmirror.ScopePool` |
| `rbd.MirrorDaemon` | `rbdmirror.Daemon` |
| `rbd.MirrorDaemonStatus` | `rbdmirror.DaemonStatus` |
| `rbd.MirrorPoolReplayerStatus` | `rbdmirror.PoolReplayerStatus` |
| `rbd.MirrorImageStatus` | `rbdmirror.ImageStatus` |
| `rbd.MirrorNamespace` | `rbdmirror.Namespace` |
| `rbd.MirrorNamespaceState` | `rbdmirror.NamespaceState` |
| `rbd.MirrorPolicies` | `rbdmirror.Policies` |
| `rbd.MirrorReceiverStatus` | `rbdmirror.ReceiverStatus` |
| `rbd.MirrorReceiverDaemonStatus` | `rbdmirror.ReceiverDaemonStatus` |
| `rbd.RunMirror` | `rbdmirror.Run` |
| `rbd.RunMirrorWithImages` | `rbdmirror.RunWithImages` |

All methods keep their names and receivers on the moved types:
`Mirror.{EnableImage, ImageStatus, WaitReplayReady, PolicyStatus, ReceiverStatus,
WaitReceiverReady, BindNamespace, AddDaemon, RemoveDaemon, Daemons, Rebootstrap,
RefreshMonitorConfig, InterruptPeerLink, SourceRBD, DestinationRBD, Terminate}`,
`Namespace.{ImageStatus, WaitReplayReady, ReceiverStatus, WaitReceiverReady}`
and `Daemon.{Status, Terminate}`. The embedded container, field values, error
kinds, image admission, scoped identity, locking, cancellation and cleanup
behavior are unchanged, and so are the test names and `//ci:` batch names of
native scenarios.

Pool and namespace preparation (`rbd.Run`, `rbd.WithPools`, `rbd.InitPool`,
`rbd.CreateNamespace`, `rbd.ListNamespaces`, `rbd.RemoveNamespace`) stays in
`rbd`. Change the package qualifier and the names above; no call needs a
different argument.

See the [RBD mirror reference](../rbd-mirror.md) and the
[package boundary record](../design/mirror-package-boundaries.md).
