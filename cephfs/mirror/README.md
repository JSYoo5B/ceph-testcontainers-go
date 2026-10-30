# CephFS mirror

## Introduction

The `cephfs/mirror` package mirrors snapshots of selected directories from a
filesystem in one Ceph cluster to a filesystem in another, with a native
`cephfs-mirror` daemon. The clusters, filesystems and data stay in the
[cephfs package](../README.md); RBD mirroring is a separate fixture in
`rbd/mirror` with its own semantics. The two services share no public mirror
interface.

Import it with an alias, because the RBD family also uses the package name
`mirror`:

```golang
import cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
```

`cephfsmirror.Run` takes the clusters as `*ceph.Container`, so it works with a
cluster from `cephfs.Run` or `ceph.Run`. Pass the embedded value of a
`*cephfs.Container`, for example `cluster.Container`.

## Module Reference

### Snapshot mirroring

CephFS snapshot mirroring copies snapshots of selected directories from a source
filesystem to a destination filesystem in another cluster. A `cephfs-mirror`
daemon reads the source and writes the destination; Ceph's MGR `mirroring`
module assigns directories to daemons. Mirroring is one-way and Ceph supports
one peer per source filesystem.

The two clusters, their filesystems and their data belong to the caller. The
mirror fixture owns only its daemon containers and the network attachments it
adds. Register the clusters' cleanup first and the mirror's cleanup after them,
so the mirror is terminated first.

#### Run

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Mirror, error)
func WithConfig(config Config) Option
func (Option) Customize(*testcontainers.GenericContainerRequest) error
```

Configures one-way snapshot mirroring between two existing filesystems and
starts the initial daemons. It:

1. enables the MGR `mirroring` module on both clusters and waits for it;
2. in bridge mode, connects every source MGR candidate to the destination
   network (the source MGR checks the destination filesystem when the peer is
   imported);
3. creates the source principal `client.tc-cephfs-mirror-<suffix>` and
   authorizes `client.tc-cephfs-peer-<suffix>` with `rwps` on `/` of the
   destination filesystem;
4. enables snapshot mirroring on the source filesystem and imports a peer
   bootstrap token from the destination;
5. starts the daemons (named `a` through `z`, then `node-27`, ...), waits for
   them to register, then registers `Directories`.

`img` is the mirror daemon image and is required: it must contain
`cephfs-mirror` and the `ceph` CLI (the cephfs role or `all`), usually
`FactsOf(source).Images.CephFS`. The `Config` comes from exactly one
`WithConfig` among the options. `Run` refuses an image that a cluster selects for
another role, such as its essential image, when that cluster has a separate
cephfs image. The setup commands run in each cluster's client toolkit. The other customizers apply to every daemon, including daemons
added later. Both clusters must use the same network mode. In bridge mode each
daemon joins the destination network; in host mode no attachment is needed.

A non-nil result returned with an error must be terminated. Terminating the
mirror leaves Ceph auth entities, the peer and directory policies, and all data
in the caller's clusters.

```golang
sourceFacts, err := ceph.FactsOf(source)
mirror, err := cephfsmirror.Run(ctx, sourceFacts.Images.CephFS, cephfsmirror.WithConfig(cephfsmirror.Config{
	Source:                source.Container,
	Destination:           destination.Container,
	SourceFilesystem:      sourceFS.FilesystemName,
	DestinationFilesystem: destinationFS.FilesystemName,
	Directories:           []string{"/data"},
}))
if mirror != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := mirror.Terminate(ctx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}
```

#### Config

| Field | Meaning |
| --- | --- |
| `Source`, `Destination` | Existing clusters with independent FSIDs. |
| `SourceFilesystem`, `DestinationFilesystem` | Existing filesystem names. |
| `DestinationSite` | Site name recorded in the peer. Default `destination`; no `@`, `/` or whitespace. |
| `Directories` | At least one absolute, non-overlapping path relative to the filesystem root (not a container mount point). Registration does not create the directory. |
| `DaemonCount` | Initial daemons. Zero means one; negative values are rejected. |
| `NoInitialDaemons` | Configure the peer and directories without starting a daemon. `DaemonCount` must be zero. Start daemons later with `AddDaemon`. |
| `OriginalProcessClientFactory` | Optional `func(context.Context) (*client.Client, error)` returning a new raw Docker client (`github.com/moby/moby/client`). It enables process observation for interrupted removals; see [Recovering from an interrupted removal](#recovering-from-an-interrupted-removal). |

With `NoInitialDaemons`, no mirror container is created, the embedded container
is nil and `Daemons()` is empty. Directory policies can still be registered, but
directory readiness and snapshot waits stay unready until a daemon is added. The
first daemon added becomes the embedded compatibility container.

#### Mirror

`*cephfsmirror.Mirror` has `SourceFilesystem`, `DestinationFilesystem`,
`SourceClientEntity`, `DestinationClientEntity` (the Cephx entities the fixture
created on each side) and `Directories` (the paths registered through the
fixture). Its embedded container is the initial daemon; it becomes nil once that
daemon is removed and is not reassigned. Use `Daemons()` for the current
membership.

#### Daemon

`*cephfsmirror.Daemon` is one owned `cephfs-mirror` process. `DaemonName` is its
stable fixture name; Ceph identifies the process by its client GID. All daemons
share the source principal and the peer. The embedded container provides `Stop`
and `Start`.

### Mirror Methods: directories

#### AddDirectory

```golang
func (mirror *Mirror) AddDirectory(ctx context.Context, directory string) error
```

Registers an absolute path for snapshot mirroring. Ceph rejects paths that
overlap a registered path. Registration does not create the directory. Create
the directory and its data with a client first. To survive a lost reply, use
`BeginDirectoryAddition` instead.

#### RemoveDirectory

```golang
func (mirror *Mirror) RemoveDirectory(ctx context.Context, directory string) error
```

Stops mirroring a path. Source and destination data and existing snapshots are
kept. Success means Ceph accepted the policy change; daemons react
asynchronously and a sync in progress can still finish. To prove that the
daemons have released the directory, use `BeginDirectoryRemoval`. If a tracked
removal of the same path is pending, `RemoveDirectory` retries that removal.

#### DirectoryStatus

```golang
func (mirror *Mirror) DirectoryStatus(ctx context.Context, directory string) (DirectoryStatus, error)
```

Reads the current state of an owned directory policy, bounded to 30 seconds.
It never rebalances or changes policy. `DirectoryStatus` fields:

| Field | Meaning |
| --- | --- |
| `SourceFilesystem`, `DestinationFilesystem`, `SourceFilesystemID`, `DestinationFilesystemID` | Original filesystem names and IDs. |
| `Directory`, `PeerID` | The path and the current peer UUID. |
| `MappingState`, `InstanceID`, `DaemonName` | MGR assignment state, the assigned daemon's native instance and its fixture name. |
| `State`, `FailureReason` | The assigned daemon's replay state: `idle`, `syncing` or `failed`. |
| `CurrentSnapshot`, `LastSyncedSnapshot` | `*Snapshot` being synced and last synced, each with the source snapshot `ID` and `Name`. |
| `SnapshotsSynced`, `SnapshotsDeleted`, `SnapsRenamed` | Native counters. They restart at zero when the daemon restarts or the directory moves to another daemon. |
| `DaemonProblems` | Other owned daemons that are stopped or unavailable, by name. |
| `Ready` | The directory is assigned to a running owned daemon with the expected peer and is `idle` or `syncing`. |

`Ready` does not mean every daemon is healthy, data is equal or a checkpoint has
arrived. When another member is stopped, `DirectoryStatus` returns the status
together with a partial error and fills `DaemonProblems`, even if this
directory's daemon is ready.

#### WaitDirectoryReady

```golang
func (mirror *Mirror) WaitDirectoryReady(ctx context.Context, directory string) (DirectoryStatus, error)
```

Polls `DirectoryStatus` until `Ready`, for at most four minutes or the caller's
deadline. Problems of other members are reported in `DaemonProblems` but do not
block this directory. On timeout the last status and the last query error are
returned (`errors.Is(err, context.DeadlineExceeded)` works). A changed
filesystem or peer identity fails at once; if you recreate the peer, start a new
wait.

#### WaitSnapshotSynced

```golang
func (mirror *Mirror) WaitSnapshotSynced(ctx context.Context, directory string, checkpoint Snapshot) (DirectoryStatus, error)
```

Waits, for at most four minutes, until the daemon assigned to the directory
reports exactly `checkpoint` (source snapshot `ID` and `Name`) as its last
synced snapshot. Read the ID on the source, for example with libcephfs
`snap_info('/data/.snap/<name>')['id']`. The destination's own snapshot ID is a
different number, and taking the expected value from `LastSyncedSnapshot` would
not test anything.

The wait is exact. If a newer snapshot becomes the last synced one before the
wait observes the checkpoint, the wait times out, so do not take new snapshots
of the directory while waiting. Success is a point-in-time report from the
daemon; verify the destination snapshot's content with a destination client.

#### RebalanceDirectories

```golang
func (mirror *Mirror) RebalanceDirectories(ctx context.Context) error
```

Spreads this fixture's directories over its running daemons. It unregisters the
owned paths, waits until the MGR has released their assignments, and registers
them again. Mirroring of these paths pauses meanwhile; files, snapshots, the
peer and auth entities are kept. Every member must be running, so remove
stopped members first. Policies added outside the fixture and other filesystems
are untouched. After a partial failure, retry with a fresh context.

On Ceph 20.2.4 the MGR does not move already assigned directories when a daemon
joins (it logs `'DirectoryState' object is not subscriptable`). Call
`RebalanceDirectories` after `AddDaemon` when the test needs the new daemon to
take work. Failover from a dead daemon to a survivor works without it.

It removes one path at a time, and only while no tracked directory has an
assignment change in flight: `fs snapshot mirror dirmap` reports each as
`mapped`, or as `stalled` while no daemon runs. The MGR's mirroring module
holds its policy lock while a removal waits for a RADOS write, and the
acknowledgement of an earlier release, which needs that lock, runs on the same
librados callback strand as the write's completion. Removals sent back to back
can therefore hang the module for good: with Ceph 20.2.4 and 19.2.5,
`fs snapshot mirror remove`, `ls`, `dirmap` and `show distribution` for the
filesystem stop answering until the active MGR restarts, for example after
`ceph mgr fail`.

This ordering closes the window that the rebalance's own removals would open.
It cannot fence what happens outside the fixture while a removal waits: a
mirror daemon that joins, restarts or times out, or another client that changes
directories at the same time, can still trigger the same hang. `RemoveDirectory` and
`BeginDirectoryRemoval` do not wait for an idle policy, so when you remove
directories yourself, wait until the others report `mapped` first.

### Mirror Methods: daemons

#### Daemons

```golang
func (mirror *Mirror) Daemons() []*Daemon
```

Returns a copy of the owned daemon list sorted by name, including stopped and
partially started daemons.

#### AddDaemon

```golang
func (mirror *Mirror) AddDaemon(ctx context.Context, daemonName string, opts ...testcontainers.ContainerCustomizer) (*Daemon, error)
```

Starts another daemon with the fixture's credentials, peer and directories.
Names have 1 to 63 letters, digits, `.`, `_` or `-`. The constructor's
customizers apply first, then `opts`. A non-nil partial result stays owned; remove
it with `Daemon.Remove` or `Terminate`. Ceph discovers the daemon and assigns
directories asynchronously (see `RebalanceDirectories`). Ceph's Tentacle
documentation describes assignment and failover across several daemons but marks
multiple mirror daemons as untested upstream.

#### Daemon.Remove

```golang
func (daemon *Daemon) Remove(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Terminates this daemon and removes it from its mirror. Removing the last
daemon is allowed and models a complete replication outage; `AddDaemon` resumes
with the same auth, peer and directories. Data is preserved. If termination
fails, the daemon stays listed so the call can be retried. A daemon that its
mirror no longer lists, or one whose mirror was terminated, is refused.

#### Daemon.Terminate

```golang
func (daemon *Daemon) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes the daemon's container without changing Ceph policy or fixture
membership. Prefer `Remove`, which also updates membership; interrupted
removal recovery requires `Remove`.

#### Daemon.ProcessObserverBindingStatus

```golang
func (d *Daemon) ProcessObserverBindingStatus() ProcessBindingStatus
```

Reports whether this daemon has the optional process observer configured by
`OriginalProcessClientFactory`. `ProcessBindingStatus` has `Available`,
`State`, `EngineID` and `ContainerID`. `State` is one of `disabled`, `bound`,
`partial-startup`, `context-unavailable`, `factory-error`, `empty-client`,
`positive-binding-failed` or `closed`. It is a local value and does not contact
Docker.

#### Daemon.InterruptPeerLink

```golang
func (daemon *Daemon) InterruptPeerLink(ctx context.Context) (*fault.NetworkInterruption, error)
```

Disconnects this daemon from the destination network while it stays connected to
the source and keeps its MGR registration. The daemon is not stopped or
reassigned, and policies are unchanged. Restore the returned handle to let
synchronization resume:

```golang
cut, err := daemon.InterruptPeerLink(ctx)
if cut != nil {
	defer cut.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// ... snapshots taken now do not arrive ...
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

Keep a non-nil handle returned with an error: a transport error cannot prove
that Docker did not disconnect the endpoint. The fixture also restores it on
`Terminate`. Host networking has no bridge endpoint to cut, so the call is
refused there.

### Mirror Methods: peer and connectivity

#### PeerIDs

```golang
func (mirror *Mirror) PeerIDs(ctx context.Context) ([]string, error)
```

Lists the source filesystem's peer UUIDs. These are monitor policies; an absent
peer does not prove that daemons stopped syncing.

#### RemovePeer

```golang
func (mirror *Mirror) RemovePeer(ctx context.Context, id string) error
```

Unregisters the peer this mirror bootstrapped. Destination data, snapshots and
credentials are kept. Daemons tear down asynchronously. Before
`RebootstrapPeer`, check that synchronization stopped (for example with the
admin socket's `fs mirror status`), or use `BeginPeerRemoval` and `WaitDrained`,
which prove it.

#### RebootstrapPeer

```golang
func (mirror *Mirror) RebootstrapPeer(ctx context.Context) (string, error)
```

Creates a new token on the destination configured at construction, imports it
into the source filesystem and returns the new peer UUID. Previously mirrored
snapshots are kept. An existing peer is refused, because Ceph supports only one.
Source MGR candidates are given network access first, including candidates
added after construction. If the import or the following lookup fails, retry
with a fresh context; the pending import is matched by the exact destination
identity.

#### AttachManagers

```golang
func (mirror *Mirror) AttachManagers(ctx context.Context) error
```

Connects every owned source MGR candidate (active, standby and stopped) to the
destination network. Call it after `source.AddManager(ctx, name)` so the new
candidate can reach the peer after a later failover. `RebootstrapPeer` calls it
itself. Host-network fixtures need no attachment. Attachments the caller created
remain the caller's.

#### RefreshMonitorConfig

```golang
func (mirror *Mirror) RefreshMonitorConfig(ctx context.Context) error
```

After the source cluster's MONs were replaced, writes the current source
`mon_host` into every owned daemon's `ceph.conf`, including stopped daemons.
Nothing else changes and no process is restarted. Successful copies remain after
a partial failure; retry with a fresh context.

#### RefreshPeerMonitorConfig

```golang
func (mirror *Mirror) RefreshPeerMonitorConfig(ctx context.Context) error
```

After the destination cluster's MONs were replaced, updates only `mon_host` in
the existing peer's config-key, keeping the peer UUID, credentials and
destination FSID. Daemons read the peer settings when their replayer starts, so
stop and start the daemons afterwards. A retry skips a value that is already
current. Do not write the config-key from outside meanwhile.

```golang
// After replacing the MONs of both clusters:
if err := mirror.RefreshMonitorConfig(ctx); err != nil {
	t.Fatal(err)
}
if err := mirror.RefreshPeerMonitorConfig(ctx); err != nil {
	t.Fatal(err)
}
// Cold-start the daemons, then wait for the next checkpoint.
```

#### Terminate

```golang
func (mirror *Mirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes the owned daemon containers and the MGR network attachments, and
restores interrupted peer links. Clusters, auth entities, peer and directory
policies and snapshots stay. It can be retried after a cleanup error.

### Mirror Methods: tracked directory and peer changes

The plain calls `AddDirectory`, `RemoveDirectory` and `RemovePeer` only change
Ceph's policy. That is enough when the test checks the effect on data (a new
snapshot arrives, or stops arriving). The tracked calls return a receipt that
keeps the original state and lets the test prove what happened, even after a
lost reply:

| Goal | Plain call | Tracked call | What the tracked call adds |
| --- | --- | --- | --- |
| Start mirroring a path | `AddDirectory` | `BeginDirectoryAddition` | A lost reply can be retried without a second native add; `Status` reports whether the policy is registered and owned. |
| Stop mirroring a path | `RemoveDirectory` | `BeginDirectoryRemoval`, then `WaitReleased` | Proof that every original daemon dropped the directory's sync cycle. |
| Remove the peer | `RemovePeer` | `BeginPeerRemoval`, then `WaitDrained` | Proof that every original daemon tore down its replayer for that peer. |

Receipts own no cluster, container, directory, data or cleanup. Every `Begin*`
call is bounded to two minutes, each `Status` to 30 seconds and each wait to two
minutes, or to the caller's earlier deadline. All three share these rules:

- `Begin*` returns `(nil, err)` when it refused before changing Ceph.
- It returns `(receipt, err)` when the native request may or may not have been
  applied. Keep the receipt and call the same `Begin*` again with the same path
  or peer UUID and a fresh context. The retry returns the same receipt, checks
  Ceph first and does not resend a request that was already applied.
- While a receipt is pending, the fixture refuses changes that would disturb it
  (listed per call below). `Daemon.Remove` and `Terminate` are always allowed.
- Query errors from `Status` and waits are retryable. A changed identity
  (cluster FSID, filesystem or metadata pool ID, peer UUID, daemon process or
  session) is a permanent guard error for that receipt.
- External policy writers and direct container start/stop must not race these
  calls: Ceph's path and peer policies have no compare-and-swap.

#### BeginDirectoryAddition

```golang
func (mirror *Mirror) BeginDirectoryAddition(ctx context.Context, directory string) (*DirectoryAddition, error)
```

Registers a path that is absent from Ceph's policy and not yet owned, recording
the intent before the native add. It works with zero daemons. A path added
earlier with `AddDirectory`, or registered outside the fixture, cannot be
adopted. Only one addition can be pending, and starting an addition of another
path supersedes the previous completed receipt. While it is pending, `AddDaemon`,
`AddDirectory`, `RebalanceDirectories`, directory and peer removal and
`RebootstrapPeer` are refused.

`DirectoryAddition.Status(ctx)` is read-only and returns
`DirectoryAdditionStatus`: `Directory`, `PeerID`, the filesystem names and
IDs, `PolicyPresent` (the path is in Ceph's policy) and `Registered` (the
fixture also owns it). After a lost reply the policy can be present while
`Registered` stays false; only a repeated `BeginDirectoryAddition` confirms
ownership. Neither flag says that a daemon picked up the path or replayed a
snapshot; use `WaitDirectoryReady` and `WaitSnapshotSynced` for that.

```golang
receipt, err := mirror.BeginDirectoryAddition(ctx, "/reports")
if receipt == nil {
	t.Fatal(err) // refused, nothing changed
}
if err != nil {
	// Uncertain: confirm with the same path and a fresh context.
	retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if again, err := mirror.BeginDirectoryAddition(retryCtx, "/reports"); err != nil || again != receipt {
		t.Fatal(err)
	}
}
status, err := receipt.Status(ctx)
if err != nil || !status.Registered {
	t.Fatalf("not registered: %+v %v", status, err)
}
```

#### BeginDirectoryRemoval

```golang
func (mirror *Mirror) BeginDirectoryRemoval(ctx context.Context, directory string) (*DirectoryRemoval, error)
```

Removes a path's policy and keeps a receipt with the original daemon cohort.
Preconditions, checked before Ceph is changed:

- the path was registered by this fixture (constructor, `AddDirectory` or
  `BeginDirectoryAddition`);
- it is assigned to a daemon (call `WaitDirectoryReady` first) and that
  daemon's peer status lists it;
- every owned daemon is running normally, with its filesystem session, watcher
  and peer admin command. Zero daemons, stopped daemons or a partial cohort are
  refused; use `RemoveDirectory` for those topologies.

On a retry after an uncertain reply, a policy that is already gone is not
removed again. If the policy is still present, the fixture checks the original
assigned daemon and cohort again before resending; it waits while the
assignment is moving and refuses if another daemon took over.

`DirectoryRemoval.Status(ctx)` and `WaitReleased(ctx)` observe the
original cohort and return `DirectoryRemovalStatus`: `Directory`,
`PeerID`, the filesystem names and IDs, `PolicyRemoved`, `Released` and
`Daemons`, a map from daemon name to `DirectoryRemovalDaemonStatus`
(`ContainerID`, `InstanceID`, `State`, `Problem`). Per-daemon `State` values:

| State | Meaning |
| --- | --- |
| `unobserved` | Not observed yet in this call. |
| `tracked` | The daemon still has a sync cycle registered for the path. |
| `released` | The daemon has no sync cycle for the path in its original session. |
| `stopped`, `unavailable` | The daemon is not running or could not be queried (retryable). |
| `retired` | The daemon was removed from the fixture. |
| `session-changed` | The process, filesystem session or peer command is not the original one (guard error). |

`Released` is true when the policy is gone and every original daemon reports
`released` in the same process and session it had at `Begin`. It does not prove
that worker threads exited, that the remote lock was released, that a checkpoint
completed or that destination metadata is unchanged. `WaitReleased` polls,
releasing the fixture lock between polls; on deadline or cancellation it returns
the last partial status with `Released=false` and both the caller's cause and
the last query error.

While a removal is pending (not yet `Released`), the fixture refuses
`AddDirectory`, `AddDaemon`, `RebalanceDirectories`, peer removal,
`RebootstrapPeer` and removal of other paths. Stopping, restarting or removing an
original daemon makes `Released` unreachable, because a new process is never
accepted as evidence for the old one; see
[Recovering from an interrupted removal](#recovering-from-an-interrupted-removal).

Adding the same path again later (`AddDirectory`) supersedes the old receipt
before the native add; its `Status` then returns a guard error, and the next
`BeginDirectoryRemoval` creates a new receipt.

```golang
if _, err := mirror.WaitDirectoryReady(ctx, "/data"); err != nil {
	t.Fatal(err)
}
removal, err := mirror.BeginDirectoryRemoval(ctx, "/data")
if removal == nil {
	t.Fatal(err)
}
if err != nil {
	retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if removal, err = mirror.BeginDirectoryRemoval(retryCtx, "/data"); err != nil {
		t.Fatal(err)
	}
}
status, err := removal.WaitReleased(ctx)
if err != nil || !status.Released {
	t.Fatalf("not released: %+v %v", status, err)
}
// Snapshots of /data taken from now on are not mirrored.
```

#### BeginPeerRemoval

```golang
func (mirror *Mirror) BeginPeerRemoval(ctx context.Context, id string) (*PeerRemoval, error)
```

Removes this mirror's peer (`id` from `PeerIDs`) and keeps a receipt with the
original daemon cohort. Every owned daemon must be running normally with its
filesystem watcher and the peer's admin command; zero, stopped or partial
cohorts are refused before Ceph is changed (use `RemovePeer` for those).

`PeerRemoval.Status(ctx)` and `WaitDrained(ctx)` return
`PeerRemovalStatus`: `PeerID`, the filesystem names and IDs,
`PolicyRemoved` (the UUID is absent from both the MON FSMap and the MGR peer
list), `Drained` and `Daemons`, a map of `PeerRemovalDaemonStatus`
(`ContainerID`, `InstanceID`, `State`, `Problem`). Per-daemon `State` values are
`unobserved`, `pending`, `awaiting-peer-removal`,
`awaiting-command-unregister`, `drained`, `stopped`, `unavailable`, `retired`
and `session-changed`.

`Drained` is true when every original daemon, in its original process and
session, has unregistered the peer's admin command. Ceph unregisters that
command only after it joined the replayer's worker threads and unmounted the
remote filesystem, so this proves the replayer was torn down. It does not prove
that a snapshot completed, that the remote lock was released or that
destination metadata is unchanged.

While the receipt lacks `Drained`, `RebootstrapPeer`, `AddDaemon` and directory
addition, removal and rebalancing are refused. After `Drained`, call
`RebootstrapPeer`; the new peer supersedes the receipt, whose `Status` then
returns a guard error.

```golang
peers, err := mirror.PeerIDs(ctx)
if err != nil {
	t.Fatal(err)
}
removal, err := mirror.BeginPeerRemoval(ctx, peers[0])
if err != nil {
	t.Fatal(err) // with removal != nil, retry BeginPeerRemoval(freshCtx, peers[0])
}
status, err := removal.WaitDrained(ctx)
if err != nil || !status.Drained {
	t.Fatalf("not drained: %+v %v", status, err)
}
newPeer, err := mirror.RebootstrapPeer(ctx)
```

#### Recovering from an interrupted removal

A tracked removal can only finish while the original daemons keep running. When
a test stops, restarts or removes them before `Released` or `Drained`, the
receipt stays pending and keeps blocking the fixture. The way out is an explicit
acknowledgement, which needs the process observer:

1. Set `Config.OriginalProcessClientFactory` when calling `Run`. The
   factory returns a new raw Docker client for each daemon; the fixture owns and
   closes it. Without the factory, an interrupted removal cannot be acknowledged
   and the fixture stays blocked until it is terminated.

   ```golang
   config.OriginalProcessClientFactory = func(ctx context.Context) (*client.Client, error) {
   	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
   	if err != nil {
   		return nil, err
   	}
   	return docker.Client, nil // github.com/moby/moby/client
   }
   ```

   Check `daemon.ProcessObserverBindingStatus().State == "bound"` for each
   daemon before `Begin*`; strict removal also verifies it.

2. Observe (optional). `ProcessQuiescence(ctx)` on either receipt is read-only
   and bounded to 30 seconds. It returns `ProcessQuiescenceStatus`:
   `PeerID`, `Directory`, the filesystem names and IDs, `PolicyRemoved`,
   `OriginalQuiescent` and `Daemons`, a map of `OriginalProcessStatus`
   (`EngineID`, `ContainerID`, `StartedAt`, `InstanceID`, `OriginalTaskEnded`,
   `OriginalWatcherRetired`, `OriginalQuiescent`, `Evidence`, `Problem`).
   `Evidence` tells how the original run ended: `exited` (stopped), `later-run`
   (restarted, which is a new run) or `container-removed`. `OriginalQuiescent`
   means every original run ended and its watcher GID left the source
   filesystem. It does not change `Drained`, `Released` or the pending gates.

3. Remove every owned daemon with `Daemon.Remove` (not just
   `Daemon.Terminate`), so that the fixture's inventory is empty.

4. Acknowledge. `AcknowledgeProcessQuiescence(ctx)`, bounded to 30 seconds,
   freshly checks that the original peer or path policy is absent, the inventory
   is empty, every original container is gone and every original watcher GID
   left the filesystem. `ProcessQuiescenceAcknowledgment.Acknowledged` is
   true only when this call proved all of it; `Observation` holds the evidence
   it read. On success the gates open. The acknowledgement does not set
   `Drained` or `Released` and does not resend any removal.

5. Continue: `RebootstrapPeer` (peer receipt) or `AddDirectory` (directory
   receipt), then `AddDaemon`.

```golang
receipt, err := mirror.BeginPeerRemoval(ctx, peerID)
if receipt == nil {
	t.Fatal(err)
}
// ... the test stops or restarts the original daemons here ...
for _, daemon := range mirror.Daemons() {
	if err := daemon.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}
ack, err := receipt.AcknowledgeProcessQuiescence(ctx)
if err != nil || !ack.Acknowledged {
	t.Fatalf("not acknowledged: %+v %v", ack.Observation, err)
}
if _, err := mirror.RebootstrapPeer(ctx); err != nil {
	t.Fatal(err)
}
if _, err := mirror.AddDaemon(ctx, "replacement"); err != nil {
	t.Fatal(err)
}
```

Each acknowledgement attempt reads all evidence again. A later failed attempt
returns `Acknowledged=false` but does not undo an earlier acceptance. A receipt
that already reached `Drained` or `Released` does not need an acknowledgement
and refuses one. A plain `Stop` or a restarted container is not enough:
acknowledgement requires the original containers to be removed. The observer
assumes a trusted Docker endpoint; proxies that impersonate an engine,
checkpoint/restore and concurrent external container changes are outside its
contract.

## Examples

The examples share a small helper that runs a command and fails the test on a
non-zero exit, and a helper that attaches a client container:

```golang
func mustExec(t *testing.T, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	code, reader, err := ctr.Exec(t.Context(), args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%v exited %d: %s", args, code, output)
	}
	return strings.TrimSpace(string(output))
}

func startClient(t *testing.T, cluster *cephfs.Container) testcontainers.Container {
	t.Helper()
	client, err := testcontainers.Run(t.Context(), ceph.DefaultImage,
		cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
```

### Mount a filesystem with libcephfs in a client container

Ceph clients talk to the MON, the MDS and every OSD directly, so the client
must reach the addresses Ceph advertises. In the default bridge mode, run the
client in a container attached with `cluster.WithClient()` (admin credentials)
or `client.WithIdentity()` (one principal). The container needs the
client library you test; `ceph.DefaultImage` has Python libcephfs, and go-ceph
programs can run in any Linux image with `libcephfs` installed.

```golang
const listRoot = `import cephfs, sys
fsname, root = sys.argv[1], sys.argv[2]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(mount_root=root.encode(), filesystem_name=fsname.encode())
try:
    d = fs.opendir('/')
    entry = fs.readdir(d)
    while entry:
        print(entry.d_name.decode())
        entry = fs.readdir(d)
    fs.closedir(d)
finally:
    fs.shutdown()
`

cluster, err := cephfs.Run(ctx, ceph.DefaultImage,
	cephfs.WithFilesystems(cephfs.Config{Name: "app-fs"}, cephfs.Config{Name: "logs"}))
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
fs := cluster.Filesystems()[0] // sorted by name: app-fs
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{Name: "data"})
if err != nil {
	t.Fatal(err)
}
client := startClient(t, cluster)
t.Log(mustExec(t, client, "python3", "-c", listRoot, fs.FilesystemName, volume.Path))
```

Notes:

- With several filesystems, always pass `filesystem_name`; otherwise the client
  mounts Ceph's default filesystem.
- Each `python3` process opens a new session. Start a new process for each phase
  of a test (before and after a failure) so cached data cannot hide a problem.
- Long scripts can be copied once with `client.CopyToContainer` and run by path.
- No kernel or FUSE mount is used, so the client container needs no privileges
  or `/dev/fuse`.
- For a native client in the test process, start the cluster with
  `ceph.WithHostNetwork()` on a Linux Docker host and pass the bytes from
  `cluster.ConnectionConfig(ctx)` to the client. See
  [Host network](../../docs/guides/host-network.md).

### Test quota enforcement (EDQUOT)

A subvolume's `SizeBytes` becomes the `ceph.quota.max_bytes` attribute of its
directory. libcephfs refuses a write that would cross the limit with `EDQUOT`
(errno 122), which the Python binding raises as `cephfs.DiskQuotaExceeded`.

```golang
const quotaProbe = `import cephfs, errno, os, sys
fsname, path, size = sys.argv[1], sys.argv[2], int(sys.argv[3])
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_quota', 'true')
fs.mount(filesystem_name=fsname.encode())
fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
try:
    fs.write(fd, b'x' * size, 0)
    fs.fsync(fd, 0)
    print('written')
except cephfs.DiskQuotaExceeded as e:
    assert e.errno == errno.EDQUOT
    print('EDQUOT')
finally:
    fs.close(fd)
    fs.shutdown()
`

// fs and client as in the usage example.
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{Name: "limited", SizeBytes: 1 << 20})
if err != nil {
	t.Fatal(err)
}
file := volume.Path + "/big"
if out := mustExec(t, client, "python3", "-c", quotaProbe, fs.FilesystemName, file, "2097152"); out != "EDQUOT" {
	t.Fatalf("2 MiB write into a 1 MiB quota: %s", out)
}
if err := volume.Resize(ctx, 8<<20); err != nil {
	t.Fatal(err)
}
if out := mustExec(t, client, "python3", "-c", quotaProbe, fs.FilesystemName, file, "2097152"); out != "written" {
	t.Fatalf("write after resize: %s", out)
}
```

Notes:

- Quotas are enforced by the client from the usage it knows. A single write that
  would cross the limit fails reliably; many concurrent writers on several
  clients can overshoot before enforcement catches up.
- `BytesUsed` from `SubvolumeInfo` is recursive accounting and lags writes.
  Poll it until it reflects your data before testing that a resize below usage
  is refused.
- A group quota (`SubvolumeGroupConfig.SizeBytes`) limits all subvolumes in the
  group; the smaller of the two limits applies.

### Give each subvolume its own restricted principal

Each tenant gets a namespace-isolated subvolume and a principal that can only
reach it. The client container attached with `ClientConfig.WithIdentity` holds only
that principal's keyring.

```golang
const tenantProbe = `import cephfs, errno, os, sys
fsname, user, keyring, own, other = sys.argv[1:]
def mount(root):
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id=user)
    fs.conf_set('keyring', keyring)
    fs.conf_set('client_mount_timeout', '10')
    fs.mount(mount_root=root.encode(), filesystem_name=fsname.encode())
    return fs
fs = mount(own)
fd = fs.open('/owned', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
fs.write(fd, b'tenant data', 0)
fs.fsync(fd, 0)
fs.close(fd)
fs.shutdown()
try:
    mount(other)
except cephfs.Error as e:
    assert abs(e.args[0]) in (errno.EPERM, errno.EACCES), e
    print('denied')
else:
    raise AssertionError('mounted another tenant')
`

group, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{Name: "tenants"})
if err != nil {
	t.Fatal(err)
}
volumes := map[string]*cephfs.Subvolume{}
for _, name := range []string{"tenant-a", "tenant-b"} {
	volumes[name], err = fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
		Name: name, GroupName: group.Name, SizeBytes: 64 << 20, NamespaceIsolated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
}
grant, err := volumes["tenant-a"].Authorize(ctx,
	cephfs.SubvolumeAuthorizationConfig{ClientID: "tenant-a", Access: "rw"})
if err != nil {
	t.Fatal(err) // a non-nil grant may be partial; do not authorize the same ID again
}
tenant, err := testcontainers.Run(ctx, ceph.DefaultImage,
	grant.Client.WithIdentity(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, tenant)
if err != nil {
	t.Fatal(err)
}
out := mustExec(t, tenant, "python3", "-c", tenantProbe, fs.FilesystemName,
	grant.Client.User(), grant.Client.KeyringPath(), grant.Path, volumes["tenant-b"].Path)
if out != "denied" {
	t.Fatalf("tenant probe: %s", out)
}

// Revoke: new connections are refused, then established sessions are evicted.
if err := grant.Deauthorize(ctx); err != nil {
	t.Fatal(err)
}
if err := grant.EvictClients(ctx); err != nil {
	t.Fatal(err)
}
if err := grant.Client.Delete(ctx); err != nil { // optional
	t.Fatal(err)
}
```

Notes:

- `SubvolumeAuthorizedClients(ctx, "tenant-a", "tenants")` lists the
  principals Ceph recorded for the subvolume.
- Test revocation with a new process. A principal left with no caps at all can
  time out during mount instead of failing with `EPERM`, so do not use a timeout
  alone as proof of revocation.
- `rw` does not allow changing layouts (`p`) or managing snapshots (`s`). For
  other cap combinations, create the principal with `cluster.CreateClient` and
  `ceph.ClientCaps`.
- Terminate tenant containers before the cluster; `CleanupContainer` does this
  when the containers are registered after the cluster.

### Take a consistent snapshot of several subvolumes

Snapshots are taken one subvolume at a time and do not pause writers. To get one
checkpoint across several subvolumes, quiesce them, take the snapshots and
release.

```golang
q, err := fs.QuiesceSubvolumes(ctx, []*cephfs.Subvolume{orders, payments},
	cephfs.QuiesceConfig{Timeout: 20 * time.Second, Expiration: time.Minute})
if err != nil {
	// A non-nil q may exist after an uncertain creation; Ceph releases it
	// when Expiration passes.
	t.Fatal(err)
}
for _, volume := range []*cephfs.Subvolume{orders, payments} {
	if _, err := volume.CreateSnapshot(ctx, "checkpoint-1"); err != nil {
		t.Fatal(err) // Expiration resumes I/O even if the test stops here.
	}
}
if err := q.Release(ctx); err != nil {
	// The set expired, timed out or was changed: the snapshots are not a
	// consistent checkpoint.
	t.Fatal(err)
}
```

Notes:

- Choose `Expiration` longer than the snapshot step. While the set is
  `QUIESCED`, client I/O on the members is paused.
- `q.Status(ctx)` shows the state without extending the expiration.

### Fail over an MDS and replace a stopped MDS

With one active and one standby daemon, stopping the active daemon makes the
standby take rank 0. The test then either restarts the old daemon or retires it
and creates a new one.

```golang
cluster, err := cephfs.Run(ctx, ceph.DefaultImage,
	cephfs.WithFilesystems(cephfs.Config{ActiveMDS: 1, StandbyMDS: 1}))
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
fs := cluster.Filesystems()[0]

before, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
var old *cephfs.MDS
for _, mds := range fs.MDSs() {
	if mds.ID == before.Active[0].Name {
		old = mds
	}
}
timeout := 10 * time.Second
if err := old.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Fail the old GID now instead of waiting for mds_beacon_grace.
if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(before.Active[0].GID, 10)); err != nil {
	t.Fatal(err)
}
deadline := time.Now().Add(2 * time.Minute)
for {
	status, err := fs.MDSStatus(ctx)
	if err == nil && len(status.Active) == 1 && status.Active[0].Name != old.ID &&
		status.Active[0].State == "up:active" {
		break
	}
	if time.Now().After(deadline) {
		t.Fatalf("no takeover: %+v %v", status, err)
	}
	time.Sleep(time.Second)
}
// Clients can mount again. WaitReady would fail now: standby capacity is missing.

// Option 1: restart the old daemon; it rejoins as a standby with a new GID.
//     err = old.Start(ctx)
// Option 2: retire it and let ScaleMDS start a daemon with a new name.
if err := old.RemoveStopped(ctx); err != nil {
	t.Fatal(err)
}
if err := fs.ScaleMDS(ctx, 1, 1); err != nil {
	t.Fatal(err)
}
if err := fs.WaitReady(ctx); err != nil {
	t.Fatal(err)
}
```

With `StandbyReplay: true` the follower takes over faster because it already
replays the active daemon's journal; a restarted daemon rejoins as the new
follower.

When the filesystem has a single MDS and no standby, nothing can take over. The
filesystem is unavailable: Ceph reports `FS_WITH_FAILED_MDS` and `FS_DEGRADED`
(warning) and `MDS_ALL_DOWN` (error), and new mounts fail after
`client_mount_timeout` with errno 110. `MDS.AddReplacement` starts a new daemon
for the failed rank:

```golang
fs := cluster.Filesystems()[0] // 1 active, 0 standby
status, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
old := fs.MDSs()[0]
timeout := 10 * time.Second
if err := old.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Without a standby Ceph may keep a dead daemon registered as laggy.
if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(status.Active[0].GID, 10)); err != nil {
	t.Fatal(err)
}
replacement, err := old.AddReplacement(ctx)
if err != nil {
	t.Fatal(err) // a non-nil replacement stays owned by the cluster
}
if err := old.RemoveStopped(ctx); err != nil {
	t.Fatal(err)
}
if err := fs.WaitReady(ctx); err != nil {
	t.Fatal(err)
}
t.Logf("rank 0 is now mds.%s", replacement.ID)
```

The old daemon's Cephx key stays in the cluster. Files written before the
failure are readable through the new daemon.

Instead of a replacement, the same container can be started again with
`old.Start(ctx)` after `mds fail`. The restarted daemon takes rank 0 with a
new GID, and a client that stayed mounted reconnects: its session keeps the
same client ID, PID and address, and a file it opened before the failure keeps
working. See [MDS sessions](../../docs/guides/messenger-client-fixtures.md#mds-sessions).

### Evict a frozen client

A client that stops responding keeps its session and capabilities until the MDS
gives up on it. Freezing the client container with `fault.Pause` reproduces
this. With shortened session limits, the test can observe the eviction, the
blocklist entry and the loss of unsynced data.

```golang
// The holder writes "durable" and syncs it, then caches "-volatile" without
// syncing, publishes its client ID and waits.
const holderScript = `import cephfs, os, sys, time
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.mount(filesystem_name=sys.argv[1].encode())
fd = fs.open('/held', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
fs.write(fd, b'durable', 0)
fs.fsync(fd, 0)
fs.write(fd, b'-volatile', 7)
with open('/tmp/holder-id', 'w') as out:
    out.write(str(fs.get_instance_id()))
time.sleep(3600)
`
const readerScript = `import cephfs, os, sys
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=sys.argv[1].encode())
fd = fs.open('/held', os.O_RDONLY)
print(fs.read(fd, 0, 64).decode())
fs.close(fd)
fs.shutdown()
`

findSession := func(id string) *cephfs.Session {
	sessions, err := fs.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range sessions {
		if strconv.FormatUint(session.ID, 10) == id {
			return &session
		}
	}
	return nil
}

change, err := fs.OverrideSessionTimeouts(ctx, cephfs.SessionTimeouts{
	Timeout: 30 * time.Second, Autoclose: time.Minute,
})
if change != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = change.Restore(ctx)
	})
}
if err != nil {
	t.Fatal(err)
}

held := startClient(t, cluster)
mustExec(t, held, "sh", "-c", `nohup python3 -c "$1" "$2" >/tmp/holder.log 2>&1 &`,
	"sh", holderScript, fs.FilesystemName)
var id string
for deadline := time.Now().Add(time.Minute); id == ""; time.Sleep(200 * time.Millisecond) {
	if time.Now().After(deadline) {
		t.Fatal(mustExec(t, held, "cat", "/tmp/holder.log"))
	}
	id = mustExec(t, held, "sh", "-c", "cat /tmp/holder-id 2>/dev/null || true")
}
session := findSession(id)
if session == nil {
	t.Fatal("holder has no session")
}

pause, err := fault.Pause(ctx, cluster, held)
if pause != nil {
	t.Cleanup(func() { _ = pause.Resume(context.Background()) })
}
if err != nil {
	t.Fatal(err)
}

// The reader needs the frozen holder's capabilities, so the MDS evicts the
// holder as soon as Timeout passes. The reader gets only the synced bytes.
other := startClient(t, cluster)
if got := mustExec(t, other, "python3", "-c", readerScript, fs.FilesystemName); got != "durable" {
	t.Fatalf("reader got %q", got)
}
for deadline := time.Now().Add(2 * time.Minute); findSession(id) != nil; time.Sleep(2 * time.Second) {
	if time.Now().After(deadline) {
		t.Fatal("the MDS kept the frozen client's session")
	}
}
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
entries, err := observer.BlocklistEntries(ctx)
if err != nil {
	t.Fatal(err)
}
if !slices.ContainsFunc(entries, func(e ceph.BlocklistEntry) bool { return e.Address == session.Address }) {
	t.Fatalf("%s is not blocklisted", session.Address)
}
if err := pause.Resume(ctx); err != nil {
	t.Fatal(err)
}
```

What to expect:

| Situation | Eviction |
| --- | --- |
| Another client wants the capabilities | After about `Timeout` (30 s here). The waiting client blocks until then. |
| Nobody wants them | After about `Autoclose` (60 s here). The session stays `open` meanwhile. |

After `Resume`, the holder's old session fails with `ESHUTDOWN` (errno 108); a
new mount from the same container gets a new client ID and works. The cluster
health returns to `HEALTH_OK`; Ceph evicts before it would raise
`MDS_CLIENT_LATE_RELEASE`. The blocklist entry expires by itself (one hour by
default) and is not removed by `Restore`. To test eviction
without blocklisting, set `mds_session_blocklist_on_timeout` to `false` with
`override.SetConfig` (section `mds`).

### Keep an MDS active while it drops client messages

Pausing or stopping an MDS stops its beacons, so the MON marks it failed and a
standby takes over. To test a client against an MDS that stays `up:active` but
does not answer, enable the
messenger option `ms_blackhole_client` on that daemon. The MDS keeps talking to
the MON, no failover happens, and new client sessions cannot open. See
[Fault injection](../../docs/guides/fault-injection.md) for the other messenger options.

```golang
const mountProbe = `import cephfs, sys
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '5')
try:
    fs.mount(filesystem_name=sys.argv[1].encode())
    print('mounted')
except cephfs.Error as e:
    print('failed', abs(e.args[0]))
finally:
    fs.shutdown()
`

// cluster, fs and client as in the usage example.
status, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
active := status.Active[0]
hold, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: "mds." + active.Name, Name: "ms_blackhole_client", Value: "true",
})
t.Cleanup(func() { _ = hold.Restore(context.Background()) })
if err != nil {
	t.Fatal(err)
}
if out := mustExec(t, client, "python3", "-c", mountProbe, fs.FilesystemName); out != "failed 110" {
	t.Fatalf("mount against a silent MDS: %s", out)
}
after, err := fs.MDSStatus(ctx)
if err != nil || after.Active[0].Name != active.Name || after.Active[0].GID != active.GID {
	t.Fatalf("the MDS was replaced: %+v %v", after, err)
}
if err := hold.Restore(ctx); err != nil {
	t.Fatal(err)
}
if out := mustExec(t, client, "python3", "-c", mountProbe, fs.FilesystemName); out != "mounted" {
	t.Fatalf("mount after restore: %s", out)
}
```

Notes:

- Use a new client process for each phase. The effect on sessions that were
  already open is not covered by this recipe.
- Never set `ms_blackhole_client` on the MON, the MGR or the `client` section:
  the fixture's own CLI is a client, so `override.SetConfig` and `Restore` would
  hang.
- `ms_inject_delay_type`, `ms_inject_delay_probability` and
  `ms_inject_delay_max` add random delays instead of dropping messages.

### Mirror a directory and wait for a snapshot

Two independent clusters each get a filesystem. The source directory is created
first, mirroring starts, and the test waits for one exact snapshot before
reading it on the destination.

```golang
const sourceScript = `import cephfs, json, os, sys
fsname, phase = sys.argv[1], sys.argv[2]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.mount(filesystem_name=fsname.encode())
try:
    if phase == 'seed':
        fs.mkdir('/data', 0o755)
        fd = fs.open('/data/file', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        fs.write(fd, b'mirrored bytes', 0)
        fs.fsync(fd, 0)
        fs.close(fd)
    elif phase == 'snapshot':
        name = sys.argv[3]
        fs.mkdir('/data/.snap/' + name, 0o755)
        print(json.dumps({'id': fs.snap_info('/data/.snap/' + name)['id'], 'name': name}))
finally:
    fs.shutdown()
`
const readFile = `import cephfs, os, sys
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    fd = fs.open(sys.argv[2], os.O_RDONLY)
    print(fs.read(fd, 0, 1 << 20).decode())
    fs.close(fd)
finally:
    fs.shutdown()
`

func TestCephFSMirror(t *testing.T) {
	ctx := t.Context()
	sourceCluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	testcontainers.CleanupContainer(t, sourceCluster)
	if err != nil {
		t.Fatal(err)
	}
	destinationCluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	testcontainers.CleanupContainer(t, destinationCluster)
	if err != nil {
		t.Fatal(err)
	}
	sourceFS := sourceCluster.Filesystems()[0]
	destinationFS := destinationCluster.Filesystems()[0]
	sourceClient := startClient(t, sourceCluster)
	destinationClient := startClient(t, destinationCluster)

	mustExec(t, sourceClient, "python3", "-c", sourceScript, sourceFS.FilesystemName, "seed")

	// Registered after the clusters, so it is terminated before them.
	sourceClusterFacts, err := ceph.FactsOf(sourceCluster)
	mirror, err := cephfsmirror.Run(ctx, sourceClusterFacts.Images.CephFS, cephfsmirror.WithConfig(cephfsmirror.Config{
		Source:                sourceCluster.Container,
		Destination:           destinationCluster.Container,
		SourceFilesystem:      sourceFS.FilesystemName,
		DestinationFilesystem: destinationFS.FilesystemName,
		Directories:           []string{"/data"},
	}))
	if mirror != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := mirror.Terminate(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.WaitDirectoryReady(ctx, "/data"); err != nil {
		t.Fatal(err)
	}

	var checkpoint cephfsmirror.Snapshot // decoded from {"id": ..., "name": ...}
	out := mustExec(t, sourceClient, "python3", "-c", sourceScript, sourceFS.FilesystemName, "snapshot", "checkpoint-1")
	if err := json.Unmarshal([]byte(out), &checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.WaitSnapshotSynced(ctx, "/data", checkpoint); err != nil {
		t.Fatal(err)
	}

	got := mustExec(t, destinationClient, "python3", "-c", readFile,
		destinationFS.FilesystemName, "/data/.snap/checkpoint-1/file")
	if got != "mirrored bytes" {
		t.Fatalf("destination snapshot holds %q", got)
	}
}
```

Notes:

- Deleting a snapshot on the source deletes it on the destination too.
  Mirroring is replication, not a backup with its own retention.
- To test an outage, stop a daemon (`mirror.Daemons()[0].Stop`), remove all
  daemons with `Daemon.Remove`, or cut the peer link with `Daemon.InterruptPeerLink`.
  Snapshots taken meanwhile arrive after recovery.
- With `DaemonCount: 2`, a stopped daemon's directories move to the survivor.
  After `AddDaemon`, call `RebalanceDirectories` on Ceph 20.2.4.
- Compare file bytes, names, symlinks, modes and owners on the destination.
  Native mirroring is not guaranteed to reproduce `user.*` extended attributes.

## Limitations

- One-way, one peer per source filesystem. Several daemons share one peer and
  do not create a fan-out to several destinations. Active-active writes and
  automatic site failover are not provided.
- On Ceph 20.2.4 the MGR does not redistribute directories when a daemon joins;
  call `RebalanceDirectories`. Multiple mirror daemons are marked as untested
  in Ceph's documentation.
- The MGR's mirroring module can hang for good when a directory is removed
  while another directory's assignment change is in flight or a mirror daemon
  joins or leaves. `RebalanceDirectories` waits for an idle policy before each
  removal; `RemoveDirectory` and `BeginDirectoryRemoval` do not. Recover with
  `ceph mgr fail`; see [RebalanceDirectories](#rebalancedirectories).
- Native mirroring is not guaranteed to reproduce `user.*` extended attributes;
  compare them separately if they matter.
- `WaitSnapshotSynced` waits for one exact snapshot and times out if a newer one
  is synced first.
- `Daemon.InterruptPeerLink` requires bridge networking.
- `Released`, `Drained` and the process-quiescence acknowledgement do not prove
  remote unlock, checkpoint completion or unchanged destination metadata.
