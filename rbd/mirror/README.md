# RBD mirror

## Introduction

The `rbd/mirror` package connects two Ceph clusters with native RBD mirroring.
It starts the setup CLI containers and the `rbd-mirror` receiver daemons and
observes their readiness. RBD pools, namespaces, images and backups stay in the
[rbd package](../README.md); CephFS snapshot mirroring is a separate fixture in the
`cephfs` package with its own semantics. The two services share no public
mirror interface.

Import it with an alias, because the CephFS family also uses the package name
`mirror`:

```golang
import rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
```

Two clusters need the pool and namespaces prepared with `rbd.WithPools`,
`rbd.InitPool` and `rbd.CreateNamespace`. `rbdmirror.Run` takes the clusters as
`*ceph.Container`, so it works with a cluster from `ceph.Run` and with any
other service package's `Run`.

`Run` takes the mirror daemon image, which is required: the rbd-aux role (or
the aggregate `all` image). The daemons run for the destination cluster, so the
destination's `Images.RBDAux` is the usual choice. The setup CLI clients are not
started from it: each cluster's `Images.Client` supplies them. `Run`
refuses an image that a cluster selects for another role, such as its essential
image, when that cluster has a separate rbd-aux image. See the
[image guide](../../docs/guides/releases-and-images.md).

## Module Reference

### Run function

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Mirror, error)
func WithConfig(config Config) Option
func (Option) Customize(*testcontainers.GenericContainerRequest) error
```

- `context.Context`, the Go context.
- `string`, the image for the `rbd-mirror` daemons (rbd-aux or `all` role), required. The setup clients use each cluster's `Images.Client`.
- `testcontainers.ContainerCustomizer`, exactly one `WithConfig` with the two clusters and the mirroring policy, and options applied to every `rbd-mirror` daemon container (not to the setup clients).

```golang
facts, err := ceph.FactsOf(destination)
link, err := rbdmirror.Run(ctx, facts.Images.RBDAux,
	rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: "rbd"}))
```

`Run` connects two existing, independent clusters with native RBD
mirroring. Replication is one-way, from `Source` to `Destination`; the
`rbd-mirror` daemons run next to the destination and pull from the source. For
two-way replication (failover and failback), create a second link with the
clusters swapped.

In order, `Run`:

1. validates the configuration and context before touching any cluster;
2. checks that `Pool` is replicated on both clusters and that the named namespaces exist, and records the native pool IDs;
3. starts two setup CLI containers, one per cluster, each also attached to the other cluster's network;
4. enables mirroring on the selected pool/namespace of both clusters with the configured scope, site name and remote namespace;
5. creates a bootstrap token on the source (`rbd mirror pool peer bootstrap create`) and imports it on the destination as a receive-only (`rx-only`) peer;
6. starts the initial daemons, each with its own destination Cephx user `client.rbd-mirror.tc-<uuid>` (`mon 'profile rbd-mirror' osd 'profile rbd'`).

Mirroring a named namespace while the default namespace is disabled sets the
default namespace to `init-only`, as Ceph requires. An existing active default
policy is preserved.

`Run` never silently reconfigures existing state. If the target namespace
already has an active policy with a different scope or remote namespace, or the
cluster already has a different site name, it returns an error. When the
destination already has a transmit-only (`tx-only`) peer for the source (left
by a link in the opposite direction), it becomes `rx-tx`, so the existing
transmission keeps working.

Success means the policies, the peer and the daemon processes exist. It does
not mean that any image is replicating. Enable images, then observe readiness
with the methods below.

Customizers apply to the daemon containers only and must preserve their
networking, Ceph configuration, credentials and entrypoint. Use them for
things like `testcontainers.WithEnv`, `testcontainers.WithLogger` or labels.

Both clusters must use the same network mode. In bridge mode each daemon joins
both clusters' networks. In host mode all containers share the Docker host
network namespace.

The caller must terminate a non-nil `*Mirror` even when `Run` returns an
error. Register the clusters' cleanup first and the link's cleanup after, so
the link is terminated before the clusters:

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
}))
testcontainers.CleanupContainer(t, link)
if err != nil {
	t.Fatal(err)
}
```

#### Config

| Field | Default | Meaning |
| --- | --- | --- |
| `Source`, `Destination` | required | Two independent clusters (`*ceph.Container`). |
| `Pool` | required | Pool name, the same on both clusters. It must be replicated and initialized for RBD on both. No namespace or option syntax. |
| `SourceSite`, `DestinationSite` | `"source"`, `"destination"` | Native mirror site names. They must differ and must not start with `-`. Site names are cluster-wide: every link that touches a cluster must use the same name for it. |
| `Scope` | `ScopeImage` | `ScopeImage` mirrors only images you enable. `ScopePool` automatically mirrors every image that has the `journaling` feature in the selected namespace, including existing ones. |
| `SourceNamespace`, `DestinationNamespace` | `""` | Empty means the default namespace. Named namespaces must already exist on their cluster (`CreateNamespace`); `Run` never creates or removes them. Different names, including named/default pairs, are mapped explicitly (Ceph 20 or later). |
| `Mode` | `ModeSnapshot` for image scope, `ModeJournal` for pool scope | Per-image replication mechanism. Pool scope supports journal mode only. |
| `DaemonCount` | 1 | Number of initial `rbd-mirror` daemons. Negative values are rejected. |
| `NoInitialDaemons` | false | Configure everything but start no daemon; start receivers later with `AddDaemon`. Requires `DaemonCount` 0. |

Namespace names use letters, digits, `_`, `.` and `-` and must not start with
`.` or `-`.

How `NoInitialDaemons` and `DaemonCount` combine:

| `NoInitialDaemons` | `DaemonCount` | Initial daemons |
| --- | --- | --- |
| false | 0 | 1 |
| false | N > 0 | N |
| true | 0 | 0 |
| true | N > 0 | rejected |
| either | negative | rejected |

Invalid combinations and an already cancelled context are rejected before any
cluster inspection, container start or customizer call.

Initial daemons are named `a` through `z`, then `node-27`, `node-28` and so on.
Several daemons share the pool's receiving peer and take part in Ceph's native
leader election and image assignment. A ready admin socket does not mean that
the daemon has discovered the pool or that the election has converged; use
`ReceiverStatus`/`WaitReceiverReady`.

#### Mode and Scope

| Constant | Value | Meaning |
| --- | --- | --- |
| `ModeSnapshot` | `"snapshot"` | The destination copies the image at each mirror snapshot. Enabling an image creates the first one; later changes need `rbd mirror image snapshot` (or a snapshot schedule). |
| `ModeJournal` | `"journal"` | Every write is journaled on the source and replayed on the destination. Needs the `exclusive-lock` feature; no mirror snapshots are used. |
| `ScopeImage` | `"image"` | Images are enabled one by one (`EnableImage` or `rbd mirror image enable`). |
| `ScopePool` | `"pool"` | Images with the `journaling` feature are enrolled automatically. |

Mode and scope are independent settings, except that pool scope implies
journal mode.

### Mirror methods

`Mirror` owns its setup CLI containers and daemons, not the clusters. The
typed methods serialize on the link's lock, so they do not interleave on one
link; the waits release the lock between polls. `SourceRBD` and
`DestinationRBD` do not take the lock.

#### EnableImage

```golang
func (m *Mirror) EnableImage(ctx context.Context, imageName string) error
```

Enables the link's mode (snapshot or journal) on an existing source image in
the link's `Pool` and `SourceNamespace`. `imageName` is a bare name, without
pool, namespace, snapshot or option syntax.

- An image that already uses the same mode is left unchanged. An image that uses a different mode is rejected before any change.
- Journal mode requires the image's `exclusive-lock` feature. The native enable command adds `journaling`; no other feature is changed.
- In snapshot mode, enabling creates the first mirror snapshot. Later writes need explicit mirror snapshots.
- Only for image scope. Pool scope enrolls journaled images automatically, and `EnableImage` returns an error.
- Quiesce the application before changing an existing image's mirroring.

#### ImageStatus

```golang
func (m *Mirror) ImageStatus(ctx context.Context, imageName string) (ImageStatus, error)
```

One read-only observation of an image in the link's source and destination
namespaces. It never enables mirroring, creates checkpoints, resyncs, changes
the primary or restarts a daemon. It verifies the original pool and namespace
policies before and after the read and requires both sides to report the same
global image ID. It takes at most 30 seconds, bounded by `ctx`. A partial
observation (for example the source image before the destination image exists)
can accompany an error.

#### WaitReplayReady

```golang
func (m *Mirror) WaitReplayReady(ctx context.Context, imageName string) (ImageStatus, error)
```

Polls `ImageStatus` for up to four minutes, bounded by `ctx`, until a currently
running daemon owned by this link reports the image as `up+replaying`.

```golang
status, err := link.WaitReplayReady(ctx, "disk")
if err != nil {
	// status still holds the last observation; errors.Is(err, context.DeadlineExceeded) works.
	t.Fatalf("not replaying: %+v: %v", status, err)
}
```

- It does not wait for any particular write or mirror snapshot. Check the destination data separately (see [`waitReplicated`](#helpers-used-by-the-examples)).
- It keeps waiting while the destination image is missing or not yet enrolled for mirroring: rbd-mirror creates it first, with mirroring disabled, and enables mirroring right after. The destination's identity is pinned from its first appearance. A source image whose mirroring is disabled ends the wait at once, and so does a destination whose mirroring becomes disabled after it was enrolled.
- The source image identity is pinned at the first observation, and the destination identity as soon as it appears. An image deleted and re-created under the same name is rejected, never adopted.
- Transient query failures are retried. A successful read that proves a changed pool, policy or image identity, or an invalid schema, fails immediately.
- On deadline or cancellation it returns the last observation together with the last query error.

#### PolicyStatus

```golang
func (m *Mirror) PolicyStatus(ctx context.Context) (Policies, error)
```

Reads the current native mirroring policy of the link's source and destination
namespaces, after checking that the original pools still exist with the same
IDs. External changes are reported as they are; compare `Mode` and
`RemoteNamespace` with what your test expects.

#### ReceiverStatus

```golang
func (m *Mirror) ReceiverStatus(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error)
```

Observes whether the receiving daemons have discovered the link's pool and
namespace mapping and agree on one leader. It needs no mirrored image, so it is
the readiness check for an empty link.

- With no names, every daemon the link currently owns is expected, including stopped and partially started ones. Name the daemons explicitly to check an HA survivor while another daemon is stopped.
- `Ready` requires that every expected daemon is running, has discovered the pool, peer and exact namespace mapping, and reports a unique instance; that exactly one owned daemon is leader and its member set equals the expected set; and that every member reports the same leader.
- Instances that the link does not own (`ForeignInstances`, for example another fixture's daemon for the same pool) make `Ready` false. This does not mean that the other fixture is unhealthy.
- A link with no daemons is a successful, non-ready observation.
- It never starts a daemon, bootstraps a peer or creates an image. It takes at most 30 seconds, bounded by `ctx`. A partial report can accompany an error.

`Ready` does not prove that any image replays or that any write arrived.

#### WaitReceiverReady

```golang
func (m *Mirror) WaitReceiverReady(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error)
```

Polls `ReceiverStatus` for up to four minutes, bounded by `ctx`, until `Ready`.

- The first admitted observation pins the pool, policies, peer generation and the exact daemon containers. A daemon removed, replaced under the same name or added during the wait is not adopted; start a new wait after an explicit topology change.
- Stopping and starting the same daemon container is allowed; the daemon gets a new native instance ID.
- Polls release the link's lock, so other link methods can run between them.
- On cancellation it returns the last report with `Ready` cleared. Errors carry only canonical context causes (`errors.Is(err, context.DeadlineExceeded)`), never native stderr or credentials.
- With zero daemons it waits until `ctx` ends.

```golang
status, err := link.WaitReceiverReady(ctx)
if err != nil {
	t.Fatal(err)
}
t.Logf("leader %s (%s), members %v", status.LeaderDaemonName, status.LeaderInstanceID, status.Instances)
```

#### BindNamespace

```golang
func (m *Mirror) BindNamespace(ctx context.Context, sourceNamespace, destinationNamespace string) (*Namespace, error)
```

Returns a read-only view of another namespace pair that is already configured
for mirroring in the same pool, sharing this link's clusters, peer and daemons.
It changes no policy, peer, image or container, and it owns nothing.

- Both namespace policies must already be active (`image` or `pool`), with the same scope and reciprocal remote namespaces. Named namespaces must exist; empty names select the default namespace.
- Binding the default namespace requires an active reciprocal default policy. A default namespace in `init-only` mode cannot be bound.
- The link's own successful setup is required. Binding takes at most 30 seconds, bounded by `ctx`.
- Each call returns a new, immutable view. Binding again does not update an earlier view.
- A `Rebootstrap` that reaches the native bootstrap step invalidates all earlier views permanently, even if it fails. Bind again after a successful `Rebootstrap`. Terminating the link invalidates them too.

Configure the extra namespace pair yourself with `SourceRBD` and
`DestinationRBD`; do not start a second `Run` to reuse the daemons. See
[Mirror several namespace pairs with one set of daemons](#mirror-several-namespace-pairs-with-one-set-of-daemons).

#### AddDaemon

```golang
func (m *Mirror) AddDaemon(ctx context.Context, name string, opts ...testcontainers.ContainerCustomizer) (*Daemon, error)
```

Starts another `rbd-mirror` daemon that uses the existing receiving peer. No
re-bootstrap or image change happens.

- `name` is 1 to 63 ASCII letters, digits, `_` or `-`, not starting with `-`, and unique within the link.
- The customizers given to `Run` apply first, then `opts` for this daemon only.
- Startup waits up to one minute for the daemon's admin socket.
- A non-nil result returned with an error (partial startup) is still owned by the link. Remove it with `Daemon.Remove` or let `Terminate` clean it up.
- After `NoInitialDaemons`, the first `AddDaemon` that returns a non-nil daemon becomes the embedded compatibility handle.

#### Daemons

```golang
func (m *Mirror) Daemons() []*Daemon
```

Returns a snapshot of the link's current daemons sorted by name, including
stopped and partially started ones. It does not discover daemons owned by
other fixtures, even when they serve the same pool.

#### Rebootstrap

```golang
func (m *Mirror) Rebootstrap(ctx context.Context) error
```

Creates or refreshes the destination's receiving peer with a new bootstrap
token from the source (`Run` calls it once during setup).

- After an explicit peer removal (`rbd mirror pool peer remove`) it creates a new peer with a new UUID. Request `rbd mirror image resync` on the destination for images that must catch up.
- On an existing peer it refreshes the source monitor addresses and key, fills in the client name, and turns a matching `tx-only` peer into `rx-tx`.
- Quiescing writers and resynchronizing images are the caller's decisions.
- The previous receiver-observation state is invalidated as soon as Rebootstrap reaches its first native change. A call rejected earlier (busy link, cancelled context, failed preflight) keeps it. Only a fully confirmed call installs the new peer generation. Waits already in progress never adopt the new generation, and `Namespace` views must be bound again.

#### RefreshMonitorConfig

```golang
func (m *Mirror) RefreshMonitorConfig(ctx context.Context) error
```

After you replace monitors on either cluster (`AddMonitor`/`MonitorContainer.Remove`),
running Ceph sessions learn the new monmap, but a restarted process reads the
monitor addresses from its local `ceph.conf`. This method rewrites the global
`mon_host` entry in the local configuration of both setup clients and every
owned daemon, including stopped ones. It keeps their credentials, identities
and process state, and never starts or restarts anything.

```golang
// Finish the monitor replacement on both clusters first.
if err := link.RefreshMonitorConfig(ctx); err != nil {
	t.Fatal(err)
}
// The destination peer stores the source monitor addresses separately.
if err := link.Rebootstrap(ctx); err != nil {
	t.Fatal(err)
}
// Restart daemons explicitly if they must pick up the new files, then
// check WaitReplayReady and the destination data.
```

It takes at most two minutes, bounded by `ctx`. The original pool and
namespace policies must still match. Successful file updates are kept when
another copy fails; retry with a new context. Do not run it concurrently with
topology changes or external configuration writers. Refresh your own client
containers with `cluster.RefreshClientMonitorConfig(ctx, client)` (see
[`ceph`](../../ceph/README.md)).

#### SourceRBD and DestinationRBD

```golang
func (m *Mirror) SourceRBD(ctx context.Context, args ...string) ([]byte, error)
func (m *Mirror) DestinationRBD(ctx context.Context, args ...string) ([]byte, error)
```

Run the `rbd` CLI with admin credentials in the source or destination setup
client and return its stdout. Arguments are passed without shell
interpolation. A non-zero exit becomes an error that includes the output. Use
`--format json` for state queries.

```golang
out, err := link.DestinationRBD(ctx, "mirror", "image", "status", "volumes/disk", "--format", "json")
```

These are the escape hatch for everything the link does not wrap: promote,
demote, resync, mirror snapshots, peer inspection and removal, extra namespace
policies. Such commands are your own native writes. The typed methods detect
and reject changes to the policies captured at setup.

#### Terminate

```golang
func (m *Mirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes every container the link owns (setup clients and all daemons,
including partially started or previously removed ones). Both clusters, their
pools and image data, and the mirroring mode, peer and Cephx configuration
written during setup remain; they disappear with the disposable clusters. A
failed cleanup can be retried.

#### Embedded container

`Mirror` embeds a `testcontainers.Container` that is the initial daemon's
compatibility handle. `link.Stop(ctx, &timeout)` and `link.Start(ctx)` affect
only that daemon. The handle is nil after `NoInitialDaemons` until the first
`AddDaemon`, and nil again after the initial daemon is removed. Use `Daemons()`
for the current membership.

### Namespace methods

`Namespace` is the view returned by `BindNamespace`. It has no
`Terminate`, no container and no daemon control. Call `AddDaemon`,
`Rebootstrap` and `Terminate` on the owning `Mirror`, and `Remove` on a `Daemon`.

#### ImageStatus

```golang
func (v *Namespace) ImageStatus(ctx context.Context, imageName string) (ImageStatus, error)
```

Like `Mirror.ImageStatus`, for an image in the bound namespace pair. The mode
is derived from the source image (snapshot or journal) instead of the owner's
configured mode, and must match on the destination; a pool-scope policy accepts
journal mode only. `ReplayReady` needs one live owned daemon replaying the
image; it does not require the whole daemon cohort's election to be ready. 30
seconds at most, bounded by `ctx`.

#### WaitReplayReady

```golang
func (v *Namespace) WaitReplayReady(ctx context.Context, imageName string) (ImageStatus, error)
```

Waits up to four minutes, bounded by `ctx`. The owner's daemon set is pinned at
the first observation, even when it is empty or the destination image does not
exist yet; the image's mode and IDs are pinned when first seen. `AddDaemon`,
`Daemon.Remove` or a replacement during the wait makes it fail; start a new wait
afterwards. Restarting the same daemon container is allowed.

#### ReceiverStatus

```golang
func (v *Namespace) ReceiverStatus(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error)
```

Like `Mirror.ReceiverStatus`, for the bound namespace pair with the owner's
current daemons. With no owned daemons the result is not ready. 30 seconds at
most.

#### WaitReceiverReady

```golang
func (v *Namespace) WaitReceiverReady(ctx context.Context, expectedDaemons ...string) (ReceiverStatus, error)
```

Like `Mirror.WaitReceiverReady`, for the bound namespace pair. Four minutes at
most.

### Daemon

`Daemon` is one destination-side `rbd-mirror` process. It embeds a
`testcontainers.Container` and has two fields:

| Field | Meaning |
| --- | --- |
| `DaemonName` | The name inside the link (`a`, `b`, or the `AddDaemon` name). |
| `ClientName` | The daemon's own Cephx user on the destination, `client.rbd-mirror.tc-<uuid>`. |

The receiving peer's credentials stay in the destination's config-key store.
`Stop` and `Start` on a daemon keep its identity and native pool state; after a
restart it reports a new instance ID. The daemon logs to stderr, so
`daemon.Logs(ctx)` returns its log.

#### Status

```golang
func (d *Daemon) Status(ctx context.Context) (*DaemonStatus, error)
```

Runs `ceph --admin-daemon /tmp/rbd-mirror.asok rbd mirror status` in the daemon
container and decodes the pool replayers. It fails when the daemon is stopped
or its socket is unavailable. A running daemon can report no pool replayers
until Ceph finishes discovery, so look for the pool and peer you expect. Prefer
`ReceiverStatus` for readiness decisions.

#### Remove

```golang
func (d *Daemon) Remove(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Terminates this daemon and removes it from its link. Removing the last
daemon pauses replication on purpose while keeping the peer and image state;
`AddDaemon` resumes it. A failed removal keeps the daemon in the link so you
can retry. Removing the initial daemon clears the embedded handle; later
`AddDaemon` calls do not reassign it. A daemon its link no longer lists, or one
whose link was terminated, is refused.

#### InterruptPeerLink

```golang
func (d *Daemon) InterruptPeerLink(ctx context.Context) (*fault.NetworkInterruption, error)
```

Disconnects this owned daemon from the source cluster's public bridge network
while keeping its destination endpoint, so the daemon stays in the destination
pool's election but cannot read from the source. The daemon keeps running and
the peer and image policies are unchanged.

```golang
cut, err := daemon.InterruptPeerLink(ctx)
if cut != nil {
	defer cut.Restore(ctx) // a non-nil handle must be restored, even with an error
}
if err != nil {
	t.Fatal(err)
}
// Writes and checkpoints made now do not reach the destination.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
// Observe the catch-up separately (WaitReplayReady plus a data check).
```

- `Restore` reconnects the original IP address and aliases and can be retried. It does not overwrite an endpoint someone reconnected differently.
- The link also restores outstanding interruptions during cleanup.
- Host network mode has no bridge endpoint, so the call is rejected.
- This disconnects a whole endpoint; it is not a per-peer packet filter. For message delays and drops, see [the fault injection guide](../../docs/guides/fault-injection.md).

#### Terminate

```golang
func (d *Daemon) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes this daemon's container. A successful removal is remembered, so link
cleanup does not repeat it; a failed one stays retryable. It does not remove
the daemon from `Daemons()`; use `Remove` for that.

### Mirror status types

#### ImageStatus

Returned by `ImageStatus` and `WaitReplayReady`.

| Field | Meaning |
| --- | --- |
| `Pool`, `SourceNamespace`, `DestinationNamespace`, `Name` | The image observed. |
| `GlobalID` | Native global mirror image ID, equal on both sides. |
| `SourceImageID`, `DestinationImageID` | Local image IDs on each cluster. |
| `Mode` | `ModeSnapshot` or `ModeJournal`. |
| `SourcePrimary`, `DestinationPrimary` | Primary flags on each side. |
| `SourceMirrorState`, `DestinationMirrorState` | Native mirroring state (`enabled`, `creating`, `disabling`, ...). |
| `State`, `Description`, `LastUpdate` | The destination's local replay status, for example `up+replaying`. Not the remote peer status. `LastUpdate` is not a freshness token. |
| `DaemonName`, `InstanceID` | The owned daemon and native instance that reported the status. |
| `ReplayReady` | Source primary, destination secondary, both enabled, and `up+replaying` reported by a currently running owned daemon for this pool, peer and namespace. |

`ReplayReady` proves neither that a particular write arrived nor that a mirror
snapshot completed. Verify the application data or an explicit checkpoint
separately.

#### ReceiverStatus

Returned by `ReceiverStatus` and `WaitReceiverReady`.

| Field | Meaning |
| --- | --- |
| `Pool`, `SourceNamespace`, `DestinationNamespace` | The observed scope. |
| `SourceFSID`, `DestinationFSID`, `SourcePoolID`, `DestinationPoolID` | Original cluster and pool identities. |
| `PeerID` | UUID of the destination's receiving peer. |
| `ExpectedDaemons` | The daemon names that had to be ready. |
| `Daemons` | Per-daemon detail (`ReceiverDaemonStatus`), keyed by daemon name. |
| `LeaderDaemonName`, `LeaderInstanceID` | The owned leader, when one was agreed. |
| `Instances` | The leader's native member instances. |
| `ForeignInstances` | Native members not owned by this link. |
| `DaemonProblems` | Short reason codes per daemon, such as `receiver-scope-not-ready` or `receiver-leader-converging`. |
| `Ready` | See [ReceiverStatus](#receiverstatus). |

`Ready` can be true while the destination has not yet learned the remote
mirror UUID (an explicitly empty `mirror_uuid` in native output). It is a
discovery and election check, nothing more.

#### ReceiverDaemonStatus

| Field | Meaning |
| --- | --- |
| `ContainerID`, `ClientName`, `StartedAt` | The daemon container and Cephx user. |
| `Running` | Docker reports the process running. |
| `InstanceID`, `LeaderInstanceID`, `Leader` | The daemon's native election view. |
| `PoolState` | Native pool replayer state. |
| `NamespaceDiscovered` | The daemon has discovered the exact namespace mapping. |

This is one daemon's discovery state, not an image replay state.

#### Policies and NamespaceState

`Policies` has `Source` and `Destination`, each a `NamespaceState`:

| Field | Meaning |
| --- | --- |
| `PoolID`, `Pool` | Native pool ID and name. |
| `Namespace`, `RemoteNamespace` | Empty means the default namespace. |
| `Mode` | `disabled`, `init-only`, `image` or `pool`. |
| `MirrorUUID` | The native mirroring identity of the pool/namespace (not the cluster FSID). |

A configured policy alone proves no replication. Reading it does not adopt the
namespaces or allow their deletion.

#### DaemonStatus and PoolReplayerStatus

`DaemonStatus.PoolReplayers` lists one `PoolReplayerStatus` per
pool and peer:

| Field | Meaning |
| --- | --- |
| `Pool` | Pool name. |
| `Peer` | Native peer description, for example `uuid: <uuid> cluster: source client: client.rbd-mirror-peer`. |
| `State` | Native replayer state. |
| `InstanceID`, `LeaderInstanceID`, `Leader` | Election state of this daemon. |
| `Instances` | Members known to the leader. |

Images are distributed among instances: the leader does not replay every image
itself.

## Examples

### Helpers used by the examples

The examples share these helpers. `run` executes a command in any container and
returns its exit code and combined output. `waitReplicated` compares the
exported bytes of the source and destination images; stop writers and create
the checkpoint first. `waitTransmitPeer` waits until the source pool knows the
destination as a peer it sends to (see
[the pitfalls](#common-pitfalls)). `promote` retries a non-forced promotion
until the demotion has propagated.

```golang
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func run(ctx context.Context, ctr testcontainers.Container, cmd ...string) (int, string, error) {
	code, reader, err := ctr.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return 0, "", err
	}
	out, err := io.ReadAll(reader)
	return code, string(out), err
}

func poll(ctx context.Context, t *testing.T, what string, done func() bool) {
	t.Helper()
	for !done() {
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", what, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func waitReplicated(ctx context.Context, t *testing.T, link *rbdmirror.Mirror, sourceSpec, destinationSpec string) {
	t.Helper()
	want, err := link.SourceRBD(ctx, "export", "--no-progress", sourceSpec, "-")
	if err != nil {
		t.Fatal(err)
	}
	poll(ctx, t, destinationSpec+" did not match the source", func() bool {
		got, err := link.DestinationRBD(ctx, "export", "--no-progress", destinationSpec, "-")
		return err == nil && bytes.Equal(got, want)
	})
}

func waitTransmitPeer(ctx context.Context, t *testing.T, link *rbdmirror.Mirror, pool, destinationSite string) {
	t.Helper()
	poll(ctx, t, "source never listed "+destinationSite+" as a transmit peer", func() bool {
		out, err := link.SourceRBD(ctx, "mirror", "pool", "info", pool, "--format", "json")
		if err != nil {
			return false
		}
		var info struct {
			Peers []struct {
				SiteName   string `json:"site_name"`
				MirrorUUID string `json:"mirror_uuid"`
				Direction  string `json:"direction"`
			} `json:"peers"`
		}
		if json.Unmarshal(out, &info) != nil {
			return false
		}
		for _, p := range info.Peers {
			if p.SiteName == destinationSite && p.MirrorUUID != "" &&
				(p.Direction == "tx-only" || p.Direction == "rx-tx") {
				return true
			}
		}
		return false
	})
}

// rbdCLI is link.SourceRBD or link.DestinationRBD of the cluster to promote.
func promote(ctx context.Context, t *testing.T, rbdCLI func(context.Context, ...string) ([]byte, error), spec string) {
	t.Helper()
	poll(ctx, t, "promote "+spec, func() bool {
		_, err := rbdCLI(ctx, "mirror", "image", "promote", spec)
		return err == nil
	})
}
```

The examples write test data with `rbd bench --io-type write`, which goes
through librbd like any application writer. Use your own client in a real test.

### Mirror an image with snapshot mirroring

```golang
func TestSnapshotMirroring(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	newCluster := func() *ceph.Container {
		cluster, err := ceph.Run(ctx, ceph.DefaultImage,
			rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
		)
		testcontainers.CleanupContainer(t, cluster)
		if err != nil {
			t.Fatal(err)
		}
		return cluster
	}
	source, destination := newCluster(), newCluster()

	sourceFacts, err := ceph.FactsOf(source)
	link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: "volumes",
	}))
	testcontainers.CleanupContainer(t, link) // runs before the clusters' cleanup
	if err != nil {
		t.Fatal(err)
	}
	waitTransmitPeer(ctx, t, link, "volumes", "destination")

	const spec = "volumes/disk"
	write := func() {
		if _, err := link.SourceRBD(ctx, "bench", "--io-type", "write",
			"--io-size", "64K", "--io-total", "2M", "--io-pattern", "rand", spec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := link.SourceRBD(ctx, "create", "--size", "8M",
		"--image-feature", "layering,exclusive-lock", spec); err != nil {
		t.Fatal(err)
	}
	write()

	// Enabling snapshot mirroring takes the first mirror snapshot.
	if err := link.EnableImage(ctx, "disk"); err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitReplayReady(ctx, "disk"); err != nil {
		t.Fatal(err)
	}
	waitReplicated(ctx, t, link, spec, spec)

	// Later writes travel only with the next mirror snapshot.
	write()
	if _, err := link.SourceRBD(ctx, "mirror", "image", "snapshot", spec); err != nil {
		t.Fatal(err)
	}
	waitReplicated(ctx, t, link, spec, spec)
}
```

Snapshot mirroring is asynchronous and copies the image as of each mirror
snapshot. Writes after the latest mirror snapshot are not on the destination.
Instead of explicit snapshots you can let the MGR `rbd_support` module take
them (`rbd mirror snapshot schedule add --pool volumes 1m`), but explicit
snapshots make tests deterministic.

### Mirror an image with journal mirroring

Journal mirroring replays every write. No mirror snapshots are involved.

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
	Mode: rbdmirror.ModeJournal,
}))
testcontainers.CleanupContainer(t, link)
if err != nil {
	t.Fatal(err)
}

// Journal mode needs exclusive-lock; EnableImage adds the journaling feature.
if _, err := link.SourceRBD(ctx, "create", "--size", "8M",
	"--image-feature", "layering,exclusive-lock", "volumes/disk"); err != nil {
	t.Fatal(err)
}
if err := link.EnableImage(ctx, "disk"); err != nil {
	t.Fatal(err)
}
if _, err := link.WaitReplayReady(ctx, "disk"); err != nil {
	t.Fatal(err)
}
write() // any librbd writer
waitReplicated(ctx, t, link, "volumes/disk", "volumes/disk")
```

With pool scope, every image that has the `journaling` feature is mirrored
without `EnableImage` (which is rejected in pool scope):

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
	Scope: rbdmirror.ScopePool, // Mode defaults to journal
}))
// ...
_, err = link.SourceRBD(ctx, "create", "--size", "8M",
	"--image-feature", "layering,exclusive-lock,journaling", "volumes/disk")
```

Pool scope also enrolls existing journaled images, so quiesce their writers
before creating the link.

A stopped receiver does not lose journaled writes: they wait in the source
journal and are replayed when the daemon starts again. While the daemon is
stopped, `ImageStatus` reports `ReplayReady == false` with the same image IDs,
and `WaitReplayReady` ends at the caller's deadline.

### Fail over and fail back between two sites

Planned failover moves the primary image from A to B and back without
`--force`. Each direction needs its own link, so each cluster runs receivers
for the other. Give each cluster one site name and keep it in both links.

```golang
aFacts, err := ceph.FactsOf(a)
forward, err := rbdmirror.Run(ctx, aFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: a, Destination: b, Pool: "volumes",
	SourceSite: "site-a", DestinationSite: "site-b",
}))
testcontainers.CleanupContainer(t, forward)
if err != nil {
	t.Fatal(err)
}
bFacts, err := ceph.FactsOf(b)
reverse, err := rbdmirror.Run(ctx, bFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: b, Destination: a, Pool: "volumes",
	SourceSite: "site-b", DestinationSite: "site-a",
}))
testcontainers.CleanupContainer(t, reverse)
if err != nil {
	t.Fatal(err)
}
waitTransmitPeer(ctx, t, forward, "volumes", "site-b")
waitTransmitPeer(ctx, t, reverse, "volumes", "site-a")

// forward.SourceRBD and reverse.DestinationRBD run on A;
// forward.DestinationRBD and reverse.SourceRBD run on B.
const spec = "volumes/disk"
// ... create the image on A, write, forward.EnableImage(ctx, "disk") ...
waitReplicated(ctx, t, forward, spec, spec)

// Failover A -> B. Stop A's writers first.
if _, err := forward.SourceRBD(ctx, "mirror", "image", "demote", spec); err != nil {
	t.Fatal(err)
}
promote(ctx, t, forward.DestinationRBD, spec) // retries until the demotion arrives

// B is primary. Its changes reach A through the reverse link.
// ... write on B ...
if _, err := reverse.SourceRBD(ctx, "mirror", "image", "snapshot", spec); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, reverse, spec, spec)

// Failback B -> A.
if _, err := reverse.SourceRBD(ctx, "mirror", "image", "demote", spec); err != nil {
	t.Fatal(err)
}
promote(ctx, t, reverse.DestinationRBD, spec)

// A replicates to B again.
// ... write on A ...
if _, err := forward.SourceRBD(ctx, "mirror", "image", "snapshot", spec); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, forward, spec, spec)
```

User snapshots taken on either side travel with the image and keep their
content through both moves. With journal mode the sequence is the same without
the `mirror image snapshot` calls; the promotion retries until the receiver
has consumed the demotion marker from the journal.

When the reverse link is created, A already has a `tx-only` peer for B, which
the forward link's daemon registered. `Run` turns it into `rx-tx`, so the
forward transmission keeps working. Using different site names for the same
cluster in the two links is rejected.

### Recover from split-brain with resync

A forced promotion while the other side is still primary forks the image. Ceph
reports split-brain, and you recover by choosing one side and resynchronizing
the other. This discards the losing side's changes; it is not a merge.

```golang
const spec = "volumes/disk"
// The image is enabled and replicated from A (source) to B (destination).

// Fork it on purpose: stop B's receiver and force-promote B.
grace := 3 * time.Second
if err := link.Stop(ctx, &grace); err != nil { // the initial daemon
	t.Fatal(err)
}
out, err := link.DestinationRBD(ctx, "mirror", "pool", "info", "volumes", "--format", "json")
if err != nil {
	t.Fatal(err)
}
var info struct{ Peers []struct{ UUID string } }
if err := json.Unmarshal(out, &info); err != nil || len(info.Peers) != 1 {
	t.Fatalf("expected one peer: %s %v", out, err)
}
// In snapshot mode B needs a transmit-capable peer to create its own mirror snapshots.
if _, err := link.DestinationRBD(ctx, "mirror", "pool", "peer", "set", "volumes",
	info.Peers[0].UUID, "direction", "rx-tx"); err != nil {
	t.Fatal(err)
}
if _, err := link.DestinationRBD(ctx, "mirror", "image", "promote", "--force", spec); err != nil {
	t.Fatal(err)
}
// ... both sides are primary now; write different data on A and on B ...

// Keep A: demote B, restart its receiver and wait for the split-brain report.
if _, err := link.DestinationRBD(ctx, "mirror", "image", "demote", spec); err != nil {
	t.Fatal(err)
}
if err := link.Start(ctx); err != nil {
	t.Fatal(err)
}
poll(ctx, t, "split-brain report", func() bool {
	out, err := link.DestinationRBD(ctx, "mirror", "image", "status", spec, "--format", "json")
	var status struct{ State, Description string }
	return err == nil && json.Unmarshal(out, &status) == nil &&
		strings.Contains(status.State, "error") &&
		strings.Contains(strings.ToLower(status.Description), "split-brain")
})
if _, err := link.DestinationRBD(ctx, "mirror", "image", "resync", spec); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, link, spec, spec)
```

After the resync, B holds A's data and A's snapshots, B's own snapshots are
gone, and the next mirror snapshot from A replicates normally. Snapshot
mirroring never protects writes made after the latest checkpoint, and nothing
in the module fails over automatically.

### Remove and re-create the receiving peer

```golang
// Remove the destination's receiving peer while the receiver is stopped.
grace := 3 * time.Second
if err := link.Stop(ctx, &grace); err != nil {
	t.Fatal(err)
}
if _, err := link.DestinationRBD(ctx, "mirror", "pool", "peer", "remove", "volumes", peerUUID); err != nil {
	t.Fatal(err)
}
if err := link.Start(ctx); err != nil {
	t.Fatal(err)
}
// The daemon runs, but new checkpoints no longer reach the destination.
// Existing destination data is kept.

if err := link.Rebootstrap(ctx); err != nil { // new peer with a new UUID
	t.Fatal(err)
}
if _, err := link.DestinationRBD(ctx, "mirror", "image", "resync", "volumes/disk"); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, link, "volumes/disk", "volumes/disk")
```

### Configure mirroring before starting receivers

`NoInitialDaemons` sets up policies and the peer but starts no daemon, so you
can prepare data on the source and choose when replication begins.

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
	NoInitialDaemons: true,
}))
testcontainers.CleanupContainer(t, link)
if err != nil {
	t.Fatal(err)
}
status, err := link.ReceiverStatus(ctx) // Ready == false, no daemons
if err != nil {
	t.Fatal(err)
}
_ = status

// ... create images and write on the source; nothing replicates yet ...

if _, err := link.AddDaemon(ctx, "receiver"); err != nil {
	t.Fatal(err) // a non-nil daemon returned with the error is still owned by link
}
if _, err := link.WaitReceiverReady(ctx); err != nil {
	t.Fatal(err)
}
waitTransmitPeer(ctx, t, link, "volumes", "destination")
if err := link.EnableImage(ctx, "disk"); err != nil {
	t.Fatal(err)
}
if _, err := link.SourceRBD(ctx, "mirror", "image", "snapshot", "volumes/disk"); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, link, "volumes/disk", "volumes/disk")
```

The link still owns its two setup containers. `Daemons()` is empty and the
embedded container is nil until the first `AddDaemon`. A freshly configured
link differs from one whose daemons ran and were removed: the source does not
know the destination as a transmit peer until a daemon has connected, which is
why the example waits for it before enabling the image. Pool scope with
journaled images needs neither `EnableImage` nor a mirror snapshot. The option
controls only this link's daemons; it does not prove that no other
`rbd-mirror` serves the pool.

### Run several receivers and survive the loss of one

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
	DaemonCount: 2, // daemons "a" and "b"
}))
testcontainers.CleanupContainer(t, link)
if err != nil {
	t.Fatal(err)
}
status, err := link.WaitReceiverReady(ctx)
if err != nil {
	t.Fatal(err)
}

// Stop the leader and wait until the survivor leads alone.
survivor := "a"
if status.LeaderDaemonName == "a" {
	survivor = "b"
}
grace := 3 * time.Second
for _, d := range link.Daemons() {
	if d.DaemonName == status.LeaderDaemonName {
		if err := d.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
	}
}
if _, err := link.WaitReceiverReady(ctx, survivor); err != nil {
	t.Fatal(err)
}
```

Without names, the stopped daemon stays expected and the wait never succeeds.
While the stopped member is still in the native election, the wait keeps
polling until Ceph drops it. Add a replacement with `AddDaemon` and start a
new wait afterwards; an ongoing wait does not adopt new daemons.

### Mirror several namespace pairs with one set of daemons

One link owns the daemons and the peer for a pool. Additional namespace pairs
in the same pool are configured with native commands and observed through
`BindNamespace`. Different source and destination namespace names need
Ceph 20.

```golang
// The namespaces exist on both clusters (rbd.CreateNamespace on each).
sourceFacts, err := ceph.FactsOf(source)
owner, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{
	Source: source, Destination: destination, Pool: "volumes",
	SourceNamespace: "app", DestinationNamespace: "app-replica",
}))
testcontainers.CleanupContainer(t, owner)
if err != nil {
	t.Fatal(err)
}

// A second pair with the same site names and an explicit remote namespace.
if _, err := owner.SourceRBD(ctx, "mirror", "pool", "enable", "--site-name", "source",
	"volumes/batch", "image", "--remote-namespace", "batch-replica"); err != nil {
	t.Fatal(err)
}
if _, err := owner.DestinationRBD(ctx, "mirror", "pool", "enable", "--site-name", "destination",
	"volumes/batch-replica", "image", "--remote-namespace", "batch"); err != nil {
	t.Fatal(err)
}

view, err := owner.BindNamespace(ctx, "batch", "batch-replica")
if err != nil {
	t.Fatal(err)
}
if _, err := view.WaitReceiverReady(ctx); err != nil {
	t.Fatal(err)
}
_, _ = owner.SourceRBD(ctx, "create", "--size", "8M", "volumes/batch/disk")
_, _ = owner.SourceRBD(ctx, "mirror", "image", "enable", "volumes/batch/disk", "snapshot")
if _, err := view.WaitReplayReady(ctx, "disk"); err != nil {
	t.Fatal(err)
}
waitReplicated(ctx, t, owner, "volumes/batch/disk", "volumes/batch-replica/disk")
```

The two native commands are your own writes; they can leave partial
configuration if the second one fails, and nothing rolls them back.
`BindNamespace` only reads. Separate views can become ready at different
times; their results are not one atomic observation.

## Limitations and release differences

### Scope and behavior

- Replication is asynchronous. Snapshot mode copies the image at mirror snapshots; journal mode replays writes. There is no automatic failover, and split-brain recovery discards the losing side.
- `ReplayReady` and `Ready` are readiness observations. They never prove that a write or checkpoint arrived; compare destination data.
- Pool scope supports journal mode only. `EnableImage` works only in image scope.
- Both clusters of a link must use the same network mode. `InterruptPeerLink` works only in bridge mode.
- `Run` does not reconfigure existing, different policies or site names. Site names are cluster-wide.
- Typed methods check the policies captured at setup before and after each operation. Native changes made concurrently by other clients are not prevented; do not race them with link operations.
- `Terminate` leaves the mirroring policies, peers and Cephx users in the clusters.
- Two clusters plus mirror daemons need noticeably more memory than one cluster. In a small Docker VM, run multi-cluster tests one at a time.

### Squid (Ceph 19.2.x)

Tentacle (20.2.x, `ceph.DefaultImage`) is the default release. Squid works
with these differences, which the fixture handles from the options of `rbd`
in each setup client and each cluster's client toolkit, and from each
cluster's `ClusterFacts.CephVersion` where no tool shows the difference:

| Area | Tentacle | Squid |
| --- | --- | --- |
| Default namespace while mirroring only a named namespace | Set to `init-only`. | Squid has no `init-only`; the default namespace is set to `image` mode, which mirrors nothing until an image is enabled explicitly. |
| Namespace mapping | `SourceNamespace` and `DestinationNamespace` may differ. | Squid has no remote namespaces and its `rbd` no `--remote-namespace`, so only namespaces with the same name are mirrored. A different pair makes `Run` fail before any change with `ceph.ErrUnsupported`; the `*ceph.OperationError` names the side whose `rbd` lacks `--remote-namespace`: a cluster, with its `Version` and client toolkit, or a setup client. A backported Squid image that has the option maps namespaces. The `BindNamespace` example above therefore needs Ceph 20 on both sides. |
| Mirror UUID | `rbd mirror pool info` prints `mirror_uuid` and `remote_namespace`. | Neither is printed. The fixture reads the UUID from the `mirror_uuid` omap key of the `rbd_mirroring` object (`rados --pool volumes [--namespace ns] getomapval rbd_mirroring mirror_uuid /dev/stdout`) and fills the remote namespace with the same name. Do the same if your test reads `mirror pool info` directly. |
| `rbd-mirror` admin socket status | `rbd mirror status` lists each namespace under `namespace_replayers` with its local and remote namespace. | The default namespace's `image_replayers` sit on the pool entry, and named namespaces appear under `namespaces` by name only. `ReceiverStatus` and `WaitReceiverReady` normalize both shapes. `Daemon.Status` decodes only the pool-level fields. |

Client libraries differ too: Squid's Python `rbd` binding has no `Group.id()`
or `get_snap_info()`, so group snapshot tests read the group ID from the
`rbd_group_directory` object and member snapshots from each image.

A refused mapping is the contract on Squid, so assert it instead of skipping
the test:

```golang
sourceFacts, err := ceph.FactsOf(source)
link, err := rbdmirror.Run(ctx, sourceFacts.Images.RBDAux, rbdmirror.WithConfig(config))
if link != nil {
	t.Cleanup(func() { _ = link.Terminate(context.Background()) })
}
var unsupported *ceph.OperationError
switch {
case errors.As(err, &unsupported) && unsupported.Kind == ceph.ErrUnsupported:
	t.Logf("%s (Ceph %s) cannot map namespaces; nothing changed", unsupported.Target, unsupported.Version)
	return
case err != nil:
	t.Fatal(err)
}
```

See [Classified errors](../../docs/guides/errors.md#unsupported-features).

### Common pitfalls

- **Enabling snapshot mirroring too early.** Until a daemon has connected, the source does not know the destination as a transmit peer, and a mirror snapshot taken then is not addressed to it. Wait until `rbd mirror pool info` on the source lists the destination site with direction `tx-only` or `rx-tx` and a non-empty `mirror_uuid` (`waitTransmitPeer` above) before enabling images or taking checkpoints you rely on.
- **Promoting right after a demotion.** A non-forced `promote` fails until the demotion has propagated. Retry it; do not reach for `--force`.
- **Treating readiness as delivery.** `WaitReplayReady` can succeed before your latest checkpoint arrives. Compare data.
- **Waiting on a stopped daemon.** `WaitReceiverReady()` without names expects every owned daemon, including stopped ones. Name the survivors.
- **Cleanup order.** Register the clusters' cleanup before the link's, so the link terminates first. Terminate a non-nil `*Mirror` even after an error.
