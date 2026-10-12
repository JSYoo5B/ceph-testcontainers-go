# Fault injection

## Introduction

Client code that talks to Ceph has to survive slow daemons, failovers, fenced
sessions, full clusters and damaged replicas. This guide lists every way the
module can create such a condition in a disposable cluster, explains what the
client and the other daemons observe, and shows how to undo it.

All techniques but one work with the unmodified official Ceph image and the
Docker API, without privileged containers, extra capabilities or additional
tools in the image. The exception is packet delay below Ceph, an opt-in
extension: a separate netem helper image applies it, and only that helper gets
`NET_ADMIN` (see [Delaying packets below Ceph](#delaying-packets-below-ceph)).

| Condition you want | API | What peers observe | Host network | Undo |
| --- | --- | --- | --- | --- |
| A daemon process exits | `Stop`/`Start` on a daemon handle | Closed connections right away | Yes | `Start` |
| A daemon or client is alive but silent | `ceph.Container.PauseContainer` | No replies, no closed connection; down only after heartbeat grace | Yes | `ContainerPause.Resume` |
| A container loses one Docker network | `ceph.Container.InterruptNetwork` | The endpoint disappears | No | `NetworkInterruption.Restore` |
| Packets that a container sends on one network arrive late | `ceph.Container.TemporaryNetworkDelay` (opt-in netem helper) | Slow replies from that container, connections stay up | No | `NetworkDelayLease.Restore` |
| Replication between two clusters stops | `rbd.Mirror.InterruptPeerLink`, `cephfs.Mirror.InterruptPeerLink`, `rgw.Multisite.InterruptZoneLink` | Local service works, the peer link is gone | No | `NetworkInterruption.Restore` |
| A daemon drops or delays messages from one kind of peer | `TemporaryConfig` with `ms_blackhole_*` or `ms_inject_delay_*` | The daemon stays up and keeps talking to the MON | Yes | `ConfigOverride.Restore` |
| One client session is fenced | `TemporaryBlocklist` | The OSDs reject that session | Yes | `BlocklistOverride.Restore` |
| A failed OSD stays up or in | `TemporaryOSDFlag` (`nodown`, `noout`, ...) | Map flags hold the OSD state | Yes | `OSDFlagOverride.Restore` |
| The cluster reports nearfull, backfillfull or full | `TemporaryFullRatios` | `OSD_NEARFULL`, `OSD_BACKFILLFULL`, `OSD_FULL` | Yes | `FullRatiosOverride.Restore` |
| A replica is damaged | `InjectObjectDataError`, `DeepScrubPG`, `RepairPG` | Scrub errors, `PG_DAMAGED` | Yes | `RepairPG` or OSD restart |
| An OSD leaves the cluster | `RemoveOSD`, `SetOSDIn` | Data moves to the remaining OSDs | Yes | `AddOSD`, `SetOSDIn(..., true)` |
| The MDS evicts a silent CephFS client quickly | `cephfs.Filesystem.TemporarySessionTimeouts` with `PauseContainer` | Eviction and blocklisting after 30 s or more | Yes | `SessionTimeoutsOverride.Restore` |
| An RBD exclusive lock moves to another client | `rbd.ImageClients` with `PauseContainer` and `TemporaryBlocklist` | Lock break and fencing | Yes | Resume, then restore the blocklist |

Each of these conditions ends with the cluster: when the test terminates the
cluster afterwards, the undo step is optional. See [Contexts, ownership and cleanup](contexts-and-cleanup.md)
for the general rules about handles returned together with an error. The
snippets below use `cleanupCtx` for a context that outlives the test's own
context, for example one made with
`context.WithTimeout(context.Background(), time.Minute)`.

## Out of scope: packet loss, bandwidth limits and pairwise filtering

The official `quay.io/ceph/ceph` image contains no `tc`, `ip`, `iptables` or
`nft`, and the Docker network API can only connect or disconnect a whole
container endpoint. Packet delay below Ceph is available as an opt-in
extension with its own helper image (see
[Delaying packets below Ceph](#delaying-packets-below-ceph)). The module does
not offer:

- random packet loss, corruption, duplication, reordering or bandwidth limits;
- delay of incoming packets, or of packets to selected peers only;
- pairwise rules such as "OSD 1 cannot reach OSD 2 but can reach the MON";
- any change in the host namespace, so no packet fault on host networks.

Use one of these instead:

- `InterruptNetwork` to cut one container off one network entirely;
- `WithSeparateClusterNetwork` plus `ceph.ClusterNetworkPlane` to cut only OSD
  replication and heartbeat traffic;
- the Ceph messenger options described below to make one daemon drop or delay
  messages from one kind of peer (clients, OSDs, MONs, MDSs or MGRs).

## Stopping and starting daemons

Every daemon handle embeds `testcontainers.Container`, so `Stop` and `Start`
work on it directly. The process exits and its peers see closed connections
immediately. The container, its Ceph identity and its data are kept.

```golang
osd := cluster.OSDs()[0]
timeout := 10 * time.Second
if err := osd.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Exercise degraded reads and writes here.
if err := osd.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

| Daemon | Handle | Notes |
| --- | --- | --- |
| MON | `cluster.Monitors()[i]` | Use `ceph.WithMonitorCount(3)` or more. With one MON, the MON container is also the control container that runs every fixture command, so stopping it makes every fixture call fail until it starts again. `WaitForQuorum` waits for a majority of the current monmap. |
| MGR | `cluster.Managers()[i]` | Use `ceph.WithManagerCount(2)` so a standby takes over. `ManagerStatus` reports the active name. |
| OSD | `cluster.OSDs()[i]` | Ceph marks a down OSD out after `mon_osd_down_out_interval` (600 s by default). Hold `noout` with `TemporaryOSDFlag` to avoid data movement during long outages. |
| MDS | `fs.MDSs()[i]` | Configure `StandbyMDS` so a standby takes the rank. Use `fs.MDSStatus` to see who holds each rank; `fs.WaitReady` also waits for the requested standby capacity, so restart the stopped daemon before calling it. |
| RGW | `rgw.Gateways(cluster)[i]` | In bridge mode Docker can assign a new mapped port on restart. Call `S3Endpoint` again after `Start`. |
| RBD and CephFS mirror daemons | `mirror.Daemons()[i]` | `rbd.MirrorDaemon.Status` reports the native leader election. |

`Start` does not wait for an OSD to be up and in again. Wait with `WaitForClean`, `OSDStates` or `HealthDetails`
before asserting recovery.

## Freezing a container

`PauseContainer` freezes every process in a container through Docker's cgroup
freezer, like `docker pause`. The process keeps its memory, sockets and Ceph
identity. Peers do not see a closed connection; they only stop getting
replies. Use it to reproduce an unresponsive OSD, MDS or client.

```golang
hold, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
if hold != nil {
	defer hold.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
pause, err := cluster.PauseContainer(ctx, cluster.OSDs()[1])
// A lost Docker reply does not prove the container is still running.
// Resume a non-nil handle even when err is not nil.
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// The client's rados_osd_op_timeout expires; HealthDetails reports SLOW_OPS.
```

Contract:

- The container must be running and not paused. An already paused container is
  refused, because the fixture cannot know who paused it.
- The container that runs the fixture's control CLI is refused: freezing it
  would block every fixture command. In a single-MON cluster this is the MON;
  with several MONs it is a separate control container.
- One pause per container. A second request returns the existing handle and an
  error until that handle is resumed.
- `Resume` is retryable and idempotent. A removed, stopped or already running
  container completes the handle without starting anything. If the state
  cannot be read, `Resume` returns an error and keeps the handle open.
- The cluster keeps every pause handle and resumes it in `Terminate` before
  stopping other containers. Resume a caller-owned client container yourself
  before you terminate it.
- Pausing changes no Ceph policy, flag or setting.

### What Ceph does with a frozen OSD

With two OSDs and a replica-2 pool every PG contains both OSDs, so freezing one
OSD stalls writes for every PG.

- With `nodown` held, the frozen OSD stays up in the OSD map. Client operations
  wait until the client's own `rados_osd_op_timeout` and then fail with
  ETIMEDOUT (errno 110). The surviving primary reports the operations it is
  waiting on as `SLOW_OPS` after `osd_op_complaint_time` (30 s by default);
  lower it with `TemporaryConfig` (section `osd`). The frozen OSD cannot report
  its own slow operations.
- Without `nodown`, peers report the OSD down after the heartbeat grace
  (20 s by default) and degraded I/O resumes once the MON marks it down. The
  MON needs `mon_osd_min_down_reporters` reporters (2 by default). A cluster
  with only two OSDs has one possible reporter, so set
  `mon_osd_min_down_reporters=1` in section `mon` with `TemporaryConfig`;
  otherwise the OSD is only marked down after `mon_osd_report_timeout`
  (900 s by default).
- After `Resume`, the OSD notices it was marked down and rejoins.

Set the client timeout in your own client, for example with
`conf_set("rados_osd_op_timeout", "5")` in the librados bindings or
`--rados-osd-op-timeout 5` on a Ceph CLI.

### Frozen MDS

Freezing an MDS stops its beacons, so after the beacon grace the MON replaces
it with a standby, if one exists. To keep the MDS active while it ignores
clients, use `ms_blackhole_client` instead (see
[Dropping or delaying messages](#dropping-or-delaying-messages)).

### Frozen clients

Pause a client container attached with `WithClient` or `WithClientIdentity` to
reproduce a client that holds resources and stops answering. The server side
reclaims those resources on its own timers:

| Client resource | Observe with | Behaviour |
| --- | --- | --- |
| RADOS watch | `cluster.ObjectWatchers(ctx, pool, namespace, object)` | A notify from another client fails with ETIMEDOUT after its own timeout. The watch expires after `osd_client_watch_timeout` (30 s by default); later notifies succeed at once. No blocklist entry is created. |
| RBD exclusive lock | `rbd.ImageClients` | See [Handing over an RBD exclusive lock](#handing-over-an-rbd-exclusive-lock). |
| CephFS session | `cephfs.Filesystem.Sessions` | See [Evicting a silent CephFS client](#evicting-a-silent-cephfs-client). |

`ObjectWatchers` parses `rados listwatchers`, which ignores `--format json`.
Each `Address` has the form `IP:port/nonce`, the same form used by
`BlocklistEntries` and accepted by `TemporaryBlocklist`.

## Interrupting a network endpoint

`InterruptNetwork` disconnects one container from one of the cluster's bridge
networks. The process keeps running. `Restore` reconnects the original
endpoint with the same IP address, aliases and gateway priority, so the
address Ceph advertised stays valid and no daemon restarts.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithSeparateClusterNetwork())
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
	t.Fatal(err)
}
// Cut only replication and heartbeat traffic of osd.0.
cut, err := cluster.InterruptNetwork(ctx, cluster.OSDs()[0], ceph.ClusterNetworkPlane)
if cut != nil {
	defer cut.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Observe the cluster while the backend link is down.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

Typical uses:

- `ceph.PublicNetworkPlane` on a MON of a 5-MON cluster: two MONs can be cut
  off while the other three keep quorum, then rejoin after `Restore`.
- `ceph.PublicNetworkPlane` on a client container attached with `WithClient`:
  the client loses the whole cluster.
- `ceph.ClusterNetworkPlane` with `WithSeparateClusterNetwork`: the OSD keeps
  serving its public listener while replication and heartbeats on the backend
  fail. With a single bridge, both planes select the same endpoint.

Contract:

- Bridge mode only. Host-network clusters are refused because Docker cannot
  disconnect a host-network endpoint.
- The container may be an owned daemon or a caller-owned `WithClient`
  container. The control container is not refused, but every fixture command
  fails while it is disconnected. `Restore` is a Docker operation and still
  works.
- One interruption per container and network. A second request returns the
  existing handle and an error until it is restored.
- A non-nil handle returned with an error must be restored: a transport error
  does not prove that Docker left the endpoint connected. `Restore` is
  retryable.
- `Restore` refuses to overwrite an endpoint that someone reattached with a
  different address. A removed container needs no restoration.
- The cluster keeps the handle and restores it in `Terminate` before removing
  networks. If restoration fails, `Terminate` keeps that network, reports the
  error and can be retried. Restore a caller-owned container's endpoint before
  you terminate that container.

The package function `ceph.InterruptNetwork(ctx, container, networkName)` does
the same for any named bridge network, but the caller owns the returned handle
and no fixture restores it.

## Delaying packets below Ceph

`TemporaryNetworkDelay` delays every packet that one container sends through
its endpoint on one of the cluster's bridges, with the kernel's netem qdisc.
Ceph does not know about it: the daemon keeps its sessions, and every peer
sees its answers late, while the container's other bridge and the other
daemons are unaffected.

This is an opt-in extension. `NetworkDelay.HelperImage` names an image with
`tc`, `ip` and `python3`, such as `ceph.NetemHelperImage`, which
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md)
publishes for `linux/amd64` and `linux/arm64`. The module starts that image as
a helper container in the target's network namespace and grants `NET_ADMIN` to
the helper alone; the target keeps its own image and capabilities.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
	t.Fatal(err)
}
delay, err := cluster.TemporaryNetworkDelay(ctx, cluster.OSDs()[0], ceph.NetworkDelay{
	HelperImage: ceph.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Delay:       200 * time.Millisecond,
	Jitter:      20 * time.Millisecond,
})
if delay != nil {
	defer delay.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Reads of objects whose primary is osd.0 now take at least 200 ms; reads
// served by osd.1 do not.
status, err := delay.Status(ctx)
if err != nil || !status.Applied {
	t.Fatalf("delay: %+v, %v", status, err)
}
t.Logf("netem delayed %d packets", status.Packets)
if err := delay.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

Before it changes anything, the call checks:

- the target: an owned daemon, or a caller-owned container attached to the
  plane's bridge, running and not paused, with its own network namespace and
  an IPv4 endpoint. Host-network clusters are refused, because a delay there
  would delay the host;
- the helper, in a separate helper container without a network: the
  contract's executables, `tc` with JSON output and netem, `python3`,
  `NET_ADMIN`, and the kernel's netem on that container's loopback. The errors
  wrap `ceph.ErrNetemImageUnsupported`, which belongs to
  `ceph.ErrImageCapability`, or `ceph.ErrNetemPermissionDenied` or
  `ceph.ErrNetemKernelUnsupported`, which belong to
  `ceph.ErrEnvironmentUnavailable`. A request without `HelperImage` has not
  selected the extension and is `ceph.ErrImageCapability` too, and a `tc`
  command that fails otherwise is a `*ceph.NativeFailure` (see
  [Classified errors](errors.md#image-capabilities-and-optional-extensions));
- the interface, found by the endpoint's address and MAC address and then
  followed by its index: its root qdisc must be the kernel's default. A qdisc that someone else installed is
  refused, not replaced, and only one delay per endpoint is held.

What to expect:

- The delay applies to packets that leave the target. A request to the target
  and its answer cross the interface once each way, so the answer arrives at
  least `Delay` late; a TCP connection setup waits for it too. To delay both
  directions between two containers, delay both.
- `Jitter` varies each packet's delay uniformly. With jitter, packets of one
  connection can be reordered, which TCP repairs at some cost.
- `Status` reads the kernel's state through the helper: whether the lease's
  qdisc is the interface's root qdisc, its delay, jitter and packet counters,
  and tc's JSON output. It is native evidence, separate from the requested
  values and from what your client measures.
- The delay ends with the interface. A target that stops, restarts, is removed
  or loses the bridge (also through `InterruptNetwork`) comes back without it,
  and `Status` reports `Gone`; `Restore` then only removes the helper and never
  touches the new namespace.
- `Restore` removes only the lease's own qdisc. A root qdisc that someone
  replaced is refused with an error, and the lease stays retryable. A helper
  that exited or was removed is replaced first, because removing a helper does
  not remove its qdisc from a live namespace. The replacement runs the image
  that was checked, by its ID.
- Every `tc` command runs under the module's command supervisor. A canceled
  call returns its lease, because the change may have reached the kernel; a
  stop that cannot be confirmed fences the lease until `Restore` confirms it.
- The cluster keeps the lease and restores it at the start of `Terminate`.
  Restore a caller-owned container's delay before you remove that container.

The netem helper has been checked on Docker Engine (GitHub-hosted Ubuntu 24.04
runners, `amd64` and `arm64`) and on Docker Desktop on macOS (`arm64`).
Rootless engines, Podman and Docker Desktop's Enhanced Container Isolation may
refuse `NET_ADMIN` or the namespace join; the call then fails with one of the
errors above instead of falling back to another mechanism.

## Interrupting links between clusters

The multicluster fixtures cut the network path between two clusters while both
clusters keep serving their local clients.

| API | Endpoint removed | Kept |
| --- | --- | --- |
| `rbd.Mirror.InterruptPeerLink(ctx, daemonName)` | The receiving `rbd-mirror` daemon's endpoint on the source cluster's public bridge | Its destination endpoint, pool leader election, peer and image policies |
| `cephfs.Mirror.InterruptPeerLink(ctx, daemonName)` | The source `cephfs-mirror` daemon's endpoint on the destination cluster's public bridge | Source filesystem, MGR registration, directory and peer policies |
| `rgw.Multisite.InterruptZoneLink(ctx, zoneName)` | The gateway's endpoint on the private multisite HTTP bridge | The local S3 endpoint, realm and period membership |

```golang
daemon := mirror.Daemons()[0].DaemonName
cut, err := mirror.InterruptPeerLink(ctx, daemon)
if cut != nil {
	defer cut.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Write to the source image; the destination does not advance.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
if _, err := mirror.WaitReplayReady(ctx, "disk"); err != nil {
	t.Fatal(err)
}
// Compare the destination bytes with your own client.
```

The handles are `*ceph.NetworkInterruption` with the same contract as above.
Only owned daemons and zones can be selected, the daemon is never stopped or
restarted, and host-network fixtures are refused. Each fixture restores its
interruptions during its own cleanup. Endpoint restoration does not mean the
backlog has been replicated: wait with `WaitReplayReady`, `WaitSnapshotSynced`
or `WaitSyncReady` and compare the data.

## Dropping or delaying messages

Ceph's messenger has built-in fault options. Set them with `TemporaryConfig`
to make a daemon drop or delay messages from one kind of peer. The options
exist in both the Tentacle and Squid official images, need no container
privileges and work on host networking.

```golang
// osd.1 drops client messages. It keeps talking to its peers and stays up.
hold, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
	Section: fmt.Sprintf("osd.%d", cluster.OSDs()[1].ID),
	Name:    "ms_blackhole_client",
	Value:   "true",
})
if hold != nil {
	defer hold.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Operations on objects whose acting primary is osd.1 fail with ETIMEDOUT
// after the client's rados_osd_op_timeout. Other objects work normally.
```

```golang
// Every OSD delays each client message by a random 0 to 2 seconds.
for _, setting := range []ceph.ConfigSetting{
	{Section: "osd", Name: "ms_inject_delay_type", Value: "client"},
	{Section: "osd", Name: "ms_inject_delay_probability", Value: "1"},
	{Section: "osd", Name: "ms_inject_delay_max", Value: "2"},
} {
	hold, err := cluster.TemporaryConfig(ctx, setting)
	if hold != nil {
		defer hold.Restore(cleanupCtx)
	}
	if err != nil {
		t.Fatal(err)
	}
}
```

| Option | Effect |
| --- | --- |
| `ms_blackhole_client`, `ms_blackhole_osd`, `ms_blackhole_mon`, `ms_blackhole_mds`, `ms_blackhole_mgr` | The daemon discards messages it receives from that kind of peer. The connection stays open, so the sender only notices through its own timeout. |
| `ms_inject_delay_type` | Peer kinds whose messages are delayed, as space-separated entity types such as `client` or `osd`. |
| `ms_inject_delay_probability` | Probability of delaying each message; `1` delays every message. |
| `ms_inject_delay_max` | Maximum delay in seconds. Each delay is chosen uniformly between 0 and this value. |

### Choosing the section

`Section` selects all daemons of a kind (`osd`) or one daemon (`osd.1`). If you
target one OSD, only operations whose acting primary is that OSD are affected.
Find the primary of an object before the test:

```golang
out, err := cluster.Ceph(ctx, "osd", "map", "volumes", "object-01", "--format", "json")
if err != nil {
	t.Fatal(err)
}
var mapping struct {
	PGID          string `json:"pgid"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}
if err := json.Unmarshal(out, &mapping); err != nil {
	t.Fatal(err)
}
```

### An MDS that ignores clients

`ms_blackhole_client` on the active MDS makes it ignore clients while it keeps
sending beacons to the MON. The MDS stays active and no failover happens. New
clients cannot open a session and their mount fails with ETIMEDOUT after
`client_mount_timeout`. This is the way to produce "the MDS is alive but does
not answer clients"; pausing the MDS container produces a failover instead.

```golang
status, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
hold, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
	Section: "mds." + status.Active[0].Name,
	Name:    "ms_blackhole_client",
	Value:   "true",
})
if hold != nil {
	defer hold.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

### Warnings

- Never set `ms_blackhole_client` on MONs (section `mon`, `mon.<name>` or
  `global`). The fixture's control CLI is a client too. If the MON drops client
  messages, `TemporaryConfig` cannot confirm the change and `Restore` cannot
  reach the MON, so nothing can undo it.
- Avoid the same option on MGRs. Fixture commands handled by the MGR
  (subvolumes, MGR modules, PG statistics) stop for the same reason.
- Do not set `ms_blackhole_*` in the `client` section. The fixture's CLI reads
  that section. To inject faults on the client side, put the options in your
  own client's configuration (the librados `conf` arguments or its
  `ceph.conf`).
- Open a new client connection for each fault phase. The documented behaviour
  was verified with clients that connected after the change; whether already
  open connections behave the same way was not checked.
- `ms_inject_socket_failures` is not recommended: small values prevent any
  connection and larger values have no visible effect on short I/O, and the
  outcome varies between runs.
- The effect of these options on multicluster replication paths has not been
  verified.

## Fencing a client session

`TemporaryBlocklist` adds one exact client session to the OSD map blocklist.
The OSDs then reject that session, which reproduces fencing.

```golang
fence, err := cluster.TemporaryBlocklist(ctx, address, 10*time.Minute)
if fence != nil {
	defer fence.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

- `address` must be `IP:port/nonce` with a non-zero nonce, so two clients on
  the same host stay distinct. Get it from the client itself (librados
  `get_addrs` after connecting) or from `ObjectWatchers`, `rbd.ImageClients`
  or `cephfs.Filesystem.Sessions`. A v1/v2 address vector is accepted only if
  every entry has the same IP, port and nonce.
- Bare IPs, CIDR ranges, nonce zero and entries that already exist are refused.
- The duration must be at least one second.
- `Restore` removes only the entry this handle created. It refuses to remove an
  entry whose expiration someone else renewed. An entry that expired on its own
  counts as removed.
- Removing the entry restores OSD admission, but the client library decides
  whether the old session recovers. Most tests reconnect.
- `BlocklistEntries` lists every entry and range, including entries created by
  Ceph itself, without granting ownership of them.

## Holding OSD map flags

`TemporaryOSDFlag` sets or clears one global OSD map flag and remembers the
previous state. Supported flags: `noup`, `nodown`, `noout`, `noin`,
`nobackfill`, `norebalance`, `norecover`, `noscrub`, `nodeep-scrub`,
`nosnaptrim` and `noautoscale`. The composite `pause` flag and irreversible
compatibility flags are not supported.

```golang
flag, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
if flag != nil {
	defer flag.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

Useful combinations:

| Goal | Flags |
| --- | --- |
| A frozen OSD stays up so clients hit their own timeouts | `nodown` with `PauseContainer` |
| A stopped OSD stays in so no data moves | `noout` with `Stop` |
| A placement change or recovery stays incomplete | `nobackfill` and `norecover` together (`nobackfill` alone does not stop log-based recovery of small PGs) |
| No background scrub interferes with a test | `noscrub`, `nodeep-scrub` (they can also keep `DeepScrubPG` from finishing) |

`noout` only suppresses automatic out marking; an explicit `SetOSDIn(ctx, id,
false)` still marks the OSD out. Per-OSD, CRUSH-node and device-class flags
are separate native state; set them with `cluster.Ceph` (for example
`osd add-noout osd.1`) and undo them yourself.

## Making the cluster full

`TemporaryFullRatios` changes the global nearfull, backfillfull and full
thresholds of the OSD map. Very low ratios make a cluster with a little data
report the corresponding health states, without filling a device.

```golang
change, err := cluster.TemporaryFullRatios(ctx, ceph.FullRatios{
	NearFull: .0001, BackfillFull: .0002, Full: .0003,
})
// Several native commands run in sequence; some may have succeeded.
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Poll HealthDetails until Checks contains OSD_FULL, then exercise the client.
```

- Values must be finite fractions with `0 < NearFull < BackfillFull < Full <= 1`
  after conversion to float32, which is how the OSD map stores them. The
  original map must satisfy the same order. Inputs that become equal after
  conversion are refused.
- `FullRatios` reads the current snapshot. Compare against its values rather
  than your float64 literals.
- The thresholds are map policy. OSDs report fullness after their statistics
  propagate, so wait for `OSD_NEARFULL`, `OSD_BACKFILLFULL` or `OSD_FULL` in
  `HealthDetails`.
- On a full map librados makes ordinary writes wait. To observe the OSD's
  ENOSPC (errno 28) or EDQUOT instead, set the `FULL_TRY` operation flag in your
  client. Do not confuse it with `FULL_FORCE`.
- One override per cluster. Restore accepts only the last tuple it applied (or
  either side of one uncertain command) and refuses anything else as an
  outside edit.

## Damaging a replica

`InjectObjectDataError` makes one replica of one object unreadable for scrub
and recovery, using Ceph's `injectdataerr` debug command. No disk is touched
and no OSD stops. Combine it with `DeepScrubPG`, `PGInconsistencies` and
`RepairPG` to test monitoring of `OSD_SCRUB_ERRORS` and `PG_DAMAGED`, client
behaviour on an inconsistent PG, or repair automation.

```golang
// mapping comes from "ceph osd map <pool> <object> --format json" (see above).
replica := mapping.Acting[0]
if replica == mapping.ActingPrimary {
	replica = mapping.Acting[1]
}
if err := cluster.InjectObjectDataError(ctx, "scrub", "victim", replica); err != nil {
	t.Fatal(err)
}
if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
objects, err := cluster.PGInconsistencies(ctx, mapping.PGID)
if err != nil {
	t.Fatal(err)
}
// objects[0].UnionShardErrors is []string{"read_error"}; HealthDetails reports
// OSD_SCRUB_ERRORS and PG_DAMAGED.
if err := cluster.RepairPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
```

- The pool must be replicated; erasure-coded pools are not supported. A pool
  with `PGNum: 1` keeps every object in one PG, which simplifies the test.
- The OSD must be owned by the cluster and in the object's current acting set.
  Object names that are empty, contain NUL or start with `-` are refused.
- The method enables `bluestore_debug_inject_read_err` on that OSD only, as a
  runtime setting that is not stored centrally. The injected error lives in
  OSD memory until repair rewrites the replica or the OSD restarts.
- Injecting into a replica: client reads served by the primary still succeed.
  Deep scrub reports `read_error` on that shard and the PG becomes
  `active+clean+inconsistent`. Repair copies the primary's data back.
- Injecting into the primary: client reads still succeed and the next deep
  scrub reports nothing. Use a replica to leave a lasting inconsistency.
- `DeepScrubPG` and `RepairPG` wait for a deep-scrub stamp newer than the one
  before the request, with no `scrubbing` or `repair` state left. `RepairPG`
  returns an error if the PG is still inconsistent, which happens when no
  authoritative copy exists. If the `noscrub` or `nodeep-scrub` flag is set
  or the PG has no primary, the scrub does not start and the wait fails at the
  shorter of the cluster startup timeout and your context deadline.
- `PGInconsistencies` reads the result of the last deep scrub; it does not
  start a scrub. Shard numbers are -1 in replicated pools.

## Removing an OSD or marking it out

`RemoveOSD(ctx, id)` is a planned removal: it drains the OSD, waits until
`safe-to-destroy`, stops it, waits for it to be down, purges it from Ceph and
removes its container. It refuses the last owned OSD. If the remaining OSDs
cannot hold the data (replica count or capacity), it times out and leaves the
OSD out and reweighted; it does not mark it back in. Each phase checks the
OSD's registration UUID, and a lost purge reply can be retried with a fresh
context. `AddOSD` is refused until a removal has finished.

`SetOSDIn(ctx, id, false)` marks an owned OSD out without stopping it, so data
migrates away while the daemon keeps running. `SetOSDIn(ctx, id, true)` brings
it back. `WaitForPGClean` waits for recovery while an OSD is intentionally
out or down; `WaitForClean` also requires every owned OSD up and in.

For an abrupt loss, `Stop` the OSD instead.

## Evicting a silent CephFS client

The MDS evicts a client that stopped answering after `session_timeout` or
`session_autoclose`, which default to 60 s and 300 s. These are filesystem
(MDS map) settings, so `TemporaryConfig` cannot change them.
`TemporarySessionTimeouts` shortens them for one filesystem.

```golang
fs := cephfs.Filesystems(cluster)[0]
change, err := fs.TemporarySessionTimeouts(ctx, cephfs.SessionTimeouts{
	Timeout:   30 * time.Second,
	Autoclose: time.Minute,
})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
pause, err := cluster.PauseContainer(ctx, client)
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Poll fs.Sessions until the client's session is gone and
// cluster.BlocklistEntries until its Address appears.
```

- Values must be whole seconds, at least 30 s (the MDS monitor rejects
  less) and within int32 range.
- If no other client wants the frozen client's capabilities, its session stays
  `open` until `Autoclose` passes, and is then evicted. With Ceph's default
  `mds_defer_session_stale=true` it is not even marked stale.
- If another client requests those capabilities, the MDS marks the session
  stale as soon as `Timeout` passes and evicts it right away. The other client
  waits roughly that long and then sees only data the frozen client had
  flushed.
- With the default `mds_session_blocklist_on_timeout=true`, eviction also
  blocklists the client address. No `MDS_CLIENT_LATE_RELEASE` warning appears,
  because eviction happens first.
- After `Resume`, the old session fails with ESHUTDOWN (errno 108). A new mount
  gets a new nonce and works.
- The eviction blocklist entry is created by Ceph, not by the fixture;
  `Restore` does not remove it. Ceph drops it after `mds_blocklist_interval`
  (1 hour by default).
- One override per filesystem. `Restore` refuses a value that is neither the
  applied nor the original one, and refuses a filesystem recreated under the
  same name.
- `Sessions` queries every active rank, so a client with sessions on several
  ranks appears once per rank. The fixture's own short libcephfs probes can
  appear too. Find your client by `ID` (the libcephfs instance ID) or
  `Hostname`. On host networking every container shares the host's hostname,
  so use `ID`.

## Handing over an RBD exclusive lock

`rbd.ImageClients` reads the watchers and locks of an image. Together with
`PauseContainer` and `TemporaryBlocklist` it reproduces an unresponsive RBD
client that holds the exclusive lock.

```golang
status, err := rbd.ImageClients(ctx, cluster, "rbd", "", "disk")
if err != nil {
	t.Fatal(err)
}
owner := status.ExclusiveOwner() // nil when nobody holds the lock
pause, err := cluster.PauseContainer(ctx, ownerClient)
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
fence, err := cluster.TemporaryBlocklist(ctx, owner.Address, 10*time.Minute)
if fence != nil {
	defer fence.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Another client now takes the lock almost immediately. Resume the old owner,
// wait until its write fails with errno 108, and only then call fence.Restore.
```

- Without fencing, another client's write waits until the frozen owner's
  watch expires (`osd_client_watch_timeout`, 30 s by default), then breaks the
  lock and blocklists the old owner. With `TemporaryBlocklist` the handover
  takes about a second.
- After `Resume`, the old owner's writes fail with ESHUTDOWN (errno 108).
- Keep the fence until the old owner has observed the blocklist. The exclusive
  lock is cooperative: an unfenced old owner asks the current owner for the
  lock, receives it, and its write succeeds.
- The blocklist entry librbd adds when it breaks a lock is not owned by the
  fixture. It stays until Ceph expires it (1 hour by default).
- Watchers and locks are read with two separate commands, so during a handover
  you can see a new lock without its watch or the reverse. Poll for a stable
  answer.
- `ImageLock.Managed` marks the lock librbd takes for the exclusive-lock
  feature; other locks are advisory locks from `rbd lock add`. Pool, namespace
  and image names may contain letters, digits, `_`, `.` and `-`; an empty
  namespace selects the default namespace.

## Observing the effect

Fault injection is only useful with an observation. These read-only checks
query native state:

| Check | Use it for |
| --- | --- |
| `HealthDetails` | Health codes such as `SLOW_OPS`, `OSD_DOWN`, `OSD_FULL`, `OSD_SCRUB_ERRORS`, `PG_DAMAGED` |
| `OSDStates`, `OSDFlags` | Up/in state and weight of owned OSDs, global flags |
| `PoolPGs` | PG states, up/acting sets and primaries of one pool |
| `QuorumStatus`, `ManagerStatus` | MON quorum and the active MGR |
| `cephfs.Filesystem.MDSStatus`, `Sessions` | Rank ownership, standbys and client sessions |
| `BlocklistEntries`, `ObjectWatchers`, `rbd.ImageClients` | Fencing, watches and locks |

Health and placement reports lag behind the event that caused them. Poll with a
deadline:

```golang
func waitForHealthCheck(ctx context.Context, cluster *ceph.Container, code string) error {
	for {
		snapshot, err := cluster.HealthDetails(ctx)
		if err == nil {
			if _, ok := snapshot.Checks[code]; ok {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}
```

## Related APIs

- [ceph.md](../ceph.md): `PauseContainer`, `InterruptNetwork`,
  `TemporaryConfig`, `TemporaryOSDFlag`, `TemporaryBlocklist`,
  `TemporaryFullRatios`, scrub helpers, `RemoveOSD`, `SetOSDIn`.
- [rbd.md](../rbd.md): `ImageClients`, `Mirror.InterruptPeerLink`.
- [cephfs.md](../cephfs.md): `Filesystem.TemporarySessionTimeouts`,
  `Filesystem.Sessions`, `Mirror.InterruptPeerLink`.
- [rgw.md](../rgw.md): `Multisite.InterruptZoneLink`.
- [Host network and native clients](host-network.md): which faults are
  available in host mode.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): handles returned
  with errors and `Restore` semantics.
- [Running native commands](native-commands.md): anything without a typed API.
