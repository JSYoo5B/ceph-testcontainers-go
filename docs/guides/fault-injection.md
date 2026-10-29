# Fault injection

Network faults and container pauses use `github.com/JSYoo5B/ceph-testcontainers-go/fault` with the
existing `*ceph.Container`; see the [API and migration guide](../fault.md).

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
| A daemon or client is alive but silent | `fault.Pause` | No replies, no closed connection; down only after heartbeat grace | Yes | `fault.PauseLease.Resume` |
| A container loses one Docker network | `fault.InterruptNetwork` | The endpoint disappears | No | `NetworkInterruption.Restore` |
| Packets that a container sends on one network arrive late | `fault.ApplyNetworkProfile` (opt-in netem helper) | Slow replies from that container, connections stay up | No | `NetworkProfileLease.Restore` |
| A container sends on one network at a limited bandwidth | `fault.ApplyNetworkProfile` with `NetworkProfile.Rate` (opt-in netem helper) | Large transfers from that container progress at the limit, connections stay up | No | `NetworkProfileLease.Restore` |
| Packets that a container sends arrive reordered, duplicated or corrupted | `fault.ApplyNetworkProfile` with `NetworkProfile.Reorder`, `Duplicate` or `Corrupt` (opt-in netem helper) | The receiver's TCP reorders, discards duplicates and drops corrupted segments for retransmission; connections stay up | No | `NetworkProfileLease.Restore` |
| Replication between two clusters stops | `rbdmirror.Mirror.InterruptPeerLink`, `cephfsmirror.Mirror.InterruptPeerLink`, `multisite.Fixture.InterruptZoneLink` | Local service works, the peer link is gone | No | `NetworkInterruption.Restore` |
| A daemon drops or delays messages from one kind of peer | `override.SetConfig` with `ms_blackhole_*` or `ms_inject_delay_*` | The daemon stays up and keeps talking to the MON | Yes | `ConfigHandle.Restore` |
| An OSD runs a client's write after the client gave up | `override.SetConfig` with `osd_debug_inject_dispatch_delay_*` on that OSD | The client's call ends with ETIMEDOUT; the OSD runs the write later, or discards it if the client disconnected first | Yes | `ConfigHandle.Restore` |
| One client session is fenced | `override.AddBlocklist` | The OSDs reject that session | Yes | `override.BlocklistHandle.Restore` |
| A failed OSD stays up or in | `override.SetOSDFlag` (`nodown`, `noout`, ...) | Map flags hold the OSD state | Yes | `override.OSDFlagHandle.Restore` |
| The cluster reports nearfull, backfillfull or full | `override.SetFullRatios` | `OSD_NEARFULL`, `OSD_BACKFILLFULL`, `OSD_FULL` | Yes | `override.FullRatiosHandle.Restore` |
| A replica is damaged | `fault.InjectObjectDataError`, `DeepScrubPG`, `RepairPG` | Scrub errors, `PG_DAMAGED` | Yes | `RepairPG` or OSD restart |
| An OSD leaves the cluster | `OSDContainer.Remove`, `OSDContainer.SetIn` | Data moves to the remaining OSDs | Yes | `AddOSD`, `OSDContainer.SetIn(..., true)` |
| The MDS evicts a silent CephFS client quickly | `cephfs.Filesystem.OverrideSessionTimeouts` with `fault.Pause` | Eviction and blocklisting after 30 s or more | Yes | `SessionTimeoutsOverrideHandle.Restore` |
| An RBD exclusive lock moves to another client | `rbd.ImageClients` with `fault.Pause` and `override.AddBlocklist` | Lock break and fencing | Yes | Resume, then restore the blocklist |

Each of these conditions ends with the cluster: when the test terminates the
cluster afterwards, the undo step is optional. See [Contexts, ownership and cleanup](contexts-and-cleanup.md)
for the general rules about handles returned together with an error. The
snippets below use `cleanupCtx` for a context that outlives the test's own
context, for example one made with
`context.WithTimeout(context.Background(), time.Minute)`.

## Out of scope: packet faults without the netem extension

The official `quay.io/ceph/ceph` image contains no `tc`, `ip`, `iptables` or
`nft`, and the Docker network API can only connect or disconnect a whole
container endpoint. With the official image alone, the module therefore offers
no packet fault below Ceph. The opt-in netem extension, with its own helper
image, delays, drops, rate-limits, reorders, duplicates or corrupts the
packets that a container sends, on the whole interface or only to selected
peers (see
[Delaying packets below Ceph](#delaying-packets-below-ceph)). Pairwise faults
such as "OSD 1 cannot reach OSD 2 but can reach the MON" need that extension.

Neither the baseline nor the extension offers:

- faults or shaping on the receiving side. To delay, drop, rate-limit,
  reorder, duplicate or corrupt the packets that a container receives from a
  peer, apply the fault to the peer's endpoint with the container's address;
- firewall rules, TCP resets, socket destruction or packet capture;
- any change in the host namespace, so no packet fault on host networks.

Without the extension, use one of these instead:

- `InterruptNetwork` to cut one container off one network entirely;
- `NetworkConfig.WithSeparateClusterNetwork` plus `ceph.ClusterNetworkPlane` to cut only OSD
  replication and heartbeat traffic;
- the Ceph messenger options described below to make one daemon drop or delay
  messages from one kind of peer (clients, OSDs, MONs, MDSs or MGRs).

## Stopping and starting daemons

Every daemon handle embeds `testcontainers.Container`, so `Stop` and `Start`
work on it directly. The process exits and its peers see closed connections
immediately. The container, its Ceph identity and its data are kept.

```golang
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
osd := osds[0]
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
| MON | element `i` of `cluster.Monitors(ctx)` | Use `ceph.WithMonitorCount(3)` or more. With one MON, the MON container is also the control container that runs every fixture command, so stopping it makes every fixture call fail until it starts again. `WaitForQuorum` waits for a majority of the current monmap. |
| MGR | element `i` of `cluster.Managers(ctx)` | Use `ceph.WithManagerCount(2)` so a standby takes over. `ManagerStatus` reports the active name. |
| OSD | element `i` of `cluster.OSDs(ctx)` | Ceph marks a down OSD out after `mon_osd_down_out_interval` (600 s by default). Hold `noout` with `override.SetOSDFlag` to avoid data movement during long outages. |
| MDS | `fs.MDSs()[i]` | Configure `StandbyMDS` so a standby takes the rank. Use `fs.MDSStatus` to see who holds each rank; `fs.WaitReady` also waits for the requested standby capacity, so restart the stopped daemon before calling it. |
| RGW | element `i` of `rgw.Gateways(ctx, cluster)` | In bridge mode Docker can assign a new mapped port on restart. Call `S3Endpoint` again after `Start`. |
| RBD and CephFS mirror daemons | `mirror.Daemons()[i]` | `rbdmirror.Daemon.Status` reports the native leader election. |

`Start` does not wait for an OSD to be up and in again. Wait with `WaitForClean`, `OSDStates` or `HealthDetails`
before asserting recovery.

## Freezing a container

`fault.Pause` freezes every process in a container through Docker's cgroup
freezer, like `docker pause`. The process keeps its memory, sockets and Ceph
identity. Peers do not see a closed connection; they only stop getting
replies. Use it to reproduce an unresponsive OSD, MDS or client.

```golang
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoDown, true)
// hold is never nil: after a refusal its Restore does nothing.
defer hold.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
pause, err := fault.Pause(ctx, cluster, osds[1])
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
  lower it with `override.SetConfig` (section `osd`). The frozen OSD cannot report
  its own slow operations.
- Without `nodown`, peers report the OSD down after the heartbeat grace
  (20 s by default) and degraded I/O resumes once the MON marks it down. The
  MON needs `mon_osd_min_down_reporters` reporters (2 by default). A cluster
  with only two OSDs has one possible reporter, so set
  `mon_osd_min_down_reporters=1` in section `mon` with `override.SetConfig`;
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
| RADOS watch | `Observer.ObjectWatchers(ctx, pool, namespace, object)` | A notify from another client fails with ETIMEDOUT after its own timeout. The watch expires after `osd_client_watch_timeout` (30 s by default); later notifies succeed at once. No blocklist entry is created. |
| RBD exclusive lock | `rbd.ImageClients` | See [Handing over an RBD exclusive lock](#handing-over-an-rbd-exclusive-lock). |
| CephFS session | `cephfs.Filesystem.Sessions` | See [Evicting a silent CephFS client](#evicting-a-silent-cephfs-client). |

`ObjectWatchers` parses `rados listwatchers`, which ignores `--format json`.
Each `Address` has the form `IP:port/nonce`, the same form used by
`BlocklistEntries` and accepted by `override.AddBlocklist`.

## Interrupting a network endpoint

`InterruptNetwork` disconnects one container from one of the cluster's bridge
networks. The process keeps running. `Restore` reconnects the original
endpoint with the same IP address, aliases and gateway priority, so the
address Ceph advertised stays valid and no daemon restarts.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()))
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
	t.Fatal(err)
}
// Cut only replication and heartbeat traffic of osd.0.
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
cut, err := fault.InterruptNetwork(ctx, cluster, osds[0], ceph.ClusterNetworkPlane)
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
- `ceph.ClusterNetworkPlane` with `NetworkConfig.WithSeparateClusterNetwork`: the OSD keeps
  serving its public listener while replication and heartbeats on the backend
  fail. With two OSDs that lasts, because one reporter is not enough to mark an
  OSD down (`mon_osd_min_down_reporters` is 2). With three or more OSDs on
  separate hosts, both peers report it after `osd_heartbeat_grace` (20 s) and
  the MON marks it down; the OSD then fails to rebind its cluster messenger to
  the missing address, logs `rebind cluster_messenger failed` and exits. Call
  `Restore`, then `Start` on its handle; it comes up from a later epoch. See
  `TestOSDHeartbeatFailure`. With a single bridge, both planes select the same endpoint.

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

The package function `fault.InterruptLink(ctx, container, networkName)` does
the same for any named bridge network, but the caller owns the returned handle
and no fixture restores it.

## Delaying packets below Ceph

`ApplyNetworkProfile` delays every packet that one container sends through
its endpoint on one of the cluster's bridges, with the kernel's netem qdisc.
Ceph does not know about it: the daemon keeps its sessions, and every peer
sees its answers late, while the container's other bridge and the other
daemons are unaffected.

This is an opt-in extension. `NetworkProfile.HelperImage` names an image with
`tc`, `ip` and `python3`, such as `fault.NetemHelperImage`, which
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
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
delay, err := fault.ApplyNetworkProfile(ctx, cluster, osds[0], fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
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
  an IPv4 endpoint, and an IPv6 address for an IPv6 peer. Host-network clusters are refused, because a delay there
  would delay the host;
- the helper, in a separate helper container without a network: the
  contract's executables, `tc` with JSON output and netem, `python3`,
  `NET_ADMIN`, and the kernel's netem on that container's loopback. With a
  `Rate` or a packet fault, it also adds and removes netem with that option
  there and checks the values that the kernel reports back. With
  peers, it also adds and removes a `prio` qdisc with a u32 filter there, so
  a kernel without either is refused before the target changes. The errors
  wrap `fault.ErrNetemImageUnsupported`, which belongs to
  `ceph.ErrImageCapability`, or `fault.ErrNetemPermissionDenied` or
  `fault.ErrNetemKernelUnsupported`, which belong to
  `ceph.ErrEnvironmentUnavailable`. A request without `HelperImage` has not
  selected the extension and is `ceph.ErrImageCapability` too, and a `tc`
  command that fails otherwise is a `*ceph.NativeFailure` (see
  [Classified errors](errors.md#image-capabilities-and-optional-extensions));
- the interface, found by the endpoint's address and MAC address and then
  followed by its index: its root qdisc must be the kernel's default. A qdisc that someone else installed is
  refused, not replaced, and only one delay per endpoint is held.

### Selected peers

With `NetworkProfile.Peer`, only the packets that the target sends to that
address are delayed, and with `PeerPort` only TCP packets to that destination
port. The lease owns a `prio` root qdisc, a netem qdisc in its third band and
one u32 filter that selects the peer's packets; everything else uses the first
band without delay. `TestNetworkDelay/peer` delays osd.0's packets to a client
container by 200 ms ± 20 ms: that client's reads from osd.0 took a median of
220 ms, its reads from osd.1 0.4 ms, and the control container's reads from
osd.0 0.1 ms; netem counted only the selected packets. `TestNetworkDelayPeerIPv6`
does the same through the IPv6 filter on an IPv6 cluster. The filter matches
the IP header without options or extension headers, which Ceph's TCP traffic
does not use.

`NetworkProfile.Peers` selects several peers in one lease, each a
`fault.NetworkPeer` with an address and an optional TCP port, up to
`fault.MaxNetworkPeers`. Only one delay is held per endpoint, so a daemon's
paths to several peers are faulted with one delay that lists them all. The
lease owns one u32 filter per peer, all leading to the same netem child: the
IPv4 peers in preference 10 and u32 table `800:`, with handles `800::1`,
`800::2` and so on, and the IPv6 peers in the next preference and table, `11`
and `801:`, or `10` and `800:` without IPv4 peers. The filters are added after
the root and the child, one by one, and `Status` reports `Applied` only when
every filter is there with exactly its keys. An apply that stops after some of
them returns the lease, and `Restore` removes the root with the filters that
it has.

```golang
// The MDS loses both OSDs; clients and the MON still reach it.
cut, err := fault.ApplyNetworkProfile(ctx, cluster, mds, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peers:       []fault.NetworkPeer{{Addr: osd0}, {Addr: osd1}},
	Loss:        100,
})
```

The native tests check this on Tentacle and Squid, with the official image and
the role images:

- `TestNetworkDelay/peers_blackhole` drops osd.0's packets to two client
  containers with one lease: the kernel shows the filters `800::1` and
  `800::2`, both clients' reads from osd.0 fail with `ETIMEDOUT` after 5 s,
  while the first client's read from osd.1 and the control container's read
  from osd.0 take under a millisecond, and both clients read again after
  `Restore`.
- `TestNetworkDelayPeerIPv6` also delays, with one lease, osd.0's packets to
  the client's IPv6 address and to the control container's IPv4 address. The
  kernel shows `ip/10/800::1` and `ipv6/11/801::1`. The client's reads over
  IPv6 took a median of 206 to 212 ms, and the control container's 0.1 ms. A
  TCP connect from osd.0's namespace to the control container's IPv4 address
  was refused after 194 to 203 ms, and one to the client's IPv4 address after
  0.1 ms.
- `TestCephFSMDSOSDsPathCut` drops the MDS's packets to both OSDs of a
  two-OSD filesystem while the clients reach both; see
  [MDS sessions](messenger-client-fixtures.md#mds-sessions).

A delay selects the direction in which its target sends. To fault the other
direction of a path, delay the peer's endpoint with the target's address; to
fault both, hold one delay on each endpoint. `TestCephFSClientMDSPathCut`
cuts each direction of a client-MDS path this way.

`Loss` drops a percentage of the selected packets; with `Loss: 100` and a
`Peer`, and no `Delay`, nothing that the target sends to that peer arrives.
`TestNetworkDelay/peer_blackhole` drops osd.0's packets to a client container:
that client's read from osd.0 failed with `ETIMEDOUT` after its 5-second
`rados_osd_op_timeout`, while its read from osd.1 and the control
container's read from osd.0 took under a millisecond, and after `Restore`
the client read the object again. Only one direction is dropped: the
client's requests still reach osd.0, so an operation that changes data may
have run although its reply was lost. Read the outcome back with a client
of its own before treating it as failed.

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
  replaced, or a qdisc or filter that someone added under the lease's root, is
  refused with an error, and the lease stays retryable. A helper
  that exited or was removed is replaced first, because removing a helper does
  not remove its qdisc from a live namespace. The replacement runs the image
  that was checked, by its ID.
- Every `tc` command runs under the module's command supervisor. A canceled
  call returns its lease, because the change may have reached the kernel; a
  stop that cannot be confirmed fences the lease until `Restore` confirms it.
  The module never runs a `tc` command again by itself.
- The cluster keeps the lease and restores it at the start of `Terminate`.
  Restore a caller-owned container's delay before you remove that container.

### Bandwidth limits

`NetworkProfile.Rate` limits the bandwidth of the packets that the target sends
through its endpoint, on the whole interface or only to the selected peers,
with netem's rate in the lease's netem qdisc. A `Rate` composes with `Delay`
and `Loss` in the same qdisc, and `Delay` may then be zero. The lease, its
selection, its checks and `Restore` are those of any other delay.

```golang
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
limit, err := fault.ApplyNetworkProfile(ctx, cluster, osds[0], fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Rate:        16 * fault.MegabitPerSecond,
})
if limit != nil {
	defer limit.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
status, err := limit.Status(ctx)
if err != nil || !status.Applied || status.Rate != 16*fault.MegabitPerSecond {
	t.Fatalf("rate limit: %+v, %v", status, err)
}
```

How the limit behaves:

- netem paces packets: each leaves only after the previous one has taken its
  length over the rate. There is no burst allowance. The length includes the
  Ethernet header, and a TCP segmentation offload packet counts with all of
  its segments, so an application's throughput stays below `Rate`.
- Packets waiting for the rate, and for a `Delay`, share one queue of
  `QueueLimit` packets (`fault.DefaultNetworkQueueLimit`, 1000, when zero).
  A packet that finds the queue full is dropped and counted in `Drops`. A
  large queue at a low rate adds latency to every selected packet, including
  Ceph's own heartbeats when the selection carries them.
- `Rate` accepts `fault.MinNetworkRate` (64 Kbit/s) to `fault.MaxNetworkRate`
  (10 Gbit/s) in whole bytes per second. Below about 1 Mbit/s, one 64 KiB
  offload packet already takes over half a second, so Ceph sessions with
  short timeouts may fail; that is the limit working, not the module.
- `Status` separates three things. The lease's `Rate` and `QueueLimit` are
  the request. The status's `Rate`, `QueueLimit`, `Qdiscs` and `Filters` are
  the configuration that the kernel reports. `Packets`, `Bytes`, `Drops`,
  `Overlimits`, `Requeues`, `Backlog` and `QueueLength` are netem's counters
  and queue. None of them is a throughput: time the workload yourself.
- A `Rate` needs a `tc` that parses netem's `rate` and a kernel whose netem
  keeps it. The preflight helper checks both on its own loopback before the
  target changes: a `tc` that cannot parse it is `ErrNetemImageUnsupported`,
  and a kernel that does not report the rate and queue limit back is
  `ErrNetemKernelUnsupported`. The module installs no package and never falls
  back to another mechanism. `fault.NetemHelperImage` parses it.
- The limit shapes what the target sends. Limit the other direction on the
  peer's endpoint, with the target's address.

The native tests `TestNetworkBandwidth` and `TestNetworkBandwidthIPv6` limit
osd.0's packets to a client container to 16 Mbit/s (2 MB/s) and read one
16 MiB object through a single-replica pool whose primary is osd.0. On
Docker Engine with x86_64 hosts, with Tentacle 20.2.4 and Squid 19.2.5, both
as the official image and as the role images:

- before the limit, the client read it at 550 to 835 MB/s; through the limit
  at 1.88 to 1.91 MB/s, and again at 585 to 820 MB/s after `Restore`. The
  tests accept 40 to 110 percent of the rate under the limit and at least four
  times the rate without it;
- the control container read the same object from the same OSD at 570 to
  850 MB/s while the client's path was limited: the unselected peer is not
  limited;
- netem's child sent 17.5 to 17.8 MB for the 16 MiB read through the selected
  filter, headers included, without drops. On the IPv6 cluster this is the
  IPv6 filter; a second lease
  that selects the client's IPv4 address on the same cluster sent 0 bytes and
  left the read unlimited, so the family that Ceph used is observed, not
  inferred from the configuration;
- with a 20 ms `Delay` in the same lease the read stayed bounded at the same
  rate, and without a `Peer` both the client's and the control container's
  reads were bounded;
- after every `Restore` the root qdisc is the kernel's default again and no
  helper is left.

#### Severe limits

A limit far below what a transfer needs does not fail the transfer by
itself. netem keeps pacing the packets, so the connection stays up and the
data keeps arriving slowly. Whether the client fails depends on the client's
own timeouts. A librados client with `rados_osd_op_timeout` gives up on an
operation after that many seconds and returns `ETIMEDOUT` (errno 110); the
default, 0, waits without limit. Each profile below writes its objects before
any limit and only reads under a limit, so a failed operation can be read
again safely after `Restore`. `TestNetworkBandwidthSevere` measured, in the
same environments:

- Operation timeout: with osd.0's packets to the client limited to
  `fault.MinNetworkRate` (64 Kbit/s), a client session whose
  `rados_osd_op_timeout` is 10 s read a 1 MiB object, which needs about
  131 s at that rate. The read failed with `ETIMEDOUT` after 10.0 s. By then
  netem had sent about 80 KB of the reply and still held about 67 KB in 23
  packets in its queue (`Backlog`, `QueueLength`). A fresh read after
  `Restore` returned the exact object in a few milliseconds.
- Extreme slowdown: at the same rate, a client without an operation timeout
  read a 256 KiB object in about 34.5 s, against 32.8 s for its payload alone
  and under a millisecond before the limit. The control container read it
  from the same OSD in about a millisecond during the limit. The bytes were
  exact.
- Queue overflow: at 100 Mbit/s with a 20 ms `Delay`, the 1 MiB read took
  0.18 s with the default queue of 1000 packets and no drops. With a
  `QueueLimit` of 2, which cannot hold the 250 KB that the path keeps in
  flight, netem dropped 137 to 185 packets, TCP retransmitted them, and the
  same read took 4.8 to 7.1 s (27 to 40 times as long) and returned the exact
  object. The test
  requires drops and at least 1.25 times the default queue's time.

#### Replication under a limit

A limit on the link between a replicating daemon and the other cluster slows
replication without stopping it: a backlog builds up, the daemon keeps
copying at the limit, and after `Restore` the destination catches up. Select
the link the same way as for a [cut](#interrupting-links-between-clusters):
the source OSD's packets to the daemon's address on the source network carry
the daemon's reads of the source image, so a `Peer` limit there slows the
copy and nothing else.

```go
inspect, err := daemon.Inspect(ctx) // mirror.Daemons()[0]
if err != nil {
	return err
}
sourceFacts, err := ceph.FactsOf(source)
peer := inspect.NetworkSettings.Networks[sourceFacts.NetworkName].IPAddress
sourceOSDs, err := source.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
lease, err := fault.ApplyNetworkProfile(ctx, source, sourceOSDs[0].Container, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        peer,
	Rate:        2 * fault.MegabitPerSecond,
})
```

Read the backlog and the progress from the mirror's own status, not from the
daemon's readiness. While rbd-mirror syncs a snapshot, the description of
`rbdmirror.Mirror.ImageStatus` carries `replay_state` `syncing`, the
`syncing_snapshot_timestamp`, a `syncing_percent` of the image's objects and
the measured `bytes_per_second`. The daemon writes its status to the cluster
only every 30 seconds, so poll it read-only over windows longer than that.
After `Restore`, check the destination's bytes with the destination's own
client; the status reports the snapshot in sync (`local_snapshot_timestamp`
equal to `remote_snapshot_timestamp`, `replay_state` `idle`) up to 30 seconds
later.

`TestRBDMirrorBandwidthCatchUp` limits osd.0's packets to the destination's
rbd-mirror daemon to 2 Mbit/s (250 KB/s) on a snapshot-mode mirror between two
single-OSD clusters. The source client then rewrites the whole 48 MiB image
(256 KiB objects) and creates a mirror snapshot. In the same environments as
above:

- the source client read the image back through the same OSD at 174 to 185
  MB/s while the daemon's path was limited: the unselected path is not
  limited;
- the status reported the new snapshot syncing at 7 to 9 percent 26 to 34 s
  after the snapshot and at 22 to 23 percent after 56 to 60 s, at 236 to
  245 KB/s, with an estimated 205 to 213 s left. The test requires two status
  updates with a growing percentage below 100, and that each percentage's
  bytes fit the limit since the snapshot;
- the destination's SHA-256 still differed from the source's, and netem's
  child had sent 14.0 to 14.9 MB to the daemon without drops, more than the
  reported percentage of the image;
- within 5 s of `Restore` the destination's exported image had the source's
  SHA-256 and exact bytes. The status reported the snapshot in sync 26 to
  31 s after `Restore`, with all 50331648 bytes copied once in a sync of 57
  to 61 s.

Bandwidth limits share the netem helper's requirements and unsupported
environments listed below. Shaping a host-network target, or what a target
receives, is not supported.

The netem helper has been checked on Docker Engine (GitHub-hosted Ubuntu 24.04
runners, `amd64` and `arm64`) and on Docker Desktop on macOS (`arm64`).
Rootless engines, Podman and Docker Desktop's Enhanced Container Isolation may
refuse `NET_ADMIN` or the namespace join; the call then fails with one of the
errors above instead of falling back to another mechanism.

### Packet faults

`NetworkProfile.Reorder`, `Duplicate` and `Corrupt` add netem's packet faults
to the lease's netem qdisc. Each is a `fault.NetworkPacketFault` with a
`Percent` of the selected packets and an optional `Correlation`. The lease,
its selection, its checks and `Restore` are those of any other delay.

```golang
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
fault, err := fault.ApplyNetworkProfile(ctx, cluster, osds[0], fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Duplicate:   fault.NetworkPacketFault{Percent: 50},
})
if fault != nil {
	defer fault.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
status, err := fault.Status(ctx)
if err != nil || !status.Applied || status.Duplicate != (fault.NetworkPacketFault{Percent: 50}) {
	t.Fatalf("duplicate: %+v, %v", status, err)
}
```

The supported combinations are those whose installed state the kernel
reports back and a receiver can attribute:

- `Reorder` sends the chosen packets at once, ahead of the packets that wait
  for the `Delay`, so it needs a `Delay`. It refuses `Jitter`, which
  reorders packets by itself so the two could not be told apart, and `Rate`,
  because netem sends the reordered packets past the rate limit. It works
  with `Loss`, `Duplicate` and `Corrupt`.
- `Duplicate` and `Corrupt` work with every other field, and `Delay` may then
  be zero.

What they do to TCP, and what that means for a test:

- netem decides per packet that it queues, and a TCP segmentation offload
  packet carries many segments. A duplicated offload packet duplicates all
  of its segments. A corrupted one is split first and one segment is
  corrupted. The same `Percent` therefore affects a different number of
  segments depending on how large the sender's packets are, which a delay or
  losses change.
- The receiving kernel absorbs the faults before Ceph sees the bytes. It puts
  reordered segments back in order (`TcpExtTCPOFOQueue` counts each segment
  that waited), discards a segment that it already has and answers it with a
  D-SACK (`TcpExtTCPDSACKOldSent`), and drops a segment whose TCP checksum
  fails (`TcpInCsumErrors`), which the sender retransmits. Reordering also
  makes the sender retransmit segments that were only late, so a reorder
  shows D-SACKs too.
- `Corrupt` flips one bit in what the kernel holds in the packet's first
  buffer: the Ethernet, IP and TCP headers, and payload bytes only when they
  are in that buffer. netem first computes the checksum that the device
  would have offloaded (`tx-checksum` was on in every environment below), so
  the receiver checks it in software. A packet with a flipped bit in its
  IPv4 header never reached the receiver's IP layer in these environments
  (`IpExtInCsumErrors` and `IpInHdrErrors` stayed 0). The host probably
  dropped it while validating IPv4 headers on the bridge, but the tests do
  not read the host's counters, so that is not verified. On IPv6,
  which has no header checksum, a flipped destination address reached the
  receiver and was counted in `Ip6InAddrErrors`. A flipped Ethernet address
  sends the packet elsewhere on the bridge.
- A Ceph operation that returns the exact bytes therefore does not show
  that any packet was altered, and a counter that moved does not show that
  an altered Messenger frame reached Ceph's parser: TCP's checksum and
  sequence numbers stop both. `Status` reports the configuration that the
  kernel installed, and with a `Reorder` netem's `Requeues` counts the
  packets that it sent ahead; neither is proof of what the receiver got.
  Read the receiver's own counters, from `/proc/net/netstat`, `/proc/net/snmp`
  and `/proc/net/snmp6` in its network namespace, before and after a bounded
  window, and compare them with an unselected receiver and with a window
  without a fault. Receivers count a few reordered segments and D-SACKs
  without any fault, so expect a margin, not zero, and never an exact count
  of random events.
- The preflight helper adds and removes netem with each requested fault and
  a correlation on its own loopback and checks what the kernel reports
  back. A `tc` that cannot parse a fault is `ErrNetemImageUnsupported`, and
  a kernel that does not report the fault, its correlation or a reorder's
  gap of 1 is `ErrNetemKernelUnsupported`. The apply compares the target's
  installed faults with the request the same way and returns the lease with
  an error on any difference. iproute2 6.1, the `tc` of
  `fault.NetemHelperImage`, sends a duplicate's correlation only together
  with a delay's correlation, so the lease passes a zero delay correlation
  with a `Duplicate.Correlation`; without that, the preflight would see the
  correlation missing.

The native tests `TestNetworkPacketFaults` and `TestNetworkPacketFaultsIPv6`
fault osd.0's packets to a client container, on an IPv4 and an IPv6
cluster, and read a 4 MiB object from a single-replica pool whose primary is
osd.0. Each window reads the object from the client and from the control
container, which the selection does not include, and helpers in the
client's, the control container's and osd.0's namespaces read the kernel
counters before and after. The client must count at least 10 of the effect
and 4 times what the control container counted in the same window and what
either receiver counted in four reads without a fault. Every read returns
the exact SHA-256. On a native Linux Docker Engine on x86_64,
with Tentacle 20.2.4 and Squid 19.2.5, both as the official image and as the
role images:

- without a fault, four reads counted 0 to 2 out-of-order segments, 0 to 2
  D-SACKs and no checksum failure at either receiver. An earlier run counted
  19 D-SACKs at the control container without any fault, so the test
  compares margins instead of expecting zero;
- `Reorder` 25 percent with a 10 ms `Delay`: one read took 5.3 to 6.5 s,
  against 8 ms without the fault. The client queued 274 to 339 segments out
  of order, the control container 0 or 1. netem reported 468 to 570
  `Requeues`, and osd.0 counted reordering (`TcpExtTCPSACKReorder`) and
  retransmitted segments that were only late, which the client answered with
  D-SACKs;
- `Duplicate` 50 percent: four reads took about 8 ms each, and the client
  sent 179 to 212 D-SACKs, the control container 0 or 1;
- `Corrupt` 5 percent: in eight reads of 7 to 266 ms the client counted 67
  to 132 TCP checksum failures and the control container none. The client
  also queued segments out of order behind each dropped one, and on IPv6
  counted 21 to 29 packets with a corrupted destination address
  (`Ip6InAddrErrors`). No IPv4 header checksum failure reached it;
- `Duplicate` 30 percent with a 25 percent correlation and `Corrupt`
  5 percent, with a 5 ms `Delay` and a 256 Mbit/s `Rate` in the same lease
  (IPv4 only): in two reads of 0.8 to 2.7 s the client sent 454 to 592
  D-SACKs and counted 112 to 131 checksum failures, the control container
  0 or 1 D-SACKs and no checksum failure;
- after every `Restore` the root qdisc is the kernel's default again, no
  helper is left, and the same reads took about 8 ms again without the
  effects.

## Interrupting links between clusters

The multicluster fixtures cut the network path between two clusters while both
clusters keep serving their local clients.

| API | Endpoint removed | Kept |
| --- | --- | --- |
| `rbdmirror.Mirror.InterruptPeerLink(ctx, daemonName)` | The receiving `rbd-mirror` daemon's endpoint on the source cluster's public bridge | Its destination endpoint, pool leader election, peer and image policies |
| `cephfsmirror.Mirror.InterruptPeerLink(ctx, daemonName)` | The source `cephfs-mirror` daemon's endpoint on the destination cluster's public bridge | Source filesystem, MGR registration, directory and peer policies |
| `multisite.Fixture.InterruptZoneLink(ctx, zoneName)` | The gateway's endpoint on the private multisite HTTP bridge | The local S3 endpoint, realm and period membership |

The mirror path tests cut one path of a mirror daemon with
`ApplyNetworkProfile` and a `Peer` instead of a whole endpoint. For RGW,
`multisite.Fixture.ApplyZoneLinkProfile(ctx, zoneName, peerZone, delay)` does
the same on the multisite HTTP network: with the netem helper it drops or
delays only the packets that one zone's gateway sends to another zone's
gateway port, so that zone stops reading the peer's changes while the peer
keeps reading its own. `TestMultiClusterRGWZoneLinkPathCut` drops zone b's
packets to zone a's gateway: zone a read b's new object, and its data sync
from b was caught up, while b's data sync from a had shards behind and b
lacked a's new object for 12 s. After `Restore` b read a's object byte for
byte, its data sync from a was caught up, and both gateways kept their
processes and the period.

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

The handles are `*fault.NetworkInterruption` with the same contract as above.
Only owned daemons and zones can be selected, the daemon is never stopped or
restarted, and host-network fixtures are refused. Each fixture restores its
interruptions during its own cleanup. Endpoint restoration does not mean the
backlog has been replicated: wait with `WaitReplayReady`, `WaitSnapshotSynced`
or `WaitSyncReady` and compare the data.

## Dropping or delaying messages

Ceph's messenger has built-in fault options. Set them with `override.SetConfig`
to make a daemon drop or delay messages from one kind of peer. The options
exist in both the Tentacle and Squid official images, need no container
privileges and work on host networking.

```golang
// osd.1 drops client messages. It keeps talking to its peers and stays up.
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
hold, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: fmt.Sprintf("osd.%d", osds[1].ID),
	Name:    "ms_blackhole_client",
	Value:   "true",
})
defer hold.Restore(cleanupCtx)
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
	hold, err := override.SetConfig(ctx, cluster, setting)
	defer hold.Restore(cleanupCtx)
	if err != nil {
		t.Fatal(err)
	}
}
```

| Option | Effect |
| --- | --- |
| `ms_blackhole_client`, `ms_blackhole_osd`, `ms_blackhole_mon`, `ms_blackhole_mds`, `ms_blackhole_mgr` | The daemon discards the messages that it receives from and sends to that kind of peer. The connection stays open, so the peer only notices through its own timeout. |
| `ms_inject_delay_type` | Peer kinds whose messages are delayed, as space-separated entity types such as `client` or `osd`. |
| `ms_inject_delay_probability` | Probability of delaying each message; `1` delays every message. |
| `ms_inject_delay_max` | Maximum delay in seconds. Each delay is chosen uniformly between 0 and this value. |

### Delaying operations inside an OSD

`ms_blackhole_*` loses messages in both directions. In a client's own
configuration, `ms_blackhole_osd` therefore drops the client's requests (the
client logs `send_msgosd blackhole osd_op(...)`), and the OSD never sees the
write. To make the OSD run a write after its client gave up, delay the
operation inside the OSD instead:

```golang
for _, setting := range []ceph.ConfigSetting{
	{Section: "osd.0", Name: "osd_debug_inject_dispatch_delay_duration", Value: "2"},
	{Section: "osd.0", Name: "osd_debug_inject_dispatch_delay_probability", Value: "1"},
} {
	hold, err := override.SetConfig(ctx, cluster, setting)
	defer hold.Restore(cleanupCtx)
	if err != nil {
		t.Fatal(err)
	}
}
// A client with rados_osd_op_timeout=2 gets ETIMEDOUT; osd.0 runs the write
// 6 to 8 seconds after it received it.
```

The OSD sleeps for the duration at each of several points that an operation
passes: when it dispatches the message and before and after taking the PG
lock. With a duration of 2 seconds, a write ran 6 to 8 seconds after the OSD
received it in 20.2.4 and 19.2.5. The sleeps also slow the OSD's handling of
new OSD maps, so keep the duration short and restore it before you read the
results.

Whether the OSD runs the delayed write depends on the client's connection
when the OSD gets to it:

- A client that timed out but stays connected: the OSD runs the write. Its
  late reply is dropped; the completion reported `-110` once.
- A client whose process was killed: if the OSD has seen the connection
  close by the time it would start the write, it discards the write without
  running it, because `osd_discard_disconnected_ops` is true by default. With
  a duration of 3 seconds, the write reached its PG 9 seconds after the OSD
  received it, which left time for that.

The OSD's admin socket shows both outcomes. `dump_ops_in_flight` lists the
held operation as soon as the OSD received it. Until the OSD decodes it, its
description names the client and the PG but not the object, so look it up by
client, as in `osd_op(client.4266.0:1 2.0 2.fdab9b31 (undecoded) ...)`. Shortly
after the operation ends, `dump_historic_ops_by_duration` keeps its events,
and the delay makes it one of the longest operations that this history
retains. An operation that ran has `started` and `commit_sent` events; a
discarded one goes from `reached_pg` to `done` without them. Read the object
with another client as well; see
[Oracle recipe: write outcomes](native-commands.md#oracle-recipe-write-outcomes).

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
	PGID          ceph.PGID `json:"pgid"`
	Acting        []int     `json:"acting"`
	ActingPrimary int       `json:"acting_primary"`
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
hold, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: "mds." + status.Active[0].Name,
	Name:    "ms_blackhole_client",
	Value:   "true",
})
defer hold.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
```

### Warnings

- Never set `ms_blackhole_client` on MONs (section `mon`, `mon.<name>` or
  `global`). The fixture's control CLI is a client too. If the MON drops client
  messages, `override.SetConfig` cannot confirm the change and `Restore` cannot
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

`override.AddBlocklist` adds one exact client session to the OSD map blocklist.
The OSDs then reject that session, which reproduces fencing.

```golang
fence, err := override.AddBlocklist(ctx, cluster, address, 10*time.Minute)
// fence is never nil: after a refusal its Restore does nothing.
defer fence.Restore(cleanupCtx)
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

`override.SetOSDFlag` sets or clears one global OSD map flag and remembers the
previous state. Supported flags are the `ceph.OSDFlag` constants: `OSDFlagNoUp`,
`OSDFlagNoDown`, `OSDFlagNoOut`, `OSDFlagNoIn`, `OSDFlagNoBackfill`,
`OSDFlagNoRebalance`, `OSDFlagNoRecover`, `OSDFlagNoScrub`,
`OSDFlagNoDeepScrub`, `OSDFlagNoSnapTrim` and `OSDFlagNoAutoscale`. The composite `pause` flag and irreversible
compatibility flags are not supported.

```golang
flag, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
defer flag.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
```

Useful combinations:

| Goal | Flags |
| --- | --- |
| A frozen OSD stays up so clients hit their own timeouts | `nodown` with `fault.Pause` |
| A stopped OSD stays in so no data moves | `noout` with `Stop` |
| A placement change or recovery stays incomplete | `nobackfill` and `norecover` together (`nobackfill` alone does not stop log-based recovery of small PGs) |
| No background scrub interferes with a test | `noscrub`, `nodeep-scrub` (they can also keep `DeepScrubPG` from finishing) |

`noout` only suppresses automatic out marking; an explicit `OSDContainer.SetIn(ctx,
false)` still marks the OSD out. Per-OSD, CRUSH-node and device-class flags
are separate native state; set them with `cluster.Ceph` (for example
`osd add-noout osd.1`) and undo them yourself.

## Making the cluster full

`override.SetFullRatios` changes the global nearfull, backfillfull and full
thresholds of the OSD map. Very low ratios make a cluster with a little data
report the corresponding health states, without filling a device.

```golang
change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{
	NearFull: .0001, BackfillFull: .0002, Full: .0003,
})
// Several native commands run in sequence; some may have succeeded. change
// is never nil: after a refusal its Restore does nothing.
defer change.Restore(cleanupCtx)
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

[`fault.InjectObjectDataError`](../fault.md#injectobjectdataerror) makes one replica of one object unreadable for scrub
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
if err := fault.InjectObjectDataError(ctx, cluster, "scrub", "victim", replica); err != nil {
	t.Fatal(err)
}
if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
objects, err := observer.PGInconsistencies(ctx, mapping.PGID)
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

`OSDContainer.Remove(ctx)` is a planned removal: it drains the OSD, waits until
`safe-to-destroy`, stops it, waits for it to be down, purges it from Ceph and
removes its container. It refuses the last owned OSD. If the remaining OSDs
cannot hold the data (replica count or capacity), it times out and leaves the
OSD out and reweighted; it does not mark it back in. Each phase checks the
OSD's registration UUID, and a lost purge reply can be retried with a fresh
context. `AddOSD` is refused until a removal has finished.

`OSDContainer.SetIn(ctx, false)` marks an owned OSD out without stopping it, so data
migrates away while the daemon keeps running. `OSDContainer.SetIn(ctx, true)` brings
it back. `WaitForPGClean` waits for recovery while an OSD is intentionally
out or down; `WaitForClean` also requires every owned OSD up and in.

For an abrupt loss, `Stop` the OSD instead.

## Evicting a silent CephFS client

The MDS evicts a client that stopped answering after `session_timeout` or
`session_autoclose`, which default to 60 s and 300 s. These are filesystem
(MDS map) settings, so `override.SetConfig` cannot change them.
`OverrideSessionTimeouts` shortens them for one filesystem.

```golang
fs := cephfs.Filesystems(cluster)[0]
change, err := fs.OverrideSessionTimeouts(ctx, cephfs.SessionTimeouts{
	Timeout:   30 * time.Second,
	Autoclose: time.Minute,
})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
pause, err := fault.Pause(ctx, cluster, client)
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// Poll fs.Sessions until the client's session is gone and
// observer.BlocklistEntries until its Address appears.
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
`fault.Pause` and `override.AddBlocklist` it reproduces an unresponsive RBD
client that holds the exclusive lock.

```golang
status, err := rbd.ImageClients(ctx, cluster, "rbd", "", "disk")
if err != nil {
	t.Fatal(err)
}
owner := status.ExclusiveOwner() // nil when nobody holds the lock
pause, err := fault.Pause(ctx, cluster, ownerClient)
if pause != nil {
	defer pause.Resume(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
fence, err := override.AddBlocklist(ctx, cluster, owner.Address, 10*time.Minute)
defer fence.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
// Another client now takes the lock almost immediately. Resume the old owner,
// wait until its write fails with errno 108, and only then call fence.Restore.
```

- Without fencing, another client's write waits until the frozen owner's
  watch expires (`osd_client_watch_timeout`, 30 s by default), then breaks the
  lock and blocklists the old owner. With `override.AddBlocklist` the handover
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
| `Pool.PGs` | PG states, up/acting sets and primaries of one pool |
| `QuorumStatus`, `ManagerStatus` | MON quorum and the active MGR |
| `cephfs.Filesystem.MDSStatus`, `Sessions` | Rank ownership, standbys and client sessions |
| `BlocklistEntries`, `ObjectWatchers`, `rbd.ImageClients` | Fencing, watches and locks |

Health and placement reports lag behind the event that caused them. Poll with a
deadline:

```golang
func waitForHealthCheck(ctx context.Context, cluster *ceph.Container, code string) error {
	observer, err := ceph.Observe(cluster)
	if err != nil {
		return err
	}
	for {
		snapshot, err := observer.HealthDetails(ctx)
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

- [fault.md](../fault.md): `fault.Pause`, `InterruptNetwork` and network profiles.
- [ceph.md](../ceph.md):
  scrub helpers, `OSDContainer.Remove`, `OSDContainer.SetIn`.
- [override.md](../override.md): `override.SetConfig`, `override.SetMessengerPolicy`, `override.AddBlocklist`, `override.SetFullRatios`, `override.SetOSDFlag`.
- [rbd.md](../rbd.md): `ImageClients`.
- [rbd-mirror.md](../rbd-mirror.md): `Mirror.InterruptPeerLink`.
- [cephfs.md](../cephfs.md): `Filesystem.OverrideSessionTimeouts`,
  `Filesystem.Sessions`.
- [cephfs-mirror.md](../cephfs-mirror.md): `Mirror.InterruptPeerLink`.
- [rgw.md](../rgw.md): `Multisite.InterruptZoneLink`, `Multisite.ApplyZoneLinkProfile`.
- [Host network and native clients](host-network.md): which faults are
  available in host mode.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): handles returned
  with errors and `Restore` semantics.
- [Running native commands](native-commands.md): anything without a typed API.
