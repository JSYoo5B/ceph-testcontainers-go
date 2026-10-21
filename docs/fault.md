# Dedicated faults

Import `github.com/JSYoo5B/ceph-testcontainers-go/fault` alongside `ceph`.
This package uses the cluster created by `ceph.Run`; there is no separate
fixture lifetime. `NetworkPlane` and its constants remain in `ceph`.

## Upgrade from the ceph fault APIs

This is an intentional source-breaking move for the next minor release.
There are no compatibility aliases or wrappers in `ceph`. Existing released
versions keep their original APIs. Update imports and calls together:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.PauseContainer(ctx, target)` | `fault.Pause(ctx, cluster, target)` |
| `ceph.ContainerPause` | `fault.PauseLease` (same `Resume` contract) |
| `cluster.TemporaryNetworkDelay(ctx, target, delay)` | `fault.TemporaryNetworkDelay(ctx, cluster, target, delay)` |
| `ceph.TemporaryLinkDelay(ctx, target, network, delay)` | `fault.TemporaryLinkDelay(ctx, target, network, delay)` |
| `cluster.InterruptNetwork(ctx, target, plane)` | `fault.InterruptNetwork(ctx, cluster, target, plane)` |
| `ceph.InterruptNetwork(ctx, target, network)` | `fault.InterruptLink(ctx, target, network)` |
| `ceph.NetworkDelay`, `NetworkPeer`, `NetworkRate`, `NetworkPacketFault` | Same names in `fault` |
| `ceph.NetworkDelayLease`, `NetworkDelayStatus`, `NetworkInterruption` | Same names in `fault` |
| Netem constants, rate units and `ErrNetem*` sentinels | Same names in `fault` |

`rbd.Mirror.InterruptPeerLink`, `cephfs.Mirror.InterruptPeerLink` and
`rgw.Multisite.InterruptZoneLink` now return `*fault.NetworkInterruption`.
`rgw.Multisite.TemporaryZoneLinkDelay` takes `fault.NetworkDelay` and returns
`*fault.NetworkDelayLease`. These service fixtures still register cleanup.

Cluster-bound operations keep shared-cluster admission and cleanup ordering:
shaping first, endpoint restoration second, then container resumes. Faults
refuse a shared tenant cluster and are allowed in an exclusive lease.
Standalone `InterruptLink` and `TemporaryLinkDelay` remain caller-owned:
restore them before removing their container or bridge. Preserve non-nil
handles returned with errors; restore remains retryable after uncertain results.
The error categories remain in `ceph`; the moved sentinels still match them
with `errors.Is`.

Fullness policy uses
[`Container.OverrideFullRatios`](ceph.md#overridefullratios), returning
`*ceph.FullRatiosOverride`, with explicit caller-managed `Restore` and no
expiry or context/scope-exit undo. Session blocklist and full-ratio policy
belong to `ceph`, including their
mutation APIs and restoration handles. They use Ceph control-plane operations
to change admission or policy; testing failure outcomes does not move these
controls into `fault`. The blocklist extraction in PR #12 is reverted.
Object-error injection remains in `ceph` for its separately reviewed checkpoint.

## Pause

```golang
func Pause(ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container) (*fault.PauseLease, error)
```

Freezes every process of an owned daemon or of a caller-owned `WithClient`
container through Docker's cgroup freezer, as `docker pause` does. Unlike `Stop`,
the process keeps its memory, sockets and Ceph identity, and peers see silence
instead of a closed connection: a frozen OSD stops answering client operations
and heartbeats until its peers report it down after the heartbeat grace. Hold
`nodown` with [TemporaryOSDFlag](ceph.md#temporaryosdflag) to keep it up, so that client
operations hit their own timeouts (`rados_osd_op_timeout`) or are reported as
`SLOW_OPS` (after `osd_op_complaint_time`, 30 seconds by default). Works in host
mode.

The control container is refused (in a single-MON cluster that is MON `a`),
because every fixture command would block. A container that is already paused is
refused rather than adopted, and only one pause per container is allowed. The
returned `*fault.PauseLease` has `ContainerID` and `Resume`; `Resume` thaws the
container without restarting it and treats a removed or already running
container as done. `Terminate` resumes remaining pauses after network fault restoration and before
container teardown; resume a
caller-owned container yourself before removing it.

```golang
hold, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
if hold != nil {
	defer hold.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
pause, err := fault.Pause(ctx, cluster, cluster.OSDs()[1])
if pause != nil {
	defer pause.Resume(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// Client operations on PGs that include osd.1 now wait for their timeouts.
```

## InterruptNetwork

```golang
func InterruptNetwork(ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container, plane ceph.NetworkPlane) (*fault.NetworkInterruption, error)
```

Disconnects one container from one of the cluster's bridges without stopping it.
`ctr` can be an owned daemon or a caller-owned `WithClient` container.
`ceph.PublicNetworkPlane` selects the client/public bridge and
`ceph.ClusterNetworkPlane` the OSD replication bridge; with a single bridge both
select the same endpoint. Host mode is refused because Docker cannot disconnect a
host-network endpoint.

The returned `*fault.NetworkInterruption` has `ContainerID`, `NetworkName` and
`OriginalAddress`. `Restore` reconnects the endpoint with its original IP, also
its original IPv6 address on the dual-stack bridge of
[WithAddressFamily](ceph.md#withaddressfamily)`(AddressIPv6)`, and its aliases, so the
address Ceph advertises stays valid and the daemon is not restarted. An
endpoint that someone else reattached with a different or missing address is
refused, and the handle can be restored again once the endpoint is back. It is a whole-endpoint interruption, not a packet filter between
specific peers. The cluster keeps the handle and restores it during `Terminate`;
restore caller-owned containers explicitly before removing them.

An interrupted MON stays detached when it is stopped and started: `Start`
refuses to run it with `ceph.ErrInvalidRequest` until `Restore` has
reattached its endpoint, because the MON can only bind its monmap address.

```golang
cut, err := fault.InterruptNetwork(ctx, cluster, cluster.OSDs()[0], ceph.ClusterNetworkPlane)
if cut != nil {
	defer cut.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// With WithSeparateClusterNetwork, osd.0 keeps its public listener and process
// but loses replication and heartbeat traffic until Restore.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

A cut cluster endpoint can end the OSD process. With three or more OSDs on
separate CRUSH hosts, the OSD's peers report its failed heartbeats, and after
`osd_heartbeat_grace` (20 s) the MON marks it down. The OSD then tries to
rebind its cluster messenger to an address that no longer exists, logs
`rebind cluster_messenger failed` and exits. `Restore` does not restart it:
call `Start` on its handle afterwards. With two OSDs one reporter is not
enough (`mon_osd_min_down_reporters` is 2), so the OSD stays up.

## TemporaryNetworkDelay

```golang
func TemporaryNetworkDelay(ctx context.Context, cluster *ceph.Container, ctr testcontainers.Container, delay fault.NetworkDelay) (*fault.NetworkDelayLease, error)
```

Delays every packet that `ctr` sends through its endpoint on one of the
cluster's bridges, with the kernel's netem qdisc, below Ceph. It is an opt-in
extension: `delay.HelperImage` names a netem helper image, such as
`fault.NetemHelperImage` (`ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1`),
or any image that meets
[netem contract 1](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md).
The module starts the helper in `ctr`'s network namespace as root and grants
`NET_ADMIN` to the helper only; `ctr` keeps its own image and capabilities.

| `fault.NetworkDelay` field | Meaning |
| --- | --- |
| `HelperImage` | The helper image. Required; nothing selects it implicitly. |
| `Plane` | `ceph.PublicNetworkPlane` or `ceph.ClusterNetworkPlane`, as for [InterruptNetwork](#interruptnetwork). |
| `Delay` | Added to every packet that leaves `ctr` through that endpoint, from 1 ms to `fault.MaxNetworkDelay` (10 s), in whole microseconds. With a `Loss` or a `Rate` it may be zero. |
| `Jitter` | Varies each packet's delay uniformly within ±`Jitter`; at most `Delay`. |
| `Peer` | Optional. Delays only the packets that `ctr` sends to this address; every other packet leaves without delay. An IPv4 address, or an IPv6 address when `ctr` has one on that bridge (the dual-stack bridge of `AddressIPv6` or `AddressDualStack`; see [Dual-stack clusters](ceph.md#dual-stack-clusters) for cutting one family of a path). Mapped, zoned or unspecified addresses are refused. |
| `Loss` | Optional. Drops this percentage of the selected packets at random, from more than 0 to 100. With 100 and a `Peer`, the packets to that peer never arrive: a blackhole of one direction of one path. |
| `PeerPort` | Optional, with `Peer`: only TCP packets to this destination port, such as one of the peer's Messenger ports. An IPv6 packet with extension headers before its TCP header is not selected. |
| `Peers` | Optional, instead of `Peer` and `PeerPort`: several `fault.NetworkPeer` entries, each an `Addr` and an optional TCP `Port`, of either family, at most `fault.MaxNetworkPeers` (16) distinct ones. The packets to any of them get the same delay, loss and rate. Use it to cut a daemon's paths to several peers, since an endpoint holds one delay. |
| `Rate` | Optional. Limits the selected packets to this bandwidth, a `fault.NetworkRate` in bits per second (`fault.BitPerSecond`, `fault.KilobitPerSecond`, `fault.MegabitPerSecond`, `fault.GigabitPerSecond`), from `fault.MinNetworkRate` (64 Kbit/s) to `fault.MaxNetworkRate` (10 Gbit/s), in whole bytes per second (a multiple of 8 bit/s). It uses netem's rate in the same qdisc as `Delay` and `Loss`, which it composes with. See [Bandwidth limits](#bandwidth-limits). |
| `QueueLimit` | Optional, with `Rate`: the netem queue in packets, from 1 to `fault.MaxNetworkQueueLimit` (100000). Zero means `fault.DefaultNetworkQueueLimit` (1000, tc's default). |
| `Reorder` | Optional, a `fault.NetworkPacketFault`: sends this percentage of the selected packets at once, ahead of the packets that wait for the `Delay`. Needs a `Delay`, and takes no `Jitter` and no `Rate`. See [Packet faults](#packet-faults). |
| `Duplicate` | Optional, a `fault.NetworkPacketFault`: sends a second copy of this percentage of the selected packets. With it `Delay` may be zero. |
| `Corrupt` | Optional, a `fault.NetworkPacketFault`: flips one random bit in this percentage of the selected packets. With it `Delay` may be zero. |

`ctr` can be an owned daemon or a caller-owned container attached to the
plane's bridge; it must be running, not paused, with its own network
namespace and an IPv4 endpoint, and an IPv6 address for an IPv6 peer. Host-network clusters are refused. Before it
changes anything, the call checks the helper image, `NET_ADMIN` and the
kernel's netem, with a `Rate` or a packet fault also those netem options,
and with peers also its `prio` qdisc and u32 classifier, in a
separate helper container without a network, and fails
with an error that wraps `fault.ErrNetemImageUnsupported`,
`fault.ErrNetemPermissionDenied` or `fault.ErrNetemKernelUnsupported`. It
refuses an interface whose root qdisc is not the kernel's default, and a
second delay of the same endpoint.

| Failure | Category | Narrower error |
| --- | --- | --- |
| No `HelperImage`: the extension is not selected | `ErrImageCapability` | |
| The helper image does not meet the contract: a missing or broken tool, a `tc` without netem, for a `Rate` a `tc` that cannot parse netem's `rate`, or for a packet fault a `tc` that cannot parse it | `ErrImageCapability` | `ErrNetemImageUnsupported` |
| The engine gave the helper no `NET_ADMIN` | `ErrEnvironmentUnavailable` | `ErrNetemPermissionDenied` |
| The kernel has no netem qdisc, for peers no `prio` qdisc or u32 classifier, for a `Rate` a netem that does not keep the rate, or for a packet fault a netem that does not keep the fault, its correlation or a reorder's gap | `ErrEnvironmentUnavailable` | `ErrNetemKernelUnsupported` |
| A malformed request, a refused target, a held endpoint or a foreign root qdisc | `ErrInvalidRequest` | |
| A `tc` command that ran and failed otherwise | `ErrNativeFailure` | `*NativeFailure` of `tc` |

The returned `*fault.NetworkDelayLease` records the target (`ContainerID`,
`NetworkName`, `SandboxID`, `Namespace`, `Interface`, `InterfaceIndex`,
`MACAddress`, `Address`), the helper's `HelperImageID` and the requested
`Delay`, `Jitter`, `Peer`, `PeerPort`, `Loss`, `Rate`, `Reorder`, `Duplicate`
and `Corrupt`, and in `Peers` the
whole selection: `Peer` and `PeerPort` as one entry, or the requested `Peers`.
With a `Rate`, `QueueLimit` is the queue limit that the lease installed: the
requested one, or `fault.DefaultNetworkQueueLimit`.

- `Status(ctx)` returns a `fault.NetworkDelayStatus`, the kernel's state read
  through the helper: `Applied`, the interface's current name (`Interface`)
  and root qdisc (`RootKind`, `RootHandle`), the `Delay`, `Jitter`, `Loss`
  percentage, `Rate`, `QueueLimit`, `Reorder`, `Duplicate`, `Corrupt` and
  the reorder `Gap`, the `Packets`, `Bytes`, `Drops`,
  `Overlimits` and `Requeues` counters and the queue's `Backlog` (bytes) and
  `QueueLength` (packets) while `Applied`, and tc's JSON output (`Qdiscs`, and `Filters` for the egress
  filters). After the first lookup by address and MAC, the lease follows
  the interface by its index, so a changed address, MAC or name does not lose
  it. `Gone` means that the delayed
  interface no longer exists, because `ctr` stopped, restarted, was removed or
  left the bridge.
- With a `Peer` or `Peers`, the lease owns a `prio` root qdisc whose third
  band leads to a netem child, and one u32 filter per peer that sends its
  packets there; every priority maps to the first band, so nothing else is
  delayed. The IPv4 peers' filters are in preference 10 and u32 table `800:`,
  with handles `800::1`, `800::2` and so on in the given order, and the IPv6
  peers' in the next preference and table (`11` and `801:` after IPv4 peers,
  otherwise `10` and `800:`). They are added root first and filters last, so
  no packet is delayed before the hierarchy is complete. `Status` reports
  `Applied` only when the root, the child and exactly those filters, with the
  requested address and port keys, are all present and nothing else is under
  the root; `Filters` has tc's filter
  dump with each u32 filter's keys under `matches`. The counters are the
  child's, so they count only selected packets.
- `Restore(ctx)` removes the lease's qdisc and the helper. With peers,
  removing the lease's root removes its child and filters with it, also when
  an apply stopped half way. A filter or qdisc that the apply did not add,
  under the lease's root, is refused with `ErrInvalidRequest` instead of
  removed with the root, with or without a `Peer` and also while the delay
  itself is intact (`Status` then reports it not `Applied`). Qdiscs outside
  the root, such as an ingress qdisc, are not removed and do not count; the lease stays retryable, and `Restore` succeeds
  once its owner removed it. A gone interface
  needs no removal, and `Restore` never touches a new namespace. A root qdisc
  that someone replaced is refused with an error, and the lease stays
  retryable. A helper that exited or was removed is replaced first, from the
  checked image by its ID (`HelperImageID`), even if the tag moved.
- Every `tc` command runs under the module's command supervisor. A non-nil
  lease returned with an error must be restored: the change may have reached
  the kernel.
- The cluster keeps the lease and removes the delay at the start of
  `Terminate`. Restore a caller-owned container's delay before removing that
  container.

```golang
delay, err := fault.TemporaryNetworkDelay(ctx, cluster, cluster.OSDs()[0], fault.NetworkDelay{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Delay:       200 * time.Millisecond,
})
if delay != nil {
	defer delay.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// Answers from osd.0 arrive at least 200 ms late until Restore.
```

To delay only one peer, set `Peer` to its address on the same plane, for
example a client container's address or one from
[DaemonAddresses](ceph.md#daemonaddresses). The delay applies to packets that leave
`ctr` toward that peer; for the other direction, delay the peer's endpoint
with `ctr`'s address.

```golang
clientAddress, err := client.ContainerIP(ctx)
if err != nil {
	t.Fatal(err)
}
delay, err := fault.TemporaryNetworkDelay(ctx, cluster, cluster.OSDs()[0], fault.NetworkDelay{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Delay:       200 * time.Millisecond,
	Peer:        netip.MustParseAddr(clientAddress),
})
// Answers from osd.0 reach this client late; other clients and the other
// OSDs see no delay.
```

To fault a container's paths to several peers, list them in `Peers`, for
example every OSD's address from [DaemonAddresses](ceph.md#daemonaddresses):

```golang
addresses, err := cluster.DaemonAddresses(ctx)
if err != nil {
	t.Fatal(err)
}
var peers []fault.NetworkPeer
for _, osd := range addresses.OSDs {
	host, _, _ := net.SplitHostPort(osd.Public[0].Addr)
	peers = append(peers, fault.NetworkPeer{Addr: netip.MustParseAddr(host)})
}
cut, err := fault.TemporaryNetworkDelay(ctx, cluster, mds, fault.NetworkDelay{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peers:       peers,
	Loss:        100,
})
// The MDS's packets to every OSD are dropped; clients still reach the OSDs.
```

### Bandwidth limits

`Rate` limits what `ctr` sends through the endpoint, on the whole interface or
only to the selected peers, until `Restore`:

```golang
limit, err := fault.TemporaryNetworkDelay(ctx, cluster, cluster.OSDs()[0], fault.NetworkDelay{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Rate:        16 * fault.MegabitPerSecond,
})
if limit != nil {
	defer limit.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// osd.0 sends to this client at no more than 16 Mbit/s; other clients and
// the other OSDs are not limited.
```

- netem sends each packet only after the previous one has taken its length
  over the rate. There is no burst allowance. A packet counts with its
  Ethernet header as the kernel queues it, and a TCP segmentation offload
  packet counts with all of its segments, so what an application achieves is
  below `Rate`.
- The packets waiting for the rate, and with a `Delay` for the delay, share
  one queue of `QueueLimit` packets. A packet that arrives at a full queue is
  dropped and counted in `Drops`. With a `Loss`, netem drops first, then
  delays, then paces what is left.
- `Status` reports what the kernel has: `Rate` and `QueueLimit` are the
  installed configuration, and the counters and queue are what netem counted.
  Neither is a throughput. Measure the workload, or divide a difference of
  `Bytes` by a period that you time.
- A `Rate` needs a `tc` that parses netem's `rate` and a kernel whose netem
  keeps it. The preflight helper adds and removes a netem qdisc with a rate
  and a queue limit on its loopback and checks the values that the kernel
  reports, so either missing is an error before `ctr` changes. A delay
  without a `Rate` does not need either.
- The limit is the sender's egress only. To limit what `ctr` receives from a
  peer, limit the peer's endpoint with `ctr`'s address.

### Packet faults

`Reorder`, `Duplicate` and `Corrupt` add netem's packet faults to the
lease's netem qdisc, on the whole interface or only to the selected peers.
Each is a `fault.NetworkPacketFault` with a `Percent`, from more than 0 to 100,
and an optional `Correlation`, from 0 to 100, which makes each packet's
decision that much dependent on the previous one's. A zero value is off
(`IsZero()` reports it); a `Correlation` without a `Percent` is refused.
`String()` formats the fault as tc's arguments do, such as `25%` or
`5% 10%`.

```golang
fault, err := fault.TemporaryNetworkDelay(ctx, cluster, cluster.OSDs()[0], fault.NetworkDelay{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Delay:       10 * time.Millisecond,
	Reorder:     fault.NetworkPacketFault{Percent: 25},
})
if fault != nil {
	defer fault.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// A quarter of osd.0's packets to this client leave at once, ahead of the
// others, which wait 10 ms.
```

| Field | What netem does | Combinations |
| --- | --- | --- |
| `Reorder` | Sends this percentage of the selected packets at once, ahead of the packets that wait for the `Delay`; netem's gap is 1, so every packet may be chosen. | Needs a `Delay`. Refuses `Jitter`, which reorders packets by itself, and `Rate`, which the packets sent at once bypass. Works with `Loss`, `Duplicate` and `Corrupt`. |
| `Duplicate` | Sends a second copy of this percentage of the selected packets. | Works with everything; `Delay` may be zero. |
| `Corrupt` | Flips one random bit in this percentage of the selected packets, within what the kernel holds in the packet's first buffer: the Ethernet, IP and TCP headers, and payload bytes only when they are there too. netem first computes a checksum that the device would have offloaded, and splits a segmentation offload packet first, corrupting one of its segments. | Works with everything; `Delay` may be zero. |

- netem decides per packet that it queues. A TCP segmentation offload packet
  carries many segments, so a percentage of packets is not a percentage of
  segments or of bytes, and the same percentage affects more segments when
  the delay or the losses keep packets small.
- `Status` reports the kernel's `Reorder`, `Duplicate`, `Corrupt` and `Gap`
  apart from the lease's request, and with a `Reorder` netem's `Requeues`
  counts the packets that it sent ahead. None of them shows what the
  receiver got. The apply returns the lease with an error when the kernel
  reports other values than the request, and Restore removes it.
- The preflight helper adds and removes netem with each requested fault and
  a correlation on its own loopback. A `tc` that cannot parse a fault is
  `ErrNetemImageUnsupported`; a kernel that does not report the fault, its
  correlation or a reorder's gap of 1 back is `ErrNetemKernelUnsupported`.
  There is no fallback and no package install. A delay without a packet
  fault does not need them.
- iproute2 6.1, the `tc` of `fault.NetemHelperImage`, sends a duplicate's
  correlation only together with a delay's correlation and silently drops it
  otherwise. With a `Duplicate.Correlation` the lease therefore passes the
  delay with a correlation of zero (`delay 0us 0us 0%` without a `Delay`),
  which is the kernel's state without one.
- TCP absorbs these faults before Ceph sees them: the receiver puts
  reordered segments back in order, discards duplicates and answers them
  with a D-SACK, and drops a segment whose checksum fails, which the sender
  then retransmits. A Ceph operation that returns the right bytes therefore
  does not show that a packet was altered, and a corrupted packet does not
  mean that an altered Messenger frame reached Ceph. Read the receiver's
  kernel counters (see
  [Fault injection](guides/fault-injection.md#packet-faults)) for the
  transport effect.

See [Fault injection](guides/fault-injection.md#delaying-packets-below-ceph)
for what clients observe and the engines this was checked on.


## Standalone bridge functions

```go
func InterruptLink(ctx context.Context, target testcontainers.Container, network string) (*NetworkInterruption, error)
func TemporaryLinkDelay(ctx context.Context, target testcontainers.Container, network string, delay NetworkDelay) (*NetworkDelayLease, error)
```

Both operate on an attached endpoint of a named bridge. `TemporaryLinkDelay`
requires an empty `Plane`; the explicit network selects the link. All other
profile validation, helper admission and restoration checks match the
cluster-bound functions. The caller owns cleanup; these functions do not
register with a cluster.

## Profile values and handles

`NetworkRate.String` formats bits per second with decimal prefixes.
`NetworkPacketFault.IsZero` reports an inactive profile, and
`NetworkPacketFault.String` formats its tc percentage and correlation.
`NetworkDelayLease.Status` reads installed kernel state;
`NetworkDelayLease.Restore` and `NetworkInterruption.Restore` keep their
retryable ownership contracts.

The bounds are `MaxNetworkDelay`, `MaxNetworkPeers`, `MinNetworkRate`,
`MaxNetworkRate`, `DefaultNetworkQueueLimit` and `MaxNetworkQueueLimit`.
Rate units are `BitPerSecond`, `KilobitPerSecond`, `MegabitPerSecond` and
`GigabitPerSecond`. `NetemHelperImage` names the opt-in helper;
`ErrNetemImageUnsupported`, `ErrNetemPermissionDenied` and
`ErrNetemKernelUnsupported` retain their narrower error identities.
