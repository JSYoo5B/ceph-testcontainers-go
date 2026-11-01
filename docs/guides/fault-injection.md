# Fault injection

This guide maps the conditions a Ceph client has to survive (slow daemons,
failovers, fenced sessions, full clusters, damaged replicas) to the APIs that
create them in a disposable cluster, and lists the traps of each. The function
contracts are on the package pages: [`fault`](../../fault/README.md),
[`override`](../../override/README.md) and [`ceph`](../../ceph/README.md).

Every technique works with the official Ceph image and needs no privileged
container, except packet shaping below Ceph: that is an opt-in extension whose
separate helper image alone gets `NET_ADMIN` (see
[Delaying packets below Ceph](#delaying-packets-below-ceph)).

| Condition you want | API | What peers observe | Host network | Undo |
| --- | --- | --- | --- | --- |
| A daemon process exits | `Stop`/`Start` on a daemon handle | Closed connections right away | Yes | `Start` |
| A daemon or client is alive but silent | `fault.Pause` | No replies, no closed connection; down only after heartbeat grace | Yes | `PauseLease.Resume` |
| A container loses one Docker network | `fault.InterruptNetwork` | The endpoint disappears | No | `NetworkInterruption.Restore` |
| Packets a container sends are delayed, dropped, rate-limited, reordered, duplicated or corrupted | `fault.ApplyNetworkProfile` (opt-in netem helper) | Slow or lossy replies from that container; connections stay up | No | `NetworkProfileLease.Restore` |
| Replication between two clusters stops | `rbdmirror.Daemon.InterruptPeerLink`, `cephfsmirror.Daemon.InterruptPeerLink`, `multisite.Fixture.InterruptZoneLink` | Local service works, the peer link is gone | No | `NetworkInterruption.Restore` |
| A daemon drops or delays messages from one kind of peer | `override.SetConfig` with `ms_blackhole_*` or `ms_inject_delay_*` | The daemon stays up and keeps talking to the MON | Yes | `ConfigHandle.Restore` |
| An OSD runs a client's write after the client gave up | `override.SetConfig` with `osd_debug_inject_dispatch_delay_*` | The call ends with ETIMEDOUT; the OSD runs the write later, or discards it if the client disconnected | Yes | `ConfigHandle.Restore` |
| One client session is fenced | `override.AddBlocklist` | The OSDs reject that session | Yes | `BlocklistHandle.Restore` |
| A failed OSD stays up or in | `override.SetOSDFlag` (`nodown`, `noout`, ...) | Map flags hold the OSD state | Yes | `OSDFlagHandle.Restore` |
| The cluster reports nearfull, backfillfull or full | `override.SetFullRatios` | `OSD_NEARFULL`, `OSD_BACKFILLFULL`, `OSD_FULL` | Yes | `FullRatiosHandle.Restore` |
| A replica is damaged | `fault.InjectObjectDataError`, `DeepScrubPG`, `RepairPG` | Scrub errors, `PG_DAMAGED` | Yes | `RepairPG` or OSD restart |
| An OSD leaves the cluster | `OSDContainer.Remove`, `OSDContainer.SetIn` | Data moves to the remaining OSDs | Yes | `AddOSD`, `SetIn(ctx, true)` |
| The MDS evicts a silent CephFS client quickly | `session.OverrideTimeouts` (`cephfs/session`) with `fault.Pause` | Eviction and blocklisting after 30 s or more | Yes | `TimeoutsOverrideHandle.Restore` |
| An RBD exclusive lock moves to another client | `rbd.ImageClients` with `fault.Pause` and `override.AddBlocklist` | Lock break and fencing | Yes | Resume, then restore the blocklist |

Each condition ends with the cluster, so the undo step is optional when the test
terminates the cluster afterwards. Handles returned together with an error
follow [Contexts, ownership and cleanup](contexts-and-cleanup.md). The snippets
use `cleanupCtx` for a context that outlives the test's own context.

Not supported at all: firewall rules, TCP resets, socket destruction, and any
packet fault on host networks. The receiving side is shaped with
`NetworkProfile.Direction` (`fault.Inbound`), and packets are recorded with
[`capture`](../../capture/README.md). Without the netem helper, the alternatives are `InterruptNetwork`,
a separate cluster network (`NetworkConfig.WithSeparateClusterNetwork` with
`ceph.ClusterNetworkPlane`) and the [messenger options](#dropping-or-delaying-messages).

## Stopping and starting daemons

`Stop` and `Start` on a daemon handle end and restart the process; peers see
closed connections at once. The container, its Ceph identity and its data are
kept.

| Daemon | Handle | Traps |
| --- | --- | --- |
| MON | `cluster.Monitors(ctx)` | With one MON, the MON is also the control container, so every fixture call fails until it starts again. Use `ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3))` or more. |
| MGR | `cluster.Managers(ctx)` | Use `ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2))` so a standby takes over; `ManagerStatus` reports the active one. |
| OSD | `cluster.OSDs(ctx)` | A down OSD is marked out after `mon_osd_down_out_interval` (600 s). Hold `noout` to avoid data movement. `Start` does not wait for up and in: wait with `WaitForClean`, `OSDStates` or `HealthDetails`. |
| MDS | `fs.MDSs()` | Configure `StandbyMDS` so a standby takes the rank. `fs.WaitReady` also waits for standby capacity, so restart the stopped daemon first. |
| RGW | `rgw.Gateways(ctx, cluster)` | In bridge mode Docker can map a new port on restart: call `S3Endpoint` again. |
| RBD and CephFS mirror daemons | `mirror.Daemons()` | `rbdmirror.Daemon.Status` reports the leader election. |

## Freezing a container

[`fault.Pause`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#Pause) freezes every process of a
container like `docker pause`. Sockets stay open, so peers see silence instead
of a closed connection.

```golang
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoDown, true)
defer hold.Restore(cleanupCtx) // never nil
if err != nil {
	t.Fatal(err)
}
pause, err := fault.Pause(ctx, cluster, osds[1])
if pause != nil {
	defer pause.Resume(cleanupCtx) // also when err is not nil
}
if err != nil {
	t.Fatal(err)
}
// Client operations hit rados_osd_op_timeout; HealthDetails reports SLOW_OPS.
```

The control container and an already paused container are refused, and a
second pause of the same container returns the existing handle with an error.
`Resume` is idempotent and retryable. `Terminate` resumes owned pauses after
it restores network faults and before it removes containers; resume a caller-owned client container yourself before terminating it.

What Ceph does with the frozen container:

- OSD with `nodown` held: it stays up, client operations fail with ETIMEDOUT
  (errno 110) after the client's `rados_osd_op_timeout`, and the surviving
  primary reports `SLOW_OPS` after `osd_op_complaint_time` (30 s; lower it with
  `override.SetConfig`, section `osd`). Set the client timeout yourself, for
  example `conf_set("rados_osd_op_timeout", "5")` or `--rados-osd-op-timeout 5`.
- OSD without `nodown`: peers report it down after the heartbeat grace (20 s).
  The MON needs `mon_osd_min_down_reporters` (2) reporters, so in a two-OSD
  cluster set it to 1 in section `mon`, or the OSD is marked down only after
  `mon_osd_report_timeout` (900 s). After `Resume` the OSD rejoins.
- MDS: beacons stop and the MON fails over to a standby. To keep the MDS
  active while it ignores clients, use
  [`ms_blackhole_client`](#an-mds-that-ignores-clients).
- Client attached with `WithClient`: the servers reclaim its resources on their
  own timers. A RADOS watch (see `Observer.ObjectWatchers`) expires after
  `osd_client_watch_timeout` (30 s), and until then another client's notify
  fails with ETIMEDOUT. For RBD locks and CephFS sessions see
  [Handing over an RBD exclusive lock](#handing-over-an-rbd-exclusive-lock) and
  [Evicting a silent CephFS client](#evicting-a-silent-cephfs-client).

## Interrupting a network endpoint

[`fault.InterruptNetwork`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#InterruptNetwork) disconnects
one container from one cluster bridge without stopping it. `Restore`
reconnects the same IP addresses and aliases, so the address Ceph advertised
stays valid. Bridge mode only.

```golang
// The cluster runs with NetworkConfig{}.WithSeparateClusterNetwork().
// Cut only replication and heartbeat traffic of osd.0.
cut, err := fault.InterruptNetwork(ctx, cluster, osds[0], ceph.ClusterNetworkPlane)
if cut != nil {
	defer cut.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

Typical uses:

- `PublicNetworkPlane` on two MONs of a 5-MON cluster: the other three keep
  quorum, and the cut MONs rejoin after `Restore`.
- `PublicNetworkPlane` on a `WithClient` container: the client loses the whole
  cluster.
- `ClusterNetworkPlane` on an OSD: it keeps its public listener while
  replication and heartbeats fail. With three or more OSDs on separate hosts,
  the MON marks it down after `osd_heartbeat_grace` and the OSD exits with
  `rebind cluster_messenger failed`; call `Restore`, then `Start`. With two
  OSDs one reporter is not enough and it stays up. With a single bridge both
  planes select the same endpoint.

Traps:

- The control container is not refused, but every fixture command fails while
  it is disconnected.
- An interrupted MON cannot `Start` until `Restore` reattaches it.
- Restore a non-nil handle returned with an error: Docker may have
  disconnected the endpoint anyway.
- `Terminate` restores owned interruptions before removing networks. Restore a
  caller-owned container's endpoint before you terminate that container.
  `fault.InterruptLink` on a named bridge is always caller-owned.

## Delaying packets below Ceph

[`fault.ApplyNetworkProfile`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#ApplyNetworkProfile) applies
the kernel's netem qdisc to the packets one container sends on one cluster
bridge. Ceph does not know about it: sessions stay up and peers see answers
late, while the container's other bridge and other daemons are unaffected.

This is an opt-in extension. `NetworkProfile.HelperImage` must name a helper
image such as `fault.NetemHelperImage`, which
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md)
publishes for `linux/amd64` and `linux/arm64`. The helper runs in the target's
network namespace with `NET_ADMIN`; the target keeps its own image and
capabilities.

```golang
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
// Reads served by osd.0 now take at least 200 ms; reads served by osd.1 do not.
status, err := delay.Status(ctx)
if err != nil || !status.Applied {
	t.Fatalf("delay: %+v, %v", status, err)
}
```

Before changing the target, the call checks the helper image, `NET_ADMIN` and
the kernel's netem (and every requested netem option) in a separate helper
without a network. Missing support wraps `fault.ErrNetemImageUnsupported`
(`ceph.ErrImageCapability`), `fault.ErrNetemPermissionDenied` or
`fault.ErrNetemKernelUnsupported` (`ceph.ErrEnvironmentUnavailable`); there is
no fallback. The full table is in the
[`fault` reference](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#ApplyNetworkProfile), and the
classification in [Classified errors](errors.md#image-capabilities-and-optional-extensions).
Rootless engines, Podman and Docker Desktop's Enhanced Container Isolation may
refuse `NET_ADMIN` or the namespace join.

What to expect:

- By default only egress of the target is shaped. A request and its answer
  cross the interface once each way, so the answer arrives at least `Delay`
  late. To shape the other direction too, set `Direction` (see
  [Direction and phase](#direction-and-phase)).
- `Jitter` can reorder packets of one connection, which TCP repairs at some
  cost.
- An interface whose root qdisc is not the kernel's default is refused, and an
  endpoint holds one lease. List several peers in one lease instead.
- The lease ends with the interface. A target that stops, restarts or loses the
  bridge (also through `InterruptNetwork`) comes back unshaped and `Status`
  reports `Gone`.
- `Restore` removes only the lease's own qdisc. A replaced root, or a qdisc or
  filter someone added under it, is refused and the lease stays retryable.
- A canceled call can still have reached the kernel: restore every non-nil
  lease. `Terminate` restores owned leases first; restore a caller-owned
  container's lease before you remove the container.
- `Status` reports the kernel's installed state and netem's counters, not what
  the client measured.

### Selected peers

With `Peer`, only packets to that address are shaped, and with `PeerPort`
only TCP packets to that destination port. `Peers` lists up to
`fault.MaxNetworkPeers` `fault.NetworkPeer` entries of either family in one
lease. Every other packet leaves unshaped.

```golang
// The MDS loses both OSDs; clients and the MON still reach it.
cut, err := fault.ApplyNetworkProfile(ctx, cluster, mds, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peers:       []fault.NetworkPeer{{Addr: osd0}, {Addr: osd1}},
	Loss:        100,
})
```

`Loss: 100` without a `Delay` is a blackhole of one direction of one path.
Requests to the target still arrive, so a write may have run although its reply
was lost: read the outcome back with another client before treating the
operation as failed. Get peer addresses from a client's `ContainerIP` or from
[`DaemonAddresses`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#DaemonAddresses). For an MDS path cut,
see [MDS sessions](messenger-client-fixtures.md#mds-sessions).

### Direction and phase

`Direction` picks which side of the selected paths is shaped:

| `Direction` | Shaped | Where the lease lives |
| --- | --- | --- |
| `fault.Outbound` (default) | What the target sends to the peers | The target's endpoint |
| `fault.Inbound` | What each peer sends to the target | Each peer's endpoint, one lease per peer container in `lease.PeerLeases` |
| `fault.BothDirections` | Both | The target's endpoint and each peer's |

The helper can only shape what an endpoint sends, so `Inbound` and
`BothDirections` put the same profile on each peer's own endpoint, selecting the
packets it sends to the target. Each peer address must therefore belong to a
running container on the same bridge, such as a daemon's address from
`DaemonAddresses`; any other address is refused before anything changes. A
`PeerPort` stays the peer's port in both directions: outbound it is the
destination port, inbound the source port of what the peer sends from it, so
one `NetworkPeer` selects both halves of a conversation with a daemon's
Messenger port. With `Inbound` alone nothing is applied to the target:
`lease.Status` is refused, so read each `lease.PeerLeases[i].Status`. `Restore`
and `Terminate` restore all of them.

`Phase: fault.ConnectionSetup` shapes only TCP segments with SYN set: the
target's connection attempts, and with `Inbound` the peer's handshake answers.
Established connections are untouched. With `Loss: 100` every session that
exists keeps working while every new connection on the path, a reconnect
included, never completes. It needs `Peer` or `Peers`.

```golang
// osd.0's msgr2 address: clients that are connected keep reading from it,
// a client that connects now times out on its objects.
setup, err := fault.ApplyNetworkProfile(ctx, cluster, client, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        osd0,
	PeerPort:    osd0Port,
	Phase:       fault.ConnectionSetup,
	Loss:        100,
})

// The same OSD's answers to client, cut at the OSD's endpoint.
answers, err := fault.ApplyNetworkProfile(ctx, cluster, client, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        osd0,
	PeerPort:    osd0Port,
	Direction:   fault.Inbound,
	Loss:        100,
})
```

Both selectors work below Ceph, on TCP. They cannot pick a Messenger frame or
message type: msgr2 frames are not at a fixed offset of a packet, and in secure
mode they are encrypted. To drop or delay messages by peer kind inside a
daemon, use `ms_blackhole_*` or `ms_inject_delay_*` (see
[Dropping or delaying messages](#dropping-or-delaying-messages)).
`TestNetworkDirectionAndPhase` shows both selectors against two OSDs.

### Bandwidth limits

`Rate` limits the selected packets to a `fault.NetworkRate`, from
`fault.MinNetworkRate` (64 Kbit/s) to `fault.MaxNetworkRate` (10 Gbit/s). It
composes with `Delay` and `Loss` in the same qdisc, and `Delay` may then be
zero.

```golang
fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Rate:        16 * fault.MegabitPerSecond,
}
```

- There is no burst allowance, and the rate counts Ethernet headers and whole
  segmentation offload packets, so application throughput stays a few percent
  below `Rate`. `Status` counters are not a throughput: time the workload.
- Waiting packets share one queue of `QueueLimit` packets (1000 by default).
  A full queue drops packets (`Drops`), and a large queue at a low rate adds
  latency to every selected packet, including Ceph heartbeats.
- Below about 1 Mbit/s one 64 KiB offload packet takes over half a second, so
  sessions with short timeouts may fail.
- A severe limit does not fail a transfer by itself: the connection stays up
  and data arrives slowly. The client fails only through its own timeout
  (`rados_osd_op_timeout`, which defaults to 0, no limit). A very small
  `QueueLimit` instead causes drops and retransmissions that make a transfer
  many times slower but still correct.

A limit on the path between a replicating daemon and the other cluster slows
replication without stopping it. Apply the lease on the source cluster to a
source OSD, with `Peer` set to the mirror daemon's address on the source
network (from the daemon's `Inspect` and the `NetworkName` field of `ceph.FactsOf(source)`).

Read progress from the mirror status, not from daemon readiness: during a
snapshot sync, the `rbdmirror.Mirror.ImageStatus` description carries
`replay_state` `syncing`, `syncing_percent` and `bytes_per_second`. rbd-mirror
writes its status only every 30 seconds, so poll over longer windows. After
`Restore`, compare the destination's bytes with its own client; the status
reports the snapshot in sync up to 30 seconds later.

### Packet faults

`Reorder`, `Duplicate` and `Corrupt` are `fault.NetworkPacketFault` values
with a `Percent` and an optional `Correlation`, added to the lease's netem
qdisc. `Reorder` needs a `Delay` and refuses `Jitter` and `Rate`; `Duplicate`
and `Corrupt` combine with every field.

```golang
fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Plane:       ceph.PublicNetworkPlane,
	Peer:        netip.MustParseAddr(clientAddress),
	Delay:       10 * time.Millisecond,
	Reorder:     fault.NetworkPacketFault{Percent: 25},
	Duplicate:   fault.NetworkPacketFault{Percent: 30, Correlation: 25},
}
```

The receiving kernel absorbs these faults before Ceph sees the bytes. It puts
reordered segments back in order, discards duplicates with a D-SACK, and drops
segments whose TCP checksum fails, which the sender retransmits. A Ceph
operation returning the right bytes therefore proves nothing about the fault,
and a moved counter does not mean an altered Messenger frame reached Ceph.

To observe the transport effect, read the receiver's own counters from
`/proc/net/netstat`, `/proc/net/snmp` and `/proc/net/snmp6` in its network
namespace before and after a bounded window:

| Fault | Receiver counter |
| --- | --- |
| `Reorder` | `TcpExtTCPOFOQueue` (and D-SACKs for segments retransmitted only because they were late) |
| `Duplicate` | `TcpExtTCPDSACKOldSent` |
| `Corrupt` | `TcpInCsumErrors`; on IPv6 also `Ip6InAddrErrors` for a flipped destination address |

Compare with an unselected receiver and with a window without the fault.
Receivers count a few reordered segments and D-SACKs without any fault, so
assert a margin, never zero or an exact count.

Traps:

- netem decides per queued packet, and a segmentation offload packet carries
  many segments, so a percentage is not a percentage of segments or bytes.
- `Corrupt` flips a bit in the packet's first buffer, mostly headers. A flipped
  IPv4 header does not reach the receiver's IP layer, and a flipped Ethernet
  address sends the packet elsewhere on the bridge.
- `Status` reports the installed faults and, for `Reorder`, netem's `Requeues`;
  neither shows what the receiver got.

## Interrupting links between clusters

The multicluster fixtures cut the path between two clusters while both keep
serving local clients.

| API | Endpoint removed | Kept |
| --- | --- | --- |
| [`rbdmirror.Daemon.InterruptPeerLink`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror#Daemon.InterruptPeerLink) | The `rbd-mirror` daemon's endpoint on the source cluster's public bridge | Its destination endpoint, leader election, peer and image policies |
| [`cephfsmirror.Daemon.InterruptPeerLink`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror#Daemon.InterruptPeerLink) | The `cephfs-mirror` daemon's endpoint on the destination cluster's public bridge | Source filesystem, MGR registration, directory and peer policies |
| [`multisite.Fixture.InterruptZoneLink`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite#Fixture.InterruptZoneLink) | The gateway's endpoint on the multisite HTTP bridge | The local S3 endpoint, realm and period |

The handles have the `InterruptNetwork` contract, and each fixture restores
them in its own cleanup. Host-network fixtures are refused. A restored
endpoint does not mean the backlog is replicated: wait with `WaitReplayReady`,
`WaitSnapshotSynced` or `WaitSyncReady` and compare the data.

To cut one direction instead of a whole endpoint, use `ApplyNetworkProfile`
with a `Peer` and a `Direction`, or for RGW
[`multisite.Fixture.ApplyZoneLinkProfile`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite#Fixture.ApplyZoneLinkProfile),
which shapes only the packets one zone's gateway sends to another zone's
gateway. The peer zone then stops receiving changes while it still pulls its
own.

## Dropping or delaying messages

Ceph's messenger fault options, set with `override.SetConfig`, make a daemon
drop or delay messages from one kind of peer. They need no privileges and work
on host networking.

```golang
// osd.1 drops client messages but stays up and keeps talking to its peers.
hold, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: fmt.Sprintf("osd.%d", osds[1].ID),
	Name:    "ms_blackhole_client",
	Value:   "true",
})
defer hold.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
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
| `ms_blackhole_client`, `_osd`, `_mon`, `_mds`, `_mgr` | The daemon discards messages to and from that kind of peer. The connection stays open, so the peer notices only through its own timeout. |
| `ms_inject_delay_type` | Space-separated peer kinds whose messages are delayed, such as `client` or `osd`. |
| `ms_inject_delay_probability` | Probability of delaying each message; `1` delays all. |
| `ms_inject_delay_max` | Maximum delay in seconds, chosen uniformly from 0. |

`Section` selects all daemons of a kind (`osd`) or one daemon (`osd.1`). On one
OSD, only operations whose acting primary is that OSD are affected. Find the
primary before the test:

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

### Delaying operations inside an OSD

`ms_blackhole_*` drops both directions, so in a client's own configuration
`ms_blackhole_osd` drops the request and the OSD never sees the write. To make
the OSD run a write after its client gave up, delay the operation inside the
OSD:

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
// A client with rados_osd_op_timeout=2 gets ETIMEDOUT; osd.0 runs the write later.
```

The OSD sleeps for the duration at several points of each operation, so a
2-second duration delays a write by roughly 6 to 8 seconds. The sleeps also
slow OSD map handling: keep the duration short and restore it before reading
results.

An OSD map epoch published while the delay is held costs more than its own
sleeps. An operation stamped with the new epoch that reaches a PG still on the
old one waits for the map and goes through the queue again, about 4 seconds
more with a 2-second duration, which can exceed a client timeout sized for
the delay. Even an idle cluster publishes one such epoch: an OSD sends its
first beacon only once the host has been up for `osd_beacon_report_interval`
(300 seconds by default), and the MON records the beacon in a new epoch. On a
freshly booted CI host that point can fall inside the test. Call
`AdminSocket(ctx, "send_beacon")` on the `OSDContainer`, wait until
`osd_xinfo` in `ceph osd dump` has a `last_purged_snaps_scrub` time and
`ceph pg <pgid> query` reports that epoch, then set the delay.

The outcome depends on the client's connection when the OSD reaches the write:

- A client that timed out but stays connected: the OSD runs the write and its
  late reply is dropped.
- A client whose process was killed: once the OSD has seen the connection
  close, it discards the write (`osd_discard_disconnected_ops` is true by
  default).

The admin socket shows which happened. `dump_ops_in_flight` lists the held
operation; before it is decoded the description names the client and PG but
not the object, as in `osd_op(client.4266.0:1 2.0 2.fdab9b31 (undecoded) ...)`.
In `dump_historic_ops_by_duration`, an operation that ran has `started` and
`commit_sent` events, and a discarded one goes from `reached_pg` to `done`
without them. Read the object with another client as well; see
[Oracle recipe: write outcomes](native-commands.md#oracle-recipe-write-outcomes).

### An MDS that ignores clients

`ms_blackhole_client` on the active MDS makes it ignore clients while it keeps
sending beacons, so no failover happens. New mounts fail with ETIMEDOUT after
`client_mount_timeout`. Use `"mds." + status.Active[0].Name` from
`fs.MDSStatus` as the `Section`. Pausing the MDS container produces a failover
instead.

### Warnings

- Never set `ms_blackhole_client` on MONs (section `mon`, `mon.<name>` or
  `global`). The fixture's CLI is a client, so neither `SetConfig` nor
  `Restore` can reach the MON afterwards and nothing can undo it.
- Avoid it on MGRs: fixture commands served by the MGR (subvolumes, modules,
  PG statistics) stop.
- Do not set `ms_blackhole_*` in the `client` section, which the fixture's CLI
  reads. Put client-side options in your own client's configuration.
- Open a new client connection for each fault phase; the behaviour above holds
  for connections made after the change.
- Avoid `ms_inject_socket_failures`: its effect on short I/O is either total or
  invisible and varies between runs.

## Fencing a client session

[`override.AddBlocklist`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/override#AddBlocklist) blocklists one
exact session, so the OSDs reject it.

```golang
fence, err := override.AddBlocklist(ctx, cluster, address, 10*time.Minute)
defer fence.Restore(cleanupCtx) // never nil
if err != nil {
	t.Fatal(err)
}
```

`address` is `IP:port/nonce` with a non-zero nonce, from the client itself
(librados `get_addrs`), `ObjectWatchers`, `rbd.ImageClients` or
`session.List` (`cephfs/session`). Bare IPs, ranges and existing entries are
refused. `Restore` removes only its own entry; whether the old session then
recovers is up to the client library, so most tests reconnect.

## Holding OSD map flags

[`override.SetOSDFlag`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/override#SetOSDFlag) sets or clears one
global flag (a `ceph.OSDFlag` constant) and restores its previous state.

```golang
flag, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
defer flag.Restore(cleanupCtx)
if err != nil {
	t.Fatal(err)
}
```

| Goal | Flags |
| --- | --- |
| A frozen OSD stays up so clients hit their own timeouts | `nodown` with `fault.Pause` |
| A stopped OSD stays in so no data moves | `noout` with `Stop` |
| Placement change or recovery stays incomplete | `nobackfill` and `norecover` together (`nobackfill` alone does not stop log-based recovery of small PGs) |
| No background scrub interferes | `noscrub`, `nodeep-scrub` (they also keep `DeepScrubPG` from finishing) |

`noout` suppresses only automatic out marking; `OSDContainer.SetIn(ctx, false)`
still marks the OSD out. Per-OSD, CRUSH-node and device-class flags (such as
`osd add-noout osd.1`) are set with `cluster.Ceph` and undone by you.

## Making the cluster full

[`override.SetFullRatios`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/override#SetFullRatios) lowers the
nearfull, backfillfull and full thresholds, so a cluster with little data
reports the full states without filling a device.

```golang
change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{
	NearFull: .0001, BackfillFull: .0002, Full: .0003,
})
defer change.Restore(cleanupCtx) // never nil; restore also after a partial apply
if err != nil {
	t.Fatal(err)
}
// Poll HealthDetails until Checks contains OSD_FULL, then exercise the client.
```

- Ratios are stored as float32. Compare against `Observer.FullRatios`, not your
  float64 literals.
- Success verifies the map, not OSD reports: wait for the health check.
- On a full map librados makes ordinary writes wait. To get ENOSPC (errno 28)
  or EDQUOT, set the `FULL_TRY` operation flag in your client, not
  `FULL_FORCE`. A delete with `FULL_TRY` is admitted while the map is full.

### The ratios are a policy, not a capacity

The full ratio acts on the utilization that the OSD last reported. It does not
reserve space. With one small OSD (`OSDStorage.WithBlockSize`), the native
tests observed three different stops:

| Workload | What stops the writes | OSD |
| --- | --- | --- |
| Writes paced at about 1 MiB/s, default ratios | `OSD_FULL` and `POOL_FULL`; later writes wait | Keeps running; deleting objects clears `OSD_FULL` and writes resume |
| An unpaced burst, default ratios | BlueStore's allocator: `_do_alloc_write failed ... (28) No space left on device` | Aborts before the next report raises `OSD_FULL` |
| Any writes with the full ratio and `osd_failsafe_full_ratio` at 1 | The allocator, as above | Aborts |

An OSD that aborted this way does not start again in place: BlueFS cannot get
space at mount, and the daemon aborts, either in `BlueRocksEnv` or in BlueFS
itself with `bluefs enospc`. Both were observed on Tentacle 20.2.4.
Lengthening the block file is not enough. `ceph-bluestore-tool
bluefs-bdev-expand` must then run on the stopped store before the OSD boots
and serves its acknowledged data again.

So test a client's reaction to a full cluster with `SetFullRatios`, as above.
Do not fill a device and expect `OSD_FULL` first.

Two other storage layers stop differently:

- A separate BlueFS DB device (`bluestore_block_db_*` before the OSD is added)
  that fills up does not refuse writes. BlueFS moves metadata to the main
  device and raises `BLUEFS_SPILLOVER`.
- By default the MON store lives in the container's writable layer (with
  `MonitorStorage.WithVolume`, in its own volume), on the engine's
  disk, which the fixture never fills. `mon_data_avail_warn` and
  `mon_data_avail_crit` are percentages of that filesystem's available
  space. Setting one above the current share (`override.SetConfig`, section
  `mon`) raises `MON_DISK_LOW` or `MON_DISK_CRIT` without writing anything.

## Damaging a replica

[`fault.InjectObjectDataError`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#InjectObjectDataError)
makes one replica of one object unreadable for scrub and recovery through
Ceph's `injectdataerr`. No disk is touched and no OSD stops.

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

Traps:

- Inject into a replica, not the primary. An error on the primary leaves
  client reads working and the next deep scrub reports nothing.
- The pool must be replicated and use BlueStore OSDs. A pool with `PGNum: 1`
  keeps every object in one PG.
- The injection lives in OSD memory until repair or an OSD restart. Nothing
  undoes it at `Terminate`, and there is no handle.
- With `noscrub` or `nodeep-scrub` set, or no PG primary, `DeepScrubPG` and
  `RepairPG` wait until the startup timeout or your deadline. `RepairPG` fails
  when no authoritative copy exists.
- `PGInconsistencies` reads the last deep scrub; it does not start one.

## Removing an OSD or marking it out

[`OSDContainer.Remove`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#OSDContainer.Remove) drains, stops,
purges and removes an OSD. It refuses the last owned OSD, and when the remaining
OSDs cannot hold the data it times out and leaves the OSD out and reweighted.
`AddOSD` is refused until a removal has finished.

[`OSDContainer.SetIn`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#OSDContainer.SetIn)`(ctx, false)`
marks an OSD out while it keeps running. During that time wait with
`WaitForPGClean`; `WaitForClean` also requires every owned OSD up and in. For
an abrupt loss, `Stop` the OSD instead.

## Evicting a silent CephFS client

The MDS evicts a silent client after `session_timeout` (60 s) or
`session_autoclose` (300 s). These are MDS map settings, so `override.SetConfig`
cannot change them;
[`session.OverrideTimeouts`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session#OverrideTimeouts)
shortens them for one filesystem, to no less than 30 s.

```golang
fs := cephfs.Filesystems(cluster)[0]
change, err := session.OverrideTimeouts(ctx, fs, session.Limits{
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
// Poll session.List until the session is gone and BlocklistEntries until its
// Address appears.
```

- If no other client wants the frozen client's capabilities, its session stays
  `open` until `Autoclose`. If another client requests them, the MDS evicts it
  once `Timeout` passes, and the other client sees only data the frozen client
  had flushed.
- Eviction also blocklists the address. Ceph created that entry, so `Restore`
  does not remove it; it expires after `mds_blocklist_interval` (1 hour).
- After `Resume`, the old session fails with ESHUTDOWN (errno 108). A new mount
  gets a new nonce and works.
- `session.List` lists the client once per rank and includes the fixture's own
  short-lived probes. Match your client by `ID`, since on host networking every
  container shares the hostname.

## Handing over an RBD exclusive lock

[`rbd.ImageClients`](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rbd#ImageClients) reads an image's
watchers and locks. With `fault.Pause` and `override.AddBlocklist` it
reproduces an unresponsive client that holds the exclusive lock.

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

- Without fencing, another client's write waits until the frozen owner's watch
  expires (`osd_client_watch_timeout`, 30 s), then librbd breaks the lock and
  blocklists the old owner itself. That entry is not the fixture's and stays
  for an hour.
- Keep the fence until the old owner has observed it. The lock is cooperative:
  an unfenced old owner asks for the lock back, gets it, and its write
  succeeds.
- Watchers and locks are two separate reads, so poll for a stable answer
  during a handover.

## Observing the effect

| Check | Use it for |
| --- | --- |
| `HealthDetails` | `SLOW_OPS`, `OSD_DOWN`, `OSD_FULL`, `OSD_SCRUB_ERRORS`, `PG_DAMAGED` |
| `OSDStates`, `OSDFlags` | Up/in state and weight of owned OSDs, global flags |
| `Pool.PGs` | PG states, up/acting sets and primaries |
| `QuorumStatus`, `ManagerStatus` | MON quorum and the active MGR |
| `cephfs.Filesystem.MDSStatus`, `session.List` | Rank ownership, standbys and client sessions |
| `BlocklistEntries`, `ObjectWatchers`, `rbd.ImageClients` | Fencing, watches and locks |

Health and placement reports lag behind the event that caused them, so poll
them under a deadline instead of asserting once.

## Related APIs

- [`fault`](../../fault/README.md): `Pause`, `InterruptNetwork`, `ApplyNetworkProfile`, `InjectObjectDataError`.
- [`override`](../../override/README.md): `SetConfig`, `SetMessengerPolicy`, `AddBlocklist`, `SetFullRatios`, `SetOSDFlag`.
- [`ceph`](../../ceph/README.md): scrub helpers, `OSDContainer.Remove`, `OSDContainer.SetIn`.
- [`rbd`](../../rbd/README.md), [`rbd/mirror`](../../rbd/mirror/README.md),
  [`cephfs`](../../cephfs/README.md), [`cephfs/mirror`](../../cephfs/mirror/README.md),
  [`rgw/multisite`](../../rgw/multisite/README.md).
- [Host network and native clients](host-network.md): which faults work in host mode.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): handles returned with errors.
- [Running native commands](native-commands.md): anything without a typed API.
