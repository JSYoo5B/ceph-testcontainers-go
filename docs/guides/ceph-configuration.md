# Ceph cluster configuration

Concepts of `ceph.Run`'s configuration that span several options. Each
option's own contract, defaults and refusals are in its godoc
(`go doc github.com/JSYoo5B/ceph-testcontainers-go/ceph`, or
[pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph));
the [package README](../../ceph/README.md) gives the usual order of use, and
[Releases and images](releases-and-images.md) the image roles.

## Composition model

Ceph settings are grouped into plain values with chainable, value-returning
methods (`ClusterConfig`, `OSDStorage`, `NetworkConfig`, `ImageOptions`), each
passed to `Run` through its one option function (`WithClusterConfig`,
`WithOSDStorage`, `WithNetworkConfig`, `WithImageOptions`). Docker and
container execution policy (`WithHostNetwork`, `WithStartupTimeout`) and the
testcontainers-style `WithConfigFile` stay option functions, as do `WithAuth`
and the service options of the `rbd`, `cephfs` and `rgw` packages.

A grouped value records which fields were set, so an explicit setting differs
from an unspecified one. Within one value a later call replaces an earlier
one; `Run` refuses a field that two carriers of the same group both set, even
when they agree, so argument order never decides a conflict. All options are
validated together before any Docker resource exists; `Run`'s godoc lists the
refused combinations. The generic `testcontainers` options (`WithEnv`,
`WithLogger`, `WithFiles`, any `CustomizeRequestOption`) apply only to the
container of MON `a`; replacing its image, network, entrypoint, command or
internal paths breaks the bootstrap. The rules are in
[configuration composition](../contributing/configuration-composition.md).

## Service packages

The `rbd`, `cephfs` and `rgw` packages add service resources to the same cluster.
Their options are `ceph.Option` values that `ceph.Run` accepts, so one cluster can
serve RBD, CephFS and S3 clients together:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)),
	rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
	rgw.WithGateways(rgw.Config{Name: "s3"}),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
filesystems := cephfs.Filesystems(cluster) // owned filesystem descriptors
gateways, err := rgw.Gateways(ctx, cluster) // owned gateway descriptors
if err != nil {
	t.Fatal(err)
}
```

| Option | Creates during `Run` | Add to a running cluster | Details |
| --- | --- | --- | --- |
| `rbd.WithPools(configs...)` | Replicated pools initialized with `rbd pool init` (application `rbd`) | `rbd.InitPool` on a pool from `CreatePool` | [rbd](../../rbd/README.md) |
| `cephfs.WithFilesystems(configs...)` | Filesystems with their pools and MDS daemons | `cephfs.Start` | [cephfs](../../cephfs/README.md) |
| `rgw.WithGateways(configs...)` | Named gateways sharing the cluster's zone, each with a test S3 user unless `SkipUserCreation` is set | `rgw.Start` | [rgw](../../rgw/README.md) |

`Run` creates `WithPools` pools first, then RBD pools, filesystems and gateways.
All names, including the pools a filesystem creates, are validated together.
Filesystems and gateways need OSDs and a manager, so they are rejected with
`ClusterConfig.WithNoInitialOSDs` or `ClusterConfig.WithNoInitialManagers`; RBD pools need OSDs but no
manager. The cluster owns all service containers: `Terminate` removes them, and
[ServiceContainers](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.ServiceContainers) lists them.

Each service package also has its own `Run`, which takes every `ceph.Run` option
and adds that service's default when its own option is absent: `rbd.Run` creates
an initialized pool `rbd`, `cephfs.Run` the filesystem `tc-cephfs` with one
active MDS, and `rgw.Run` the default gateway with a test user. Their result is
that package's `Container`, which embeds `*ceph.Container`, so every method on
this page is available on it. A service `Run` rejects bootstrap options that
leave its service without the storage or manager it needs (for example
`ceph.ClusterConfig.WithNoInitialOSDs`); `rbd.Run` accepts `ClusterConfig.WithNoInitialManagers`. For staged
bootstraps, start with `ceph.Run` and add the service later.

## Address families

`NetworkConfig.WithAddressFamily`'s godoc describes each family. The sections
below cover what spans the network options, the configuration file, the
observers and package `fault` on a dual-stack cluster.

### Dual-stack clusters

`AddressDualStack` makes every Messenger bind and advertise both families.
The generated `ceph.conf` sets `ms_bind_ipv4`, `ms_bind_ipv6` and
`ms_bind_prefer_ipv4` to `true` and `public network` to both subnets of the
bridge, such as `172.19.0.0/16,fd62:93fe:2c9e::/64`. Every address vector
then has one entry of each family for each enabled Messenger protocol: v2,
and v1 unless [NetworkConfig.WithMessengerMode](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#NetworkConfig.WithMessengerMode) removes it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack)))
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.DaemonAddresses(ctx)
// addresses.Monitors[0].Addrs:
//   v2 [fd62:93fe:2c9e::2]:3300, v1 [fd62:93fe:2c9e::2]:6789,
//   v2 172.19.0.2:3300, v1 172.19.0.2:6789
// addresses.ActiveManager.Addrs:
//   v2 172.19.0.3:6800, v1 172.19.0.3:6801,
//   v2 [fd62:93fe:2c9e::3]:6802, v1 [fd62:93fe:2c9e::3]:6803
```

The MON vectors in the monmap and in `mon_host` list the IPv6 address first;
MGR, OSD and MDS vectors list the IPv4 address first by default. The order is
deliberate. A MGR fills its vector from one scalar address that it picks first, which is
the IPv4 one, and turns its IPv6 entry into the wildcard `[::]` unless its
own MON session runs over IPv6. A client cannot connect to `[::]`, so with an
IPv4-first MON vector `mgr_command` and `ceph tell mgr` time out from any
other container. Listing the MON's IPv6 address first gives the MGR an IPv6
MON session and a concrete IPv6 entry. The upstream fix would be in
`ceph_mgr.cc`, which seeds that scalar address.

The MGR still binds its IPv6 listener to the wildcard, such as `[::]:6800`,
while it advertises its concrete IPv6 address on the same port; the other
daemons bind the concrete address of each family. The wildcard listener
accepts connections to the advertised address, so clients are not affected,
but a test that reads the MGR container's listening sockets sees `[::]`.

`Run`, [AddManager](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.AddManager) and [ManagerContainer.Remove](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ManagerContainer.Remove) then read
[DaemonAddresses](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#DaemonAddresses) and check that every MON, the active MGR
and every up OSD advertise exactly one concrete IPv4 and one concrete IPv6
address for each protocol. An unspecified, IPv4-mapped or link-local address,
or a missing family, fails the call with `ErrUnsupported` (see
[Error categories](errors.md#the-categories)), naming the daemon and its vector. A
MGR that a caller restarts directly, such as with `Stop` and `Start` on its
container, is not checked; call `DaemonAddresses` after it.

A native client connects to the first entry of each vector it learns. Its own
`ms_bind_*` settings do not change that, and it does not fall back to the
other family: Ceph's `AsyncMessenger::create_connect` takes the first entry of
a supported protocol. Bootstrapped from IPv4 MON addresses, librados still
moves its MON session to the monmap's first, IPv6, entry unless
`mon_host_override` pins it. With the default order, librados and libcephfs
reach MGRs, OSDs and MDSs over IPv4.

To reach a daemon over IPv6, make it list its IPv6 address first with
`ms_bind_prefer_ipv4 = false` in its section of a
[WithConfigFile](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#WithConfigFile) file. `Run` accepts this key only with
`AddressDualStack`.

```ini
# Clients reach mgr.b, osd.1 and the MDS tc-cephfs-1 over IPv6, and the
# other MGRs, OSDs and MDSs over IPv4.
[mgr.b]
ms bind prefer ipv4 = false
[osd.1]
ms bind prefer ipv4 = false
[mds.tc-cephfs-1]
ms bind prefer ipv4 = false
```

In `[global]` the setting makes every MGR, OSD and MDS list IPv6 first. MON
vectors are unaffected, and both orders give concrete addresses of both
families, which the admission check accepts.

`TestDualStackAddressFamily` checks on Tentacle 20.2.4 and Squid 19.2.5:

- every MON, the active MGR before and after a failover, and the public,
  cluster and heartbeat vectors of every OSD advertise a concrete IPv4 and
  IPv6 address;
- a Python librados client in another container, pinned to the IPv4 and then
  to the IPv6 MON addresses with `mon_host_override`, completes a MON command,
  a `pg stat` `mgr_command` and an object write and read, and its open sockets
  show the MON session in that family and the MGR and OSD sessions over IPv4;
- every advertised v2 address of both families, MON, MGR and OSD, answers with
  the Messenger v2 banner.

`TestDualStackSeparateClusterNetwork` checks the same with
`NetworkConfig.WithSeparateClusterNetwork`, with OSD cluster and heartbeat back addresses of
both families from the replication bridge.

`TestDualStackServices` starts CephFS and RGW with two MGRs, two OSDs, an
active and a standby MDS and one gateway, with `[mgr.b]`, `[osd.1]` and
`[mds.tc-cephfs-1]` set to IPv6 first as above. It checks:

- every MDS vector in `fs dump`, active and standby, has a concrete IPv4 and
  IPv6 address, in the configured order. The MDS picks its vector directly
  and has no MGR-like wildcard problem;
- a librados client completes a `pg stat` `mgr_command` and an object write
  and read on each OSD, before and after a MGR failover, and a libcephfs
  client mounts and writes, fsyncs and reads a file, before and after an MDS
  failover. Each session goes to the first entry of the daemon's vector, so
  MGR, OSD and MDS operations complete over IPv4 with one daemon and over IPv6
  with the other. The same TCP connection appears in the client container's
  and in the daemon container's socket table;
- the gateway listens on `0.0.0.0:7480` and `[::]:7480`. A client in another
  container creates a bucket and writes and reads objects with signed S3
  requests to both the gateway's IPv4 and IPv6 addresses, and the published
  host port serves the same objects.

The RGW's own librados session follows the vectors like any other client.

`TestDualStackReplacement` replaces one daemon of each kind with a new
container on a CephFS cluster with two MONs, one MGR, two OSDs and one MDS:

1. [AddMonitor](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.AddMonitor) `c`, then [MonitorContainer.Remove](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#MonitorContainer.Remove) `b`;
2. [AddOSD](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.AddOSD) (osd.2), then [OSDContainer.Remove](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#OSDContainer.Remove) `0`;
3. the MDS container stopped, its GID failed with `ceph mds fail` and a new
   MDS started with `MDS.AddReplacement` (see [cephfs](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/cephfs#MDS.AddReplacement));
4. [AddManager](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Container.AddManager) `b`, then [ManagerContainer.Remove](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#ManagerContainer.Remove) `a`.

`[osd.2]`, `[mds.tc-cephfs-1]` and `[mgr.b]` set `ms bind prefer ipv4 =
false`, so clients reach each replacement over IPv6 and the daemon it
replaces over IPv4. One Python process keeps a librados session and a
libcephfs mount open through all four steps. Before the first step and after
each one it sends a MON command and a `pg stat` `mgr_command`, writes and
reads an object on each OSD, writes, fsyncs and reads a file and reads the
files of the earlier steps. After each step the test checks that:

- the replacement advertises a concrete IPv4 and IPv6 address, and the map's
  epoch has grown. The replacement MDS and MGR have new GIDs, and the
  replacement MDS holds rank 0 with a later incarnation (`fs dump`), while
  the MDS's GID and incarnation stay the same through the other steps;
- no MON, MGR, OSD or MDS vector lists an entry, address and nonce, of a
  replaced daemon, no vector holds `[::]`, and
  [MonitorBootstrapAddresses](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#Observer.MonitorBootstrapAddresses) lists mon.c and not
  mon.b;
- the retained process completed every operation. Its two MON sessions, one
  of librados and one of libcephfs, are on the first, IPv6, entry of a
  current MON, its MGR, OSD and MDS sessions go to the
  first v2 entry of the current daemons, so over IPv6 to each replacement,
  and it has no connection left to an address that only a replaced daemon
  advertised. Each session is matched in the daemon container's socket table
  as above;
- new clients whose MON session `mon_host_override` pins to mon.c's IPv4 or
  IPv6 address complete their operations over that family.

The MGR is replaced last, so the admission check of `ManagerContainer.Remove` also reads
the vectors of mon.c and osd.2. `AddMonitor` and `AddOSD` do not run the
check, and MDS vectors are not part of it. At the end a new process reads back
every file and the last objects.

Docker gives the address of a removed or stopped container to a later one.
A replacement can therefore advertise the old IP address of its predecessor
or of another replaced daemon, such as a new MDS on the address of a removed
OSD. An address alone does not identify a daemon; compare the nonce of the
entry, or the GID of a MGR or MDS. MONs always use nonce 0, so a MON is
identified by its name in the monmap.

#### Cutting one family of a path

[fault.ApplyNetworkProfile](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/fault#ApplyNetworkProfile) with a `Peer`, a `PeerPort`
and `Loss: 100` drops every packet that one container sends to one address
and port. On a dual-stack cluster, a `Peer` that is a daemon's IPv6 address
cuts only that family of the path: the same daemon stays reachable over IPv4,
and the container's paths to other daemons are untouched.

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.DaemonAddresses(ctx)
// osd.1 lists [fd62:93fe:2c9e::4]:6800 first; a native client dials it.
peer := netip.MustParseAddrPort("[fd62:93fe:2c9e::4]:6800")
cut, err := fault.ApplyNetworkProfile(ctx, cluster, client, fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage, Plane: ceph.PublicNetworkPlane,
	Peer: peer.Addr(), PeerPort: peer.Port(), Loss: 100})
// Operations of the client on osd.1 stay pending until cut.Restore(ctx).
```

A native client does not fall back to the other family (see above), so its
operations on that path wait instead of failing over, and they complete after
`Restore` on the same session.

`TestDualStackFamilyCut` checks this on a cluster with one MON, one MGR and
two OSDs, where osd.1 lists IPv6 first. A client container runs two librados
processes whose MON sessions `mon_host_override` pins to mon.a's IPv6 and
IPv4 address. The test cuts the container's packets first to mon.a's IPv6 v2
address and then to osd.1's. During each cut:

- the operation on the cut path, a MON command or a read of an object whose
  primary is osd.1, stays pending;
- the same daemon keeps serving over IPv4: the IPv4-pinned process completes
  MON commands, and osd.1's IPv4 v2 address and its IPv6 v1 port answer with
  their Messenger banners while its IPv6 v2 address does not;
- the process's other paths complete MON, MGR and object operations, and
  netem reports dropped and no sent selected packets.

After `Restore` the pending operation completes on the same TCP connection,
matched in the daemon's socket table, and the client keeps its instance ID.
The pending read returns the bytes of the object it started to read, compared
by SHA-256.
Quorum, the up OSDs, the active MGR's GID and the blocklist are unchanged,
and a new session reads back the exact objects. Finally osd.1's container is
stopped and started: the new process advertises both families again with a
new nonce, and the same client reads from it over IPv6 on a new connection.
The test needs the netem
helper and `NET_ADMIN`, like every `ApplyNetworkProfile`.

## Limitations and release differences

### Test fixture, not a production cluster

- All daemons run on one Docker engine. CRUSH hosts and racks are logical; they
  test placement rules, not physical failure tolerance.
- OSDs use sparse files. Low full ratios simulate fullness; nothing fills a real
  device.
- Kernel RBD mapping and kernel or FUSE CephFS mounts are not part of the module.
  Test data paths with user-space clients (librados, librbd, libcephfs, S3).
- The module prepares and observes server-side conditions. It is not an
  operations SDK and does not wrap data APIs.
- IPv4 is the default. [NetworkConfig.WithAddressFamily](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#NetworkConfig.WithAddressFamily) selects IPv6,
  IPv4-mapped IPv6 or dual-stack on a bridge network; the host network is IPv4 only.

### Fixture defaults that differ from a stock cluster

| Setting | Fixture value | Change with |
| --- | --- | --- |
| `osd_pool_default_size` / `min_size` | `min(2, initial OSDs)` / 1 | `ClusterConfig.WithPoolDefaults` |
| `osd_pool_default_pg_num`, autoscale mode | 8, `off` | `PoolConfig.PGNum`, `Pool.SetPGCount` |
| `mon_allow_pool_size_one` | `true` (replica-1 pools allowed) | config file |
| `mgr_stats_period` | 1 second (faster PG reports) | config file |
| `bluestore_block_size` | 1 GiB sparse file | `OSDStorage.WithBlockSize` |
| `bluestore_cache_size`, autotune | 64 MiB, off | config file `[osd]` |
| `osd_max_object_name_len`, `osd_max_object_namespace_len` | 256, 64 | config file `[osd]` |
| `auth_allow_insecure_global_id_reclaim` | `false` | not configurable |
| Logging | stderr (container logs), no log files | not configurable |

### Networking

- In bridge mode, MON `a` is the only Ceph daemon with published ports
  (`3300/tcp` and `6789/tcp`, or only `3300/tcp` with `MessengerV2Only` and
  `MessengerV2Secure`).
  Native RADOS, RBD and CephFS clients must run in a `WithClient` container; a
  host process cannot reach the OSDs. RGW's HTTP endpoint is published and
  reachable from the host (see [rgw](../../rgw/README.md)).
- Host mode needs a Linux engine, or Docker Desktop with host networking enabled,
  and cannot be combined with a separate cluster network or Docker network
  interruption.

### Asynchronous Ceph behaviour

- PG states, pool usage, quotas and health checks are updated asynchronously.
  Poll observations with a deadline rather than asserting immediately.
- `MGR_DOWN` appears only after `mon_mgr_mkfs_grace` (2 minutes) on a new
  cluster, and `SLOW_OPS` after `osd_op_complaint_time` (30 seconds). Lower such
  values with `override.SetConfig` when a test needs them sooner.
- Changed capabilities and revoked identities take effect for new connections;
  existing sessions may keep their tickets.
- After `Pool.SetPlacement` or `Pool.SetPGCount`, `WaitForClean` can pass before
  the new PG set is reported.

### Concurrency with raw commands

Ceph has no compare-and-swap for configuration, flags, CRUSH, pools or auth.
Helpers check identities (FSID, pool IDs, OSD UUIDs, keys) and refuse to
overwrite changes they did not make, but they cannot detect a concurrent raw
write of the same value. Do not change the same state from raw CLI commands while
a helper applies or restores it.

### Ceph releases

- Tentacle 20.2.4 (`DefaultImage`) is the default and is tested on every change.
- Squid 19.2.5 is supported and its role images run the short test category on
  every change, with the longer categories weekly. The `ceph` package behaves the
  same on Squid; release-specific behaviour is in the [rbd](../../rbd/README.md),
  [cephfs](../../cephfs/README.md) and [rgw](../../rgw/README.md) packages.
- Squid has no `messenger dump` admin command, so encryption of individual
  connections cannot be inspected there (see
  [NetworkConfig.WithMessengerMode](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#NetworkConfig.WithMessengerMode)).
- Squid 19.2.6 has an RGW multisite regression; use 19.2.5 for multisite tests
  (see [rgw](../../rgw/README.md)).
- `ClusterFacts.CephVersion` reflects only the first MON's image (essential). Mixing releases across role
  images is not supported.

See [Releases and images](releases-and-images.md) for details.

### Common pitfalls

- A cluster with one OSD needs replica-1 pools to become clean. The default pool
  size already follows `min(2, OSD count)`; `CreatePool` refuses an explicit
  `Replicas: 2` there, and a size-2 pool created through the CLI never becomes
  clean.
- Small `OSDStorage.WithBlockSize` values with initial OSDs make `Run` time out (see
  [OSDStorage.WithBlockSize](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#OSDStorage.WithBlockSize)).
- A key set with `WithConfigFile` cannot be changed with `override.SetConfig`;
  `mon_allow_pool_delete = false` in a config file blocks `Pool.Remove`.
- `t.Context()` is canceled before cleanup functions run. Use a fresh context in
  `t.Cleanup` for `Restore`, `Pool.Remove` and `CollectDiagnostics`.
- Terminate client containers before the cluster: register their cleanup after
  the cluster's.
- Keys copied by `WithClient` and `ConnectionConfig` are the cluster's admin
  credentials.
- `WaitForClean` and other waits are capped by `WithStartupTimeout` even if your
  context allows more time.
