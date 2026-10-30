# Ceph cluster configuration

How `ceph.Run` builds a cluster and every option that shapes it: topology, OSD storage, the configuration file, networks, Messenger and Cephx, service packages, limitations and release differences. API signatures and short contracts are in the godoc (`go doc github.com/JSYoo5B/ceph-testcontainers-go/ceph`); the [package README](../../ceph/README.md) gives the usual order of use.

## Run function

The Ceph module exposes one entrypoint function to create the Ceph container, and this function receives three parameters:

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error)
```

- `context.Context`, the Go context.
- `string`, the Docker image to use.
- `testcontainers.ContainerCustomizer`, a variadic argument for passing options.

`Run` builds the whole cluster before it returns. The steps are bounded by `ctx`
and by [WithStartupTimeout](#withstartuptimeout) (3 minutes by default):

1. Validate all options together. Contradictory combinations fail before any
   Docker resource is created (see the table below).
2. Create a dedicated bridge network (two with
   [NetworkConfig.WithSeparateClusterNetwork](#networkconfigwithseparateclusternetwork)), or verify the
   engine in host mode.
3. Start MON `a`, record `ClusterFacts.CephVersion` (see [FactsOf](ceph-operations.md#factsof)), and read the generated
   `ceph.conf` and the `client.admin` keyring.
4. Add the remaining monitors (`b`, `c`, ...). With more than one MON, a separate
   CLI control container is started as well (see
   [ControlContainer](ceph-operations.md#controlcontainer)).
5. Start managers `a`, `b`, ...; the first becomes active, the others standby.
6. Register, format and start each OSD and wait until it is up and in.
7. Wait for an available active MGR. With
   [ClusterConfig.WithNoInitialManagers](#clusterconfigwithnoinitialmanagers), wait for MON quorum and for
   the OSDs to be up and in instead.
8. Create the [ClusterConfig.WithPools](#clusterconfigwithpools) pools, then the service resources passed by
   service package options: RBD pools, CephFS filesystems and RGW gateways.

`Run` does not wait for clean placement groups. Call
[WaitForClean](ceph-operations.md#waitforclean) before application I/O.

When `Run` fails after it created Docker resources, it returns the partially
built `*Container` together with the error. That container owns every resource
created so far and must be terminated. Calling
`testcontainers.CleanupContainer(t, cluster)` before checking the error does this;
it ignores a nil container.

The returned `*Container` embeds the `testcontainers.Container` of MON `a`, so
generic methods such as `Exec`, `Logs`, `Host` and `MappedPort` address that
container. `Terminate` is overridden and removes the whole cluster (see
[Terminate](ceph-operations.md#terminate)).

`Run` rejects these option combinations before creating any resource, regardless
of option order:

| Combination | Result |
| --- | --- |
| `NetworkConfig.WithHostAddress` without `WithHostNetwork` | `NetworkConfig.WithHostAddress requires WithHostNetwork` |
| `WithHostNetwork` with `NetworkConfig.WithSeparateClusterNetwork` | `NetworkConfig.WithSeparateClusterNetwork requires bridge mode` |
| `NetworkConfig.WithMessengerPolicy` with `MessengerV2Secure` | rejected; `MessengerV2Secure` fixes every connection mode |
| `NetworkConfig.WithAddressFamily` with any family but `AddressIPv4`, and `WithHostNetwork` | `NetworkConfig.WithAddressFamily(...) requires bridge mode` |
| `NetworkConfig.WithAddressFamily(AddressIPv4MappedIPv6)` with `NetworkConfig.WithSeparateClusterNetwork` | rejected |
| One field of a value group set by two carriers, for example two `WithClusterConfig` values that both set the OSDs | rejected, even when they agree |
| `ClusterConfig.WithNoInitialOSDs` with initial pools, filesystems or gateways | rejected; add OSDs before provisioning them |
| `ClusterConfig.WithNoInitialManagers` with initial filesystems or gateways | rejected; add a manager before provisioning them (initial pools are allowed) |
| Fewer OSDs in the default CRUSH root than the default replica count | `default CRUSH root ... needs N initial OSDs` |
| An initial pool needs more failure domains than the initial layout has | `initial pool ... needs N distinct ... domains` |
| Duplicate pool, filesystem or gateway names, including the pools a filesystem creates | rejected as duplicates |

Some environments can only be checked after resources exist. In host mode, `Run`
fails with [ErrHostNetworkUnavailable](ceph-operations.md#errhostnetworkunavailable) when the engine
does not run Linux containers, the selected address is not assigned on the
Docker host, or the test process cannot reach the first MON.

### Image

`Run(ctx, image)` uses a complete aggregate image; the default is
`ceph.DefaultImage`. `ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(set))`, passed to `Run` with
an empty image argument, selects the `ImageSet` role model. Essential supplies MON/MGR/OSD; Client, CephFS,
RBDAux and RGW are required only by their selected features. Missing optional
roles never fall back to essential. See [image selection](releases-and-images.md#role-images).

## Container Options

Ceph settings are grouped into plain values with chainable, value-returning
methods (`ClusterConfig`, `OSDStorage`, `NetworkConfig`, `ImageOptions`), each
passed to `Run` through its one option function. Docker and container execution
policy (`WithHostNetwork`, `WithStartupTimeout`) and the testcontainers-style
`WithConfigFile` stay functions.

### ClusterConfig

```golang
type ClusterConfig struct { /* unexported */ }

func (ClusterConfig) WithMonitorCount(count int) ClusterConfig
func (ClusterConfig) WithManagerCount(count int) ClusterConfig
func (ClusterConfig) WithNoInitialManagers() ClusterConfig
func (ClusterConfig) WithOSDCount(count int) ClusterConfig
func (ClusterConfig) WithInitialOSDs(configs ...OSDConfig) ClusterConfig
func (ClusterConfig) WithNoInitialOSDs() ClusterConfig
func (ClusterConfig) WithDefaultCRUSHRoot(root string) ClusterConfig
func (ClusterConfig) WithPoolDefaults(replicas, minSize int) ClusterConfig
func (ClusterConfig) WithPools(configs ...PoolConfig) ClusterConfig
func WithClusterConfig(cluster ClusterConfig) Option
```

The topology and placement group: the initial MON, MGR and OSD topology, the
initial OSD layout, the default CRUSH root, the pool defaults and the initial
pools. The zero value changes nothing, and each field records whether it was
set. The managers (`WithManagerCount`, `WithNoInitialManagers`) are one field,
and so are the initial OSDs (`WithOSDCount`, `WithInitialOSDs`,
`WithNoInitialOSDs`): within one value a later call replaces the earlier one,
and `Run` refuses a field that two `WithClusterConfig` options both set, even
when they agree.

```golang
cluster := ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3).WithPoolDefaults(3, 2)
c, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(cluster))
```

### ClusterConfig.WithOSDCount

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3))
```

Sets the number of initial OSD containers. The default is 2 and the minimum is 1;
use [WithNoInitialOSDs](#clusterconfigwithnoinitialosds) to start without storage. Each OSD
gets the default CRUSH location: a logical host `osd-<ID>` under the default
root.

Without [WithPoolDefaults](#clusterconfigwithpooldefaults), the cluster's default pool size is
`min(2, OSD count)` with `min_size` 1, so a one-OSD cluster gets replica-1 pools
that can become clean.

### ClusterConfig.WithInitialOSDs

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithInitialOSDs(
	ceph.OSDConfig{Host: "host-a", Rack: "rack-a", DeviceClass: "fast"},
	ceph.OSDConfig{Host: "host-b", Rack: "rack-b", DeviceClass: "fast"},
	ceph.OSDConfig{Host: "host-c", Rack: "rack-b", DeviceClass: "slow"},
))
```

Selects the number of initial OSDs and the logical CRUSH location of each. Each
`ceph.OSDConfig` describes one OSD:

| Field | Default | Meaning |
| --- | --- | --- |
| `Host` | `osd-<ID>` | CRUSH host bucket. Several OSDs may share a host. |
| `Rack` | none | Optional CRUSH rack bucket. Several hosts may share a rack. |
| `Root` | the cluster's default root | CRUSH root bucket (see [WithDefaultCRUSHRoot](#clusterconfigwithdefaultcrushroot)). |
| `DeviceClass` | none | Device class used by `PoolConfig.DeviceClass` and `PoolPlacement.DeviceClass`. |

An empty list is rejected, and one host cannot appear under different racks or
roots. Hosts and
racks are simulated placement domains on one Docker engine; they let you test
CRUSH behaviour, not physical failure tolerance. Pools that need `N` distinct
domains are validated against this layout before anything is created.

### How Run starts the initial OSDs

`Run` registers the initial OSDs one after another in the order of the layout,
so OSD IDs, keys and CRUSH host buckets are allocated in that order on every
run: the first OSD gets the lowest free ID, and each host bucket is created
before any OSD boots. It then creates and starts up to four OSD containers at
a time and waits until all OSDs are up and in. The first manager boots while
the OSDs are registered and started; further managers from
[WithManagerCount](#clusterconfigwithmanagercount) are added after it is registered, so it
stays the active one.

If an OSD fails to start, `Run` starts no further OSD and returns an error that
names it. Every registered OSD, and every container that was created, stays
owned by the returned cluster, and `Terminate` removes them. On
`quay.io/ceph/ceph:v20.2.4`, starting the OSDs together cuts `Run` from about 17
to 11 seconds for three OSDs and from about 27 to 16 seconds for five.
[AddOSD](ceph-operations.md#addosd) still adds one OSD at a time.

### ClusterConfig.WithNoInitialOSDs

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs())
```

Starts only the MON and MGR topology. Add storage later with
[AddOSD](ceph-operations.md#addosd), then create pools.

- Initial pools, filesystems and gateways are rejected before allocation.
- Prospective pool defaults stay at size 2 and `min_size` 1. Use
  `WithPoolDefaults(1, 1)` if you will add only one OSD; adding OSDs does not
  change existing pool sizes.
- Run still waits for an available MGR. No storage readiness is implied: Ceph's
  own `.mgr` pool can have pending PGs, and health is usually `HEALTH_WARN`
  (for example `TOO_FEW_OSDS`) until OSDs join.
- With [OSDStorage.WithInMemory](#osdstoragewithinmemory), the tmpfs volume is
  created on the first `AddOSD`.
- [OSDStorage.WithVolume](#osdstoragewithvolume) also defers its ordinary
  named volume until the first `AddOSD`.
- Removing the last OSD is still refused later.

Use it for MON-only or MGR-only tests, bootstrap-ordering tests, and to apply OSD
configuration (for example a scheduler or benchmark setting) before any OSD
starts. Combined with [WithNoInitialManagers](#clusterconfigwithnoinitialmanagers) it gives a
MON-only phase. See [Start without OSDs, then add storage](ceph-recipes.md#start-without-osds-then-add-storage).

### ClusterConfig.WithMonitorCount

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3))
```

Sets the initial number of monitors (default 1, minimum 1). Monitors are named
`a`, `b`, `c`, ... Three monitors allow quorum failover tests; even counts are
allowed for deliberate quorum-loss scenarios. With more than one monitor the
cluster also starts an independent CLI control container, so fixture commands
keep working while MON `a` is stopped.

### ClusterConfig.WithManagerCount

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithManagerCount(2))
```

Sets the initial number of managers (default 1, minimum 1). Managers are named
`a`, `b`, ...; one becomes active and the rest register as standbys.
`WithManagerCount(0)` is invalid; use
[WithNoInitialManagers](#clusterconfigwithnoinitialmanagers) instead.

### ClusterConfig.WithNoInitialManagers

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialManagers())
```

Starts the MON and OSD topology without creating a manager. Add the first manager
with [AddManager](ceph-operations.md#addmanager).

- `Run` waits for MON quorum and for the initial OSDs to be up and in. It does not
  wait for MGR availability, modules, PG statistics or health.
- Initial pools are allowed and created; initial filesystems and gateways are
  rejected.
- [WaitForClean](ceph-operations.md#waitforclean) and [WaitForPGClean](ceph-operations.md#waitforpgclean) need an
  available manager, so they cannot succeed until you add one.
- Ceph may raise `MGR_DOWN` (only after the new monitor's `mon_mgr_mkfs_grace`,
  2 minutes by default), and it can escalate to `HEALTH_ERR` while client data I/O
  still works.
- The essential image must still contain `ceph-mgr` and its dependencies.

To simulate a manager outage in an already configured cluster, stop the existing
manager container instead (see [Managers](ceph-operations.md#managers)).

### ClusterConfig.WithPools

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.
	WithOSDCount(3). // the EC pool needs K+M = 3 distinct OSDs
	WithPools(
		ceph.PoolConfig{Name: "app-data", Application: "rados"},
		ceph.PoolConfig{Name: "app-ec", Application: "rados",
			ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
	))
```

Creates pools after the initial OSDs have started, with the same validation and
result as [CreatePool](ceph-operations.md#createpool). The value copies the configs. Defaults from
other settings are resolved after all options have been applied. `Run` does not wait for the pools to become clean.

`ceph.PoolConfig` fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `Name` | required | Letters, digits, `_`, `.` and `-`; must not start with `.` or `-`. |
| `PGNum` | 8 | PG and PGP count. The autoscaler is off for fixture pools. |
| `Replicas` | cluster default | Replica count, 1 to 10. Must be zero for erasure-coded pools. |
| `MinSize` | cluster default (`K+1` for EC) | Replicated: 1 to `Replicas`. EC: `K` to `K+M`. |
| `Application` | none | `rados`, `rbd`, `cephfs`, `rgw` or a custom name. |
| `FailureDomain` | `osd` | `osd`, `host` or `rack`; each replica or shard goes to a distinct bucket. |
| `CRUSHRoot` | the cluster's default root | Root of the placement hierarchy. |
| `DeviceClass` | none | Restricts placement to OSDs configured with this class. |
| `ErasureCode` | nil (replicated) | `*ceph.ErasureCodeConfig`. |
| `NoHashPSPool` | false | Clears the pool's `hashpspool` flag right after creation. The placement seed of each PG is then the legacy sum of PG number and pool ID instead of a hash of the two, as for pools that predate the flag. Other pools, such as the MGR's `.mgr`, keep the flag unless `osd_pool_default_flag_hashpspool` is off (see [Reproduce a minimal legacy placement setup](ceph-recipes.md#reproduce-a-minimal-legacy-placement-setup)). |

`ceph.ErasureCodeConfig` selects `K` data and `M` parity shards (`K >= 2`,
`M >= 1`, `K+M <= 128`) with the portable Jerasure `reed_sol_van` plugin. An
explicit `MinSize` of `K` permits I/O without any spare shard during a failure.
`AllowOverwrites` enables partial object writes; it is required (and enforced)
for EC pools with application `rbd` or `cephfs`. RBD and CephFS metadata must
stay in a replicated pool; use the EC pool as the image's data pool or as an
additional CephFS data pool.

For RBD pools that need `rbd pool init`, use `rbd.WithPools` instead (see
[Service packages](#service-packages)).

### ClusterConfig.WithPoolDefaults

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPoolDefaults(3, 2))
```

Sets `osd_pool_default_size` and `osd_pool_default_min_size` for pools that Ceph
or services create and for `PoolConfig` values that omit `Replicas`/`MinSize`.
Requires `1 <= minSize <= replicas <= 10`. Without this setting the default is
`size = min(2, initial OSD count)` and `min_size = 1` (2/1 with
`WithNoInitialOSDs`). Set the defaults here rather than in a config file; the
config file rejects these keys.

### ClusterConfig.WithDefaultCRUSHRoot

```golang
ceph.WithClusterConfig(ceph.ClusterConfig{}.WithDefaultCRUSHRoot("tc-storage"))
```

Selects the CRUSH root used for pools that Ceph creates itself (such as `.mgr`)
and for omitted `PoolConfig.CRUSHRoot` and `OSDConfig.Root` fields. Without this
setting the root is `default`. The initial layout must contain enough OSDs in this
root for the default replica count; this check is skipped with
`WithNoInitialOSDs`.

### OSDStorage

```golang
type OSDStorage struct { /* unexported */ }

func (OSDStorage) WithBlockSize(size int64) OSDStorage
func (OSDStorage) WithObjectStore(store OSDObjectStore) OSDStorage
func (OSDStorage) WithInMemory(maxBytes int64) OSDStorage
func (OSDStorage) WithVolume() OSDStorage
func WithOSDStorage(storage OSDStorage) Option
```

The OSD storage group: where and how every OSD of the cluster, including OSDs
added later, keeps its data. `OSDStorage` is a plain value with chainable,
value-returning methods; `WithOSDStorage` is its one option function. The zero
value changes nothing, and each field records whether it was set, so an explicit
setting differs from an unspecified one (`WithBlockSize(0)` is refused, not the
default). Within one value a later call of the same method replaces the earlier
one. `Run` refuses a field that two `WithOSDStorage` options both set, even when
they agree. Incompatible combinations fail before resource allocation.

```golang
storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
```

### OSDStorage.WithBlockSize

```golang
ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(256 << 20))
```

Sets the logical size in bytes of each OSD's sparse BlueStore file, or the
capacity of each MemStore OSD (see [OSDStorage.WithObjectStore](#osdstoragewithobjectstore)).
The default is 1 GiB and the minimum accepted value is 64 MiB. These are disposable files,
not real disks; the space is allocated as data is written.

Caveat: with Ceph's default mClock scheduler, a fresh OSD runs a startup capacity
benchmark that can write about 100 objects of 4 MiB (400 MiB) before deleting
them. That alone fills a 64 to 256 MiB file, BlueStore aborts the OSD, and `Run`
times out waiting for it. For small OSDs, start with
[ClusterConfig.WithNoInitialOSDs](#clusterconfigwithnoinitialosds), set `osd_mclock_skip_benchmark` with
[override.SetConfig](../../override/README.md#setconfig), then add OSDs. The option itself never
changes the scheduler or the benchmark. See
[Run small OSDs](ceph-recipes.md#run-small-osds).

### OSDStorage.WithInMemory

```golang
ceph.WithOSDStorage(ceph.OSDStorage{}.WithInMemory(2 << 30))
```

Keeps every OSD's BlueStore file in one Docker-managed tmpfs volume instead of
the OSD containers' writable layers. `maxBytes` must be positive and is the
allocation ceiling for the whole cluster's OSD volume, independent of each file's
logical [OSDStorage.WithBlockSize](#osdstoragewithblocksize). It can be smaller than the sum of
the logical sizes.

- It is not a per-OSD reservation and does not limit daemon memory. The volume
  uses the Docker daemon's memory (the Linux VM's memory on Docker Desktop, the
  remote host's on a remote engine), competes with all daemons for RAM, and may
  swap.
- A full volume makes BlueStore fail I/O and can abort an OSD; there is no
  fallback to disk. Size the cap for replicas, metadata, recovery and the startup
  benchmark described above. Deleting Ceph objects does not necessarily release
  allocated tmpfs pages.
- The module creates a uniquely named volume and one idle keeper container (from
  the essential image, without network) before the first OSD. The keeper holds the
  volume so OSD `Stop`/`Start` keeps data. Purging an OSD with
  [OSDContainer.Remove](ceph-operations.md#osdcontainerremove) discards its directory.
- `Terminate` removes the OSDs, the keeper and the volume. Losing the keeper or
  restarting the Docker daemon or VM loses data; the module refuses to continue
  with a restarted keeper.
- Requires a Linux Docker engine that can mount local-driver tmpfs volumes
  (compatible Docker Desktop engines work). Rootless or managed engines may
  refuse it. No host path, privileged container or extra image is needed.

Without either storage-volume option, the default remains a container-local
sparse file per OSD.

### OSDStorage.WithVolume

```golang
ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume())
```

Keeps classic BlueStore OSD directories in one ordinary Docker local named
volume owned by the cluster. Each OSD still gets its own sparse file with
[OSDStorage.WithBlockSize](#osdstoragewithblocksize) as its logical capacity. Unlike the tmpfs
option, bytes do not depend on a live keeper. This option cannot be combined
with MemStore or `OSDStorage.WithInMemory`; invalid combinations fail before
resource allocation.

The first OSD allocates a unique volume and an isolated idle helper from the
essential image. The helper has no network or cluster credentials and manages
fresh and purged directories. A stopped or restarted helper refuses further
storage operations before new OSD registration; it is not automatically replaced.
Existing OSDs can still stop/start with their original containers.

`OSDContainer.Remove` drains and purges the native registration, removes its container,
then discards only its directory. `Terminate` removes all owned OSDs and the
helper before deleting the volume without force. A failed or busy deletion
retains ownership for another `Terminate` call. Changed volume metadata is
refused before directory operations or deletion. Never externally modify or
attach this owned volume while using the cluster.

There is no caller-supplied volume, host path, device or driver option.
[OSDContainer.Detach](ceph-operations.md#osdcontainerdetach) and [DetachedOSD.Reopen](ceph-operations.md#detachedosdreopen) restart a retained store in
a new container; this backend gives no host-reboot durability. `Terminate`
always discards its owned volume, including detached stores. See
[the storage design](../internals/osd-storage.md#volume-storage) and
[retained reopen](../internals/osd-storage.md#retained-detach-and-reopen).

### OSDStorage.WithObjectStore

```golang
ceph.WithOSDStorage(ceph.OSDStorage{}.WithObjectStore(ceph.OSDMemStore))
```

Selects the object store of every OSD, including OSDs added later. The
argument is a `ceph.OSDObjectStore`:

| Value | Store |
| --- | --- |
| `ceph.OSDBlueStore` | Default. A sparse BlueStore file per OSD. |
| `ceph.OSDMemStore` | Ceph's in-memory test store, `memstore`. Data lives in the OSD process's memory; [OSDStorage.WithBlockSize](#osdstoragewithblocksize) sets its capacity (`memstore_device_bytes`). |

- MemStore saves its contents to the OSD's data directory when the store is
  unmounted, so the fixture disables Ceph's fast OSD shutdown for MemStore
  OSDs. A stopped OSD keeps its data across `Stop` and `Start`; an OSD that is
  killed, or whose container is removed, loses it.
- MemStore cannot be combined with
  [OSDStorage.WithInMemory](#osdstoragewithinmemory) or
  [OSDStorage.WithVolume](#osdstoragewithvolume), which use BlueStore files, and [fault.InjectObjectDataError](../../fault/README.md#injectobjectdataerror)
  needs BlueStore.
- `ceph osd metadata` reports `osd_objectstore` as `memstore`.

### WithConfigFile

```golang
ceph.WithConfigFile(filepath.Join("testdata", "ceph.conf"))
```

Merges a native `ceph.conf` file from the test host into the bootstrap
configuration, like the `WithConfigFile` options of other testcontainers-go
modules. The cluster layout is still chosen with Go options.

```ini
# testdata/ceph.conf
[osd]
bluestore cache size = 134217728
osd memory target = 1073741824
[mon]
mon_max_pg_per_osd = 320
[client.admin]
rados_osd_op_timeout = 25
```

- The merged file becomes `/etc/ceph/ceph.conf` before the first MON creates its
  store. Every MON, MGR, OSD, MDS and RGW, every `WithClient` container and
  [ConnectionConfig](ceph-operations.md#connectionconfig) get it, so bootstrap-only settings work.
  It does not change a running cluster; use [override.SetConfig](../../override/README.md#setconfig)
  for that.
- An entry replaces the fixture's default for the same section and key. Spaces,
  `-` and `_` in key names are equivalent, as in Ceph. Several files are merged in
  order, later files overriding earlier ones key by key, so a shared base file
  plus a per-test file works.
- Ceph gives local files precedence over the MON configuration database. A key set
  in this file cannot be changed later by `override.SetConfig` or `ceph config set`;
  leave keys you want to change during the test out of the file.
- RGW runs as `client.admin`, so RGW settings belong in `[client]` or
  `[client.admin]`. `[client.rgw.*]` sections do not apply.
- Ceph silently ignores some values: for example `osd_memory_target` below
  896 MiB keeps the 4 GiB default. Check the effective value with
  `ceph config show osd.0 osd_memory_target`.

The file is validated before any container is created. Keys that the fixture
owns, or passes on daemon command lines where a file value would be ignored, are
rejected with the option to use instead:

| Keys | Use instead |
| --- | --- |
| `fsid`, `mon_host`, `mon_initial_members`, `mon_addr` | generated; `ClusterConfig.WithMonitorCount` |
| `public_network`, `cluster_network`, `public_addr`, `cluster_addr`, `*_bind_addr` | `WithHostNetwork`, `NetworkConfig.WithSeparateClusterNetwork`, `NetworkConfig.WithHostAddress` |
| `auth_*`, `keyring`, `key`, `keyfile` | `WithAuth`; fixture Cephx setup |
| `ms_*_mode`, `ms_mon_*_mode`, `ms_bind_*` except `ms_bind_prefer_ipv4` | `NetworkConfig.WithMessengerMode`, `NetworkConfig.WithMessengerPolicy`, `ClientContainerOptions.WithMessengerPolicy`, `NetworkConfig.WithAddressFamily`, `WithHostNetwork` |
| `osd_pool_default_size`, `osd_pool_default_min_size` | `ClusterConfig.WithPoolDefaults` |
| `osd_pool_default_crush_rule` | `ClusterConfig.WithDefaultCRUSHRoot` |
| `bluestore_block_size` | `OSDStorage.WithBlockSize` |
| `osd_objectstore`, `bluestore_block_path`, `bluestore_block_create` | fixed OSD storage layout |
| `log_file`, `log_to_file`, `log_to_stderr`, `err_to_stderr`, `mon_cluster_log_to_*` | daemon logs always go to container output |
| `admin_socket`, `run_dir`, `mon_data`, `osd_data`, `mgr_data`, `mds_data` | fixed paths |
| `crash_dir` | fixed at `/var/lib/ceph/crash` (`ceph.CrashDir`), where [CollectCrashMetadata](ceph-operations.md#collectcrashmetadata) reads |
| `mds_join_fs`, `mds_cache_memory_limit` | set on the MDS command line |
| `rgw_frontends`, `rgw_thread_pool_size`, `rgw_exit_timeout_secs`, `rgw_realm`, `rgw_zonegroup`, `rgw_zone`, `rgw_sync_obj_etag_verify`, `rgw_sync_lease_period` | set on the RGW command line |

`ms_bind_prefer_ipv4` is the one accepted `ms_bind_*` key, and only with
`AddressDualStack`: `Run` rejects it with `ErrInvalidRequest` for the other
families. It orders the families in a daemon's advertised vectors; see
[Dual-stack clusters](#dual-stack-clusters).

The parser also rejects keys outside a section, `!include`/`include`/`includedir`,
lines continued with a trailing `\`, duplicate keys within one section of one
file, NUL bytes or invalid UTF-8, and files (or merged results) larger than
64 KiB. Ceph treats `#` and `;` as comment starts even inside quotes, so a value
such as `"a;b"` is rejected; escape literal characters as `\#` and `\;`.

### NetworkConfig

```golang
type NetworkConfig struct { /* unexported */ }

func (NetworkConfig) WithAddressFamily(family AddressFamily) NetworkConfig
func (NetworkConfig) WithSeparateClusterNetwork() NetworkConfig
func (NetworkConfig) WithHostAddress(address string) NetworkConfig
func (NetworkConfig) WithMessengerMode(mode MessengerMode) NetworkConfig
func (NetworkConfig) WithMessengerPolicy(policy MessengerPolicy) NetworkConfig
func WithNetworkConfig(network NetworkConfig) Option
```

The network and Messenger group: the address family, the separate cluster
network, the host-mode address, the Messenger protocol mode and the Messenger
connection policy. `NetworkConfig` is a plain value with chainable,
value-returning methods; `WithNetworkConfig` is its one option function. The zero
value changes nothing, and each field records whether it was set, so an explicit
setting differs from an unspecified one (`WithAddressFamily(ceph.AddressIPv4)`
and `WithMessengerMode(ceph.MessengerDefault)` are settings, `WithHostAddress("")`
is refused). Within one value a later call of the same method replaces the earlier
one. `Run` refuses a field that two `WithNetworkConfig` options both set, even
when they agree; different fields in different options compose in any order. The
value copies the lists of a `MessengerPolicy`, so changing them afterwards does
not change it, and a non-nil empty list is still refused. Incompatible
combinations (see the table above) fail before resource allocation.
`WithHostNetwork` is container execution policy and stays its own option. It
replaces `WithAddressFamily`, `WithSeparateClusterNetwork`, `WithHostAddress`,
`WithMessengerMode` and `WithMessengerPolicy`.

```golang
network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network))
```

### NetworkConfig.WithMessengerMode

```golang
ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure))
```

Selects, at bootstrap, the Messenger protocols that every daemon listens on and
advertises, and whether the connection modes stay configurable. Both are
immutable for the lifetime of the cluster.

| Mode | Listeners and addresses | Connection modes |
| --- | --- | --- |
| `ceph.MessengerDefault` | v1 and v2 | Ceph's defaults, changeable with [NetworkConfig.WithMessengerPolicy](#networkconfigwithmessengerpolicy), [override.SetMessengerPolicy](../../override/README.md#setmessengerpolicy) and [ClientContainerOptions.WithMessengerPolicy](#clientcontaineroptions). Not every connection is encrypted. |
| `ceph.MessengerV2Only` | v2 only | As with `MessengerDefault`: CRC, secure or a per-daemon-type mix, changeable later. |
| `ceph.MessengerV2Secure` | v2 only | Fixed to encrypted Messenger v2 (`secure`, AES-GCM) for every connection; no CRC mode. |

With `MessengerV2Only`, the global `ceph.conf` sets `ms_bind_msgr1` to `false`
and `ms_bind_msgr2` to `true` and nothing else, so a CRC-only cluster is:

```golang
crc := []ceph.MessengerConnectionMode{ceph.MessengerCRC}
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Only).WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc})),
)
// Native clients attached with WithClient(ceph.ClientContainerOptions{}.WithMessengerPolicy(
// ceph.ClientMessengerPolicy{MonitorsAndManagers: crc, OtherDaemons: crc}))
// connect with CRC; a client that offers only secure is refused.
```

Connections between daemons keep Ceph's defaults in both configurable modes, so
every policy leaves the cluster working.

To express a `ceph.conf` with `ms_bind_msgr1 = false` with these options:

| `ceph.conf` | Options |
| --- | --- |
| All six `ms_*_mode` and `ms_mon_*_mode` options `secure` | `NetworkConfig.WithMessengerMode(ceph.MessengerV2Secure)` |
| `ms_mon_service_mode` of the MONs or MGRs, `ms_service_mode` of the OSDs | `NetworkConfig.WithMessengerMode(ceph.MessengerV2Only)` and the matching fields of `NetworkConfig.WithMessengerPolicy` |
| `ms_mon_client_mode` and `ms_client_mode` of a client | `ClientContainerOptions.WithMessengerPolicy` on that client's container |
| `ms_cluster_mode` and `ms_mon_cluster_mode` between daemons | Ceph's defaults: the policy options leave them alone; with `MessengerV2Only`, [override.SetConfig](../../override/README.md#setconfig) can change them |
| `mon_host` with only v2 addresses, port 3300 | [MonitorBootstrapAddresses](ceph-operations.md#monitorbootstrapaddresses), or `MappedPort(ctx, "3300/tcp")` in bridge mode; there is no 6789 to map |

With `MessengerV2Secure`, the global `ceph.conf` sets `ms_cluster_mode`,
`ms_service_mode`, `ms_client_mode`, `ms_mon_cluster_mode`,
`ms_mon_service_mode` and `ms_mon_client_mode` to `secure`, `ms_bind_msgr1` to
`false` and `ms_bind_msgr2` to `true`.

In both v2 modes:

- The monmap and `mon_host` contain only v2 addresses, and no daemon listens on
  a v1 port. In bridge mode the MON publishes only `3300/tcp`, so
  `MappedPort(ctx, "6789/tcp")` fails; in host mode only one MON port is
  reserved.
- Later daemons, `WithClient` and `ClientConfig.WithIdentity` containers and
  [ConnectionConfig](ceph-operations.md#connectionconfig) inherit the policy. MON refreshes only
  rewrite `mon_host` and keep it.
- [override.SetConfig](../../override/README.md#setconfig) refuses `ms_bind_msgr1` and
  `ms_bind_msgr2` in every section and mask with `ErrInvalidRequest`, because
  the local file would override them anyway. With `MessengerV2Secure` it also
  refuses the six mode options.

With `MessengerV2Secure`:

- A client configured for CRC only is rejected by the cluster (`AUTH_BAD_METHOD`)
  rather than timing out.
- Cephx authentication and Messenger encryption are separate settings; Cephx is
  always required.
- This option does not enable HTTPS for RGW (see [rgw](../../rgw/README.md)).

`ClusterFacts.MessengerMode` ([FactsOf](ceph-operations.md#factsof)) reports the selected mode. It does not inspect
live connections. To verify encryption on Tentacle, query a daemon's
`messenger dump` admin command (through `ceph tell`), which reports the mode and
cipher of each READY connection. Squid has no `messenger dump`; check the
daemons' `ms_*_mode` settings instead.

### WithAuth

```golang
ceph.WithAuth(ceph.AuthConfig{
	KeyType:          ceph.CipherAES256K,
	ServiceCipher:    ceph.CipherAES,
	MonTicketTTL:     10500 * time.Millisecond,
	ServiceTicketTTL: 30 * time.Second,
})
```

Selects the Cephx key type, the MonMap ciphers and the ticket lifetimes of a new
cluster. Zero fields keep the defaults of the image's Ceph release. On Tentacle
20.2.4 and later every key and cipher is `aes256k`. Squid and Tentacle before
20.2.4 generate only `aes` keys and have no MonMap ciphers.

| Field | Sets |
| --- | --- |
| `KeyType` | the type of the keys the fixture generates itself: `mon.`, `client.admin` and OSD keys, and the default for [CreateClient](ceph-operations.md#createclient) |
| `ServiceCipher` | the MonMap's `auth_service_cipher`, which encrypts service tickets; defaults to `KeyType` |
| `AllowedCiphers` | the MonMap's `auth_allowed_ciphers`, the key types that the MONs accept for authentication; defaults to the distinct values of `KeyType`, `ServiceCipher` and `PreferredCipher`, and must contain all three |
| `PreferredCipher` | the MonMap's `auth_preferred_cipher`, the type of keys that the MONs generate with `auth get-or-create`: MGR, MDS and RGW identities and mirror peers; defaults to `KeyType` |
| `MonTicketTTL` | `auth_mon_ticket_ttl` |
| `ServiceTicketTTL` | `auth_service_ticket_ttl` |

The cipher type `ceph.CephxCipher` has two values: `ceph.CipherAES` (`aes`,
AES-128) and `ceph.CipherAES256K` (`aes256k`, AES-256 with Kerberos-style key
derivation). The decoded key itself
records its type: the first two bytes, little-endian, are 1 for `aes` and 2 for
`aes256k`.

- The key type and ciphers apply from the first MON: they are passed to
  `ceph-authtool` and `monmaptool` before the MON creates its store. A cipher
  field needs `KeyType`.
- A `PreferredCipher` of `aes` also sets `mon_auth_allow_insecure_key = true` in
  `[mon]`, which Tentacle requires before its MONs generate AES keys.
- Lifetimes keep fractions of a second and are written to the `[global]` section
  of the bootstrap `ceph.conf`. Every daemon, `WithClient` container and
  [ConnectionConfig](ceph-operations.md#connectionconfig) gets them, and
  [override.SetConfig](../../override/README.md#setconfig) cannot change them later. MONs rotate
  service secrets on their tick, so lifetimes of a few seconds also need a
  shorter `mon_tick_interval`, which [WithConfigFile](#withconfigfile) can set.
- Keep `MonTicketTTL` at ten seconds or more. MON clients check their tickets
  only every few seconds, and a renewal that comes after the MONs dropped the
  secret of the old ticket is refused with `EACCES`, after which the client
  does not authenticate again. With 1.5 seconds, a MGR that was busy loading its
  modules lost its MON session this way in about one of eight bootstraps on a
  loaded host. Ceph 19.2.5 and 20.2.4 then abort that MGR with
  `ceph_assert(active_modules)` once `mgr_module_load_expiration` (20 seconds)
  has passed, so `Run` fails waiting for the MGR.
- The MonMap ciphers can change on a running Tentacle cluster with `ceph mon set`:

  ```golang
  _, err := cluster.Ceph(ctx, "mon", "set", "auth_allowed_ciphers", "aes,aes256k")
  ```

  `ceph mon dump` reports the current values; it lists allowed ciphers in its own
  order.
- `aes256k` and the MonMap ciphers need an image whose `ceph-authtool` has
  `--key-type` and whose `monmaptool` has the cipher options. Squid and Tentacle
  20.2.0 to 20.2.3 have neither. `Run` checks the options themselves, not the
  release number. Such an image accepts a configuration in which every cipher
  is `aes`, and `Run` fails before any daemon starts, with
  `needs ceph-authtool --key-type and monmaptool cipher options`, when one is
  `aes256k`.
- `WithConfigFile` rejects `auth_*` keys, so set the lifetimes here.

### NetworkConfig.WithMessengerPolicy

```golang
ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerPolicy(ceph.MessengerPolicy{
	Monitors: []ceph.MessengerConnectionMode{ceph.MessengerSecure},
	Managers: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
}))
```

Selects, for each daemon type, the Messenger v2 connection modes that it accepts
from clients. A `ceph.MessengerConnectionMode` is `ceph.MessengerSecure`
(`secure`, AES-GCM) or `ceph.MessengerCRC` (`crc`). A nil field keeps Ceph's
default, which accepts both. The order of a daemon's list does not matter: the
connection uses the first mode in the client's order of preference that the
daemon accepts, so a daemon cannot force `secure` on a client that prefers
`crc` while it accepts both.

| Field | Native option |
| --- | --- |
| `Monitors` | `ms_mon_service_mode` in the `mon` section |
| `Managers` | `ms_mon_service_mode` in the `mgr` section. Ceph treats MGRs like MONs here, so `ms_service_mode` has no effect on them |
| `OSDs` | `ms_service_mode` in the `osd` section |

- `Run` stores the policy in the central configuration right after MON `a`
  starts, before any other daemon. Later daemons start with it, and it survives
  daemon restarts.
- Connections between daemons keep their defaults, so the cluster itself and the
  fixture's own CLI keep working under every policy.
- [override.SetMessengerPolicy](../../override/README.md#setmessengerpolicy) changes the policy on a
  running cluster. The daemons apply it to new connections without a restart.
- It cannot be combined with `MessengerV2Secure`, whose local configuration
  fixes every mode.

A rejected client sees one of two behaviours, depending on the daemon:

| Daemon | A client that offers no accepted mode |
| --- | --- |
| MON | fails at once: the connect returns `EACCES`, and the `ceph` CLI prints `RADOS permission denied` and exits with 13 |
| MGR, OSD | gets no error; the client keeps retrying the connection until its own timeout |

Bound calls that a MGR or an OSD may reject, for example with the context of
your client or with `timeout` around the CLI.

The MON client also logs a `handle_auth_bad_method` line, but do not assert
on it in the CLI's output. Ceph writes its log from a separate thread, and
after a failed connect the `ceph` CLI exits without shutting librados down,
so the line is sometimes missing. A librados client of your own writes it
reliably to its log when you call `shutdown` after the failed connect.

### ClientContainerOptions

The policy of a native client is set separately, in that client's own
configuration, with `ceph.ClientContainerOptions.WithMessengerPolicy`.
`ClientContainerOptions` is the configuration value of [WithClient](ceph-operations.md#withclient) and
[ClientConfig.WithIdentity](ceph-operations.md#clientconfigwithidentity); a field set by two values is refused. So a mode rejection test can give
the native reference client and the client under test different policies:

```golang
crcOnly := ceph.ClientContainerOptions{}.WithMessengerPolicy(ceph.ClientMessengerPolicy{
	MonitorsAndManagers: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
facts, err := ceph.FactsOf(cluster)
native, err := testcontainers.Run(ctx, facts.Images.Client,
	cluster.WithClient(crcOnly), ceph.WithIdleEntrypoint())
```

| Field | Native option | Used for connections to |
| --- | --- | --- |
| `MonitorsAndManagers` | `ms_mon_client_mode` | MONs and MGRs |
| `OtherDaemons` | `ms_client_mode` | other daemons, such as OSDs |

The option writes the fields into the `[client]` section of that container's
`ceph.conf`. A local file takes precedence over the central configuration, and
`[client]` over `[global]`, so the client policy also applies on a
`MessengerV2Secure` cluster. The cluster's own control CLI and other containers
are unaffected. A process that reads [ConnectionConfig](ceph-operations.md#connectionconfig)
instead sets the same two options itself, for example with go-ceph
`conn.SetConfigOption("ms_mon_client_mode", "crc")`. A policy with an empty
list or an unknown mode makes the container request fail with
`ceph.ErrInvalidRequest`.

### WithHostNetwork

```golang
ceph.WithHostNetwork()
```

Places every cluster daemon and every `WithClient`/`ClientConfig.WithIdentity` container
in the Docker daemon's host network. Ceph then advertises host addresses, so a
native client process on the Docker host (for example a Go test using go-ceph on
Linux) can reach MONs and OSDs directly.

- MON (and RGW) ports are selected automatically; MGR, OSD and MDS ports use
  Ceph's dynamic port selection. MON port reservation is retried up to five times
  when another process takes a port.
- The advertised address defaults to `127.0.0.1`; use
  [NetworkConfig.WithHostAddress](#networkconfigwithhostaddress) for a remote Docker host.
- `Run` checks that the engine runs Linux containers and that the test process
  reaches the first MON's advertised v2 endpoint (retried for up to 5 seconds)
  before creating MGRs and OSDs. Otherwise it returns an error wrapping
  [ErrHostNetworkUnavailable](ceph-operations.md#errhostnetworkunavailable). Later MON, MGR, OSD,
  MDS and RGW endpoints are not probed.
- Docker Desktop needs host networking enabled in its settings (version 4.34 or
  later), and its network behaviour differs from a Linux engine. A test process
  that itself runs in a bridge-network container cannot use host mode.
- Cannot be combined with [NetworkConfig.WithSeparateClusterNetwork](#networkconfigwithseparateclusternetwork),
  and Docker endpoint interruption ([fault.InterruptNetwork](../../fault/README.md#interruptnetwork)) and
  packet delay ([fault.ApplyNetworkProfile](../../fault/README.md#applynetworkprofile)) are not
  available.

`ClusterFacts.UsesHostNetwork` and `ClusterFacts.PublicAddress` ([FactsOf](ceph-operations.md#factsof))
report the mode and address. See [Host networking](host-network.md) for native
clients, Docker Desktop, remote engines and port behaviour.

### NetworkConfig.WithHostAddress

```golang
ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithHostAddress("192.0.2.10"))
```

Selects the IPv4 address that daemons bind and advertise in host mode, typically
the Docker host's address when Docker is remote. The address must be assigned on
the Docker host and reachable from the test process; `Run` verifies both and
fails with `ErrHostNetworkUnavailable` otherwise. Requires
[WithHostNetwork](#withhostnetwork).

### NetworkConfig.WithSeparateClusterNetwork

```golang
ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork())
```

Creates a second owned bridge for OSD replication, recovery and heartbeat
traffic. Docker selects both IPv4 subnets, and Ceph's `public network` and
`cluster network` settings make OSDs use them. Only OSDs join both bridges;
MON, MGR, MDS, RGW and `WithClient` containers use only the public bridge.
`ClusterFacts.NetworkName` and `ClusterFacts.ClusterNetworkName` ([FactsOf](ceph-operations.md#factsof))
return the two bridges, and [fault.InterruptNetwork](../../fault/README.md#interruptnetwork) with
`ceph.ClusterNetworkPlane` disconnects one OSD's replication endpoint only.
Requires bridge mode.

With `NetworkConfig.WithAddressFamily(ceph.AddressIPv6)`, both bridges are dual-stack and
Ceph's `public network` and `cluster network` are their IPv6 subnets. The
MONs, MGRs and OSDs then advertise only IPv6 addresses: MONs, MGRs and the
public and heartbeat front addresses of OSDs from the public subnet, and the
cluster and heartbeat back addresses of OSDs from the cluster subnet.
`InterruptNetwork` with `ceph.ClusterNetworkPlane` disconnects an OSD's whole
cluster endpoint, IPv4 and IPv6, and keeps its public endpoint.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv6).WithSeparateClusterNetwork()))
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.DaemonAddresses(ctx)
// With Docker Desktop's default IPv6 pool, for example:
// addresses.OSDs[0].Public[0].Addr  == "[fd62:93fe:2c9e::4]:6800"
// addresses.OSDs[0].Cluster[0].Addr == "[fd62:93fe:2c9e:1::2]:6800"
```

With `NetworkConfig.WithAddressFamily(ceph.AddressDualStack)`, both bridges are dual-stack
as well, and `public network` and `cluster network` each list both subnets of
their bridge. Every daemon then advertises one address of each family from
the matching bridge.

### NetworkConfig.WithAddressFamily

```golang
ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv6))
```

Selects the addresses that the Messenger of every MON, MGR, OSD and MDS binds
and advertises. [FactsOf](ceph-operations.md#factsof) returns the selected value as `ClusterFacts.AddressFamily`.

| Family | Bridge | Messenger listeners and advertised addresses |
| --- | --- | --- |
| `ceph.AddressIPv4` (default) | IPv4 | the container's IPv4 address, such as `172.19.0.2:3300` |
| `ceph.AddressIPv6` | dual-stack: one IPv4 and one IPv6 subnet | the container's IPv6 address, such as `[fd62:93fe:2c9e::2]:3300`; no IPv4 listener |
| `ceph.AddressIPv4MappedIPv6` | IPv4 | the container's IPv4 address as an IPv4-mapped IPv6 address on an IPv6 socket, such as `[::ffff:172.19.0.2]:3300` |
| `ceph.AddressDualStack` | dual-stack: one IPv4 and one IPv6 subnet | both the container's IPv4 and IPv6 address, such as `[v2:[fd62:93fe:2c9e::2]:3300,v2:172.19.0.2:3300]`; clients of either family connect |

The MONs get these addresses in the monmap and bind them. For `AddressIPv6`
and `AddressIPv4MappedIPv6`, the generated `ceph.conf` sets
`ms_bind_ipv4 = false` and `ms_bind_ipv6 = true`, so MGRs,
OSDs and MDSs bind the IPv6 wildcard address, `[::]`, and advertise the address
from which the MONs see them. No daemon has an IPv4 Messenger socket; with
`AddressIPv4MappedIPv6` the IPv6 sockets accept the IPv4 connections that the
mapped addresses stand for. The address
vectors from [DaemonAddresses](ceph-operations.md#daemonaddresses) and `ceph ... dump` show the
selected family for every daemon, including the heartbeat addresses of OSDs.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv4MappedIPv6)))
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
addresses, err := observer.DaemonAddresses(ctx)
// addresses.Monitors[0].Addrs[0].Addr == "[::ffff:172.19.0.2]:3300"
```

- Bridge mode only. `AddressIPv6` and `AddressDualStack` work with
  [NetworkConfig.WithSeparateClusterNetwork](#networkconfigwithseparateclusternetwork), which then makes
  both bridges dual-stack; `AddressIPv4MappedIPv6` cannot be combined with it.
- `AddressIPv6` and `AddressDualStack` need a Docker Engine that allocates an IPv6 subnet for a new
  IPv6 bridge without an explicit subnet, from its default pool of unique local
  addresses. `Run` fails with the Engine's error otherwise.
- With `AddressIPv6`, Docker forwards an IPv4 connection to a published MON
  port to the container's IPv4 address, where no MON listens, so an IPv4
  connection to `MappedPort` does not reach a MON. Run clients in containers on
  the cluster network with [WithClient](ceph-operations.md#withclient). With
  `AddressIPv4MappedIPv6`, the IPv6 sockets also accept the forwarded IPv4
  connections.
- Ceph prints an IPv4-mapped address in dotted form, `::ffff:172.19.0.2`, but
  parses only hexadecimal groups, `::ffff:ac13:2`. The fixture writes
  `mon_host` in the parseable form; convert an address the same way before you
  pass it to a Ceph tool, for example with `-m`. Go's `net/netip` parses both.
- HTTP endpoints, such as RGW's, are not Messenger listeners and keep their
  addresses.

#### Dual-stack clusters

`AddressDualStack` makes every Messenger bind and advertise both families.
The generated `ceph.conf` sets `ms_bind_ipv4`, `ms_bind_ipv6` and
`ms_bind_prefer_ipv4` to `true` and `public network` to both subnets of the
bridge, such as `172.19.0.0/16,fd62:93fe:2c9e::/64`. Every address vector
then has one entry of each family for each enabled Messenger protocol: v2,
and v1 unless [NetworkConfig.WithMessengerMode](#networkconfigwithmessengermode) removes it.

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

`Run`, [AddManager](ceph-operations.md#addmanager) and [ManagerContainer.Remove](ceph-operations.md#managercontainerremove) then read
[DaemonAddresses](ceph-operations.md#daemonaddresses) and check that every MON, the active MGR
and every up OSD advertise exactly one concrete IPv4 and one concrete IPv6
address for each protocol. An unspecified, IPv4-mapped or link-local address,
or a missing family, fails the call with `ErrUnsupported` (see
[Error categories](ceph-operations.md#error-categories)), naming the daemon and its vector. A
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
[WithConfigFile](#withconfigfile) file. `Run` accepts this key only with
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

1. [AddMonitor](ceph-operations.md#addmonitor) `c`, then [MonitorContainer.Remove](ceph-operations.md#monitorcontainerremove) `b`;
2. [AddOSD](ceph-operations.md#addosd) (osd.2), then [OSDContainer.Remove](ceph-operations.md#osdcontainerremove) `0`;
3. the MDS container stopped, its GID failed with `ceph mds fail` and a new
   MDS started with `MDS.AddReplacement` (see [cephfs](../../cephfs/README.md#mdsaddreplacement));
4. [AddManager](ceph-operations.md#addmanager) `b`, then [ManagerContainer.Remove](ceph-operations.md#managercontainerremove) `a`.

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
  [MonitorBootstrapAddresses](ceph-operations.md#monitorbootstrapaddresses) lists mon.c and not
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

##### Cutting one family of a path

[fault.ApplyNetworkProfile](../../fault/README.md#applynetworkprofile) with a `Peer`, a `PeerPort`
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

### WithStartupTimeout

```golang
ceph.WithStartupTimeout(5 * time.Minute)
```

Bounds `Run` as a whole and each later topology, readiness and wait operation
(`AddOSD`, `WaitForClean`, `Pool.WaitForPGCount`, `DeepScrubPG`, ...). The default
is 3 minutes and the value must be positive. Every operation is also bounded by
the caller's context, so a wait never outlives the shorter of the two. Increase
it for slow engines, large pools or long recovery waits.

The generic options of the `testcontainers` package, such as
`testcontainers.WithEnv`, `testcontainers.WithLogger`, `testcontainers.WithFiles`
or any `testcontainers.CustomizeRequestOption`, also apply, but only to the
container of MON `a`. They are not propagated to other monitors, managers, OSDs,
the control container or services. Replacing that container's image, network,
entrypoint, command or internal paths breaks the bootstrap; use
[WithHostNetwork](#withhostnetwork) to change networking.

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
[ServiceContainers](ceph-operations.md#servicecontainers) lists them.

Each service package also has its own `Run`, which takes every `ceph.Run` option
and adds that service's default when its own option is absent: `rbd.Run` creates
an initialized pool `rbd`, `cephfs.Run` the filesystem `tc-cephfs` with one
active MDS, and `rgw.Run` the default gateway with a test user. Their result is
that package's `Container`, which embeds `*ceph.Container`, so every method on
this page is available on it. A service `Run` rejects bootstrap options that
leave its service without the storage or manager it needs (for example
`ceph.ClusterConfig.WithNoInitialOSDs`); `rbd.Run` accepts `ClusterConfig.WithNoInitialManagers`. For staged
bootstraps, start with `ceph.Run` and add the service later.

## Independent test fixtures

Call `Run` for each test, register owned cleanup before checking its error, and
create the resources and restricted identities that the test needs. See the [usage example](../../ceph/README.md#usual-order-of-use) for a complete
fixture.

RADOS namespaces are selected by the client operation; naming one in Cephx caps
does not write an object. Isolation tests must perform real I/O and verify both
allowed access and sibling namespace or identity denial.

A test owns every external client it attaches, so stop those clients before
fixture termination and register resource cleanup in reverse dependency order.
Cleanup needs a fresh bounded background context because `t.Context()` has
ended when Go runs test cleanups. Keep non-nil handles returned with errors:
an uncertain operation may still own a resource, and failed cleanup remains
retryable. See [Contexts and cleanup](contexts-and-cleanup.md) and
[TerminateOnCleanup](ceph-operations.md#terminateoncleanup).


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
- IPv4 is the default. [NetworkConfig.WithAddressFamily](#networkconfigwithaddressfamily) selects IPv6
  or IPv4-mapped IPv6 on a bridge network; the host network is IPv4 only.

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
  [NetworkConfig.WithMessengerMode](#networkconfigwithmessengermode)).
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
  [Run small OSDs](ceph-recipes.md#run-small-osds)).
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


## Role image selection

`ImageSet` selects Essential, Client, CephFS, RBDAux and RGW, or an exclusive
All image. `ImageOptions.WithImageSet` carries the set, and `ceph.WithImageOptions` passes it to `Run`; it applies it
to `Run`, `rbd.Run`, `cephfs.Run` and `rgw.Run`. The image argument is then empty
or equal to the set's essential image and is not a second input; any other value
is refused with `ErrInvalidRequest` before anything is created.
`Run` without an explicit set uses its image as the aggregate.
`ClusterFacts.Images` returns the effective selection, and its `Essential` field
is the essential image. `ImageOptions.WithCephFSImage` and `ImageOptions.WithRGWImage` replace the cephfs and rgw
roles of a set.

`ClientContainer` lazily creates one owned toolkit for a separate
client image; aggregate selections reuse the control executor. Initial
attachment is serialized with MON topology changes and honors cancellation.
The cluster owns partial/successful toolkit teardown. RBD/CephFS operations,
`ObjectWatchers` and `PGInconsistencies` use that toolkit, while daemon-local
sockets and process observations stay on their daemon. ClientConfig and
ClientConfig.WithIdentity keep identity/credential ownership; a separate public
client lifecycle type is unnecessary.

### Copying a capture file explicitly

```golang
func (f *Finalization) CaptureWithFile(name string, session CaptureFileSource, maxBytes int64)
```

This is the raw-packet consent form of `Capture`. Settlement and metadata
handling are identical; only a failure archive opens and verifies the selected
file. See [capture file retention](../../capture/README.md#raw-files-in-a-failure-archive)
for byte/count budgets, identity checks, privacy and retention outcomes.
`CaptureFileSource` embeds `CaptureSettler` and provides
`CaptureFile(context.Context) (io.ReadCloser, int64, string, error)` for a
read-only finalized file with its frozen retrieved byte count and full SHA256.
It must honor context and leave the caller's original file untouched.
