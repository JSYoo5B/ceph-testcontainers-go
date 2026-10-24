# Ceph

## Introduction

The Testcontainers module for Ceph.

It starts a small, disposable Ceph cluster for integration tests. Real
`ceph-mon`, `ceph-mgr` and `ceph-osd` daemons run in separate containers on an
isolated Docker network. The default topology is one monitor (MON), one manager
(MGR) and two object storage daemons (OSDs). Each OSD keeps its data in a sparse
BlueStore file inside its container (1 GiB logical size by default), so the Ceph
daemons need no privileged mode, host disks, LVM, Docker socket or systemd.
Testcontainers itself (and Ryuk) still needs access to the Docker engine.

The module drives Ceph through the command-line tools inside the containers. The
Go module has no go-ceph, librados or cgo dependency and builds with
`CGO_ENABLED=0`. Your application talks to the cluster with its own client:
librados or go-ceph, librbd, libcephfs, the Ceph CLI or an S3 SDK. The module
prepares and changes the server side; it does not wrap object, image, file or S3
data APIs.

The `ceph` package provides:

- cluster topology: MON, MGR and OSD counts, logical CRUSH locations, adding,
  removing and replacing daemons, quorum, and starting without OSDs or managers;
- pools: replicated and erasure-coded pools, CRUSH placement, quotas, PG counts,
  relocation, removal and usage;
- Cephx identities with exact capabilities, and options that attach your client
  containers to the cluster;
- temporary configuration and OSDMap changes that return restoration handles;
- fault injection: stopping, pausing and disconnecting daemons or clients, OSD
  flags, client blocklisting, full ratios and replica read errors;
- observation: health checks, PG reports, pool usage, quorum, manager services
  and bounded diagnostics reports.

A [shared cluster](#shared-cluster) lends one cluster to many tests, each
confined to a RADOS namespace of its own.

RBD, CephFS and RGW (S3) live in their own packages: [rbd](rbd.md),
[cephfs](cephfs.md) and [rgw](rgw.md). Their options plug into the same
`ceph.Run`; see [Service packages](#service-packages).

Requirements:

- Go 1.26 or later.
- A Docker engine that runs Linux containers. Docker Desktop works; host
  networking has extra requirements (see [WithHostNetwork](#withhostnetwork)).
- Access to pull the Ceph image and the Ryuk image.

The module is experimental. There are no release tags yet.

## Adding this module to your project dependencies

Please run the following command to add the Ceph module to your Go dependencies:

```
go get github.com/JSYoo5B/ceph-testcontainers-go/ceph
```

Until the first release tag exists, `go get` records a pseudo-version of the
latest commit on `main`.

## Usage example

The test below starts the default cluster with one pool, attaches a client
container that carries the Ceph CLI, and writes and reads one RADOS object.

```golang
package app_test

import (
	"io"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRADOSRoundTrip(t *testing.T) {
	ctx := t.Context()

	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		ceph.WithPools(ceph.PoolConfig{Name: "app-data", Application: "rados"}),
	)
	// Register cleanup before checking err: a partially started cluster
	// is returned together with the error and still owns containers.
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	// Run does not wait for placement groups; wait before client I/O.
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// The client image must contain the tools or libraries the test uses.
	// WithClient joins the cluster network and copies ceph.conf and the
	// admin keyring into /etc/ceph.
	client, err := testcontainers.Run(ctx, ceph.DefaultImage,
		cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
	)
	// Registered after the cluster, so it is terminated first.
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) string {
		t.Helper()
		code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		output, _ := io.ReadAll(reader)
		if code != 0 {
			t.Fatalf("%v exited %d: %s", args, code, output)
		}
		return string(output)
	}
	run("sh", "-c", "printf hello > /tmp/greeting")
	run("rados", "-p", "app-data", "put", "greeting", "/tmp/greeting")
	if got := run("rados", "-p", "app-data", "get", "greeting", "-"); got != "hello" {
		t.Fatalf("read %q, want hello", got)
	}
}
```

Starting a cluster takes tens of seconds and pulls a large image the first time.
Give `go test` a generous `-timeout` when several clusters run in one package.

## Module Reference

### Run function

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
   [WithSeparateClusterNetwork](#withseparateclusternetwork)), or verify the
   engine in host mode.
3. Start MON `a`, record [CephVersion](#cephversion), and read the generated
   `ceph.conf` and the `client.admin` keyring.
4. Add the remaining monitors (`b`, `c`, ...). With more than one MON, a separate
   CLI control container is started as well (see
   [ControlContainer](#controlcontainer)).
5. Start managers `a`, `b`, ...; the first becomes active, the others standby.
6. Register, format and start each OSD and wait until it is up and in.
7. Wait for an available active MGR. With
   [WithNoInitialManagers](#withnoinitialmanagers), wait for MON quorum and for
   the OSDs to be up and in instead.
8. Create the [WithPools](#withpools) pools, then the service resources passed by
   service package options: RBD pools, CephFS filesystems and RGW gateways.

`Run` does not wait for clean placement groups. Call
[WaitForClean](#waitforclean) before application I/O.

When `Run` fails after it created Docker resources, it returns the partially
built `*Container` together with the error. That container owns every resource
created so far and must be terminated. Calling
`testcontainers.CleanupContainer(t, cluster)` before checking the error does this;
it ignores a nil container.

The returned `*Container` embeds the `testcontainers.Container` of MON `a`, so
generic methods such as `Exec`, `Logs`, `Host` and `MappedPort` address that
container. `Terminate` is overridden and removes the whole cluster (see
[Terminate](#terminate)).

`Run` rejects these option combinations before creating any resource, regardless
of option order:

| Combination | Result |
| --- | --- |
| `WithHostAddress` without `WithHostNetwork` | `WithHostAddress requires WithHostNetwork` |
| `WithHostNetwork` with `WithSeparateClusterNetwork` | `WithSeparateClusterNetwork requires bridge mode` |
| `WithMessengerPolicy` with `MessengerV2Secure` | rejected; `MessengerV2Secure` fixes every connection mode |
| `WithAddressFamily` with any family but `AddressIPv4`, and `WithHostNetwork` | `WithAddressFamily(...) requires bridge mode` |
| `WithAddressFamily(AddressIPv4MappedIPv6)` with `WithSeparateClusterNetwork` | rejected |
| `WithNoInitialOSDs` with `WithOSDCount` or `WithInitialOSDs` | rejected; pick one initial layout |
| `WithNoInitialOSDs` with initial pools, filesystems or gateways | rejected; add OSDs before provisioning them |
| `WithNoInitialManagers` with `WithManagerCount` | rejected |
| `WithNoInitialManagers` with initial filesystems or gateways | rejected; add a manager before provisioning them (initial pools are allowed) |
| Fewer OSDs in the default CRUSH root than the default replica count | `default CRUSH root ... needs N initial OSDs` |
| An initial pool needs more failure domains than the initial layout has | `initial pool ... needs N distinct ... domains` |
| Duplicate pool, filesystem or gateway names, including the pools a filesystem creates | rejected as duplicates |

Some environments can only be checked after resources exist. In host mode, `Run`
fails with [ErrHostNetworkUnavailable](#errhostnetworkunavailable) when the engine
does not run Linux containers, the selected address is not assigned on the
Docker host, or the test process cannot reach the first MON.

#### Image

`Run(ctx, image)` uses a complete aggregate image; the default is
`ceph.DefaultImage`. `RunWithImages` and `WithImages` accept the same
`ImageSet` role model. Essential supplies MON/MGR/OSD; Client, CephFS,
RBDAux and RGW are required only by their selected features. Missing optional
roles never fall back to essential. See [image selection](guides/releases-and-images.md#role-images).

### Container Options

When starting the Ceph container, you can pass options in a variadic way to configure it.

#### WithOSDCount

```golang
ceph.WithOSDCount(3)
```

Sets the number of initial OSD containers. The default is 2 and the minimum is 1;
use [WithNoInitialOSDs](#withnoinitialosds) to start without storage. Each OSD
gets the default CRUSH location: a logical host `osd-<ID>` under the default
root. A later `WithOSDCount` replaces an earlier
[WithInitialOSDs](#withinitialosds) layout.

Without [WithPoolDefaults](#withpooldefaults), the cluster's default pool size is
`min(2, OSD count)` with `min_size` 1, so a one-OSD cluster gets replica-1 pools
that can become clean.

#### WithInitialOSDs

```golang
ceph.WithInitialOSDs(
	ceph.OSDConfig{Host: "host-a", Rack: "rack-a", DeviceClass: "fast"},
	ceph.OSDConfig{Host: "host-b", Rack: "rack-b", DeviceClass: "fast"},
	ceph.OSDConfig{Host: "host-c", Rack: "rack-b", DeviceClass: "slow"},
)
```

Selects the number of initial OSDs and the logical CRUSH location of each. Each
`ceph.OSDConfig` describes one OSD:

| Field | Default | Meaning |
| --- | --- | --- |
| `Host` | `osd-<ID>` | CRUSH host bucket. Several OSDs may share a host. |
| `Rack` | none | Optional CRUSH rack bucket. Several hosts may share a rack. |
| `Root` | the cluster's default root | CRUSH root bucket (see [WithDefaultCRUSHRoot](#withdefaultcrushroot)). |
| `DeviceClass` | none | Device class used by `PoolConfig.DeviceClass` and `PoolPlacement.DeviceClass`. |

An empty list is rejected, and one host cannot appear under different racks or
roots. A later [WithOSDCount](#withosdcount) replaces this layout. Hosts and
racks are simulated placement domains on one Docker engine; they let you test
CRUSH behaviour, not physical failure tolerance. Pools that need `N` distinct
domains are validated against this layout before anything is created.

#### How Run starts the initial OSDs

`Run` registers the initial OSDs one after another in the order of the layout,
so OSD IDs, keys and CRUSH host buckets are allocated in that order on every
run: the first OSD gets the lowest free ID, and each host bucket is created
before any OSD boots. It then creates and starts up to four OSD containers at
a time and waits until all OSDs are up and in. The first manager boots while
the OSDs are registered and started; further managers from
[WithManagerCount](#withmanagercount) are added after it is registered, so it
stays the active one.

If an OSD fails to start, `Run` starts no further OSD and returns an error that
names it. Every registered OSD, and every container that was created, stays
owned by the returned cluster, and `Terminate` removes them. On
`quay.io/ceph/ceph:v20.2.4`, starting the OSDs together cuts `Run` from about 17
to 11 seconds for three OSDs and from about 27 to 16 seconds for five.
[AddOSD](#addosd) and [AddOSDWithConfig](#addosdwithconfig) still add one OSD
at a time.

#### WithNoInitialOSDs

```golang
ceph.WithNoInitialOSDs()
```

Starts only the MON and MGR topology. Add storage later with
[AddOSD](#addosd) or [AddOSDWithConfig](#addosdwithconfig), then create pools.

- Initial pools, filesystems and gateways, and an explicit `WithOSDCount` or
  `WithInitialOSDs`, are rejected before allocation.
- Prospective pool defaults stay at size 2 and `min_size` 1. Use
  `WithPoolDefaults(1, 1)` if you will add only one OSD; adding OSDs does not
  change existing pool sizes.
- Run still waits for an available MGR. No storage readiness is implied: Ceph's
  own `.mgr` pool can have pending PGs, and health is usually `HEALTH_WARN`
  (for example `TOO_FEW_OSDS`) until OSDs join.
- With [WithOSDInMemoryStorage](#withosdinmemorystorage), the tmpfs volume is
  created on the first `AddOSD`.
- [WithOSDVolumeStorage](#withosdvolumestorage) also defers its ordinary
  named volume until the first `AddOSD`.
- Removing the last OSD is still refused later.

Use it for MON-only or MGR-only tests, bootstrap-ordering tests, and to apply OSD
configuration (for example a scheduler or benchmark setting) before any OSD
starts. Combined with [WithNoInitialManagers](#withnoinitialmanagers) it gives a
MON-only phase. See [Start without OSDs, then add storage](#start-without-osds-then-add-storage).

#### WithMonitorCount

```golang
ceph.WithMonitorCount(3)
```

Sets the initial number of monitors (default 1, minimum 1). Monitors are named
`a`, `b`, `c`, ... Three monitors allow quorum failover tests; even counts are
allowed for deliberate quorum-loss scenarios. With more than one monitor the
cluster also starts an independent CLI control container, so fixture commands
keep working while MON `a` is stopped.

#### WithManagerCount

```golang
ceph.WithManagerCount(2)
```

Sets the initial number of managers (default 1, minimum 1). Managers are named
`a`, `b`, ...; one becomes active and the rest register as standbys.
`WithManagerCount(0)` is invalid; use
[WithNoInitialManagers](#withnoinitialmanagers) instead.

#### WithNoInitialManagers

```golang
ceph.WithNoInitialManagers()
```

Starts the MON and OSD topology without creating a manager. Add the first manager
with [AddManager](#addmanager).

- `Run` waits for MON quorum and for the initial OSDs to be up and in. It does not
  wait for MGR availability, modules, PG statistics or health.
- Initial pools are allowed and created; initial filesystems and gateways and an
  explicit `WithManagerCount` are rejected.
- [WaitForClean](#waitforclean) and [WaitForPGClean](#waitforpgclean) need an
  available manager, so they cannot succeed until you add one.
- Ceph may raise `MGR_DOWN` (only after the new monitor's `mon_mgr_mkfs_grace`,
  2 minutes by default), and it can escalate to `HEALTH_ERR` while client data I/O
  still works.
- The control image must still contain `ceph-mgr` and its dependencies.

To simulate a manager outage in an already configured cluster, stop the existing
manager container instead (see [Managers](#managers)).

#### WithPools

```golang
ceph.WithOSDCount(3), // the EC pool needs K+M = 3 distinct OSDs
ceph.WithPools(
	ceph.PoolConfig{Name: "app-data", Application: "rados"},
	ceph.PoolConfig{Name: "app-ec", Application: "rados",
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
),
```

Creates pools after the initial OSDs have started, with the same validation and
result as [CreatePool](#createpool). Repeated `WithPools` options replace the list;
a final `WithPools()` clears it. Defaults from other options are resolved after
all options have been applied. `Run` does not wait for the pools to become clean.

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
| `NoHashPSPool` | false | Clears the pool's `hashpspool` flag right after creation. The placement seed of each PG is then the legacy sum of PG number and pool ID instead of a hash of the two, as for pools that predate the flag. Other pools, such as the MGR's `.mgr`, keep the flag unless `osd_pool_default_flag_hashpspool` is off (see [Reproduce a minimal legacy placement setup](#reproduce-a-minimal-legacy-placement-setup)). |

`ceph.ErasureCodeConfig` selects `K` data and `M` parity shards (`K >= 2`,
`M >= 1`, `K+M <= 128`) with the portable Jerasure `reed_sol_van` plugin. An
explicit `MinSize` of `K` permits I/O without any spare shard during a failure.
`AllowOverwrites` enables partial object writes; it is required (and enforced)
for EC pools with application `rbd` or `cephfs`. RBD and CephFS metadata must
stay in a replicated pool; use the EC pool as the image's data pool or as an
additional CephFS data pool.

For RBD pools that need `rbd pool init`, use `rbd.WithPools` instead (see
[Service packages](#service-packages)).

#### WithPoolDefaults

```golang
ceph.WithPoolDefaults(3, 2)
```

Sets `osd_pool_default_size` and `osd_pool_default_min_size` for pools that Ceph
or services create and for `PoolConfig` values that omit `Replicas`/`MinSize`.
Requires `1 <= minSize <= replicas <= 10`. Without this option the default is
`size = min(2, initial OSD count)` and `min_size = 1` (2/1 with
`WithNoInitialOSDs`). Set the defaults here rather than in a config file; the
config file rejects these keys.

#### WithDefaultCRUSHRoot

```golang
ceph.WithDefaultCRUSHRoot("tc-storage")
```

Selects the CRUSH root used for pools that Ceph creates itself (such as `.mgr`)
and for omitted `PoolConfig.CRUSHRoot` and `OSDConfig.Root` fields. Without this
option the root is `default`. The initial layout must contain enough OSDs in this
root for the default replica count; this check is skipped with
`WithNoInitialOSDs`.

#### WithOSDBlockSize

```golang
ceph.WithOSDBlockSize(256 << 20)
```

Sets the logical size in bytes of each OSD's sparse BlueStore file, or the
capacity of each MemStore OSD (see [WithOSDObjectStore](#withosdobjectstore)).
The default is 1 GiB and the minimum accepted value is 64 MiB. These are disposable files,
not real disks; the space is allocated as data is written.

Caveat: with Ceph's default mClock scheduler, a fresh OSD runs a startup capacity
benchmark that can write about 100 objects of 4 MiB (400 MiB) before deleting
them. That alone fills a 64 to 256 MiB file, BlueStore aborts the OSD, and `Run`
times out waiting for it. For small OSDs, start with
[WithNoInitialOSDs](#withnoinitialosds), set `osd_mclock_skip_benchmark` with
[OverrideConfig](#overrideconfig), then add OSDs. The option itself never
changes the scheduler or the benchmark. See
[Run small OSDs](#run-small-osds).

#### WithOSDInMemoryStorage

```golang
ceph.WithOSDInMemoryStorage(2 << 30)
```

Keeps every OSD's BlueStore file in one Docker-managed tmpfs volume instead of
the OSD containers' writable layers. `maxBytes` must be positive and is the
allocation ceiling for the whole cluster's OSD volume, independent of each file's
logical [WithOSDBlockSize](#withosdblocksize). It can be smaller than the sum of
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
  the control image, without network) before the first OSD. The keeper holds the
  volume so OSD `Stop`/`Start` keeps data. Purging an OSD with
  [RemoveOSD](#removeosd) discards its directory.
- `Terminate` removes the OSDs, the keeper and the volume. Losing the keeper or
  restarting the Docker daemon or VM loses data; the module refuses to continue
  with a restarted keeper.
- Requires a Linux Docker engine that can mount local-driver tmpfs volumes
  (compatible Docker Desktop engines work). Rootless or managed engines may
  refuse it. No host path, privileged container or extra image is needed.

Without either storage-volume option, the default remains a container-local
sparse file per OSD.

#### WithOSDVolumeStorage

```golang
ceph.WithOSDVolumeStorage()
```

Keeps classic BlueStore OSD directories in one ordinary Docker local named
volume owned by the cluster. Each OSD still gets its own sparse file with
[WithOSDBlockSize](#withosdblocksize) as its logical capacity. Unlike the tmpfs
option, bytes do not depend on a live keeper. This option cannot be combined
with MemStore or `WithOSDInMemoryStorage`; invalid combinations fail before
resource allocation.

The first OSD allocates a unique volume and an isolated idle helper from the
control image. The helper has no network or cluster credentials and manages
fresh and purged directories. A stopped or restarted helper refuses further
storage operations before new OSD registration; it is not automatically replaced.
Existing OSDs can still stop/start with their original containers.

`RemoveOSD` drains and purges the native registration, removes its container,
then discards only its directory. `Terminate` removes all owned OSDs and the
helper before deleting the volume without force. A failed or busy deletion
retains ownership for another `Terminate` call. Changed volume metadata is
refused before directory operations or deletion. Never externally modify or
attach this owned volume while using the cluster.

There is no caller-supplied volume, host path, device or driver option.
[DetachOSD](#detachosd) and [ReopenOSD](#reopenosd) restart a retained store in
a new container; this backend gives no host-reboot durability. `Terminate`
always discards its owned volume, including detached stores. See
[the storage design](design/osd-volume-storage.md) and
[retained reopen](design/osd-retained-reopen.md).

#### WithOSDObjectStore

```golang
ceph.WithOSDObjectStore(ceph.OSDMemStore)
```

Selects the object store of every OSD, including OSDs added later. The
argument is a `ceph.OSDObjectStore`:

| Value | Store |
| --- | --- |
| `ceph.OSDBlueStore` | Default. A sparse BlueStore file per OSD. |
| `ceph.OSDMemStore` | Ceph's in-memory test store, `memstore`. Data lives in the OSD process's memory; [WithOSDBlockSize](#withosdblocksize) sets its capacity (`memstore_device_bytes`). |

- MemStore saves its contents to the OSD's data directory when the store is
  unmounted, so the fixture disables Ceph's fast OSD shutdown for MemStore
  OSDs. A stopped OSD keeps its data across `Stop` and `Start`; an OSD that is
  killed, or whose container is removed, loses it.
- MemStore cannot be combined with
  [WithOSDInMemoryStorage](#withosdinmemorystorage) or
  [WithOSDVolumeStorage](#withosdvolumestorage), which use BlueStore files, and [InjectObjectDataError](#injectobjectdataerror) needs
  BlueStore.
- `ceph osd metadata` reports `osd_objectstore` as `memstore`.

#### WithConfigFile

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
  [ConnectionConfig](#connectionconfig) get it, so bootstrap-only settings work.
  It does not change a running cluster; use [OverrideConfig](#overrideconfig)
  for that.
- An entry replaces the fixture's default for the same section and key. Spaces,
  `-` and `_` in key names are equivalent, as in Ceph. Several files are merged in
  order, later files overriding earlier ones key by key, so a shared base file
  plus a per-test file works.
- Ceph gives local files precedence over the MON configuration database. A key set
  in this file cannot be changed later by `OverrideConfig` or `ceph config set`;
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
| `fsid`, `mon_host`, `mon_initial_members`, `mon_addr` | generated; `WithMonitorCount` |
| `public_network`, `cluster_network`, `public_addr`, `cluster_addr`, `*_bind_addr` | `WithHostNetwork`, `WithSeparateClusterNetwork`, `WithHostAddress` |
| `auth_*`, `keyring`, `key`, `keyfile` | `WithAuth`; fixture Cephx setup |
| `ms_*_mode`, `ms_mon_*_mode`, `ms_bind_*` except `ms_bind_prefer_ipv4` | `WithMessengerMode`, `WithMessengerPolicy`, `WithClientMessengerPolicy`, `WithAddressFamily`, `WithHostNetwork` |
| `osd_pool_default_size`, `osd_pool_default_min_size` | `WithPoolDefaults` |
| `osd_pool_default_crush_rule` | `WithDefaultCRUSHRoot` |
| `bluestore_block_size` | `WithOSDBlockSize` |
| `osd_objectstore`, `bluestore_block_path`, `bluestore_block_create` | fixed OSD storage layout |
| `log_file`, `log_to_file`, `log_to_stderr`, `err_to_stderr`, `mon_cluster_log_to_*` | daemon logs always go to container output |
| `admin_socket`, `run_dir`, `mon_data`, `osd_data`, `mgr_data`, `mds_data` | fixed paths |
| `crash_dir` | fixed at `/var/lib/ceph/crash` (`ceph.CrashDir`), where [CollectCrashMetadata](#collectcrashmetadata) reads |
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

#### WithMessengerMode

```golang
ceph.WithMessengerMode(ceph.MessengerV2Secure)
```

Selects, at bootstrap, the Messenger protocols that every daemon listens on and
advertises, and whether the connection modes stay configurable. Both are
immutable for the lifetime of the cluster.

| Mode | Listeners and addresses | Connection modes |
| --- | --- | --- |
| `ceph.MessengerDefault` | v1 and v2 | Ceph's defaults, changeable with [WithMessengerPolicy](#withmessengerpolicy), [OverrideMessengerPolicy](#overridemessengerpolicy) and [WithClientMessengerPolicy](#withclientmessengerpolicy). Not every connection is encrypted. |
| `ceph.MessengerV2Only` | v2 only | As with `MessengerDefault`: CRC, secure or a per-daemon-type mix, changeable later. |
| `ceph.MessengerV2Secure` | v2 only | Fixed to encrypted Messenger v2 (`secure`, AES-GCM) for every connection; no CRC mode. |

With `MessengerV2Only`, the global `ceph.conf` sets `ms_bind_msgr1` to `false`
and `ms_bind_msgr2` to `true` and nothing else, so a CRC-only cluster is:

```golang
crc := []ceph.MessengerConnectionMode{ceph.MessengerCRC}
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithMessengerMode(ceph.MessengerV2Only),
	ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: crc, Managers: crc, OSDs: crc}),
)
// Native clients attached with WithClient(ceph.WithClientMessengerPolicy(
// ceph.ClientMessengerPolicy{MonitorsAndManagers: crc, OtherDaemons: crc}))
// connect with CRC; a client that offers only secure is refused.
```

Connections between daemons keep Ceph's defaults in both configurable modes, so
every policy leaves the cluster working.

To move a cluster that a `ceph.conf` with `ms_bind_msgr1 = false` used to
describe to these options:

| `ceph.conf` | Options |
| --- | --- |
| All six `ms_*_mode` and `ms_mon_*_mode` options `secure` | `WithMessengerMode(ceph.MessengerV2Secure)` |
| `ms_mon_service_mode` of the MONs or MGRs, `ms_service_mode` of the OSDs | `WithMessengerMode(ceph.MessengerV2Only)` and the matching fields of `WithMessengerPolicy` |
| `ms_mon_client_mode` and `ms_client_mode` of a client | `WithClientMessengerPolicy` on that client's container |
| `ms_cluster_mode` and `ms_mon_cluster_mode` between daemons | Ceph's defaults: the policy options leave them alone; with `MessengerV2Only`, [OverrideConfig](#overrideconfig) can change them |
| `mon_host` with only v2 addresses, port 3300 | [MonitorBootstrapAddresses](#monitorbootstrapaddresses), or `MappedPort(ctx, "3300/tcp")` in bridge mode; there is no 6789 to map |

With `MessengerV2Secure`, the global `ceph.conf` sets `ms_cluster_mode`,
`ms_service_mode`, `ms_client_mode`, `ms_mon_cluster_mode`,
`ms_mon_service_mode` and `ms_mon_client_mode` to `secure`, `ms_bind_msgr1` to
`false` and `ms_bind_msgr2` to `true`.

In both v2 modes:

- The monmap and `mon_host` contain only v2 addresses, and no daemon listens on
  a v1 port. In bridge mode the MON publishes only `3300/tcp`, so
  `MappedPort(ctx, "6789/tcp")` fails; in host mode only one MON port is
  reserved.
- Later daemons, `WithClient` and `WithClientIdentity` containers and
  [ConnectionConfig](#connectionconfig) inherit the policy. MON refreshes only
  rewrite `mon_host` and keep it.
- [OverrideConfig](#overrideconfig) refuses `ms_bind_msgr1` and
  `ms_bind_msgr2` in every section and mask with `ErrInvalidRequest`, because
  the local file would override them anyway. With `MessengerV2Secure` it also
  refuses the six mode options.

With `MessengerV2Secure`:

- A client configured for CRC only is rejected by the cluster (`AUTH_BAD_METHOD`)
  rather than timing out.
- Cephx authentication and Messenger encryption are separate settings; Cephx is
  always required.
- This option does not enable HTTPS for RGW (see [rgw](rgw.md)).

[MessengerMode](#messengermode) reports the selected mode. It does not inspect
live connections. To verify encryption on Tentacle, query a daemon's
`messenger dump` admin command (through `ceph tell`), which reports the mode and
cipher of each READY connection. Squid has no `messenger dump`; check the
daemons' `ms_*_mode` settings instead.

#### WithAuth

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
| `KeyType` | the type of the keys the fixture generates itself: `mon.`, `client.admin` and OSD keys, and the default for [CreateClient](#createclient) |
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
  [ConnectionConfig](#connectionconfig) gets them, and
  [OverrideConfig](#overrideconfig) cannot change them later. MONs rotate
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

#### WithMessengerPolicy

```golang
ceph.WithMessengerPolicy(ceph.MessengerPolicy{
	Monitors: []ceph.MessengerConnectionMode{ceph.MessengerSecure},
	Managers: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
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
- [OverrideMessengerPolicy](#overridemessengerpolicy) changes the policy on a
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

#### WithClientMessengerPolicy

The policy of a native client is set separately, in that client's own
configuration, with `ceph.WithClientMessengerPolicy`. It is a
`ceph.ClientContainerOption` of [WithClient](#withclient) and
[WithClientIdentity](#withclientidentity), so a mode rejection test can give
the native reference client and the client under test different policies:

```golang
crcOnly := ceph.WithClientMessengerPolicy(ceph.ClientMessengerPolicy{
	MonitorsAndManagers: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
native, err := testcontainers.Run(ctx, cluster.ControlImage(),
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
are unaffected. A process that reads [ConnectionConfig](#connectionconfig)
instead sets the same two options itself, for example with go-ceph
`conn.SetConfigOption("ms_mon_client_mode", "crc")`. A policy with an empty
list or an unknown mode makes the container request fail with
`ceph.ErrInvalidRequest`.

#### WithHostNetwork

```golang
ceph.WithHostNetwork()
```

Places every cluster daemon and every `WithClient`/`WithClientIdentity` container
in the Docker daemon's host network. Ceph then advertises host addresses, so a
native client process on the Docker host (for example a Go test using go-ceph on
Linux) can reach MONs and OSDs directly.

- MON (and RGW) ports are selected automatically; MGR, OSD and MDS ports use
  Ceph's dynamic port selection. MON port reservation is retried up to five times
  when another process takes a port.
- The advertised address defaults to `127.0.0.1`; use
  [WithHostAddress](#withhostaddress) for a remote Docker host.
- `Run` checks that the engine runs Linux containers and that the test process
  reaches the first MON's advertised v2 endpoint (retried for up to 5 seconds)
  before creating MGRs and OSDs. Otherwise it returns an error wrapping
  [ErrHostNetworkUnavailable](#errhostnetworkunavailable). Later MON, MGR, OSD,
  MDS and RGW endpoints are not probed.
- Docker Desktop needs host networking enabled in its settings (version 4.34 or
  later), and its network behaviour differs from a Linux engine. A test process
  that itself runs in a bridge-network container cannot use host mode.
- Cannot be combined with [WithSeparateClusterNetwork](#withseparateclusternetwork),
  and Docker endpoint interruption ([fault.InterruptNetwork](fault.md#interruptnetwork)) and
  packet delay ([fault.ApplyNetworkProfile](fault.md#applynetworkprofile)) are not
  available.

[UsesHostNetwork](#useshostnetwork) and [PublicAddress](#publicaddress) report the
mode and address. See [Host networking](guides/host-network.md) for native
clients, Docker Desktop, remote engines and port behaviour.

#### WithHostAddress

```golang
ceph.WithHostAddress("192.0.2.10")
```

Selects the IPv4 address that daemons bind and advertise in host mode, typically
the Docker host's address when Docker is remote. The address must be assigned on
the Docker host and reachable from the test process; `Run` verifies both and
fails with `ErrHostNetworkUnavailable` otherwise. Requires
[WithHostNetwork](#withhostnetwork).

#### WithSeparateClusterNetwork

```golang
ceph.WithSeparateClusterNetwork()
```

Creates a second owned bridge for OSD replication, recovery and heartbeat
traffic. Docker selects both IPv4 subnets, and Ceph's `public network` and
`cluster network` settings make OSDs use them. Only OSDs join both bridges;
MON, MGR, MDS, RGW and `WithClient` containers use only the public bridge.
[NetworkName](#networkname) and [ClusterNetworkName](#clusternetworkname) return
the two bridges, and [fault.InterruptNetwork](fault.md#interruptnetwork) with
`ceph.ClusterNetworkPlane` disconnects one OSD's replication endpoint only.
Requires bridge mode.

With `WithAddressFamily(ceph.AddressIPv6)`, both bridges are dual-stack and
Ceph's `public network` and `cluster network` are their IPv6 subnets. The
MONs, MGRs and OSDs then advertise only IPv6 addresses: MONs, MGRs and the
public and heartbeat front addresses of OSDs from the public subnet, and the
cluster and heartbeat back addresses of OSDs from the cluster subnet.
`InterruptNetwork` with `ceph.ClusterNetworkPlane` disconnects an OSD's whole
cluster endpoint, IPv4 and IPv6, and keeps its public endpoint.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithAddressFamily(ceph.AddressIPv6), ceph.WithSeparateClusterNetwork())
addresses, err := cluster.DaemonAddresses(ctx)
// With Docker Desktop's default IPv6 pool, for example:
// addresses.OSDs[0].Public[0].Addr  == "[fd62:93fe:2c9e::4]:6800"
// addresses.OSDs[0].Cluster[0].Addr == "[fd62:93fe:2c9e:1::2]:6800"
```

With `WithAddressFamily(ceph.AddressDualStack)`, both bridges are dual-stack
as well, and `public network` and `cluster network` each list both subnets of
their bridge. Every daemon then advertises one address of each family from
the matching bridge.

#### WithAddressFamily

```golang
ceph.WithAddressFamily(ceph.AddressIPv6)
```

Selects the addresses that the Messenger of every MON, MGR, OSD and MDS binds
and advertises. `cluster.AddressFamily()` returns the selected value.

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
vectors from [DaemonAddresses](#daemonaddresses) and `ceph ... dump` show the
selected family for every daemon, including the heartbeat addresses of OSDs.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithAddressFamily(ceph.AddressIPv4MappedIPv6))
addresses, err := cluster.DaemonAddresses(ctx)
// addresses.Monitors[0].Addrs[0].Addr == "[::ffff:172.19.0.2]:3300"
```

- Bridge mode only. `AddressIPv6` and `AddressDualStack` work with
  [WithSeparateClusterNetwork](#withseparateclusternetwork), which then makes
  both bridges dual-stack; `AddressIPv4MappedIPv6` cannot be combined with it.
- `AddressIPv6` and `AddressDualStack` need a Docker Engine that allocates an IPv6 subnet for a new
  IPv6 bridge without an explicit subnet, from its default pool of unique local
  addresses. `Run` fails with the Engine's error otherwise.
- With `AddressIPv6`, Docker forwards an IPv4 connection to a published MON
  port to the container's IPv4 address, where no MON listens, so an IPv4
  connection to `MappedPort` does not reach a MON. Run clients in containers on
  the cluster network with [WithClient](#withclient). With
  `AddressIPv4MappedIPv6`, the IPv6 sockets also accept the forwarded IPv4
  connections.
- Ceph prints an IPv4-mapped address in dotted form, `::ffff:172.19.0.2`, but
  parses only hexadecimal groups, `::ffff:ac13:2`. The fixture writes
  `mon_host` in the parseable form; convert an address the same way before you
  pass it to a Ceph tool, for example with `-m`. Go's `net/netip` parses both.
- HTTP endpoints, such as RGW's, are not Messenger listeners and keep their
  addresses.

##### Dual-stack clusters

`AddressDualStack` makes every Messenger bind and advertise both families.
The generated `ceph.conf` sets `ms_bind_ipv4`, `ms_bind_ipv6` and
`ms_bind_prefer_ipv4` to `true` and `public network` to both subnets of the
bridge, such as `172.19.0.0/16,fd62:93fe:2c9e::/64`. Every address vector
then has one entry of each family for each enabled Messenger protocol: v2,
and v1 unless [WithMessengerMode](#withmessengermode) removes it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithAddressFamily(ceph.AddressDualStack))
addresses, err := cluster.DaemonAddresses(ctx)
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

`Run`, [AddManager](#addmanager) and [RemoveManager](#removemanager) then read
[DaemonAddresses](#daemonaddresses) and check that every MON, the active MGR
and every up OSD advertise exactly one concrete IPv4 and one concrete IPv6
address for each protocol. An unspecified, IPv4-mapped or link-local address,
or a missing family, fails the call with `ErrUnsupported` (see
[Error categories](#error-categories)), naming the daemon and its vector. A
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
`WithSeparateClusterNetwork`, with OSD cluster and heartbeat back addresses of
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

1. [AddMonitor](#addmonitor) `c`, then [RemoveMonitor](#removemonitor) `b`;
2. [AddOSD](#addosd) (osd.2), then [RemoveOSD](#removeosd) `0`;
3. the MDS container stopped, its GID failed with `ceph mds fail` and a new
   MDS started with `AddMDSReplacement` (see [cephfs](cephfs.md#addmdsreplacement));
4. [AddManager](#addmanager) `b`, then [RemoveManager](#removemanager) `a`.

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
  [MonitorBootstrapAddresses](#monitorbootstrapaddresses) lists mon.c and not
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

The MGR is replaced last, so the admission check of `RemoveManager` also reads
the vectors of mon.c and osd.2. `AddMonitor` and `AddOSD` do not run the
check, and MDS vectors are not part of it. At the end a new process reads back
every file and the last objects.

Docker gives the address of a removed or stopped container to a later one.
A replacement can therefore advertise the old IP address of its predecessor
or of another replaced daemon, such as a new MDS on the address of a removed
OSD. An address alone does not identify a daemon; compare the nonce of the
entry, or the GID of a MGR or MDS. MONs always use nonce 0, so a MON is
identified by its name in the monmap.

###### Cutting one family of a path

[fault.ApplyNetworkProfile](fault.md#applynetworkprofile) with a `Peer`, a `PeerPort`
and `Loss: 100` drops every packet that one container sends to one address
and port. On a dual-stack cluster, a `Peer` that is a daemon's IPv6 address
cuts only that family of the path: the same daemon stays reachable over IPv4,
and the container's paths to other daemons are untouched.

```golang
addresses, err := cluster.DaemonAddresses(ctx)
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

#### WithStartupTimeout

```golang
ceph.WithStartupTimeout(5 * time.Minute)
```

Bounds `Run` as a whole and each later topology, readiness and wait operation
(`AddOSD`, `WaitForClean`, `WaitForPoolPGCount`, `DeepScrubPG`, ...). The default
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

### Service packages

The `rbd`, `cephfs` and `rgw` packages add service resources to the same cluster.
Their options are `ceph.Option` values that `ceph.Run` accepts, so one cluster can
serve RBD, CephFS and S3 clients together:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(3),
	rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
	rgw.WithGateways(rgw.Config{Name: "s3"}),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
filesystems := cephfs.Filesystems(cluster) // owned filesystem descriptors
gateways := rgw.Gateways(cluster)          // owned gateway descriptors
```

| Option | Creates during `Run` | Add to a running cluster | Details |
| --- | --- | --- | --- |
| `rbd.WithPools(configs...)` | Replicated pools initialized with `rbd pool init` (application `rbd`) | `rbd.InitPool` on a pool from `CreatePool` | [rbd](rbd.md) |
| `cephfs.WithFilesystems(configs...)` | Filesystems with their pools and MDS daemons | `cephfs.Start` | [cephfs](cephfs.md) |
| `rgw.WithGateways(configs...)` | Named gateways sharing the cluster's zone, each with a test S3 user unless `SkipUserCreation` is set | `rgw.Start` | [rgw](rgw.md) |

`Run` creates `WithPools` pools first, then RBD pools, filesystems and gateways.
All names, including the pools a filesystem creates, are validated together.
Filesystems and gateways need OSDs and a manager, so they are rejected with
`WithNoInitialOSDs` or `WithNoInitialManagers`; RBD pools need OSDs but no
manager. The cluster owns all service containers: `Terminate` removes them, and
[ServiceContainers](#servicecontainers) lists them.

Each service package also has its own `Run`, which takes every `ceph.Run` option
and adds that service's default when its own option is absent: `rbd.Run` creates
an initialized pool `rbd`, `cephfs.Run` the filesystem `tc-cephfs` with one
active MDS, and `rgw.Run` the default gateway with a test user. Their result is
that package's `Container`, which embeds `*ceph.Container`, so every method on
this page is available on it. A service `Run` rejects bootstrap options that
leave its service without the storage or manager it needs (for example
`ceph.WithNoInitialOSDs`); `rbd.Run` accepts `WithNoInitialManagers`. For staged
bootstraps, start with `ceph.Run` and add the service later.

### Shared cluster

A cluster takes 15 to 25 seconds to start, so a package with many small RADOS
tests spends most of its time starting clusters. A `SharedCluster` starts one
cluster for the whole test binary and lends each test a tenant: a RADOS
namespace of one shared pool and a Cephx client that can reach nothing else.
A test that needs the whole cluster takes it with `Exclusive`. Tests that need a
cluster with other `Run` options, or several clusters, keep using `Run`.

```golang
func NewShared(image string, opts ...SharedOption) *SharedCluster
func WithSharedRunOptions(opts ...testcontainers.ContainerCustomizer) SharedOption
func WithMaxTenants(n int) SharedOption
func WithMaxRecycles(n int) SharedOption
func RunTests(m *testing.M, s ...*SharedCluster) int

func (s *SharedCluster) Lease(t testing.TB, opts ...LeaseOption) *Tenant
func (s *SharedCluster) Exclusive(t testing.TB) *Container
func (s *SharedCluster) Close(ctx context.Context) error
func (s *SharedCluster) Stats() SharedStats
func WithKeepOnTenantFailure() LeaseOption
```

Create the shared cluster in a package variable and end it in `TestMain`.
`NewShared` starts nothing; the first `Lease` starts the cluster with the
options of `WithSharedRunOptions`, which are `Run` options. `RunTests` runs the
tests and then closes every shared cluster you pass it:

```golang
var shared = ceph.NewShared(ceph.DefaultImage,
	ceph.WithSharedRunOptions(ceph.WithOSDCount(1)))

func TestMain(m *testing.M) { os.Exit(ceph.RunTests(m, shared)) }

func TestStoresObject(t *testing.T) {
	t.Parallel()
	tenant := shared.Lease(t)
	client, err := testcontainers.Run(t.Context(), ceph.DefaultImage,
		tenant.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}
	// rados --id <tenant.Client().User()> -p <tenant.Pool()> -N <tenant.Namespace()> ...
}
```

Each test binary has its own shared cluster: `go test` runs every package in a
process of its own, and Ryuk removes what a process leaves. A shared cluster
that `RunTests` does not close stays until the process exits, and the first
`Lease` logs a warning about it. Call `Close` yourself when you do not use
`RunTests`.

`Lease` fails the test when the cluster cannot start or the tenant cannot be
set up, and releases the tenant in `t.Cleanup`. Leases are safe under
`t.Parallel`. `WithMaxTenants` bounds the leases held at once (4 by default);
`Lease` waits for a free one, so a test that holds a lease must not wait for a
second one when the running tests can hold all of them. `Lease` also waits while
a test holds the [exclusive lease](#exclusive-lease), and behind an `Exclusive`
that asked first.

#### Tenant

```golang
func (t *Tenant) ID() string
func (t *Tenant) Pool() string
func (t *Tenant) Namespace() string
func (t *Tenant) Client() *ClientConfig
func (t *Tenant) WithClient(opts ...ClientContainerOption) testcontainers.CustomizeRequestOption
func (t *Tenant) WithClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption
func (t *Tenant) CreateClient(ctx context.Context, name string, access TenantAccess) (*ClientConfig, error)
func (t *Tenant) RunNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error)

func (t *Tenant) Status(ctx context.Context) (Status, error)
func (t *Tenant) HealthDetails(ctx context.Context) (HealthSnapshot, error)
func (t *Tenant) Pools(ctx context.Context) ([]PoolState, error)
func (t *Tenant) PoolStatus(ctx context.Context, name string) (PoolState, error)
func (t *Tenant) PoolUsage(ctx context.Context, name string) (PoolUsageSnapshot, error)
func (t *Tenant) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error)
func (t *Tenant) OSDFlags(ctx context.Context) ([]string, error)
func (t *Tenant) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error)

const SharedPool = "tenants"
const (
	TenantReadOnly TenantAccess = iota + 1
	TenantReadWrite
)
```

`ID` is `t` and 8 hex digits. The tenant owns the namespace `ID` of the pool
`SharedPool`, which the shared cluster creates once with the cluster's pool
defaults. `Client` is `client.ID` with these capabilities:

| Service | Capability |
| --- | --- |
| mon | `allow r` |
| osd | `allow rw pool=tenants namespace=ID` |

`CreateClient` creates `client.ID-name` with `mon 'allow r'` and
`TenantReadOnly` (`allow r`) or `TenantReadWrite` (`allow rw`) on the same
namespace only. `WithClient` and `WithClientIdentity` attach a container like
the `Container` methods of the same names: it gets `ceph.conf` and the
client's keyring, never the admin keyring, and a label that marks it as the
tenant's. `RunNativeCommand` runs a command only in a container with that
label. The other methods are the read-only views of the `Container` methods of
the same names.

What a tenant can do:

- read and write objects of its own namespace, through its clients;
- create read-only and read-write clients of that namespace;
- read the cluster's status, health, pools, usage, watchers, OSD flags and
  blocklist.

What it cannot do, checked natively:

- read, write or list another tenant's namespace, every namespace (`ls --all`),
  the default namespace of the shared pool or another pool: Ceph returns
  `EPERM`;
- change configuration, OSD flags or the blocklist: Ceph returns `EACCES`.

Tenancy is isolation, not secrecy: a tenant sees pool names, usage and the
cluster's health.

#### Service tenants

A tenant can also lease RBD, CephFS and RGW resources of the shared cluster.
Start the services with the usual `Run` options in `WithSharedRunOptions`, and
call the service package's `Lease` with the tenant:

```golang
var shared = ceph.NewShared(ceph.DefaultImage, ceph.WithSharedRunOptions(
	ceph.WithOSDCount(1),
	rbd.WithPools(ceph.PoolConfig{Name: "rbd"}),
	cephfs.WithFilesystems(),
	rgw.WithGateways(),
))

func TestUsesEveryService(t *testing.T) {
	tenant := shared.Lease(t)
	images, err := rbd.Lease(t.Context(), tenant, "rbd")      // RBD namespace ID of pool rbd
	files, err := cephfs.Lease(t.Context(), tenant, "")       // subvolume group ID
	objects, err := rgw.Lease(t.Context(), tenant)            // RGW tenant ID, user ID$user
	...
}
```

| Lease | The tenant owns | Its client or credentials |
| --- | --- | --- |
| `rbd.Lease(ctx, tenant, pool)` | RBD namespace `ID` of `pool` | `client.ID-rbd-POOL`: `mon 'allow r'`, `osd` and `mgr 'profile rbd pool=POOL namespace=ID'` |
| `cephfs.Lease(ctx, tenant, fs)` | subvolume group `ID`, directory `/volumes/ID`, data in namespace `fsns-ID` of the default data pool | `client.ID-cephfs-FS`: `mon 'allow r'`, `mds 'allow rw fsname=FS path=/volumes/ID'`, `osd 'allow rw pool=DATA namespace=fsns-ID'` |
| `rgw.Lease(ctx, tenant)` | RGW tenant `ID` and its buckets | S3 keys of user `ID$user` |

The service leases are described in [RBD](rbd.md#shared-cluster-tenants),
[CephFS](cephfs.md#shared-cluster-tenants) and
[RGW](rgw.md#shared-cluster-tenants). A tenant leases each RBD pool, each
filesystem and RGW once. The clients are the tenant's: `Tenant.WithClientIdentity`
and `RunNativeCommand` accept them, and the release deletes them.

Before the baseline, the shared cluster creates what a service would
otherwise create on a tenant's first use: the pools of the RGW zone, such as
the bucket index and data pools. It also waits until the MGR `volumes` module
answers.

#### Release, verification and poisoning

When the shared cluster starts, it waits for active+clean PGs and records a
baseline: auth entities and their capabilities, pools and their policies, OSD
flags, the configuration database, the blocklist, the unmuted health checks
the objects of each namespace of the shared pool, and the service resources
that tenants could create: the RBD namespaces of every RBD pool, the subvolume
groups of every filesystem, and the RGW users and buckets. `MON_DISK_LOW` is left
out, because it reports the free space of the Docker host, which changes
whatever the tests do. Before a lease, it
checks that the daemon containers run, PGs are active+clean, and health, OSD
flags and the blocklist match the baseline, waiting up to 30 seconds.

A release first removes the tenant's service resources, in reverse order of
their leases: each service checks that nothing of the tenant's remains. It then
deletes the tenant's clients, removes the objects of its namespace, and
compares the cluster with the baseline, ignoring the scopes of the other
tenants. Health gets 30 seconds to settle. A difference means that the test
changed something outside its scope, for example through a raw admin path;
the release then fails the test and names what changed, without config values.

These poison the cluster:

- a release that finds a difference or a service resource that remains, or
  that cannot finish its removals and checks;
- a failed test of a tenant, unless the lease used `WithKeepOnTenantFailure`
  and the release found the cluster clean;
- a daemon container that exited, found by a lease or a release.

A poisoned cluster takes no new leases. It is terminated when its last lease
ends, and the next `Lease` starts a fresh cluster. `WithMaxRecycles` bounds
these restarts (2 by default); a `Lease` after that fails the test. A cluster
that fails to start is not retried: every later `Lease` fails with the same
error.

`Stats` returns `SharedStats`: the clusters started and recycled, the leases,
exclusive leases and poisonings, and the total time spent starting clusters,
waiting for and setting up leases, releasing them, and waiting for exclusive
leases.

#### Exclusive lease

`Exclusive` lends a test the whole shared cluster: the `*ceph.Container`, with
every method available except `Terminate`. Use it for a test that needs what
the tenant gate refuses, such as `OverrideConfig`, OSD flags, pool changes,
the raw `Ceph` CLI or `AddOSD`, in a package whose other tests share the
cluster:

```golang
func TestRecoversWithNoout(t *testing.T) {
	cluster := shared.Exclusive(t)
	if _, err := cluster.OverrideOSDFlag(t.Context(), "noout", true); err != nil {
		t.Fatal(err)
	}
	osd, err := cluster.AddOSD(t.Context())
	...
}
```

- **Waiting.** `Exclusive` waits until no tenant holds a lease. From the moment
  it asks, new leases wait behind it, so a stream of leases cannot starve it;
  leases that asked before it go first. While the exclusive lease is held, every
  `Lease` and other `Exclusive` waits. A test that already holds a lease can
  still take a second one while an `Exclusive` waits, because the `Exclusive`
  waits for that test's first lease. The same holds for a test whose parallel
  subtest (`t.Parallel`) holds a lease: the subtest releases it only after
  its parent returns, so the parent's lease goes before the waiting
  `Exclusive`.
- **Admission.** The cluster is checked against its baseline as for a lease. A
  cluster that drifted is poisoned and replaced first.
- **Release.** The release, in `t.Cleanup`, does not try to restore the
  cluster. It retires and terminates it, and the next `Lease` or `Exclusive`
  starts a fresh cluster. The baseline comparison cannot see everything an
  exclusive test can change: MGR modules, network shaping, paused containers,
  daemon keys, data, or what the service packages' functions do with the
  `Container`. These restarts do not count against `WithMaxRecycles`.
  Each `Exclusive` therefore costs one cluster start, so group the exclusive
  tests of a package or keep them few. A cluster that cannot be terminated
  stays with the shared cluster, and `Close` tries again and reports it.
- **Failing fast.** A test that holds a lease, or whose parent test holds one,
  cannot take the exclusive lease, because the exclusive lease would wait for
  that lease forever; `Exclusive` fails the test at once. `Lease` and
  `Exclusive` in the test that holds the exclusive lease, or in its subtests,
  also fail at once. So do `Exclusive` in a test whose parallel subtest
  (`t.Parallel`) still holds a lease, and `Lease` or `Exclusive` in a test
  whose parallel subtest holds the exclusive lease: the subtest is paused
  until its parent returns, so it cannot release before the parent's request
  would be granted.
- **Cancellation.** `Exclusive` fails the test when the test's context ends
  while it waits, also while the cluster starts, and the leases behind it go
  on.

#### The shared Container

The tenant never gets the `*ceph.Container`. While a cluster is shared, its
methods that could reach outside one tenant return an error that matches
`ErrSharedCluster` and names the method in `OperationError.Op`. The methods
that only read state stay available.

| Methods | Why they are refused |
| --- | --- |
| `CreateClient`, `DeleteClient`, `UpdateClientCaps`, `ReplaceClientKey`, `RevokeClient`, `ClientCapabilities`, `WithClientIdentity`, `RefreshClientMonitorConfig`, `RunNativeCommand` | Act on any identity or container; use the `Tenant` methods. |
| `OverrideConfig`, `OverrideDebugLevel`, `OverrideOSDFlag`, `OverrideFullRatios`, `OverrideMGRModule`, `OverrideMessengerPolicy`, `TemporaryBlocklist`, `SetOSDIn`, `CreatePool`, `RemovePool`, `SetPoolQuota`, `SetPoolReplication`, `SetPoolPGCount`, `SetPoolPlacement`, `DeepScrubPG`, `RepairPG`, `RotateDaemonKey`, `RefreshMonitorConfig` | Change cluster-wide state. |
| `Ceph`, `AdminSocket`, `ControlContainer`, `ControlContainerContext`, `ConnectionConfig`, `ConnectionConfigContext`, `WithClient`, `ManagerContainer`, `Monitors`, `MonitorsContext`, `Managers`, `ManagersContext`, `OSDs`, `OSDsContext`, `ServiceContainers`, and the `Exec`, `CopyToContainer`, `CopyDirToContainer`, `CopyFileToContainer`, `CopyFileFromContainer`, `Start` and `Stop` of the primary MON container | Give raw admin access. Methods without an error result return nil. |
| `AddOSD`, `AddOSDWithConfig`, `RemoveOSD`, `DetachOSD`, `ReopenOSD`, `AddMonitor`, `RemoveMonitor`, `AddManager`, `RemoveManager`, `InjectObjectDataError` | Change the topology or damage data. |
| `Terminate` | Only `SharedCluster.Close` ends a shared cluster. |

Under an [exclusive lease](#exclusive-lease) the gate refuses only `Terminate`.

`*ceph.Container` overrides those promoted methods of the MON's
`testcontainers.Container` only to apply this gate; on a cluster of your own
they behave as before.

#### Limits

- An exclusive lease costs a cluster start, because its release retires the
  cluster instead of restoring it.
- Service tenants get the scoped operations listed in their packages. The
  other service functions take a `*ceph.Container`, which a tenant never
  gets.
- Tenants share daemons, so a busy neighbour can slow a timing-sensitive test.
- Tests that inject faults, change ratios or quotas, or exercise recovery need
  an exclusive lease or a cluster of their own.
- The checks catch what a tenant left behind, not what it did and undid
  before its release.

### Container Methods

`*ceph.Container` owns every daemon, network and helper container it creates.
These rules apply across its methods:

- Context. Methods are bounded by the caller's context, and most also by
  [WithStartupTimeout](#withstartuptimeout). Topology changes are serialized;
  waiting for a concurrent operation to finish also honours the context. A call
  canceled while waiting returns without native changes and can be retried with
  a fresh context. A call canceled after it sent a native command can leave that
  command applied, which is why results and handles are kept (below).
- Partial results. A method that creates something (an OSD, a pool, a client
  identity, a handle) can return a non-nil result together with an error. The
  result identifies what was attempted and stays owned by the cluster; inspect it,
  retry the documented operation, or terminate the cluster. No method deletes
  data to roll back a partial failure.
- Observations. Read methods return the native state at the moment of the call.
  Several native reads are not one atomic snapshot. Health, PG statistics and
  usage are reported asynchronously by Ceph; poll with a deadline when you assert
  an eventual state. Several observation methods also verify the cluster's
  original FSID and pool IDs and return a zero value with an error when the
  identity differs.
- No compare-and-swap. Ceph offers no compare-and-swap for configuration, flags,
  CRUSH or auth. Do not change the same keys, flags or identities with raw CLI
  commands while a helper applies or restores them.

Methods that change cluster state temporarily return a restoration handle:

| Handle | Returned by | Ends with |
| --- | --- | --- |
| `*ceph.ConfigOverrideHandle` | [OverrideConfig](#overrideconfig) | `Restore` |
| `*ceph.DebugLevelOverrideHandle` | [OverrideDebugLevel](#overridedebuglevel) | `Restore` |
| `*ceph.OSDFlagOverrideHandle` | [OverrideOSDFlag](#overrideosdflag) | `Restore` |
| `*ceph.MGRModuleOverrideHandle` | [OverrideMGRModule](#overridemgrmodule) | `Restore` |
| `*ceph.FullRatiosOverride` | [OverrideFullRatios](#overridefullratios) | `Restore` |
| `*ceph.BlocklistOverride` | [TemporaryBlocklist](#temporaryblocklist) | `Restore` |
| `*fault.PauseLease` | [fault.Pause](fault.md#pause) | `Resume` |
| `*fault.NetworkInterruption` | [fault.InterruptNetwork](fault.md#interruptnetwork) | `Restore` |
| `*fault.NetworkProfileLease` | [fault.ApplyNetworkProfile](fault.md#applynetworkprofile) | `Restore` |
| `*ceph.ClientRevocationHandle` | [RevokeClient](#revokeclient) | `Restore` |

All handles follow the same contract:

- Keep a non-nil handle even when the call returned an error. A lost reply or a
  canceled context can leave the change applied, so restore it or terminate the
  cluster.
- `Restore`/`Resume` is idempotent and retryable with a fresh context. Copies of a
  handle share state.
- `Restore` returns only the changed item to its captured prior state, including
  "absent". It refuses, with an error, to overwrite a value that someone else
  changed in the meantime. Writes of the same value cannot be detected.
- A second handle for the same item (configuration key, daemon debug option,
  flag, module, ratios, container) is refused while the first is active.
- `Terminate` disposes of configuration, debug levels, flags, modules, ratios
  and the blocklist with the cluster, so restoring is optional when the
  condition lasts until the end of the test. Pauses are resumed and network
  interruptions restored by `Terminate` before it removes anything.

Restore in `t.Cleanup` with a context that is still live. `t.Context()` is
canceled before cleanup functions run:

```golang
change, err := cluster.OverrideConfig(ctx, setting)
if change != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := change.Restore(ctx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}
```

See [Contexts and cleanup](guides/contexts-and-cleanup.md) for cancellation,
retry and cleanup ordering in detail.

### Container Methods: Lifecycle and control

#### Terminate

```golang
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes the whole cluster: it restores network interruptions and resumes paused
containers first, cancels the commands that [RunNativeCommand](#runnativecommand)
still runs, releases host port reservations, stops services (RGW, MDS)
before OSDs, removes all owned daemons, helper containers, the OSD memory
volume and the shared run volume of the admin sockets, and finally removes the
owned networks. Every container that the cluster creates carries the label
`org.testcontainers.ceph.cluster` with the cluster's ID, and `Terminate` also
removes a container with that label that it has no handle for. Docker can
create such a container when the reply to a create request was lost, for
example when the startup timeout ended during the request. It works on a partially
initialized cluster returned by `Run` with an error, and a failed cleanup can be
retried. Each container gets a 2-second stop timeout instead of Docker's
10 seconds, because the data is discarded; a `testcontainers.StopTimeout` option
passed by the caller takes precedence.

A cluster that a [shared cluster](#shared-cluster) shares refuses `Terminate`
with `ErrSharedCluster`; `SharedCluster.Close` ends it.

Containers you attached with [WithClient](#withclient) or
[WithClientIdentity](#withclientidentity) are yours: terminate them before the
cluster, because Docker cannot remove a network that still has attached
containers. If an interrupted endpoint cannot be restored, its network is kept,
the restore failure is returned, and a later `Terminate` retries. With
`testcontainers.CleanupContainer`, register the cluster first and clients
afterwards.

#### TerminateOnCleanup

```golang
func (c *Container) TerminateOnCleanup(t testing.TB, config ceph.FinalizationConfig) (*ceph.Finalization, error)
func (f *Finalization) Check(name string, check func(context.Context) error)
func (f *Finalization) Cleanup(name string, cleanup func(context.Context) error)
func (f *Finalization) TrackDebugLevel(handle *ceph.DebugLevelOverrideHandle)
```

Registers one `t.Cleanup` that finalizes the cluster and then calls
[Terminate](#terminate). Use it instead of another cleanup that terminates the
cluster, and register it right after `Run`. The cleanup runs these phases in
this order, whatever order the test added its steps in:

1. The checks added with `Check`, in the order they were added. A check is a
   post-test verification that needs the running cluster. It fails the test
   by returning an error, by panicking or through `t` itself.
2. With `Archive`, the failure archive, if the test has failed by now. A
   failure that a check reported therefore triggers it too.
3. The cleanups added with `Cleanup`, in reverse order, such as the removal
   of a client container that the archive should still see. Test cleanups
   registered after `TerminateOnCleanup` run before all of this, as Go runs
   cleanups last-in first-out, so route such removals through `Cleanup`.
   With `Archive`, a cleanup that fails when nothing had failed before
   triggers an archive of what still runs, labelled `after-cleanup`.
4. `Terminate`. Its error fails the test.

```golang
cluster, err := ceph.Run(ctx, image)
if cluster != nil {
	finalization, regErr := cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{Archive: true})
	if regErr != nil {
		t.Fatal(regErr)
	}
	// client is a container that the test attached to the cluster.
	finalization.Cleanup("client", func(ctx context.Context) error { return client.Terminate(ctx) })
}
if err != nil {
	t.Fatal(err)
}
```

Each phase gets a fresh context with its own bound, because the test's
context has already ended when cleanups run. Errors stay separate: a check's
error and a cleanup's error fail the test under their names, the
archive's collection and storage errors are only logged and recorded in its
manifest, and none of them skips a later phase. A shared cluster refuses
`TerminateOnCleanup` with `ErrSharedCluster`. Adding a step after the
finalization has started fails the test instead.

`ceph.FinalizationConfig` fields (zero selects the default):

| Field | Default | Meaning |
| --- | --- | --- |
| `Archive` | off | Write the failure archive when the test fails. |
| `ArchiveDirectory` | the test's `ArtifactDir` | Root of the archives; each gets a new directory under `<root>/<test name>/`. |
| `Scenario` | none | One-line label for the manifest. |
| `CheckTimeout` | 2 minutes | All checks together; at most 10 minutes. |
| `ArchiveTimeout` | 3 minutes | The whole archive, both collectors and every write; at most 10 minutes. |
| `CleanupTimeout` | 3 minutes | The cleanups together, and `Terminate` separately; at most 10 minutes. |
| `Diagnostics` | defaults | The [CollectDiagnostics](#collectdiagnostics) configuration, including `AdditionalContainers` and `RedactValues`. |
| `CrashMetadata` | defaults | The [CollectCrashMetadata](#collectcrashmetadata) configuration. |

Invalid fields are rejected before anything is registered.

##### The failure archive

A passing test collects and writes nothing. In a failed test, the archive
runs `CollectDiagnostics` and `CollectCrashMetadata` at the same time, within
`ArchiveTimeout`, and writes each report it gets even when the collector
returned an error, so a failed query or container leaves the rest. It writes
into a new directory `ceph-<cluster>-<execution ID>`, where the execution ID
is random, so parallel tests, repeated executions and several clusters of one
test never share one:

| File | Contents |
| --- | --- |
| `diagnostics.json` | The `ceph.DiagnosticsReport`, absent when the collector returned none. |
| `crash-metadata.json` | The `ceph.CrashMetadataReport`, absent when the collector returned none. |
| `verification-<n>-<name>.json` | The `ceph.VerificationReport` of each [verification](#verify) whose check ran, in activation order. |
| `manifest.json` | Schema version, test, scenario, cluster ID, execution identity, role images, trigger, phase, each check, collector, file write, cleanup, the termination outcome and each tracked debug level window (see below). |

The manifest's execution identity has the Go version and platform, the VCS
revision when the test binary carries one, and the repository, workflow
event revision (`workflow_sha`, from `GITHUB_SHA`), workflow, job, run ID and
run attempt of a GitHub Actions run; no other environment variable is read.
The workflow event revision is not the checked-out source: a comment-triggered
run checks out a pull request's head, so only the binary's VCS revision counts
as the source revision. Provenance that the library does not know is listed in
`missing_provenance`, such as the source revision of a binary without VCS
information and the bug compatibility selection
(image IDs are in the diagnostics' `container-inspect` artifacts). A check
or cleanup records its error (a returned error, a panic, or a `t.Fatal` or
`t.FailNow` that ended its goroutine, which never skips the later phases)
apart from `failed_test`, set when the step turned a passing test into a
failed one through the test; the trigger has both kinds for checks and for
cleanups. Each
collector entry has `reported`, `complete` and its error; the crash entry
counts the containers per `ceph.CrashOutcome`, so an empty or absent crash
directory stays distinct from rejected metadata and found records. The
archive is first written while the cluster runs; after the cleanups and
`Terminate`, the manifest is replaced with their outcomes and `final` set.
Nothing is collected again after termination. The manifest records the
archive's directory name, not its host path.

By default the archive goes into the test's `ArtifactDir`, which `go test`
keeps under its output directory only with `-artifacts` (and `-outputdir`
for another place than the package directory). Without the flag the
directory is temporary and is removed after the test; the manifest's
`retained` field and the log line say so. Errors in creating the directory
or writing a file are logged and recorded, never retried in a loop, and do
not stop the later phases. If `ArtifactDir` itself cannot be created, Go
fails the test, and the cleanups and `Terminate` still run.

`Finalization.TrackDebugLevel` registers an [OverrideDebugLevel](#overridedebuglevel)
handle, including the non-nil handle that comes with an apply error. Each call
is one window in registration order (`debug_windows[].sequence`); windows on the
same daemon, option and generation are never merged or deduplicated. The
manifest keeps a copy of each window's receipt when the failure archive is
taken, labeled with the archive's phase (`archive`), and another after the
cleanups (`final`), so a restoration run by `Finalization.Cleanup` is a later,
separate record and the earlier copy still shows the raised level. The copy
reads only the handle's local state; it never sets, restores or rereads a
daemon, and a generic cleanup success does not stand in for `restored-verified`.
A nil handle, or one that is not from `OverrideDebugLevel`, is recorded with
`present: false` and no receipt, never as a restored window. The records are
bounded: 32 windows (`debug_windows_omitted` counts the rest), the first 16
and last 16 transitions of a window (`transitions_omitted` counts the middle),
256-byte fields and 2048-byte transition errors, all redacted like the other
manifest errors. Registering after finalization started fails the test.
Passing runs archive nothing. The copies are not native evidence of a restored
daemon level and do not cover other daemons, options or a window that spans an
OSD replacement.

Limits:

- The reports are a snapshot during finalization: observations are not
  atomic, and log tails are not continuous history.
- No crash records is not proof that a daemon never crashed. A container
  removed earlier took its records along.
- Nothing runs after a forced process exit, a Go test timeout panic or a lost
  runner.
- Redaction is that of the collectors, also for the errors in the manifest;
  review arbitrary daemon logs before sharing an archive.

#### Verify

```golang
func (f *Finalization) Verify(ctx context.Context, config ceph.VerificationConfig) (*ceph.Verification, error)
func (v *Verification) Evaluate(ctx context.Context) (*ceph.VerificationReport, error)
```

Opt-in post-test verification of a dedicated cluster: the log lines and the
new crashes of its daemons between the activation and the end of the test,
each against its own expected events. `Verify` activates it on a
`Finalization` from [TerminateOnCleanup](#terminateoncleanup) and adds a check
named `verification <name>`, so it runs before the failure archive and the
cleanups. A verification that is not clean fails the test, and with
`Archive` its report is archived with the diagnostics before anything is
removed. Runs that do not call `Verify` keep their behavior.

```golang
finalization, err := cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{Archive: true})
if err != nil {
	t.Fatal(err)
}
// The test crashes osd.0 on purpose: require that crash and its signal line.
_, err = finalization.Verify(ctx, ceph.VerificationConfig{
	Logs: []ceph.LogExpectation{{Name: "osd0-signal", Required: true, Daemon: "osd.0",
		Event: ceph.LogFatalSignal, Pattern: `Caught signal`}},
	Crashes: []ceph.CrashExpectation{{Name: "osd0-crash", Required: true, MaxCount: 1,
		Daemon: "osd.0", Process: "ceph-osd"}},
})
if err != nil {
	t.Fatal(err)
}
```

The activation records the baseline: the selected container generations
(the cluster's daemon containers and `AdditionalContainers`, not the control
container or the toolkit client), with their container ID, start time and
restart count; the crash IDs already in their crash directories; and the
window's start on the Docker engine's clock. A crash is new when its crash ID
was not in that container's directory at activation. `Verify` returns an
error for an invalid configuration, when the engine's clock cannot be read,
after the finalization has started, for a name already active on the
finalization, and with `ErrSharedCluster` on a cluster that a `SharedCluster`
shares, even exclusively: cluster-wide events cannot be attributed to one
tenant. `Evaluate` returns the report of the window until now without failing
the test, and can be repeated; it returns an error only when it cannot
evaluate at all.

Log events are lines of the containers' Docker log streams in the window,
read with Docker's timestamps. A line is an event of one `ceph.LogEvent`
kind, the first that matches:

| Kind | Line |
| --- | --- |
| `LogAssertion` | `FAILED ceph_assert`, `FAILED assert` or `ceph_abort` |
| `LogFatalSignal` | `Caught signal` |
| `LogClusterError` | a cluster log entry `[ERR]` |
| `LogClusterSecurity` | a cluster log entry `[SEC]` |
| `LogClusterWarning` | a cluster log entry `[WRN]` |

Daemons write their cluster log entries to their own output and the MON
writes them again as the cluster log, so one entry can be two events. Other
lines are not evaluated.

Each event goes to the first expectation of its kind (log or crash) that
matches it and still permits one more; one that none permits is unexpected.
A `ceph.LogExpectation` matches all of its non-empty criteria: `Daemon` (the
source as the report names it, such as `osd.0` or `mgr.a`), `Event`, and the
required `Pattern`, a regular expression on the redacted line. A
`ceph.CrashExpectation` matches `Daemon`, `Process` (such as `ceph-osd`) and
`Signature`, a regular expression on one line of the crash's `stack_sig`,
`assert_*` or `backtrace` fields; at least one of the three is required.
Both have `Name`, which identifies them in the report and is unique, and:

- `Required`: the expectation must match at least `MinCount` events (at least
  one) or the check is unexpected. Without it the expectation only permits,
  and `MinCount` must be zero.
- `MaxCount`: the number of events it permits, zero for any. Further events go
  on to the next expectations.

A log expectation never matches a crash, and a crash expectation never a log
line. Expectations do not repair, restart or archive anything, and they do
not suppress the test's own checks.

`ceph.VerificationConfig` fields (zero selects the default):

| Field | Default | Maximum | Meaning |
| --- | --- | --- | --- |
| `Name` | `post-test` | 128 bytes | Names the check and the report. |
| `Logs`, `Crashes` | none | 256 each | The expected events. |
| `Timeout` | 2 minutes | 5 minutes | The activation, and each evaluation; the check is also bounded by `CheckTimeout`. |
| `OperationTimeout` | 20 seconds | 1 minute | Each Docker operation, including one container's log stream. |
| `MaxLogBytes` | 32 MiB | 256 MiB | Log stream bytes of the window per container. |
| `MaxFindings` | 256 | 4096 | Findings kept per check. |
| `AdditionalContainers` | none | 64 | Caller-owned daemon containers, such as mirror daemons; never adopted. |
| `RedactValues` | none | 128 | Additional secrets to remove from the report. |

`ceph.VerificationReport` has `Name`, `Cluster`, `Images`, `Outcome`, the
`Window` (`ceph.VerificationWindow`: `Phase` `test`, `StartedAt` and
`EndedAt` on the engine's clock, `EvaluatedAt`), the `Sources`
(`ceph.VerificationSource`: `Source`, `Role`, `Name`, `ContainerID`,
`ImageID`, `StartedAt`, `RestartCount`, `State`, `LogDriver`, `LogBytes` and
the `BaselineCrashes`), the effective `LogExpectations` and
`CrashExpectations`, `Notes` on its scope, and one `ceph.VerificationCheck`
each for `Logs` and `Crashes`. A check has its `Outcome`, the `Permitted`
and `Unexpected` counts, `Findings` (`ceph.VerificationFinding`: `Source`,
`ContainerID`, `Event`, `Time`, the redacted `Text` of at most 1 KiB with
`Truncated`, `CrashID`, and the permitting `Expectation`, empty when
unexpected), with the unexpected ones first and `OmittedFindings` beyond
`MaxFindings`, the `Expectations` (`ceph.ExpectationResult`: `Name`,
`Required`, `MinCount`, `MaxCount`, `Matched`, `Satisfied`) and the `Gaps`
(`ceph.VerificationGap`: `Source`, `Reason`).

Each check's `ceph.VerificationOutcome`, and the report's, which is the worse
of the two:

| Outcome | Meaning |
| --- | --- |
| `VerificationUnexpected` | An event was not permitted, or a required expectation was not satisfied. Gaps found besides stay in the report. |
| `VerificationIncomplete` | Nothing unexpected was found, but an observation could not be completed; see `Gaps`. |
| `VerificationClean` | Every selected source was observed for the whole window, and nothing unexpected was found. |

An observation that cannot be completed is a gap, never clean: a container
that was not created, removed, restarted or inaccessible at activation or
since; a log driver other than `json-file`, or `json-file` with `max-size`
rotation; a log stream that fails, is cut inside a frame, has a line without
a valid timestamp or exceeds `MaxLogBytes`; a crash directory that is
missing, unreadable, cut off or holds rejected metadata; a new crash without
its done marker; a container added after the activation; more than 128
containers; an unknown end of the window, or an engine clock that did not
advance since the activation; and an expired context.

Limits:

- The window starts at the activation and ends at the check; bootstrap before
  it and the cleanups and termination after it are outside it. Only the
  daemons' output is observed, not lines below their log level.
- Only the container generations selected at activation are covered. A
  removed or replaced container takes its logs and crash records along and is
  reported as a gap, not covered.
- Clean is evidence for the reported sources and window, not proof that no
  daemon failed. Its log and crash coverage has not yet been measured against
  native log windows, crash completion and collection limits (#47).
- Shared-cluster tenants, scrub results, fresh scrubs and fault recovery waits
  are not verified, and the bug compatibility selection is not known to the
  report.

#### Ceph

```golang
func (c *Container) Ceph(ctx context.Context, args ...string) ([]byte, error)
```

Runs the `ceph` CLI in the control container with `--connect-timeout 5` and
returns its standard output. Arguments are passed directly, without a shell. Add
`--format json` for machine-readable output. A non-zero exit returns the
standard output with a [`*NativeFailure`](#nativefailure): the exit code, the
errno that Ceph reported, such as `ENOENT`, and Ceph's reason without its log
lines, with keys redacted. `errors.Is(err, fs.ErrNotExist)` holds for
`ENOENT`. When no MON answers the connection within 5 seconds, the CLI exits
before it sends the command; `Ceph` then connects again, until a MON
answers, `ctx` ends or a minute has passed, so a stalled or electing MON
delays the command instead of failing it, and the command runs once. See
[Classified errors](guides/errors.md).

```golang
out, err := cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "noscrub", "true")
```

See [Running native commands](#running-native-commands).

#### RunNativeCommand

```golang
func (c *Container) RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
```

Runs one command in `target` under a supervisor that bounds its run time and
each output stream inside the container, and that stops the command when
`ctx` ends. `target` is the control container, a daemon or service container
of this cluster, or a container of yours attached to the cluster's network.
It must be running, not paused, and have `python3`, which OSD, RGW and MDS
role images need not have.

```golang
result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:      []string{"ceph-dencoder", "type", "OSDMap", "import", "/tmp/osdmap", "decode", "dump_json"},
	Timeout:   time.Minute,
	MaxStdout: 4 << 20,
	MaxStderr: 64 << 10,
	Label:     "decode OSDMap",
})
```

| `NativeCommand` field | Meaning |
| --- | --- |
| `Argv` | Program and arguments, without a shell |
| `Timeout` | Up to `MaxNativeCommandTimeout` (one hour); `ctx`'s deadline shortens it |
| `MaxStdout`, `MaxStderr` | Up to `MaxNativeCommandOutput` (64 MiB) each |
| `Label` | Names the command in errors; at most 128 bytes |
| `Input`, `MaxInput` | Optional standard input from `NewNativeInput`, and the bytes it may carry in all, up to `MaxNativeCommandInput` (64 MiB) |

- The result keeps stdout and stderr as separate raw bytes, and the exit code;
  a nonzero exit is not an error.
- A timed-out or truncated command returns its partial result with
  `TimedOut`, `StdoutTruncated` or `StderrTruncated` set, and an error that
  wraps `ErrNativeCommandIncomplete`.
- When `ctx` ends first, the command is stopped inside the container before
  the call returns, within `NativeCommandStopTimeout` (15 seconds) of its own.
  When that cannot be confirmed, the error is a
  `*NativeCommandUnstoppedError`; keep it and call its `Stop` with a fresh
  context. Its `Unwrap` returns the cause, so `errors.Is(err, context.Canceled)`
  still holds. `Terminate` also cancels and stops these commands. It does so
  first, before it waits for any running cluster operation, and from then on
  refuses new ones, also when it then fails.
- Stopping a command does not undo what it did, and a command is never run
  again.
- With `Input`, the command reads what `Input.Write` sends, in order, and the
  end of input after `Input.Close`. `InputWritten`, `InputDelivered` and
  `InputEOF` in the result say how much of it the command could read. Writes
  fail with `ErrNativeInputClosed` once the command ended.
- `Complete()` checks time and output limits, not input delivery. Assert the
  byte counts, EOF and the worker's receipt separately; bytes delivered into
  its pipe do not prove application execution or durability.
- Errors contain the label but never the arguments or the output. A Docker
  or container failure appears only by its kind; `errors.Unwrap` returns the
  original error.
- Failures before the command runs are classified: a malformed command, a
  container that the cluster neither owns nor has attached, or one that is
  stopped or paused is `ErrInvalidRequest`; a target without `python3`, or a
  program that does not exist or is not executable, is `ErrImageCapability`;
  a program that could not be started for want of a process or memory, or
  because a security policy refused it, and another Docker failure are
  `ErrEnvironmentUnavailable`. A container that disappears or stops after
  the command was sent matches no category, since the command may have run.
  Context errors, `ErrNativeCommandIncomplete`, `ErrNativeInputClosed` and
  `*NativeCommandUnstoppedError` belong to no category. See
  [Classified errors](guides/errors.md).

See [Bounded commands with confirmed cleanup](guides/native-commands.md#bounded-commands-with-confirmed-cleanup)
for cancellation details, [standard input](guides/native-commands.md#standard-input),
and an oracle recipe with `rados_stat2` and `ceph-dencoder`.

#### NativeInput

```golang
func NewNativeInput() *ceph.NativeInput
func (in *NativeInput) Write(ctx context.Context, data []byte) error
func (in *NativeInput) Close() error
```

The standard input of one `NativeCommand`. `Write` sends binary data after
the data of earlier writes and returns once the input stream to the
container took it; before the command starts it waits, within `ctx`. A write
that would pass `MaxInput` is refused whole with `ErrInvalidRequest`, and the
input stays open. `Close` ends the input. The transport does not put input in
arguments, the environment, files, errors or diagnostics; the command's own
output remains raw, so a worker handling secrets must not echo them. An
input serves one command and must be created with `NewNativeInput`.

#### RedactSecrets

```golang
func (c *Container) RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error)
```

Returns a copy of `data` in which the keys of the cluster's keyrings, service
credentials, `values`, and the private keys, secret assignments and bearer
tokens it recognizes are replaced by `[REDACTED]`, as in
[CollectDiagnostics](#collectdiagnostics). Use it before logging native
output; assertions compare the raw bytes. Redaction is best effort.

#### ControlContainer

```golang
func (c *Container) ControlContainer() testcontainers.Container
```

Returns the container in which the fixture runs CLI commands. In a single-MON
cluster this is MON `a`. With more than one monitor it is an independent CLI
container created by the first [AddMonitor](#addmonitor) (also during `Run`), so
commands keep working while MON `a` is stopped or removed. The cluster owns it.
Use it to run other tools from the control image:

```golang
code, reader, err := cluster.ControlContainer().Exec(ctx,
	[]string{"rados", "-p", "app-data", "ls"}, tcexec.Multiplexed())
```

Do not pause or stop it; [fault.Pause](fault.md#pause) refuses it.

#### ControlContainerContext

```golang
func (c *Container) ControlContainerContext(ctx context.Context) (testcontainers.Container, error)
```

Like [ControlContainer](#controlcontainer), but bounds the wait for a concurrent
control-plane creation or termination by `ctx`. The returned handle is a snapshot;
it does not promise that the container stays alive.

#### ControlImage

```golang
func (c *Container) ControlImage() string
```

Returns the image passed to `Run`, used for MONs, MGRs and auxiliary CLI, client
and mirror containers. It stays available after termination. Use it as the image
of your client containers when they need the Ceph tools.

#### CephVersion

```golang
func (c *Container) CephVersion() string
```

Returns the Ceph release of the control image, such as `"20.2.4"`, read once from
`ceph --version` after MON `a` starts. `Run` fails if the output cannot be
parsed. Role images selected with `WithImages`, `rgw.WithImage` or
`cephfs.WithMDSImage` may run another release; the value describes only the
control image. The fixture uses it to choose commands that differ between
releases, and tests can use it to choose release-specific assertions:

```golang
if strings.HasPrefix(cluster.CephVersion(), "19.") {
	// Squid has no messenger dump: assert the fault through client errors.
	assertClientTimesOut(t, ctx, client)
} else {
	assertMessengerDropsFrames(t, ctx, cluster)
}
```

#### ServiceContainers

```golang
func (c *Container) ServiceContainers() []testcontainers.Container
```

Returns the owned optional service containers (RGW gateways and MDS daemons),
sorted by service name. The cluster owns their cleanup. Use the service packages
(`rgw.Gateways`, `cephfs.Filesystems`) for typed descriptors.

### Container Methods: Cluster topology

#### Monitors

```golang
func (c *Container) Monitors() []*ceph.MonitorContainer
```

Returns the owned monitors sorted by daemon name. Each `*ceph.MonitorContainer`
embeds its `testcontainers.Container` and has a `DaemonName`. `Stop` and `Start`
on the handle inject failures and preserve the monitor's store; use
[RemoveMonitor](#removemonitor) to change membership.

```golang
mon := cluster.Monitors()[1]
timeout := 10 * time.Second
if err := mon.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// With 3 MONs, 2 keep quorum and existing client sessions keep working.
if err := mon.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForQuorum(ctx); err != nil {
	t.Fatal(err)
}
```

When a majority is stopped, quorum is lost and new clients cannot authenticate.
After the monitors start again, both existing and new sessions work.

A MON binds the address that the monmap records for it. On a bridge network,
each MON returns at that address whatever the order of `Start` calls: before
a stopped MON starts, the fixture requests its original address from Docker
again. A MON whose endpoint is interrupted with
[fault.InterruptNetwork](fault.md#interruptnetwork) does not start until the interruption is
restored. MON a's container waits for a quorum when it starts, so after all
MONs were stopped, start the others first.

#### MonitorsContext

```golang
func (c *Container) MonitorsContext(ctx context.Context) ([]*ceph.MonitorContainer, error)
```

Like [Monitors](#monitors), but bounds the wait for a concurrent topology change
by `ctx`. The returned slice is independent; descriptors and containers remain
owned by the cluster.

#### AddMonitor

```golang
func (c *Container) AddMonitor(ctx context.Context, name string) (*ceph.MonitorContainer, error)
```

Starts a new monitor with the given name and joins it to the live quorum's
monmap with the shared key. Names already in the monmap are rejected, and `a` can
never be reused. The first call also starts the independent control container.
After joining, it copies the new quorum's addresses into the `mon_host` of every
owned daemon's `ceph.conf`, including stopped daemons. A partially started member
stays owned on error; if only the configuration copy failed, retry
[RefreshMonitorConfig](#refreshmonitorconfig). Client containers you own are not
updated (see [RefreshClientMonitorConfig](#refreshclientmonitorconfig)).

#### RemoveMonitor

```golang
func (c *Container) RemoveMonitor(ctx context.Context, name string) error
```

Removes an owned monitor from the monmap, deletes its container and refreshes the
owned daemons' `mon_host`. It refuses a removal that would leave fewer than a
majority of the current map running; add a replacement first. MON `a` can be
removed too; afterwards the embedded container of `*ceph.Container` is gone and
fixture commands use the control container. If the `mon remove` reply is lost,
the call returns an error without resending; calling `RemoveMonitor` again with a
fresh context reads current membership, skips the completed native step and
finishes the container and configuration cleanup.

#### QuorumStatus

```golang
func (c *Container) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
```

Returns the native `quorum_status`: `QuorumNames` (members currently in quorum)
and `MonMap` with the FSID and each monitor's name and advertised address vector
(`PublicAddrs.Addrvec`, entries of `Type` such as `v2`/`v1` and `Addr`). The query
runs under a 5-second process limit inside the control container, so it returns
an error rather than hanging while quorum is lost.

#### WaitForQuorum

```golang
func (c *Container) WaitForQuorum(ctx context.Context) error
```

Waits until a majority of the current monmap is in quorum, including when a
monitor is deliberately stopped. It does not require `HEALTH_OK` or every monitor
to run.

#### MonitorBootstrapAddresses

```golang
func (c *Container) MonitorBootstrapAddresses(ctx context.Context) (string, error)
```

Returns the current majority monmap's versioned address vectors in the format of
Ceph's `mon_host` setting, after checking that the live FSID matches the
cluster's original bootstrap configuration. Use it to configure a client you
manage yourself after replacing monitors. It changes nothing.

#### RefreshMonitorConfig

```golang
func (c *Container) RefreshMonitorConfig(ctx context.Context) error
```

Copies the live quorum's bootstrap addresses into the global `mon_host` entry of
every owned daemon's `/etc/ceph/ceph.conf`, including stopped daemons, without
starting or restarting them. Other entries and comments are kept, and files that
are already current are not rewritten. `AddMonitor` and `RemoveMonitor` already
do this; call it to retry after an `AddMonitor` that joined the monmap but could
not copy every file. Caller-owned clients and cross-cluster links are not
updated. Running daemons learn new monitors from the monmap; the file matters for
their next start.

#### Managers

```golang
func (c *Container) Managers() []*ceph.ManagerContainer
```

Returns the owned manager candidates sorted by daemon name. Each
`*ceph.ManagerContainer` embeds its container and has a `DaemonName`. Stop the
active manager's container to trigger a standby takeover, or stop the only
manager to test behaviour without a manager:

```golang
status, err := cluster.ManagerStatus(ctx)
if err != nil {
	t.Fatal(err)
}
for _, mgr := range cluster.Managers() {
	if mgr.DaemonName == status.ActiveName {
		timeout := 10 * time.Second
		if err := mgr.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
	}
}
```

#### ManagersContext

```golang
func (c *Container) ManagersContext(ctx context.Context) ([]*ceph.ManagerContainer, error)
```

Like [Managers](#managers), but bounds the wait for a concurrent topology change
by `ctx`. The returned slice is independent; descriptors and containers remain
owned by the cluster.

#### ManagerContainer

```golang
func (c *Container) ManagerContainer() testcontainers.Container
```

Returns the container of the initial manager `a`, kept for compatibility. It is
nil when `a` has been removed or was never created (for example when the first
manager added after `WithNoInitialManagers` has another name). Use
[Managers](#managers) and [ManagerStatus](#managerstatus) for the current
candidates and the active identity.

#### AddManager

```golang
func (c *Container) AddManager(ctx context.Context, name string) (*ceph.ManagerContainer, error)
```

Creates a named manager and waits for it to register in the MGR map, as active
if no manager is active, otherwise as standby. Existing daemon names are rejected,
and it never forces the active role away from a running manager. Success does
not mean that every MGR module is ready; use
[WaitMGRModuleReady](#waitmgrmoduleready) or [WaitForClean](#waitforclean)
afterwards. A non-nil descriptor returned with an error stays owned. On an
`AddressDualStack` cluster it then checks the advertised addresses of the
active MGR; see [Dual-stack clusters](#dual-stack-clusters).

#### RemoveManager

```golang
func (c *Container) RemoveManager(ctx context.Context, name string) error
```

Deletes an owned manager after confirming that another running, registered
candidate is available. Removing the last candidate is refused; add a replacement
first. Removing the active manager makes a standby take over; on an
`AddressDualStack` cluster the new active manager's addresses are checked as in
[Dual-stack clusters](#dual-stack-clusters). On failure the handle stays owned
for a retry or for cluster cleanup.

#### ManagerStatus

```golang
func (c *Container) ManagerStatus(ctx context.Context) (ceph.ManagerStatus, error)
```

Returns the native MGR map summary: `Available`, `ActiveName`, `ActiveGID` and
`Standbys` (each with `Name` and `GID`). The GID changes when a manager process
restarts.

#### OSDs

```golang
func (c *Container) OSDs() []*ceph.OSDContainer
```

Returns a snapshot of the owned OSDs sorted by ID. Each `*ceph.OSDContainer`
embeds its container and has the numeric `ID`; `Placement()` returns its resolved
CRUSH location. `Stop`/`Start` on the handle inject daemon failures; the OSD keeps
its identity and data (also with in-memory storage):

```golang
osd := cluster.OSDs()[0]
timeout := 10 * time.Second
if err := osd.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Exercise degraded I/O. Ceph marks the OSD down after the heartbeat grace and
// out after mon_osd_down_out_interval unless the noout flag is set.
if err := osd.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

OSD bootstrap initializes only an empty directory. It refuses non-empty partial
stores without a regular ready marker before copying the launch keyring or
running mkfs. A ready store must have matching numeric ID, OSD UUID and configured
cluster FSID, with non-empty regular identity/keyring files, before daemon
startup. Failed initialization is not automatically replayed. See
[bootstrap admission](design/osd-bootstrap-admission.md) for the exact checks
and limits. [Launch recipes](design/osd-launch-recipes.md) capture file/config
inputs and rebuild fresh Readers without replaying container creation; neither
change introduces retained-store detach/reopen support.

#### OSDsContext

```golang
func (c *Container) OSDsContext(ctx context.Context) ([]*ceph.OSDContainer, error)
```

Like [OSDs](#osds), but bounds the wait for a concurrent topology change by
`ctx`. The returned slice is independent; descriptors and containers remain
owned by the cluster.

#### AddOSD

```golang
func (c *Container) AddOSD(ctx context.Context) (*ceph.OSDContainer, error)
```

Registers, formats and starts a new OSD with the default CRUSH location and waits
until it is up and in. It uses the OSD image, block size and storage options
selected at `Run`. An OSD returned with an error is tracked for cleanup and can be
inspected; nothing is purged automatically. `AddOSD` is refused while an
earlier [RemoveOSD](#removeosd) has not finished its cleanup, so a reused numeric
ID cannot be confused with the old handle. Adding OSDs does not change existing
pool sizes.

#### AddOSDWithConfig

```golang
func (c *Container) AddOSDWithConfig(ctx context.Context, config ceph.OSDConfig) (*ceph.OSDContainer, error)
```

Like [AddOSD](#addosd), with an explicit logical CRUSH location
([OSDConfig](#withinitialosds)). An omitted host resolves to `osd-<ID>` after
native registration. If that name then conflicts with the selected root or rack,
the registered descriptor is returned with an error before its container starts;
terminate the cluster in that case.

```golang
osd, err := cluster.AddOSDWithConfig(ctx, ceph.OSDConfig{
	Host: "host-d", Rack: "rack-b", DeviceClass: "fast",
})
```

#### RemoveOSD

After identity and last-OSD admission, removal permanently fences that Docker
instance from new public `RunNativeCommand` calls and drains its tracked native
commands before reweight/out/stop/purge. A command admitted before the fence
must pass a second admission check before dispatch. An unconfirmed stop or
cancelled drain retains the invocation, OSD ownership and fence for a fresh
removal retry. Commands targeting other instances remain admitted. This does
not close cluster-wide native admission.

This increment does not add retained-store detach/reopen, attachment leases or
fault-handle fencing. Internal service shell admission and raw Docker handles
keep their existing contracts; raw references are not revoked by a Go admission
fence. `RemoveOSD` still confirms container cleanup after permanent purge.


```golang
func (c *Container) RemoveOSD(ctx context.Context, id int) error
```

Removes an owned OSD in the safe order: mark it out and drain it, wait for
`safe-to-destroy`, stop it, wait until it is down, purge it from Ceph and remove
its container. Every step checks the OSD's registration UUID, so an OSD that
someone else re-registered under the same ID is refused. Removing the last
registered OSD is refused.

If the remaining OSDs cannot hold the data (replica count or free space), the
drain does not finish and the call fails at the timeout; the OSD is left out and
reweighted and is not marked in again automatically. If the purge reply is lost,
the handle is kept: a retry with a fresh context treats the OSD's absence as
completion of its own purge, and after a completed purge only the Docker cleanup
is retried. Do not register or replace OSDs externally while a removal runs.
`RemoveOSD` refuses an OSD that [DetachOSD](#detachosd) is detaching or has
detached.

#### DetachOSD

```golang
func (c *Container) DetachOSD(ctx context.Context, id int) (*ceph.DetachedOSD, error)
```

Stops and removes an owned OSD's container but keeps its store in the owned
volume and its MON registration, so that [ReopenOSD](#reopenosd) can start a
new container on them. It needs [WithOSDVolumeStorage](#withosdvolumestorage);
other storage is an invalid request. The OSD is down while it is detached, not
out or purged: hold `noout` with [OverrideOSDFlag](#overrideosdflag) if Ceph
must not mark it out.

Detach checks the registration UUID, refuses an OSD whose container a held
network delay, network interruption or pause still targets, then fences the
instance against new native commands as [RemoveOSD](#removeosd) does, stops it,
waits for it to be down and removes the container. Only after the removal does
it read the store's identity files, keyring and block inode and size, and
returns a `*ceph.DetachedOSD` with `ID()`, `UUID()`, `ClusterFSID()` and
`Placement()`. The OSD leaves [OSDs](#osds); the previous `*ceph.OSDContainer`
keeps naming the removed container.

On error the OSD stays owned and fenced. Retry `DetachOSD` with a fresh context;
it resumes after a completed removal. `RemoveOSD` refuses it meanwhile.

#### ReopenOSD

```golang
func (c *Container) ReopenOSD(ctx context.Context, detached *ceph.DetachedOSD) (*ceph.OSDContainer, error)
```

Starts a new container on a store that `DetachOSD` of the same cluster
retained and waits for the OSD to be up/in. It returns a new
`*ceph.OSDContainer` with the same ID, UUID and placement and a different
container.

Before anything is written it requires the store's identity files, keyring and
block inode and size to be unchanged since the detach, the MON registration to
hold the same UUID and be down, and the MONs' key for the OSD to equal the
stored one. A refusal leaves the OSD detached for another attempt. Once the
container is requested the `DetachedOSD` is consumed: on a later error the
returned handle is owned by the cluster, so detach or remove it, or terminate
the cluster. The OSD bootstrap checks the stored identity again and never
formats a store that has its ready marker.

```golang
noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
if err != nil {
	t.Fatal(err)
}
detached, err := cluster.DetachOSD(ctx, 0)
if err != nil {
	t.Fatal(err)
}
// The store and registration of osd.0 have no container here.
osd, err := cluster.ReopenOSD(ctx, detached)
if err != nil {
	t.Fatal(err)
}
if err := noout.Restore(ctx); err != nil {
	t.Fatal(err)
}
_ = osd // same ID and UUID, new container
```

Only graceful detach of one cluster's classic BlueStore store on the same
image is covered. Abrupt exit, host reboot, other images and reopening a store
in another cluster are not.

#### OSDStates

```golang
func (c *Container) OSDStates(ctx context.Context) ([]ceph.OSDState, error)
```

Returns the OSDMap state of the owned OSDs sorted by ID: `ID`, `UUID`, `Up`, `In`
and `Weight`. `Weight` is the OSDMap reweight in `[0,1]`, not the CRUSH capacity
weight. An owned OSD that is missing, purged or registered with another UUID is
reported as an error.

#### SetOSDIn

```golang
func (c *Container) SetOSDIn(ctx context.Context, id int, in bool) error
```

Marks an owned OSD in or out of the OSDMap, then reads the state back and checks
the UUID. Out does not stop, purge or reweight the OSD in CRUSH: the container,
identity and data stay. Ceph restores the previous reweight when the OSD is
marked in again. Use [WaitForPGClean](#waitforpgclean) to wait for recovery while
an OSD remains out. The `noout` flag suppresses automatic out-marking by the MON
but does not block an explicit `SetOSDIn(ctx, id, false)`.

#### OSDFlags

```golang
func (c *Container) OSDFlags(ctx context.Context) ([]string, error)
```

Lists the exact global OSDMap flag tokens, including Ceph's internal
compatibility flags (for example `sortbitwise`). Per-OSD, CRUSH-node and
device-class flags are separate native state.

#### OverrideOSDFlag

```golang
func (c *Container) OverrideOSDFlag(ctx context.Context, flag string, enabled bool) (*ceph.OSDFlagOverrideHandle, error)
```

Sets or clears one global OSDMap flag and remembers its prior state. Supported
flags: `noup`, `nodown`, `noout`, `noin`, `nobackfill`, `norebalance`,
`norecover`, `noscrub`, `nodeep-scrub`, `nosnaptrim` and `noautoscale`. The
composite `pause` flag and irreversible compatibility flags are not supported.
`Restore` returns only this flag to its prior state (a flag that was already set
stays set) and never touches other flags. While set, flags such as `noout` raise
the `OSDMAP_FLAGS` health warning (per-OSD flags raise `OSD_FLAGS` instead).

```golang
hold, err := cluster.OverrideOSDFlag(ctx, "noout", true)
```

### Container Methods: Pools

#### CreatePool

```golang
func (c *Container) CreatePool(ctx context.Context, config ceph.PoolConfig) (*ceph.Pool, error)
```

Creates a new pool with explicit placement and replication (see the
[PoolConfig fields](#withpools)). The whole configuration is validated and the
cluster must own enough eligible failure domains (`Replicas`, or `K+M` for EC,
distinct buckets of `FailureDomain` in `CRUSHRoot`, restricted to `DeviceClass`)
before any command is sent. Existing pools, CRUSH rules and EC profiles are
rejected rather than changed.

The pool uses a dedicated CRUSH rule `tc-<name>-replicated` (or `tc-<name>-ec` with
an EC profile of the same name) and is created with the autoscaler off. The
returned `*ceph.Pool` embeds the resolved `PoolConfig` and adds `CRUSHRule` and
`ErasureCodeProfile`. It describes the requested settings at creation; use
[PoolStatus](#poolstatus) for current state.

A non-nil `Pool` with an error identifies resources whose creation was attempted;
no data is deleted after a partial failure. Call [WaitForClean](#waitforclean)
before application I/O.

```golang
pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "app-data", Replicas: 2, MinSize: 1, Application: "rados",
})
```

For RBD on an EC pool, create the images in a replicated metadata pool and pass
the EC pool as the data pool (`--data-pool` for the `rbd` CLI, or the data pool
option of librbd).

#### Pools

```golang
func (c *Container) Pools(ctx context.Context) ([]ceph.PoolState, error)
```

Lists the current native policy of every pool, including pools created by Ceph,
by services or through the CLI.

#### PoolStatus

```golang
func (c *Container) PoolStatus(ctx context.Context, name string) (ceph.PoolState, error)
```

Returns the current policy of one pool by its exact name. `ceph.PoolState` fields:

| Field | Meaning |
| --- | --- |
| `ID`, `Name`, `Type` | Native pool ID, name and type (`replicated` or `erasure`). The ID changes if a pool is deleted and recreated under the same name. |
| `Size`, `MinSize` | Replica count or EC shard count (`K+M`), and `min_size`. |
| `PGNum` | Current PG count. |
| `PGNumTarget`, `PGNumPending`, `PGPlacementNum`, `PGPlacementNumTarget` | Split or merge still in progress; zero when the native map omits them. |
| `CRUSHRule` | Native CRUSH rule ID. |
| `AutoscaleMode`, `ErasureCodeProfile`, `Flags` | Native values. |
| `Quota` | `ceph.PoolQuota` with `MaxBytes` and `MaxObjects` (zero means unlimited). |

#### SetPoolReplication

```golang
func (c *Container) SetPoolReplication(ctx context.Context, name string, replicas, minSize int) error
```

Changes a replicated pool's `size` and `min_size` without changing data, PG count
or placement. The pool's CRUSH rule must contain enough owned placement domains
for the new size. EC shard counts cannot be changed. Commands are sequential; on
a partial failure inspect [PoolStatus](#poolstatus). Use
[WaitForClean](#waitforclean) for the resulting recovery.

#### SetPoolQuota

```golang
func (c *Container) SetPoolQuota(ctx context.Context, name string, quota ceph.PoolQuota) error
```

Replaces both pool quota limits; a zero field removes that limit. Existing data
is never removed. The native pool ID is checked before each command, and the two
commands can partially succeed (inspect `PoolStatus` after an error). Quotas are
enforced from asynchronously reported PG statistics, so writes can overshoot
briefly; they are not a per-write accounting boundary. When a quota is reached,
Ceph marks the pool full and clients see writes blocked or `EDQUOT`, depending on
the client.

```golang
err := cluster.SetPoolQuota(ctx, "app-data", ceph.PoolQuota{MaxBytes: 64 << 20, MaxObjects: 1000})
```

#### SetPoolPlacement

```golang
func (c *Container) SetPoolPlacement(ctx context.Context, name string, placement ceph.PoolPlacement) error
```

Moves a replicated pool to another simple CRUSH rule. `ceph.PoolPlacement` has
`FailureDomain` (`osd` by default), `CRUSHRoot` (`default` by default) and
`DeviceClass` (none by default), with the same meaning as in `PoolConfig`. Size
and `min_size` stay unchanged; the new placement needs enough owned domains.

The rule is chosen as follows:

1. If the current rule already matches, nothing is sent.
2. The pool's `CreatePool` rule `tc-<pool>-replicated` is reused if it matches.
3. An existing `tc-<pool>-placement-<hash>` with matching steps is reused; one with
   different steps is refused.
4. Otherwise `tc-<pool>-placement-<hash>` is created. Rules are never deleted, so a
   pool can move back.

Success means the pool references the rule; data moves asynchronously. Until PG
reports reflect the new map, [WaitForClean](#waitforclean) can succeed on the old
placement, so poll [PoolPGs](#poolpgs) for the expected up/acting sets. To hold
the move incomplete, set both `nobackfill` and `norecover` with
[OverrideOSDFlag](#overrideosdflag): small PGs recover through the PG log
instead of backfill, so `nobackfill` alone does not stop them. EC pools are
refused because their rule is bound to the EC profile.

#### SetPoolPGCount

```golang
func (c *Container) SetPoolPGCount(ctx context.Context, name string, pgNum int) error
```

Requests a new PG count (1 to 65536) for a pool whose autoscale mode is `off` or
`warn`; pools with autoscaling `on` are refused. It records the `pg_num` and
`pgp_num` targets. The active manager then splits or merges PGs in steps
(merges only proceed with clean PGs), so success means the targets were accepted,
not that the PGs changed. Without an active manager the targets stay pending.
Ceph's per-OSD PG limit (`mon_max_pg_per_osd`) can still reject the request. The
pool ID is checked before each command; after a partial failure inspect
[PoolStatus](#poolstatus).

#### WaitForPoolPGCount

```golang
func (c *Container) WaitForPoolPGCount(ctx context.Context, name string, pgNum int) error
```

Waits until the pool's current, pending and target PG and PGP counts all equal
`pgNum` and the manager reports exactly `pgNum` PGs for it. Until then
`WaitForClean` can pass on the previous PG set. The pool is identified by the
native ID first observed; the wait fails if the name is bound to another pool.
Reaching the count does not imply clean PGs; call `WaitForClean` afterwards.

```golang
if err := cluster.SetPoolPGCount(ctx, "app-data", 16); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForPoolPGCount(ctx, "app-data", 16); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

#### RemovePool

```golang
func (c *Container) RemovePool(ctx context.Context, name string) error
```

Deletes a pool created by `CreatePool` or `WithPools`, together with its
`tc-<name>-replicated` or `tc-<name>-ec` CRUSH rule and EC profile, so that a
long-lived cluster can give each test its own pool without reaching PG limits.

- Every object in the pool is destroyed, including RBD images and namespaces.
  Stop clients and mirroring that use it first.
- Pools that Ceph or a service created (`.mgr`, RGW zone pools, CephFS pools)
  and pools in use by a CephFS filesystem are refused. Remove filesystems with
  `cephfs.Remove`, which also removes the pools their configuration created.
- Ownership is recognised by the pool's rule name. A pool moved to a
  `tc-<pool>-placement-<hash>` rule with `SetPoolPlacement` is refused; move it
  back to its original placement first. Placement rules are not removed.
- `mon_allow_pool_delete` is enabled in the central configuration only during the
  removal and then restored. It is refused while an `OverrideConfig` handle holds
  that key, and a config file that sets it to `false` blocks removal.
- A retry after an error removes whatever owned resources remain. An unknown name
  is an error.

#### PoolUsage

```golang
func (c *Container) PoolUsage(ctx context.Context, name string) (ceph.PoolUsageSnapshot, error)
```

Reads one pool's row of `ceph df detail` and checks the cluster FSID and the
pool's native ID and name before and after. `ceph.PoolUsageSnapshot` fields (all
sizes in bytes):

| Field | Meaning |
| --- | --- |
| `FSID`, `Name`, `ID` | Cluster and pool identity. |
| `StoredBytes` (`StoredDataBytes`, `StoredOMAPBytes`) | Estimated logical user data, DATA plus OMAP. |
| `AllocatedBytes` (`AllocatedDataBytes`, `AllocatedOMAPBytes`) | OSD allocation including replication or EC and allocation overhead, excluding the BlueStore database. |
| `Objects` | Logical object count. |
| `MaxAvailableBytes` | Estimated writable logical capacity. |
| `UsedRatio` | Fraction in `[0, 1]`. |

The values come from asynchronous PG statistics: they are neither a
read-after-write barrier nor a quota check. Poll with a deadline after writing.
A pool without a statistics row is reported as an error, not as empty. Do not
assume `AllocatedBytes` equals `StoredBytes` times the replica count.

```golang
usage, err := cluster.PoolUsage(ctx, "app-data")
if err != nil {
	t.Fatal(err)
}
t.Logf("objects=%d logical=%d allocated=%d", usage.Objects, usage.StoredBytes, usage.AllocatedBytes)
```

#### PoolPGs

```golang
func (c *Container) PoolPGs(ctx context.Context, name string) (ceph.PoolPGSnapshot, error)
```

Reads the reported state of every PG of one pool (`pg ls-by-pool`), bracketed by
two OSDMap reads that verify the FSID and the pool's name and ID. It works before
any OSD exists. `ceph.PoolPGSnapshot` contains `FSID`, `PoolBefore`/`PoolAfter`
(`PoolState` observed around the query; policies and epochs may differ),
`OSDMapEpochBefore`/`OSDMapEpochAfter`, the manager's `PGReady` flag and `PGs`.

Each `ceph.PGState` has `PGID`, the native `State` string, `Up` and `Acting` (in
native order), `UpPrimary` and `ActingPrimary` (`-1` means none),
`ReportedEpoch`, `ReportedSequence`, `MappingEpoch`, `LastEpochClean`,
`StatsInvalid` and `Stats`. `ceph.PGStats` holds signed counters: `Objects`,
`Bytes` (logical payload), `ObjectCopies`, `ObjectsDegraded`, `ObjectsMisplaced`
and `ObjectsUnfound`.

These are reports, not current mappings:

- A removed OSD can still appear, EC vectors can contain `2147483647`
  (`CRUSH_ITEM_NONE`) for missing shards, and `unknown` states, empty vectors and
  zero epochs are valid observations.
- `PGReady == true` does not mean that every PG is active, clean or reported.
- Counters arrive asynchronously, even with `StatsInvalid == false`.

```golang
snapshot, err := cluster.PoolPGs(ctx, "app-data")
if err != nil {
	t.Fatal(err)
}
for _, pg := range snapshot.PGs {
	t.Log(pg.PGID, pg.State, pg.Up, pg.Acting, pg.Stats.Objects)
}
```

### Container Methods: Clients and Cephx identities

#### WithClient

```golang
func (c *Container) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
```

Returns a customizer for your own container that connects it to the cluster and
copies the bootstrap `/etc/ceph/ceph.conf` and the admin keyring
`/etc/ceph/ceph.client.admin.keyring` into it. In bridge mode the container joins
the cluster's public network; in host mode it joins the host network.

- The container must already contain the Ceph clients it uses (CLI, librados,
  go-ceph binary, ...). `WithClient` installs nothing; `ceph.DefaultImage` or
  [ControlImage](#controlimage) work for CLI and Python clients.
- The admin key has full privileges on this test cluster. Use it only in trusted
  test containers; use [WithClientIdentity](#withclientidentity) for restricted
  clients.
- Combine it with [WithIdleEntrypoint](#withidleentrypoint) for a container in
  which you run commands with `Exec`.
- Options change the copied `ceph.conf`: `ceph.WithClientMessengerPolicy` sets
  the client's Messenger connection modes (see
  [WithMessengerPolicy](#withmessengerpolicy)).
- You own the container. Terminate it before the cluster (register its cleanup
  after the cluster's).

Ceph clients connect directly to the OSD addresses that the MONs advertise. In
bridge mode those are container addresses, so a process on the Docker host
(especially on macOS or Windows) cannot reach RADOS, RBD or CephFS through the
MON's mapped port alone. Run the client in a `WithClient` container, or use
[WithHostNetwork](#withhostnetwork) on Linux.

#### WithClientIdentity

```golang
func (c *Container) WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
```

Like [WithClient](#withclient), but copies only the keyring of a restricted
identity created by [CreateClient](#createclient). The container gets
`/etc/ceph/ceph.conf` (with the current MON addresses and a `[client.<id>]`
section pointing to the keyring) and the keyring at
`/etc/ceph/ceph.client.<id>.keyring` (`client.KeyringPath()`). The client must
select its identity, for example `rados --id reader ...`,
`CEPH_ARGS="--id reader"`, librados `rados_create(&cluster, "reader")` or go-ceph
`rados.NewConnWithUser("reader")`. Descriptors from another cluster, incomplete
creations and revoked identities are rejected, and so are descriptors whose key
[ReplaceClientKey](#replaceclientkey) replaced, whose identity
[RevokeClient](#revokeclient) removed until its
`Restore`, or whose identity has a change that is not resolved yet. You own the container. The options are those of
[WithClient](#withclient).

#### CreateClient

```golang
func (c *Container) CreateClient(ctx context.Context, name string, caps ceph.ClientCaps, opts ...ceph.ClientOption) (*ceph.ClientConfig, error)
```

Creates a fresh Cephx identity with exactly the given capabilities. `name` is an
ID or `client.ID` using letters, digits, dots, underscores or hyphens. An existing
identity is rejected before any change; its key and caps are never updated.

`ceph.ClientCaps` has one field per service, each a native capability expression
passed as a separate CLI argument; empty grants nothing for that service:

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=app-data namespace=tenant-a",
})
```

| Field | Service |
| --- | --- |
| `Mon` | monitors |
| `OSD` | OSDs (RADOS data) |
| `MGR` | managers |
| `MDS` | CephFS metadata servers |

Ceph validates the syntax. A non-nil result returned with an error identifies an
attempted or partial creation; inspect auth state or terminate the cluster rather
than retrying with broader caps. The returned `*ceph.ClientConfig` keeps the
credentials private; see [ClientConfig](#clientconfig).

The key has the cluster's [WithAuth](#withauth) `KeyType`, or the release
default without one. The `ceph.ClientOption` `ceph.WithClientKeyType` selects
another type for this identity, for example to check that the MONs reject a key type they do not
allow:

```golang
legacy, err := cluster.CreateClient(ctx, "legacy", ceph.ClientCaps{Mon: "allow r"},
	ceph.WithClientKeyType(ceph.CipherAES))
```

The identity is created whether or not the MONs allow its type; whether it can
authenticate depends on the MonMap's `auth_allowed_ciphers`. `aes256k` needs a
control image whose `ceph-authtool` has `--key-type` (see
[WithAuth](#withauth)); without it the call fails before any change.

#### ClientCapabilities

```golang
func (c *Container) ClientCapabilities(ctx context.Context, client *ceph.ClientConfig) (ceph.ClientCaps, error)
```

Returns the current capabilities of an identity created by this cluster, after
verifying that its native key is still the one that was created. It does not
return secrets.

#### UpdateClientCaps

```golang
func (c *Container) UpdateClientCaps(ctx context.Context, client *ceph.ClientConfig, caps ceph.ClientCaps) error
```

Replaces all capabilities of an owned identity while keeping its key. Services
omitted from `caps` lose their previous capabilities. Sessions that already hold
tickets may keep the previous permissions for a while; verify the change with a
fresh client connection. The change is fenced like the import of
[ReplaceClientKey](#replaceclientkey): a failed or canceled call reports what
the MONs hold once nothing can change it any more, and an outcome that stays
uncertain is resolved by the next operation on the identity.

#### DeleteClient

```golang
func (c *Container) DeleteClient(ctx context.Context, client *ceph.ClientConfig) error
```

Revokes an identity created by this cluster, without deleting data. It refuses if
the native key was replaced externally. A repeated call after success is a no-op.
Established sessions may keep their tickets; test revocation with a new
connection. Credentials already copied elsewhere are not erased. The removal is
fenced like the import of [ReplaceClientKey](#replaceclientkey).

#### ReplaceClientKey

```golang
func (c *Container) ReplaceClientKey(ctx context.Context, client *ceph.ClientConfig, opts ...ceph.ClientOption) (*ceph.ClientConfig, error)
```

Gives an identity created by this cluster a fresh key and keeps its name and
its exact capabilities. The change is one `ceph auth import` of the identity's
own keyring with the new key, so the identity never disappears from the MONs.
The new key has the type of the old one; `ceph.WithClientKeyType` selects
another (see [CreateClient](#createclient)). The identity needs at least one
capability, because Ceph refuses to import an entry without any; an identity
without capabilities is refused before any change.

```golang
renewed, err := cluster.ReplaceClientKey(ctx, client)
```

The call returns a new descriptor for the new key and leaves the old one
stale:

- The old descriptor's `ConnectionConfig` still returns the old credentials,
  so a test can check that they are rejected.
- Every other operation with the old descriptor fails:
  [WithClientIdentity](#withclientidentity),
  [ClientCapabilities](#clientcapabilities),
  [UpdateClientCaps](#updateclientcaps) and [DeleteClient](#deleteclient).
  Use the returned descriptor for them.
- `ReplaceClientKey` with the old descriptor again returns the same new
  descriptor without another change, as long as that descriptor is still the
  identity's current one.
- Containers attached with the old descriptor keep the old keyring.
- Copies of the old descriptor go stale as well. The cluster records the
  current key of each identity it created, and every operation refuses a
  descriptor with another key or of a deleted identity.

The old key behaves as follows on the official images:

- A fresh connection with the old key fails at once with `EACCES`.
- A client that authenticated before keeps its session until it renews its
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](#withauth)
  `MonTicketTTL`). The MONs then reject the renewal (`_finish_auth -13` in its
  `debug_monc` log).
- What follows the rejection differs. On Tentacle 20.2.4 the client loses its
  MON session, and librados does not authenticate again on its own. On Squid
  19.2.5 the MON session that is already open keeps answering, and only the
  renewals keep failing.
- MGR and OSD sessions that the client already holds keep working in both.
- Fresh MON, MGR and OSD clients with the new key work at once, also when it
  has another key type than the old one.

The identity must still have the descriptor's key. An identity whose key was
changed outside the module is refused before any change, and external auth
changes must not race the call.

The import is fenced like the MON commands of
[RotateDaemonKey](#rotatedaemonkey): the command runs under the supervisor,
and after it the call settles the MONs and reads the outcome. So a failed or
canceled call reports what the MONs hold once nothing can change it any more:
either the new key, in which case the call returns the new descriptor without
an error, or the old one. When settling itself fails, the outcome stays open:

- The call returns a descriptor of the attempted key that stays unusable,
  with an error saying so. A returned descriptor never changes afterwards.
- The next operation on the identity settles first and then resolves the
  outcome. `ReplaceClientKey` with the old descriptor returns a usable
  descriptor of the attempted key once the import turns out to have applied,
  and replaces the key anew once it turns out not to have.
- Until then, [WithClientIdentity](#withclientidentity) refuses both
  descriptors.

[UpdateClientCaps](#updateclientcaps) and [DeleteClient](#deleteclient) are
fenced the same way, so none of these operations races a command of another
one that may still apply. The CephFS subvolume grants of the
[cephfs](cephfs.md) package settle these commands before they change a
client. Their own commands go through the MGR, though, which this settling
cannot fence, and the module cannot tell when a failed one has ended: the
volumes module may still create or change the identity later. Do not replace
or revoke a client after a failed `AuthorizeSubvolume` or
`DeauthorizeSubvolume` for it. A `SubvolumeAuthorizedClients` listing shows
only the state at that moment.

#### RevokeClient

```golang
func (c *Container) RevokeClient(ctx context.Context, client *ceph.ClientConfig) (*ceph.ClientRevocationHandle, error)
```

Removes an identity created by this cluster from the MONs until `Restore`
puts back exactly its key and its capabilities, so a recovery test can watch
the rejection of a client and then its return with the same credentials. The
identity keeps its name and key type, and the descriptor is not recreated.

```golang
revocation, err := cluster.RevokeClient(ctx, client)
// ... check that the client is rejected ...
err = revocation.Restore(ctx)
```

While the identity is revoked:

- The descriptor's `ConnectionConfig` keeps returning its credentials, which
  work again after `Restore`.
- [WithClientIdentity](#withclientidentity),
  [ClientCapabilities](#clientcapabilities),
  [UpdateClientCaps](#updateclientcaps), [ReplaceClientKey](#replaceclientkey)
  and another `RevokeClient` refuse the descriptor.
- [CreateClient](#createclient) refuses the name.
- [DeleteClient](#deleteclient) makes the removal permanent, after which
  `Restore` fails.

The client behaves as follows on the official images:

- A fresh connection fails at once with `EACCES`.
- A client that authenticated before keeps its sessions until it renews its
  tickets, within `auth_mon_ticket_ttl` ([WithAuth](#withauth)
  `MonTicketTTL`). The MONs then reject the renewal, and the client keeps the
  MGR and OSD sessions it already holds.
- On Tentacle 20.2.4 the client also loses its MON session, and librados does
  not authenticate again on its own. On Squid 19.2.5 the open MON session
  keeps answering.
- After `Restore`, an open session renews its tickets only while the MONs
  still keep the rotating secret of its last auth ticket. They replace that
  secret every `MonTicketTTL` and keep a few generations. After a longer
  revocation, the MONs refuse every renewal of that session and log
  `attempt to reclaim global_id ... using bad ticket`, because the cluster
  sets `auth_allow_insecure_global_id_reclaim = false`. A new connection with
  the same credentials authenticates once `Restore` returns, so a recovery
  test reconnects its client instead of waiting for a renewal.

`Restore` checks what the MONs hold first:

- An identity that already has the original key and capabilities counts as
  restored, so a repeated `Restore` succeeds.
- An identity that was created again with another key or other capabilities
  is refused and never overwritten.
- A `Restore` of an earlier revocation does not undo a later one.

The revocation belongs to the identity, not to one descriptor: copies of the
descriptor are refused the same way, and `DeleteClient` through any
descriptor of the revoked key makes the removal permanent.

The removal and the restoration are fenced like the import of
[ReplaceClientKey](#replaceclientkey). A failed or canceled call settles the
MONs and reports what they hold:

- A removal that did not apply returns no handle, and the descriptor stays
  usable.
- When the outcome stays uncertain, `RevokeClient` returns the
  handle with the error, and `Restore` settles first and then restores.

The cluster needs no `Restore` before `Terminate`. The handle's `String` and
`GoString` never include credentials.

#### RotateDaemonKey

```golang
func (c *Container) RotateDaemonKey(ctx context.Context, daemon string) error
```

Replaces the Cephx key of a running MGR or OSD that the cluster owns, without a
restart, the way cephadm rotates daemon keys. `daemon` is `mgr.<name>` or
`osd.<id>`.

1. `ceph auth get-or-create-pending` creates the new key on the MONs.
2. The daemon's keyring files in its container get the new key, so a later
   restart authenticates with it.
3. `ceph tell <daemon> rotate-key` hands the key to the running daemon.
4. `ceph auth commit-pending` makes it the entity's only key.

```golang
err := cluster.RotateDaemonKey(ctx, "osd.0")
key, err := cluster.Ceph(ctx, "auth", "get-key", "osd.0") // the new key
```

- The daemon keeps running and answering throughout.
- Without step 2, a restarted daemon would read its old key and fail to start;
  `ceph tell <daemon> rotate-key` changes only the running process.
- MONs share the `mon.` key and are refused, as are stopped, paused and
  unknown daemons; nothing changes in these cases.
- Until step 4, the MONs accept both keys, so the running daemon and a
  restarted one can always authenticate. The MONs also commit the pending key
  on their own as soon as the daemon authenticates with it, after a restart or
  a handoff. Failures keep that property:

  | Failure | State afterwards | Recovery |
  | --- | --- | --- |
  | Before step 3, including cancellation | The MONs keep the pending key, and each keyring file holds the old or the new key, so the daemon authenticates with either. Nothing is undone: undoing would race with a restart from a file that already holds the new key. If such a restart has already made the MONs commit the new key, the call returns nil instead. | Call `RotateDaemonKey` again: it finishes with the same key, unless a restart has made the MONs commit it meanwhile, which finishes the rotation. |
  | Step 3 | Unknown whether the daemon received the new key, and a `rotate-key` that was already sent may still reach it later. The keyring files hold the new key; both keys stay valid. | Call `RotateDaemonKey` again: it finishes with the same key. Until the daemon's container restarts, for example through `Stop` and `Start` on its `testcontainers.Container`, the cluster refuses to give that daemon any other key, because a late `rotate-key` would switch it back to this one. |
  | Step 4 | The daemon and its files hold the new key, which the MONs accept whether or not the commit applied. `RotateDaemonKey` settles the MONs (see below) and reads the keys: a commit that applied returns nil. | If the error says that the commit did not apply, call `RotateDaemonKey` again or run `ceph auth commit-pending <daemon>`. |

  A call that finds a pending key from an earlier one resumes with it and
  never clears it, because the daemon may already hold it.
- A call does not return while one of its commands may still change the keys
  or the keyring files, unless it records that command for the daemon; the
  next call then deals with it before it changes anything, and fails while it
  cannot:
  - Every `ceph` command runs under the supervisor that
    [AdminSocket](#adminsocket) uses. A command that the supervisor could not
    confirm as stopped is stopped by the next call.
  - Keyring files are written to a new file in the daemon container and moved
    into place with `mv` through a Docker exec, whose end is confirmed. Docker
    may complete a file copy after its request failed, but such a copy only
    creates the new file. An exec whose end is unknown is waited for by the
    next call. The daemon image needs `mv`, next to the `sh`, `cp` and `mkdir`
    that the daemon start scripts use.
  - The MON commands that change the keys (`auth get-or-create-pending` and
    `auth commit-pending`) are sent by a short Python script through
    librados, which first records the nonce by which the MONs list its
    client's sessions. Before a call returns, and before it reports the
    outcome of a failed command, it settles the MONs for these commands:
    1. It waits until no MON lists a session of those clients. TCP delivers
       a connection's data before its end, and a MON drops a command whose
       connection has closed before it handles it, so each command has then
       been handled or dropped.
    2. It adds and removes `client.tc-key-rotation-<uuid>`, an identity
       without capabilities, through every MON in turn. Each CLI contacts
       only its MON (`mon_host_override`), and its MON client log must show
       that it reached no other MON. MONs forward commands to the leader in
       the order they handle them, and the leader commits auth changes in
       order, so these changes come after every handled command.
    3. The quorum's election epoch and monmap must stay the same meanwhile.

    Settling needs every MON in the monmap to answer. A call that cannot
    settle records its commands, and the next call settles them first.
- How to observe keys and rotating service secrets from the daemon side is in
  [Running native commands](guides/native-commands.md#observing-cephx-keys-from-the-daemon-side).

#### ConnectionConfig

```golang
func (c *Container) ConnectionConfig() ([]byte, []byte, error)
```

Returns independent copies of the bootstrap `ceph.conf` and the admin keyring for
a native client that you start yourself, for example a test process using
go-ceph in host mode or a custom client implementation. The configuration
contains the addresses that the cluster advertises: container addresses in bridge
mode, host addresses (with final MON ports) in host mode. The keyring grants full
privileges. It fails after termination or before bootstrap completed.

```golang
config, keyring, err := cluster.ConnectionConfig()
if err != nil {
	t.Fatal(err)
}
dir := t.TempDir()
confPath := filepath.Join(dir, "ceph.conf")
keyringPath := filepath.Join(dir, "ceph.client.admin.keyring")
if err := os.WriteFile(confPath, config, 0o644); err != nil {
	t.Fatal(err)
}
if err := os.WriteFile(keyringPath, keyring, 0o600); err != nil {
	t.Fatal(err)
}
// Point the client at confPath and keyringPath (for example librados
// conf_read_file, plus "keyring" = keyringPath).
```

For a restricted identity use `ClientConfig.ConnectionConfig()` instead.

#### ConnectionConfigContext

```golang
func (c *Container) ConnectionConfigContext(ctx context.Context) ([]byte, []byte, error)
```

Like [ConnectionConfig](#connectionconfig), but bounds the wait for a concurrent
topology change or configuration update by `ctx`.

#### RefreshClientMonitorConfig

```golang
func (c *Container) RefreshClientMonitorConfig(ctx context.Context, client testcontainers.Container) error
```

Updates only the global `mon_host` entry in `/etc/ceph/ceph.conf` of a container
you own (attached with `WithClient` or `WithClientIdentity`) to the live quorum's
addresses. Use it after replacing monitors: running sessions follow the monmap,
but a restarted client reads the file. The client's FSID must match this cluster
and its live quorum. It works on running and stopped containers through Docker
file copies and never starts, stops or adopts the container. Keyrings, other
settings and `mon_host` overrides in other sections are kept. Successful writes
stay on partial failure, and a retry with a fresh context skips files that are
already current. Do not edit the file concurrently.

### Container Methods: Configuration

#### OverrideConfig

```golang
func (c *Container) OverrideConfig(ctx context.Context, setting ceph.ConfigSetting) (*ceph.ConfigOverrideHandle, error)
```

Sets one entry in the MON configuration database (`ceph config set`) and records
the native normalized value. `ceph.ConfigSetting` has `Section`, `Mask`, `Name`
and `Value`:

- `Section` is `global`, a daemon type (`mon`, `mgr`, `osd`, `mds`, `client`) or
  one identity such as `osd.0` or `mds.a`.
- `Mask` is optional: `class:ssd`, `host:node-a` or `host:node-a/class:ssd`.
- `Name` can be any option, including MGR module keys such as
  `mgr/volumes/max_concurrent_clones` or `mgr/prometheus/server_addr`.

`Restore` returns the exact entry to its prior value, or removes it if it was
absent, instead of pinning the inherited or default value. Overlapping handles for
the same section, mask and name are refused. See the
[handle contract](#container-methods) for errors and retries.

```golang
change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{
	Section: "osd", Name: "osd_max_backfills", Value: "1",
})
if change != nil {
	// A failed apply can still have persisted; keep the handle.
	defer func() {
		if err := change.Restore(context.Background()); err != nil {
			t.Error(err)
		}
	}()
}
if err != nil {
	t.Fatal(err)
}
```

The helper verifies the stored value, not its effect:

- Local `ceph.conf` entries (including [WithConfigFile](#withconfigfile)),
  command-line arguments and runtime overrides take precedence over the database.
- Some options need a daemon restart; for example `osd_op_queue` only applies when
  an OSD starts.
- Check the effective value of a running daemon with
  `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_max_backfills")`.
- With [MessengerV2Secure](#withmessengermode), the Messenger mode and bind options
  are refused; with `MessengerV2Only`, the `ms_bind_msgr1` and `ms_bind_msgr2`
  options.

Configuration output can contain secrets; log only selected fields. Typical uses:
lowering thresholds such as `osd_op_complaint_time` (when `SLOW_OPS` is
reported), enabling Messenger fault injection (`ms_blackhole_client`,
`ms_inject_delay_*`, see [Fault injection](guides/fault-injection.md)), and
preparing OSDs before they start.

#### Configuration

```golang
func (c *Container) Configuration(ctx context.Context) ([]ceph.ConfigEntry, error)
```

Lists the exact entries stored in the central configuration database, including
masked entries. Each `ceph.ConfigEntry` has `Section`, `Mask`, `Name`, `Value`,
`Level` and `CanUpdateAtRuntime`. These are stored entries, not inherited or
effective values; use `ceph config show <daemon> <option>` for a daemon's
effective value. Entries can contain sensitive values.

#### OverrideDebugLevel

```golang
func (c *Container) OverrideDebugLevel(ctx context.Context, setting ceph.DebugLevelSetting) (*ceph.DebugLevelOverrideHandle, error)
```

Opens one runtime debug level window on one running OSD that the cluster owns.
`ceph.DebugLevelSetting` has `Daemon` (such as `osd.0`), `Name` (`debug_ms` or
`debug_osd`) and `Level` (`N` or `N/M`, from 0 to 99). Other roles and options
are refused; neither verbose levels nor all daemons are ever selected
automatically.

The window changes the daemon's runtime value through its admin socket, as
[AdminSocket](#adminsocket) reaches it, and never the central configuration
database; [OverrideConfig](#overrideconfig) is the stored-entry model. A daemon
restart drops a runtime value. The call reads the effective prior level, sets
the requested level and verifies the normalized readback, so `20` reads back as
`20/20`. The window is bound to the OSD container's generation (container ID and
start time). Only one window per daemon and option is admitted at a time. Each
set is issued only while the OSD still runs that generation, and the cluster's
own topology operations, such as `RemoveOSD`, `DetachOSD` and `ReopenOSD`, wait
until the set has ended.

`DebugLevelOverrideHandle.Restore` first checks the same generation and that
the daemon still shows the window's own value, then sets the prior level and
verifies it:

- A prior level that is already in place counts as restored.
- Any other value is an outside edit; it is preserved, not overwritten.
- A stopped, restarted, replaced or removed OSD, or a terminated cluster, is not
  restored: the window's process is gone.
- A paused OSD, an unavailable socket or an unverified readback leaves the
  window open; a later `Restore` reconciles it first.
- A set of the window whose end could not be confirmed may still apply. The
  window keeps it open as unconfirmed, and a later `Restore` first confirms that
  the set has stopped before it reads, sets or releases anything. Until then
  `RemoveOSD` and `DetachOSD` first stop that set or refuse, so it cannot reach
  a replacement OSD through the shared socket path.

The generation is checked again after each read, so a replacement during a read
is reported as a changed target instead of a level to restore.

A set or readback that fails returns a non-nil handle with the error, because a
lost reply can still have applied. Readbacks after a set use a fresh context
bounded by [WithStartupTimeout](#withstartuptimeout).

`DebugLevelOverrideHandle.Receipt` returns a JSON-serializable
`ceph.DebugLevelReceipt`: daemon, option, generation, requested text, prior,
applied and restored readbacks, the current `ceph.DebugLevelState` and every
`ceph.DebugLevelTransition` with its time, readback and error. The states are
`DebugLevelPriorRead`, `DebugLevelApplyAttempted`, then `DebugLevelApplied` or
`DebugLevelApplyUncertain`; `Restore` records `DebugLevelRestoreAttempted`,
then `DebugLevelRestored`, `DebugLevelConflict`, `DebugLevelTargetChanged` or
`DebugLevelUnconfirmed`. Restored, conflict and target-changed are terminal and
release the admission; `Restore` then returns nil or the same error again.

With [TerminateOnCleanup](#terminateoncleanup), register the removal of clients
first and the restoration afterward. The cleanups run in reverse order after the
failure archive, so a known failure is archived while the level is still raised,
and the level is restored before the clients are removed. A restoration that is
the first failure triggers the `after-cleanup` archive, and Terminate still
runs:

```golang
window, err := cluster.OverrideDebugLevel(ctx, ceph.DebugLevelSetting{
	Daemon: "osd.0", Name: "debug_osd", Level: "20",
})
if window != nil {
	finalization.TrackDebugLevel(window) // optional: receipts in the failure manifest
	finalization.Cleanup("restore osd.0 debug_osd", window.Restore)
}
if err != nil {
	t.Fatal(err)
}
```

The readbacks around each set are not an atomic compare-and-swap. Do not change
the same daemon and option elsewhere while a window is open; an outside write of
the window's own value cannot be told apart from it. A restart of the OSD
container from outside the cluster after the last check is not held off.
Verbose levels cost log
volume and can change timing; levels and durations for particular investigations
are not established yet.

### Container Methods: MGR modules and services

#### MGRModules

```golang
func (c *Container) MGRModules(ctx context.Context) ([]ceph.MGRModuleState, error)
```

Lists configured and available manager modules sorted by name. Each
`ceph.MGRModuleState` combines configured membership with the active manager's
module metadata: `Name`, `Enabled`, `AlwaysOn`, `ForceDisabled`, `Available`
(metadata present), `CanRun` (meaningful only when `Available`) and `ErrorString`
(Ceph's dependency or load diagnostic). `Enabled` and `CanRun` do not prove that
a module's commands are ready.

#### OverrideMGRModule

```golang
func (c *Container) OverrideMGRModule(ctx context.Context, name string, enabled bool) (*ceph.MGRModuleOverrideHandle, error)
```

Enables or disables one module and restores its original membership with
`Restore`. It refuses to enable a module whose metadata is missing or reports
that it cannot run (it never forces dependencies), to disable an always-on module,
and to disable `volumes` or `mirroring` while filesystems or mirror policies use
them. An enabled always-on module accepts only a no-op `true` request. A module
change can restart the active manager; the call waits for the module state to be
readable again. It verifies membership, not command readiness; probe the module
afterwards.

#### WaitMGRModuleReady

```golang
func (c *Container) WaitMGRModuleReady(ctx context.Context, name string) error
```

Waits until a module is enabled and can run, and its read-only commands succeed.
Only `rbd_support` (task and snapshot schedule lists) and `volumes`
(`fs volume ls`) are supported; for other modules probe your intended command.
Success does not mean that queued worker tasks have completed.

#### ManagerServices

```golang
func (c *Container) ManagerServices(ctx context.Context) (map[string]string, error)
```

Returns the service URLs that the active manager advertises in the MGR map,
keyed by module name (for example `prometheus` or `dashboard`). A module appears
only after it has started its listener; after a failover the URLs are the new
active manager's. URLs contain the address the module bound: a container address
reachable from `WithClient` containers in bridge mode, or the configured address
in host mode. They are not mapped Docker host ports. An empty map means no module
advertises a service. See
[Scrape the MGR Prometheus exporter](#scrape-the-mgr-prometheus-exporter).

#### ManagerMetrics

```golang
func (c *Container) ManagerMetrics(ctx context.Context) (ceph.ManagerMetrics, error)
```

Scrapes the active manager's `prometheus` module once and returns its
samples parsed from the Prometheus text exposition format. It never enables
the module: enable it with [OverrideMGRModule](#overridemgrmodule) and wait
until [ManagerServices](#managerservices) lists `prometheus`; until then the
call fails with `ErrInvalidRequest`. The scrape runs with `python3` in the
control container, at the address that `ManagerServices` advertises, and is
bounded to 30 seconds and 32 MiB. `TestManagerMetrics` checks it in bridge
mode; in host mode the module first needs its own address and port per
manager, as the recipe below shows. A failed fetch is a `*NativeFailure`, and a
standby's empty page or text that does not parse is an error.

| `ceph.ManagerMetrics` field or method | Meaning |
| --- | --- |
| `URL` | The scraped endpoint, the active manager's `…/metrics`. |
| `Types`, `Help` | Each metric family's `# TYPE` and `# HELP` text. |
| `Samples` | Every `ceph.MetricSample` (`Name`, `Labels`, `Value`, `Timestamp` in milliseconds or 0), in the module's order. `Value` may be `NaN` or ±Inf. |
| `Select(name, labels)` | The samples of `name` whose labels include all of `labels`; nil `labels` selects every sample of `name`. |
| `Value(name, labels)` | The value of the one sample that `Select` finds; none or several is an error. |

The module reports cluster-level metrics: pools (`ceph_pool_wr`,
`ceph_pool_objects`, `ceph_pool_stored`), OSD state (`ceph_osd_up`,
`ceph_osd_in`, `ceph_osd_metadata`), health and the MGRs. Since Reef it leaves
out each daemon's perf counters, such as `ceph_osd_op_w`, by default; the
module's `mgr/prometheus/exclude_perf_counters` option set to `false` adds
them back (see the
[Ceph documentation](https://docs.ceph.com/en/tentacle/mgr/prometheus/#ceph-daemon-performance-counters-metrics);
the module's tests do not cover that setting). Each daemon's own counters are
also in `cluster.AdminSocket(ctx, "osd.0", "perf", "dump")`.

```golang
metrics, err := cluster.ManagerMetrics(ctx)
if err != nil {
	t.Fatal(err)
}
writes, err := metrics.Value("ceph_pool_wr", map[string]string{"pool_id": "2"})
```

The pool counters come from the MGR's PG statistics, which OSDs report every
few seconds: poll until the expected value appears.

### Container Methods: Health and status

#### Status

```golang
func (c *Container) Status(ctx context.Context) (ceph.Status, error)
```

Runs `ceph status --format json` and decodes the subset used for readiness:
`FSID`, `Health.Status`, `MgrMap.Available`, `OSDMap.NumOSDs`/`NumUpOSDs`/
`NumInOSDs`, and `PGMap.NumPGs` with `PGsByState`. For other fields, run
`cluster.Ceph(ctx, "status", "--format", "json")` and decode what you need.

#### HealthDetails

```golang
func (c *Container) HealthDetails(ctx context.Context) (ceph.HealthSnapshot, error)
```

Reads `ceph health detail` from one control container, checking the cluster's
original FSID before and after. It works before any OSD or manager exists, makes
no changes and does not retry. On any failure, including cancellation after the
query, it returns a zero snapshot and an error without native output.

| Value | Meaning |
| --- | --- |
| `HealthSnapshot.FSID` | The cluster's original FSID. |
| `HealthSnapshot.Status` | `HEALTH_OK`, `HEALTH_WARN` or `HEALTH_ERR` for the worst unmuted check. |
| `HealthSnapshot.Checks` | Map keyed by the native, case-sensitive code (`OSD_DOWN`, `MGR_DOWN`, ...), including codes unknown to this module. |
| `HealthCheck.Severity` | The check's own severity, kept when it is muted. |
| `HealthCheck.Summary`, `Details` | Native summary and detail messages; Ceph may truncate details. |
| `HealthCheck.Count` | Native signed count; need not equal the number of details. |
| `HealthCheck.Muted` | Whether a stored mute covers this check. |
| `HealthSnapshot.Mutes` | Stored mutes (`HealthMute`), whether or not their check is present. |
| `HealthMute.Code`, `Summary`, `Count`, `Sticky` | Native values kept by the mute. |
| `HealthMute.ExpiresAt` | Ceph's TTL timestamp string, empty without TTL. Not compared with your clock. |

`HEALTH_OK` can include muted warnings, so assert on the code and its `Muted`
flag. A sticky mute stays after its check clears, and an expired or cleared
non-sticky mute can remain visible until the next MON tick. Messages can contain
pool names, paths or other application data; choose what you log.

Ceph raises many checks only after a delay, so poll with a deadline:

```golang
func waitForHealthCheck(ctx context.Context, cluster *ceph.Container, code string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		health, err := cluster.HealthDetails(ctx)
		if err == nil {
			if check, ok := health.Checks[code]; ok && !check.Muted {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("health check %s not reported: %w", code, errors.Join(ctx.Err(), err))
		case <-time.After(time.Second):
		}
	}
}
```

#### WaitForClean

```golang
func (c *Container) WaitForClean(ctx context.Context) error
```

Waits until a manager is available, the cluster's OSD count equals the owned OSD
count with all of them up and in, at least one PG exists, and every PG is exactly
`active+clean`. `HEALTH_WARN` alone does not block it. Call it after creating
pools and after topology changes, before client I/O.

- It never succeeds with an intentionally stopped or out OSD; use
  [WaitForPGClean](#waitforpgclean) then.
- PG states with extra flags (`active+clean+scrubbing`,
  `active+clean+inconsistent`) do not count as clean.
- OSDs registered outside the fixture make the count differ.
- After `SetPoolPGCount` or `SetPoolPlacement` it can pass before the new PG set
  is reported; see [WaitForPoolPGCount](#waitforpoolpgcount) and
  [PoolPGs](#poolpgs).
- A pool whose replica count exceeds the OSD count never becomes clean (for
  example size 2 with one OSD).

#### WaitForPGClean

```golang
func (c *Container) WaitForPGClean(ctx context.Context) error
```

Waits until every existing PG is exactly `active+clean`, without checking OSD
topology, so it works while an OSD is deliberately down or out. It requires at
least one PG and an available manager. It is a recovery barrier, not a statement
about health or daemon availability.

### Container Methods: Messenger mode

#### MessengerMode

```golang
func (c *Container) MessengerMode() ceph.MessengerMode
```

Returns the bootstrap policy selected with [WithMessengerMode](#withmessengermode)
(`ceph.MessengerDefault` for a nil container). It does not inspect effective
settings or live connections.

#### OverrideMessengerPolicy

```golang
func (c *Container) OverrideMessengerPolicy(ctx context.Context, policy ceph.MessengerPolicy) (*ceph.MessengerPolicyOverrideHandle, error)
```

Changes the connection modes that the running daemons accept from clients (see
[WithMessengerPolicy](#withmessengerpolicy) for the fields). Each non-nil field
becomes a [OverrideConfig](#overrideconfig) override of its option; nil fields
stay as they are.

```golang
override, err := cluster.OverrideMessengerPolicy(ctx, ceph.MessengerPolicy{
	Monitors: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
if override != nil {
	defer override.Restore(ctx)
}
```

- The daemons apply the change to new connections without a restart.
  Established connections keep their mode.
- `MessengerPolicyOverrideHandle.Restore` returns every option to its previous value,
  including the bootstrap policy. It is idempotent and can be retried.
- A non-nil result returned with an error holds the overrides that were made;
  restore it.
- A single daemon, such as only `osd.0`, is changed with `OverrideConfig` and
  the option in the table, with the daemon name as `Section`. It takes
  precedence over the daemon type's entry.
- A policy that selects no valid mode, and any policy on a `MessengerV2Secure`
  cluster, is refused with `ceph.ErrInvalidRequest` before any change.

### Container Methods: Object watchers and client fencing

#### ObjectWatchers

```golang
func (c *Container) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ceph.ObjectWatcher, error)
```

Lists the clients watching one RADOS object (`rados listwatchers`), including
clients outside the fixture. `namespace` is empty for the default namespace. Each
`ceph.ObjectWatcher` has `Address` (`IP:port/nonce`, the same form as
[BlocklistEntries](#blocklistentries)), `ClientID` and `Cookie`. A client that
stops answering keeps its watch until the OSD's `osd_client_watch_timeout`
expires (30 seconds by default); until then a notify on the object waits for it
and times out. Use this to assert that a stuck watcher is still registered or has
been removed. It reads native state only.

#### BlocklistEntries

```golang
func (c *Container) BlocklistEntries(ctx context.Context) ([]ceph.BlocklistEntry, error)
```

Lists the OSDMap blocklist: exact client addresses and CIDR ranges, including
entries created by other tools or by Ceph itself (for example when CephFS or RBD
evicts a client). Each `ceph.BlocklistEntry` has `Address`, `Range` (true for a
CIDR range) and `Until` (the MON's absolute expiration time). Listing grants no
ownership to remove an entry.

#### TemporaryBlocklist

```golang
func (c *Container) TemporaryBlocklist(ctx context.Context, address string, duration time.Duration) (*ceph.BlocklistOverride, error)
```

Fences one specific client session for `duration` (at least one second). The
address must be the session's exact `IP:port/nonce` with a nonzero nonce, so
other clients on the same host are unaffected. Get it from the client after it
connects (librados `rados_getaddrs`, go-ceph `Conn.GetAddrs`), from
[ObjectWatchers](#objectwatchers), from an RBD lock owner or from a CephFS
session. A v1/v2 address vector is accepted only when all entries denote the same
IP, port and nonce. Bare IPs, CIDR ranges, nonce zero and existing entries are
refused.

`Restore` removes only this entry (`Address()` returns it), after checking the
captured expiration; an entry renewed by someone else is refused, and natural
expiry counts as removed. After removal, OSDs admit the address again, but how the
session recovers depends on the client library; with librados both the existing
session and a new connection recover. See [Fence one client session](#fence-one-client-session).

### Container Methods: Full ratios

#### FullRatios

```golang
func (c *Container) FullRatios(ctx context.Context) (ceph.FullRatioSnapshot, error)
```

Reads the OSDMap's global `nearfull`, `backfillfull` and `full` ratios, checking
the original FSID before and after. `ceph.FullRatioSnapshot` has `FSID`, `Epoch`
(the whole OSDMap epoch, which unrelated changes also advance) and `Ratios`
(`ceph.FullRatios` with `NearFull`, `BackfillFull` and `Full` as fractions of
capacity). Values are Ceph's float32 values, so do not compare them with
float64 literals for equality. Zero, equal or misordered native values are
reported as they are. These ratios are OSDMap policy, separate from central
configuration and pool quotas, and do not say whether any OSD has reported
fullness.

#### OverrideFullRatios

For the next minor release, rename
`cluster.TemporaryFullRatios(ctx, ratios)` to
`cluster.OverrideFullRatios(ctx, ratios)`. The return type remains
`*ceph.FullRatiosOverride`; `Restore` and its ownership/retry semantics are
unchanged. There is no compatibility alias. Published releases keep their APIs.

```golang
func (c *Container) OverrideFullRatios(ctx context.Context, requested ceph.FullRatios) (*ceph.FullRatiosOverride, error)
```

This is a cluster policy override in `ceph`, even when lower thresholds are
used to induce FULL or ENOSPC in a test. It has no expiry, timer or implicit
scope-exit restoration. Cancelling the apply context does not undo committed
changes. Keep a non-nil handle even when apply returns an error, and explicitly
call `FullRatiosOverride.Restore` with a fresh context as needed, or terminate
the disposable cluster. `Terminate` does not reset individual thresholds.

Applies the three ratios and restores all three with `Restore`. Requested values
must be finite and satisfy `0 < NearFull < BackfillFull < Full <= 1` after float32
normalization (inputs that collapse to equal values are refused), and the current
map must also be ordered. The CLI changes one ratio per command; the helper orders
the commands so that every intermediate state stays ordered, so apply and restore
are not atomic. Only one handle per cluster is allowed, including no-op requests.

Low ratios reproduce full conditions with a small amount of existing data; the
module does not fill real devices. Success verifies the map, not OSD reports:
wait for `OSD_NEARFULL`, `OSD_BACKFILLFULL` or `OSD_FULL` with
[HealthDetails](#healthdetails). See
[Fill the cluster with low full ratios](#fill-the-cluster-with-low-full-ratios).

### Container Methods: Scrub inconsistencies

These methods reproduce a damaged replica so that monitoring, client behaviour on
an inconsistent PG, or repair automation can be tested without corrupting a disk
or stopping an OSD. Get an object's PG and acting set with
`cluster.Ceph(ctx, "osd", "map", pool, object, "--format", "json")` (fields
`pgid`, `acting` and `acting_primary`).

#### InjectObjectDataError

```golang
func (c *Container) InjectObjectDataError(ctx context.Context, pool, object string, osdID int) error
```

Makes one replica of an existing object on an owned OSD return a read error to
scrub and recovery, through Ceph's `injectdataerr` debug command. It needs
BlueStore OSDs and refuses a cluster that uses `ceph.OSDMemStore`. The OSD must be
in the object's current acting set, and the pool must be replicated (EC pools are
refused). The method enables `bluestore_debug_inject_read_err` on that daemon only,
at runtime, until it restarts; the setting affects only objects with an injected
error. The injection lives in OSD memory until the replica is rewritten (for
example by [RepairPG](#repairpg)) or the OSD restarts. Object names that are
empty, contain NUL or start with `-` are refused.

Observed behaviour on Ceph 20.2.4:

- An error on a non-primary replica does not affect client reads served by the
  primary. A deep scrub reports `read_error` on that shard, the PG becomes
  `active+clean+inconsistent`, and health reports `OSD_SCRUB_ERRORS` and
  `PG_DAMAGED`.
- An error on the primary does not make client reads fail, and a later deep scrub
  reports no inconsistency. Inject into a replica to leave scrub evidence.

#### DeepScrubPG

```golang
func (c *Container) DeepScrubPG(ctx context.Context, pgid string) error
```

Requests a deep scrub of one PG and waits until it reports a deep-scrub stamp
newer than before the request and is no longer scrubbing. Inconsistencies are
reported, not repaired. The `noscrub` or `nodeep-scrub` flags, or an unavailable
primary, keep the scrub from starting, and the wait then fails at the deadline.

#### PGInconsistencies

```golang
func (c *Container) PGInconsistencies(ctx context.Context, pgid string) ([]ceph.InconsistentObject, error)
```

Lists the objects that the last deep scrub of a PG found inconsistent
(`rados list-inconsistent-obj`). It does not start a scrub; an empty result means
that scrub found nothing. A PG whose results are no longer available returns an
error. Each `ceph.InconsistentObject` has `Name`, `Namespace`, object-level
`Errors`, `UnionShardErrors` (for example `read_error` or `data_digest_mismatch`)
and `Shards`. Each `ceph.InconsistentShard` has `OSD`, `Shard` (`-1` for
replicated pools), `Primary` and `Errors`.

#### RepairPG

```golang
func (c *Container) RepairPG(ctx context.Context, pgid string) error
```

Requests a repair of one PG and waits until it reports a newer deep-scrub stamp
and is no longer inconsistent. Repair rewrites bad replicas from an authoritative
copy; an object without one cannot be repaired, and the call then fails with the
PG still inconsistent.

### Container Methods: Fault injection

Daemon handles (`Monitors()`, `Managers()`, `OSDs()`, and the MDS and RGW handles
of the service packages) support `Stop` and `Start`: the process ends and peers
see the connection close. The dedicated operations in [`fault`](fault.md) add other failure shapes. For
Messenger-level delays and dropped messages (`ms_blackhole_*`,
`ms_inject_delay_*` through `OverrideConfig`), OSD flags, client blocklisting
and full ratios, see [Fault injection](guides/fault-injection.md).

#### Container pause faults

[`fault.Pause`](fault.md#pause) freezes a daemon or client
with the existing cluster's ownership and cleanup lifetime. Its
`*fault.PauseLease` handle resumes the process with `Resume`.

#### Network faults

Network fault functions and handles are in [`fault`](fault.md). Use
`fault.InterruptNetwork(ctx, cluster, target, plane)` and
`fault.ApplyNetworkProfile(ctx, cluster, target, delay)` with the existing
cluster; its `Terminate` restores cluster-bound faults.

### Container Methods: Networking

See [Host networking](guides/host-network.md) for the networking modes in detail.

#### NetworkName

```golang
func (c *Container) NetworkName() string
```

Returns the name of the cluster's public bridge, or `"host"` in host mode. Use it
to attach containers with other customizers, for example
`network.WithNetworkName(nil, cluster.NetworkName())`.

#### ClusterNetworkName

```golang
func (c *Container) ClusterNetworkName() string
```

Returns the OSD replication bridge created by
[WithSeparateClusterNetwork](#withseparateclusternetwork). With a single bridge it
returns `NetworkName()`; in host mode both planes share `"host"`.

#### HasSeparateClusterNetwork

```golang
func (c *Container) HasSeparateClusterNetwork() bool
```

Reports whether OSDs have a dedicated replication bridge.

#### UsesHostNetwork

```golang
func (c *Container) UsesHostNetwork() bool
```

Reports whether the cluster shares Docker's host network.

#### PublicAddress

```golang
func (c *Container) PublicAddress() string
```

Returns the IPv4 address advertised in host mode (`127.0.0.1` unless
[WithHostAddress](#withhostaddress) selected another); empty in bridge mode.

#### DaemonAddresses

```golang
func (c *Container) DaemonAddresses(ctx context.Context) (ceph.DaemonAddresses, error)
```

Reads the names and advertised addresses of the cluster's MONs, MGRs and OSDs
from the native maps. These are the addresses a client learns from the cluster,
so a test of a client implementation can compare what it decoded or connected
to with what Ceph advertises.

| Field | Source and meaning |
| --- | --- |
| `MonMapEpoch`, `Monitors` | monmap epoch; every member in rank order with its name and public address vector |
| `MgrMapEpoch`, `ActiveManager`, `ManagerAvailable`, `StandbyManagers` | MGRMap epoch; the active MGR's name and address vector (nil without an active MGR), whether it is available, and the standby names |
| `OSDMapEpoch`, `OSDs` | OSDMap epoch; every OSD in ID order with `Up`, `In` and its `Public`, `Cluster`, `HeartbeatFront` and `HeartbeatBack` address vectors |

Each address is a `ceph.EntityAddress` with `Type` (`v2`, `v1` or `any`),
`Addr` as Ceph prints it (`172.20.0.3:6800`, `[fd00::3]:6800`) and `Nonce`, the
instance nonce that MONs leave at 0. `ceph.DaemonAddress` pairs a MON or MGR
name with its vector, and `ceph.OSDAddress` holds one OSD's entry.

```golang
addresses, err := cluster.DaemonAddresses(ctx)
for _, mon := range addresses.Monitors {
	fmt.Println(mon.Name, mon.Addrs) // a [{v2 172.20.0.2:3300 0} {v1 172.20.0.2:6789 0}]
}
```

- In bridge mode the addresses are the daemon containers' addresses on the
  fixture network; in host mode they are [PublicAddress](#publicaddress) with
  the allocated ports.
- Standby MGRs advertise no address in the MGRMap. A MGR that has just become
  active advertises its address vector only once it is available, so wait for
  `ManagerAvailable` after a failover.
- An OSD that is down keeps the addresses of its last boot; one that never
  booted has empty vectors.
- The three maps are read one after another, not as one snapshot; compare the
  epochs when the order matters.

### Container Methods: Admin sockets

#### AdminSocket

```golang
func (c *Container) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error)
```

Runs one command on the admin socket of a MON, MGR or OSD that the cluster
owns and returns its output. `daemon` is the Ceph daemon name, such as `mon.a`,
`mgr.b` or `osd.0`; `args` are the command words. `help` lists a daemon's
commands.

The admin socket bypasses the Messenger and Cephx: the command reaches the
daemon through a local Unix socket. Use it to check what a daemon has applied
independently of the client under test, for example:

| Command | Shows |
| --- | --- |
| `version` | the daemon's build version, release name and release type |
| `status` (OSD) | `newest_map` and `oldest_map`: the OSDMap epochs the OSD holds, as JSON strings |
| `mon_status` (MON) | the MON's own name, rank, state and monmap, including `auth_epoch` on Tentacle |
| `config get <option>` | the value the running daemon uses, after local files and the central database |
| `perf dump` | the daemon's performance counters |

```golang
// Wait until osd.0 has applied the OSDMap epoch the cluster reports.
out, err := cluster.AdminSocket(ctx, "osd.0", "status")
var status struct {
	NewestMap string `json:"newest_map"`
}
err = json.Unmarshal(out, &status)
```

Every MON, MGR, OSD and control container shares one run directory, a Docker
volume mounted at `/var/run/ceph`, and the command runs in the control
container. It therefore works with OSD role images that have no `ceph` CLI. The
volume belongs to the cluster and `Terminate` removes it.

- The daemon must be running. A stopped or removed daemon returns an error. A
  daemon that has just started can answer before it registers all of its
  commands (`no valid command found`); poll with a deadline.
- A paused daemon (see [fault.Pause](fault.md#pause)) accepts the
  connection but does not answer, so the call waits until the context ends.
- A nonzero exit returns the command's output together with an error.
- The call is bounded by the context and by
  [WithStartupTimeout](#withstartuptimeout), and runs no command for a daemon
  the cluster does not own. When the context ends first, the call stops its
  command in the control container before it returns, including a command that
  Docker has accepted but not yet started, so canceled calls leave no process
  behind and other calls keep running. An effect that the daemon has already
  applied stays.
- MDS and RGW daemons are not included; reach them with `ceph tell` (see
  [Running native commands](guides/native-commands.md#daemon-admin-sockets)).

### Container Methods: Diagnostics

#### CollectDiagnostics

```golang
func (c *Container) CollectDiagnostics(ctx context.Context, config ceph.DiagnosticsConfig) (*ceph.DiagnosticsReport, error)
```

Collects a bounded, read-only snapshot for failure analysis: the state and
selected metadata of every tracked MON, MGR, OSD, MDS, RGW and control container,
network metadata, log tails, and fixed Ceph queries (`version`, `status`,
`health`, `pgs`, `quorum`, `monmap`, `mgrmap`, `osdmap`, `fsmap`). It works on
partially bootstrapped clusters and on stopped or removed containers. It never
starts, stops or reconfigures anything, and it never collects files, environment
variables, keyrings, the auth database, mount sources or arbitrary commands.

[TerminateOnCleanup](#terminateoncleanup) with `Archive` calls it, and
[CollectCrashMetadata](#collectcrashmetadata), when a test fails, and writes
both reports with a manifest. Otherwise it is not called automatically. Call
it before cleanup, with its own short context (the test context may already be
canceled):

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})
	if report != nil {
		// Save the report even when err is set; it keeps the successful parts.
		data, _ := json.MarshalIndent(report, "", "  ")
		name := strings.ReplaceAll(t.Name(), "/", "_") + "-ceph-diagnostics.json"
		path := filepath.Join(os.TempDir(), name) // or your CI artifact directory
		if writeErr := os.WriteFile(path, data, 0o644); writeErr == nil {
			t.Log("Ceph diagnostics saved to", path)
		}
	}
	if err != nil {
		t.Log("diagnostics incomplete:", err)
	}
})
```

Register this cleanup after the cluster's, so it runs before termination.

`ceph.DiagnosticsConfig` fields (zero selects the default):

| Field | Default | Limit | Meaning |
| --- | --- | --- | --- |
| `Timeout` | 1 minute | 5 minutes | Whole collection; the caller's deadline also applies. |
| `OperationTimeout` | 10 seconds | 1 minute | Each query. |
| `MaxOutputBytes` | 64 KiB | 1 MiB | Each artifact's `Data` and `Error`. |
| `LogTail` | 1000 lines | 10000 | Log lines per container. |
| `Concurrency` | 4 | 8 | Collection workers. |
| `MaxContainers` | 128 | 128 | Distinct containers; extra ones are recorded as omitted. |
| `AdditionalContainers` | none | 64 | `ceph.DiagnosticsContainer` values (`Role`, `Name`, `Container`) for clients or mirror daemons you own; they are only inspected. |
| `RedactValues` | none | 128 values of up to 4096 bytes | Extra secrets to mask. |

Invalid limits are rejected before collection, without a report.

`ceph.DiagnosticsReport` is JSON-serializable (snake_case field names) with
`SchemaVersion` (1), `StartedAt`, `FinishedAt`, `HostNetwork`, `Closed` (the
cluster was already terminated), `Complete` and `Artifacts`. Each
`ceph.DiagnosticArtifact` has `Kind` (`inventory`, `container-inspect`,
`container-logs`, `network-inspect` or `ceph`), `Name`, `Role`, `ContainerID`,
`Data` (text; JSON for untruncated inspect results), `Truncated` and `Error`.

- If some queries fail, the report and an aggregate error are both returned, so
  save the report before checking the error. Truncation alone is not an error.
- `Complete` means that every observation succeeded without truncation, not that
  Ceph is healthy.
- Observations are not taken at one atomic moment.
- A `ceph` artifact holds the query's stdout followed by its stderr. A query
  that fails or times out keeps the output it produced.
- When `ctx` ends, every running query is stopped inside the control container
  before `CollectDiagnostics` returns, so a canceled collection leaves no Ceph
  CLI behind.
- Known admin and RGW secrets, every client key the cluster generated (also
  keys that [ReplaceClientKey](#replaceclientkey) replaced or only attempted),
  `RedactValues` and named secret patterns are masked, but redaction is best
  effort; review arbitrary application logs before sharing a report.

#### CollectCrashMetadata

```golang
func (c *Container) CollectCrashMetadata(ctx context.Context, config ceph.CrashMetadataConfig) (*ceph.CrashMetadataReport, error)
```

Collects the crash reports that Ceph daemons wrote, for failure analysis. When a
daemon dies on a fatal signal or a failed assertion, Ceph writes a report
directory into its crash directory, `ceph.CrashDir` (`/var/lib/ceph/crash`).
The directory holds a `meta` JSON file with the daemon, version, time,
assertion and backtrace, a `log` file of recent events, and a `done` marker.
`CollectCrashMetadata` reads only the `meta` files, of every MON, MGR, OSD, MDS
and RGW container and of `config.AdditionalContainers`. It is separate from
`CollectDiagnostics` and is not called automatically.

```golang
t.Cleanup(func() {
	if !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	report, err := cluster.CollectCrashMetadata(ctx, ceph.CrashMetadataConfig{})
	if report != nil {
		data, _ := json.MarshalIndent(report, "", "  ")
		t.Logf("Ceph crash metadata:\n%s", data)
	}
	if err != nil {
		t.Log("crash metadata incomplete:", err)
	}
})
```

- The files come through Docker's archive API, so stopped containers work,
  and so do a partially bootstrapped or terminated cluster. Nothing is started,
  stopped, reconfigured or executed in the containers, and nothing is extracted
  to disk. Caller-owned containers are only read.
- Only regular files named `meta` in a report directory, or in `posted/`, are
  read. Crash logs, process memory dumps and every other file are skipped and
  counted in `IgnoredEntries`. Links, paths outside the directory, files over
  the size limit and files that are not one JSON object are listed in
  `Omitted` with the reason.
- Each record keeps the known Ceph crash fields in `Fields`: `crash_id`,
  `timestamp`, `process_name`, `entity_name`, `ceph_version`, the `utsname_*` and
  `os_*` fields, `backtrace`, the `assert_*` and `io_error*` fields,
  `archived` and `stack_sig`. Other fields are named in `OmittedFields` without
  their values. Strings are redacted like `CollectDiagnostics`, but host names
  and paths stay; review a report before sharing it.
- Each `ceph.CrashRecord` has `CrashID`, `Path`, the convenience fields
  `Entity`, `Process`, `CephVersion` and `Timestamp`, `Done`, which reports
  Ceph's `done` marker that completes a report, `Posted` for a report in
  `posted/`, `Fields` and `OmittedFields`. Each `ceph.CrashOmission` has the
  file's `Path` and the `Reason`.

`ceph.CrashMetadataConfig` fields (zero selects the default):

| Field | Default | Limit | Meaning |
| --- | --- | --- | --- |
| `Timeout` | 1 minute | 5 minutes | Whole collection; the caller's deadline also applies. |
| `OperationTimeout` | 20 seconds | 1 minute | Each container. |
| `MaxArchiveBytes` | 16 MiB | 256 MiB | Archive bytes read per container, crash logs included. |
| `MaxEntries` | 4096 | 65536 | Archive entries per container. |
| `MaxRecordBytes` | 64 KiB | 1 MiB | Each `meta` file. |
| `MaxRecords` | 64 | 1024 | Records per container; later ones are listed in `Omitted`. |
| `AdditionalContainers` | none | 64 | Caller-owned containers, such as mirror daemons, to read as well. Each one is read whatever its `Role` and `Name`; the cluster's separate control container, which runs no daemon, is read only when it is listed here. |
| `RedactValues` | none | 128 values of up to 4096 bytes | Extra secrets to mask. |

Each `ceph.CrashMetadataContainer` in `ceph.CrashMetadataReport.Containers` has
the container's role, name, ID, image ID and Docker state (`State`,
`ExitCode`, `OOMKilled`, `StartedAt`, `FinishedAt`), for correlation with the
records. Its `Outcome` (a `ceph.CrashOutcome`) is one of:

| Outcome | Meaning |
| --- | --- |
| `CrashRecordsFound` | The directory was read and held at least one accepted record. |
| `CrashEntriesRejected` | The directory was read, but every `meta` file was rejected; see `Omitted`. |
| `CrashDirectoryEmpty` | The directory was read and held no report. This does not prove that the daemon never crashed. |
| `CrashDirectoryAbsent` | The container has no crash directory. |
| `CrashContainerUnavailable` | The container was never created, or was removed with its reports. |
| `CrashReadFailed` | Docker or the container implementation failed, for example after the context ended; see `Error`. |
| `CrashArchiveIncomplete` | The archive was malformed, cut off or over a limit; `Records` holds what was read before. |

- A failed or incomplete container makes the call return an error, with the
  report and every other container's records. Save the report before checking
  the error.
- `Complete` means that every crash directory was read in full and every
  `meta` file in it was accepted, or that the directory is absent. It does not
  mean that every crash in the cluster's history was captured.
- A container's exit code alone does not tell a crash. A daemon that is PID 1
  of its container, as in this cluster, exits 1 after Ceph wrote its report,
  not with the signal's code.

### Other module functions and types

#### WithIdleEntrypoint

```golang
func WithIdleEntrypoint() testcontainers.CustomizeRequestOption
```

Keeps a client or helper container running with `sleep infinity` under Docker's
init process, so you can run commands in it with `Exec`. As PID 1, a bare `sleep`
ignores `SIGTERM` and Docker would wait for the full stop timeout (10 seconds) on
every termination; init forwards the signal, so termination is immediate. Init
also reaps background processes started through `Exec`, such as a test server.
Pass it to `testcontainers.Run`, not to `ceph.Run`.

```golang
client, err := testcontainers.Run(ctx, cluster.ControlImage(),
	cluster.WithClient(),
	ceph.WithIdleEntrypoint(),
)
```

#### Standalone network faults

[`fault.InterruptLink`](fault.md#standalone-bridge-functions) and
[`fault.ApplyLinkProfile`](fault.md#standalone-bridge-functions) operate on
caller-owned bridges. The caller restores their handles before removing the
container or bridge.

#### ErrHostNetworkUnavailable

```golang
var ErrHostNetworkUnavailable error
```

Wrapped by the error that `Run` returns in host mode when the engine does not run
Linux containers, the [WithHostAddress](#withhostaddress) address is not assigned
on the Docker host, or the test process cannot reach the first MON's advertised
endpoint. `Run` returns it before creating managers and OSDs; later MON and RGW
port reservations can also return it. The error message explains the likely
cause for the detected Docker environment. It belongs to
[ErrEnvironmentUnavailable](#error-categories).

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithHostNetwork())
testcontainers.CleanupContainer(t, cluster)
if errors.Is(err, ceph.ErrHostNetworkUnavailable) {
	t.Skip("host networking is not usable here:", err)
}
if err != nil {
	t.Fatal(err)
}
```

#### Error categories

```golang
var (
	ErrInvalidRequest         error
	ErrUnsupported            error
	ErrImageCapability        error
	ErrEnvironmentUnavailable error
	ErrNativeFailure          error
	ErrSharedCluster          error
)
```

Every package of the module classifies its failures with these categories, so
that a test can tell a refused request from a missing feature, image tool or
engine capability, and from a native command that failed. Match them with
`errors.Is`; an error joined from several failures can match several.

| Category | Meaning |
| --- | --- |
| `ErrInvalidRequest` | Refused before any change: malformed arguments, or an operation that the state of the cluster or of the selected resource forbids, such as a terminated cluster, an unowned container or an override that is already held. |
| `ErrUnsupported` | The Ceph of the relevant target lacks the feature, judged from that target's tools or from a documented limit of its release, or, for `AddressDualStack`, from the addresses its daemons advertise after startup. `*OperationError` names the target, its version and the feature. |
| `ErrImageCapability` | The image lacks an executable or runtime that the operation needs, or an optional extension was not selected or does not meet its contract. |
| `ErrEnvironmentUnavailable` | The Docker engine, host, kernel or container permissions cannot provide what the operation needs, although the image may be valid. |
| `ErrNativeFailure` | A native command ran and failed. `*NativeFailure` carries its status. |
| `ErrSharedCluster` | A cluster that a `SharedCluster` shares refused the method before any change; see [The shared Container](#the-shared-container). |

Context errors, `ErrNativeCommandIncomplete`, `ErrNativeInputClosed` and
`*NativeCommandUnstoppedError` keep their own contracts and belong to no
category; a call that `ctx` ended is never classified by what it could not
finish. An error that matches no category is a failed check of the fixture's
own, such as native output that it could not parse, or a failure whose
outcome is unknown, such as a container that disappeared after a native
command was sent to it. Only `ErrInvalidRequest` and `ErrSharedCluster`
promise that nothing changed. See [Classified errors](guides/errors.md).

```golang
_, err := cluster.CreateClient(ctx, "modern", ceph.ClientCaps{Mon: "allow r"},
	ceph.WithClientKeyType(ceph.CipherAES256K))
var unsupported *ceph.OperationError
if errors.As(err, &unsupported) && unsupported.Kind == ceph.ErrUnsupported {
	t.Logf("Ceph %s lacks %s", unsupported.Version, unsupported.Capability)
}
```

#### OperationError

```golang
type OperationError struct {
	Kind       error
	Op         string
	Target     string
	Version    string
	Image      string
	Capability string
	Err        error
}
func (e *OperationError) Error() string
func (e *OperationError) Is(target error) bool
func (e *OperationError) Unwrap() error
```

A classified failure with the details that identify it. `Kind` is one of the
[error categories](#error-categories), and `Is` matches it. `Op` names the
operation when the cause's text does not, `Target` the container or cluster,
such as `control`, `destination cluster` or `zone source`, `Version` the Ceph
release observed on it, `Image` its immutable image ID, and `Capability` the
missing feature, such as `ceph-authtool --key-type` or `python3`. Unset fields
were not known or do not apply. The text is `Op`, the cause, and `Target`,
`Version` and `Image` in parentheses; it never includes command arguments,
raw output or credentials. Service packages return it too.

#### NativeFailure

```golang
type NativeFailure struct {
	Program  string
	ExitCode int
	Errno    string
	Reason   string
}
const MaxNativeFailureReason = 512
func (f *NativeFailure) Error() string
func (f *NativeFailure) Is(target error) bool
```

The status of a module command that ran and exited nonzero, such as one of
[Ceph](#ceph) or of the rbd and radosgw-admin helpers of the service packages.
`Program` is the program alone, never its arguments. `Errno` is the error
code that the tool printed, from the `Error ENOENT:` that starts a line of
Ceph or the last `(2) No such file or directory` of a line of the other tools,
which print it after the names of the request; it is empty when the tool
printed none and is never guessed from the exit code. `Reason` is the
tool's own explanation, its error output without Ceph's log lines joined with
`; `, at most `MaxNativeFailureReason` bytes, with
keys and the secrets that [RedactSecrets](#redactsecrets) recognizes replaced;
it can name resources of the request, such as a pool. Errors whose text hides
native output keep a copy without `Reason`.

`errors.Is` matches it with `ErrNativeFailure`, with `fs.ErrNotExist` for
`ENOENT`, `fs.ErrPermission` for `EACCES` and `EPERM`, and `fs.ErrExist` for
`EEXIST`. [RunNativeCommand](#runnativecommand) returns a nonzero exit as its
result instead.

```golang
_, err := cluster.Ceph(ctx, "osd", "pool", "get", "missing", "size")
var failure *ceph.NativeFailure
if errors.As(err, &failure) {
	t.Logf("%s exited %d with %s: %s", failure.Program, failure.ExitCode, failure.Errno, failure.Reason)
}
if !errors.Is(err, fs.ErrNotExist) {
	t.Fatalf("a missing pool returned %v", err)
}
```

#### Option

```golang
type Option func(*options) error
func (Option) Customize(*testcontainers.GenericContainerRequest) error
```

The type of every module option (`WithOSDCount`, ..., and the service package
options). It implements `testcontainers.ContainerCustomizer`, so it can be mixed
with generic options in `Run`. `Run` reads it as cluster configuration; its
`Customize` method does nothing, so passing it to another container has no
effect.

#### MessengerMode

```golang
type MessengerMode int

const (
	MessengerDefault MessengerMode = iota
	MessengerV2Secure
	MessengerV2Only
)
```

The bootstrap protocols and connection policy for [WithMessengerMode](#withmessengermode).
Unknown values are rejected by `Run`.

#### NetworkPlane

```golang
type NetworkPlane string

const (
	PublicNetworkPlane  NetworkPlane = "public"
	ClusterNetworkPlane NetworkPlane = "cluster"
)
```

Selects the client/public network or the OSD replication network for
[fault.InterruptNetwork](fault.md#interruptnetwork).

#### Daemon handles

| Type | Fields and methods | Notes |
| --- | --- | --- |
| `MonitorContainer` | embeds `testcontainers.Container`; `DaemonName` | `Stop`/`Start` keep the store. See [Monitors](#monitors). |
| `ManagerContainer` | embeds `testcontainers.Container`; `DaemonName` | Active or standby. See [Managers](#managers). |
| `OSDContainer` | embeds `testcontainers.Container`; `ID`; `Placement() OSDConfig` | Backed by a sparse BlueStore file. `Placement` returns a copy of the resolved CRUSH location. See [OSDs](#osds). |
| `DetachedOSD` | `ID() int`; `UUID() string`; `ClusterFSID() string`; `Placement() OSDConfig` | A retained store and registration without a container. See [DetachOSD](#detachosd) and [ReopenOSD](#reopenosd). |

The cluster owns these containers. Do not terminate them individually; use
`RemoveMonitor`, `RemoveManager` or `RemoveOSD`.

#### ClientConfig

`*ceph.ClientConfig` describes an identity created by [CreateClient](#createclient).
The cluster owns the auth database; containers you attach with it are yours.

| Method | Returns |
| --- | --- |
| `Name() string` | The full entity, for example `client.reader`. |
| `User() string` | The ID without `client.`, for APIs such as librados `rados_create` and go-ceph `rados.NewConnWithUser`. |
| `KeyringPath() string` | The keyring path used inside attached containers, `/etc/ceph/ceph.client.<id>.keyring`. |
| `ConnectionConfig() ([]byte, []byte, error)` | Independent copies of a `ceph.conf` and the identity's keyring for a client you run yourself. The configuration is a snapshot taken at creation; after replacing monitors, update `mon_host` (see [MonitorBootstrapAddresses](#monitorbootstrapaddresses)). Revocation does not erase copies, and a descriptor that [ReplaceClientKey](#replaceclientkey) replaced keeps returning its old key. |
| `String() string`, `GoString() string` | A description that never includes credentials, safe for logs and `%v`/`%#v`. |

#### Restoration handles

| Type | Methods | Created by |
| --- | --- | --- |
| `ConfigOverrideHandle` | `Restore(ctx) error`; `String()`, `GoString()` for logging | [OverrideConfig](#overrideconfig) |
| `OSDFlagOverrideHandle` | `Restore(ctx) error` | [OverrideOSDFlag](#overrideosdflag) |
| `MGRModuleOverrideHandle` | `Restore(ctx) error` | [OverrideMGRModule](#overridemgrmodule) |
| `FullRatiosOverride` | `Restore(ctx) error` | [OverrideFullRatios](#overridefullratios) |
| `BlocklistOverride` | `Address() string`, `Restore(ctx) error` | [TemporaryBlocklist](#temporaryblocklist) |
| `fault.PauseLease` | `ContainerID`; `Resume(ctx) error` | [fault.Pause](fault.md#pause) |
| `fault.NetworkInterruption` | `ContainerID`, `NetworkName`, `OriginalAddress`; `Restore(ctx) error` | [fault.InterruptNetwork](fault.md#interruptnetwork) (cluster-bound or standalone) |
| `ClientRevocationHandle` | `Name() string`, `Restore(ctx) error`; `String()`, `GoString()` for logging | [RevokeClient](#revokeclient) |

See the [handle contract](#container-methods).

#### Configuration and result types

| Type | Used by |
| --- | --- |
| `OSDConfig` | [WithInitialOSDs](#withinitialosds), [AddOSDWithConfig](#addosdwithconfig), `OSDContainer.Placement` |
| `OSDState` | [OSDStates](#osdstates) |
| `PoolConfig`, `ErasureCodeConfig` | [WithPools](#withpools), [CreatePool](#createpool) |
| `Pool` | [CreatePool](#createpool) |
| `PoolState`, `PoolQuota` | [PoolStatus](#poolstatus), [Pools](#pools), [SetPoolQuota](#setpoolquota) |
| `PoolPlacement` | [SetPoolPlacement](#setpoolplacement) |
| `PoolUsageSnapshot` | [PoolUsage](#poolusage) |
| `PoolPGSnapshot`, `PGState`, `PGStats` | [PoolPGs](#poolpgs) |
| `ClientCaps` | [CreateClient](#createclient), [ClientCapabilities](#clientcapabilities), [UpdateClientCaps](#updateclientcaps) |
| `ClientOption` | [CreateClient](#createclient), [ReplaceClientKey](#replaceclientkey) |
| `ConfigSetting` | [OverrideConfig](#overrideconfig) |
| `ConfigEntry` | [Configuration](#configuration) |
| `MGRModuleState` | [MGRModules](#mgrmodules) |
| `Status` | [Status](#status) |
| `QuorumStatus` | [QuorumStatus](#quorumstatus) |
| `ManagerStatus` | [ManagerStatus](#managerstatus) |
| `HealthSnapshot`, `HealthCheck`, `HealthMute` | [HealthDetails](#healthdetails) |
| `ObjectWatcher` | [ObjectWatchers](#objectwatchers) |
| `BlocklistEntry` | [BlocklistEntries](#blocklistentries) |
| `FullRatios`, `FullRatioSnapshot` | [FullRatios](#fullratios), [OverrideFullRatios](#overridefullratios) |
| `InconsistentObject`, `InconsistentShard` | [PGInconsistencies](#pginconsistencies) |
| `DiagnosticsConfig`, `DiagnosticsContainer`, `DiagnosticsReport`, `DiagnosticArtifact` | [CollectDiagnostics](#collectdiagnostics) |
| `FinalizationConfig`, `Finalization` | [TerminateOnCleanup](#terminateoncleanup) |
| `VerificationConfig`, `LogExpectation`, `CrashExpectation`, `Verification`, `VerificationReport` and its parts | [Verify](#verify) |

## Examples

The fragments below run inside a test with `ctx := t.Context()` and a `cluster`
started as in the [usage example](#usage-example), unless they show their own
`Run`. They use `waitForHealthCheck` from [HealthDetails](#healthdetails).

### Attach a client with a restricted identity

Create an identity limited to one pool and namespace, attach a container that
only has that identity's keyring, and verify the limits with a fresh connection.

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=app-data namespace=tenant-a",
})
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(ctx, cluster.ControlImage(),
	cluster.WithClientIdentity(reader),
	ceph.WithIdleEntrypoint(),
)
testcontainers.CleanupContainer(t, client)
if err != nil {
	t.Fatal(err)
}
// Inside the container: rados --id reader -p app-data -N tenant-a ls
// Writes, other namespaces and other pools are denied with EPERM.

// Widen the identity; the key stays the same. Omitted services lose their caps.
if err := cluster.UpdateClientCaps(ctx, reader, ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow rw pool=app-data namespace=tenant-a",
}); err != nil {
	t.Fatal(err)
}
// Use a new client process to observe the change; existing sessions may keep
// their old tickets for a while.
if err := cluster.DeleteClient(ctx, reader); err != nil {
	t.Fatal(err)
}
```

### Give each test its own pool on a shared cluster

Starting a cluster costs tens of seconds, so a package can share one cluster
(for example from `TestMain`) and give each test a pool that it removes again.
Removing pools keeps the cluster below Ceph's per-OSD PG limit.

```golang
func newTestPool(t *testing.T, cluster *ceph.Container) string {
	t.Helper()
	ctx := t.Context()
	name := "t-" + strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := cluster.RemovePool(ctx, name); err != nil {
			t.Error(err)
		}
	})
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	return name
}
```

Pool names must use letters, digits, `_`, `.` and `-` and must be unique among
pools, CRUSH rules and EC profiles; derive them from `t.Name()` as above.

### Create an erasure-coded data pool for RBD images

RBD keeps image metadata in a replicated pool and can store data in an EC pool
that allows overwrites.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(3),
	rbd.WithPools(ceph.PoolConfig{Name: "rbd-meta"}),
	ceph.WithPools(ceph.PoolConfig{
		Name: "rbd-data", Application: "rbd",
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	}),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// In a WithClient container:
// rbd create --size 64M --data-pool rbd-data rbd-meta/volume
```

`K+M` OSDs (three here) are needed because each shard goes to a distinct OSD.

### Simulate failure domains with CRUSH hosts and racks

Logical hosts, racks, roots and device classes let you test placement rules and
domain failures on one Docker engine.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithInitialOSDs(
		ceph.OSDConfig{Host: "host-a", Rack: "rack-a", Root: "tc-storage", DeviceClass: "fast"},
		ceph.OSDConfig{Host: "host-b", Rack: "rack-b", Root: "tc-storage", DeviceClass: "fast"},
		ceph.OSDConfig{Host: "host-c", Rack: "rack-c", Root: "tc-storage", DeviceClass: "fast"},
	),
	ceph.WithPoolDefaults(3, 2),
	ceph.WithDefaultCRUSHRoot("tc-storage"),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "by-host", Application: "rados",
	FailureDomain: "host", CRUSHRoot: "tc-storage", DeviceClass: "fast",
})
if err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// Each PG now has one replica per host. Stopping the only OSD of host-a takes
// that whole failure domain down; with min_size 2 the pool stays writable.
_ = pool
```

Verify the hierarchy with `cluster.Ceph(ctx, "osd", "tree", "--format", "json")`
or `"osd", "find", "<id>"`.

### Start without OSDs, then add storage

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNoInitialOSDs())
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// MON and MGR are up; the application can be tested before storage exists.
// Pool defaults stay size 2, min_size 1.
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err) // a non-nil partial OSD is still owned by the cluster
	}
}
if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "application", Application: "rados"}); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

`CreatePool` before any OSD exists is refused by the failure-domain check.

### Start without a manager

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNoInitialManagers(),
	ceph.WithPools(ceph.PoolConfig{Name: "application", Application: "rados"}),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// MON quorum and both OSDs are up and in; the pool exists. Direct RADOS I/O
// works, MGR-backed commands and PG statistics do not.
if _, err := cluster.AddManager(ctx, "a"); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

Add `ceph.WithNoInitialOSDs()` for a MON-only phase; you can add the first
manager before or after the first OSD.

### Reproduce a minimal legacy placement setup

A client that implements only the oldest CRUSH and placement features can be
tested against one MemStore OSD, a legacy CRUSH map and a pool with one PG, one
replica and no `hashpspool` flag on any pool. The fixture has no option for a
hand-written CRUSH map; install it with the native tools, as below. `crushtool`
is in the official image and the published control images.

```golang
// Ceph creates the MGR's .mgr pool with the MON's default flags, so turn
// the hashpspool default off before any pool exists.
legacyPools := filepath.Join(t.TempDir(), "legacy-pools.conf")
if err := os.WriteFile(legacyPools, []byte("[global]\nosd_pool_default_flag_hashpspool = false\n"), 0o644); err != nil {
	t.Fatal(err)
}
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(1),
	ceph.WithPoolDefaults(1, 1),
	ceph.WithOSDObjectStore(ceph.OSDMemStore),
	ceph.WithOSDBlockSize(256<<20),
	ceph.WithConfigFile(legacyPools),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}

// Keep OSDs from moving themselves into host buckets or setting a device
// class when they start, so the installed map stays as written.
for _, name := range []string{"osd_crush_update_on_start", "osd_class_update_on_start"} {
	if _, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: name, Value: "false"}); err != nil {
		t.Fatal(err)
	}
}
const legacyMap = `tunable choose_local_tries 2
tunable choose_local_fallback_tries 5
tunable choose_total_tries 19
tunable chooseleaf_descend_once 0
tunable chooseleaf_vary_r 0
tunable chooseleaf_stable 0
tunable straw_calc_version 0
device 0 osd.0
type 0 osd
type 10 root
root default {
    id -1
    alg straw
    hash 0
    item osd.0 weight 1.00000
}
rule replicated_rule {
    id 0
    type replicated
    step take default
    step choose firstn 0 type osd
    step emit
}
`
control := cluster.ControlContainer()
if err := control.CopyToContainer(ctx, []byte(legacyMap), "/tmp/crush.txt", 0o644); err != nil {
	t.Fatal(err)
}
if code, _, err := control.Exec(ctx, []string{"crushtool", "-c", "/tmp/crush.txt", "-o", "/tmp/crush.bin"}); err != nil || code != 0 {
	t.Fatalf("crushtool exited %d: %v", code, err)
}
if _, err := cluster.Ceph(ctx, "osd", "setcrushmap", "-i", "/tmp/crush.bin"); err != nil {
	t.Fatal(err)
}

pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "legacy", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true, Application: "rados",
})
```

- While any pool has `hashpspool`, the MON requires the `OSDHASHPSPOOL`
  feature of every client and refuses a client without it when it opens its
  MON session. `NoHashPSPool` clears the flag only on the pool it creates;
  the configuration file keeps it off `.mgr` and every later pool. Check
  every pool with `cluster.Pools` once `.mgr` exists.
- `CreatePool` adds its own rule, `tc-<pool>-replicated`, which with the
  default `osd` failure domain also chooses OSDs directly. To use the map's
  `replicated_rule`, create the pool with `cluster.Ceph(ctx, "osd", "pool",
  "create", ...)` instead; such a pool is not one that `RemovePool` removes.
- The legacy tunables lower the features a client needs to read the CRUSH map.
  Switching to modern tunables later (`ceph osd crush tunables optimal`) raises
  them again, which tests a client's refusal of unsupported features.
- Ceph reports `POOL_NO_REDUNDANCY` for a pool with one replica; `WaitForClean`
  still passes.

### Run small OSDs

Small logical files keep disk and tmpfs usage low. Skip mClock's startup
benchmark before the first OSD starts and keep the setting for the cluster's
whole lifetime, because restored defaults would run the benchmark at the next OSD
start.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithNoInitialOSDs(),
	ceph.WithOSDBlockSize(64<<20),
	ceph.WithOSDInMemoryStorage(256<<20), // optional; omit for container-local files
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
// No Restore: Terminate disposes of the configuration database.
if _, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{
	Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true",
}); err != nil {
	t.Fatal(err)
}
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err)
	}
}
```

Known-good pairs for a workload of a few MiB, including OSD restarts, backfill to
a third OSD and the removal of an original OSD, are 64 MiB OSDs with a 256 MiB
shared cap, 128 MiB with 512 MiB, 256 MiB with 1 GiB, and 512 MiB with 2 GiB.
Size both bounds for your own workload with headroom for replicas, metadata and
recovery. The three limits are independent:

| Resource | Bounded by | Includes |
| --- | --- | --- |
| Logical storage per OSD | `WithOSDBlockSize` | BlueStore data, metadata and temporary writes inside each file |
| Shared RAM backing | `WithOSDInMemoryStorage` | Allocated tmpfs pages of all OSD files |
| Process memory | Docker host or VM limits and daemon settings | MON, MGR, OSD and client memory |

If you customize daemon arguments or config files, confirm the effective value
with `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_mclock_skip_benchmark")`.

### Choose the OSD scheduler before adding OSDs

`osd_op_queue` is read when an OSD starts, so select it before the first OSD.
In a small comparison with HDD-class file-backed OSDs, default WPQ recovered and
backfilled more slowly than the default mClock scheduler, and setting the two WPQ
HDD recovery sleeps to 0 removed most of that difference. The module keeps
Ceph's default (mClock); measure your own scenario before changing it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNoInitialOSDs())
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
for _, setting := range []ceph.ConfigSetting{
	{Section: "osd", Name: "osd_op_queue", Value: "wpq"},
	{Section: "osd", Name: "osd_recovery_sleep_hdd", Value: "0"},
	{Section: "osd", Name: "osd_recovery_sleep_degraded_hdd", Value: "0"},
} {
	if _, err := cluster.OverrideConfig(ctx, setting); err != nil {
		t.Fatal(err)
	}
}
for range 2 {
	if _, err := cluster.AddOSD(ctx); err != nil {
		t.Fatal(err)
	}
}
// Confirm the running daemon's value: {"osd_op_queue": "wpq"}
out, err := cluster.Ceph(ctx, "tell", "osd.0", "config", "get", "osd_op_queue")
if err != nil {
	t.Fatal(err)
}
t.Log(string(out))
```

Restoring the override, or setting it on a running OSD, does not change the queue
until the OSD restarts.

### Replace every monitor and refresh clients

Rolling replacement adds a new monitor before removing an old one, so quorum is
never at risk. Owned daemons are updated automatically; your client containers
are not.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithMonitorCount(3))
// ... CleanupContainer, error check, client container `client` with WithClient()
for _, pair := range [][2]string{{"a", "d"}, {"b", "e"}, {"c", "f"}} {
	if _, err := cluster.AddMonitor(ctx, pair[1]); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemoveMonitor(ctx, pair[0]); err != nil {
		t.Fatal(err)
	}
}
if err := cluster.WaitForQuorum(ctx); err != nil {
	t.Fatal(err)
}
// Running sessions follow the monmap; a restarted client reads ceph.conf.
if err := cluster.RefreshClientMonitorConfig(ctx, client); err != nil {
	t.Fatal(err)
}
// For a client you configure yourself, write the new mon_host value.
addresses, err := cluster.MonitorBootstrapAddresses(ctx)
if err != nil {
	t.Fatal(err)
}
t.Log("mon_host =", addresses)
```

For RBD and CephFS mirror links between clusters, refresh the link as described
in [rbd](rbd.md) and [cephfs](cephfs.md).

### Keep an OSD down without data movement

```golang
hold, err := cluster.OverrideOSDFlag(ctx, "noout", true)
if hold != nil {
	defer hold.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
osd := cluster.OSDs()[0]
timeout := 10 * time.Second
if err := osd.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// Degraded reads and writes; the MON does not mark the OSD out, so no backfill
// starts. Expect OSD_DOWN, PG_DEGRADED and OSDMAP_FLAGS in HealthDetails.
if err := osd.Start(ctx); err != nil {
	t.Fatal(err)
}
if err := hold.Restore(ctx); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

To move data off a running OSD instead, use `SetOSDIn(ctx, id, false)`, wait with
`WaitForPGClean`, and mark it in again.

### Hold a pool relocation mid-flight

Create the state in which PGs have moved to new OSDs but objects are still being
recovered, and test client I/O against it.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithInitialOSDs(
		ceph.OSDConfig{DeviceClass: "fast"},
		ceph.OSDConfig{DeviceClass: "slow"},
	),
)
// ... CleanupContainer and error check
if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name: "app", Application: "rados", Replicas: 1, DeviceClass: "fast",
}); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
// ... write objects with the client
for _, flag := range []string{"nobackfill", "norecover"} {
	hold, err := cluster.OverrideOSDFlag(ctx, flag, true)
	if hold != nil {
		defer hold.Restore(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
}
if err := cluster.SetPoolPlacement(ctx, "app", ceph.PoolPlacement{DeviceClass: "slow"}); err != nil {
	t.Fatal(err)
}
// PoolPGs now reports up/acting on the slow OSD with most PGs in
// active+recovery_wait+degraded. Reads of old objects trigger on-demand
// recovery and succeed; new writes succeed.
// Restore both flags, then poll PoolPGs until every PG is active+clean on the
// slow OSD (WaitForClean alone can pass before the new mapping is reported).
```

Moving the pool back with `SetPoolPlacement(ctx, "app", ceph.PoolPlacement{DeviceClass: "fast"})`
reuses its original rule, which also makes it removable with `RemovePool` again.

### Fill the cluster with low full ratios

```golang
change, err := cluster.OverrideFullRatios(ctx, ceph.FullRatios{
	NearFull: 0.0001, BackfillFull: 0.0002, Full: 0.0003,
})
if change != nil {
	defer change.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
if err := waitForHealthCheck(ctx, cluster, "OSD_FULL"); err != nil {
	t.Fatal(err)
}
// Ordinary RADOS writes now block. A librados write with the FULL_TRY
// operation flag fails with ENOSPC (errno 28) instead; reads still work.
if err := change.Restore(ctx); err != nil {
	t.Fatal(err)
}
```

The cluster needs a little existing data for the OSDs to cross the thresholds.
To test only `OSD_NEARFULL` or `OSD_BACKFILLFULL`, lower only those ratios (for
example `NearFull: 0.0001, BackfillFull: 0.90, Full: 0.95`); ordinary writes
still succeed in those states. Setting `FULL_TRY` is the client's job; the module
adds no client SDK.

### Reproduce a scrub inconsistency and repair it

```golang
// A replicated pool with one PG and two replicas; the object "victim" exists.
var mapping struct {
	PGID          string `json:"pgid"`
	Acting        []int  `json:"acting"`
	ActingPrimary int    `json:"acting_primary"`
}
out, err := cluster.Ceph(ctx, "osd", "map", "app", "victim", "--format", "json")
if err != nil {
	t.Fatal(err)
}
if err := json.Unmarshal(out, &mapping); err != nil {
	t.Fatal(err)
}
replica := mapping.Acting[0]
if replica == mapping.ActingPrimary {
	replica = mapping.Acting[1]
}
if err := cluster.InjectObjectDataError(ctx, "app", "victim", replica); err != nil {
	t.Fatal(err)
}
if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
objects, err := cluster.PGInconsistencies(ctx, mapping.PGID)
if err != nil {
	t.Fatal(err)
}
// objects[0].Name == "victim", objects[0].UnionShardErrors == []string{"read_error"}
if err := waitForHealthCheck(ctx, cluster, "PG_DAMAGED"); err != nil {
	t.Fatal(err)
}
if err := cluster.RepairPG(ctx, mapping.PGID); err != nil {
	t.Fatal(err)
}
```

After the repair the health checks clear, and a new `DeepScrubPG` reports no
inconsistency.

### Fence one client session

```golang
// address is the client's own session address, e.g. from rados_getaddrs or
// go-ceph Conn.GetAddrs: "172.19.0.6:0/3461995546".
fence, err := cluster.TemporaryBlocklist(ctx, address, time.Minute)
if fence != nil {
	defer fence.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// The fenced session's operations fail (ESHUTDOWN in librados); another session
// from the same host keeps working.
if err := fence.Restore(ctx); err != nil {
	t.Fatal(err)
}
// The client can resume or reconnect, depending on its library.
```

`BlocklistEntries` shows the entry and its expiry. For RBD exclusive-lock
handover, the lock owner's address comes from `rbd.ImageClients` (see
[rbd](rbd.md)).

### Detect a stuck watcher

A client that holds a watch and then stops responding blocks notifies on that
object until the OSD's `osd_client_watch_timeout` (30 seconds by default)
expires.

```golang
// `watcher` is a WithClient container whose process watches "notify-object".
pause, err := fault.Pause(ctx, cluster, watcher)
if pause != nil {
	defer pause.Resume(context.Background())
}
if err != nil {
	t.Fatal(err)
}
watchers, err := cluster.ObjectWatchers(ctx, "app-data", "", "notify-object")
if err != nil {
	t.Fatal(err)
}
// The frozen client is still listed, and a notify from another client fails
// with ETIMEDOUT (errno 110) after its own timeout. Poll ObjectWatchers: after
// the watch timeout the list is empty and notifies complete at once.
_ = watchers
```

An expired watch does not blocklist its client; `BlocklistEntries` stays
unchanged.

### Scrape the MGR Prometheus exporter

```golang
module, err := cluster.OverrideMGRModule(ctx, "prometheus", true)
if module != nil {
	defer module.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
var url string
for url == "" {
	services, err := cluster.ManagerServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	url = services["prometheus"] // e.g. "http://172.19.0.3:9283/"
	if url == "" {
		time.Sleep(time.Second) // bound this loop with ctx in real tests
	}
}
metrics, err := cluster.ManagerMetrics(ctx) // parsed samples; see ManagerMetrics
```

In bridge mode each manager listens on its own container address on port 9283,
and standbys answer with an empty 200 response. After a failover
(`cluster.Ceph(ctx, "mgr", "fail")`), `ManagerServices` returns the new active
manager's URL.

In host mode all managers share the host network, so give each manager its own
port and bind the public address before enabling the module (using
[OverrideConfig](#overrideconfig)). With the default settings and two
managers, `ManagerServices` stayed empty and no manager served metrics:

```golang
settings := []ceph.ConfigSetting{
	{Section: "mgr", Name: "mgr/prometheus/server_addr", Value: cluster.PublicAddress()},
	{Section: "mgr", Name: "mgr/prometheus/a/server_port", Value: "19283"},
	{Section: "mgr", Name: "mgr/prometheus/b/server_port", Value: "19284"},
}
```

Managers added later with `AddManager` need their own port setting too. Pick
ports that no other process or cluster uses.

### Connect a native client in host mode

On a Linux Docker host, host mode lets a test process use go-ceph or librados
directly:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithHostNetwork())
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
config, keyring, err := cluster.ConnectionConfig()
// Write both files and point librados at them (see ConnectionConfig).
```

See [Host networking](guides/host-network.md) for remote engines, Docker
Desktop and port selection.

### Run a cluster that accepts only encrypted connections

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithMessengerMode(ceph.MessengerV2Secure),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
config, keyring, err := cluster.ConnectionConfig()
// Hand config and keyring to the client under test (for example an
// independent Messenger v2 implementation), or attach a container with
// cluster.WithClient() / cluster.WithClientIdentity(...).
```

The client must reach the advertised addresses: run it in a `WithClient`
container, or use host mode.

### Save diagnostics when a test fails

Use [TerminateOnCleanup](#terminateoncleanup) with `Archive: true`: it
writes the diagnostics, the crash metadata and a manifest into the test's
artifact directory before the cluster is terminated. Run `go test -artifacts`
to keep them. Add caller-owned clients and mirror daemons with
`Diagnostics.AdditionalContainers`, remove them through
`Finalization.Cleanup`, and mask application secrets with `RedactValues`. To
collect by hand, see [CollectDiagnostics](#collectdiagnostics) and
[CollectCrashMetadata](#collectcrashmetadata).

To also fail a test whose client operations succeeded although a daemon
crashed or logged an unexpected warning, activate a post-test
[verification](#verify) on the same finalization, with the crashes and log
lines that the test causes on purpose as expectations.

## Running native commands

Everything without a typed helper can be done with Ceph's own tools in the
cluster's containers:

| Task | How |
| --- | --- |
| Any `ceph` command | `cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "nodelete", "true")` |
| JSON output | `cluster.Ceph(ctx, "osd", "dump", "--format", "json")` and `json.Unmarshal` |
| `rados`, `rbd`, `ceph-authtool`, Python clients | `cluster.ControlContainer().Exec(ctx, []string{"rados", "-p", "app-data", "ls"}, tcexec.Multiplexed())` |
| A command with bounded run time and output that must not outlive the call | [RunNativeCommand](#runnativecommand) |
| Daemon admin socket commands | `cluster.Ceph(ctx, "tell", "osd.0", "dump_ops_in_flight")` |
| A daemon's effective configuration | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_queue")` |
| Any central option | [OverrideConfig](#overrideconfig), so it is restored |
| Commands inside one daemon's container | `cluster.OSDs()[0].Exec(ctx, []string{...})` (OSD images may not contain the `ceph` CLI) |
| `radosgw-admin` | `gateway.Admin(ctx, args...)` from the [rgw](rgw.md) package |

Arguments are passed without a shell. `cluster.Ceph` adds `--connect-timeout 5`
and returns stdout; on failure the error contains the output. Raw commands can
change anything, so do not race them against a helper that changes the same
state, and prefer typed helpers when they exist because they restore state and
check identities. See [Native commands](guides/native-commands.md) for more
examples.

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
- IPv4 is the default. [WithAddressFamily](#withaddressfamily) selects IPv6
  or IPv4-mapped IPv6 on a bridge network; the host network is IPv4 only.

### Fixture defaults that differ from a stock cluster

| Setting | Fixture value | Change with |
| --- | --- | --- |
| `osd_pool_default_size` / `min_size` | `min(2, initial OSDs)` / 1 | `WithPoolDefaults` |
| `osd_pool_default_pg_num`, autoscale mode | 8, `off` | `PoolConfig.PGNum`, `SetPoolPGCount` |
| `mon_allow_pool_size_one` | `true` (replica-1 pools allowed) | config file |
| `mgr_stats_period` | 1 second (faster PG reports) | config file |
| `bluestore_block_size` | 1 GiB sparse file | `WithOSDBlockSize` |
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
  reachable from the host (see [rgw](rgw.md)).
- Host mode needs a Linux engine, or Docker Desktop with host networking enabled,
  and cannot be combined with a separate cluster network or Docker network
  interruption.

### Asynchronous Ceph behaviour

- PG states, pool usage, quotas and health checks are updated asynchronously.
  Poll observations with a deadline rather than asserting immediately.
- `MGR_DOWN` appears only after `mon_mgr_mkfs_grace` (2 minutes) on a new
  cluster, and `SLOW_OPS` after `osd_op_complaint_time` (30 seconds). Lower such
  values with `OverrideConfig` when a test needs them sooner.
- Changed capabilities and revoked identities take effect for new connections;
  existing sessions may keep their tickets.
- After `SetPoolPlacement` or `SetPoolPGCount`, `WaitForClean` can pass before
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
  same on Squid; release-specific behaviour is in the [rbd](rbd.md),
  [cephfs](cephfs.md) and [rgw](rgw.md) packages.
- Squid has no `messenger dump` admin command, so encryption of individual
  connections cannot be inspected there (see
  [WithMessengerMode](#withmessengermode)).
- Squid 19.2.6 has an RGW multisite regression; use 19.2.5 for multisite tests
  (see [rgw](rgw.md)).
- `CephVersion` reflects only the control image. Mixing releases across role
  images is not supported.

See [Releases and images](guides/releases-and-images.md) for details.

### Common pitfalls

- A cluster with one OSD needs replica-1 pools to become clean. The default pool
  size already follows `min(2, OSD count)`; `CreatePool` refuses an explicit
  `Replicas: 2` there, and a size-2 pool created through the CLI never becomes
  clean.
- Small `WithOSDBlockSize` values with initial OSDs make `Run` time out (see
  [Run small OSDs](#run-small-osds)).
- A key set with `WithConfigFile` cannot be changed with `OverrideConfig`;
  `mon_allow_pool_delete = false` in a config file blocks `RemovePool`.
- `t.Context()` is canceled before cleanup functions run. Use a fresh context in
  `t.Cleanup` for `Restore`, `RemovePool` and `CollectDiagnostics`.
- Terminate client containers before the cluster: register their cleanup after
  the cluster's.
- Keys copied by `WithClient` and `ConnectionConfig` are the cluster's admin
  credentials.
- `WaitForClean` and other waits are capped by `WithStartupTimeout` even if your
  context allows more time.


## Role image selection

`ImageSet` selects Essential, Client, CephFS, RBDAux and RGW, or an exclusive
All image. `RunWithImages` accepts the set directly; `WithImages` applies it
to service Run entry points with images.Essential as the image argument.
`Run` without an explicit set uses its image as the aggregate. `Images()`
returns the effective selection and `ControlImage()` returns essential.

`ClientContainerContext` lazily creates one owned toolkit for a separate
client image; aggregate selections reuse the control executor. Initial
attachment is serialized with MON topology changes and honors cancellation.
The cluster owns partial/successful toolkit teardown. RBD/CephFS operations,
`ObjectWatchers` and `PGInconsistencies` use that toolkit, while daemon-local
sockets and process observations stay on their daemon. ClientConfig and
WithClientIdentity keep identity/credential ownership; a separate public
client lifecycle type is unnecessary.
