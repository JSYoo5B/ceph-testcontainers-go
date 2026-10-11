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

RBD, CephFS and RGW (S3) live in their own packages: [rbd](rbd.md),
[cephfs](cephfs.md) and [rgw](rgw.md). Their options plug into the same
`ceph.Run`; see [Service packages](#service-packages).

Requirements:

- Go 1.25 or later.
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
| `WithAddressFamily(AddressIPv6)` or `WithAddressFamily(AddressIPv4MappedIPv6)` with `WithHostNetwork` | `WithAddressFamily(...) requires bridge mode` |
| `WithAddressFamily(AddressIPv6)` or `WithAddressFamily(AddressIPv4MappedIPv6)` with `WithSeparateClusterNetwork` | rejected |
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

Use the second argument in the `Run` function to set a valid Docker image.

`ceph.DefaultImage` pins the official Ceph Tentacle 20.2.4 image by digest. The
digest refers to a multi-architecture index for `linux/amd64` and `linux/arm64`:

```
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
```

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
```

The image passed to `Run` is the control image. It must contain everything the
control role needs: `ceph-mon`, `ceph-mgr` with its modules and dependencies, the
`ceph` and `rados` command-line tools, Python `rados`/`rbd`/`cephfs` bindings, and
the `rbd-mirror` and `cephfs-mirror` daemons used by the service packages. The
official image contains all of them. Other roles use the same image unless you
select a role image:

| Role | Option | Used for |
| --- | --- | --- |
| control | `img` argument of `Run` | MONs, MGRs, CLI control container, mirror daemons, fixture helpers |
| OSD | [WithOSDImage](#withosdimage) | initial and later OSDs |
| RGW | `rgw.WithImage` | gateways ([rgw](rgw.md)) |
| MDS | `cephfs.WithMDSImage` | metadata servers ([cephfs](cephfs.md)) |

Role images must run the same Ceph release. The ceph-testcontainers-images
project checks and publishes role images extracted from the official image to
`ghcr.io/jsyoo5b/ceph-testcontainers-images`, for example:

```golang
const registry = "ghcr.io/jsyoo5b/ceph-testcontainers-images:"

cluster, err := ceph.Run(ctx, registry+"official-20.2.4-control",
	ceph.WithOSDImage(registry+"official-20.2.4-osd"),
	rgw.WithImage(registry+"official-20.2.4-rgw"),
	cephfs.WithMDSImage(registry+"official-20.2.4-mds"),
)
```

Squid 19.2.5 (`quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b`)
is also supported. [ControlImage](#controlimage) returns the selected control
image and [CephVersion](#cephversion) the release it runs. See
[Releases and images](guides/releases-and-images.md) for role image contents,
private registries and release differences.

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
| `NoHashPSPool` | false | Clears the pool's `hashpspool` flag right after creation. The placement seed of each PG is then the legacy sum of PG number and pool ID instead of a hash of the two, as for pools that predate the flag. |

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
[TemporaryConfig](#temporaryconfig), then add OSDs. The option itself never
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

The default (without this option) remains a container-local sparse file per OSD.

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
  [WithOSDInMemoryStorage](#withosdinmemorystorage), which keeps BlueStore files
  in memory, and [InjectObjectDataError](#injectobjectdataerror) needs
  BlueStore.
- `ceph osd metadata` reports `osd_objectstore` as `memstore`.

#### WithOSDImage

```golang
ceph.WithOSDImage("ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-osd")
```

Selects the image for initial and later OSDs. Without it, OSDs use the image
passed to `Run`. A blank image is rejected. Use the same Ceph release as the
control image.

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
  It does not change a running cluster; use [TemporaryConfig](#temporaryconfig)
  for that.
- An entry replaces the fixture's default for the same section and key. Spaces,
  `-` and `_` in key names are equivalent, as in Ceph. Several files are merged in
  order, later files overriding earlier ones key by key, so a shared base file
  plus a per-test file works.
- Ceph gives local files precedence over the MON configuration database. A key set
  in this file cannot be changed later by `TemporaryConfig` or `ceph config set`;
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
| `ms_*_mode`, `ms_mon_*_mode`, `ms_bind_*` | `WithMessengerMode`, `WithMessengerPolicy`, `WithClientMessengerPolicy`, `WithHostNetwork` |
| `osd_pool_default_size`, `osd_pool_default_min_size` | `WithPoolDefaults` |
| `osd_pool_default_crush_rule` | `WithDefaultCRUSHRoot` |
| `bluestore_block_size` | `WithOSDBlockSize` |
| `osd_objectstore`, `bluestore_block_path`, `bluestore_block_create` | fixed OSD storage layout |
| `log_file`, `log_to_file`, `log_to_stderr`, `err_to_stderr`, `mon_cluster_log_to_*` | daemon logs always go to container output |
| `admin_socket`, `run_dir`, `mon_data`, `osd_data`, `mgr_data`, `mds_data` | fixed paths |
| `mds_join_fs`, `mds_cache_memory_limit` | set on the MDS command line |
| `rgw_frontends`, `rgw_thread_pool_size`, `rgw_exit_timeout_secs`, `rgw_realm`, `rgw_zonegroup`, `rgw_zone`, `rgw_sync_obj_etag_verify`, `rgw_sync_lease_period` | set on the RGW command line |

The parser also rejects keys outside a section, `!include`/`include`/`includedir`,
lines continued with a trailing `\`, duplicate keys within one section of one
file, NUL bytes or invalid UTF-8, and files (or merged results) larger than
64 KiB. Ceph treats `#` and `;` as comment starts even inside quotes, so a value
such as `"a;b"` is rejected; escape literal characters as `\#` and `\;`.

#### WithMessengerMode

```golang
ceph.WithMessengerMode(ceph.MessengerV2Secure)
```

Selects the Messenger connection policy at bootstrap. The policy is immutable for
the lifetime of the cluster.

| Mode | Behaviour |
| --- | --- |
| `ceph.MessengerDefault` | Ceph's own defaults: v1 and v2 MON addresses; connection modes as configured in the image. Not every connection is encrypted. |
| `ceph.MessengerV2Secure` | Only encrypted Messenger v2 (`secure`, AES-GCM). No CRC mode and no v1 listener. |

With `MessengerV2Secure`, the global `ceph.conf` sets `ms_cluster_mode`,
`ms_service_mode`, `ms_client_mode`, `ms_mon_cluster_mode`,
`ms_mon_service_mode` and `ms_mon_client_mode` to `secure`, `ms_bind_msgr1` to
`false` and `ms_bind_msgr2` to `true`.

- The monmap and `mon_host` contain only v2 addresses. In bridge mode the MON
  publishes only `3300/tcp`, so `MappedPort(ctx, "6789/tcp")` fails.
- Later daemons, `WithClient` and `WithClientIdentity` containers and
  [ConnectionConfig](#connectionconfig) inherit the policy. MON refreshes only
  rewrite `mon_host` and keep it.
- [TemporaryConfig](#temporaryconfig) refuses the six mode options and the two
  bind options in every section and mask, because the local file would override
  them anyway.
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
	MonTicketTTL:     1500 * time.Millisecond,
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
  [TemporaryConfig](#temporaryconfig) cannot change them later. MONs rotate
  service secrets on their tick, so lifetimes of a few seconds also need a
  shorter `mon_tick_interval`, which [WithConfigFile](#withconfigfile) can set.
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
- [TemporaryMessengerPolicy](#temporarymessengerpolicy) changes the policy on a
  running cluster. The daemons apply it to new connections without a restart.
- It cannot be combined with `MessengerV2Secure`, whose local configuration
  fixes every mode.

A rejected client sees one of two behaviours, depending on the daemon:

| Daemon | A client that offers no accepted mode |
| --- | --- |
| MON | fails at once: `handle_auth_bad_method ... (95) Operation not supported` |
| MGR, OSD | gets no error; the client keeps retrying the connection until its own timeout |

Bound calls that a MGR or an OSD may reject, for example with the context of
your client or with `timeout` around the CLI.

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
`conn.SetConfigOption("ms_mon_client_mode", "crc")`.

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
  and Docker endpoint interruption ([InterruptNetwork](#interruptnetwork)) is not
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
the two bridges, and [InterruptNetwork](#interruptnetwork) with
`ceph.ClusterNetworkPlane` disconnects one OSD's replication endpoint only.
Requires bridge mode.

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

The MONs get these addresses in the monmap and bind them. The generated
`ceph.conf` sets `ms_bind_ipv4 = false` and `ms_bind_ipv6 = true`, so MGRs,
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

- Bridge mode only, without [WithSeparateClusterNetwork](#withseparateclusternetwork).
- `AddressIPv6` needs a Docker Engine that allocates an IPv6 subnet for a new
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
| `*ceph.ConfigOverride` | [TemporaryConfig](#temporaryconfig) | `Restore` |
| `*ceph.OSDFlagOverride` | [TemporaryOSDFlag](#temporaryosdflag) | `Restore` |
| `*ceph.MGRModuleOverride` | [TemporaryMGRModule](#temporarymgrmodule) | `Restore` |
| `*ceph.FullRatiosOverride` | [TemporaryFullRatios](#temporaryfullratios) | `Restore` |
| `*ceph.BlocklistOverride` | [TemporaryBlocklist](#temporaryblocklist) | `Restore` |
| `*ceph.ContainerPause` | [PauseContainer](#pausecontainer) | `Resume` |
| `*ceph.NetworkInterruption` | [InterruptNetwork](#interruptnetwork) | `Restore` |

All handles follow the same contract:

- Keep a non-nil handle even when the call returned an error. A lost reply or a
  canceled context can leave the change applied, so restore it or terminate the
  cluster.
- `Restore`/`Resume` is idempotent and retryable with a fresh context. Copies of a
  handle share state.
- `Restore` returns only the changed item to its captured prior state, including
  "absent". It refuses, with an error, to overwrite a value that someone else
  changed in the meantime. Writes of the same value cannot be detected.
- A second handle for the same item (configuration key, flag, module, ratios,
  container) is refused while the first is active.
- `Terminate` disposes of configuration, flags, modules, ratios and the blocklist
  with the cluster, so restoring is optional when the condition lasts until the
  end of the test. Pauses are resumed and network interruptions restored by
  `Terminate` before it removes anything.

Restore in `t.Cleanup` with a context that is still live. `t.Context()` is
canceled before cleanup functions run:

```golang
change, err := cluster.TemporaryConfig(ctx, setting)
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
containers first, releases host port reservations, stops services (RGW, MDS)
before OSDs, removes all owned daemons, helper containers, the OSD memory
volume and the shared run volume of the admin sockets, and finally removes the
owned networks. It works on a partially
initialized cluster returned by `Run` with an error, and a failed cleanup can be
retried. Each container gets a 2-second stop timeout instead of Docker's
10 seconds, because the data is discarded; a `testcontainers.StopTimeout` option
passed by the caller takes precedence.

Containers you attached with [WithClient](#withclient) or
[WithClientIdentity](#withclientidentity) are yours: terminate them before the
cluster, because Docker cannot remove a network that still has attached
containers. If an interrupted endpoint cannot be restored, its network is kept,
the restore failure is returned, and a later `Terminate` retries. With
`testcontainers.CleanupContainer`, register the cluster first and clients
afterwards.

#### Ceph

```golang
func (c *Container) Ceph(ctx context.Context, args ...string) ([]byte, error)
```

Runs the `ceph` CLI in the control container with `--connect-timeout 5` and
returns its standard output. Arguments are passed directly, without a shell. Add
`--format json` for machine-readable output. A non-zero exit returns an error
that includes the command's output.

```golang
out, err := cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "noscrub", "true")
```

See [Running native commands](#running-native-commands).

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

Do not pause or stop it; [PauseContainer](#pausecontainer) refuses it.

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
parsed. Role images selected with `WithOSDImage`, `rgw.WithImage` or
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
afterwards. A non-nil descriptor returned with an error stays owned.

#### RemoveManager

```golang
func (c *Container) RemoveManager(ctx context.Context, name string) error
```

Deletes an owned manager after confirming that another running, registered
candidate is available. Removing the last candidate is refused; add a replacement
first. Removing the active manager makes a standby take over. On failure the
handle stays owned for a retry or for cluster cleanup.

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

#### TemporaryOSDFlag

```golang
func (c *Container) TemporaryOSDFlag(ctx context.Context, flag string, enabled bool) (*ceph.OSDFlagOverride, error)
```

Sets or clears one global OSDMap flag and remembers its prior state. Supported
flags: `noup`, `nodown`, `noout`, `noin`, `nobackfill`, `norebalance`,
`norecover`, `noscrub`, `nodeep-scrub`, `nosnaptrim` and `noautoscale`. The
composite `pause` flag and irreversible compatibility flags are not supported.
`Restore` returns only this flag to its prior state (a flag that was already set
stays set) and never touches other flags. While set, flags such as `noout` raise
the `OSDMAP_FLAGS` health warning (per-OSD flags raise `OSD_FLAGS` instead).

```golang
hold, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
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
[TemporaryOSDFlag](#temporaryosdflag): small PGs recover through the PG log
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
  removal and then restored. It is refused while a `TemporaryConfig` handle holds
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
creations and revoked identities are rejected. You own the container. The
options are those of [WithClient](#withclient).

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
fresh client connection. After an uncertain error, inspect
[ClientCapabilities](#clientcapabilities) before retrying.

#### DeleteClient

```golang
func (c *Container) DeleteClient(ctx context.Context, client *ceph.ClientConfig) error
```

Revokes an identity created by this cluster, without deleting data. It refuses if
the native key was replaced externally. A repeated call after success is a no-op.
Established sessions may keep their tickets; test revocation with a new
connection. Credentials already copied elsewhere are not erased.

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

#### TemporaryConfig

```golang
func (c *Container) TemporaryConfig(ctx context.Context, setting ceph.ConfigSetting) (*ceph.ConfigOverride, error)
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
change, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
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
  are refused.

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

#### TemporaryMGRModule

```golang
func (c *Container) TemporaryMGRModule(ctx context.Context, name string, enabled bool) (*ceph.MGRModuleOverride, error)
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

#### TemporaryMessengerPolicy

```golang
func (c *Container) TemporaryMessengerPolicy(ctx context.Context, policy ceph.MessengerPolicy) (*ceph.MessengerPolicyOverride, error)
```

Changes the connection modes that the running daemons accept from clients (see
[WithMessengerPolicy](#withmessengerpolicy) for the fields). Each non-nil field
becomes a [TemporaryConfig](#temporaryconfig) override of its option; nil fields
stay as they are.

```golang
override, err := cluster.TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{
	Monitors: []ceph.MessengerConnectionMode{ceph.MessengerCRC},
})
if override != nil {
	defer override.Restore(ctx)
}
```

- The daemons apply the change to new connections without a restart.
  Established connections keep their mode.
- `MessengerPolicyOverride.Restore` returns every option to its previous value,
  including the bootstrap policy. It is idempotent and can be retried.
- A non-nil result returned with an error holds the overrides that were made;
  restore it.
- A single daemon, such as only `osd.0`, is changed with `TemporaryConfig` and
  the option in the table, with the daemon name as `Section`. It takes
  precedence over the daemon type's entry.
- It is refused with `MessengerV2Secure`.

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

#### TemporaryFullRatios

```golang
func (c *Container) TemporaryFullRatios(ctx context.Context, requested ceph.FullRatios) (*ceph.FullRatiosOverride, error)
```

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
see the connection close. The two methods below add other failure shapes. For
Messenger-level delays and dropped messages (`ms_blackhole_*`,
`ms_inject_delay_*` through `TemporaryConfig`), OSD flags, client blocklisting
and full ratios, see [Fault injection](guides/fault-injection.md).

#### PauseContainer

```golang
func (c *Container) PauseContainer(ctx context.Context, ctr testcontainers.Container) (*ceph.ContainerPause, error)
```

Freezes every process of an owned daemon or of a caller-owned `WithClient`
container through Docker's cgroup freezer, as `docker pause` does. Unlike `Stop`,
the process keeps its memory, sockets and Ceph identity, and peers see silence
instead of a closed connection: a frozen OSD stops answering client operations
and heartbeats until its peers report it down after the heartbeat grace. Hold
`nodown` with [TemporaryOSDFlag](#temporaryosdflag) to keep it up, so that client
operations hit their own timeouts (`rados_osd_op_timeout`) or are reported as
`SLOW_OPS` (after `osd_op_complaint_time`, 30 seconds by default). Works in host
mode.

The control container is refused (in a single-MON cluster that is MON `a`),
because every fixture command would block. A container that is already paused is
refused rather than adopted, and only one pause per container is allowed. The
returned `*ceph.ContainerPause` has `ContainerID` and `Resume`; `Resume` thaws the
container without restarting it and treats a removed or already running
container as done. `Terminate` resumes remaining pauses first; resume a
caller-owned container yourself before removing it.

```golang
hold, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
if hold != nil {
	defer hold.Restore(context.Background())
}
if err != nil {
	t.Fatal(err)
}
pause, err := cluster.PauseContainer(ctx, cluster.OSDs()[1])
if pause != nil {
	defer pause.Resume(context.Background())
}
if err != nil {
	t.Fatal(err)
}
// Client operations on PGs that include osd.1 now wait for their timeouts.
```

#### InterruptNetwork

```golang
func (c *Container) InterruptNetwork(ctx context.Context, ctr testcontainers.Container, plane ceph.NetworkPlane) (*ceph.NetworkInterruption, error)
```

Disconnects one container from one of the cluster's bridges without stopping it.
`ctr` can be an owned daemon or a caller-owned `WithClient` container.
`ceph.PublicNetworkPlane` selects the client/public bridge and
`ceph.ClusterNetworkPlane` the OSD replication bridge; with a single bridge both
select the same endpoint. Host mode is refused because Docker cannot disconnect a
host-network endpoint.

The returned `*ceph.NetworkInterruption` has `ContainerID`, `NetworkName` and
`OriginalAddress`. `Restore` reconnects the endpoint with its original IP and
aliases, so the address Ceph advertises stays valid and the daemon is not
restarted. An endpoint that someone else reattached with a different address is
refused. It is a whole-endpoint interruption, not a packet filter between
specific peers. The cluster keeps the handle and restores it during `Terminate`;
restore caller-owned containers explicitly before removing them.

```golang
cut, err := cluster.InterruptNetwork(ctx, cluster.OSDs()[0], ceph.ClusterNetworkPlane)
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
- A paused daemon (see [PauseContainer](#pausecontainer)) accepts the
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

It is not called automatically. Call it before cleanup, with its own short
context (the test context may already be canceled):

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
- Known admin and RGW secrets, `RedactValues` and named secret patterns are
  masked, but redaction is best effort; review arbitrary application logs before
  sharing a report.

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

#### InterruptNetwork

```golang
func InterruptNetwork(ctx context.Context, ctr testcontainers.Container, name string) (*ceph.NetworkInterruption, error)
```

Disconnects one existing container from the Docker bridge network `name` and
returns a restoration handle. Unlike the
[method of the same name](#interruptnetwork), it works for any bridge network,
for example a network shared by containers of two clusters, and the caller owns
both the container and the handle; nothing restores it automatically. It does
not stop a daemon, change Ceph policies or remove the network. Host networking
cannot be interrupted. A non-nil handle returned with an error must be restored,
because a transport failure cannot prove that Docker did not disconnect the
endpoint.

#### ErrHostNetworkUnavailable

```golang
var ErrHostNetworkUnavailable error
```

Wrapped by the error that `Run` returns in host mode when the engine does not run
Linux containers, the [WithHostAddress](#withhostaddress) address is not assigned
on the Docker host, or the test process cannot reach the first MON's advertised
endpoint. `Run` returns it before creating managers and OSDs; later MON and RGW
port reservations can also return it. The error message explains the likely
cause for the detected Docker environment.

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
)
```

The bootstrap connection policy for [WithMessengerMode](#withmessengermode).
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
[InterruptNetwork](#interruptnetwork).

#### Daemon handles

| Type | Fields and methods | Notes |
| --- | --- | --- |
| `MonitorContainer` | embeds `testcontainers.Container`; `DaemonName` | `Stop`/`Start` keep the store. See [Monitors](#monitors). |
| `ManagerContainer` | embeds `testcontainers.Container`; `DaemonName` | Active or standby. See [Managers](#managers). |
| `OSDContainer` | embeds `testcontainers.Container`; `ID`; `Placement() OSDConfig` | Backed by a sparse BlueStore file. `Placement` returns a copy of the resolved CRUSH location. See [OSDs](#osds). |

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
| `ConnectionConfig() ([]byte, []byte, error)` | Independent copies of a `ceph.conf` and the identity's keyring for a client you run yourself. The configuration is a snapshot taken at creation; after replacing monitors, update `mon_host` (see [MonitorBootstrapAddresses](#monitorbootstrapaddresses)). Revocation does not erase copies. |
| `String() string`, `GoString() string` | A description that never includes credentials, safe for logs and `%v`/`%#v`. |

#### Restoration handles

| Type | Methods | Created by |
| --- | --- | --- |
| `ConfigOverride` | `Restore(ctx) error`; `String()`, `GoString()` for logging | [TemporaryConfig](#temporaryconfig) |
| `OSDFlagOverride` | `Restore(ctx) error` | [TemporaryOSDFlag](#temporaryosdflag) |
| `MGRModuleOverride` | `Restore(ctx) error` | [TemporaryMGRModule](#temporarymgrmodule) |
| `FullRatiosOverride` | `Restore(ctx) error` | [TemporaryFullRatios](#temporaryfullratios) |
| `BlocklistOverride` | `Address() string`, `Restore(ctx) error` | [TemporaryBlocklist](#temporaryblocklist) |
| `ContainerPause` | `ContainerID`; `Resume(ctx) error` | [PauseContainer](#pausecontainer) |
| `NetworkInterruption` | `ContainerID`, `NetworkName`, `OriginalAddress`; `Restore(ctx) error` | [InterruptNetwork](#interruptnetwork) (method or function) |

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
| `ConfigSetting` | [TemporaryConfig](#temporaryconfig) |
| `ConfigEntry` | [Configuration](#configuration) |
| `MGRModuleState` | [MGRModules](#mgrmodules) |
| `Status` | [Status](#status) |
| `QuorumStatus` | [QuorumStatus](#quorumstatus) |
| `ManagerStatus` | [ManagerStatus](#managerstatus) |
| `HealthSnapshot`, `HealthCheck`, `HealthMute` | [HealthDetails](#healthdetails) |
| `ObjectWatcher` | [ObjectWatchers](#objectwatchers) |
| `BlocklistEntry` | [BlocklistEntries](#blocklistentries) |
| `FullRatios`, `FullRatioSnapshot` | [FullRatios](#fullratios), [TemporaryFullRatios](#temporaryfullratios) |
| `InconsistentObject`, `InconsistentShard` | [PGInconsistencies](#pginconsistencies) |
| `DiagnosticsConfig`, `DiagnosticsContainer`, `DiagnosticsReport`, `DiagnosticArtifact` | [CollectDiagnostics](#collectdiagnostics) |

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
replica and no `hashpspool` flag. The fixture has no option for a hand-written
CRUSH map; install it with the native tools, as below. `crushtool` is in the
official image and the published control images.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(1),
	ceph.WithPoolDefaults(1, 1),
	ceph.WithOSDObjectStore(ceph.OSDMemStore),
	ceph.WithOSDBlockSize(256<<20),
)
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}

// Keep OSDs from moving themselves into host buckets or setting a device
// class when they start, so the installed map stays as written.
for _, name := range []string{"osd_crush_update_on_start", "osd_class_update_on_start"} {
	if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: name, Value: "false"}); err != nil {
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
if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
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
	if _, err := cluster.TemporaryConfig(ctx, setting); err != nil {
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
hold, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
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
	hold, err := cluster.TemporaryOSDFlag(ctx, flag, true)
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
change, err := cluster.TemporaryFullRatios(ctx, ceph.FullRatios{
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
pause, err := cluster.PauseContainer(ctx, watcher)
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
module, err := cluster.TemporaryMGRModule(ctx, "prometheus", true)
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
// Scrape from a WithClient container, for example:
// python3 -c 'import sys,urllib.request; print(urllib.request.urlopen(sys.argv[1]+"metrics").read().decode())' <url>
```

In bridge mode each manager listens on its own container address on port 9283,
and standbys answer with an empty 200 response. After a failover
(`cluster.Ceph(ctx, "mgr", "fail")`), `ManagerServices` returns the new active
manager's URL.

In host mode all managers share the host network, so give each manager its own
port and bind the public address before enabling the module (using
[TemporaryConfig](#temporaryconfig)). With the default settings and two
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

See [CollectDiagnostics](#collectdiagnostics). Add caller-owned clients and
mirror daemons with `DiagnosticsConfig.AdditionalContainers` and mask
application secrets with `RedactValues`.

## Running native commands

Everything without a typed helper can be done with Ceph's own tools in the
cluster's containers:

| Task | How |
| --- | --- |
| Any `ceph` command | `cluster.Ceph(ctx, "osd", "pool", "set", "app-data", "nodelete", "true")` |
| JSON output | `cluster.Ceph(ctx, "osd", "dump", "--format", "json")` and `json.Unmarshal` |
| `rados`, `rbd`, `ceph-authtool`, Python clients | `cluster.ControlContainer().Exec(ctx, []string{"rados", "-p", "app-data", "ls"}, tcexec.Multiplexed())` |
| Daemon admin socket commands | `cluster.Ceph(ctx, "tell", "osd.0", "dump_ops_in_flight")` |
| A daemon's effective configuration | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_queue")` |
| Any central option | [TemporaryConfig](#temporaryconfig), so it is restored |
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
- Only IPv4 is configured (`ms_bind_ipv6 = false`).

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
  (`3300/tcp` and `6789/tcp`, or only `3300/tcp` with `MessengerV2Secure`).
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
  values with `TemporaryConfig` when a test needs them sooner.
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
- A key set with `WithConfigFile` cannot be changed with `TemporaryConfig`;
  `mon_allow_pool_delete = false` in a config file blocks `RemovePool`.
- `t.Context()` is canceled before cleanup functions run. Use a fresh context in
  `t.Cleanup` for `Restore`, `RemovePool` and `CollectDiagnostics`.
- Terminate client containers before the cluster: register their cleanup after
  the cluster's.
- Keys copied by `WithClient` and `ConnectionConfig` are the cluster's admin
  credentials.
- `WaitForClean` and other waits are capped by `WithStartupTimeout` even if your
  context allows more time.
