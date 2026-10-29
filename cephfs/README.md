# CephFS

## Introduction

The Testcontainers module for CephFS.

The `cephfs` package starts a disposable Ceph cluster with one or more CephFS
filesystems and prepares the server side of CephFS client tests:

- filesystems and their MDS topology: active, standby and standby-replay
  daemons, failure injection, replacement of stopped daemons;
- data pools attached at creation or while the filesystem runs;
- subvolume groups and subvolumes with quotas, data pool layouts and RADOS
  namespaces;
- subvolume snapshots and asynchronous clones;
- Cephx principals restricted to one subvolume;
- directory pins, quiesce sets and MDS client session limits;
- one-way snapshot mirroring of directories between two clusters (the separate `cephfs/mirror` package, documented in [`cephfs/mirror`](mirror/README.md)).

The Go module does not link go-ceph or cgo and never mounts CephFS on the test
host. File I/O is done by the client under test: libcephfs, go-ceph or another
native client running in a Linux container attached to the cluster, or in the
test process itself when the cluster uses host networking on Linux. The fixture
drives Ceph through the `ceph` CLI in the cluster's control container.

The cluster itself (MON, MGR, OSD, pools, Cephx, central configuration, fault
injection, health) is documented in [Ceph](../ceph/README.md).

## Adding this module to your project dependencies

Please run the following command to add the CephFS module to your Go dependencies:

```
go get github.com/JSYoo5B/ceph-testcontainers-go/cephfs
```

The module needs Go 1.26 or later and a Docker engine that runs Linux
containers.

## Usage example

The test below starts a cluster with the default filesystem, attaches a client
container and writes a file through libcephfs (the Python binding shipped in the
default Ceph image).

```golang
package cephfs_test

import (
	"io"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// writeAndRead runs inside the client container.
const writeAndRead = `import cephfs, os, sys
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=sys.argv[1].encode())
try:
    fd = fs.open('/hello.txt', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    fs.write(fd, b'hello from cephfs', 0)
    fs.fsync(fd, 0)
    fs.close(fd)
    fd = fs.open('/hello.txt', os.O_RDONLY)
    print(fs.read(fd, 0, 64).decode())
    fs.close(fd)
finally:
    fs.shutdown()
`

func TestCephFS(t *testing.T) {
	ctx := t.Context()

	cluster, err := cephfs.Run(ctx, ceph.DefaultImage)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	fs := cluster.Filesystems()[0] // the default filesystem tc-cephfs

	// The client joins the cluster network and receives ceph.conf and the
	// admin keyring. Register it after the cluster so it is removed first.
	client, err := testcontainers.Run(ctx, ceph.DefaultImage,
		cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
	)
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}

	code, reader, err := client.Exec(ctx,
		[]string{"python3", "-c", writeAndRead, fs.FilesystemName}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, _ := io.ReadAll(reader)
	if code != 0 || !strings.Contains(string(output), "hello from cephfs") {
		t.Fatalf("client exited %d: %s", code, output)
	}
}
```

## Module Reference

### Run function

The CephFS module exposes one entrypoint function to create the CephFS container, and this function receives three parameters:

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error)
```

- `context.Context`, the Go context.
- `string`, the Docker image to use.
- `testcontainers.ContainerCustomizer`, a variadic argument for passing options.

`Run` starts the same cluster as `ceph.Run` (one MON, one MGR and two OSDs by
default) and creates the filesystems selected with `WithFilesystems`. Without
that option it creates the default filesystem `tc-cephfs` with the pools
`tc-cephfs-metadata` and `tc-cephfs-data` and one active MDS named `a`, and
returns after rank 0 is `up:active`.

The returned `*cephfs.Container` embeds `*ceph.Container`, so every method
documented in [Ceph](../ceph/README.md) is available on it. Package functions that take a
`*ceph.Container` (`Start`, `Filesystems`, and the `Source` and
`Destination` fields of `cephfsmirror.Config`) take the embedded value, for example
`cephfs.Start(ctx, cluster.Container, config)`. The container's `Filesystems`
method is a shortcut for `cephfs.Filesystems(cluster.Container)`.

`Run` accepts every `ceph.Run` option and the options of the other service
packages, such as `rbd.WithPools` and `rgw.WithGateways`, so one cluster can
serve RBD, CephFS and S3 clients. Bootstrap options that leave the cluster
without the OSDs or managers a filesystem needs, such as
`ceph.WithNoInitialOSDs()` and `ceph.WithNoInitialManagers()`, are rejected
before any container is created. For staged bootstraps, start the cluster with
`ceph.Run` and add the filesystem later with `Start`.

A non-nil container returned together with an error must still be terminated,
so register `testcontainers.CleanupContainer` before checking the error. The
cluster owns its MDS containers: terminating the cluster removes them,
including stopped and partially started daemons.

#### Image

Use the second argument in the `Run` function to set a valid Docker image.
The default is `ceph.DefaultImage`, the pruned Ceph Tentacle 20.2.4 aggregate
`quay.io/ceph/ceph:v20.2.4` pinned by its multi-architecture digest. It contains
every role, including `cephfs-mirror` and the Python `cephfs` and `rados`
bindings.

The image passed to `Run` runs the MON, the MGR and the control container that
executes the fixture's `ceph` commands. Other roles can use separate images:

| Role | Image | Required content |
| --- | --- | --- |
| MON, MGR, control CLI | `Run` image | `ceph` CLI, MGR modules (`volumes`, `mirroring`), Python `cephfs` binding (pin policy reads and clone identity checks run libcephfs in the control container) |
| OSD | `ImageSet.Essential` | `ceph-osd` |
| MDS | `cephfs.WithMDSImage`, otherwise the `Run` image | `ceph-mds` |
| CephFS mirror daemon | `image` argument of `cephfsmirror.Run`; empty selects `FactsOf(source).Images.CephFS` | `cephfs-mirror` and the `ceph` CLI used to query its admin socket |
| Client containers | chosen by the test | the client under test; `ceph.DefaultImage` provides Python libcephfs |

Use the same Ceph release for every role. `FactsOf(cluster).CephVersion` reports the
release of the `Run` image, which the fixture uses to pick release-specific
commands. Squid 19.2.5 is supported (see
[Limitations and release differences](#limitations-and-release-differences)).
Role images and release selection are covered in
[Releases and images](../docs/guides/releases-and-images.md).

### Container Options

When starting the CephFS container, you can pass options in a variadic way to configure it.

#### WithFilesystems

```golang
func WithFilesystems(configs ...Config) ceph.Option
```

Creates the named filesystems and their MDS daemons while `Run` starts the
cluster. It works with `cephfs.Run`, `ceph.Run`, `rbd.Run` and `rgw.Run`. With no
arguments it selects the default filesystem. Repeating the option replaces the
earlier list. `Start` adds further filesystems after `Run` returns.

```golang
cluster, err := cephfs.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(3),
	cephfs.WithFilesystems(
		cephfs.Config{Name: "app-fs", ActiveMDS: 2, StandbyMDS: 1},
		cephfs.Config{Name: "scratch"},
	),
)
```

`Config` fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `Name` | `tc-cephfs` | Filesystem name: letters, digits, `_`, `.` or `-`, starting with a letter, digit or `_`. |
| `MetadataPool` | `ceph.PoolConfig{Name: "<Name>-metadata"}` | Metadata pool. Must be replicated. |
| `DataPool` | `ceph.PoolConfig{Name: "<Name>-data"}` | Default data pool. Must be replicated. It also stores inode backtraces and cannot be detached. |
| `AdditionalDataPools` | none | Further data pools, replicated or erasure coded with `AllowOverwrites: true`. Select them through subvolume, group or clone `DataPool` fields or directory layouts. |
| `ActiveMDS` | 1 | Number of active ranks (`max_mds`), 1 to 256. |
| `StandbyMDS` | 0 | Number of standby daemons. |
| `StandbyReplay` | `false` | Turns standbys into journal followers, at most one per active rank; extra standbys stay ordinary. Requires `StandbyMDS >= 1`. |
| `NoInitialMDS` | `false` | Creates the filesystem and pools without any MDS. See below. |

Every pool gets `application=cephfs`; another application value is rejected.
Pool placement, replica counts and PG counts follow `ceph.PoolConfig` (see
[Ceph](../ceph/README.md)). All pools and OSD placements are validated before anything is
created.

MDS daemon names are fixed by the fixture. The first MDS of the default
filesystem is `a`; every other daemon is `<Name>-<index>` (`app-fs-0`,
`app-fs-1`, ...). Indexes are never reused, so a replacement daemon gets a new
name. Each standby is bound to its own filesystem and never takes over a rank of
another filesystem. Creating a second filesystem sets Ceph's global
`enable_multiple` flag.

**Creating a filesystem without an MDS.** `NoInitialMDS: true` creates the
filesystem and all its pools but no MDS, no MDS Cephx key and no MDS container.
Use it for deferred provisioning and for testing clients against a filesystem
that has no metadata service. Leave `ActiveMDS` and `StandbyMDS` at zero and
`StandbyReplay` false; conflicting values are rejected before anything is
created. The filesystem's `max_mds` is still one.

While the filesystem is cold, its descriptor has a nil embedded container and an
empty `MDSs()`; do not call promoted container methods on it. `WaitReady` keeps
waiting until its deadline, and a fresh libcephfs mount fails after
`client_mount_timeout` with errno 110 (`ETIMEDOUT`). Ceph reports `MDS_ALL_DOWN`
for the filesystem. The first MDS is started with `ScaleMDS(ctx, 1, 0)`; after
that the filesystem behaves like any other.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
fs, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "deferred", NoInitialMDS: true})
if err != nil {
	t.Fatal(err)
}
// ... run the cold-filesystem part of the test ...
if err := fs.ScaleMDS(ctx, 1, 0); err != nil { // starts deferred-0
	t.Fatal(err)
}
if err := fs.WaitReady(ctx); err != nil {
	t.Fatal(err)
}
```

The first `ScaleMDS` must request exactly 1 active and 0 standby daemons. It
refuses to start if the filesystem or its pools were recreated, its policy
changed, or a foreign MDS already serves it. A call that is canceled or refused
before Ceph is changed can be retried. Once the first MDS start has been
attempted, a failure is not retried automatically: any partially started MDS
stays owned by the cluster for cleanup.

#### WithMDSImage

```golang
func WithMDSImage(image string) ceph.Option
```

Selects the image for every MDS container started by `Run`, `Start`,
`ScaleMDS` and `MDS.AddReplacement`. Without it, MDS daemons use the `Run` image.
Use an image of the same Ceph release.

```golang
cluster, err := cephfs.Run(ctx, "registry.example.com/ceph:20.2.4-all",
	cephfs.WithMDSImage("registry.example.com/ceph:20.2.4-mds"),
)
```

The generic options of the `testcontainers` package (for example
`testcontainers.WithEnv`, `testcontainers.WithLogger` and
`testcontainers.CustomizeRequestOption`) also apply. Passed to `Run`, they
customise only the MON container. To customise MDS containers, pass customizers
to `Start`; mirror daemons take customizers from `cephfsmirror.Run` and
`Mirror.AddDaemon`. The cluster options of `ceph.Run` (`ceph.WithOSDCount`,
`ceph.WithHostNetwork`, `ceph.WithStartupTimeout`, `ceph.WithConfigFile`, ...)
are documented in [Ceph](../ceph/README.md).

### Container Methods

#### Filesystems

```golang
func (c *Container) Filesystems() []*Filesystem
```

Returns the cluster's filesystems sorted by name, like
`cephfs.Filesystems(c.Container)`. A cluster from `Run` without
`WithFilesystems` has one, `tc-cephfs`.

The returned container also embeds `*ceph.Container`; its methods are
documented in [Ceph](../ceph/README.md). The ones CephFS tests use most are:

| Method | Use in CephFS tests |
| --- | --- |
| `Ceph(ctx, args...)` | Run `ceph fs ...`, `ceph tell mds...` and other CLI commands in the control container. |
| `WithClient()` | Attach a client container with `ceph.conf` and the admin keyring. |
| `WithClientIdentity(client)` | Attach a client container with only one principal's keyring, for example `SubvolumeAuthorization.Client`. |
| `CreateClient`, `ClientConfig.Delete` | Create or revoke a principal with hand-written caps (`ceph.ClientCaps{MDS: "allow rw fsname=app-fs path=/app", ...}`). |
| `ConnectionConfig(ctx)` | `ceph.conf` and admin keyring bytes for a native client in the test process (host networking). |
| `override.SetConfig(ctx, cluster, setting)` | Change a central option for MDS daemons or clients and restore it. |
| `fault.Pause(ctx, cluster, ctr)` | Freeze a client or MDS container without stopping its process. |
| `Observer.BlocklistEntries(ctx)` | List OSDMap blocklist entries, for example after a client eviction. |
| `CreatePool`, `Pool.Remove`, `WaitForClean` | Create a pool to attach with `AddDataPool`, delete it afterwards, wait for clean PGs. |
| `ControlContainer(ctx)`, `ClientContainer(ctx)` | The control container (`ceph` CLI, essential image) and the client toolkit. Mirror daemons use `ClusterFacts.Images.CephFS` or `ClusterFacts.Images.RBDAux`. |
| `ServiceContainers()` | Owned MDS (and RGW) containers. |
| `ceph.FactsOf(cluster)` | The static facts, including `CephVersion`, the Ceph release of the `Run` image. |

### Filesystem functions and types

#### Start

```golang
func Start(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Filesystem, error)
```

Creates one filesystem with its own pools and `ActiveMDS + StandbyMDS` daemons
on a running cluster and waits until it is ready. A zero `Config` selects the
default filesystem `tc-cephfs` with one active MDS named `a`. Existing
filesystems are rejected rather than modified; existing filesystems and their
settings are preserved when a second one is added.

The customizers apply to every MDS of this filesystem, including daemons added
later by `ScaleMDS`. They must not change the daemon's identity, configuration
files, keyring, entrypoint or networking.

A non-nil result returned with an error describes the attempted resources. The
cluster still owns their cleanup, including partially started MDS containers.

```golang
fs, err := cephfs.Start(ctx, cluster.Container, cephfs.Config{
	Name:       "app-fs",
	ActiveMDS:  1,
	StandbyMDS: 1,
	AdditionalDataPools: []ceph.PoolConfig{{
		Name: "app-fs-ec",
		// K=2, M=1 needs three OSDs: ceph.WithOSDCount(3).
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	}},
})
```

#### Filesystems

```golang
func Filesystems(c *ceph.Container) []*Filesystem
```

Returns the cluster's owned filesystem descriptors sorted by name, including a
descriptor whose setup partially failed.

#### Filesystem.Remove

```golang
func (fs *Filesystem) Remove(ctx context.Context) error
```

Deletes this owned filesystem. It gracefully stops its ranks and removes the filesystem in the FSMap,
terminates its MDS daemons, deletes their Cephx keys, and deletes the metadata,
default data and `AdditionalDataPools` pools created from its `Config`, together
with their CRUSH rules and erasure-code profiles. Pools attached later with
`AddDataPool` are detached but kept; delete them with [Pool.Remove](../ceph/README.md#poolremove).

All file data in the deleted pools is destroyed. Stop clients, mirrors and
subvolume operations on the filesystem first; their handles fail afterwards. A
retry after an error continues with whatever remains. A descriptor that was
already removed, or that is not the one the cluster owns for its name, returns an
error instead of acting on a later filesystem of the same name.

Removal requests `fs set <name> down true`, then requires the same filesystem's
explicit native `up` map to be empty before `fs rm` and resource cleanup. A rank
that is stopping or reconnecting still blocks removal. This uses the ordinary
[graceful shutdown](https://docs.ceph.com/en/squid/cephfs/administration/#taking-the-cluster-down)
contract, including journal flushing, rather than forcing `fs fail` through a
health warning. Target warnings do not authorize a forced removal: the target
must finish draining even when it has warnings. Errors, cancellation or timeout
leave ownership intact and may leave the filesystem marked down; no automatic
force fallback or rollback runs. A fresh-context retry reconciles the current
state. The caller must still stop operations on the target filesystem first.

This common sequence avoids the unrelated-MDS warning refusal in affected
`fs fail` implementations without selecting by runtime release strings or
registering a compatibility tag.

#### Filesystem

`*cephfs.Filesystem` describes one filesystem:

| Field | Meaning |
| --- | --- |
| `FilesystemName` | Filesystem name. Pass it to clients (`filesystem_name` in libcephfs). |
| `MetadataPool`, `DataPool` | Pool names at creation. |
| `AdditionalDataPools` | Additional pool names from `Config`. `DataPools` reports the current registration. |
| embedded `testcontainers.Container` | One MDS container. It is nil while a `NoInitialMDS` filesystem is cold and is moved to a surviving daemon when the original is retired. Use `MDSs()` for the topology. |

#### MDS

`*cephfs.MDS` is one owned metadata daemon. `ID` is its fixed Ceph daemon name
(`mds.<ID>`) and `FilesystemName` the filesystem it serves. The embedded
`testcontainers.Container` provides `Stop` and `Start` for failure injection;
`fault.Pause` freezes it instead. A restarted daemon keeps its name
but gets a new GID. The cluster removes every MDS container when it terminates.

On a `ceph.AddressDualStack` cluster every MDS advertises a concrete IPv4 and
IPv6 address, IPv4 first. Clients reach an MDS through the first entry, so
`ms_bind_prefer_ipv4 = false` in its `[mds.<ID>]` section of a
`ceph.WithConfigFile` file makes them use IPv6; see
[Dual-stack clusters](../ceph/README.md#dual-stack-clusters).

### Handles and ownership

The filesystem methods below share these rules:

- Handles returned by `Create*`, `AddDataPool`, `AuthorizeSubvolume`,
  `CloneSubvolumeSnapshot`, `QuiesceSubvolumes` and `Temporary*` are owned by
  the fixture. Their exported fields describe the resource at creation.
  Changing a field does not redirect later operations, which use the identity
  captured at creation (filesystem ID, pool IDs, subvolume UUID path, inode and
  birth time, Cephx key). Copies of a handle share its state.
- A non-nil handle returned with an error records an attempted or uncertain
  operation. Keep it: methods that document a retry reconcile the native state
  on the next call with a fresh context. Anything left over is removed when the
  cluster terminates.
- Read-only queries (`SubvolumeInfo`, `Subvolumes`, `SubvolumeSnapshotInfo`,
  `SubvolumeAuthorizedClients`, ...) also report resources created outside the
  fixture, but they grant no ownership: such resources cannot be resized,
  removed or cloned through these methods.
- The Ceph CLI has no compare-and-swap for these resources. Do not run external
  `ceph fs ...` edits on the same resources while a fixture operation is in
  progress.
- Fixture operations wait for each other within the caller's context. A call
  canceled while waiting returns without changing Ceph and can be retried with a
  new context.

Fixture termination removes the owned resources after the test. Use explicit
resource cleanup to assert removal behavior or inspect intermediate state.

### Filesystem Methods: MDS topology

#### WaitReady

```golang
func (fs *Filesystem) WaitReady(ctx context.Context) error
```

Waits until every configured rank is held by an owned `up:active` daemon and the
configured number of standbys (and, with `StandbyReplay`, journal followers) are
registered. The wait is bounded by the cluster startup timeout
(`ceph.WithStartupTimeout`, three minutes by default) or the caller's earlier
deadline.

Readiness includes standby capacity. After a standby has replaced a failed
daemon, `WaitReady` fails until the standby count is restored by restarting the
old container, or by `MDS.RemoveStopped` followed by `ScaleMDS`. Use `MDSStatus`
to check that a rank recovered.

#### MDSStatus

```golang
func (fs *Filesystem) MDSStatus(ctx context.Context) (*FilesystemStatus, error)
```

Reads one FSMap epoch and reports, for this filesystem:

| `FilesystemStatus` field | Meaning |
| --- | --- |
| `FilesystemName`, `FilesystemID` | Name and native filesystem ID (FSCID). |
| `MaxMDS` | Current `max_mds`. |
| `Active` | Active ranks in rank order, including daemons not owned by the fixture. |
| `Standby` | This fixture's ordinary standbys bound to this filesystem. |
| `StandbyReplay` | This fixture's journal followers. |

Each entry is an `MDSStatus` with `Name`, `Rank` (-1 for an ordinary standby;
the followed rank for a replay standby), `GID` (changes when the process
restarts), `State` (`up:active`, `up:standby`, `up:standby-replay`, ...) and
`Owned`. Standbys of other filesystems are never counted.

```golang
status, err := fs.MDSStatus(ctx)
if err != nil {
	t.Fatal(err)
}
active := status.Active[0] // rank 0
t.Logf("rank 0 is mds.%s gid %d", active.Name, active.GID)
```

#### MDSs

```golang
func (fs *Filesystem) MDSs() []*MDS
```

Returns a snapshot of the filesystem's owned daemons in creation order,
including stopped daemons. Match handles to ranks by comparing `MDS.ID` with
`MDSStatus.Name`.

#### ScaleMDS

```golang
func (fs *Filesystem) ScaleMDS(ctx context.Context, active, standby int) error
```

Changes the number of active ranks (1 to 256) and standby daemons of an existing
filesystem without touching its pools or files. New daemons get new indexed
names and the customizers given to `Start`. When the active count shrinks, Ceph
first hands the retired ranks back, then the fixture stops the surplus daemons;
ordinary standbys are removed before journal followers. Scaling to zero standbys
keeps the filesystem's standby-replay setting.

`ScaleMDS` confirms Ceph's `max_mds` health warning (`--yes-i-really-mean-it`),
which is acceptable for a disposable cluster. Run it while the owned daemons are
healthy and without concurrent failure injection or FSMap edits. On error the
requested counts and every partial resource are kept for cleanup; the filesystem
and its data are never deleted. For a `NoInitialMDS` filesystem the first call
must be `ScaleMDS(ctx, 1, 0)`.

```golang
if err := fs.ScaleMDS(ctx, 2, 1); err != nil {
	t.Fatal(err)
}
if err := fs.WaitReady(ctx); err != nil {
	t.Fatal(err)
}
```

#### MDS.RemoveStopped

```golang
func (m *MDS) RemoveStopped(ctx context.Context) error
```

Removes the container of an original, stopped MDS after Ceph has dropped it.
Use it after a failover when the test wants to retire the failed daemon instead
of restarting it. Preconditions:

- this `*cephfs.MDS` comes from `MDSs()` and started successfully (partially started
  daemons stay owned for cluster cleanup);
- its container is stopped, and its name is absent from every filesystem and
  from the global standby list. A stopped daemon that is still registered is
  refused. Ceph drops it after `mds_beacon_grace` (15 seconds by default), or at
  once with `ceph mds fail <gid>`;
- healthy owned daemons still hold every configured active rank (after
  `MDS.AddReplacement`, the replacement counts).

It does not fail an MDS in Ceph, delete its Cephx key or change the requested
counts. To restore standby capacity afterwards, call `ScaleMDS` with the
original counts; it starts a daemon with a new name (the old key is not reused).
If the removed daemon backed the filesystem's embedded container, the embedded
container is moved to a surviving daemon.

If removing the container fails or its reply is lost, the handle and its
registration are kept: retry with the same handle (or a copy) after fixing the
reported problem. A repeated call after success does not remove anything again.

#### MDS.AddReplacement

```golang
func (m *MDS) AddReplacement(ctx context.Context) (*MDS, error)
```

Starts a replacement for the only MDS of a filesystem that has lost it. It
applies to a filesystem configured with one active and no standby daemon and no
standby replay. Preconditions:

- this `*cephfs.MDS` is that filesystem's original daemon, from `MDSs()`, and
  its container is stopped;
- Ceph has failed rank 0: the old name is absent from the FSMap and no standby
  can take the rank. Without a standby, Ceph can keep a dead daemon registered
  as laggy, so fail its GID explicitly with `ceph mds fail <gid>`.

The replacement gets a new indexed name and Cephx key; pools, data and the
requested counts are unchanged. Once its rank is active, call
`stopped.RemoveStopped(ctx)` to retire the old container. `MDS.AddReplacement`
never fails a daemon in Ceph, deletes keys, removes the old container or changes
`max_mds`.

Retrying after success returns the same replacement handle without starting
another daemon. If the start fails after the new key or container was created,
the request can only be cleaned up: a returned non-nil handle stays owned by the
cluster, a retry returns it with an error and starts nothing, and `ScaleMDS` and
`MDS.RemoveStopped` are refused for this filesystem. Do not start, stop or fail
daemons by hand while it runs. See
[Fail over an MDS and replace a stopped MDS](#fail-over-an-mds-and-replace-a-stopped-mds).

### Filesystem Methods: data pools

#### AddDataPool

```golang
func (fs *Filesystem) AddDataPool(ctx context.Context, name string) (*DataPool, error)
```

Attaches an existing pool as an additional data pool of a running filesystem.
The pool must have only the `cephfs` application with no filesystem role yet,
and must be replicated or erasure coded with `allow_ec_overwrites`. Metadata
pools, pools of another filesystem and repeated attachments are refused. The
method neither creates nor deletes pools.

The returned `DataPool` has `Name`, `FilesystemName` and the native pool `ID`.
A handle returned with an error records an attempted attachment; calling
`AddDataPool` again with the same name reconciles it.

```golang
_, err := cluster.CreatePool(ctx, ceph.PoolConfig{
	Name:        "app-ec-data",
	Application: "cephfs",
	ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}, // needs 3 OSDs
})
if err != nil {
	t.Fatal(err)
}
attachment, err := fs.AddDataPool(ctx, "app-ec-data")
if err != nil {
	t.Fatal(err)
}
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
	Name: "ec-backed", DataPool: attachment.Name, NamespaceIsolated: true,
})
```

After a pool is detached, Ceph leaves the `data` key of its `cephfs`
application tag in place. Clear it before attaching the pool again:
`cluster.Ceph(ctx, "osd", "pool", "application", "rm", name, "cephfs", "data")`.

#### DataPools

```golang
func (fs *Filesystem) DataPools(ctx context.Context) ([]DataPoolState, error)
```

Reads the current data pool registration from the FSMap. Each `DataPoolState`
has `Name`, `ID` (distinguishes a pool recreated under the same name) and
`Default` (the first data pool, which cannot be detached). Unlike the
`AdditionalDataPools` field, it reflects later attachments and removals.

#### RemoveUnusedDataPool

```golang
func (fs *Filesystem) RemoveUnusedDataPool(ctx context.Context, attachment *DataPool) error
```

Detaches a pool attached by `AddDataPool` that was never used. It is refused if
any group, subvolume or clone created through the fixture ever selected the
pool (even a partial creation), if Ceph reports group or subvolume references,
if any RADOS namespace in the pool holds objects, or if clone or retained
snapshot information is unknown. The default data pool cannot be detached. No
pool, application tag or data is deleted.

Ceph cannot detect every POSIX layout reference (`ceph.dir.layout.pool` set by
a client). Stop external clients and reset such layouts before calling it. A
copied handle shares the removal state, and a retry reconciles a lost reply.

### Filesystem Methods: subvolume groups and subvolumes

Subvolumes are directory trees managed by Ceph's MGR `volumes` module. These
methods work inside an existing filesystem; they never create pools,
filesystems, MDS daemons or orchestrator services.

#### CreateSubvolumeGroup

```golang
func (fs *Filesystem) CreateSubvolumeGroup(ctx context.Context, config SubvolumeGroupConfig) (*SubvolumeGroup, error)
```

Creates a new group. `SubvolumeGroupConfig` fields:

| Field | Meaning |
| --- | --- |
| `Name` | Group name. An existing group is rejected (Ceph's own `create` would silently change its quota and layout). |
| `SizeBytes` | Aggregate quota of the group directory. Zero means unlimited. |
| `DataPool` | Optional data pool of this filesystem for the group's layout. |

The returned `SubvolumeGroup` has `Name`, `FilesystemName` and `Path`. A non-nil
handle returned with an error records an attempted creation; only a confirmed
handle can be resized or removed.

#### CreateSubvolume

```golang
func (fs *Filesystem) CreateSubvolume(ctx context.Context, config SubvolumeConfig) (*Subvolume, error)
```

Creates a new subvolume. `SubvolumeConfig` fields:

| Field | Meaning |
| --- | --- |
| `Name` | Subvolume name. Duplicates are rejected. |
| `GroupName` | Existing group. Empty selects Ceph's default group. |
| `SizeBytes` | Quota (`ceph.quota.max_bytes`). Zero means unlimited. |
| `DataPool` | One of the filesystem's current data pools. Empty inherits the group layout. Unknown pools are rejected before creation. |
| `NamespaceIsolated` | Stores file data in a separate RADOS namespace. Required for `AuthorizeSubvolume`. It does not grant or restrict access by itself. |

The returned `Subvolume` has `Name`, `GroupName`, `FilesystemName` and `Path`,
the real directory a client mounts (`/volumes/<group>/<name>/<uuid>`). A
non-nil handle returned with an error identifies an attempted creation; partial
data is never deleted automatically.

```golang
group, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{
	Name: "app", SizeBytes: 128 << 20,
})
if err != nil {
	t.Fatal(err)
}
volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{
	Name: "tenant-a", GroupName: group.Name, SizeBytes: 16 << 20, NamespaceIsolated: true,
})
if err != nil {
	t.Fatal(err)
}
// volume.Path is the mount root for the client under test.
```

#### ResizeSubvolumeGroup

```golang
func (fs *Filesystem) ResizeSubvolumeGroup(ctx context.Context, group *SubvolumeGroup, sizeBytes int64) error
```

Changes an owned group's quota. Zero removes the limit. A positive limit below
the current usage is refused (`--no_shrink`); files are never truncated. Usage
accounting and quota enforcement are native CephFS behaviour and can lag
behind concurrent writes.

#### ResizeSubvolume

```golang
func (fs *Filesystem) ResizeSubvolume(ctx context.Context, subvolume *Subvolume, sizeBytes int64) error
```

Changes an owned subvolume's quota with the same rules as
`ResizeSubvolumeGroup`. A clone returned by `WaitForSubvolumeClone` can be
resized too.

#### SubvolumeGroupInfo

```golang
func (fs *Filesystem) SubvolumeGroupInfo(ctx context.Context, name string) (*SubvolumeGroupInfo, error)
```

Reads a group's current metadata: `Name`, `Path`, `DataPool`, `QuotaBytes` (0
for unlimited), `BytesUsed` (Ceph's recursive directory accounting, which lags
recent writes) and `CreatedAt`.

#### SubvolumeInfo

```golang
func (fs *Filesystem) SubvolumeInfo(ctx context.Context, name, groupName string) (*SubvolumeInfo, error)
```

Reads a subvolume's current metadata; an empty `groupName` selects the default
group. `SubvolumeInfo` has `Name`, `GroupName`, `Path`, `DataPool`,
`PoolNamespace`, `QuotaBytes`, `BytesUsed`, `CreatedAt`, `State` and `Type`
(`subvolume` or `clone`). A subvolume removed with `--retain-snapshots` has no
usable `Path`.

#### SubvolumeGroups

```golang
func (fs *Filesystem) SubvolumeGroups(ctx context.Context) ([]string, error)
```

Lists named groups sorted by name, including groups created outside the
fixture. Ceph's internal default group is omitted.

#### Subvolumes

```golang
func (fs *Filesystem) Subvolumes(ctx context.Context, groupName string) ([]string, error)
```

Lists subvolume names in a group, sorted by name; empty selects the default
group. Subvolumes that only retain snapshots can appear in the list.

#### RemoveSubvolume

```golang
func (fs *Filesystem) RemoveSubvolume(ctx context.Context, subvolume *Subvolume) error
```

Deletes an owned subvolume and its contents. It uses neither `--force` nor
`--retain-snapshots`, so Ceph refuses a subvolume that still has snapshots:
remove owned snapshots first with `RemoveSubvolumeSnapshot`. It is also refused
while a grant from `AuthorizeSubvolume` is active or uncertain. Ceph purges the
deleted tree asynchronously from its trash. The filesystem, group and pools
remain. A repeated call after success does nothing.

#### RemoveSubvolumeGroup

```golang
func (fs *Filesystem) RemoveSubvolumeGroup(ctx context.Context, group *SubvolumeGroup) error
```

Deletes an owned, empty group without `--force`. If Ceph refuses (for example
because the group is not empty), the handle stays usable for a retry. A repeated
call after success does nothing.

### Filesystem Methods: snapshots and clones

#### CreateSubvolumeSnapshot

```golang
func (fs *Filesystem) CreateSubvolumeSnapshot(ctx context.Context, subvolume *Subvolume, name string) (*SubvolumeSnapshot, error)
```

Creates a new snapshot of an owned subvolume. An existing snapshot with the same
name is rejected. The returned `SubvolumeSnapshot` has `Name`, `SubvolumeName`,
`GroupName`, `FilesystemName` and `Path`.

A snapshot does not pause application writers. For a consistent checkpoint,
synchronise the writers yourself or pause I/O with `QuiesceSubvolumes` first
(see [Take a consistent snapshot of several subvolumes](#take-a-consistent-snapshot-of-several-subvolumes)).

#### SubvolumeSnapshots

```golang
func (fs *Filesystem) SubvolumeSnapshots(ctx context.Context, subvolume *Subvolume) ([]string, error)
```

Lists the snapshots of an owned subvolume, including snapshots made by a client
(`mkdir .snap/<name>`). Only snapshots returned by `CreateSubvolumeSnapshot` can
be cloned or removed through the fixture.

#### SubvolumeSnapshotInfo

```golang
func (fs *Filesystem) SubvolumeSnapshotInfo(ctx context.Context, subvolumeName, groupName, snapshotName string) (*SubvolumeSnapshotInfo, error)
```

Reads snapshot metadata for any existing snapshot: `Name`, `SubvolumeName`,
`GroupName`, `Path` (the frozen data directory), `CreatedAt`, `DataPool`,
`HasPendingClones`, `PendingClones` (each a `SnapshotPendingClone` with the
clone's `Name` and `GroupName`, empty for the default group) and
`OrphanClonesCount`.

#### CloneSubvolumeSnapshot

```golang
func (fs *Filesystem) CloneSubvolumeSnapshot(ctx context.Context, snapshot *SubvolumeSnapshot, config CloneConfig) (*SubvolumeClone, error)
```

Starts an asynchronous copy of an owned snapshot into a new subvolume.
`CloneConfig` fields:

| Field | Meaning |
| --- | --- |
| `Name` | New subvolume name. Existing targets are rejected. |
| `GroupName` | Existing target group. Empty selects the default group. |
| `DataPool` | Optional data pool of the filesystem. Without it the clone inherits the snapshot's layout and RADOS namespace; with it Ceph clears the inherited namespace. |

Ceph copies the snapshot's quota and layout; the quota is applied when the copy
completes. Right after submission the fixture records the target's UUID, inode
and birth time and checks them on every later call, so a target deleted and
recreated outside the fixture is not adopted. Do not delete or recreate the
target name with the CLI while the clone is pending. If submission or that
identity read fails, the returned descriptor can be inspected but not waited on
or canceled.

#### WaitForSubvolumeClone

```golang
func (fs *Filesystem) WaitForSubvolumeClone(ctx context.Context, clone *SubvolumeClone) (*Subvolume, error)
```

Waits until the clone completes and returns it as an owned `Subvolume` that
supports `ResizeSubvolume` and `RemoveSubvolume`. The wait is bounded by the
cluster startup timeout or the caller's earlier deadline. A timeout does not
cancel the copy, so the wait can be repeated. `failed` and `canceled` clones
return an error at once and keep their partial data.

```golang
snapshot, err := fs.CreateSubvolumeSnapshot(ctx, volume, "checkpoint")
if err != nil {
	t.Fatal(err)
}
clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, cephfs.CloneConfig{Name: "restored"})
if err != nil {
	t.Fatal(err)
}
restored, err := fs.WaitForSubvolumeClone(ctx, clone)
if err != nil {
	t.Fatal(err)
}
// Mount restored.Path in the client under test.
```

#### SubvolumeCloneStatus

```golang
func (fs *Filesystem) SubvolumeCloneStatus(ctx context.Context, clone *SubvolumeClone) (*SubvolumeCloneStatus, error)
```

Reads the clone state without changing it: `State` (`pending`, `in-progress`,
`complete`, `failed` or `canceled`), the source fields (`SourceFilesystem`,
`SourceSubvolume`, `SourceGroup`, `SourceSnapshot`; usually empty after
completion), `FailureErrno` and `FailureMessage` (zero and empty unless Ceph
recorded a failure) and Ceph's `Progress` map.

#### CancelSubvolumeClone

```golang
func (fs *Filesystem) CancelSubvolumeClone(ctx context.Context, clone *SubvolumeClone) error
```

Cancels a `pending` or `in-progress` clone and waits until it is `canceled` and
its source snapshot is no longer protected by it. The partial target is kept. A
clone that already completed or failed is reported as an error, not as a
successful cancellation. The source snapshot must still exist. A retry after a
lost reply first checks whether the clone is already canceled.

#### RemovePartialSubvolumeClone

```golang
func (fs *Filesystem) RemovePartialSubvolumeClone(ctx context.Context, clone *SubvolumeClone) error
```

Deletes the target of an owned `failed` or `canceled` clone with `--force`. It
refuses pending, in-progress and completed clones, a target recreated under the
same name, and a source snapshot that is still protected. The source snapshot
must still exist and is never touched. Success means the target name is gone;
Ceph purges its data asynchronously. A retry after a lost reply does not delete
a target recreated under the same name.

#### RemoveSubvolumeSnapshot

```golang
func (fs *Filesystem) RemoveSubvolumeSnapshot(ctx context.Context, snapshot *SubvolumeSnapshot) error
```

Deletes an owned snapshot without `--force`. Snapshots with pending clones are
refused by the fixture and by Ceph. The source data and completed clones are
kept. Copies of the handle share the removal state, and a retry reconciles an
uncertain deletion.

#### AddSnapshotSchedule

```golang
func (fs *Filesystem) AddSnapshotSchedule(ctx context.Context, schedule cephfs.SnapshotSchedule) (*cephfs.SnapshotScheduleLease, error)
```

Adds a snapshot schedule, and its retention, to one directory through the MGR
`snap_schedule` module, which then takes `scheduled-…` snapshots in that
directory's `.snap`.

| `cephfs.SnapshotSchedule` field | Meaning |
| --- | --- |
| `Path` | A clean absolute path in the filesystem, such as `/` or `/data`. |
| `Repeat` | A positive number and one of `m` (minutes), `h`, `d`, `w`, `M` (months) or `y`, such as `6h`. |
| `Start` | Optional first time slot; the zero time leaves the module's default, midnight UTC of the day it is added. |
| `Retention` | Optional counts per period (`m`, `h`, `d`, `w`, `M`, `y`), or `n` for the newest snapshots of any period. |

The call sets nothing up by itself, and refuses with `ErrInvalidRequest`:
- while the `snap_schedule` module is disabled. Enable it with
  [override.SetMGRModule](../override/README.md#setmgrmodule), and wait until the active
  MGR, which restarts to load the module, answers again;
- a minute `Repeat` while `mgr/snap_schedule/allow_m_granularity` is not
  `true`. The module refuses minutes otherwise; set it with
  [override.SetConfig](../override/README.md#setconfig) for a test;
- a directory that already has a schedule, which the lease would not own,
  or another lease from this filesystem value.

The module keeps retention per directory, not per schedule: every schedule of
a directory shares it, and it stays after the directory's last schedule is
removed. A new schedule inherits what is left, so a directory with retention
rules is refused with `ErrInvalidRequest` after the schedule was added; the
returned lease removes the schedule and leaves those rules.

The lease's `Status(ctx)` returns a `cephfs.SnapshotScheduleStatus`, the module's state: `Schedule`, `Active`,
`Start`, `Added` (when the module added it), `Retention`, the `Created` and
`Pruned` counts, the `First` and `Last` snapshot times (zero before the
first) and the module's JSON (`Raw`). A directory can have several schedules
with one repeat; the lease identifies its own by the start and the time the
module added it, read back after the add.
`Snapshots(ctx)` lists the directory's `scheduled-…` snapshots through
libcephfs, oldest first.

`Restore(ctx)` removes the retention rules that the lease added, then its
schedule; the snapshots stay. It reads the directory back on every call, so a
retry continues where an earlier call stopped, and the lease is restored only
once its rules and its schedule are both confirmed gone:

- A rule that the directory no longer lists is skipped. When no schedule of
  the directory is left to show its retention, `Restore` removes each rule
  anyway; the module's "was not set" or "No schedule found" answer is what
  confirms that a rule is gone. So a rule that outlived the lease's schedule,
  for example after another client removed that schedule, is still removed.
- It refuses with `ErrInvalidRequest`, changing nothing and staying
  retryable, while another schedule of the directory, of any repeat, shares a
  rule that the lease still has to remove; when the lease's schedule was
  replaced by another one with its repeat; and when one of its rules has
  another count. Retry after the other schedule's owner removed it.

A non-nil lease returned with an error must be restored: after an add whose
reply was lost, `Restore` takes the only schedule with the requested repeat
and start as the lease's.

```golang
module, err := override.SetMGRModule(ctx, cluster, "snap_schedule", true)
// … wait for the active MGR, and set allow_m_granularity for minutes …
schedule, err := fs.AddSnapshotSchedule(ctx, cephfs.SnapshotSchedule{
	Path: "/", Repeat: "1m", Retention: map[string]int{"n": 2},
})
if schedule != nil {
	defer schedule.Restore(context.Background())
}
```

`TestCephFSSnapshotSchedule` runs a 1-minute schedule with `n: 2`. Within
about three minutes the module had taken three snapshots and pruned one, two
`scheduled-…` snapshots were left, and after `Restore` the directory had no
schedule and kept both snapshots. It then changes leases' schedules and
rules natively, and checks that `Restore` refuses a replaced schedule and a
rule shared with another repeat's schedule, skips a removed rule, and still
removes a rule that outlived its schedule.

### Filesystem Methods: authorization

These methods create Cephx principals that can reach exactly one subvolume,
through Ceph's `fs subvolume authorize`.

#### AuthorizeSubvolume

```golang
func (fs *Filesystem) AuthorizeSubvolume(ctx context.Context, volume *Subvolume, config SubvolumeAuthorizationConfig) (*SubvolumeAuthorization, error)
```

Creates a new principal `client.<ClientID>` with a fresh key and grants it
access to the subvolume. `SubvolumeAuthorizationConfig` has `ClientID` and
`Access` (`"r"` or `"rw"`). Existing principals are refused and never adopted.
The subvolume must be created with `NamespaceIsolated: true`, because a path cap
alone cannot stop direct RADOS access to the file data.

The grant consists of:

| Service | Capability |
| --- | --- |
| MON | `allow r fsname=<filesystem>` |
| MDS | `allow r` or `allow rw` on the subvolume's real UUID path |
| OSD | `allow r` or `allow rw` on the subvolume's pool and namespace |

`rw` does not include layout changes (`p`), snapshot management (`s`) or
root-squash administration. The OSD cap covers the whole namespace; a clone that
inherits the source layout shares it.

The returned `SubvolumeAuthorization` has `Client` (a `*ceph.ClientConfig` for
`cluster.WithClientIdentity`; `Client.User()` is the auth ID and
`Client.KeyringPath()` the keyring path in the attached container), `AuthID`,
`Access`, `FilesystemName`, `SubvolumeName`, `GroupName`, `Path`, `DataPool` and
`PoolNamespace`. A non-nil result with an error can describe a partially created
principal. Do not call `AuthorizeSubvolume` again with the same ID: inspect the
principal through `cluster.Ceph`, call `DeauthorizeSubvolume` if `Client` was
created, or terminate the cluster.

#### SubvolumeAuthorizedClients

```golang
func (fs *Filesystem) SubvolumeAuthorizedClients(ctx context.Context, name, group string) ([]SubvolumeAuthorizedClient, error)
```

Reads Ceph's `authorized_list` for a subvolume: each entry has `AuthID` and
`Access`. It includes principals authorized with the raw CLI. It is the volumes
module's metadata, not an audit of every Cephx cap.

#### DeauthorizeSubvolume

```golang
func (fs *Filesystem) DeauthorizeSubvolume(ctx context.Context, grant *SubvolumeAuthorization) error
```

Removes exactly this grant's MDS path and OSD pool/namespace caps and the
volumes metadata. It first checks that the subvolume, its layout and the
principal's key are unchanged. Other caps and the key are kept (Ceph may delete
a principal left with no caps at all). Overlapping caps added separately are not
revoked, files are not removed and established sessions are not closed.

Revocation applies to new connections. Test it with a fresh client, then call
`EvictSubvolumeClients` to close existing sessions. A lost reply is reconciled
on retry; edits made outside the fixture in the meantime are reported, not
overwritten.

#### EvictSubvolumeClients

```golang
func (fs *Filesystem) EvictSubvolumeClients(ctx context.Context, grant *SubvolumeAuthorization) error
```

Evicts sessions of this principal whose mount root is exactly the subvolume's
original path. It requires a completed `DeauthorizeSubvolume`. Sessions mounted
at other roots and future connections allowed by other caps are out of scope.
Eviction can blocklist the client's address. An error can mean partial eviction
across several MDS ranks; a retry repeats the same scope.

To remove the principal itself afterwards, call
`grant.Client.Delete(ctx)`. See
[Give each subvolume its own restricted principal](#give-each-subvolume-its-own-restricted-principal).

### Filesystem Methods: directory pins

Pins control which MDS rank serves a subtree. Ceph offers three policies,
selected with `PinType`: `cephfs.PinExport` (pin to one rank),
`cephfs.PinDistributed` (spread immediate children across ranks) and
`cephfs.PinRandom` (pin descendants to a random rank with a probability).
Policies can coexist; ancestry and Ceph's precedence rules decide the actual
placement.

#### SubvolumePinPolicy

```golang
func (fs *Filesystem) SubvolumePinPolicy(ctx context.Context, volume *Subvolume) (*PinPolicy, error)
```

Reads the local pin attributes on an owned subvolume's base directory through
libcephfs in the control container. `PinPolicy` has `Path` (for subvolumes, the
base directory one level above the UUID data directory), `Inode`, `ExportRank`
(-1 when unset), `Distributed` and `RandomProbability` (0 when unset, with the
precision of Ceph's text value). These are local values: they do not show
inherited policy or the rank that actually serves the subtree.

#### SubvolumeGroupPinPolicy

```golang
func (fs *Filesystem) SubvolumeGroupPinPolicy(ctx context.Context, group *SubvolumeGroup) (*PinPolicy, error)
```

Same as `SubvolumePinPolicy` for an owned group directory.

#### OverrideSubvolumePin

```golang
func (fs *Filesystem) OverrideSubvolumePin(ctx context.Context, volume *Subvolume, setting PinSetting) (*PinOverrideHandle, error)
```

Sets one pin field of an owned subvolume with `ceph fs subvolume pin` and
returns a handle that restores it. `PinSetting` has `Type` and `Value`:

| `Type` | Accepted `Value` |
| --- | --- |
| `cephfs.PinExport` | An active rank held by an owned MDS, or -1 to remove the local pin. |
| `cephfs.PinDistributed` | 0 or 1. |
| `cephfs.PinRandom` | A probability in [0, 1], further capped by the MDS option `mds_export_ephemeral_random_max`. |

Only one handle per directory and field may be active; different fields can be
changed independently. Setting a policy does not prove its effect: ephemeral
pinning is controlled by MDS options such as
`mds_export_ephemeral_distributed`, which you can change with
`override.SetConfig`. Keep a non-nil handle even when an error is
returned, because a canceled call or lost reply can leave the field changed.

```golang
// Needs two active ranks, for example Config{ActiveMDS: 2}.
pin, err := fs.OverrideSubvolumePin(ctx, volume, cephfs.PinSetting{Type: cephfs.PinExport, Value: 1})
if pin != nil {
	t.Cleanup(func() { _ = pin.Restore(context.Background()) })
}
if err != nil {
	t.Fatal(err)
}
```

#### OverrideSubvolumeGroupPin

```golang
func (fs *Filesystem) OverrideSubvolumeGroupPin(ctx context.Context, group *SubvolumeGroup, setting PinSetting) (*PinOverrideHandle, error)
```

Same as `OverrideSubvolumePin` for an owned group.

#### PinOverrideHandle.Restore

```golang
func (change *PinOverrideHandle) Restore(ctx context.Context) error
```

Checks that the directory still has the same inode and returns only this field
to its previous value; other fields, directories and data are untouched. It is
idempotent across copies of the handle. After an uncertain reply, retry with a
fresh context. A different current value or a recreated directory is refused.
Outside edits that set the same value, or differ below Ceph's text precision,
cannot be detected; do not edit pins by hand while a handle is in use.

### Filesystem Methods: quiesce

Ceph's quiesce sets (`ceph fs quiesce`, available since Squid) pause client I/O
on a set of subvolumes so that snapshots of several subvolumes form one
consistent checkpoint. The fixture does not pause processes; it only manages the
native set.

#### QuiesceSubvolumes

```golang
func (fs *Filesystem) QuiesceSubvolumes(ctx context.Context, volumes []*Subvolume, config QuiesceConfig) (*Quiesce, error)
```

Creates a new quiesce set over owned subvolumes and waits until it is
`QUIESCED`. `QuiesceConfig` has two durations, each at least one second:

| Field | Meaning |
| --- | --- |
| `Timeout` | How long Ceph may take to reach `QUIESCED`. |
| `Expiration` | Safety timer: Ceph releases the set by itself when it expires. Reading the status does not extend it. |

The set ID is unique and created exclusively (`--if-version=0`). Other quiesce
sets, including sets that overlap the same paths, are not touched. An uncertain
creation can return a non-nil handle with an error: keep it, or let its
expiration pass before terminating the cluster. Never edit this set with the
CLI.

#### Quiesce.Status

```golang
func (q *Quiesce) Status(ctx context.Context) (*QuiesceState, error)
```

Reads the set without renewing its expiration timer. `QuiesceState` has `ID`,
`Version` (changes on every native transition or edit), `State` (`QUIESCING`,
`QUIESCED`, `RELEASING`, `RELEASED`, `EXPIRED`, `TIMEDOUT`, `CANCELED` or
`FAILED`), `Timeout` and `Expiration` in seconds, and `Members` (Ceph's `file:`
paths).

#### Quiesce.Release

```golang
func (q *Quiesce) Release(ctx context.Context) error
```

Resumes I/O, but only if the set is still at the `QUIESCED` version captured
when it was reached. A set that expired, timed out, was canceled or was edited
from outside is reported as an error, so an interrupted pause cannot be mistaken
for a consistent checkpoint. A lost reply can be retried; once `RELEASED` is
confirmed, copies of the handle do nothing. Ceph keeps released set records
until the cluster is removed.

#### Quiesce.ID

```golang
func (q *Quiesce) ID() string
```

Returns the native set ID, for example to inspect it with
`ceph fs quiesce <fs> --set-id <id> --query`.

### Filesystem Methods: client sessions

#### Sessions

```golang
func (fs *Filesystem) Sessions(ctx context.Context) ([]Session, error)
```

Runs `session ls` on every active rank and returns the combined list. A client
with sessions on several ranks appears once per rank. Each `Session` has:

| Field | Meaning |
| --- | --- |
| `Rank` | The MDS rank that reported the session. |
| `ID` | The client's global ID (libcephfs `get_instance_id()`). |
| `Address` | `IP:port/nonce`, the same form as `ceph.BlocklistEntry.Address`. |
| `State` | `open`, `stale`, ... |
| `EntityID` | Cephx ID without `client.` (for example `admin`). |
| `Hostname`, `Root`, `PID` | Metadata sent by the client: host name, mount root, process ID. |
| `Caps` | Number of capabilities held. |

The list also contains short-lived sessions of the fixture itself (for example
pin policy reads). Find a client by `Hostname` or `ID`. With host networking
every container has the host's name, so use `ID`.

#### SessionTimeouts

```golang
func (fs *Filesystem) SessionTimeouts(ctx context.Context) (SessionTimeouts, error)
```

Reads the filesystem's `session_timeout` and `session_autoclose` from the
MDSMap. Ceph's defaults are 60 and 300 seconds.

#### OverrideSessionTimeouts

```golang
func (fs *Filesystem) OverrideSessionTimeouts(ctx context.Context, requested SessionTimeouts) (*SessionTimeoutsOverrideHandle, error)
```

Shortens the time the MDS waits for an unresponsive client, so eviction can be
tested in seconds. `SessionTimeouts` has two fields:

| Field | Ceph setting | Effect on a silent client |
| --- | --- | --- |
| `Timeout` | `session_timeout` | When another client wants its capabilities, the MDS marks the session stale and evicts it as soon as `Timeout` passes. |
| `Autoclose` | `session_autoclose` | When nobody wants its capabilities, the session stays `open` (Ceph's default `mds_defer_session_stale=true` does not even mark it stale) and is evicted after `Autoclose`. |

Both values must be whole seconds and at least 30 seconds (Ceph's minimum). They
are filesystem settings in the MDSMap, so `override.SetConfig` cannot change them.
Only one handle per filesystem may be active. Ceph changes one value per
command, so applying and restoring are not atomic; keep a non-nil handle
returned with an error and restore it.

With Ceph's default `mds_session_blocklist_on_timeout=true`, an eviction also
blocklists the client's address. Ceph removes the entry when it expires, one
hour later by default (`BlocklistEntry.Until` shows the time); `Restore` does not
remove it. A new mount, even from the same process, has a new nonce and is not
affected. See
[Evict a frozen client](#evict-a-frozen-client).

#### SessionTimeoutsOverrideHandle.Restore

```golang
func (change *SessionTimeoutsOverrideHandle) Restore(ctx context.Context) error
```

Returns both values to those captured before the change. A value that is
neither the applied nor the captured one is an outside edit and is refused, as
is a filesystem recreated under the same name. Once a restoration is confirmed,
further calls on any copy of the handle return nil, even after the cluster has
been terminated.

### Snapshot mirroring

Snapshot mirroring between two clusters is provided by the `cephfs/mirror` package; see [`cephfs/mirror`](mirror/README.md).

### Independent test fixtures

Start an independent fixture with `Run`, create service resources explicitly,
and register owned cleanup. See [Independent test fixtures](../ceph/README.md#independent-test-fixtures).

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
or `cluster.WithClientIdentity(client)` (one principal). The container needs the
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
  [Host network](../docs/guides/host-network.md).

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
if err := fs.ResizeSubvolume(ctx, volume, 8<<20); err != nil {
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
reach it. The client container attached with `WithClientIdentity` holds only
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
grant, err := fs.AuthorizeSubvolume(ctx, volumes["tenant-a"],
	cephfs.SubvolumeAuthorizationConfig{ClientID: "tenant-a", Access: "rw"})
if err != nil {
	t.Fatal(err) // a non-nil grant may be partial; do not authorize the same ID again
}
tenant, err := testcontainers.Run(ctx, ceph.DefaultImage,
	cluster.WithClientIdentity(grant.Client), ceph.WithIdleEntrypoint())
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
if err := fs.DeauthorizeSubvolume(ctx, grant); err != nil {
	t.Fatal(err)
}
if err := fs.EvictSubvolumeClients(ctx, grant); err != nil {
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
	if _, err := fs.CreateSubvolumeSnapshot(ctx, volume, "checkpoint-1"); err != nil {
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
working. See [MDS sessions](../docs/guides/messenger-client-fixtures.md#mds-sessions).

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
[Fault injection](../docs/guides/fault-injection.md) for the other messenger options.

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

## Running native commands

Anything without a typed API can be done with the Ceph CLI in the control
container. `cluster.Ceph` passes arguments without a shell and returns stdout;
add `--format json` for machine-readable output.

```golang
// Filesystem, MDS and FSMap state
out, err := cluster.Ceph(ctx, "fs", "status", fs.FilesystemName, "--format", "json")
out, err = cluster.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
out, err = cluster.Ceph(ctx, "fs", "dump", "--format", "json")

// Filesystem settings without a typed API
_, err = cluster.Ceph(ctx, "fs", "set", fs.FilesystemName, "max_file_size", "1073741824")

// The volumes module
out, err = cluster.Ceph(ctx, "fs", "volume", "ls")
out, err = cluster.Ceph(ctx, "fs", "subvolume", "ls", fs.FilesystemName, "--group_name", "tenants")
_, err = cluster.Ceph(ctx, "fs", "subvolume", "metadata", "set", fs.FilesystemName,
	volume.Name, "owner", "team-a", "--group_name", volume.GroupName)

// MDS commands through the MON; works with any MDS image
out, err = cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0", "session", "ls", "--format", "json")
out, err = cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0", "get", "subtrees")
out, err = cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0", "dump_ops_in_flight")

// Fail a daemon by GID (see MDSStatus)
_, err = cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(gid, 10))

// Central configuration for every MDS, restored by the handle
hold, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{
	Section: "mds", Name: "mds_session_blocklist_on_timeout", Value: "false",
})
```

**MDS admin socket.** Each MDS container has its admin socket at
`/var/run/ceph/ceph-mds.<ID>.asok`. When the MDS image includes the `ceph` CLI
(the default image does), query it directly:

```golang
mds := fs.MDSs()[0]
code, reader, err := mds.Exec(ctx, []string{"ceph", "--admin-daemon",
	"/var/run/ceph/ceph-mds." + mds.ID + ".asok", "cache", "status"}, tcexec.Multiplexed())
```

The MDS runs with `--mds-cache-memory-limit 134217728` (128 MiB) on its command
line, so this option cannot be changed through central configuration.

**CephFS mirror.** Source-side policy and assignment:

```golang
out, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", sourceFS.FilesystemName)
out, err = source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", sourceFS.FilesystemName, "/data")
out, err = source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", sourceFS.FilesystemName)
out, err = source.Ceph(ctx, "fs", "snapshot", "mirror", "daemon", "status")
out, err = source.Ceph(ctx, "fs", "snapshot", "mirror", "show", "distribution", sourceFS.FilesystemName)
```

Each daemon's admin socket is `/var/run/ceph/cephfs-mirror.asok` inside its
container. Commands take `<filesystem>@<filesystem ID>`:

```golang
status, _ := mirror.DirectoryStatus(ctx, "/data") // SourceFilesystemID and PeerID
target := fmt.Sprintf("%s@%d", mirror.SourceFilesystem, status.SourceFilesystemID)
daemon := mirror.Daemons()[0]
code, reader, err := daemon.Exec(ctx, []string{"ceph", "--admin-daemon",
	"/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", target}, tcexec.Multiplexed())
code, reader, err = daemon.Exec(ctx, []string{"ceph", "--admin-daemon",
	"/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", target, status.PeerID},
	tcexec.Multiplexed())
```

`fs mirror status` lists the peers the daemon replays to; after `RemovePeer` the
peer disappears from it once the replayer is torn down. `fs mirror peer status`
reports each directory's `state`, `last_synced_snap` and counters.

**Retained snapshots and custom metadata.** Ceph can remove a subvolume while
keeping its snapshots, and clone from such a snapshot. There is no typed API for
this; use the CLI and clean up with the CLI:

```golang
for _, args := range [][]string{
	{"fs", "subvolume", "snapshot", "metadata", "set", fs.FilesystemName, volume.Name,
		snapshot.Name, "checkpoint-label", "nightly", "--group_name", volume.GroupName},
	{"fs", "subvolume", "rm", fs.FilesystemName, volume.Name,
		"--group_name", volume.GroupName, "--retain-snapshots"},
	{"fs", "subvolume", "snapshot", "clone", fs.FilesystemName, volume.Name, snapshot.Name,
		"restored", "--group_name", volume.GroupName, "--target_group_name", volume.GroupName},
} {
	if _, err := cluster.Ceph(ctx, args...); err != nil {
		t.Fatal(err)
	}
}
// Poll: ceph fs clone status <fs> restored --group_name <group> --format json
```

On Ceph 20.2.4, a `snapshot-retained` subvolume has no live path, and subvolume
and snapshot custom metadata calls are refused in that state. Snapshot metadata
set before retention survives; the subvolume's own custom metadata is dropped.
A snapshot does not copy the subvolume's custom metadata, and clones do not
inherit it. Metadata keys are lower-cased by Ceph, and this metadata is separate
from POSIX extended attributes. Recreating the retained name with
`fs subvolume create` makes a new live subvolume with a new UUID; while Ceph is
still purging, that command can return `EAGAIN`, which is safe to retry. The typed `Subvolume`
and `SubvolumeSnapshot` handles do not adopt the retained state or a subvolume
recreated under the same name: typed clone, resize and remove calls on them are
refused.

Raw commands on resources that typed handles own are outside the handles'
contract. Fence other writers and do not run fixture operations on the same
resources at the same time. More escape hatches are listed in
[Native commands](../docs/guides/native-commands.md).

## Limitations and release differences

**Clients and mounts**

- No kernel or FUSE mount is used or tested. Clients are userspace libraries
  (libcephfs, go-ceph) in Linux containers, or in the test process with host
  networking on a Linux Docker host. A process on a macOS or Windows host cannot
  reach the OSDs of a bridge-mode cluster.
- The fixture's own libcephfs probes (pin reads, clone identity checks) appear
  briefly in `Sessions`.

**MDS**

- MDS names, keys, entrypoint and networking are fixed by the fixture; MDS
  customizers must not change them.
- Each MDS is limited to a 128 MiB cache.
- Recovery from damaged metadata, multi-rank replacement and `MDS.AddReplacement`
  for filesystems with standbys or standby replay are not provided.

**Subvolumes, snapshots and clones**

- Snapshots do not pause writers; use quiesce or application-level
  synchronisation.
- Quota enforcement and `BytesUsed` accounting are native and can lag
  concurrent writes. Subvolume and clone removal only moves data to Ceph's trash;
  purging is asynchronous.
- On Ceph 20.2.4 the CLI does not report the UUID of a pending clone. Do not
  delete or recreate a clone target by hand before its first completed status.
- Retained snapshots (`--retain-snapshots`) and custom metadata have no typed
  API.

**Release differences**

Tentacle 20.2.4 is the default and fully tested release. Squid 19.2.5 is
supported: the fixture reads the release of the `Run` image after the first MON
starts (`FactsOf(cluster).CephVersion`) and adapts its commands.

| Area | Tentacle 20.2.x | Squid 19.2.x |
| --- | --- | --- |
| Filesystem creation | `ceph fs new ... set max_mds N standby_count_wanted M` sets the rank counts at creation. | `fs new` takes no inline settings; the fixture runs `fs set` right after `fs new`, still before any MDS starts. No visible difference. |

No other CephFS difference is known. Subvolumes, snapshots, clones,
authorization, pins, quiesce sets, session limits, the messenger fault options
and snapshot mirroring behave the same on both releases. The Ceph 20.2.4 notes
above (MGR directory redistribution, pending clone UUIDs, retained snapshots)
describe Tentacle behaviour.

Other release-dependent behaviour lives in the cluster and the RBD and RGW
packages; see [Ceph](../ceph/README.md), [RBD](../rbd/README.md), [RGW](../rgw/README.md) and
[Releases and images](../docs/guides/releases-and-images.md). Keep MDS and mirror images
on the same release as the `Run` image: `ClusterFacts.CephVersion` reflects only the `Run`
image.

```golang
cluster, err := cephfs.Run(ctx, "quay.io/ceph/ceph:v19.2.5")
testcontainers.CleanupContainer(t, cluster)
if err != nil {
	t.Fatal(err)
}
facts, err := ceph.FactsOf(cluster)
if strings.HasPrefix(facts.CephVersion, "19.") {
	t.Log("running against Squid")
}
```

**Native timing that affects tests**

| Behaviour | Default | Effect |
| --- | --- | --- |
| `mds_beacon_grace` | 15 s | A dead MDS is replaced only after this grace unless failed with `ceph mds fail`. Without a standby a dead active can stay registered as laggy. |
| `session_timeout`, `session_autoclose` | 60 s, 300 s | Time until an unresponsive client is evicted; shorten with `OverrideSessionTimeouts` (minimum 30 s). |
| Blocklist entry of an evicted client | 1 h | The address stays blocklisted until `BlocklistEntry.Until`; new mounts use a new nonce and are not affected. |
| `client_mount_timeout` | 300 s | A mount against an unavailable or silent MDS fails with errno 110 after this time. Set it low in probes. |
| Cluster startup timeout | 3 min | Bounds `Start`, `WaitReady` and `WaitForSubvolumeClone`; change with `ceph.WithStartupTimeout`. |

Context handling, cleanup ordering and partial results are covered in
[Contexts and cleanup](../docs/guides/contexts-and-cleanup.md).
