# RBD

## Introduction

The Testcontainers module for RBD.

RBD (RADOS Block Device) images live in ordinary Ceph pools, so an RBD test
needs no extra daemon: it needs a running Ceph cluster, a replicated pool that
is initialized for RBD metadata, and a client that can reach the cluster. This
package starts such a cluster and prepares the server side of RBD tests:

- a disposable cluster with one or more initialized RBD pools (`Run`, `WithPools`, `InitPool`);
- image namespaces for tenant isolation (`CreateNamespace`, `ListNamespaces`, `RemoveNamespace`);
- observation of the clients that watch an image and hold its locks (`ImageClients`), for exclusive-lock and fencing tests;
- full and incremental backup transport between independent clusters (`ExportBackup`, `ExportIncremental`, `RestoreBackup`, `RestoreIncremental`);
- the RBD immutable object cache daemon in a client container, with its cached objects and RADOS reads (`StartImmutableObjectCache`);
- native `rbd-mirror` replication between two clusters, in snapshot or journal mode, with image or pool scope, default or named namespaces, any number of receiver daemons, and typed readiness observation (the separate `rbd/mirror` package, documented in [rbd-mirror.md](rbd-mirror.md)).

Image CRUD (create, clone, flatten, trash, migration, group snapshots,
encryption, locks) belongs to the RBD client under test. The module does not
wrap it. Use librbd or go-ceph in your application container, or the `rbd` CLI
as shown in [Running native commands](#running-native-commands). The Go module
itself has no cgo or go-ceph dependency; every Ceph client runs inside a Linux
container.

The returned cluster is a `ceph` cluster. Everything in [ceph.md](ceph.md)
(clients and Cephx, pools, configuration, health, fault injection, topology
changes) applies to it.

## Adding this module to your project dependencies

Please run the following command to add the RBD module to your Go dependencies:

```
go get github.com/JSYoo5B/ceph-testcontainers-go/rbd
```

## Usage example

```golang
package example_test

import (
	"context"
	"io"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRBD(t *testing.T) {
	ctx := context.Background()

	// MON, MGR and two OSDs, plus the initialized replicated pool "rbd".
	cluster, err := rbd.Run(ctx, ceph.DefaultImage)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// A client container on the cluster network with /etc/ceph/ceph.conf and
	// the admin keyring. Use your application image instead of DefaultImage.
	client, err := testcontainers.Run(ctx, ceph.DefaultImage,
		cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
	)
	testcontainers.CleanupContainer(t, client)
	if err != nil {
		t.Fatal(err)
	}

	code, out, err := client.Exec(ctx,
		[]string{"rbd", "create", "--size", "16M", "rbd/disk"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		msg, _ := io.ReadAll(out)
		t.Fatalf("rbd create exited %d: %s", code, msg)
	}
}
```

The client container is registered for cleanup after the cluster, so it is
removed first. Ceph clients connect directly to the OSD addresses that the
monitors advertise, so in the default bridge network mode the client must run
in a container attached with `WithClient` or `WithClientIdentity`. A process on
a macOS or Windows host cannot reach the whole cluster through a mapped MON
port. For native clients outside Docker, see
[the host network guide](guides/host-network.md).

## Module Reference

### Run function

The RBD module exposes one entrypoint function to create the RBD container, and this function receives three parameters:

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error)
```

- `context.Context`, the Go context.
- `string`, the Docker image to use.
- `testcontainers.ContainerCustomizer`, a variadic argument for passing options.

`Run` starts a disposable Ceph cluster (one MON, one MGR and two file-backed
OSDs by default, each in its own container) and then creates and initializes
the RBD pools. Without a `WithPools` option it creates one replicated pool named
`rbd` with application `rbd`. The returned `Container` embeds `*ceph.Container`,
so every `ceph.Container` method is available on it.

`Run` accepts every `ceph.Run` option and the options of the other service
packages, so one cluster can serve RBD, CephFS and RGW tests at once. It rejects
bootstrap options that leave the cluster without OSDs for its pool, such as
`ceph.WithNoInitialOSDs`; start those staged clusters with `ceph.Run` and call
`InitPool` later.

`Run` returns after the pools exist and their RBD metadata is initialized. PG
peering may still be finishing, so call `cluster.WaitForClean(ctx)` before the
first client I/O.

A non-nil `Container` returned together with an error owns containers and must
still be terminated. `testcontainers.CleanupContainer(t, cluster)` handles both
a nil and a non-nil result.

The package-level functions (`InitPool`, `CreateNamespace`, `ImageClients`, ...)
take a `*ceph.Container`. They work with a cluster from
`ceph.Run` and with any other service package's `Run`. For a cluster started
with `rbd.Run`, pass its embedded `*ceph.Container`. The examples on this page
start clusters with `ceph.Run(ctx, img, rbd.WithPools(...))`, which is
equivalent to `rbd.Run` with explicit pools.

#### Image

Use the second argument in the `Run` function to set a valid Docker image.

The default is `ceph.DefaultImage`, the pruned Tentacle aggregate image
`quay.io/ceph/ceph:v20.2.4` pinned by its multi-architecture digest
(linux/amd64 and linux/arm64).

An aggregate supplies all roles. For explicit roles use `ceph.WithImages`:
essential supplies the daemons and client supplies ordinary RBD commands.
RBD mirror/cache additionally select rbd-aux. Keep Ceph releases and native
architectures consistent. See the [image guide](guides/releases-and-images.md).

### Container Options

When starting the RBD container, you can pass options in a variadic way to configure it.

#### WithPools

```golang
func WithPools(configs ...ceph.PoolConfig) ceph.Option
```

Creates replicated pools for RBD images during `Run` and initializes their RBD
metadata (`rbd pool init`). It works with `rbd.Run`, `ceph.Run` and the other
service packages' `Run`.

```golang
cluster, err := rbd.Run(ctx, ceph.DefaultImage,
	rbd.WithPools(
		ceph.PoolConfig{Name: "volumes"},
		ceph.PoolConfig{Name: "backups", Replicas: 1, MinSize: 1},
	),
)
```

- `Application` defaults to `rbd`. Any other application is rejected.
- `ErasureCode` is rejected: RBD metadata must live in a replicated pool.
- The other `ceph.PoolConfig` fields keep their `ceph` meaning: zero `PGNum`
  selects 8 PGs with the autoscaler off, and zero `Replicas`/`MinSize` select
  the cluster defaults (2/1, or 1/1 with one OSD). Placement fields
  (`FailureDomain`, `CRUSHRoot`, `DeviceClass`) are validated against the OSDs
  like any other pool.
- Repeating the option replaces the list. Passing it disables the default `rbd`
  pool of `rbd.Run`.

An erasure-coded pool can hold image data, not metadata. Create it with
`ceph.WithPools` and name it as the image's data pool:

```golang
cluster, err := rbd.Run(ctx, ceph.DefaultImage,
	ceph.WithOSDCount(3),
	rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	ceph.WithPools(ceph.PoolConfig{
		Name:        "volumes-data",
		Application: "rbd",
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	}),
)
// rbd create --size 64M --data-pool volumes-data volumes/disk
// (go-ceph: set the data pool in the image options before CreateImage)
```

`AllowOverwrites` is required for RBD data pools.

Every `ceph.Run` option is accepted as well (`ceph.WithOSDCount`,
`ceph.WithHostNetwork`, `ceph.WithImages`, `ceph.WithConfigFile`,
`ceph.WithMessengerMode`, ...); see [ceph.md](ceph.md). The generic options of
the `testcontainers` package (for example `testcontainers.WithEnv`,
`testcontainers.WithLogger` or any `testcontainers.CustomizeRequestOption`)
also apply, but they customise only the MON/control container. Module options
configure the cluster. Replacing the MON's image, network, entrypoint or
internal paths through a generic option breaks bootstrap.

### Container Methods

The `Container` returned by `Run` embeds `*ceph.Container`, so every method in
[ceph.md](ceph.md) is available. RBD-specific operations are package-level
functions (below) because they also apply to clusters started by other
packages. The `ceph.Container` methods most RBD tests use are:

| Method | Use in RBD tests |
| --- | --- |
| `WithClient()` | Attach an application container with the admin keyring. |
| `CreateClient(ctx, name, ceph.ClientCaps)`, `WithClientIdentity(client)`, `ClientConfig.Delete` | Least-privilege identities such as `profile rbd` or `profile rbd-read-only` scoped to a pool and namespace. |
| `ConnectionConfig(ctx)` | `ceph.conf` and admin keyring for a native client outside Docker (host network mode). |
| `CreatePool`, `Pool.Status`, `Pool.Remove`, `WaitForClean` | Pools created after `Run`; wait for `active+clean` before I/O. |
| `Ceph(ctx, args...)`, `ControlContainer(ctx)`, `ControlImage()` | Run the `ceph` CLI, run `rbd`/`rados` in the control container, reuse the control image. |
| `fault.Pause`, `TemporaryBlocklist`, `BlocklistEntries` | Freeze and fence RBD clients (see [the lock handover example](#hand-over-an-exclusive-lock-from-an-unresponsive-client)). |
| `OverrideConfig` | Set any Ceph option and restore it. |
| `CephVersion()` | Branch on the Ceph release (`"20.2.4"`, `"19.2.5"`). |

### Pool and namespace functions

#### InitPool

```golang
func InitPool(ctx context.Context, c *ceph.Container, pool string) error
```

Initializes an existing replicated pool for RBD metadata, for clusters started
without `WithPools` or pools created later with `CreatePool`.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithPools(ceph.PoolConfig{Name: "images", Application: "rbd"}),
)
// ... cleanup and error handling ...
if err := rbd.InitPool(ctx, cluster, "images"); err != nil {
	t.Fatal(err)
}
if err := cluster.WaitForClean(ctx); err != nil {
	t.Fatal(err)
}
```

- The pool must exist and be replicated; erasure-coded pools are rejected.
- A pool registered to another application (`cephfs`, `rgw`, ...) is rejected. No force option is used.
- Initializing an already initialized pool is safe.
- Success is verified natively (application tag and the `rbd_trash` object), not only by the CLI exit code.
- A partial failure never deletes pool data.
- Pool names may contain letters, digits, `_`, `.` and `-`, must not start with `.` or `-`, and are at most 128 characters.

#### CreateNamespace

```golang
func CreateNamespace(ctx context.Context, c *ceph.Container, pool, name string) (*Namespace, error)
```

Creates a fresh image namespace in an initialized replicated pool and returns a
descriptor owned by the cluster.

```golang
tenant, err := rbd.CreateNamespace(ctx, cluster, "volumes", "tenant-a")
if err != nil {
	t.Fatal(err)
}
// Clients use pool tenant.PoolName() and namespace tenant.Name().
```

- An existing name is rejected and its data is left untouched.
- A non-nil descriptor returned with an error identifies an attempted creation. It cannot authorize `RemoveNamespace` unless the creation was confirmed. After such an uncertain failure, inspect native state with `rbd namespace list` or dispose of the cluster.
- Name rules are the same as for pools.

#### ListNamespaces

```golang
func ListNamespaces(ctx context.Context, c *ceph.Container, pool string) ([]string, error)
```

Returns the pool's namespace names in sorted order, including namespaces
created outside the fixture. Ceph omits the unnamed default namespace. Listing
does not grant ownership for removal.

#### RemoveNamespace

```golang
func RemoveNamespace(ctx context.Context, c *ceph.Container, ns *Namespace) error
```

Removes a namespace that `CreateNamespace` confirmed.

- Ceph refuses a non-empty namespace, including one whose images are only in the trash. Remove images, snapshots and trash entries through your client first. No purge or force option is used.
- Successful removal is idempotent. A failed removal can be retried with the same descriptor and a new context.
- If the underlying pool was deleted and re-created, removal is rejected.
- You do not have to remove namespaces for cleanup: terminating the cluster disposes of them with all data.

#### Namespace

`Namespace` is the descriptor returned by `CreateNamespace`. `Name()` returns
the namespace name and `PoolName()` the replicated pool that contains it. Both
are immutable. Native RBD namespaces have no generation identity, so do not
delete and re-create a namespace with the same name through another client
while its descriptor is in use: the fixture cannot tell the replacement apart.

### Image client functions

#### ImageClients

```golang
func ImageClients(ctx context.Context, c *ceph.Container, pool, namespace, image string) (*ImageClientStatus, error)
```

Reports which clients watch an image header and which hold its locks, by
running `rbd status` and `rbd lock ls` in the control container. `namespace` is
empty for the default namespace. The result includes clients outside the
fixture. It only reads native state and grants no ownership.

```golang
status, err := rbd.ImageClients(ctx, cluster, "volumes", "", "disk")
if err != nil {
	t.Fatal(err)
}
if owner := status.ExclusiveOwner(); owner != nil {
	t.Logf("%s holds the exclusive lock from %s", owner.Locker, owner.Address)
}
for _, w := range status.Watchers {
	t.Logf("watcher %s (client.%d)", w.Address, w.ClientID)
}
```

The watchers and the locks come from two separate native reads. During a lock
handover the result can show the new owner's lock without its watch, or the
reverse. Poll until both agree when you need a stable answer.

#### ImageClientStatus

`ImageClientStatus` has two fields, `Watchers []ImageWatcher` and
`Locks []ImageLock`.

`ExclusiveOwner()` returns the managed exclusive lock (the lock librbd takes
for the `exclusive-lock` image feature), or nil when no client holds it.

#### ImageWatcher

| Field | Meaning |
| --- | --- |
| `Address` | The client's `IP:port/nonce`, the same form that `BlocklistEntries` reports and `TemporaryBlocklist` accepts. |
| `ClientID` | The numeric global ID (`client.<ClientID>`). |
| `Cookie` | The watch cookie. |

A client that stops answering keeps its watch until the OSD option
`osd_client_watch_timeout` expires (30 seconds by default).

#### ImageLock

| Field | Meaning |
| --- | --- |
| `ID` | The lock ID. For a managed lock it is `auto <watch cookie>`. |
| `Locker` | The owner's entity name, for example `client.4235`. |
| `Address` | The owner's `IP:port/nonce`, usable with `TemporaryBlocklist`. |
| `Managed` | True for the librbd exclusive lock; false for advisory locks created with `rbd lock add`. |

### Backup and restore functions

These helpers move CLI-generated RBD archives between clusters through Go
`io.Reader`/`io.Writer` values. No mirror daemon is required and the clusters
stay independent.

`client` is any container that has the `rbd` CLI and credentials for its own
cluster: `cluster.ControlContainer(ctx)` or a container attached with `WithClient`.
`image` is an RBD image spec such as `volumes/disk` or `volumes/tenant-a/disk`.

Common rules:

- The archive is written to a temporary file under `/tmp` in the client container and removed afterwards. Restore first spools the reader into a private temporary file on the test host, so large archives are not held in memory, then copies it into the container.
- The helpers never close your reader or writer. You choose where the backup is kept.
- Cancellation is checked between reads. A reader or writer that can block indefinitely must support its own cancellation.
- Pool creation, image deletion and archive retention belong to the caller.

#### ExportBackup

```golang
func ExportBackup(ctx context.Context, client testcontainers.Container, image string, destination io.Writer) error
```

Runs `rbd export --export-format 2` and writes the archive to `destination`.
Format 2 includes the image's snapshots and image metadata (`rbd image-meta`).

#### ExportIncremental

```golang
func ExportIncremental(ctx context.Context, client testcontainers.Container, image, fromSnapshot string, destination io.Writer) error
```

Runs `rbd export-diff --from-snap <fromSnapshot>`. `image` can end with
`@snapshot` to export the changes up to that snapshot; restoring it then
creates that snapshot on the target. `fromSnapshot` is a bare snapshot name.

#### RestoreBackup

```golang
func RestoreBackup(ctx context.Context, client testcontainers.Container, image string, archive io.Reader) error
```

Runs `rbd import --export-format 2` into a new image on the client's own
cluster. The pool must already exist and be initialized; the image must not
exist.

#### RestoreIncremental

```golang
func RestoreIncremental(ctx context.Context, client testcontainers.Container, image string, archive io.Reader) error
```

Runs `rbd import-diff` against an existing image. Ceph refuses the diff when
the image lacks the baseline snapshot. The check is by snapshot name only:
there is no manifest that proves the baseline has the same content.

See [Back up an image and restore it on another cluster](#back-up-an-image-and-restore-it-on-another-cluster).

### Immutable object cache

The RBD immutable object cache, `ceph-immutable-object-cache`, keeps whole
parent-image objects of clones on the client's local disk. librbd clients that
enable the parent cache ask the daemon for each parent object first, so after
the first read a clone's unmodified data comes from the cache instead of the
OSDs. Use it to study clone read paths: what a read promotes, how many RADOS
reads it saves, and what clients do when the daemon goes away.

The daemon runs in a client container that you own, next to the librbd
consumer: both use the same socket and cache directory. The container needs
`ceph-immutable-object-cache` on its `PATH` and the librbd `parent_cache`
plugin in its plugin directory. The official Ceph image has both, and so do
the current `rbd-aux` role images of
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images).
Select `cluster.Images().RBDAux` for the caller-owned cache client container. A container without them is refused with
`ceph.ErrImageCapability`; nothing is installed at runtime.

#### StartImmutableObjectCache

```golang
func StartImmutableObjectCache(ctx context.Context, client testcontainers.Container, config ImmutableObjectCache) (*ImmutableObjectCacheLease, error)
```

Starts the daemon in `client` and waits until its socket is open. `config`
sets:

- `ID`: the cephx ID the daemon reads parent objects with; empty means
  `admin`. A client attached with `WithClientIdentity` passes its own name.
  It needs read access to the parents' pool, such as
  `osd 'profile rbd pool=<pool>'`.
- `Path`: the cache directory, `/var/cache/ceph/immutable-object-cache` by
  default. It must be absent or empty.
- `Socket`: the daemon's client socket, `/var/run/ceph/immutable-object-cache.sock`
  by default. It must not exist and must not be inside `Path`.
- `MaxSize`: `immutable_object_cache_max_size` in bytes, 1 GiB by default. The
  daemon treats it as a target rather than a hard limit: with 16 MiB, nine
  4 MiB objects stayed cached in a native run.

The defaults are the constants `DefaultImmutableObjectCachePath`,
`DefaultImmutableObjectCacheSocket` and `DefaultImmutableObjectCacheMaxSize`.
An existing socket, a non-empty cache directory or an invalid path is refused
with `ceph.ErrInvalidRequest` before any change. A daemon that exits or never
opens its socket fails the call with `ceph.ErrNativeFailure`, and what it left
is removed.

#### ImmutableObjectCacheLease

```golang
func (l *ImmutableObjectCacheLease) ClientOptions() map[string]string
func (l *ImmutableObjectCacheLease) Status(ctx context.Context) (ImmutableObjectCacheStatus, error)
func (l *ImmutableObjectCacheLease) Stop(ctx context.Context) error
func (l *ImmutableObjectCacheLease) Restore(ctx context.Context) error
```

- `ClientOptions` returns `rbd_parent_cache_enabled=true`,
  `rbd_plugins=parent_cache` and `immutable_object_cache_sock=<Socket>`. Pass
  them to each client that should use the cache, for example as
  `rbd --rbd_parent_cache_enabled=true ...` or
  `rados.Rados(conf={...})`. Clients without them read from RADOS as usual.
- `Status` reports `Running` and `PID`, `CachedObjects` and `CachedBytes`
  (one file per cached parent object), and `RADOSReads`, the daemon's own
  objecter read count while it runs. The daemon has no hit or miss counters;
  a repeated read through the cache leaves `RADOSReads` unchanged.
- `Stop` stops the daemon with SIGTERM, then SIGKILL, and keeps the cache
  directory and the socket file. Clients that open an image afterwards log
  `Parent cache fail to register client` and read the parents from RADOS
  with the same data.
- `Restore` stops the daemon if it runs and removes its socket, admin socket,
  PID file and cached objects; it removes the cache directory only if it
  created it. A repeated call does nothing.

The lease signals a process only while its command line holds the lease's
socket, so a reused PID is never stopped. In a native run with 16 objects of
a 64 MiB clone, the first `rbd export` through the cache made 16 RADOS reads
and cached 64 MiB; a second export and a Python `rbd.Image.read` added none.
`rados df` read counters lag behind and do not show this; use `RADOSReads`.

```golang
lease, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: "rbd-client"})
if lease != nil {
    defer lease.Restore(context.WithoutCancel(ctx))
}
if err != nil {
    return err
}
args := []string{"rbd", "--id", "rbd-client", "export", "pool/clone", "/dev/null"}
for name, value := range lease.ClientOptions() {
    args = append(args, "--"+name+"="+value)
}
// Run args in client, then read lease.Status(ctx).RADOSReads.
```

### Mirroring

Mirroring two clusters moved to the `rbd/mirror` package; see [rbd-mirror.md](rbd-mirror.md).

### Shared cluster tenants

A tenant of a [shared cluster](ceph.md#shared-cluster) leases an RBD
namespace of its own. Create the RBD pool with `WithPools` in
`ceph.WithSharedRunOptions`.

```golang
func Lease(ctx context.Context, t *ceph.Tenant, pool string) (*TenantNamespace, error)

func (n *TenantNamespace) Pool() string
func (n *TenantNamespace) Namespace() string
func (n *TenantNamespace) Client() *ceph.ClientConfig
func (n *TenantNamespace) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
```

`Lease` creates the namespace `ID` (the tenant's ID) in `pool` and the client
`client.ID-rbd-POOL` with `mon 'allow r'` and `osd` and `mgr 'profile rbd
pool=POOL namespace=ID'`. It fails with `ErrInvalidRequest` for a pool without
the `rbd` application, and when the tenant already leased the pool.
`WithClient` attaches a container with that client, which the tenant's
`RunNativeCommand` accepts:

```golang
tenant := shared.Lease(t)
images, err := rbd.Lease(t.Context(), tenant, "rbd")
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(t.Context(), ceph.DefaultImage, images.WithClient(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, client)
// rbd --id <images.Client().User()> create --size 8M rbd/<images.Namespace()>/image
```

The client can create, clone, snapshot, trash and remove images in its
namespace. Checked natively, it cannot list or open another tenant's images,
create images in the default namespace, create namespaces or add blocklist
entries. It has no blocklist capability, so it cannot break another client's
exclusive lock either.

The tenant's release removes everything in the namespace: groups, trash,
snapshots (unprotected first), clones before their parents, and images. An
image that a client which ended without closing it still watches is retried
for up to 60 seconds, until the OSDs drop the watch, and its exclusive lock is
broken without a blocklist entry. Then the release removes the namespace and
any raw object left in it, and fails the test and poisons the cluster if
anything remains.

## Examples

### Helpers used by the examples

The examples share these helpers. `run` executes a command in any container and
returns its exit code and combined output. `poll` repeats a check every two
seconds until it holds or the context ends. The mirroring examples have their
own helpers in [rbd-mirror.md](rbd-mirror.md#helpers-used-by-the-examples).

```golang
import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
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
```

The examples write test data with `rbd bench --io-type write`, which goes
through librbd like any application writer. Use your own client in a real test.

### Test a librbd client in a namespace with read-write and read-only identities

Give the application its own namespace and two least-privilege identities: one
that can create and write images in the namespace, and one that can only read.

```golang
func TestTenantNamespace(t *testing.T) {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := rbd.CreateNamespace(ctx, cluster, "volumes", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	scope := " pool=" + tenant.PoolName() + " namespace=" + tenant.Name()
	writer, err := cluster.CreateClient(ctx, "tenant-a-rw", ceph.ClientCaps{
		Mon: "profile rbd",
		MGR: "profile rbd" + scope,
		OSD: "profile rbd" + scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := cluster.CreateClient(ctx, "tenant-a-ro", ceph.ClientCaps{
		Mon: "profile rbd",
		MGR: "profile rbd-read-only" + scope,
		OSD: "profile rbd-read-only" + scope,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Each container gets the cluster network, /etc/ceph/ceph.conf and only
	// its identity's keyring at KeyringPath(). Use your application image.
	start := func(id *ceph.ClientConfig) testcontainers.Container {
		ctr, err := testcontainers.Run(ctx, ceph.DefaultImage,
			cluster.WithClientIdentity(id),
			ceph.WithIdleEntrypoint(),
		)
		testcontainers.CleanupContainer(t, ctr)
		if err != nil {
			t.Fatal(err)
		}
		return ctr
	}
	rw, ro := start(writer), start(reader)

	// An image in the default namespace, created with admin credentials.
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, out, err := run(ctx, control,
		"rbd", "create", "--size", "16M", "volumes/shared"); err != nil || code != 0 {
		t.Fatalf("create volumes/shared: %d %s %v", code, out, err)
	}

	rbdAs := func(ctr testcontainers.Container, id *ceph.ClientConfig, args ...string) int {
		cmd := append([]string{"rbd", "--id", id.User(), "--keyring", id.KeyringPath()}, args...)
		code, _, err := run(ctx, ctr, cmd...)
		if err != nil {
			t.Fatal(err)
		}
		return code
	}
	if rbdAs(rw, writer, "create", "--size", "16M", "volumes/tenant-a/disk") != 0 {
		t.Fatal("writer could not create an image in its namespace")
	}
	if rbdAs(ro, reader, "info", "volumes/tenant-a/disk") != 0 {
		t.Fatal("reader could not read image metadata")
	}
	if rbdAs(ro, reader, "create", "--size", "16M", "volumes/tenant-a/other") == 0 {
		t.Fatal("read-only identity created an image")
	}
	if rbdAs(rw, writer, "info", "volumes/shared") == 0 {
		t.Fatal("namespace-scoped identity read an image in the default namespace")
	}
}
```

In the application, select the namespace on the I/O context and open images
read-only with the read-only identity. A normal (writable) open registers a
header watch, which needs write permission, so it fails with that identity.
With go-ceph inside the reader container:

```golang
import (
	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
)

conn, _ := rados.NewConnWithUser("tenant-a-ro")
_ = conn.ReadDefaultConfigFile() // /etc/ceph/ceph.conf written by WithClientIdentity
_ = conn.SetConfigOption("keyring", "/etc/ceph/ceph.client.tenant-a-ro.keyring") // reader.KeyringPath()
_ = conn.Connect()
ioctx, _ := conn.OpenIOContext("volumes")
ioctx.SetNamespace("tenant-a")
img, err := librbd.OpenImageReadOnly(ioctx, "disk", librbd.NoSnapshot)
```

A failed write through a read-only open handle (`EROFS`) is a client-side
refusal. To prove the server-side permission, test operations the server must
deny: image creation, a direct RADOS write, or access to another namespace,
which fail with `EPERM`/`EACCES`.

Cephx changes apply to new connections; an existing session can keep tickets
with the old capabilities. Change capabilities with `ClientConfig.UpdateCaps` and
revoke identities with `ClientConfig.Delete` (see [ceph.md](ceph.md)).

To remove the namespace before the cluster ends, delete its images (and empty
its trash) through the client, then call
`rbd.RemoveNamespace(ctx, cluster, tenant)`.

### Prepare the server for librbd client features

The client feature under test decides what the fixture must provide:

| Client feature | Server-side preparation |
| --- | --- |
| Clone, flatten | Format 2 images with `layering`. Clone v1 needs a protected parent snapshot. |
| Trash move, restore, purge | Initialized pool. Use a dedicated namespace so purges do not touch other tests' trash. The Python binding converts trash deferment times with local time, so run its process with `TZ=UTC`. |
| Live migration (prepare, execute, commit, abort) | Replicated initialized pool, `layering` on source and target, source writers closed. |
| Group snapshots | Format 2 images; flush and close writers before the snapshot. It is a checkpoint of those images, not an application transaction. |
| Exclusive lock | `exclusive-lock` feature and two independent RADOS sessions. See the next example. |
| Encryption format/load (LUKS1, LUKS2) | librbd built with libcryptsetup (included in `control` and `all` images). No `journaling` on the image. |
| Passphrase change | librbd has no rekey API. Run the external `cryptsetup luksChangeKey` on the exported LUKS header (the `control`/`all` image ships `cryptsetup`) and write the changed header back. |
| Erasure-coded data | Create the EC pool with `AllowOverwrites` and pass it as the image's data pool (see [WithPools](#withpools)). |

The images, the Python bindings and the native libraries live in Linux
containers; build and run go-ceph applications on Linux.

### Hand over an exclusive lock from an unresponsive client

A client that freezes while it holds an image's exclusive lock keeps its
header watch until `osd_client_watch_timeout` (30 seconds by default) expires.
A second writer blocks until then, breaks the lock and blocklists the old
owner. Fencing the frozen owner with a blocklist entry hands the lock over
without waiting.

```golang
func TestLockHandover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage,
		rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
	)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, out, err := run(ctx, control,
		"rbd", "create", "--size", "16M", "volumes/disk"); err != nil || code != 0 {
		t.Fatalf("create image: %d %s %v", code, out, err)
	}

	// waitOwner polls until a client other than `previous` holds the lock
	// and also watches the image.
	waitOwner := func(previous string) *rbd.ImageLock {
		var owner *rbd.ImageLock
		poll(ctx, t, "exclusive lock owner", func() bool {
			status, err := rbd.ImageClients(ctx, cluster, "volumes", "", "disk")
			if err != nil {
				t.Fatal(err)
			}
			owner = status.ExclusiveOwner()
			return owner != nil && owner.Address != previous &&
				slices.ContainsFunc(status.Watchers, func(w rbd.ImageWatcher) bool {
					return w.Address == owner.Address
				})
		})
		return owner
	}

	// startWriter is your helper: it runs an application container attached
	// with cluster.WithClient() whose librbd process opens volumes/disk and writes.
	first := startWriter(ctx, t, cluster)
	owner := waitOwner("")

	pause, err := fault.Pause(ctx, cluster, first)
	if pause != nil {
		// Registered after the container's cleanup, so it runs first.
		t.Cleanup(func() { _ = pause.Resume(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}

	// Fence the frozen owner instead of waiting for its watch to expire.
	fence, err := cluster.TemporaryBlocklist(ctx, owner.Address, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	startWriter(ctx, t, cluster) // the second writer
	waitOwner(owner.Address)     // takes over within seconds

	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	// Check in your application that the old owner's next write fails with
	// ESHUTDOWN (errno 108). Only then lift the fence.
	if err := fence.Restore(ctx); err != nil {
		t.Fatal(err)
	}
}
```

Behaviour to rely on:

- Without the fence, the second writer's first write blocks for roughly the watch timeout, then succeeds. Afterwards the new client owns the lock, the old owner no longer watches the image, and librbd has blocklisted the old owner's address.
- With the fence, the frozen owner's watch disappears at once and the handover takes about a second.
- After `Resume`, the old owner's writes fail with errno 108 (`ESHUTDOWN`).
- Keep the fence until the old owner has seen the blocklist. The exclusive lock is cooperative: an old owner that is unfrozen before it sees the blocklist asks the current owner for the lock, receives it, and its writes succeed.
- The blocklist entry that librbd adds when it breaks a lock is not owned by the fixture. It stays until Ceph expires it (one hour by default).
- The cluster resumes paused containers during `Terminate`, but resume a caller-owned container explicitly before it is removed.

Fault injection primitives (`fault.Pause`, `TemporaryBlocklist`, network
interruptions, messenger delays) are described in
[the fault injection guide](guides/fault-injection.md).

### Back up an image and restore it on another cluster

```golang
src, err := source.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
dst, err := destination.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
// destination has an initialized pool "volumes" (rbd.WithPools or InitPool).

// Full backup with a baseline snapshot.
_, _, _ = run(ctx, src, "rbd", "snap", "create", "volumes/disk@baseline")
var full bytes.Buffer
if err := rbd.ExportBackup(ctx, src, "volumes/disk", &full); err != nil {
	t.Fatal(err)
}

// Incremental backup of the changes since baseline, ending at snapshot "next".
// ... write more data on the source ...
_, _, _ = run(ctx, src, "rbd", "snap", "create", "volumes/disk@next")
var delta bytes.Buffer
if err := rbd.ExportIncremental(ctx, src, "volumes/disk@next", "baseline", &delta); err != nil {
	t.Fatal(err)
}

// Restore on the other cluster: full first (it carries "baseline"), then the diff.
if err := rbd.RestoreBackup(ctx, dst, "volumes/disk", bytes.NewReader(full.Bytes())); err != nil {
	t.Fatal(err)
}
if err := rbd.RestoreIncremental(ctx, dst, "volumes/disk", bytes.NewReader(delta.Bytes())); err != nil {
	t.Fatal(err)
}
// volumes/disk now has the head, @baseline and @next of the source image.
```

The restored image is independent of the source: it stays readable and
writable when the source cluster is stopped, and later writes do not change
its restored snapshots. Applying the diff to an image without the `baseline`
snapshot fails. For large images, write the archives to files (for example
under `t.TempDir()`) instead of memory.

## Running native commands

Everything without a typed API is one native command away.

| Need | How |
| --- | --- |
| `ceph` CLI on a cluster | `out, err := cluster.Ceph(ctx, "osd", "pool", "get", "volumes", "all", "--format", "json")` (stdout only) |
| `rbd` or `rados` on a cluster | `control.Exec(ctx, []string{"rbd", ...}, tcexec.Multiplexed())` with `control` from `cluster.ControlContainer(ctx)` (admin credentials) |
| `rbd` on either side of a mirror | `link.SourceRBD(ctx, args...)`, `link.DestinationRBD(ctx, args...)` (stdout only, error on non-zero exit) |
| `rbd` as a restricted identity | Exec in a container attached with `cluster.WithClientIdentity(id)`, passing `--id id.User()` |
| `rbd-mirror` admin socket | `daemon.Exec(ctx, []string{"ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status"}, tcexec.Multiplexed())` for a `daemon` from `link.Daemons()` |
| Ceph options | `cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: ..., Value: ...})`, restored with `Restore` |
| Daemon logs | `daemon.Logs(ctx)`; `rbd-mirror` logs to stderr |

Commands that RBD tests commonly need:

```sh
# images, snapshots, locks and watchers
rbd create --size 64M --image-feature layering,exclusive-lock volumes/disk
rbd snap create volumes/disk@before
rbd status volumes/disk --format json          # watchers
rbd lock ls volumes/disk --format json
rbd namespace list --pool volumes --format json
rbd trash list --pool volumes --namespace tenant-a --format json

# mirroring (run through SourceRBD/DestinationRBD without the leading "rbd")
rbd mirror pool info volumes --format json
rbd mirror pool status volumes --verbose --format json
rbd mirror image status volumes/disk --format json
rbd mirror image snapshot volumes/disk
rbd mirror image demote volumes/disk
rbd mirror image promote volumes/disk            # add --force only to simulate split-brain
rbd mirror image resync volumes/disk
rbd mirror pool peer remove volumes <peer-uuid>
rbd mirror snapshot schedule add --pool volumes 1m

# inside an rbdmirror.Daemon container
ceph --admin-daemon /tmp/rbd-mirror.asok help
ceph --admin-daemon /tmp/rbd-mirror.asok rbd mirror status
```

`rbd-mirror` reads its options from the destination cluster's central
configuration (`client` section) when it starts. You can set them with
`destination.OverrideConfig` before `AddDaemon`; this module does not test any
particular `rbd-mirror` tuning.

More patterns (exec helpers, JSON parsing, attaching clients) are in
[the native commands guide](guides/native-commands.md). Context handling and
cleanup order are in [the contexts and cleanup guide](guides/contexts-and-cleanup.md).

## Limitations and release differences

### Scope

- No image CRUD wrappers. Clone, flatten, trash, migration, group snapshots, encryption and locks are client APIs; test them through your client or the `rbd` CLI.
- No kernel RBD mapping, `rbd-nbd` or `/dev/mapper`. Containers run unprivileged and use no host block devices.
- Native clients on the test host need host network mode on a Linux Docker host (see [the host network guide](guides/host-network.md)). In bridge mode, run clients in attached containers.
- Namespaces have no generation identity; do not delete and re-create a namespace that a descriptor still refers to.
- Incremental restore checks that the baseline snapshot exists by name, not that its content matches.

### Common pitfalls

- **One OSD with replica 1.** `WaitForClean` succeeds, but Ceph reports `POOL_NO_REDUNDANCY`. Tests that require `HEALTH_OK` should use two OSDs and two replicas (the default).
- **Read-only identity with a writable open.** A normal image open registers a watch and needs write permission. Open read-only.
- **Lifting a fence too early.** Keep `TemporaryBlocklist` in place until the old lock owner has observed `ESHUTDOWN`.
