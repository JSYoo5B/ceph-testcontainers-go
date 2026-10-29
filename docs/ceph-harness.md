# Ceph harness

## Introduction

The `ceph/harness` package shares one cluster between the tests of a test
binary. It owns sharing, leasing and reuse: `SharedCluster`, `Tenant`,
`RunTests`, the lease options, the shared pool and the tenant `Observer`
constructor. Core [ceph](ceph.md) keeps the cluster, its operations and the gate
that refuses a shared cluster's raw access; this package depends on it and it
does not depend on this package. The service harness packages
[rbd/harness](rbd-harness.md), [cephfs/harness](cephfs-harness.md) and
[rgw/harness](rgw-harness.md) compose their service with this package.

All four packages are named `harness`. Import them with aliases:

```golang
import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephharness "github.com/JSYoo5B/ceph-testcontainers-go/ceph/harness"
	rbdharness "github.com/JSYoo5B/ceph-testcontainers-go/rbd/harness"
)
```

The names moved here from `ceph` in v0.4 without aliases or forwarding
declarations; see the [upgrade guide](guides/upgrading-harness-packages.md).

## Shared cluster

A cluster takes 15 to 25 seconds to start, so a package with many small RADOS
tests spends most of its time starting clusters. A `SharedCluster` starts one
cluster for the whole test binary and lends each test a tenant: a RADOS
namespace of one shared pool and a Cephx client that can reach nothing else.
A test that needs the whole cluster takes it with `Exclusive`. Tests that need a
cluster with other `ceph.Run` options, or several clusters, keep using `ceph.Run`.

```golang
func NewShared(image string, opts ...SharedOption) *SharedCluster
func WithSharedRunOptions(opts ...testcontainers.ContainerCustomizer) SharedOption
func WithMaxTenants(n int) SharedOption
func WithMaxRecycles(n int) SharedOption
func WithServices(supports ...ServiceSupport) SharedOption
func RunTests(m *testing.M, s ...*SharedCluster) int

func (s *SharedCluster) Lease(t testing.TB, opts ...LeaseOption) *Tenant
func (s *SharedCluster) Exclusive(t testing.TB) *ceph.Container
func (s *SharedCluster) Close(ctx context.Context) error
func (s *SharedCluster) Stats() SharedStats
func WithKeepOnTenantFailure() LeaseOption
```

Create the shared cluster in a package variable and end it in `TestMain`.
`NewShared` starts nothing; the first `Lease` starts the cluster with the
options of `WithSharedRunOptions`, which are `Run` options. `RunTests` runs the
tests and then closes every shared cluster you pass it:

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithSharedRunOptions(ceph.WithOSDCount(1)))

func TestMain(m *testing.M) { os.Exit(cephharness.RunTests(m, shared)) }

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

### Tenant

```golang
func (t *Tenant) ID() string
func (t *Tenant) Pool() string
func (t *Tenant) Namespace() string
func (t *Tenant) Client() *ceph.ClientConfig
func (t *Tenant) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
func (t *Tenant) WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
func (t *Tenant) CreateClient(ctx context.Context, name string, access TenantAccess) (*ceph.ClientConfig, error)
func (t *Tenant) RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)

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
label. The tenant has no read-only views of its own: `cephharness.ObserveTenant(tenant)`
returns the [Observer](#observer) that reads the shared cluster for it.

What a tenant can do:

- read and write objects of its own namespace, through its clients;
- create read-only and read-write clients of that namespace;
- read the cluster's status, health, pools, usage, watchers, OSD flags and
  blocklist through `cephharness.ObserveTenant(tenant)`.

What it cannot do, checked natively:

- read, write or list another tenant's namespace, every namespace (`ls --all`),
  the default namespace of the shared pool or another pool: Ceph returns
  `EPERM`;
- change configuration, OSD flags or the blocklist: Ceph returns `EACCES`.

Tenancy is isolation, not secrecy: a tenant sees pool names, usage and the
cluster's health.

### Service tenants

A tenant can also lease RBD, CephFS and RGW resources of the shared cluster.
Start the services with the usual `Run` options in `WithSharedRunOptions`, give
the shared cluster the support of each service that its tests lease with
`WithServices`, and call the service harness package's `Lease` with the tenant.
Importing a service harness registers nothing: a cluster that was not given a
service's support neither prepares nor probes it, and the service's `Lease`
fails with `ErrInvalidRequest` before it creates anything.

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithServices(rbdharness.Support(), cephfsharness.Support(), rgwharness.Support()),
	cephharness.WithSharedRunOptions(
		ceph.WithOSDCount(1),
		rbd.WithPools(ceph.PoolConfig{Name: "rbd"}),
		cephfs.WithFilesystems(),
		rgw.WithGateways(),
	))

func TestUsesEveryService(t *testing.T) {
	tenant := shared.Lease(t)
	images, err := rbdharness.Lease(t.Context(), tenant, "rbd")      // RBD namespace ID of pool rbd
	files, err := cephfsharness.Lease(t.Context(), tenant, "")       // subvolume group ID
	objects, err := rgwharness.Lease(t.Context(), tenant)            // RGW tenant ID, user ID$user
	...
}
```

| Lease | The tenant owns | Its client or credentials |
| --- | --- | --- |
| `rbdharness.Lease(ctx, tenant, pool)` | RBD namespace `ID` of `pool` | `client.ID-rbd-POOL`: `mon 'allow r'`, `osd` and `mgr 'profile rbd pool=POOL namespace=ID'` |
| `cephfsharness.Lease(ctx, tenant, fs)` | subvolume group `ID`, directory `/volumes/ID`, data in namespace `fsns-ID` of the default data pool | `client.ID-cephfs-FS`: `mon 'allow r'`, `mds 'allow rw fsname=FS path=/volumes/ID'`, `osd 'allow rw pool=DATA namespace=fsns-ID'` |
| `rgwharness.Lease(ctx, tenant)` | RGW tenant `ID` and its buckets | S3 keys of user `ID$user` |

The service leases are described in [RBD](rbd-harness.md),
[CephFS](cephfs-harness.md) and [RGW](rgw-harness.md). A tenant leases each RBD pool, each
filesystem and RGW once. The clients are the tenant's: `Tenant.WithClientIdentity`
and `RunNativeCommand` accept them, and the release deletes them.

Before the baseline, the shared cluster creates what a service would
otherwise create on a tenant's first use: the pools of the RGW zone, such as
the bucket index and data pools. It also waits until the MGR `volumes` module
answers.

### Release, verification and poisoning

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

### Exclusive lease

`Exclusive` lends a test the whole shared cluster: the `*ceph.Container`, with
every method available except `Terminate`. Use it for a test that needs what
the tenant gate refuses, such as `override.SetConfig`, OSD flags, pool changes,
the raw `Ceph` CLI or `AddOSD`, in a package whose other tests share the
cluster:

```golang
func TestRecoversWithNoout(t *testing.T) {
	cluster := shared.Exclusive(t)
	if _, err := override.SetOSDFlag(t.Context(), cluster, ceph.OSDFlagNoOut, true); err != nil {
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


### Service support

```golang
type ServiceSupport struct{ /* unexported */ }
func WithServices(supports ...ServiceSupport) SharedOption
```

A `ServiceSupport` is what a shared cluster prepares before its baseline and
lists when it verifies a release for one service, so that a tenant's service
resources are attributed and a leftover is found. Take it from the service's
harness package (`rbdharness.Support()`, `cephfsharness.Support()`,
`rgwharness.Support()`) and pass it to `WithServices`. The value is opaque: it
carries callbacks that receive the cluster, which this package's API does not
show. Nothing registers a service at import or at `init`, and no process-global
registry exists: each `SharedCluster` owns exactly the supports it was given, so
two clusters of one binary can select different services and a binary that
selects none prepares and probes none.

The first `Lease` or `Exclusive` returns `ceph.ErrInvalidRequest` for a zero
`ServiceSupport` and for a service given twice. A service `Lease` on a cluster
that was not given its support returns `ceph.ErrInvalidRequest` before it
creates anything.

### Observe a tenant

```golang
func ObserveTenant(tenant *Tenant) (*ceph.Observer, error)
```

Returns the [ceph.Observer](ceph.md#observer) of the tenant's shared cluster.
The Observer and its queries stay in `ceph`; this constructor binds it to the
tenant. A nil tenant returns an error that matches `ceph.ErrInvalidRequest` and
a nil Observer. The tenant is not validated or read here: every query checks
the lease again, so a retained Observer, or a `ceph.Pool` it listed, is refused
after the release and never gains the privileges of a later exclusive lease.

### Limits

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
- The release of an exclusive lease marks the cluster retired before it
  terminates it. From then on the gate refuses a retained `*ceph.Container`,
  and a handle taken from it, with `ErrSharedCluster`, except for the
  operations that only read state; a termination that failed leaves a running
  cluster that stays retired until `Close` ends it. The gate does not cover the
  embedded `Container.Container` field of the lent `*ceph.Container`, which is
  a plain testcontainers container: a caller that kept it reaches the cluster
  past every gate, and what stops it after a normal release is the terminated
  container, not an admission check. After a termination that failed it still
  answers until `Close`. A `ceph.ClientConfig` that a tenant got from
  `Tenant.Client` or `Tenant.CreateClient` is refused by the gate in every
  mode, so an exclusive lease that takes the same cluster does not give it
  more than the tenant had. This enforcement is an implementation-side
  proposal that awaits the owner's acceptance; the
  [design note](design/shared-harness-boundary.md) records the mechanism and
  what it leaves open.
