# CephFS harness

## Introduction

The `cephfs/harness` package leases a CephFS subvolume group of a [shared cluster](ceph-harness.md#shared-cluster)
to a tenant. It composes the CephFS service of the [cephfs](cephfs.md)
package with the common [ceph/harness](ceph-harness.md) package, and the
cephfs package does not import it.

All the harness packages are named `harness`; import it with an alias:

```golang
import cephfsharness "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/harness"
```

A shared cluster prepares and probes a service only when the program gives it
the service's support, so list it when you construct the cluster:

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithServices(cephfsharness.Support()),
	cephharness.WithSharedRunOptions( /* the CephFS options of ceph.Run */ ))
```

`Support` registers nothing and holds no resource. Without it the `Lease`
below fails with `ceph.ErrInvalidRequest` before it creates anything.

## Lease

A tenant of a [shared cluster](ceph-harness.md#shared-cluster) leases a subvolume
group of its own. Create the filesystem with `WithFilesystems` in
`cephharness.WithSharedRunOptions`.

```golang
func Support() cephharness.ServiceSupport
func Lease(ctx context.Context, t *cephharness.Tenant, fs string) (*TenantGroup, error)

func (g *TenantGroup) FilesystemName() string
func (g *TenantGroup) Group() string
func (g *TenantGroup) Path() string
func (g *TenantGroup) DataPool() string
func (g *TenantGroup) PoolNamespace() string
func (g *TenantGroup) Client() *ceph.ClientConfig
func (g *TenantGroup) WithClient(opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
func (g *TenantGroup) CreateSubvolume(ctx context.Context, name string, sizeBytes int64) (string, error)
```

`Lease` creates the subvolume group `ID` (the tenant's ID) in filesystem `fs`,
or in the cluster's only filesystem when `fs` is empty. Its directory is
`/volumes/ID`, and its layout puts file data in the RADOS namespace `fsns-ID`
of the filesystem's default data pool. The client `client.ID-cephfs-FS` has
these capabilities:

| Service | Capability |
| --- | --- |
| mon | `allow r` |
| mds | `allow rw fsname=FS path=/volumes/ID` |
| osd | `allow rw pool=DATA namespace=fsns-ID` |

Mount `Path` with that client, with libcephfs `mount_root`, `ceph-fuse -r` or
the kernel client's path. `CreateSubvolume` creates a subvolume in the group,
with a quota when `sizeBytes` is positive, and returns its path; it inherits
the group's data namespace, so the same client uses it.

```golang
tenant := shared.Lease(t)
files, err := cephfsharness.Lease(t.Context(), tenant, "")
if err != nil {
	t.Fatal(err)
}
client, err := testcontainers.Run(t.Context(), ceph.DefaultImage, files.WithClient(), ceph.WithIdleEntrypoint())
testcontainers.CleanupContainer(t, client)
// python3: cephfs.LibCephFS(auth_id=files.Client().User()).mount(mount_root=files.Path(), filesystem_name=files.FilesystemName())
```

Checked natively, the client cannot mount `/`, `/volumes` or another tenant's
group: the MDS refuses the root. It cannot list another group's data
namespace. It cannot set layouts, quotas or snapshots, which need the `p` and
`s` flags.

The tenant's release evicts the sessions of its client that remain, for
example of a client container that ended with the filesystem mounted, and
removes the blocklist entries that the evictions added. It then removes the
group's subvolumes with their snapshots, its other files and the group, and
waits up to 90 seconds for the MDS and the volumes module to purge the data
namespace. It fails the test and poisons the cluster if anything remains.
