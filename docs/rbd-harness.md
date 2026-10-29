# RBD harness

## Introduction

The `rbd/harness` package leases an RBD namespace of a [shared cluster](ceph-harness.md#shared-cluster)
to a tenant. It composes the RBD service of the [rbd](rbd.md)
package with the common [ceph/harness](ceph-harness.md) package, and the
rbd package does not import it.

All the harness packages are named `harness`; import it with an alias:

```golang
import rbdharness "github.com/JSYoo5B/ceph-testcontainers-go/rbd/harness"
```

A shared cluster prepares and probes a service only when the program gives it
the service's support, so list it when you construct the cluster:

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithServices(rbdharness.Support()),
	cephharness.WithSharedRunOptions( /* the RBD options of ceph.Run */ ))
```

`Support` registers nothing and holds no resource. Without it the `Lease`
below fails with `ceph.ErrInvalidRequest` before it creates anything.

## Lease

A tenant of a [shared cluster](ceph-harness.md#shared-cluster) leases an RBD
namespace of its own. Create the RBD pool with `WithPools` in
`cephharness.WithSharedRunOptions`.

```golang
func Support() cephharness.ServiceSupport
func Lease(ctx context.Context, t *cephharness.Tenant, pool string) (*TenantNamespace, error)

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
images, err := rbdharness.Lease(t.Context(), tenant, "rbd")
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
