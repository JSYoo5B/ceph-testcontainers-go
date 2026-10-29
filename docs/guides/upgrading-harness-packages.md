# Upgrading to the harness packages

> Historical record: the extraction direction below was superseded by complete
> removal of the shared-cluster/tenant harness. These packages and policies are
> absent from current source. Use [independent fixtures](remove-shared-harness.md) instead. All snapshots, counts and statements below
> describe the earlier extraction, not the supported API.

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase, no compatibility alias and no
forwarding declaration. 0.x: old names are removed without aliases.

The shared cluster and the tenant harness moved out of package `ceph` into
packages under the domains that own them:

| Package | Responsibility |
| --- | --- |
| `ceph/harness` | `SharedCluster`, `Tenant`, `RunTests`, the shared and lease options, `SharedPool`, `ObserveTenant` and the service support option |
| `rbd/harness` | The RBD tenant lease and its shared-cluster support |
| `cephfs/harness` | The CephFS tenant lease and its shared-cluster support |
| `rgw/harness` | The RGW tenant lease and its shared-cluster support |

All four are named `harness`; import them with aliases such as `cephharness`
and `rbdharness`. Core `ceph` and the service packages do not import them.

## Migration table

| Previous name | Current name |
| --- | --- |
| `ceph.NewShared`, `ceph.RunTests` | `cephharness.NewShared`, `cephharness.RunTests` |
| `ceph.SharedCluster`, `ceph.SharedOption`, `ceph.LeaseOption`, `ceph.SharedStats` | the same names in `cephharness` |
| `ceph.WithSharedRunOptions`, `ceph.WithMaxTenants`, `ceph.WithMaxRecycles`, `ceph.WithKeepOnTenantFailure` | the same names in `cephharness` |
| `ceph.Tenant`, `ceph.TenantAccess`, `ceph.TenantReadOnly`, `ceph.TenantReadWrite` | the same names in `cephharness` |
| `ceph.SharedPool` | `cephharness.SharedPool` (same value, `"tenants"`) |
| `ceph.ObserveTenant(tenant)` | `cephharness.ObserveTenant(tenant)`; it returns the same `*ceph.Observer` |
| `rbd.Lease`, `rbd.TenantNamespace` | `rbdharness.Lease`, `rbdharness.TenantNamespace` |
| `cephfs.Lease`, `cephfs.TenantGroup` | `cephfsharness.Lease`, `cephfsharness.TenantGroup` |
| `rgw.Lease`, `rgw.TenantUser` | `rgwharness.Lease`, `rgwharness.TenantUser` |
| (implicit: importing `rbd`, `cephfs` or `rgw` registered its shared-cluster support) | `cephharness.WithServices(rbdharness.Support(), ...)` in the `NewShared` call |
| `ceph.ErrSharedCluster`, `*ceph.Container` | unchanged; they stay in `ceph` |

## Service support is explicit

Importing a service package used to register its shared-cluster support for
every shared cluster of the binary. Nothing registers at import now. A
`SharedCluster` prepares and probes exactly the services it was given with
`WithServices`, and a service `Lease` on a cluster that was not given its
support fails with `ceph.ErrInvalidRequest` before it creates anything. List one
support for each service your tests lease:

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithServices(rbdharness.Support(), cephfsharness.Support(), rgwharness.Support()),
	cephharness.WithSharedRunOptions(ceph.WithOSDCount(1), rbd.WithPools(ceph.PoolConfig{Name: "rbd"})))

func TestMain(m *testing.M) { os.Exit(cephharness.RunTests(m, shared)) }
```

The behavior of leases, release verification, poisoning, recycling,
cancellation, uncertain outcomes and cleanup is unchanged by the move.
