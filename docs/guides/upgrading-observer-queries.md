# Upgrading to Observer queries

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase and no compatibility alias.

The read-only observations of a cluster are methods of `ceph.Observer` only.
`ceph.Observe(cluster)` returns the `Observer` of a `*ceph.Container`.
The `Container` methods of these names are removed. The shared-cluster and
tenant harness was subsequently removed; see [Independent test fixtures](remove-shared-harness.md).
Pools are individual objects: `Observer.Pools` lists them as `*ceph.Pool`, and
the reads of one pool are the methods of that `Pool`.

## Migration table

| Previous call | Current call |
| --- | --- |
| `c.Status(ctx)` | `observer.Status(ctx)` |
| `c.ManagerStatus(ctx)` | `observer.ManagerStatus(ctx)` |
| `c.DaemonAddresses(ctx)` | `observer.DaemonAddresses(ctx)` |
| `c.Configuration(ctx)` | `observer.Configuration(ctx)` |
| `c.BlocklistEntries(ctx)` | `observer.BlocklistEntries(ctx)` |
| `c.MGRModules(ctx)` | `observer.MGRModules(ctx)` |
| `c.OSDFlags(ctx)` | `observer.OSDFlags(ctx)` |
| `c.OSDStates(ctx)` | `observer.OSDStates(ctx)` |
| `c.ManagerServices(ctx)` | `observer.ManagerServices(ctx)` |
| `c.QuorumStatus(ctx)` | `observer.QuorumStatus(ctx)` |
| `c.MonitorBootstrapAddresses(ctx)` | `observer.MonitorBootstrapAddresses(ctx)` |
| `c.HealthDetails(ctx)` | `observer.HealthDetails(ctx)` |
| `c.FullRatios(ctx)` | `observer.FullRatios(ctx)` |
| `c.PGInconsistencies(ctx, pgid)` | `observer.PGInconsistencies(ctx, pgid)` |
| `c.ObjectWatchers(ctx, pool, ns, object)` | `observer.ObjectWatchers(ctx, pool, ns, object)` |
| `c.ManagerMetrics(ctx)` | `observer.ManagerMetrics(ctx)` |
| `c.Pools(ctx)` | `pools, err := observer.Pools(ctx)`, then `pool.Status(ctx)` for the `PoolState` of each |

`observer` is the result of `ceph.Observe(c)`. Keep it as long as you like: it
holds no result and every query reads the cluster again.

```golang
observer, err := ceph.Observe(cluster)
if err != nil {
	t.Fatal(err)
}
pools, err := observer.Pools(ctx)
if err != nil {
	t.Fatal(err)
}
for _, pool := range pools {
	state, err := pool.Status(ctx) // the former PoolState of c.Pools(ctx)
	...
}
```

## Notes

- A `Pool` from `Observer.Pools` identifies its native pool by name and native
  ID, like the one of `Container.Pool`, and carries only what the listing
  reports (`Name`, `PGNum`, `Replicas`, `MinSize`, `ErasureCodeProfile`).
  `Pool.Status` returns the full `PoolState`.
- An interface that listed these `Container` methods needs the `Observer` call
  in an adapter of your own.
