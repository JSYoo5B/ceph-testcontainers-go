# Upgrading to handle operations

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase and no compatibility alias.

An operation on one daemon, pool, Cephx identity, filesystem or gateway is now a
method of the handle it acts on and takes no name or ID. The former methods and
package functions are removed. A function that creates a handle, such as
`AddOSD`, `AddMonitor`, `AddManager`, `CreatePool`, `CreateClient`,
`cephfs.Start` and `rgw.Start`, and every cluster-wide operation stay where they
are. `Container.Pool(ctx, name)` is new: it returns the handle of a pool that
`CreatePool` did not create.

## Migration table

| Previous call | Current call |
| --- | --- |
| `c.RemoveOSD(ctx, osd.ID)` | `osd.Remove(ctx)` |
| `c.SetOSDIn(ctx, osd.ID, in)` | `osd.SetIn(ctx, in)` |
| `c.DetachOSD(ctx, osd.ID)` | `osd.Detach(ctx)` |
| `c.ReopenOSD(ctx, detached)` | `detached.Reopen(ctx)` |
| `c.RemoveMonitor(ctx, mon.DaemonName)` | `mon.Remove(ctx)` |
| `c.RemoveManager(ctx, mgr.DaemonName)` | `mgr.Remove(ctx)` |
| `c.AdminSocket(ctx, "osd.0", args...)` | `osd.AdminSocket(ctx, args...)` |
| `c.AdminSocket(ctx, "mon."+mon.DaemonName, args...)` | `mon.AdminSocket(ctx, args...)` |
| `c.AdminSocket(ctx, "mgr."+mgr.DaemonName, args...)` | `mgr.AdminSocket(ctx, args...)` |
| `c.RotateDaemonKey(ctx, "osd.0")` | `osd.RotateKey(ctx)` |
| `c.RotateDaemonKey(ctx, "mgr."+mgr.DaemonName)` | `mgr.RotateKey(ctx)` |
| `c.RotateDaemonKey(ctx, "mon.a")` | none: `*ceph.MonitorContainer` has no `RotateKey`, because the MONs share the `mon.` key |
| `c.PoolStatus(ctx, name)` | `pool.Status(ctx)` |
| `c.PoolUsage(ctx, name)` | `pool.Usage(ctx)` |
| `c.PoolPGs(ctx, name)` | `pool.PGs(ctx)` |
| `c.SetPoolPGCount(ctx, name, n)` | `pool.SetPGCount(ctx, n)` |
| `c.WaitForPoolPGCount(ctx, name, n)` | `pool.WaitForPGCount(ctx, n)` |
| `c.SetPoolPlacement(ctx, name, placement)` | `pool.SetPlacement(ctx, placement)` |
| `c.SetPoolQuota(ctx, name, quota)` | `pool.SetQuota(ctx, quota)` |
| `c.SetPoolReplication(ctx, name, replicas, minSize)` | `pool.SetReplication(ctx, replicas, minSize)` |
| `c.RemovePool(ctx, name)` | `pool.Remove(ctx)` |
| `c.ClientCapabilities(ctx, client)` | `client.Capabilities(ctx)` |
| `c.UpdateClientCaps(ctx, client, caps)` | `client.UpdateCaps(ctx, caps)` |
| `c.ReplaceClientKey(ctx, client, opts...)` | `client.ReplaceKey(ctx, opts...)` |
| `c.RevokeClient(ctx, client)` | `client.Revoke(ctx)` |
| `c.DeleteClient(ctx, client)` | `client.Delete(ctx)` |
| `cephfs.Remove(ctx, c, name)` | `fs.Remove(ctx)` |
| `rgw.Remove(ctx, c, name)` | `gateway.Remove(ctx)` |
| `fs.RemoveStoppedMDS(ctx, mds)` | `mds.RemoveStopped(ctx)` |
| `fs.AddMDSReplacement(ctx, mds)` | `mds.AddReplacement(ctx)` |

`osd`, `mon` and `mgr` come from `c.OSDs(ctx)`, `c.Monitors(ctx)` and
`c.Managers(ctx)`, or from `AddOSD`, `AddMonitor` and `AddManager`. `pool` comes
from `c.CreatePool(ctx, config)`, or from `c.Pool(ctx, name)` for a pool that
`CreatePool` did not create, such as an initial pool from `WithPools`. `fs` and
`gateway` come from `cephfs.Start`, `cephfs.Filesystems`, `rgw.Start` and
`rgw.Gateways`. Where a name or ID is all you have, look the handle up once:

```go
osds, err := cluster.OSDs(ctx)
if err != nil {
    return err
}
for _, osd := range osds {
    if osd.ID == 2 {
        err = osd.Remove(ctx)
    }
}
```

`Tenant.PoolStatus` and `Tenant.PoolUsage` keep their signatures and take a pool
name.

## Behavior changes

- A handle acts on its own daemon instance, not on whatever currently owns the ID
  or name. The handle of a removed, purged or detached daemon returns an error.
  Before, `RemoveOSD(ctx, id)` could act on a different container that took the
  same ID after `ReopenOSD`. After `Detach` and `Reopen`, the old
  `*ceph.OSDContainer` stays tied to the removed instance, as documented, and
  further operations go through the handle that `Reopen` returns.
- A handle of a terminated cluster returns an error.
- Daemon names are no longer parsed at run time. `AdminSocket` and `RotateKey`
  have no "malformed", "unknown" or "not owned" name to refuse. A call that the
  type system rejects, such as rotating a MON key, does not compile.
- `cephfs.Remove` and `rgw.Remove` do not resolve a name at call time. A
  descriptor that was already removed returns an error instead of "not found".
- A `*ceph.Pool` identifies the native pool by name and native ID. After
  `pool.Remove`, or when the name belongs to another pool, every method of the
  handle returns an error, so a later pool that reuses the name is not affected
  through an old handle. Copies of a `Pool` share that identity. A `Pool` that
  does not come from `CreatePool` or `Pool` has no cluster and its methods return
  an error.
- `CreatePool` reads the native ID of the pool it created before it returns
  without an error. If that read fails, `CreatePool` returns the handle together
  with an error. That handle is not bound yet and binds to the pool of its name
  at its first successful operation, so use it only to clean up. A handle
  returned without an error is always bound.
- Client operations behave as before for the descriptor they are called on,
  including the refusals for stale, replaced, revoked and deleted descriptors.
- Unchanged: the last-OSD refusal, the registration UUID checks in every removal
  phase, fencing and draining of native commands, retry and uncertain-outcome
  ownership, the refusal to remove a detaching or detached OSD, the
  monitor-quorum and manager-candidate preconditions, and the MDS retirement
  rules.

See the [Container reference](../ceph.md#container-methods),
[cephfs](../cephfs.md) and [rgw](../rgw.md).
