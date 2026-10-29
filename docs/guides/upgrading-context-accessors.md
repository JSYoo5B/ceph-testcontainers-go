# Upgrading the topology accessors

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase and no compatibility alias.

Each accessor now exists once, in the form that takes `ctx` first and returns
an error, under the shorter name. The no-context forms are removed and the
`...Context` forms take over their names.

| Previous call | Current call |
| --- | --- |
| `(*ceph.Container).ConnectionConfig()` | `ConnectionConfig(ctx)` (formerly `ConnectionConfigContext`) |
| `(*ceph.Container).ControlContainer()` | `ControlContainer(ctx)` (formerly `ControlContainerContext`), now with an `error` result |
| `(*ceph.Container).ClientContainerContext(ctx)` | `ClientContainer(ctx)` |
| `(*ceph.Container).Managers()` | `Managers(ctx)` (formerly `ManagersContext`), now with an `error` result |
| `(*ceph.Container).Monitors()` | `Monitors(ctx)` (formerly `MonitorsContext`), now with an `error` result |
| `(*ceph.Container).OSDs()` | `OSDs(ctx)` (formerly `OSDsContext`), now with an `error` result |
| `rgw.Gateways(c)` | `rgw.Gateways(ctx, c)` (formerly `rgw.GatewaysContext`) |
| `(*rgw.Container).Gateways()` | `Gateways(ctx)`, which delegates to `rgw.Gateways(ctx, c.Container)` |

Update calls, function values, wrappers, interface declarations and mocks:

```go
osds, err := cluster.OSDs(ctx)
if err != nil {
    return err
}
for _, osd := range osds {
    _ = osd
}
```

Behavior changes:

- An unavailable cluster is an error. The calls return `(nil, err)` rather than
  conflating failure with an empty daemon set.
- Waiting for serialized topology work honors `ctx`. A cancelled call returns
  the context error and changes nothing.
- A nil `*ceph.Container` returns an error.
- Ordering is unchanged: managers and monitors by daemon name, OSDs by ID and
  gateways by name.

`cephfs.Filesystems`, `(*cephfs.Container).Filesystems`,
`(*cephfs.Filesystem).MDSs`, `(*cephfs.Mirror).Daemons`,
`(*rbd.Mirror).Daemons` and `(*ceph.Container).ManagerContainer` exist in one
form and are unchanged.

See the [Container reference](../ceph.md#container-methods-cluster-topology)
and the [rgw reference](../rgw.md#gateways).
