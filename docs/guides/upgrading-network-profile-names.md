# Upgrading network fault profile names

These source breaks target the next minor release. Published tags keep their
original APIs; main adds no compatibility aliases. Package placement is unchanged.

| Previous name | Current name |
| --- | --- |
| `fault.TemporaryNetworkDelay` | `fault.ApplyNetworkProfile` |
| `fault.TemporaryLinkDelay` | `fault.ApplyLinkProfile` |
| `rgw.Multisite.TemporaryZoneLinkDelay` | `rgw.Multisite.ApplyZoneLinkProfile` |
| `fault.NetworkDelay` | `fault.NetworkProfile` |
| `fault.NetworkDelayLease` | `fault.NetworkProfileLease` |
| `fault.NetworkDelayStatus` | `fault.NetworkProfileStatus` |

Update explicit types, interface declarations, mocks, wrappers, struct literals
and callbacks as well as calls. All parameters and request/status fields keep
their meaning. `MaxNetworkDelay` still bounds the Delay field; `NetworkRate`,
rate units, `NetworkPeer`, `NetworkPacketFault`, `NetemHelperImage` and netem
sentinels retain their names.

```go
lease, err := fault.ApplyNetworkProfile(ctx, cluster, target, fault.NetworkProfile{
    HelperImage: fault.NetemHelperImage,
    Plane:       ceph.PublicNetworkPlane,
    Rate:        16 * fault.MegabitPerSecond,
    QueueLimit:  1000,
})
if lease != nil {
    defer lease.Restore(context.Background())
}
if err != nil {
    return err
}
status, err := lease.Status(ctx) // fault.NetworkProfileStatus
```

The lease owns the netem hierarchy and helper. Keep non-nil leases returned
with errors: the kernel may already have changed. Keep the returned lease
pointer; it contains a mutex and must not be copied after use. Restore remains
retryable, and identity/external-edit checks still refuse foreign state. A
non-nil lease alone does not prove successful application. No native expiry,
context-cancellation rollback or scope cleanup is introduced.

`ApplyNetworkProfile` still requires the cluster's exclusive admission when
shared, and cluster teardown restores held profiles in its existing phase.
`ApplyLinkProfile` remains caller-owned: restore it before removing the target
or named bridge. `ApplyZoneLinkProfile` selects its fixture-owned network/peer
and registers cleanup with the RGW multisite. IPv4/IPv6, peer/port, plane,
bandwidth/queue, loss and packet-fault semantics are unchanged. Status reports
kernel configuration/counters; workload throughput and receiver effects need
their own measurements. The `ErrNetem*` sentinels keep their `errors.Is` categories.

Native test names and scenario selectors are unchanged. This checkpoint creates
no new native capability and does not complete #14's remaining RBD helper or
baseline/field/interface audit. See the [naming inventory](../design/api-naming.md),
[fault reference](../fault.md) and [RGW reference](../rgw.md).
