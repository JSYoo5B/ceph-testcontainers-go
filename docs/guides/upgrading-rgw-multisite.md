# Upgrading to the rgw/multisite package

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase and no compatibility alias.

RGW multisite orchestration moved from package `rgw` to the new package
`rgw/multisite` (import path
`github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite`, package name
`multisite`). Single-gateway use stays in `rgw`: `Gateway`, `Config`, `Run`,
`Start`, `Gateway.Remove`, `Gateways`, `WithGateways`, `WithImage`, users,
tenants, accounts, quotas, placement and TLS are unchanged, and `Realm`,
`Zonegroup` and `Zone` of `rgw.Config` still select the native scope of one
gateway. `rgw/multisite` imports `rgw`, `rgw` does not import it, and
`Fixture.Source`, `Fixture.Destination` and `Zone.Gateway` keep the type
`*rgw.Gateway`.

Three names changed because `multisite.Multisite` would stutter and
`multisite.RunMultisite` repeats the package: `Multisite` is `Fixture`,
`RunMultisite` is `Run` and `MultisiteConfig` is `Config`. Everything else keeps
its name.

## Migration table

| Previous name | Current name |
| --- | --- |
| `rgw.RunMultisite` | `multisite.Run` |
| `rgw.RunTopology` | `multisite.RunTopology` |
| `rgw.MultisiteConfig` | `multisite.Config` |
| `rgw.Multisite` | `multisite.Fixture` |
| `rgw.BucketSyncPolicyStatus` | `multisite.BucketSyncPolicyStatus` |
| `rgw.BucketSyncStatus` | `multisite.BucketSyncStatus` |
| `rgw.DataSyncStatus` | `multisite.DataSyncStatus` |
| `rgw.MetadataSyncStatus` | `multisite.MetadataSyncStatus` |
| `rgw.SyncBucketIdentity` | `multisite.SyncBucketIdentity` |
| `rgw.SyncBucketSelector` | `multisite.SyncBucketSelector` |
| `rgw.SyncFlowConfig` | `multisite.SyncFlowConfig` |
| `rgw.SyncGroup` | `multisite.SyncGroup` |
| `rgw.SyncGroupConfig` | `multisite.SyncGroupConfig` |
| `rgw.SyncGroupStatus` | `multisite.SyncGroupStatus` |
| `rgw.SyncObjectTag` | `multisite.SyncObjectTag` |
| `rgw.SyncPipeConfig` | `multisite.SyncPipeConfig` |
| `rgw.SyncPolicyScope` | `multisite.SyncPolicyScope` |
| `rgw.SyncStatus` | `multisite.SyncStatus` |
| `rgw.TopologyConfig` | `multisite.TopologyConfig` |
| `rgw.Zone` | `multisite.Zone` |
| `rgw.ZoneConfig` | `multisite.ZoneConfig` |
| `rgw.Zonegroup` | `multisite.Zonegroup` |
| `rgw.ZonegroupConfig` | `multisite.ZonegroupConfig` |
| `rgw.SyncAllowed` | `multisite.SyncAllowed` |
| `rgw.SyncEnabled` | `multisite.SyncEnabled` |
| `rgw.SyncForbidden` | `multisite.SyncForbidden` |

All 27 methods of `rgw.Multisite` and the 3 methods of `rgw.SyncGroup` keep their names,
signatures and behavior on `multisite.Fixture` and `multisite.SyncGroup`:
`AddZone`, `AddZonegroup`, `ApplySyncGroup`, `ApplyZoneLinkProfile`, `BucketSyncStatus`, `CreateSyncFlow`, `CreateSyncGroup`, `CreateSyncPipe`, `DestinationAdmin`, `InterruptZoneLink`, `PullDestinationPeriod`, `PullSourcePeriod`, `RemoveSyncFlow`, `RemoveSyncGroup`, `RemoveSyncPipe`, `RemoveZone`, `SetSyncGroupStatus`, `SetSyncPipePrefix`, `SourceAdmin`, `SyncStatus`, `Terminate`, `WaitBucketSyncPolicyReady`, `WaitBucketSyncReady`, `WaitSyncReady`, `ZoneAdmin`, `Zonegroups`, `Zones`.
`SyncGroup` has `GoString`, `ID`, `String`.

## Import and variable names

Add `"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"` where a file
used one of the names above and keep the `rgw` import where it still calls
`rgw.Start`, `rgw.Gateway` or another single-gateway declaration. A local
variable named `multisite` hides the package; rename it.

```golang
// Before
site, err := rgw.RunMultisite(ctx, ceph.DefaultImage, rgw.MultisiteConfig{
	Source: source, Destination: destination,
})

// After
site, err := multisite.Run(ctx, ceph.DefaultImage, multisite.Config{
	Source: source, Destination: destination,
})
```

The behavior is unchanged: setup admission, period commit and reload order,
scoped sync policy, cancellation, uncertain outcomes and owned cleanup are the
same code in a new package. See [rgw-multisite.md](../rgw-multisite.md).
