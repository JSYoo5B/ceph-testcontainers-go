# Upgrading ceph policy and auth operation names

For the next minor release, update these calls together with your imports and
local wrappers. All methods remain on `*ceph.Container`. Published releases
keep their original APIs; no compatibility aliases are added on main.

| Previous call | Current call | Current management handle / unchanged undo |
| --- | --- | --- |
| `cluster.TemporaryConfig(ctx, setting)` | `cluster.OverrideConfig(ctx, setting)` | `*ceph.ConfigOverrideHandle` / `Restore(ctx)` |
| `cluster.TemporaryOSDFlag(ctx, flag, enabled)` | `cluster.OverrideOSDFlag(ctx, flag, enabled)` | `*ceph.OSDFlagOverrideHandle` / `Restore(ctx)` |
| `cluster.TemporaryMGRModule(ctx, name, enabled)` | `cluster.OverrideMGRModule(ctx, name, enabled)` | `*ceph.MGRModuleOverrideHandle` / `Restore(ctx)` |
| `cluster.TemporaryMessengerPolicy(ctx, policy)` | `cluster.OverrideMessengerPolicy(ctx, policy)` | `*ceph.MessengerPolicyOverrideHandle` / `Restore(ctx)` |
| `cluster.TemporaryClientRevocation(ctx, client)` | `cluster.RevokeClient(ctx, client)` | `*ceph.ClientRevocationHandle` / `Restore(ctx)` |

Explicit return-type declarations, slices, interfaces and wrappers must also
migrate these management objects:

| Previous type | Current type |
| --- | --- |
| `ceph.ConfigOverride` | `ceph.ConfigOverrideHandle` |
| `ceph.OSDFlagOverride` | `ceph.OSDFlagOverrideHandle` |
| `ceph.MGRModuleOverride` | `ceph.MGRModuleOverrideHandle` |
| `ceph.MessengerPolicyOverride` | `ceph.MessengerPolicyOverrideHandle` |
| `ceph.ClientRevocation` | `ceph.ClientRevocationHandle` |

Each handle retains prior state, ownership and possibly partial or uncertain
application for explicit restoration. The suffix distinguishes the management
object from input configuration or observed values; it guarantees neither a
successful apply nor automatic cleanup. Copies retain their existing shared
state. No legacy type aliases are added.

The first four names describe saved policy with explicit restoration, matching
`OverrideFullRatios`. RevokeClient describes the actual auth removal; its handle
can restore the same owned key/capabilities. `DeleteClient` remains permanent
deletion. RevokeClient has no new Tenant counterpart and does not immediately
kill every authenticated MON/MGR/OSD session; native ticket renewal and existing
release-specific session behavior remain unchanged.

These are identifier changes, not new timers or automatic context/scope undo.
Retain a non-nil handle returned with an error: a cancelled or failed command
may have committed. Use a fresh bounded context to retry Restore when needed.
If abandoning the fixture, terminate the disposable cluster. Do not infer
restoration of each individual policy from the general Terminate lifetime.

```go
change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{
    Section: "osd", Name: "osd_max_backfills", Value: "2",
})
if change != nil {
    defer func() {
        cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
        defer cancel()
        if err := change.Restore(cleanup); err != nil {
            t.Error(err)
        }
    }()
}
if err != nil {
    t.Fatal(err)
}
```

Update wrapper names, interface method declarations, mocks and admission-error
assertions if they mention the old identifier. Shared-cluster policy is unchanged:
the four overrides remain exclusive-only, and RevokeClient retains its existing
tenant-restricted admission. No method set gains an additional legacy alias.
Service containers inherit the renamed ceph methods through their embedded
`*ceph.Container`; their own service APIs are not renamed in this checkpoint.

`TemporaryBlocklist` still has a real native duration/expiry; it was kept in `ceph` at
this checkpoint and later became `override.AddBlocklist` (see
[the override package guide](upgrading-override-package.md)).
`OverrideFullRatios`, Pause/Resume and native input Close are unchanged by
this checkpoint. Later service/network/cache stages and the
[final naming audit](../design/api-naming-audit.md) complete the staged pass;
regenerating a baseline inventory alone still does not approve names.
