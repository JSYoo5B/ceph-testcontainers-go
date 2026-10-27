# Upgrading to the override package

0.x: old names are removed without aliases. The client blocklist override
moved out of `ceph` into the new package `override`, followed by the full
ratios, global OSD flag, central configuration and messenger policy overrides.
There is no deprecated alias, wrapper or forwarder in `ceph`,
and `Tenant` never had one. Published
releases keep their original API. Update the calls, the type names and the
`import` line together.

```golang
import "github.com/JSYoo5B/ceph-testcontainers-go/override"
```

| Previous call or type | New call or type |
| --- | --- |
| `cluster.TemporaryBlocklist(ctx, address, duration)` | `override.AddBlocklist(ctx, cluster, address, duration)` |
| `*ceph.BlocklistOverride` | `*override.BlocklistHandle` |
| `(*ceph.BlocklistOverride).Address` | `(*override.BlocklistHandle).Address` (same value) |
| `(*ceph.BlocklistOverride).Restore` | `(*override.BlocklistHandle).Restore` (same removal rules) |
| `cluster.OverrideFullRatios(ctx, ratios)` | `override.SetFullRatios(ctx, cluster, ratios)` |
| `*ceph.FullRatiosOverride` | `*override.FullRatiosHandle` |
| `(*ceph.FullRatiosOverride).Restore` | `(*override.FullRatiosHandle).Restore` (same tuple and identity rules) |
| `cluster.OverrideConfig(ctx, setting)` | `override.SetConfig(ctx, cluster, setting)` |
| `*ceph.ConfigOverrideHandle` | `*override.ConfigHandle` |
| `(*ceph.ConfigOverrideHandle).Restore`, `String`, `GoString` | `(*override.ConfigHandle).Restore`, `String`, `GoString` (same rules) |
| `cluster.OverrideMessengerPolicy(ctx, policy)` | `override.SetMessengerPolicy(ctx, cluster, policy)` |
| `*ceph.MessengerPolicyOverrideHandle` | `*override.MessengerPolicyHandle` |
| `(*ceph.MessengerPolicyOverrideHandle).Restore` | `(*override.MessengerPolicyHandle).Restore` (same reverse order) |

`ceph.BlocklistEntry`, `Observer.BlocklistEntries`, `ceph.FullRatios`,
`ceph.FullRatioSnapshot`, `Observer.FullRatios`, `ceph.ConfigSetting`,
`ceph.ConfigEntry`, `Observer.Configuration`, `ceph.MessengerPolicy` and
`ceph.MessengerConnectionMode` are unchanged.

The OSD flag override follows the same rules:

| Previous call or type | New call or type |
| --- | --- |
| `cluster.OverrideOSDFlag(ctx, flag, enabled)` | `override.SetOSDFlag(ctx, cluster, flag, enabled)` |
| `*ceph.OSDFlagOverrideHandle` | `*override.OSDFlagHandle` |
| `(*ceph.OSDFlagOverrideHandle).Restore` | `(*override.OSDFlagHandle).Restore` (same restoration rules) |

`ceph.OSDFlag`, its eleven constants and `Observer.OSDFlags` are unchanged.

## What is the same

Admission order, the context and lock behavior, the error texts and classes, the
input checks, the overlap refusal, the retry after a lost reply and the refusal
of a `Restore` after `Terminate` are the behavior of the former methods. For the
blocklist that includes the refusal of an existing entry and the expiration check
at `Restore`. For the OSD flag it includes the supported-flag list, the lease
without a write when the flag already has the requested state, and the refusal of
a `Restore` over an outside change. A cluster that a `SharedCluster` lends to
tenants still refuses the call with `ceph.ErrSharedCluster`; `OperationError.Op`
now reads `override.AddBlocklist` or `override.SetOSDFlag` instead of the former
method name.

## What differs

- The returned handle is never nil. A call that was refused or never applied
  returns a handle whose `Restore` does nothing: it returns nil, takes no lock
  and never reports a restoration. The reason is the returned error. Remove the
  `if fence != nil` or `if hold != nil` guard around a deferred or registered
  `Restore`, and test `err` for the outcome. A check such as `hold == nil` that
  detected a refusal must test `err != nil` instead.
- A call whose outcome is uncertain (the add or the flag write may have been
  applied) still returns a real handle together with the error.
- A nil cluster no longer panics. It returns a not-applied handle and an error
  classified `ceph.ErrInvalidRequest` with `Op` `override.AddBlocklist` or
  `override.SetOSDFlag`.

```golang
// before
fence, err := cluster.TemporaryBlocklist(ctx, address, time.Minute)
if fence != nil {
	defer fence.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}

// after
fence, err := override.AddBlocklist(ctx, cluster, address, time.Minute)
defer fence.Restore(cleanupCtx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

## Full ratios

`Container.OverrideFullRatios` became `override.SetFullRatios` and the handle
`ceph.FullRatiosOverride` became `override.FullRatiosHandle`, with the same
`Restore`. The verb is `Set` because the call replaces existing values and
`Restore` puts the captured tuple back. The validation, the ordered intermediate
steps, the single captured control container and bootstrap identity, the
deadline that starts before the wait for the topology lock, the overlap refusal
(including no-op requests), the reconcile rules at `Restore` and the refusal of a
`Restore` after `Terminate` are the behavior of the former method. A cluster that
a `SharedCluster` lends to tenants still refuses the call with
`ceph.ErrSharedCluster`; `OperationError.Op` now reads `override.SetFullRatios`.

What differs is the same as for the blocklist: the handle is never nil, a refused
or never-applied call returns a handle whose `Restore` does nothing, and a nil
cluster returns a not-applied handle with `ceph.ErrInvalidRequest` instead of a
plain error. A call that fails after the override was tracked, from the first
native step on, still returns the real handle with the error. A check such as
`change == nil` or `overlap != nil` that detected a refusal must test
`err != nil` instead.

```golang
// before
change, err := cluster.OverrideFullRatios(ctx, ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .0003})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}

// after
change, err := override.SetFullRatios(ctx, cluster, ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .0003})
defer change.Restore(cleanupCtx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

The same change applies to the OSD flag:

```golang
// before
hold, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)
if hold != nil {
	defer hold.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}

// after
hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
defer hold.Restore(cleanupCtx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

## Configuration and messenger policy

`Container.OverrideConfig` became `override.SetConfig` and
`Container.OverrideMessengerPolicy` became `override.SetMessengerPolicy`; the
handles became `override.ConfigHandle` and `override.MessengerPolicyHandle`. The
verb is `Set` because each call replaces a value that may already exist and
`Restore` puts the previous entry back (or removes an entry that was absent).
The refusal of an option that the `MessengerV2Secure` or `MessengerV2Only`
bootstrap fixes (checked before the topology lock, so it does not wait for it),
the normalized readback, the refusal to adopt an unknown value after a lost
readback, the overlap refusal, the lost-reply convergence of apply and restore,
the refusal of a restore after `Terminate`, and the reverse restore order of the
policy options are the behavior of the former methods. A cluster that a
`SharedCluster` lends to tenants still refuses the call with
`ceph.ErrSharedCluster`; `OperationError.Op` now reads `override.SetConfig` or
`override.SetMessengerPolicy`. The messenger policy is admitted once, under its
own name; its options are set without a second admission.

What differs is the same as for the other kinds: the handles are never nil, a
refused or never-applied call returns a handle whose `Restore` does nothing, and
a nil cluster returns a not-applied handle with `ceph.ErrInvalidRequest` (it was
a plain error for the configuration and a plain error for the policy). A nil
cluster is now reported before the context and the input, so a canceled context
with a nil cluster returns `ErrInvalidRequest`, not the context error. A policy
whose later option is refused returns the handle of the options made so far,
without the refused one, together with the error; a first option that is refused
returns a not-applied handle. A check such as `change == nil` that detected a
refusal must test `err != nil` instead.

```golang
// before
change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "1"})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}

// after
change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "1"})
defer change.Restore(cleanupCtx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

See [override](../override.md) for the full contract.
