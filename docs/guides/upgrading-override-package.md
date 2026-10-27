# Upgrading to the override package

0.x: old names are removed without aliases. The client blocklist override
and the global OSD flag override moved out of `ceph` into the new package
`override`. There is no deprecated
alias, wrapper or forwarder in `ceph`, and `Tenant` never had one. Published
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

`ceph.BlocklistEntry` and `Observer.BlocklistEntries` are unchanged.

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

See [override](../override.md) for the full contract.
