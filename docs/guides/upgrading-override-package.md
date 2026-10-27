# Upgrading to the override package

0.x: old names are removed without aliases. The client blocklist override
moved out of `ceph` into the new package `override`, followed by the full
ratios override. There is no deprecated alias, wrapper or forwarder in `ceph`,
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

`ceph.BlocklistEntry`, `Observer.BlocklistEntries`, `ceph.FullRatios`,
`ceph.FullRatioSnapshot` and `Observer.FullRatios` are unchanged.

## What is the same

Admission order, the context and lock behavior, the error texts and classes, the
duration and address checks, the refusal of an existing entry, the overlap
refusal, the expiration check at `Restore`, the retry after a lost reply and the
refusal of a `Restore` after `Terminate` are the behavior of the former method.
A cluster that a `SharedCluster` lends to tenants still refuses the call with
`ceph.ErrSharedCluster`; `OperationError.Op` now reads `override.AddBlocklist`
instead of `TemporaryBlocklist`.

## What differs

- The returned handle is never nil. A call that was refused or never applied
  returns a handle whose `Restore` does nothing: it returns nil, takes no lock
  and never reports a restoration. The reason is the returned error. Remove the
  `if fence != nil` guard around a deferred or registered `Restore`, and test
  `err` for the outcome. A check such as `fence == nil` that detected a refusal
  must test `err != nil` instead.
- A call whose outcome is uncertain (the add may have been applied) still
  returns a real handle together with the error.
- A nil cluster no longer panics. It returns a not-applied handle and an error
  classified `ceph.ErrInvalidRequest` with `Op` `override.AddBlocklist`.

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

See [override](../override.md) for the full contract.
