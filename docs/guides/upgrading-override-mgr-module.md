# Upgrading the MGR module override

0.x: old names are removed without aliases. The MGR module override moved out of
`ceph` into the package `override`. There is no deprecated alias, wrapper or
forwarder in `ceph`, and `Tenant` never had one. Published releases keep their
original API. Update the calls, the type names and the `import` line together.

```golang
import "github.com/JSYoo5B/ceph-testcontainers-go/override"
```

| Previous call or type | New call or type |
| --- | --- |
| `cluster.OverrideMGRModule(ctx, name, enabled)` | `override.SetMGRModule(ctx, cluster, name, enabled)` |
| `*ceph.MGRModuleOverrideHandle` | `*override.MGRModuleHandle` |
| `(*ceph.MGRModuleOverrideHandle).Restore` | `(*override.MGRModuleHandle).Restore` (same restoration rules) |

`ceph.MGRModuleState`, `Observer.MGRModules` and `Container.WaitMGRModuleReady`
are unchanged.

## What is the same

Admission order, the module name check, the refusal of missing, cannot-run and
always-on modules, the protection of `volumes` and `mirroring` before a disable,
the overlap refusal, the no-op lease when the module is already in the requested
state, the bounded fresh readback after a change, the retry after a lost reply
and the refusal of a `Restore` after `Terminate` are the behavior of the former
method. A cluster that a `SharedCluster` lends to tenants still refuses the call
with `ceph.ErrSharedCluster`; `OperationError.Op` now reads
`override.SetMGRModule` instead of `OverrideMGRModule`. The call still verifies
membership only, not command readiness.

## What differs

- The returned handle is never nil. A call that was refused or never applied
  returns a handle whose `Restore` does nothing: it returns nil, takes no lock
  and never reports a restoration. The reason is the returned error. Remove the
  `if module != nil` guard around a deferred or registered `Restore`, and test
  `err` for the outcome. A check such as `module == nil` that detected a refusal
  must test `err != nil` instead.
- A call whose outcome is uncertain (the change may have been applied) still
  returns a real handle together with the error.
- A nil cluster no longer panics. It returns a not-applied handle and an error
  classified `ceph.ErrInvalidRequest` with `Op` `override.SetMGRModule`, and it
  is reported before an invalid module name.

```golang
// before
module, err := cluster.OverrideMGRModule(ctx, "prometheus", true)
if module != nil {
	defer module.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}

// after
module, err := override.SetMGRModule(ctx, cluster, "prometheus", true)
defer module.Restore(cleanupCtx) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

See [override](../override.md) for the full contract.
