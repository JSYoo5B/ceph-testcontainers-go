# Upgrading the debug level override

0.x: old names are removed without aliases. The runtime debug level override
moved out of `ceph` into the package `override`. There is no deprecated alias,
wrapper or forwarder in `ceph`. Published releases
keep their original API. Update the calls, the type names and the `import` line
together.

```golang
import "github.com/JSYoo5B/ceph-testcontainers-go/override"
```

| Previous call or type | New call or type |
| --- | --- |
| `cluster.OverrideDebugLevel(ctx, setting)` | `override.SetDebugLevel(ctx, cluster, setting)` |
| `*ceph.DebugLevelOverrideHandle` | `*override.DebugLevelHandle` |
| `(*ceph.DebugLevelOverrideHandle).Receipt` | `(*override.DebugLevelHandle).Receipt` (same receipt) |
| `(*ceph.DebugLevelOverrideHandle).Restore` | `(*override.DebugLevelHandle).Restore` (same restoration rules) |
| `ceph.DebugLevelOverrideHandle.String`, `GoString` | `override.DebugLevelHandle.String`, `GoString` |

`ceph.DebugLevelSetting`, `ceph.DebugLevelReceipt`, `ceph.DebugLevelState`,
`ceph.DebugLevelTransition` and `ceph.DebugLevelReceipter` are unchanged.
`Finalization.TrackDebugLevel` accepts the new handle through
`ceph.DebugLevelReceipter`.

## What is the same

The generation fence, the settling of an unconfirmed set before a restoration,
the registry that admits one window per daemon and option (with a lock of its
own), the native fence that `OSDContainer.Remove` and `OSDContainer.Detach` drain,
the context and timeout behavior, the error texts and classes, and the states
and transitions of a window are the behavior of the former method. `OperationError.Op` now reads `override.SetDebugLevel`
instead of `OverrideDebugLevel`.

## What differs

- The returned handle is never nil. A call that was refused or never applied
  returns a handle whose `Restore` does nothing: it returns nil, takes no lock
  and never reports a restoration, and whose `Receipt` has the state
  `ceph.DebugLevelNotApplied` with the refusal as `Reason`. Remove the
  `if window != nil` guard around `Finalization.TrackDebugLevel` and a
  registered `Restore`, and test `err` for the outcome. A check such as
  `window == nil` that detected a refusal must test `err != nil` or the receipt
  state instead.
- A call whose outcome is uncertain (the set may have been applied) still
  returns a real handle together with the error; its receipt never reports
  `not-applied`.
- A nil cluster returns a not-applied handle and an error classified
  `ceph.ErrInvalidRequest` with `Op` `override.SetDebugLevel`. It was an
  unclassified error before.
- A nil cluster with otherwise invalid input reports the nil cluster first.

```golang
// before
window, err := cluster.OverrideDebugLevel(ctx, setting)
if window != nil {
	finalization.TrackDebugLevel(window)
	finalization.Cleanup("restore", window.Restore)
}
if err != nil {
	t.Fatal(err)
}

// after
window, err := override.SetDebugLevel(ctx, cluster, setting)
finalization.TrackDebugLevel(window) // recorded as not-applied after a refusal
finalization.Cleanup("restore", window.Restore) // a no-op after a refusal
if err != nil {
	t.Fatal(err)
}
```

See [override](../override.md) for the full contract.
