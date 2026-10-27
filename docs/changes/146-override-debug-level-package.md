---
change: 146-override-debug-level-package
kind: api-break
migration: docs/guides/upgrading-override-debug-level.md
---
Move the runtime debug level override out of `ceph` into the package `override`.
`Container.OverrideDebugLevel` becomes
`override.SetDebugLevel(ctx, cluster, setting)` and
`ceph.DebugLevelOverrideHandle` becomes `override.DebugLevelHandle`, with the
same `Receipt` and `Restore`. The generation fence, the settling of unconfirmed
sets, the one-window-per-option registry, the native fence and the error texts
are unchanged; there is no deprecated alias or forwarding layer. The returned
handle is now never nil: a refused or never-applied call returns a handle whose
`Restore` does nothing and whose receipt has the state `not-applied` with the
refusal as `Reason`, and a nil cluster is refused with `ErrInvalidRequest`.
`Finalization.TrackDebugLevel` accepts the new handle through
`ceph.DebugLevelReceipter`.
