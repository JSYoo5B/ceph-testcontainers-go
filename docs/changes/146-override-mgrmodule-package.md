---
change: 146-override-mgrmodule-package
kind: api-break
migration: docs/guides/upgrading-override-mgr-module.md
---
Move the MGR module override out of `ceph` into the package `override`.
`Container.OverrideMGRModule` becomes
`override.SetMGRModule(ctx, cluster, name, enabled)` and
`ceph.MGRModuleOverrideHandle` becomes `override.MGRModuleHandle`, with the same
`Restore`. Admission, locking, error texts, the protection of `volumes` and
`mirroring`, the always-on rules and the retry rules are unchanged, and the call
still verifies membership, not command readiness; there is no deprecated alias or
forwarding layer. The returned handle is now never nil: a refused or
never-applied call returns a handle whose `Restore` does nothing, and a nil
cluster is refused with `ErrInvalidRequest` instead of panicking.
