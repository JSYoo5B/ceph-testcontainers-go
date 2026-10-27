---
change: 146-override-osdflag-package
kind: api-break
migration: docs/guides/upgrading-override-package.md
---
Move the global OSD flag override out of `ceph` into the package `override`.
`Container.OverrideOSDFlag` becomes
`override.SetOSDFlag(ctx, cluster, flag, enabled)` and
`ceph.OSDFlagOverrideHandle` becomes `override.OSDFlagHandle`, with the same
`Restore`. The `ceph.OSDFlag` type and its constants stay in `ceph`. Admission,
locking, the supported-flag list, the overlap refusal, the refusal of a restore
over an outside change and the error texts are unchanged; there is no
deprecated alias or forwarding layer. The returned handle is now never nil: a
refused or never-applied call returns a handle whose `Restore` does nothing, and
a nil cluster is refused with `ErrInvalidRequest` instead of panicking.
