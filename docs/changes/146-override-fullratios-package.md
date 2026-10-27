---
change: 146-override-fullratios-package
kind: api-break
migration: docs/guides/upgrading-override-package.md
---
Move the full ratios override out of `ceph` into the package `override`.
`Container.OverrideFullRatios` becomes
`override.SetFullRatios(ctx, cluster, ratios)` and `ceph.FullRatiosOverride`
becomes `override.FullRatiosHandle`, with the same `Restore`. Admission,
locking, the deadline, the ordered intermediate steps, the captured control and
bootstrap identity, error texts and the reconcile rules are unchanged; there is
no deprecated alias or forwarding layer. The returned handle is now never nil:
a refused or never-applied call returns a handle whose `Restore` does nothing,
and a nil cluster is refused with `ErrInvalidRequest`.
