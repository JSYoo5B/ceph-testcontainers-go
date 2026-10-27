---
change: 146-override-config-package
kind: api-break
migration: docs/guides/upgrading-override-package.md
---
Move the central configuration override and the messenger policy override out
of `ceph` into the package `override`. `Container.OverrideConfig` becomes
`override.SetConfig(ctx, cluster, setting)` and
`Container.OverrideMessengerPolicy` becomes
`override.SetMessengerPolicy(ctx, cluster, policy)`; `ceph.ConfigOverrideHandle`
becomes `override.ConfigHandle` and `ceph.MessengerPolicyOverrideHandle` becomes
`override.MessengerPolicyHandle`, with the same `Restore`, `String` and
`GoString`. The refusals of the options that a messenger bootstrap mode fixes,
the normalized readback, the lost-reply convergence, the overlap rule, the
refusal of a restore after `Terminate` and the reverse restore order of the
policy options are unchanged; there is no deprecated alias or forwarding layer.
The returned handles are now never nil: a refused or never-applied call returns
a handle whose `Restore` does nothing, and a nil cluster is refused with
`ErrInvalidRequest`. The messenger policy is admitted once, under its own name.
