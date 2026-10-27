---
change: 146-override-blocklist-package
kind: api-break
migration: docs/guides/upgrading-override-package.md
---
Move the client blocklist override out of `ceph` into the new package
`override`. `Container.TemporaryBlocklist` becomes
`override.AddBlocklist(ctx, cluster, address, duration)` and
`ceph.BlocklistOverride` becomes `override.BlocklistHandle`, with the same
`Address` and `Restore`. Admission, locking, error texts, the expiration check
and the retry rules are unchanged; there is no deprecated alias or forwarding
layer. The returned handle is now never nil: a refused or never-applied call
returns a handle whose `Restore` does nothing, and a nil cluster is refused
with `ErrInvalidRequest` instead of panicking.
