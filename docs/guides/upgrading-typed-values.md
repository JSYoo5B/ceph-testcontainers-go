# Upgrading to typed OSD flags, PG IDs and a named connection config

This source break targets the next minor release. Published tags remain
immutable; main provides no deprecated phase and no compatibility alias.

Three public shapes that hid what a value is now have a type:

- A global OSD flag is a `ceph.OSDFlag` instead of a `string`.
- A placement group ID is a `ceph.PGID` instead of a `string`.
- The pair of byte slices from `ConnectionConfig` is one `ceph.ConnectionConfig`
  with the named fields `Conf` and `Keyring`.

There are no runtime behavior changes. Validation, error messages, override
restoration and copy semantics are unchanged.

## Migration table

| Previous call or type | Current call or type |
| --- | --- |
| `c.OverrideOSDFlag(ctx, flag string, enabled)` | `c.OverrideOSDFlag(ctx, flag ceph.OSDFlag, enabled)` |
| `c.OverrideOSDFlag(ctx, "noout", true)` | `c.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)`; the string literal still compiles |
| `flags []string` from `c.OSDFlags(ctx)` | `flags []ceph.OSDFlag` |
| `flags []string` from `tenant.OSDFlags(ctx)` | `flags []ceph.OSDFlag` |
| `c.DeepScrubPG(ctx, pgid string)` | `c.DeepScrubPG(ctx, pgid ceph.PGID)` |
| `c.RepairPG(ctx, pgid string)` | `c.RepairPG(ctx, pgid ceph.PGID)` |
| `c.PGInconsistencies(ctx, pgid string)` | `c.PGInconsistencies(ctx, pgid ceph.PGID)` |
| `ceph.PGState.PGID string` | `ceph.PGState.PGID ceph.PGID` |
| `conf, keyring, err := c.ConnectionConfig(ctx)` | `cfg, err := c.ConnectionConfig(ctx)`, then `cfg.Conf` and `cfg.Keyring` |
| `conf, keyring, err := client.ConnectionConfig()` | `cfg, err := client.ConnectionConfig()`, then `cfg.Conf` and `cfg.Keyring` |
| `_, keyring, err := c.ConnectionConfig(ctx)` | `cfg, err := c.ConnectionConfig(ctx)`, then `cfg.Keyring` |

The flag names map to constants:

| Flag | Constant |
| --- | --- |
| `noup` | `ceph.OSDFlagNoUp` |
| `nodown` | `ceph.OSDFlagNoDown` |
| `noout` | `ceph.OSDFlagNoOut` |
| `noin` | `ceph.OSDFlagNoIn` |
| `nobackfill` | `ceph.OSDFlagNoBackfill` |
| `norebalance` | `ceph.OSDFlagNoRebalance` |
| `norecover` | `ceph.OSDFlagNoRecover` |
| `noscrub` | `ceph.OSDFlagNoScrub` |
| `nodeep-scrub` | `ceph.OSDFlagNoDeepScrub` |
| `nosnaptrim` | `ceph.OSDFlagNoSnapTrim` |
| `noautoscale` | `ceph.OSDFlagNoAutoscale` |

`ceph.ParsePGID(s)` is new. It checks the form `<pool>.<hex>` and accepts and
rejects exactly what `DeepScrubPG`, `RepairPG` and `PGInconsistencies` accept.

`OverrideOSDFlag` named above later became `override.SetOSDFlag`, which takes the
cluster as its second argument (see
[the override package guide](upgrading-override-package.md)).

## What does not change

- An untyped string constant converts to a defined string type, so
  `c.OverrideOSDFlag(ctx, "noout", true)` and `c.DeepScrubPG(ctx, "1.0")` keep
  compiling. Only values held in a `string` variable stop compiling and need a
  conversion such as `ceph.OSDFlag(name)`, or `ceph.ParsePGID(s)` for a PG ID
  that comes from outside.
- The types add discoverability and stop unrelated strings from being mixed up.
  They do not make an invalid value unrepresentable. `OverrideOSDFlag` (now `override.SetOSDFlag`) still
  refuses any value that is not one of the eleven constants, and the three PG
  methods still refuse an ID that does not match `<pool>.<hex>`, before any
  native command runs.
- `OSDFlags` still reports every global flag that the cluster reports, including
  native compatibility flags such as `sortbitwise` that have no constant. That
  is why `OSDFlag` is an open string type.
- `DebugLevelSetting.Daemon` and `ConfigSetting.Section` stay strings.
- Both `ConnectionConfig` methods still return independent copies of the bytes.
  `ClientConfig.ConnectionConfig` is still a pure accessor without `ctx`, and a
  stale descriptor still returns its old credentials.

## Behavior changes

- On error, `ConnectionConfig` returns the zero `ceph.ConnectionConfig`, whose
  `Conf` and `Keyring` are nil. The old signature returned two nil slices.
- Code that assigns the result of `OSDFlags` to a `[]string` must convert each
  element, for example with `string(flag)` or `flag.String()`.
- Code that compares `PGState.PGID` with a `string` must convert one side, for
  example with `pg.PGID.String()`.
- `ceph.ConnectionConfig` holds a credential and is not redacted. Do not log it
  with `%v` or `%+v`.

## Example

```go
cfg, err := cluster.ConnectionConfig(ctx)
if err != nil {
    return err
}
hold, err := cluster.OverrideOSDFlag(ctx, ceph.OSDFlagNoOut, true)
if hold != nil {
    defer hold.Restore(context.WithoutCancel(ctx))
}
if err != nil {
    return err
}
snapshot, err := pool.PGs(ctx)
if err != nil {
    return err
}
// A reported PG passes straight to the scrub methods.
err = cluster.DeepScrubPG(ctx, snapshot.PGs[0].PGID)
_ = cfg
```
