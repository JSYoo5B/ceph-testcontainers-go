# Value configuration composition

Change `312-add-osd-options`, issue #312. This record states the composition
rules of the operation-specific value configurations that replace overload
families. The first user is `ceph.OSDOptions` for `Container.AddOSD`. Later
candidates (`Run` with image selection, tenant client attachment, the mirror
`Run` functions) are meant to reuse these rules; none of them is implemented by
this change. The rules are a proposal of the implementation side for review:
the issue delegated the concrete names and signatures, and nothing here records
an owner approval.

## Shape

- A configuration is a plain struct value with unexported fields and chainable
  `WithX` methods. Each method has a value receiver and returns a modified copy.
  There is no pointer builder, no shared mutable state and no universal options
  type: each operation domain has its own type (`OSDOptions` is not a Run
  option).
- The entry point takes one trailing typed variadic, `configs ...OSDOptions`.
  Go allows only one trailing variadic, so inputs that used to arrive as a
  second variadic (`...DaemonOption`) are reconciled into the configuration type
  (`OSDOptions.WithImage`) instead of an untyped `...any` bag.
- A `WithX` method exists only when it sets a field that callers really vary.
  No method is added for visual uniformity.

## Composition rules

1. **Unspecified is not empty.** Every field records whether it was set. The
   zero value sets nothing. An explicit empty setting (`WithPlacement(OSDConfig{})`,
   `WithImage("")`) is a setting: the first is an explicit request for the
   defaults, the second is an invalid selection that is refused, never read as
   "no override".
2. **Order and scope.** `AddOSD` merges its values in argument order. A value
   that leaves a field unset never erases what another value set.
3. **One owner per field across values.** A field set by two values is refused
   with `ErrInvalidRequest`, also when both agree. There is no
   last-value-wins across values, so argument order never silently decides a
   conflict, and moving a value cannot change the effective configuration.
4. **Replacement within one value.** Calling the same `WithX` twice on one
   value replaces the earlier setting completely, so a derived value can
   override its base (`base.WithImage(a)` then `.WithImage(b)`). This is the one
   place where a later call wins.
5. **Validate the assembled configuration once.** The effective configuration
   (merged fields, cluster defaults resolved, placement normalized, image
   validated) is assembled and validated in one place before any external
   effect: before the topology lock, any registration and any container. A
   refusal changes no state and does not wait for a lock.
6. **Ordering of checks is part of the contract and is unchanged:** the
   shared-cluster gate, then the context, then configuration validation, then
   the topology lock and the existing registration and cleanup path. Existing
   cancellation and error categories are kept.
7. **Ownership.** A value owns its data. Copies must not share mutable storage:
   a configuration that holds a slice or map clones it with `slices.Clone` or
   `maps.Clone`, never with `append([]T(nil), v...)`, because the latter turns a
   non-nil empty slice into nil and so erases the explicit-empty distinction that
   validators such as the messenger mode list rely on. Live handles that the
   operation is meant to reference (for example the `*ClientConfig` of a tenant
   attachment, whose identity the ownership check compares by pointer) keep their
   identity and are never cloned. `OSDOptions` holds only strings, so a struct
   copy is already independent.
8. **No second option surface for the dependency.** The grouped value is the
   primary API. The testcontainers escape stays narrow and is documented per
   entry point. Module settings that are `ceph.Option` values are applied to the
   settings and validated, they are not forwarded to a container request (their
   `Customize` does nothing on purpose), so a later Run migration must map those
   inputs into its one effective configuration explicitly.

## AddOSD

| Old | New |
| --- | --- |
| `AddOSD(ctx, ...DaemonOption)` | `AddOSD(ctx, ...OSDOptions)` |
| `AddOSDWithConfig(ctx, cfg, ...DaemonOption)` | `AddOSD(ctx, OSDOptions{}.WithPlacement(cfg))` |
| `AddOSD(ctx, WithDaemonImage(img))` | `AddOSD(ctx, OSDOptions{}.WithImage(img))` |

- Placement reuses `OSDConfig`. `WithPlacement` takes the four fields as one
  unit, so an explicit placement is never mixed field by field with another
  value's placement (rule 3).
- The image check is the same function as `WithDaemonImage` (`validateDaemonImage`),
  so MON, MGR and OSD images are validated alike. `WithDaemonImage` remains the
  option of `AddMonitor` and `AddManager`, whose entry points are not changed by
  this slice.
- `AddOSDWithConfig` is removed without an alias (0.x policy). The shared-gate
  table loses its row; `AddOSD` keeps the topology-change policy.
- Hosts, racks, device class and root are still validated and resolved by the
  existing `resolveOSDDefaults` and `normalizeOSDConfig`; registration, key
  generation, the publish-before-start ownership record and cleanup are
  unchanged.

## Not decided here

- How `Run` maps `ceph.Option` values and raw customizers into one effective
  configuration, and how image selection (single image or `ImageSet`) joins it.
- How tenant attachment keeps the `*ClientConfig` identity while cloning its
  owned collections.
- How the mirror `Run` functions keep source/destination role inference.
