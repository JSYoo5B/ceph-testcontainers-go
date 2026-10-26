# Design: public package for the reversible override operations

Design note for the `ceph.Container` size reduction (Refs #146). It records what
the owner decision of 2026-10-27 on override entry points requires before any
exported move: the exact package, type and function map, the before and after
import graph, and the dependency analysis. It changes no product code, no API
inventory and no caller. Every public name below is a placeholder unless it says
otherwise, and the section "Not decided" lists what stays open.

Inventory source: `main` at the merge of #209 (`9d2fabb`). Counts come from
reading the code and from textual search by method name. They are not
acceptance targets, and a textual count overstates call sites of one receiver
type. Nothing was run natively: this note is source reading only.

## The decision being designed

The owner decision records these constraints, which this note treats as fixed:

- Prefer a public subpackage for the override operations, with package-level
  entry points that take the cluster, for example `override.Config(ctx, cluster,
  setting)` returning a change with `Restore(ctx)`. A package-level function in
  `ceph` is the fallback only if an import cycle is structurally unavoidable, and
  then the concrete cycle must be documented. Moving only the implementation to
  `internal/override` does not satisfy the preference.
- Prefer a one-way dependency from the override package toward `ceph`, through
  narrow existing or internal contracts. Do not expose locks, credentials or an
  unrestricted executor, do not duplicate lifecycle or admission state, and do
  not create a broad abstraction to force the split. Difficulty reaching private
  state is not proof of a cycle.
- The existing individual change handles keep their state and Restore or
  settlement responsibilities. Per-operation shared-cluster admission,
  context and error behavior, lock ordering, restoration lifetimes and
  uncertain-outcome ownership are preserved through either layout.
- Facts (`ceph.FactsOf`-style snapshot) and live observations (`ceph.Observe`-style
  object) are separate groups with their own decisions. This note does not move
  them.

## 1. Current declarations and callers

### Override entry points on `Container`

All seven are `sharedExclusive` in `sharedMethodPolicies`. Each begins with
`c.sharedGate(<method name>)`. `Tenant` has no override forwarder: its only
override-adjacent method is `BlocklistEntries`, which is an observation and
belongs to the observation group.

| Method (file) | Returns | Takes | Private state used |
| --- | --- | --- | --- |
| `OverrideConfig` (`config.go`) | `*ConfigOverrideHandle` | `ConfigSetting` | `configOverrides` map, `mu` via `lockTopology`, `poolPolicyReady`, `settings.messengerMode` and `settings.startupTimeout`, central config read and `config set/rm` through `cephCLI` |
| `OverrideMessengerPolicy` (`messenger_policy.go`) | `*MessengerPolicyOverrideHandle` | `MessengerPolicy` | `settings.messengerMode`; no lock of its own; calls `OverrideConfig` once per option |
| `OverrideOSDFlag` (`osd_policy.go`) | `*OSDFlagOverrideHandle` | `OSDFlag`, `bool` | `flagOverrides` map, `mu`, `poolPolicyReady`, `waitPolicyReady`, `osds`, `osdPolicyDump`, `setOSDFlag`, `cephCLI` |
| `OverrideMGRModule` (`mgr_modules.go`) | `*MGRModuleOverrideHandle` | module name, `bool` | `moduleOverrides` map, `mu`, `poolPolicyReady`, `waitPolicyReady`, `poll`, MGR module reads and `mgr module enable/disable` through `cephCLI` |
| `TemporaryBlocklist` (`fencing.go`) | `*BlocklistOverride` | address, duration | `mu`, `poolPolicyReady`, fault registry (`reserveFault`, `settleFault`, `tracksFault`, `releaseFault`, kind `faultBlocklist`, phase tracked), `blocklistEntries`, `cephCLI` |
| `OverrideFullRatios` (`full_ratios.go`) | `*FullRatiosOverride` | `FullRatios` | `mu` via `lockTopology`, fault registry (kind `faultFullRatios`), `fullRatiosSession` (captured control handle plus `configMu`, `config`, `keyring`, `closed`) |
| `OverrideDebugLevel` (`debug_level.go`) | `*DebugLevelOverrideHandle` | `DebugLevelSetting` | `debugLevels` registry (own lock), `mu`, `closed`, `osds` and OSD descriptor flags, `ownsDaemon`, `run`, `nativeMu`, `nativeInvocations`, `nativeClosed`, `nativeFenced`, `controlContainerContext`, `adminSocketCommand`, `supervised` |

Related exported declarations that travel with the group or constrain it:

| Declaration | Where | Note |
| --- | --- | --- |
| Handle types: `ConfigOverrideHandle`, `MessengerPolicyOverrideHandle`, `OSDFlagOverrideHandle`, `MGRModuleOverrideHandle`, `BlocklistOverride`, `FullRatiosOverride`, `DebugLevelOverrideHandle` | one per file above | 7 types. Each unexported `*State` struct holds the restoration state; copies share it. |
| `Restore(ctx)` on each handle | same | 7 methods. None is classified in `sharedMethodPolicies` or `sharedHandlePolicies`: Restore has no shared-cluster gate today and refuses a terminated cluster through `poolPolicyReady` (see section 4). The move keeps that. |
| `BlocklistOverride.Address`, `DebugLevelOverrideHandle.Receipt`, `ConfigOverrideHandle.String`/`GoString`, `DebugLevelOverrideHandle.String`/`GoString` | `fencing.go`, `debug_level.go`, `config.go` | Read-only handle accessors. |
| Value types in the signatures: `ConfigSetting`, `MessengerPolicy` and `MessengerConnectionMode`, `OSDFlag` and its constants, `FullRatios`, `DebugLevelSetting`, `DebugLevelState`, `DebugLevelReceipt`, `DebugLevelTransition` | same files | Several are shared with other groups, see section 2. |
| `Finalization.TrackDebugLevel(*DebugLevelOverrideHandle)` | `finalization_debug.go` | The one public declaration outside the seven files that names a handle type. |

### Callers

Counted by method name over Go files, then by file.

| Method | Go files | Call lines | Markdown files naming it |
| --- | --- | --- | --- |
| `OverrideConfig` | 41 | 63 | 15 |
| `OverrideOSDFlag` | 32 | 45 | 13 |
| `OverrideMGRModule` | 4 | 26 | 9 |
| `OverrideFullRatios` | 7 | 22 | 11 |
| `TemporaryBlocklist` | 4 | 16 | 11 |
| `OverrideDebugLevel` | 4 | 9 | 5 |
| `OverrideMessengerPolicy` | 3 | 7 | 8 |

Distinct Go files that call at least one of the seven: 76 in total, 14 test
files and one non-test file in `ceph`, 60 test files in `internal/integration`,
and one test file in `internal/debugoverhead`. The only non-test caller is
`OverrideMessengerPolicy` calling `OverrideConfig` in `messenger_policy.go`.
`rbd`, `cephfs`, `rgw`, `fault` and `internal/multicluster` do not call any of the
seven. Their own `Override*`-named handle types (for example the CephFS session
timeout and pin overrides) are different operations and are not part of this
group.

Eighteen Markdown files name an override method or handle type: `CHANGELOG.md`,
the package pages `docs/ceph.md`, `docs/cephfs.md`, `docs/fault.md`,
`docs/rbd.md` and `docs/rgw.md`, seven guides (`contexts-and-cleanup`, `errors`,
`fault-injection`, `messenger-client-fixtures`, `native-commands`,
`upgrading-ceph-policy-names`, `upgrading-typed-values`), one research page
(`osd-debug-overhead`) and four design pages (`api-naming`, `api-naming-audit`,
`container-decomposition`, `fault-package`). `api-naming-inventory.json` also
names them 92 times.

### Internal consumers of override state outside the seven files

These are the places where `ceph` itself depends on override state. They decide
whether the dependency can stay one-way.

| Consumer | What it reads | File |
| --- | --- | --- |
| `Container` fields `configOverrides`, `flagOverrides`, `moduleOverrides` | Maps whose element types are the public handle types | `ceph.go` |
| `Container.debugLevels` | `debugLevelRegistry`, one open window per daemon and option | `ceph.go`, `debug_level.go` |
| `deletePool` | `configOverrides[mon_allow_pool_delete]` and the handle's `state.restored`, to refuse `Pool.Remove` while a caller holds an override of that key | `pool_removal.go` |
| `Finalization.TrackDebugLevel` and its manifest copy | `*DebugLevelOverrideHandle`, its `state` and `Receipt()` | `finalization_debug.go` |
| `faults` registry, `osd_retained.go`, `fault_hooks.go` | `faultBlocklist` and `faultFullRatios` entries, for overlap and identity; `terminate` never visits them | `fault_registry.go`, `osd_retained.go` |
| `sharedMethodPolicies`, `TestSharedGateClassifiesEveryMethod` | One policy entry per exported `Container` method | `shared_gate.go`, `shared_gate_test.go` |

## 2. Proposed package, type and function map (placeholders)

Package `override` at `github.com/JSYoo5B/ceph-testcontainers-go/override`. It
imports `ceph`; `ceph` never imports it. Every function takes `*ceph.Container`
and nothing else for the cluster, so `Tenant` has no entry point, as today.

| Today | Placeholder in `override` | Handle (placeholder) |
| --- | --- | --- |
| `Container.OverrideConfig` | `Config(ctx, c, ceph.ConfigSetting)` | `ConfigChange` |
| `Container.OverrideMessengerPolicy` | `MessengerPolicy(ctx, c, ceph.MessengerPolicy)` | `MessengerPolicyChange` |
| `Container.OverrideOSDFlag` | `OSDFlag(ctx, c, ceph.OSDFlag, enabled)` | `OSDFlagChange` |
| `Container.OverrideMGRModule` | `MGRModule(ctx, c, name, enabled)` | `MGRModuleChange` |
| `Container.TemporaryBlocklist` | `Blocklist(ctx, c, address, duration)` | `BlocklistChange` |
| `Container.OverrideFullRatios` | `FullRatios(ctx, c, ceph.FullRatios)` | `FullRatiosChange` |
| `Container.OverrideDebugLevel` | `DebugLevel(ctx, c, ceph.DebugLevelSetting)` | `DebugLevelChange` |

The map is one function and one handle per family: no override factory object, no
shared interface over the handles, no generic "Change" abstraction. Whether the
names drop the `Override` and `Temporary` prefixes, and whether the handles keep
their current type names, is not decided (see "Not decided").

### Where each type would live

| Type group | Stays in `ceph` | Reason |
| --- | --- | --- |
| Setting and result value types shared with observations: `ConfigEntry`, `MGRModuleState`, `OSDFlag` and constants, `FullRatioSnapshot`, `BlocklistEntry` | Yes | Returned by `Configuration`, `MGRModules`, `OSDFlags`, `FullRatios`, `BlocklistEntries`. The observation decision places those, and an override reads the same values back. |
| Setting inputs used by non-override API: `MessengerPolicy` and `MessengerConnectionMode` (`WithMessengerPolicy` option, `ceph.go` validation), `ConfigSetting` (shared with `ConfigEntry` decoding), `FullRatios` | Yes | `ceph` options and bootstrap use them. Moving them forces `ceph` to import `override`. |
| `DebugLevelSetting`, `DebugLevelState`, `DebugLevelReceipt`, `DebugLevelTransition` | Leaning yes | `Finalization` copies the receipt into its failure manifest. |
| The seven handle types and their state structs | Two options, see below | Only `Finalization.TrackDebugLevel` and the registries in `Container` name them. |

Handle placement has two options. Neither is chosen here.

| | A. Handles move to `override` | B. Handles stay in `ceph`, only entry points move |
| --- | --- | --- |
| Public surface | Handle types and `Restore` leave `ceph`, so `ceph` loses 7 types and 7 methods along with the 7 entry points. | `ceph` keeps 7 handle types. The entry points move, but the handles are constructed by a `ceph` function that `override` can call, so that function must be reachable (exported, which defeats the purpose, or through an internal hook). |
| State and Restore | Live in `override`, with the cluster-side registries reduced to opaque tracking entries (below). | Live in `ceph`, unchanged. |
| Cycle exposure | Requires removing the handle types from `Finalization.TrackDebugLevel`, `configOverrides` and the others (section 3). | None, because nothing new points at `override`. |
| Meets the preference | Yes: entry points and handles are grouped by package. | Partly: the grouping preference concerns the entry points, but "moving only implementation" is not satisfied if the handles remain the real public objects and `override` is a veneer over hidden `ceph` functions. |
| Type aliases in `ceph` | Not used. The v0.x policy is direct migration without deprecated aliases. | Not applicable. |

The analysis below assumes A, because B leaves the public types where they are and
makes the dependency question trivial. If A is infeasible for one family, a
mixed layout (that family stays as a documented exception) is a possibility, not a
recommendation.

## 3. Import graph and dependency analysis

### Before

```text
rbd, cephfs, rgw ---------------> ceph ---> internal/hookreg
fault -----------------------------^  ^
  |                                   |
  +--> internal/faulthook ------------+   (typed view of what ceph registers)
rbd, cephfs, rgw ---> internal/servicehook --> ceph
internal/integration, internal/debugoverhead (tests) ---> ceph
```

`ceph` imports no public service package in non-test code (its only appearances
of `rbd`, `cephfs` and `rgw` are comments and tests). The override entry points
are `ceph.Container` methods.

### After (proposed)

```text
override ---------------------> ceph ---> internal/hookreg
   |                             ^          (one more registered slot)
   +--> internal/overridehook ---+          (typed view of what ceph registers; placeholder name)
rbd, cephfs, rgw, fault --------> ceph      (unchanged)
internal/integration (tests) ---> ceph, override
```

`override` imports `ceph` and `internal/overridehook`; `ceph` imports neither.
`ceph` gains one registration in `init`, as it has for the fault registry
(`hookreg.Fault`) and the service hooks (`hookreg.Value`). `fault` and
`override` do not import each other.

### Candidate cycles and how each one resolves

A cycle would need `ceph` (non-test code) to name a type or function of
`override`. The seven places in section 1 that depend on override state were
checked one by one.

| # | Edge `ceph` to override state | Structural? | Resolution under A (placeholder, not decided) |
| --- | --- | --- | --- |
| 1 | `Finalization.TrackDebugLevel(*DebugLevelOverrideHandle)` and its snapshot code read the handle's `state` and call `Receipt()` | No. It is a public signature that would name an `override` type. | Narrow it to the one behavior it uses, a `Receipt() DebugLevelReceipt` method, with the receipt value types staying in `ceph`. Absence handling needs a decision: today a nil handle or a handle with nil `state` is recorded as absent, which a typed-nil interface value would blur. |
| 2 | `configOverrides`, `flagOverrides`, `moduleOverrides` have the handle types as element types | No. They are private fields. | Replace the element type with an opaque tracking record owned by `ceph` (key, token, and the activeness bit), in the pattern of the fault registry `Reserve`/`Settle`/`Release`/`Tracks` hooks. `override` keeps the restoration state in the handle and gives `ceph` only the token. This does not duplicate lifecycle state if `ceph` stays the single registry and the handle stays the single owner of its state. |
| 3 | `deletePool` reads `configOverrides[mon_allow_pool_delete]` and `state.restored` | No. | A private `ceph` query, "is an override of this key held", answered from the `ceph`-side tracking record. The record's activeness bit is the only state both sides read, so which side writes it and under which lock is part of the open lock-order review. |
| 4 | `debugLevels` registry and `osdDebugTarget` (`nativeMu`, `nativeInvocations`, `nativeFenced`, `run`, OSD descriptor flags) | No. `openDebugLevel` already takes the registry and a `debugLevelTarget` interface (`generation`, `get`, `set` returning a settle function), so the generic window logic does not need `Container`. | The target implementation and the `nativeInvocation` fence stay in `ceph` and are reached through a hook that returns a target for a daemon. The window logic could move or stay; this is the largest and least certain family. |
| 5 | `faultBlocklist` and `faultFullRatios` registry entries | No. The registry and its hooks already exist, and `faulthook.Blocklist` and `faulthook.FullRatios` kinds already name them. | Reuse `Reserve`, `Settle`, `Release`, `Tracks`. Kinds, phases and the rule that `terminate` leaves tracked kinds alone stay in `ceph`. |
| 6 | `sharedMethodPolicies` and its reflection test | No. | The seven entries leave with the methods. The gate for package functions follows the existing `sharedFaultPolicies`/`admitFault` and `sharedHandlePolicies`/`admitHandle` pattern (a third string-keyed table beside them): a table in `ceph` for the override operations, a hook that admits by name, and a test that requires a policy for every exported function of `override` and refuses an unclassified one even on a cluster of its own. |
| 7 | `OverrideMessengerPolicy` calls `OverrideConfig` | No, both move together. | Inside `override`, one function calls the other. Admission is applied once per public call, as today. |

On this evidence no cycle is structural: every dependency from `ceph` toward the
group is either a private field that can hold a neutral record, a private query,
or one public signature (item 1) that can name a behavior instead of a type. The
fallback of package-level functions in `ceph` is therefore not established by this
analysis. This is source reading, not a compiled prototype. A compile check of the
split, as the mirror-package note did, remains a step of the implementation PR.

### Narrow contracts `override` needs from `ceph`

The contracts below are the capabilities each family consumes today, in terms of
the hook style that `internal/faulthook` already uses. Names are placeholders.
The column "exists" says whether `faulthook` already offers it.

| Contract | Consumers | Exists in `faulthook` | Must not expose |
| --- | --- | --- | --- |
| Admission by operation name, with a policy table | all 7 | `Admit(c, op)` over `sharedFaultPolicies` | Per-method bypass; an unclassified name must be refused |
| Run a function under the topology lock, with or without waiting on the context | Config, OSDFlag, MGRModule, Blocklist, FullRatios, DebugLevel | `WithTopology`, `WithTopologyNoContext` | The mutex itself. Each family keeps its current acquisition (`OverrideConfig` and `OverrideFullRatios` wait on the context, Restore and `TemporaryBlocklist` do not) |
| Refuse a terminated cluster or one without a control container | all but MessengerPolicy | `PoolPolicyReady` | Reading `closed` directly except where the code does today |
| Tracking record for an active override (reserve, settle, release, query) | Config, OSDFlag, MGRModule, Blocklist, FullRatios | `Reserve`, `Settle`, `Release`, `Tracks` for Blocklist and FullRatios; none for the three map families | A new second registry. The map families would join one registry rather than add a copy |
| Cluster settings that the code reads: messenger mode, startup timeout | Config, MessengerPolicy, Blocklist, FullRatios, DebugLevel | `MessengerMode()` is public; the startup timeout is not offered | The whole `settings` struct |
| Central config read and `config set`/`config rm` | Config | `CLI(ctx, c, args...)` (raw) | An unrestricted executor |
| OSD map read and `osd set/unset` | OSDFlag | `CLI` (raw), plus private `osdPolicyDump` | The same |
| MGR module read, `mgr module enable/disable`, readiness poll | MGRModule | `CLI` (raw), plus private `mgrModules`, `waitMGRModules` | The same |
| Blocklist read and `osd blocklist add/rm` | Blocklist | `CLI` (raw), plus private `blocklistEntries` | The same |
| Captured control handle and bootstrap identity session | FullRatios | `Control`, `ClusterIdentity` (separate calls) | A way to switch to a replacement control container; the session must stay captured once |
| Daemon target and generation, admin socket `config get/set`, native-invocation fence | DebugLevel | none | Native-invocation registration by callers |

Two findings follow.

1. The only existing way for `faulthook` to run a native command is the raw
   `CLI(ctx, c, args...)`. It is an unrestricted administrative executor behind an
   `internal/` import path. It is not public API, but reusing it unchanged for the
   override package would hand `override` the broad capability that the owner
   decision tells the design to avoid. The narrower alternative is one typed
   operation per family (for example a config database read, set and remove), which
   costs more hook methods but keeps each hook as wide as the family. Which one to
   choose is open.
2. Reads that an override performs for its readback (`Configuration`,
   `OSDFlags`, `MGRModules`, `BlocklistEntries`, `FullRatios`) are the same reads
   as the observation group. If the observation extraction introduces an internal
   query contract, the override hooks should reuse it rather than carry a second
   copy. The order in `container-decomposition.md` already puts observations
   before overrides for this reason, and #169 is the first part of that contract.

A one-way `override` to `ceph` dependency looks achievable without exposing a lock,
a credential or the raw executor, provided the hooks are typed per family. This is
a design conclusion from reading, and it needs the implementation's compile and
race review to confirm.

## 4. What each change handle keeps

None of this changes. The handle is the only restoration path in every family,
and cluster termination ends the native state with the cluster. `terminate`
sets `closed` and undoes only the shaping, endpoint and freeze fault phases; it
never visits the override maps or the tracked fault kinds.

| Family | State kept by the handle | Settlement and Restore | After `Terminate` |
| --- | --- | --- | --- |
| Config | `previous` entry (or absence), `applied` value, `known`, `restored`; the exact setting | Restore reconciles first, refuses a changed current value, removes or resets the entry, then reads it back; idempotent after success | Refused with `errClusterTerminated` (`poolPolicyReady`) |
| MessengerPolicy | The list of `ConfigOverrideHandle` values it made | Restore calls each in reverse order, joins errors, can be retried | Same as Config |
| OSDFlag | `previous`, `applied`, `restored` | Restore reconciles and refuses an outside change | Refused (`poolPolicyReady`) |
| MGRModule | `previous`, `applied`, `alwaysOn`, `restored` | Restore reconciles membership; no readiness claim | Refused (`poolPolicyReady`) |
| Blocklist | `until`, `known`, `restored`, the registry token | Restore removes only the entry it created and refuses a changed expiry; lost reply is retryable | Registry entry stays; Restore refused (`poolPolicyReady`) |
| FullRatios | `previous`, `expected`, `pending`, FSID, epoch, `restored`, the registry token | Ordered step reconciliation through the captured session | Registry entry stays; the session refuses a closed cluster |
| DebugLevel | The window state: setting, owned level, generation identity, receipt with transitions, terminal error, a pending settle function | Restore is driven by the window's own generation checks; a set whose end is unconfirmed keeps the window open until settled | Not traced; unconfirmed |

Under option A the cluster-side contribution shrinks to the tracking record
(identity of the active override per key, plus the registry rules that already
exist for faults). It stores no restoration state and no second copy of the
lifecycle. Uncertain-outcome ownership stays with the handle: a non-nil handle
returned with an error must still be restored or the cluster terminated, and
nothing in the move adds automatic restoration or new reporting.

## 5. Migration inventory under the direct v0.x policy

The v0.x policy is a direct migration in one minor release, with no deprecated
alias and no transitional wrapper in `ceph`. A `ceph.Container` method that
returns an `override` type would need `ceph` to import `override`, so the
methods are removed rather than forwarded, as the fault package note concluded.

| Item | Work at the implementation PR | Notes |
| --- | --- | --- |
| Go callers | 76 files, 188 call lines by name (section 1). 75 are tests; the one non-test file is `messenger_policy.go`, which moves with the group | `internal/integration` is the bulk (60 files). The rewrite is mechanical: `c.OverrideX(ctx, ...)` becomes `override.X(ctx, c, ...)` and imports change. Receiver-resolved counts need the compiler, not text search |
| `ceph` tests that touch private state | `config_test.go`, `debug_level_test.go`, `fault_registry_family_test.go`, `finalization_debug_test.go`, `messenger_test.go`, `mgr_modules_test.go`, `osd_policy_test.go` | They either move with the logic and need an `override`-side seam for a fake control container, or stay in `ceph` as tests of the hook. This is a real cost of option A and is not estimated here |
| Shared-gate tables | Remove seven `sharedMethodPolicies` entries and add the package-function table and its coverage test | `TestSharedGateClassifiesEveryMethod` requires both directions, so the removal and the new table must land together |
| API inventories | `api-naming-inventory.json` (92 mentions) and the members and review receipts | Read by `internal/apiinventory`. Regenerating them is a code-checked change, not part of this note |
| Package pages | `docs/ceph.md`, `docs/cephfs.md`, `docs/fault.md`, `docs/rbd.md`, `docs/rgw.md` | Read by checks, so they are outside the documentation-only allowlist. Not touched here; a new `docs/override.md` page would be a new top-level page and also runs every check |
| Guides | `contexts-and-cleanup`, `errors`, `fault-injection`, `messenger-client-fixtures`, `native-commands`, `upgrading-ceph-policy-names`, `upgrading-typed-values`, research `osd-debug-overhead` | `messenger-client-fixtures.md` is read by an `internal/integration` coverage test |
| Design pages | `fault-package.md` decision D3 says these overrides stay in `ceph`. This note supersedes that for the override group by owner decision; D3 is amended when the move lands | `container-decomposition.md` section "Cluster-wide reversible overrides" gets a pointer to this note |
| CHANGELOG | One breaking entry listing the seven moved entry points and the handle types, with old and new names. An earlier entry says `TemporaryBlocklist` and `BlocklistOverride` stay in `ceph`; that text becomes historical | |
| Upgrade mapping | A new guide section or page mapping each old method and handle type to its `override` name | The older `upgrading-ceph-policy-names.md` maps earlier renames and would gain a pointer |
| Native evidence | The families have native tests in `internal/integration` and `internal/debugoverhead` | A move is structural. None of those results are claimed for the new layout until the implementation PR's CI runs them on the exact head |

## 6. Open questions

### Not decided

- The package name `override`, every function name, every handle type name, and
  whether the `Override` and `Temporary` prefixes are dropped.
- Handle placement (option A or B) for each family, and whether a mixed layout is
  acceptable for `DebugLevel`.
- The hook seam: typed operations per family or the existing raw `CLI` hook, and
  the name and shape of `internal/overridehook`.
- Whether the three map families (config, OSD flag, MGR module) join the existing
  fault registry or keep their own maps behind an opaque record.
- How `Finalization.TrackDebugLevel` names the handle (an interface method, or
  moving tracking out of `Finalization`) and how absence is represented.
- Where `OverrideMessengerPolicy` belongs. The decomposition note observes that
  it takes no lock of its own and is built from `OverrideConfig`; and that
  `TemporaryBlocklist` is a time-bounded fence rather than a restore-to-previous
  override. Placing either elsewhere is still possible.
- Nil cluster behavior of the package functions. `admitShared` returns nil for a
  nil cluster, so admission alone does not refuse it, and `OverrideConfig` and
  `OverrideDebugLevel` have nil-receiver branches today. The facts decision
  returns an error and a zero value for a nil cluster; the same shape is likely
  but is not decided for overrides.
- How the lock that guards the neutral tracking record is ordered against the
  handle's own state, which is a lock-order review item, not a design choice
  made here.
- The internal state and lock split, which stays a separate reviewed step.
- Whether and when the exported move happens. The owner decision does not require
  an immediate public move before the internal boundaries are ready.

### Not established by this note

- That any import cycle is or is not unavoidable in a compiled tree. The analysis
  is source reading; the fallback to package functions in `ceph` is neither
  triggered nor ruled out beyond the reading above.
- Behavior after `Terminate` for `OverrideDebugLevel`, which was not traced.
- Any native, timing or lifecycle behavior of a moved family.
- Receiver-resolved caller counts, because the counts are textual.
- The effect on `Container`'s exported method count beyond the arithmetic that
  seven entry points leave it: 73 declared exported methods today, 66 after only
  this group moves, before the facts and observation groups.
- Anything about #155, #156 and #157, which keep their own prerequisites.

## Review checklist for the implementation PR

- The package map above is replaced by the chosen names, with the exported
  declarations listed in the PR.
- A compiled before and after import graph (module-internal imports only) is part
  of the PR, with no `ceph` import of `override`.
- Every exported function of `override` is classified by the shared-cluster table,
  and a test refuses an unclassified one.
- Each family keeps its lock acquisition, context handling, error text and
  `Restore` retry behavior, shown by moved or kept unit tests with negative
  controls.
- No exported field, method or hook exposes `Container.mu`, a keyring or config
  secret, or an executor that accepts arbitrary Ceph arguments.
- Migration of the 76 caller files, the docs, `CHANGELOG.md` and the upgrade
  mapping lands in the same change as the removal of the methods.
