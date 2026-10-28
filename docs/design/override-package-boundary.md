# Design: public package for the reversible override operations

Design note for the `ceph.Container` size reduction (Refs #146). It records the
owner decisions of 2026-10-27 on the override group as decided, including the
owner answers Q1 to Q9 to the first revision of this note (section 12), and
carries the resulting concrete contract: the package, every exported signature
and name, the "not applied" handle contract, the
internal hook seam, the registry and Terminate behavior, the debug-level design,
the import graph, and the order of the implementation pull requests.

Status: implemented. All eight implementation pull requests of section 9 (I-1
to I-8) are merged and I-9 closes the series, so the package `override` holds the
seven operations and `ceph.Container` has none of the former methods. This note
is kept as the design record of that implementation. The sections below were
written before the implementation and keep their evidence labels and the line
numbers of the inspected revision; where the code now differs from a statement,
the code and [override.md](../override.md) are authoritative.

The word "kind" names one of the seven override operations (config, messenger
policy, OSD flag, MGR module, blocklist, full ratios, debug level). The word
"group" names the seven together.

Inspected `main`: `3dc0d13` (merge of #242). Every `path:line` below is a line of
that revision. Evidence is labeled:

- **R** source reading at that revision.
- **P** a throwaway prototype that was compiled and run outside the repository
  tree and deleted afterwards (section 8.2 says exactly what it did and did not
  cover).
- **U** unconfirmed: stated as a proposal or an expectation, not verified.

Nothing was run natively. No Docker was used.

## 1. Decided

The owner decisions of 2026-10-27 on #146 (override entry points and package
boundary; override package) are fixed inputs of this note.

| # | Decision | Consequence recorded here |
| --- | --- | --- |
| D1 | Prefer a public subpackage for the override operations, with package-level entry points that take the cluster. Package functions in `ceph` are the fallback only if an import cycle is structurally unavoidable. Moving only the implementation to `internal/override` does not satisfy the preference. | Section 8 shows that no cycle is structural, so the fallback is not triggered (R, P). |
| D2 | Option A: the override handle types and their `Restore` move to the `override` package together with the entry points. Option B (handles stay in `ceph`) is not chosen. The cost (private-state `ceph` tests that move or gain an `override`-side seam) is accepted; each implementation pull request decides per test file. | Sections 3, 7 and 9. |
| D3 | `Finalization.TrackDebugLevel` depends on a behavior, not a handle type: a narrow interface whose only behavior is `Receipt() DebugLevelReceipt`. The receipt value types stay in `ceph`. | Section 4.1. |
| D4 | Absence is an explicit state, not a nil. A call that is refused or never applied returns a non-nil handle that implements the same interface; its receipt states "not applied" with the reason; its `Restore` is a no-op that never reports a restoration. A handle returned with an error for an uncertain outcome stays a real restorable handle, and its receipt distinguishes "never applied" from "applied or uncertain". The implementation must pin, with tests, how the failure manifest records the earlier cases (nil handle, handle with nil state). | Section 4. |
| D5 | The handles keep their state and their `Restore` or settlement responsibilities. Per-operation shared-cluster admission, context and error behavior, lock ordering, restoration lifetimes and uncertain-outcome ownership are preserved. Do not expose locks, credentials or an unrestricted executor; do not duplicate lifecycle or admission state; do not create a broad abstraction to force the split. | Sections 5 to 7. |
| D7 | Q1: the not-applied rule applies to all seven kinds and every `override` function returns a non-nil handle. No accessor is added to the six kinds without a receipt; the reason is the returned error. | Sections 4.2 and 4.3. |
| D8 | Q2: one handle type per kind that carries an extra member (the refused or not-applied state), not a separate type behind an interface. Callers need no type assertion or reflection. | Section 4.2. |
| D9 | Q3: no `Override` or `Temporary` in function names; the verb follows the behavior of each operation: `Set...` for the six settings, `AddBlocklist` for the addition, handles `*...Handle`. Decided by the owner, including `override.AddBlocklist` returning `*BlocklistHandle` (the Ceph concept is named blocklist). | Sections 3.1 and 3.2. |
| D10 | Q4: a nil cluster returns a not-applied handle and an `ErrInvalidRequest` error for all seven kinds, with the three deliberate behavior changes. | Section 4.5. |
| D11 | Q5: typed operations per kind, not the raw CLI hook. | Section 5. |
| D12 | Q6: the config, OSD flag and MGR module maps join the fault registry. | Section 6.1. |
| D13 | Q7: the messenger policy and the blocklist both belong to `override`. | Section 3. |
| D14 | Q8 and Q9 as recommended: the debug-level registry stays a leaf-locked registry in `ceph`; pull request order as in section 9. | Sections 7 and 9. |
| D6 | Facts (`ceph.FactsOf`, `ceph/cluster_facts.go:47`) and live observations (`ceph.Observe`, `ceph/observer.go:33`) are separate groups with their own decisions. This note does not move them. | Both exist on `main`; nothing here depends on their shape except the read-back reuse noted in section 5.6. |

Two documents record earlier positions that this note leaves in place or
supersedes. `docs/design/fault-package.md` decision D3 says that the general
overrides stay in `ceph`; the owner decision D1 supersedes it for this group and
the amendment landed with the last migration pull request (section 9, I-9).
Decision D5 of the same document says that no universal `Restore`, `Close` or
cleanup interface is introduced. The implementation keeps that: the one new
interface (`ceph.DebugLevelReceipter`) has no `Restore` method and exists only
for `Finalization`.

## 2. Current state (R)

### 2.1 Entry points and their exact acquisition

All seven are `sharedExclusive` in `sharedMethodPolicies`
(`ceph/shared_gate.go:124-130`). `Tenant` has no forwarder for any of them
(no `Override*` or `TemporaryBlocklist` text in the tenant source).

| Kind | Method and line | Order of checks before the first native command | Apply lock | Restore lock |
| --- | --- | --- | --- | --- |
| config | `OverrideConfig` `config.go:82` | gate 83, `ctx.Err` 86, normalize 89, nil cluster 93, bootstrap messenger refusal 96-101 | `lockTopology(ctx)` 102, then startup timeout 113 | `c.mu.Lock()` 158 (no context) |
| messenger policy | `OverrideMessengerPolicy` `messenger_policy.go:128` | gate 129, policy validation 132, empty 136, `MessengerV2Secure` 139, then one `OverrideConfig` per setting 143 | none of its own | one `Restore` per child, reverse order 165 |
| OSD flag | `OverrideOSDFlag` `osd_policy.go:149` | gate 150, supported flag 153 | `c.mu.Lock()` 156 (no context), timeout 164 after the lock | `c.mu.Lock()` 200 |
| MGR module | `OverrideMGRModule` `mgr_modules.go:60` | gate 61, name 64 | `c.mu.Lock()` 67, timeout 75 | `c.mu.Lock()` 129 |
| blocklist | `TemporaryBlocklist` `fencing.go:66` | gate 67, address 70, duration 74 | `c.mu.Lock()` 77, timeout 87 | `c.mu.Lock()` 127 |
| full ratios | `OverrideFullRatios` `full_ratios.go:85` | gate 86, `ctx.Err` 89, nil cluster 92, ratios 95, deadline 99 (before the lock) | `lockTopology(ctx)` 101 | `lockTopology(ctx)` 147, deadline 145 |
| debug level | `OverrideDebugLevel` `debug_level.go:150` | gate 151, nil 154, `ctx.Err` 157, setting 160, daemon form 164, timeout 169 (before the lock) | `lockTopology(ctx)` 171, released at 176; each window operation re-locks (586, 625) | window operations only; no cluster lock held across `Restore` |

The messenger policy takes the gate twice: once at `messenger_policy.go:129` and
once per option inside `OverrideConfig` at `config.go:83`. Both use the same
policy, so the first refusal is the one reported.

### 2.2 Handles and their state

The seven handle types are `ConfigOverrideHandle` (`config.go:39`),
`MessengerPolicyOverrideHandle` (`messenger_policy.go:116`),
`OSDFlagOverrideHandle` (`osd_policy.go:130`), `MGRModuleOverrideHandle`
(`mgr_modules.go:42`), `BlocklistOverride` (`fencing.go:31`),
`FullRatiosOverride` (`full_ratios.go:58`) and `DebugLevelOverrideHandle`
(`debug_level.go:87`). Each unexported state struct holds the restoration state;
copies share it. Seven `Restore` methods, `BlocklistOverride.Address`,
`DebugLevelOverrideHandle.Receipt` and the `String`/`GoString` pairs of the config
and debug-level handles complete the surface. In
`docs/design/api-naming/ceph/declarations.json` that is 27 declaration rows
(7 types, 13 handle methods, 7 `Container` methods).

### 2.3 Where `ceph` itself depends on override state

| Consumer | What it reads | Where |
| --- | --- | --- |
| three maps typed with the handle types (replaced by registry kinds in I-2; the line numbers are those before it) | `configOverrides`, `flagOverrides`, `moduleOverrides` | `ceph.go:46-48`; used at `config.go:110,123,212`, `osd_policy.go:161,174,242`, `mgr_modules.go:72,102,182` |
| `deletePool` | "is an override of `mon_allow_pool_delete` held", through the map and the handle's `state.restored` (after I-2: `allowPoolDeleteHeld`, a registry lookup) | `pool_removal.go:120` |
| `Finalization.TrackDebugLevel` and the manifest | `*DebugLevelOverrideHandle`, its `state`, `Receipt()` | `finalization.go:73`, `finalization_debug.go:30,89,109-131` |
| fault registry | kinds `faultBlocklist` and `faultFullRatios`, phase tracked; `terminate` never visits them | `fault_registry.go:67-72,178-187,193-216`, `ceph.go:1014-1021` |
| debug-level registry | `Container.debugLevels`, own leaf lock | `ceph.go:113-115`, `debug_level.go:258-283` |
| shared-cluster tables | one `sharedMethodPolicies` entry per method and a reflection test | `shared_gate.go:53-158`, `shared_gate_test.go` |

### 2.4 Callers (textual counts)

Counted by method name over Go files; a textual count overstates call sites of
one receiver type, and receiver-resolved counts need the compiler (U).

| Method | Go files | Call lines | Markdown files naming it |
| --- | --- | --- | --- |
| `OverrideConfig` | 43 | 65 | 17 |
| `OverrideOSDFlag` | 32 | 45 | 14 |
| `OverrideMGRModule` | 4 | 26 | 10 |
| `OverrideFullRatios` | 7 | 22 | 12 |
| `TemporaryBlocklist` | 4 | 16 | 12 |
| `OverrideDebugLevel` | 4 | 9 | 6 |
| `OverrideMessengerPolicy` | 3 | 7 | 9 |

Distinct Go files that call at least one of the seven: 78, namely 15 test files
in `ceph`, 62 in `internal/integration`, one in `internal/debugoverhead`. The
only non-test caller is `messenger_policy.go` calling `OverrideConfig`. `rbd`,
`cephfs`, `rgw`, `fault` and `internal/multicluster` do not call them. The
handle type names also appear in test helpers outside `ceph`
(`ConfigOverrideHandle` in 12 files, `OSDFlagOverrideHandle` in 8,
`MGRModuleOverrideHandle` in 4, `FullRatiosOverride` in 5,
`DebugLevelOverrideHandle` in 6). Twenty Markdown files name an override method
or handle type, including `CHANGELOG.md` and this note. No file under `.github`
or `docs/contributing` names them, so no package-split shard is expected for
this move (R; the implementation pull request repeats the search).

## 3. Package, signatures and types

### 3.1 Package

`github.com/JSYoo5B/ceph-testcontainers-go/override`, package name `override`.
It imports `ceph`; `ceph` never imports it. Every function takes `*ceph.Container`
as the cluster. `Tenant` has no entry point, as today. A cluster lent to one
exclusive lease is allowed, because `admitShared` allows every policy but
`sharedRefused` in that mode (`shared_gate.go:260-272`).

Function names carry no `Override` or `Temporary` prefix because the package
name carries it. Settings are verbs, `override.Set...`, and every handle ends in
`Handle` (owner decision Q3). The blocklist is an addition to the OSDMap, not a
setting of an existing value, so its verb is `Add`: `override.AddBlocklist`
returns a `BlocklistHandle`. `Add` says what the call does (it creates one entry
that did not exist) and what `Restore` undoes (it removes that entry only), which
`Set` would blur because a set suggests a previous value to put back. The Ceph
concept is named blocklist, and `Add` says what the call does. The owner decided
that the verb follows the behavior of each operation and that
`override.AddBlocklist` returning `*BlocklistHandle` is the name.

Verb rule: `Set` for an operation that replaces an existing value and later puts
the previous value back; `Add` for an operation that creates an entry that did
not exist and later removes only that entry.

| Function | Verb | Why |
| --- | --- | --- |
| `SetConfig` | Set | writes one central database entry; `Restore` returns the previous value or removes the entry if it was absent |
| `SetMessengerPolicy` | Set | sets the connection modes through config entries; `Restore` returns each to its previous value |
| `SetOSDFlag` | Set | sets or unsets one global flag; `Restore` returns the previous state |
| `SetMGRModule` | Set | sets one module's enabled state; `Restore` returns the previous state |
| `SetFullRatios` | Set | replaces the three thresholds; `Restore` returns the captured tuple |
| `SetDebugLevel` | Set | replaces one runtime level; `Restore` returns the prior level |
| `AddBlocklist` | Add | creates one blocklist entry that did not exist (an existing entry is refused); `Restore` removes only that entry |

### 3.2 Old name to new name

| Today (`ceph`) | New (`override`) | Handle: old to new |
| --- | --- | --- |
| `(*Container).OverrideConfig(ctx, ConfigSetting) (*ConfigOverrideHandle, error)` | `SetConfig(ctx, c *ceph.Container, setting ceph.ConfigSetting) (*ConfigHandle, error)` | `ceph.ConfigOverrideHandle` to `override.ConfigHandle` |
| `(*Container).OverrideMessengerPolicy(ctx, MessengerPolicy) (*MessengerPolicyOverrideHandle, error)` | `SetMessengerPolicy(ctx, c *ceph.Container, policy ceph.MessengerPolicy) (*MessengerPolicyHandle, error)` | `ceph.MessengerPolicyOverrideHandle` to `override.MessengerPolicyHandle` |
| `(*Container).OverrideOSDFlag(ctx, OSDFlag, bool) (*OSDFlagOverrideHandle, error)` | `SetOSDFlag(ctx, c *ceph.Container, flag ceph.OSDFlag, enabled bool) (*OSDFlagHandle, error)` | `ceph.OSDFlagOverrideHandle` to `override.OSDFlagHandle` |
| `(*Container).OverrideMGRModule(ctx, string, bool) (*MGRModuleOverrideHandle, error)` | `SetMGRModule(ctx, c *ceph.Container, name string, enabled bool) (*MGRModuleHandle, error)` | `ceph.MGRModuleOverrideHandle` to `override.MGRModuleHandle` |
| `(*Container).TemporaryBlocklist(ctx, string, time.Duration) (*BlocklistOverride, error)` | `AddBlocklist(ctx, c *ceph.Container, address string, duration time.Duration) (*BlocklistHandle, error)` | `ceph.BlocklistOverride` to `override.BlocklistHandle` |
| `(*Container).OverrideFullRatios(ctx, FullRatios) (*FullRatiosOverride, error)` | `SetFullRatios(ctx, c *ceph.Container, ratios ceph.FullRatios) (*FullRatiosHandle, error)` | `ceph.FullRatiosOverride` to `override.FullRatiosHandle` |
| `(*Container).OverrideDebugLevel(ctx, DebugLevelSetting) (*DebugLevelOverrideHandle, error)` | `SetDebugLevel(ctx, c *ceph.Container, setting ceph.DebugLevelSetting) (*DebugLevelHandle, error)` | `ceph.DebugLevelOverrideHandle` to `override.DebugLevelHandle` |

Handle method sets after the move (everything else on the old handles is
unexported):

| Handle | Methods |
| --- | --- |
| `ConfigHandle` | `Restore(ctx) error`, `String() string`, `GoString() string` (value receivers, as today) |
| `MessengerPolicyHandle` | `Restore(ctx) error` |
| `OSDFlagHandle` | `Restore(ctx) error` |
| `MGRModuleHandle` | `Restore(ctx) error` |
| `BlocklistHandle` | `Restore(ctx) error`, `Address() string` |
| `FullRatiosHandle` | `Restore(ctx) error` |
| `DebugLevelHandle` | `Restore(ctx) error`, `Receipt() ceph.DebugLevelReceipt`, `String() string`, `GoString() string` |

The shape of each handle is the one it has today: an `owner *ceph.Container`, the
exact request, and a pointer to a state struct that copies share. The state
structs move with the handle.

### 3.3 What stays in `ceph`

| Stays | Reason |
| --- | --- |
| `ConfigSetting`, `ConfigEntry`, `MessengerPolicy`, `MessengerConnectionMode`, `OSDFlag` and its constants, `MGRModuleState`, `BlocklistEntry`, `FullRatios`, `FullRatioSnapshot` | Read by observations (`Observer.Configuration`, `Observer.OSDFlags`, `Observer.MGRModules`, `Observer.BlocklistEntries`, `Observer.FullRatios`) and by options and bootstrap (`WithMessengerPolicy`, `ceph.go` validation). Moving them would force `ceph` to import `override`. |
| `DebugLevelSetting`, `DebugLevelState`, `DebugLevelReceipt`, `DebugLevelTransition` | D3: the receipt value types stay, because `Finalization` copies the receipt. |
| every unexported helper that `ceph` still uses | Section 5. |

Additive `ceph` API, needed before the first kind can return a not-applied
debug window (section 4):

| Addition | Shape |
| --- | --- |
| interface | `type DebugLevelReceipter interface { Receipt() DebugLevelReceipt }` |
| state | `DebugLevelNotApplied DebugLevelState = "not-applied"` |
| receipt field | `Reason string` with JSON name `reason,omitempty` on `DebugLevelReceipt` |
| signature | `func (f *Finalization) TrackDebugLevel(handle DebugLevelReceipter)` |

`DebugLevelReceipter` follows the Go convention for a one-method interface. D3
fixes the behavior, not the interface name, so this name is the one choice in
this note that is still open (section 12).

Removed from `ceph` by the full migration: 7 types, 13 handle methods, 7
`Container` methods (27 declaration rows). `Container` exported methods go from
73 to 66 by the same textual count as the decomposition note
(`grep` of `func (c *Container) [A-Z]` in non-test files gives 73 at this
revision), before the facts and observation groups. No type alias and no
transitional wrapper stays in `ceph`: a `Container` method that returned an
`override` type would make `ceph` import `override`.

## 4. The "not applied" handle

### 4.1 Interface and receipt (decisions D3 and D4)

`Finalization` keeps a list of `DebugLevelReceipter`. A debug window is "absent"
when the registration is a nil interface or its `Receipt()` is the zero receipt
(`State == ""`), which is what a nil pointer, a nil-state handle and a zero-value
handle return today (`debug_level.go:103-106`). The previous test
`handle == nil || handle.state == nil` (`finalization_debug.go:111`) is therefore
equivalent for every handle that this module creates, because a real window
records `DebugLevelPriorRead` before any handle leaves `openDebugLevel`
(`debug_level.go:411-412`) (R).

A typed nil does not compare equal to nil once stored in the interface. The
prototype confirms that `var r ceph.DebugLevelReceipter = (*T)(nil)` is not
`nil` and that `Receipt()` on it is safe and returns the zero receipt (P).
Absence must therefore be decided by the receipt, never by `== nil` alone, which
is the typed-nil problem that D4 names.

A not-applied window has `State == DebugLevelNotApplied`, `Reason` set to the
error text, `Daemon`, `Name` and `Requested` copied from the request as given,
and no `Applied` or `Restored` value. The manifest records it with
`Present: true`, so it is distinguishable from an absent registration
(`Present: false`). The manifest receipt gains the same `reason` field, redacted
and bounded like the other fields (`debugField` at `finalization_debug.go:135`
with `maxFinalizationDebugError`).

### 4.2 One handle type per kind, not a second type

The design returns the same handle type for every outcome and records the
outcome in the handle's state. A separate "not applied" type would need each entry
point to return an interface instead of its concrete pointer, which would hide
`BlocklistHandle.Address` and force one new interface per kind. A handle
that carries `refused error` in its state costs one field and keeps the
concrete signatures in section 3.2. `Restore` on such a handle returns nil
before it takes any lock, leaves the state and receipt unchanged, never marks
anything restored and never touches the registry. Returning an error from a
refusal's `Restore` was rejected: `defer h.Restore(ctx)` and
`Finalization.Cleanup` steps would then report a failure for every refusal, which
reintroduces the nil check that D4 removes.

This is decision D8. One point is worth stating because two answers meet here:
D8 says callers read the state the handle carries, while D7 adds no accessor to
the six kinds without a receipt. Those six therefore carry the state internally
(it makes `Restore` a no-op) and a caller learns the outcome from the returned
error, whose class `ErrInvalidRequest` or `ErrSharedCluster` promises that nothing
changed (`errors.go:15-32`). Only `DebugLevelHandle` exposes it, through
`Receipt().State`. If the owner wants the six to expose it too, that is a new
decision, not a consequence of D7.

### 4.3 Which kinds get it

The design applies D4 to all seven kinds, so that no `override` function returns
a nil handle. For the six kinds without a receipt the reason is the returned error,
and the handle exposes no new accessor (decision D7).

### 4.4 Every refusal, failure and uncertain path

"Today" is the handle the `Container` method returns. "New" is the handle the
`override` function returns. The error is unchanged unless the last column says so.

Common paths:

| # | Path | Kinds | Today | New handle and receipt | Error |
| --- | --- | --- | --- | --- | --- |
| C1 | nil cluster | config, full ratios, debug level reach an explicit check (`config.go:93`, `full_ratios.go:92`, `debug_level.go:154`); messenger policy reaches it through config; OSD flag, MGR module and blocklist dereference `c.mu` and panic (`osd_policy.go:156`, `mgr_modules.go:67`, `fencing.go:77`) | nil handle, plain error; or panic (R, expected from the dereference, U) | not applied; debug level: `State=not-applied`, `Reason` | new, section 4.5 |
| C2 | shared-cluster refusal | all | nil, `*OperationError` with `ErrSharedCluster` | not applied | same class; `Op` becomes `override.<Function>` |
| C3 | context already ended | config, full ratios, debug level check it first | nil, context error | not applied | the context error, unclassified |
| C4 | invalid input | all | nil | not applied | same text and class as today. Config, full ratios and debug level classify with `ErrInvalidRequest`; OSD flag (`osd_policy.go:154`), MGR name (`mgr_modules.go:65`) and blocklist duration (`fencing.go:75`) return plain errors and keep doing so |
| C5 | wait for the topology lock ended by the context | config, full ratios, debug level | nil, context error | not applied | same |
| C6 | terminated cluster or no control container (`poolPolicyReady`, `pool_policy.go:181`) | all but messenger policy directly | nil, `errClusterTerminated` (`ErrInvalidRequest`, `native_command.go:47`) or a plain "control container is unavailable" | not applied | same values; the hook returns the identical `errClusterTerminated` value |
| C7 | overlap with a held override | all but debug level | nil, plain error with the kind's text (`config.go:111`, `osd_policy.go:162`, `mgr_modules.go:73`, `fault_registry.go:68,71`) | not applied | same text, unclassified |
| C8 | read before the first write fails | config 115-118, OSD flag 166-169, MGR module 77-80, blocklist 89-93 | nil, native error | not applied; the reservation is settled with no handle | same |

Kind-specific paths:

| Kind | Path | Today | New handle | Error |
| --- | --- | --- | --- | --- |
| config | messenger-fixed option (`config.go:96-101`) | nil | not applied | `ErrInvalidRequest`, same text |
| config | stored value already equal (`config.go:124-127`) | handle, nil | real handle, a lease with no write | none |
| config | `config set` or readback fails (`config.go:128-142`) | handle and joined error | real handle, uncertain | same |
| config | success without a stored entry (`config.go:143-145`) | handle and error | real handle | same |
| messenger policy | empty or invalid policy, or `MessengerV2Secure` (`messenger_policy.go:132-141`) | nil | not applied, no children | `ErrInvalidRequest` as today |
| messenger policy | first child refused (`messenger_policy.go:148-151`) | nil | not applied, no children | the child's error |
| messenger policy | later child refused or uncertain (`messenger_policy.go:152`) | handle with the earlier children and the error | handle with the children that are real handles (a refused child is not appended) | the child's error |
| OSD flag | unsupported flag (`osd_policy.go:153`) | nil | not applied | plain error |
| OSD flag | flag already in the requested state (`osd_policy.go:175-177`) | handle, nil | real handle, a lease with no write | none |
| OSD flag | set or readback fails (`osd_policy.go:178-187`) | handle and error | real handle, uncertain | same |
| MGR module | missing, always-on, cannot-run or protected module (`mgr_modules.go:81-95`) | nil | not applied | plain errors, same text |
| MGR module | already in the requested state (`mgr_modules.go:103`) | handle, nil | real handle, a lease with no write | none |
| MGR module | set or membership readback fails (`mgr_modules.go:106-116`) | handle and error | real handle, uncertain | same |
| blocklist | existing native entry (`fencing.go:94-97`) | nil, reservation settled | not applied | `ErrInvalidRequest`, same text |
| blocklist | add or readback fails after tracking (`fencing.go:100-115`) | handle and error | real handle, uncertain | same |
| full ratios | session, read or ordering fails before tracking (`full_ratios.go:110-123`) | nil, reservation settled | not applied | same |
| full ratios | any failure from the first transition on (`full_ratios.go:127-128`) | handle and error | real handle (tracked before the first native step) | same |
| debug level | registry refuses a second window (`debug_level.go:390-392`) | nil | not applied, `Reason` is the refusal | `ErrInvalidRequest`, same text |
| debug level | target not an owned running daemon (`debug_level.go:395-402`) | nil | not applied | same |
| debug level | prior read fails (`debug_level.go:404-409`) | nil | not applied, `Generation` and the reason recorded | same |
| debug level | set refused because the generation changed (`debug_level.go:419-423`) | nil | not applied | same |
| debug level | set, settle or readback uncertain (`debug_level.go:443-459`) | handle and error | real handle, state `apply-uncertain`, `target-changed` or `applied-verified` as today | same |

A not-applied debug window releases its registry slot in every row above, as the
current code does before it returns nil. A real handle never reports
`not-applied`: the receipt of a window that may have been applied is
`apply-uncertain` or a later state, so the two are distinguishable (D4).

### 4.5 Nil cluster

Every `override` function checks for a nil cluster first, returns a
not-applied handle, and returns an error classified `ErrInvalidRequest` whose
`Op` is `override.<Function>`. This matches `Observe(nil)`
(`observer.go:33-37`, `invalidRequest("observe: the cluster is nil")`) and the
facts decision (error plus zero value). It changes three things on purpose and
is decision D10:

- config, full ratios and debug level return an unclassified plain error today;
  they become `ErrInvalidRequest`.
- OSD flag, MGR module and blocklist panic today (U, from the nil dereference);
  they return an error.
- A nil cluster with otherwise invalid input now reports the nil cluster
  first, not the input error.

`admitShared` returns nil for a nil cluster (`shared_gate.go:261-263`), so
admission alone never refuses it.

## 5. The internal hook seam

### 5.1 Shape

A new internal package `internal/overridehook`, registered through a new slot
`hookreg.Override` (a second variable next to `hookreg.Value` and
`hookreg.Fault`, `internal/hookreg/hookreg.go:8,12`). `ceph` registers an
unexported `overrideHooks{}` in `init`, as `faultHooks` does
(`ceph/fault_hooks.go:16-20`). `overridehook` imports `ceph` and gives the typed
view; `ceph` imports neither `overridehook` nor `override`.

The seam is typed, one operation per native command or registry action, with
no raw executor. The alternative, reusing a single `CLI(ctx, c, args...)` hook as
`faulthook` has (`ceph/fault_hooks.go:113`), is smaller (one method) but hands
`override` an unrestricted administrative executor behind an `internal/` path and
would force `override` to re-implement every decode and readback that
`ceph` already owns (`readConfiguration` at `observation_cli_more.go:142`,
`readOSDPolicyDump` at `:164`, `readMGRModules` at `:20`, `readBlocklistEntries`
at `:134`). The owner decision rules the executor out. The cost of the typed seam
is 31 methods on the registered type (10 common and 21 per kind), against 24 in
the `faulthook` interface (R, counted from `internal/faulthook/faulthook.go`).
Each is as wide as one native command.

### 5.2 Common operations

All of them need the cluster mutex held by the caller unless the table says
otherwise, exactly as in the `faulthook` doc comment
(`internal/faulthook/faulthook.go:13-24`).

| Operation | Wraps | Lock | Note |
| --- | --- | --- | --- |
| `Admit(c, op)` | new `admitOverride` over `sharedOverridePolicies`, modeled on `admitFault` (`shared_gate.go:194-200`) | none | An operation without a policy is refused even on a cluster of its own. Names are `override.SetConfig` and so on. |
| `WithTopology(ctx, c, fn)` | `lockTopology(ctx)` (`topology.go:380`) | takes it | for the paths that wait on the context today |
| `WithTopologyNoContext(c, fn)` | `c.mu.Lock()` | takes it | for the paths that do not, including every `Restore` except full ratios and the window |
| `Ready(c)` | `poolPolicyReady` (`pool_policy.go:181`) | held | returns the identical `errClusterTerminated` |
| `StartupTimeout(c)` | `c.settings.startupTimeout` | none | the value is immutable after construction (U) |
| `Terminated()` | `errClusterTerminated` | none | |
| `Reserve`, `Settle`, `Release`, `Tracks` | `reserveFault`, `settleFault`, `releaseFault`, `tracksFault` (`fault_registry.go:113,146,164,174`) | held | tokens are opaque; only the token a key still maps to settles it |

Nothing exposes `Container.mu`, the keyring, the config bytes or a control
container.

### 5.3 Per-kind operations

| Kind | Hook operations (all with the mutex held) | What each wraps | Lock mode kept |
| --- | --- | --- | --- |
| config | `NormalizeConfigSetting`, `ConfigKey`, `ConfigWho`, `CheckConfigAllowed(c, setting)`, `ConfigEntries`, `ConfigSet`, `ConfigRemove` | `normalizeConfigSetting` (`config.go:219`, shared with `decodeConfiguration` at `:290`), `configOverrideKey`/`configWho` (`:268,262`, used by `pool_removal.go`), the messenger refusal (`:96-101`), `configuration` (`:285`), `configCommand` (`:312`, which redacts native output through `runConfigCommand`, `observation_cli_more.go:153`) | apply `WithTopology`, restore `WithTopologyNoContext` |
| OSD flag | `OSDMapFlags`, `SetOSDFlag` | `osdPolicyDump` flags (`osd_policy.go:261`, including its decode checks), `setOSDFlag` (`:247`) | both `WithTopologyNoContext` |
| MGR module | `ValidMGRModuleName`, `MGRModulesSnapshot`, `WaitMGRModuleMembership`, `SetMGRModule`, `ProtectMGRModule` | `mgrModuleNamePattern` (shared with `observation_cli_more.go:38,94`), `waitMGRModules` (`mgr_modules.go:281`), `waitMGRModuleMembership` (`:303`), `setMGRModule` (`:268`), `protectMGRModule` (`:321`, which reads settings and owned extensions) | both `WithTopologyNoContext` |
| blocklist | `NormalizeBlocklistSession`, `BlocklistEntries`, `BlocklistAdd`, `BlocklistRemove` | `normalizeBlocklistSession` (`fencing.go:231`, shared with the decoder at `:208`), `blocklistEntries` (`:183`), `osd blocklist add/rm` (`:101,155`) with the duration formatting kept in `ceph` | both `WithTopologyNoContext` |
| full ratios | `OpenFullRatios(ctx, c)`, returning a session with `FSID()`, `Read(ctx)`, `SetRatio(ctx, name, value)`, `CheckIdentity(ctx, fsid)` | `fullRatiosSession` (`full_ratios.go:186`), `read` (`:233`), the `osd set-<name>-ratio` command (`:359`) and the identity refusal (`:161-163`) | both `WithTopology` |
| debug level | `OpenDebugTarget(ctx, c, daemon)`, `DebugWindows(c)`; target methods `Generation`, `Get`, `Set`; window registry methods `Reserve`, `Release` | section 6 | window operations take the topology lock themselves |

The session and the debug target are returned as `any` and asserted to a small
interface inside `overridehook`, the same way `Reserve` returns a token as `any`
today. Go method signatures cannot name a type that `overridehook` defines,
because `ceph` may not import it. Primitive-typed methods keep that possible: the
debug target's `Generation` returns `(owned bool, identity string, running, paused
bool, err error)`.

The full ratios session stays captured once: `OpenFullRatios` is called inside
`WithTopology` and its control handle and original FSID never leave `ceph`
(`full_ratios.go:184-207`). `override` never sees the control container or a
replacement for it.

### 5.4 Admission

`sharedOverridePolicies` in `ceph` has seven `sharedExclusive` entries, one per
exported function. The seven `sharedMethodPolicies` entries go in the same
change as each method, because `TestSharedGateClassifiesEveryMethod` requires
the table and the method set to match in both directions
(`shared_gate_test.go:21-35`).

The existing fault precedent is weaker than the previous version of this note
said. `TestFaultAdmissionKeepsMethodPolicies` pins a hand-written list
(`fault_registry_test.go:382`) and `TestPublicFaultsRefuseSharedCluster` calls a
hand-written map (`fault/admission_test.go`); neither enumerates the exported
functions of the package (R). For `override` the design is a test in the package
that parses its own source with `go/parser`, collects every exported function and
requires `overridehook.Admit` to know `override.<Name>` (and to refuse an unknown
name), plus a test that calls each function with zero arguments on a shared
cluster and requires `ErrSharedCluster` before any native effect, as
`TestSharedGateClassifiesEveryMethod` does for methods.

The messenger policy admits once with its own name and then calls an unexported
`config` helper, so the second gate of today (section 2.1) disappears. The
observable result is the same, since the first refusal is reported.

### 5.5 What `override` may not import

A small test (`go list -deps`) must show that `override` imports neither
`internal/faulthook` nor `internal/servicehook` in non-test code, so that the raw
`CLI`, `Control` and cluster-lock hooks cannot be reached from the public package.
The unit tests of `override` may import `internal/servicehook` for
`TestCluster`, as `fault` tests do (`fault/admission_test.go`).

### 5.6 Reads shared with the observation group

The readbacks of the overrides are the same reads as the observations. The
typed hooks above call the existing unexported readers, so no second decoder is
introduced. If the observation extraction later changes those readers, the hooks
are the only adapters to touch.

## 6. Tracking, registry and Terminate order

### 6.1 The three maps join the fault registry

The fault registry is the cluster's internal ledger of the changes it currently
holds (`faultRegistry` in `ceph/fault_registry.go`). A change is entered with
`Reserve`, becomes held with `Settle`, is struck out with `Release`, and `Tracks`
asks whether a given entry is still the live one; the ledger is guarded by the
cluster mutex. For the tracked kinds, which are all the override kinds,
`Terminate` does not undo anything: it only ends with the cluster.

`configOverrides`, `flagOverrides` and `moduleOverrides` (`ceph.go:46-48`) have
the handle types as element types, so under option A they must go. The design is
to replace them by three new kinds in the existing registry,
`faultConfigOverride`, `faultOSDFlag` and `faultMGRModule`, all in phase
`faultTracked`, with keys `configOverrideKey(setting)`, the flag token and the
module name, and with the refusal texts of today in their `faultKindSpec.refuse`
(`config.go:111`, `osd_policy.go:162`, `mgr_modules.go:73`).

Why this and not an opaque record per map: the registry already guards by `c.mu`,
already exposes `Reserve`/`Settle`/`Release`/`Tracks`, already has the tracked
phase that `terminate` does not visit, and already holds blocklist and full ratios
the same way. Three new maps of opaque records would add three copies of the same
rule. The cost is three enum values and three table entries.

Equivalence to the maps (R, and P for config):

- "held and not restored" is "the key maps to an entry". Each handle's `finish`
  deletes the map entry today (`config.go:211-213`, `osd_policy.go:240-243`,
  `mgr_modules.go:180-183`); under the registry `finish` calls `Release`.
- "tracked by this cluster" at the top of `Restore` is `Tracks(token)`, which
  also refuses a stale handle whose key now maps to a successor.
- `deletePool` asks the registry whether the `mon_allow_pool_delete` key is held
  (`pool_removal.go:120`) through a private helper; the refusal text stays.
- Every early return between `Reserve` and the creation of the handle settles the
  reservation with no handle, as blocklist and full ratios do
  (`fencing.go:91,95`, `full_ratios.go:112,117,122`). A test must show that no
  reservation remains after each such path, in the style of
  `TestOverridesRefuseUntrackedHandlesAndFreeFailedReservations`
  (`ceph/fault_registry_family_test.go:9`).

### 6.2 Terminate undo order is untouched

`terminate` sets `closed` and runs `undoFaults` in the fixed phases shaping,
endpoint, freeze (`ceph.go:1014-1021`, `fault_registry.go:41,193-216`).
`heldFaults` returns only entries that have an `undo` function in the phase
(`fault_registry.go:178-187`); tracked kinds settle with no undo, so none of the
five tracked kinds (two existing, three new) is ever visited. The design adds no
phase, no `undo` and no cleanup registration. What a restore does after
`Terminate` is unchanged: the five registry-backed kinds are refused by `Ready`
or by the captured session with `errClusterTerminated`, and the registry entry
stays (R, `docs/design/container-decomposition.md` lists the same rows).

### 6.3 Lock order

The state of a handle that is tracked in the registry is read and written only
inside `WithTopology` or `WithTopologyNoContext`, that is under the same mutex as
today. The `faulthook` rule applies: the function must not call a public
`Container` method that takes the cluster mutex and must not lock a handle mutex
that is held while the cluster mutex is taken. The typed hooks are the "locked"
variants of the operations and take no lock themselves. No lock is split, added or
reordered, so the separately reviewed lock decomposition of #146 is unaffected. The
debug window keeps its own order: window lock first, then the cluster mutex inside
each target operation (`debug_level.go:393,586,625`), and its registry keeps its
own leaf lock (section 7).

## 7. The debug-level override

This is the largest and least certain kind (680 lines in `debug_level.go`). The
window logic is already generic over a target: `openDebugLevel` takes a
`debugLevelRegistry`, a `debugLevelTarget` and plain values
(`debug_level.go:386`), and its tests use a fake target
(`debug_level_test.go:156-161`). That seam is the cut.

| Moves to `override` | Stays in `ceph` |
| --- | --- |
| `DebugLevelHandle`, `debugLevelState`, `record`, `current`, `finish`, `sameGeneration`, `settle`, `confirmTarget`, `setRestored` (`debug_level.go:285-384,571-575`) | `DebugLevelSetting`, `DebugLevelState` (plus `DebugLevelNotApplied`), `DebugLevelReceipt` (plus `Reason`), `DebugLevelTransition`, `DebugLevelReceipter` |
| `openDebugLevel` and `Restore` (`:386-569`) | `osdDebugTarget` and its `generation`, `generationLocked`, `get`, `set` (`:577-667`): they need `lockTopology`, `c.osds`, `c.closed`, `c.run`, `controlContainerContext`, `nativeCmds.admit/finish/forget`, `adminSocketCommand` and `supervised` |
| `normalizeDebugLevel`, `normalizeDebugLevelSetting`, `sameDebugLevel`, `checkDebugLevelGeneration` (`:189-224,342-356`), all pure and used only here (R) | `decodeDebugLevel` (`:670`), `parseAdminSocketDaemon` (`admin_socket.go:175`), `daemonIdentity`, `ownsDaemon` |
| the entry point `DebugLevel` | the ownership check of `OverrideDebugLevel` (`:171-182`), exposed as `OpenDebugTarget` |
| | `debugLevelRegistry` (`:258-283`) and the field `Container.debugLevels` (`ceph.go:113-115`), retyped to hold `any` owners and exposed as an opaque window registry |

The registry keeps its own leaf lock on purpose. Putting windows in the fault
registry would make `release` take `c.mu`, and `Restore` must be able to release
without waiting behind a long topology operation that holds `c.mu`; today `release`
takes only the registry mutex (`debug_level.go:277-283`). The `NativeInvocation`
fence for an unconfirmed set (`debug_level.go:644-666`) stays with the target, so
`OSDContainer.Remove`/`Detach` keep draining it and `Terminate` keeps stopping it,
unchanged.

The target's refusal type is private (`debugLevelRefusal`, `:240`) and `override`
cannot name it. The window code reads it with `errors.As` against an interface
with `Changed() bool`, which the private type implements. The generation struct
(`:249`) becomes the primitive return values of `Generation`.

Behavior after `Terminate`, previously "not traced": by reading, a `Restore`
after `Terminate` takes the window lock, finds no pending set, and calls
`confirmTarget`. `generationLocked` reports the daemon as not owned because
`c.closed` is true (`debug_level.go:597`), `checkDebugLevelGeneration` returns
"changed" with "is no longer owned by this cluster" (`:346-347`), and the window
ends in `DebugLevelTargetChanged` with that terminal error. Nothing is restored,
which matches the documented meaning of the state (`:50-53`) (R, not executed, U).

Plan inside the window: the ownership refusals and the registry refusal produce
the not-applied rows of section 4.4; the in-window "nothing was set" paths
(`:396-402,404-409,419-423`) record `DebugLevelNotApplied` with the reason on the
state that already holds `Generation` and `Prior`, release the slot, and return the
handle; `Restore` treats `DebugLevelNotApplied` like a terminal state that returns
nil without recording `Restored`.

## 8. Imports, cycle check and compile feasibility

### 8.1 Graph

Before (R):

```text
rbd, cephfs, rgw ---------------> ceph ---> internal/hookreg
fault ---------------------------^   ^
  +--> internal/faulthook -----------+
rbd, cephfs, rgw ---> internal/servicehook --> ceph
internal/integration, internal/debugoverhead (tests) ---> ceph
```

After:

```text
override ----------------------> ceph ---> internal/hookreg   (one more slot: Override)
   +--> internal/overridehook ----^
rbd, cephfs, rgw, fault ---------> ceph                         (unchanged)
internal/integration (tests) ----> ceph, override
```

`override` imports `ceph` and `internal/overridehook`; `ceph` imports neither.
`override` and `fault` do not import each other. `override` does not import
`faulthook` or `servicehook` in non-test code (section 5.5).

### 8.2 Candidate cycles

A cycle needs non-test `ceph` code to name an `override` type or function.

| # | Edge from `ceph` toward the group | Structural? | Resolution |
| --- | --- | --- | --- |
| 1 | `TrackDebugLevel(*DebugLevelOverrideHandle)` | No: a public signature that names a type | the behavior interface of D3 |
| 2 | three maps typed with handles | No: private fields | fault registry kinds (6.1) |
| 3 | `deletePool` reads a handle's `restored` | No | a private registry query |
| 4 | `debugLevels` registry holds `*debugLevelState` | No: private field | opaque `any` owners (section 7) |
| 5 | `osdDebugTarget` is the window's target | No: it implements an interface `override` defines | stays in `ceph`, reached through `OpenDebugTarget` |
| 6 | `faultBlocklist`, `faultFullRatios` entries | No: registry already exists | reuse |
| 7 | `sharedMethodPolicies` and its test | No | seven entries leave with the methods; new `sharedOverridePolicies` |
| 8 | `OverrideMessengerPolicy` calls `OverrideConfig` | No: both move together | an unexported `config` helper inside `override` |

On this evidence no cycle is structural, so the fallback of package functions in
`ceph` is not triggered. One constraint is real and is not a cycle in non-test
code: a unit test inside `package ceph` cannot import `override`, because
`override` imports `ceph` (the compiler reports "import cycle not allowed in
test"; P). Tests of the moved logic therefore go to `override` and use
`servicehook.TestCluster(control, config)` for a cluster with a fake control
container (`internal/servicehook/servicehook.go:81,347-390`), as `fault` does.

### 8.3 Compile feasibility (P)

A prototype was built in a copy of the tree outside the repository, then
deleted. It contained: the `hookreg.Override` slot; the three new registry kinds;
`overrideHooks` and `internal/overridehook` for admission, registry, topology and
the config operations; `ceph.DebugLevelReceipter`, `DebugLevelNotApplied`,
`Reason` and the changed `TrackDebugLevel`/`debugSnapshot`; and an `override`
package with `Config` ported completely, the other six functions and handle types
as signatures only, and a minimal `DebugLevelHandle` with `Receipt` and the
not-applied constructor. Results:

- `go build ./...` succeeded and `go vet -tags=all` was clean for `override`,
  `internal/overridehook`, `ceph` and `fault`.
- `go list -deps ./ceph` contained no `override` package.
- Four Docker-free tests of the ported `Config` passed: apply then restore
  against a fake control container with the overlap text unchanged, an uncertain
  apply that returns a real handle that later restores, five not-applied cases
  (nil cluster, shared cluster, closed cluster, invalid setting) with a nil-safe
  no-op `Restore` and the classes `ErrSharedCluster` and `ErrInvalidRequest`, and
  the typed-nil and not-applied receipt behavior.
- The existing `TestFinalizationSnapshotsDebugWindowsAroundCleanup`,
  `TestFinalizationDebugWindowBoundsAndRedaction`,
  `TestFinalizationDebugWindowNegativeControls`,
  `TestFaultAdmissionKeepsMethodPolicies` and
  `TestSharedGateClassifiesEveryMethod` passed unchanged against the changed
  `Finalization`.
- A test in `package ceph` that imports `override` failed to compile with the
  import cycle error above.

Not covered by the prototype: the other six kinds' logic, the debug window, the
full ratios session, any race run, any native behavior, the API catalogue tool on
a new package, and the shape of the per-test moves. The hook interface is checked
at run time (the registered value is asserted to an interface, as `faulthook`
does), so each implementation pull request must keep a test that calls
`overridehook` through every method, as `TestRegistryThroughHooks` does for the
fault hooks (`internal/faulthook/faulthook_test.go`).

## 9. Migration order

The v0.x policy is a direct migration with no alias. Each pull request that moves
a kind removes its `Container` method and handle type and migrates every caller,
doc, CHANGELOG fragment and upgrade mapping in the same change. Smallest and least
risky first:

| PR | Content | Behavior invariants | Acceptance evidence | Native implication |
| --- | --- | --- | --- | --- |
| I-1 (#251, merged) | `DebugLevelReceipter`, `DebugLevelNotApplied`, `Reason`, `TrackDebugLevel(DebugLevelReceipter)`, manifest `reason`. `ceph` only; no move. | The manifest of every existing case is byte-identical; a nil or zero-value handle still records `Present: false`. | New tests pin: nil interface, typed nil, zero-value handle, a zero receipt from a foreign receipter, a not-applied receipt (`Present: true`, reason redacted and bounded), a real window; existing finalization tests unchanged. Catalogue regenerated and approved; fragment (kind `feature`). | None. Docker-free. |
| I-2 (#252, merged) | Three registry kinds replace the three maps; `deletePool` uses the registry. Handles stay in `ceph` and hold a token. No public change. | Overlap texts, `Restore` refusal "not tracked", `Pool.Remove` refusal text, Terminate leaves them alone. | The existing `config_test.go`, `osd_policy_test.go`, `mgr_modules_test.go`, `pool_removal_test.go` pass unchanged; new registry tests for the three kinds (phase tracked, no reservation left after each early return, stale token). | None. |
| I-3 (#259, merged) | Package birth: `hookreg.Override`, `overridehook` (common operations), `sharedOverridePolicies`, `override.AddBlocklist`/`BlocklistHandle`, the `go/parser` coverage test, the import test, new catalogue directory, move record, upgrade guide, fragment (`api-break`). | Blocklist reservation, expiry check, retry after a lost reply, `Restore` refused after `Terminate`. | Tests of `fencing_test.go` that need the handle move to `override` against `servicehook.TestCluster`; negative controls for each. | 4 Go files call it; the planner selects their batches, and the native result of the exact head is the only native claim. |
| I-4 (#260, merged) | `override.SetFullRatios`. | Session captured once; FSID check; deadline before the lock; ordered steps; tracked before the first step. | `full_ratios_test.go` override tests move; observation tests stay. | 7 files. |
| I-5 (#261, merged) | `override.SetMGRModule`. | Name check, protect before disable, always-on rules, readiness not claimed. | `mgr_modules_test.go` override tests move. | 4 files. |
| I-6 (#262, merged) | `override.SetOSDFlag`. | Supported-flag list, overlap, restore refusing outside changes. | `osd_policy_test.go` override tests move. | 32 files; mechanical rewrite. |
| I-7 (#265, merged) | `override.SetConfig` and `override.SetMessengerPolicy` together (the latter calls the former). | Messenger-fixed refusals, normalized readback, lost-reply convergence, restore order of the policy children. | `config_test.go`, `messenger_test.go` guard tests and `messenger_policy_test.go` refusal test move or stay as classified in section 10. | up to 46 files (43 and 3 callers, overlap not counted); mechanical rewrite. |
| I-8 (#263, merged) | `override.SetDebugLevel` and the window. | Generation fence, settle before restore, registry leaf lock, native fence, after-`Terminate` result. | the 16 window tests move with their fake target; the 4 `osdDebugTarget` tests stay. | 4 files, one of them in `internal/debugoverhead`. |
| I-9 (this closure) | Documentation and a guard, no behavior change: `fault-package.md` D3 amended to the final state, `container-decomposition.md` counts and rows, the measured `Container` method count (information only, no target), the remaining proposal wording of this note removed, and a guard that blocks the retired names in current docs and code. | None. | Docs only; the guard has a negative control. | None. |

I-1 and I-2 were independent and could proceed in parallel. I-5 to I-7 needed I-2. A
mechanical caller rewrite carries no native claim until CI has run those batches
on the exact head. The sizes of I-6 and I-7 are the cost of D1; a scripted rewrite
reduces review work but not the number of selected batches.

Every move pull request regenerates and approves the `ceph` and `override`
catalogues and adds a record under `docs/design/api-naming/moves/`. Whether the
catalogue tool accepts a package that does not exist yet is U.

## 10. Test files: move or stay

Classification is by test name and is U for per-test private-state access; the
implementation pull request decides each file (D2).

| File | Moves to `override` | Stays in `ceph` |
| --- | --- | --- |
| `config_test.go` (8 tests) | the 7 `TestOverrideConfig*` | `TestConfigurationRejectsMalformedDuplicateDump` |
| `osd_policy_test.go` (8) | 3 `TestOverrideOSDFlag*` | the OSD state and `WaitForPGClean` tests, `TestOSDFlagValidationAndMalformedDump` |
| `mgr_modules_test.go` (12) | the 9 `TestOverrideMGRModule*` | the 3 observation and `WaitMGRModuleReady` tests |
| `fencing_test.go` (8) | 6 `TestTemporaryBlocklist*`/`TestBlocklist*` override tests | session normalization and decode tests |
| `full_ratios_test.go` (9) | the override tests | the observation test and the strict JSON test |
| `messenger_policy_test.go`, `messenger_test.go` | the refusal and bootstrap-guard tests that call `OverrideConfig`/`OverrideMessengerPolicy` | policy settings, mode lists, client container config |
| `debug_level_test.go` (21) | the window tests and normalization (about 16) | decode, `TestOverrideDebugLevelRefusesBeforeCommands` (ownership refusal) and the 4 `osdDebugTarget` tests |
| `finalization_debug_test.go` (3) | none | stays; rewritten against a fake `DebugLevelReceipter`, because a test in `package ceph` cannot build a real `DebugLevelHandle` |
| `fault_registry_family_test.go` (1) | the full ratios and blocklist subtests | the registry kind tests |
| `fault_registry_test.go`, `shared_gate_test.go` | none | stay; gain the three kinds and lose the seven method entries |
| `pool_removal_test.go` | none | `TestRemovePoolRefusesActivePermissionOverride` stays and holds the key through the registry directly, since it cannot call `override` |

Native tests in `internal/integration` and `internal/debugoverhead` only change
their call syntax and imports. One consequence of the cycle rule: no test in
`package ceph` can exercise a real `Finalization` with a real
`override.DebugLevelHandle`; that pairing is covered by the native debug-level
scenario only (U, not established by this note).

## 11. Corrections to the previous version of this note (with evidence)

The review rule of this task asks for concrete evidence where a statement made
earlier is not what the code shows. These replace statements of the previous
version.

| Earlier statement | What the code shows |
| --- | --- |
| "Admission is applied once per public call, as today" | The messenger policy gates at `messenger_policy.go:129` and again for each option in `config.go:83`. |
| A reflection-style test "requires a policy for every exported function", following the fault precedent | The fault tests pin hand lists (`fault_registry_test.go:382`, `fault/admission_test.go`). Only `Container` methods are enumerated by reflection (`shared_gate_test.go:21-35`). Section 5.4 proposes a `go/parser` test. |
| "`OverrideConfig` and `OverrideFullRatios` wait on the context, Restore and `TemporaryBlocklist` do not" | `FullRatiosOverride.Restore` also waits (`full_ratios.go:147`); every other `Restore` and `TemporaryBlocklist` take `c.mu.Lock()` (`config.go:158`, `osd_policy.go:200`, `mgr_modules.go:129`, `fencing.go:77,127`); the debug window takes the topology lock inside each target operation. Section 2.1 lists each. |
| Behavior of `OverrideDebugLevel` after `Terminate` "not traced" | Traced by reading in section 7 (R, not executed). |
| The decomposition note's fallback shape (functions in `ceph`) is "the current direction" in `container-decomposition.md` | The owner decision of D1 prefers the subpackage; the pointer in `container-decomposition.md` is updated in this change. |
| Facts and observations are future groups | Both exist on `main` (`ceph/cluster_facts.go:47`, `ceph/observer.go:33`); the observer's tenant form validates per query. |

No earlier owner decision is contradicted by this design. Two places read a
decision narrowly and were raised as questions (Q1, Q2); both are now answered.

## 12. Owner answers (decided)

The owner answered the nine questions of the first revision on 2026-10-27 (issue
comment "Owner answers to the questions in the override package proposal").

| # | Question | Decided |
| --- | --- | --- |
| Q1 | Does D4 apply to the six kinds without a receipt? | Yes, all seven; every function returns a non-nil handle; no accessor on the six (D7). |
| Q2 | Is one handle type per kind with a refused state acceptable? | Yes (D8). |
| Q3 | Names. | Verb by behavior: `Set...` for settings, `AddBlocklist` for the addition, handles `...Handle`, no `Override`/`Temporary` (D9). |
| Q4 | Nil cluster behavior. | Adopted (D10). |
| Q5 | Typed operations or raw `CLI`. | Typed operations (D11). |
| Q6 | Do the three maps join the fault registry? | Yes, confirmed (D12). |
| Q7 | Where do the messenger policy and the blocklist belong? | Both in `override` (D13). |
| Q8, Q9 | Debug-level registry; pull request order. | As recommended (D14). |

All names in sections 3.1 and 3.2 follow from decisions D7 to D13 and are decided.

Resolved by I-1 (`ceph.DebugLevelReceipter`, #251): the name of the new `ceph` interface that
`Finalization.TrackDebugLevel` accepts. D3 decides its behavior
(`Receipt() DebugLevelReceipt`) but not its name; the note proposed
`DebugLevelReceipter` (Go convention for a one-method interface). It is a public
`ceph` name and was approved before the first implementation pull request.

## 13. Not established

This note records a design and its merged implementation. It does not establish:

- Native, timing, lifecycle or race results of any kind. Each implementation pull
  request quotes only the CI result of its own exact head.
- That the per-test classification of section 10 is what the merged tests did
  file by file; the merged tests and their negative controls are authoritative.
- Receiver-resolved caller counts of the mechanical rewrites, which this note
  estimated only.
- Any quantitative target for the number of `ceph.Container` methods. The owner
  decided that there is none; [container-decomposition.md](container-decomposition.md)
  records a measurement as information.
- Anything about #155, #156 and #157, which keep their own prerequisites.

## 14. Review checklist for the implementation pull requests

- The names of section 3.2 are implemented as decided, with the exported
  declarations listed in the PR.
- A compiled before and after import graph (module-internal imports only), with no
  `ceph` import of `override` or `overridehook`.
- Every exported function of `override` is classified; a `go/parser` test fails on
  an unclassified one; a zero-argument call on a shared cluster refuses with
  `ErrSharedCluster` before any native effect.
- Each kind keeps its lock acquisition (section 2.1), context handling, error text
  and `Restore` retry behavior, shown by moved or kept unit tests with a negative
  control for each.
- A not-applied handle: `Restore` returns nil, takes no lock, records nothing as
  restored and leaves no registry entry; the table of section 4.4 is a test table.
- No exported field, method or hook exposes `Container.mu`, a keyring, config
  secrets or an executor that accepts arbitrary Ceph arguments.
- Callers, docs, `CHANGELOG.md` fragment and the upgrade mapping land in the same
  pull request as the removal of the method.
- Native results are quoted from the exact head and attempt of that pull request.
