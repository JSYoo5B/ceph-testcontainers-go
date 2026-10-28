# Reducing the size of ceph.Container

This note is the design record that the parent issue (#146) requires before any
further extraction from `ceph.Container`. It re-measures the type after the
accessor, handle-operation and typed-flag changes (#143, #144, #145), proposes a
shape for each remaining group, and lists the dependencies, admission, state
and cleanup ownership that each extraction has to preserve. It changes no code.
Status: the override group has been extracted (see the group section below and
[override-package-boundary.md](override-package-boundary.md)), and the internal
state components of the first fifteen slices have been merged. Each component
has its own record under [container-decomposition/](container-decomposition/)
(see [Component records](#component-records)), and the residual-field record
([residual-fields.md](container-decomposition/residual-fields.md)) records the
disposition of every field that `Container` still declares.
The shapes of groups that are not marked as implemented are still proposals.

Every shape below that is not marked as implemented is a proposal. Each extraction still gets its own child issue
with exact signatures, call-site counts, caller migration, docs, `CHANGELOG.md`
and an upgrade-guide table. Moving any of these objects to a separate package
is decided in #155, #156 and #157 and is out of scope here. New objects live in
package `ceph` first, where they can reach unexported state.

## Measurement

Measured at the merge of #161 into `main` (`0a65b75`) and re-checked at `494d3bd`
(later `main`, same counts). The earlier columns are
the numbers quoted in the issue. The counts come from parsing the non-test files
of package `ceph` (method declarations whose receiver is `Container` or
`*Container`; fields of the struct) and from `wc -l`. They are a baseline for
review, not acceptance targets.

| Item | At `f61c222` (issue) | After #143 | At `0a65b75` |
| --- | --- | --- | --- |
| Exported methods declared on `*Container` | 99 | 94 | 73 |
| Unexported methods on `*Container` | 144 | 144 | 170 |
| Named struct fields | 58 | 58 | 57 (plus the embedded `testcontainers.Container`) |
| Non-test files in `ceph` | 76 | 76 | 79 |
| Non-test lines in `ceph` | about 22,500 | not recorded | 23,140 |
| `ceph.go` plus `topology.go` | more than 2,400 | not recorded | 2,340 (1,377 + 963) |
| `sharedGate("...")` call sites | 62 | not recorded | 38 |
| `admitHandle` / `admitFault` call expressions | not listed | not listed | 6 / 1 |
| `admitHandle` / `admitFault` occurrences including their declarations | not listed | not listed | 7 / 2 |
| Entries of `sharedMethodPolicies` / `sharedHandlePolicies` / `sharedFaultPolicies` | not listed | not listed | 93 / 25 / 4 |

After the override-kind slice of #146 (I-2 of the override package plan), measured
with the same parser on the base `main` and on the branch: named fields of
`Container` 45 to 42 (the three override maps are removed), unexported methods
175 to 176 (`allowPoolDeleteHeld`), exported methods 73 unchanged, `ceph.go`
1,357 to 1,354 lines. The table above is not rewritten: its absolute numbers
come from the earlier measurement.

After the override package migrations (the seven kinds of the override group left
`Container` for the package `override`) and the sixth internal slice (run directory state, see [Historical ordinals](#historical-ordinals)), measured at
`ba709ff` with the same parser (method declarations whose receiver is `Container`
or `*Container` in the non-test files of package `ceph`; fields of the struct;
`wc -l`; the embedded `testcontainers.Container` and the methods it promotes
are not counted). This is information only: there is no numeric target, and the
exported count falls for more than one reason (the facts and observation
extractions are included), so none of the change is attributed to a single group.

| Item | At `ba709ff` |
| --- | --- |
| Exported methods declared on `*Container` | 39 |
| Unexported methods on `*Container` | 189 |
| Named struct fields | 34 |
| Non-test files in `ceph` | 99 |
| Non-test lines in `ceph` | 23,561 |
| `sharedGate("...")` call sites | 31 |
| `admitHandle` / `admitFault` call expressions together | 9 |
| Entries of `sharedMethodPolicies` / `sharedHandlePolicies` / `sharedFaultPolicies` / `sharedOverridePolicies` | 59 / 49 / 4 / 7 |

Of the exported methods that left `Container`, the seven of the override group
(configuration, debug level, full ratios, MGR module, messenger policy, OSD flag
and blocklist) are the package functions of `override` now, and their policies
moved from `sharedMethodPolicies` to `sharedOverridePolicies`.

In the first table, the exported count equals the issue's expected total after
#145 (73). The unexported count grew by 26. The likely cause is that helpers stayed on
`Container` while the exported operations moved to handles; this was not
analyzed method by method (unconfirmed). The 62 versus 38 `sharedGate` call
sites may count different things, because the issue does not record how the 62
was obtained (unconfirmed); the handle and fault gates that replaced some of
them are listed separately above.

Remaining exported methods by role (73):

- Docker surface redefined for shared-cluster refusal (stays): `Start`, `Stop`,
  `Terminate`, `Exec`, `CopyDirToContainer`, `CopyFileFromContainer`,
  `CopyFileToContainer`, `CopyToContainer`, `TerminateOnCleanup`.
- Membership and creation (stays by principle 3): `AddManager`, `AddMonitor`,
  `AddOSD`, `AddOSDWithConfig`, `CreateClient`, `CreatePool`.
- Listing and raw access that return handles or containers: `Monitors`,
  `Managers`, `OSDs`, `Pool`, `Pools`, `ControlContainer`, `ManagerContainer`,
  `ClientContainer`, `ServiceContainers`, `Ceph`, `ConnectionConfig`.
- The three groups of the issue: 10 facts, 16 observations, 7 overrides.
- Operations that fit none of the above and are not yet placed (see
  [Not in the issue's tables](#not-in-the-issues-tables)): `CollectCrashMetadata`,
  `CollectDiagnostics`, `DeepScrubPG`, `RepairPG`, `RedactSecrets`,
  `RefreshClientMonitorConfig`, `RefreshMonitorConfig`, `RunNativeCommand`,
  `WaitForClean`, `WaitForPGClean`, `WaitForQuorum`, `WaitMGRModuleReady`,
  `WithClient`, `WithClientIdentity`.

## Dependencies that every extraction must keep

These come from reading the code at the commit above. Each extracted object
takes only what its group uses.

- **Topology lock.** `Container.mu` serializes topology work. Observations take
  it in two ways: `c.mu.Lock()` followed by `poolPolicyReady()` (`Configuration`,
  `BlocklistEntries`, `MGRModules`, `ObjectWatchers`, `OSDFlags`, `OSDStates`),
  or the context-aware `lockTopology(ctx)` (`FullRatios`, `HealthDetails`,
  `ManagerServices`, `MonitorBootstrapAddresses`, `PGInconsistencies`). `Status`,
  `DaemonAddresses` and `ManagerStatus` take neither and call `cephCLI` directly.
  The difference is observable (a wait that respects the caller's deadline versus
  one that does not), so the move keeps each method's own acquisition. Unifying
  them is a behavior change and a separate decision.
- **Liveness check.** `poolPolicyReady()` refuses a terminated cluster
  (`errClusterTerminated`) or one without a control container. `EssentialImage`
  documents that it remains available after termination, so facts must not use it.
- **Command execution.** Observations do not share one execution path. Audit of
  the observation files at `494d3bd` (the method names are those of that revision; the sixteen are now methods of `ceph.Observer`, obtained with `ceph.Observe` or `ceph.ObserveTenant`):

  | Path | Methods | What it needs |
  | --- | --- | --- |
  | `cephCLI` (selects the control container on every call through `controlContainerContext`, runs `ceph --connect-timeout 5`) | `Status`, `DaemonAddresses`, `ManagerServices`, `ManagerStatus`, `OSDFlags`, `OSDStates`, `Configuration`, `BlocklistEntries`, `MGRModules` | Control container lookup per call. Not gated. |
  | `controlContainerContext` then a query helper | `QuorumStatus` (`queryMonitorQuorum`); `MonitorBootstrapAddresses` (through `QuorumStatus`, the original bootstrap config for the FSID comparison, and `monitorClientRefreshContext`) | Control container and, for the second, the bootstrap identity. |
  | Captured control handle and captured bootstrap identity, queried directly | `HealthDetails` (FSID, health, FSID queries on one captured handle, with `configMu` read for the original FSID and keyring presence); `FullRatios` (through `fullRatiosSession`, which also serves `override.SetFullRatios`) | The control handle chosen once under the topology lock and never switched to a replacement, the original FSID, and the `closed` check. This fence is part of the behavior and must be preserved, not replaced by `cephCLI`. |
  | `clientContainerContext` then `rados` in that container | `PGInconsistencies` (`rados list-inconsistent-obj`), `ObjectWatchers` (`rados listwatchers`) | Client image role selection: the control container when the client image equals the essential image, otherwise the feature client under `clientMu` and `nativeMu` (refused after native commands are closed). Client lifecycle and configuration, not the control container. |
  | `ManagerServices` then `supervised.Run` in the control container | `ManagerMetrics` (python3 scrape of the advertised HTTP endpoint) | Supervised bounded execution (`managerMetricsTimeout`, `managerMetricsLimit`) plus the control container. |

  The extraction contract therefore needs, per method: the control lookup, the
  captured-control and identity fence, the client container lookup, and
  supervised execution, in addition to `cephCLI`. A `cephCLI`-only contract would
  drop collaborators or change which container runs the query. No override uses a
  path outside this list except `override.SetDebugLevel`, which runs admin socket
  commands through a native invocation and `supervised` (`override/debug_level.go`
  and the debug target of `ceph/debug_level.go`; not audited in detail,
  unconfirmed). Shared-cluster admission for observations is
  unaffected because they are `sharedOpen`; the executors themselves are not gated.
- **Admission.** `sharedMethodPolicies` is keyed by method name and checked by
  `TestSharedGateClassifiesEveryMethod`, which reflects over `*Container`, requires
  a policy for every method and calls every non-open method on a shared cluster
  to see it refused. `sharedHandlePolicies` and
  `TestSharedGateClassifiesEveryHandleOperation` apply the same scheme to the
  handle receivers through `sharedHandleReceivers`. `sharedFaultPolicies` covers
  the fault entry points. `sharedOverridePolicies` covers the seven `override`
  functions (keys `override.AddBlocklist` and so on) through
  `Container.admitOverride`, which `override` reaches by the admit hook. All 16
  observations and 10 facts are `sharedOpen`; all 7 overrides are
  `sharedExclusive`.
- **Fault registry.** `override.AddBlocklist`, `override.SetFullRatios`,
  `override.SetConfig`, `override.SetOSDFlag` and `override.SetMGRModule` reserve
  and settle entries in `Container.faults` (`reserveFault`, `settleFault`)
  through `internal/overridehook`, guarded by `mu`. All five kinds
  (`faultBlocklist`, `faultFullRatios`, `faultConfigOverride`, `faultOSDFlag`,
  `faultMGRModule`) have phase `faultTracked`; the last three replaced the
  former maps `configOverrides`, `flagOverrides` and `moduleOverrides`, with the
  configuration key, the flag token and the module name as keys.
  `override.SetDebugLevel` uses `debugLevels`, which has its own lock.
- **Override state.** The handles of `override` hold a registry token in their
  state, the owning `*ceph.Container` and run `Restore` against it through the
  hook; `Pool.Remove` asks the registry whether `mon_allow_pool_delete` is held
  (`allowPoolDeleteHeld`). The current lifetime of every kind is in the table
  under the overrides group below.
- **Messenger mode.** `override.SetConfig` refuses options fixed by the bootstrap
  policy (`settings.messengerMode`, through the `CheckConfigAllowed` hook).
  `override.SetMessengerPolicy` builds its result from `override.SetConfig` calls.

An interface does not authorize anything. A narrow executor interface that
receives a raw monitor or manager command could still issue broad administrative
commands, so each new entry point must call the same admission as the method it
replaces, and a new object must never hand out the underlying executor.

## Gate policy: keep the string-keyed tables

The issue asks whether the gate policy should attach to a component instead of
string-keyed calls. Evaluation of the options:

| Option | Effect | Assessment |
| --- | --- | --- |
| Keep string-keyed tables, add one table per kind of entry point | The tables already exist for methods, handles and fault operations. The tests enumerate receivers by reflection, so a new exported method without a policy fails a test, and the refusal test calls each non-open method on a shared cluster. | Keeps the coverage guarantee. Recommended. |
| Policy as a typed value passed at the call site (for example `c.admit(policyExclusive)`) | Removes string keys. The refusal error names the operation, which then has to be passed separately. | Loses the property that the test knows every method and its policy, because nothing enumerates the call sites. Not recommended without an equivalent enumerator. |
| Policy attached to each component type (an `admission()` method per object) | One policy per object instead of per method. | The groups are not uniform: facts and observations are open, overrides exclusive, `Pool` has both. Per-object policy would either be too coarse or collapse into the table. Not recommended. |

Consequence for the extractions: a new object's methods get entries in a table
keyed `Receiver.Method` (as `sharedHandlePolicies` does), the receiver is added
to the reflection map, and the refusal test runs against it. A package function
that takes the cluster gets an entry keyed by the function name and a test that
enumerates the exported functions of that group. Entries leave
`sharedMethodPolicies` in the same change that removes the method, so its
"classifies a method `*Container` does not have" check keeps passing.

## Proposed shape per group

### Static cluster facts (10 methods, no I/O)

`AddressFamily`, `CephVersion`, `ClusterNetworkName`, `EssentialImage`,
`HasSeparateClusterNetwork`, `Images`, `MessengerMode`, `NetworkName`,
`PublicAddress`, `UsesHostNetwork`.

- **Shape.** An immutable value returned by one package function that takes the
  cluster, for example `ceph.FactsOf(c) ClusterFacts` (name undecided). No
  accessor on `Container`.
- **Dependencies actually consumed.** `settings` (address family, essential image,
  image set, messenger mode, host network, public address), `network` and
  `clusterNetwork` names, and `cephVersion`. No lock, no command, no Docker call.
- **Admission.** None: all ten are `sharedOpen`.
- **State and lock owner.** The value is a copy taken when called. It never aliases
  `Container` state, so a later `Terminate` does not change a value already held.
- **Cleanup lifetime.** None. `EssentialImage` and `Images` must keep working after
  termination, as documented today.
- **Nil handling.** Today `AddressFamily`, `EssentialImage`, `Images`, `CephVersion`
  and `MessengerMode` accept a nil `*Container`; `NetworkName`, `UsesHostNetwork`,
  `PublicAddress`, `ClusterNetworkName` and `HasSeparateClusterNetwork` dereference
  it and would panic (read from the code, not run). The child issue must decide the
  nil behavior of the function explicitly rather than inherit both.
- **Open questions.** `cephVersion` is written once during `Run` (`version.go`); whether
  any reader can observe it before that write was not checked (unconfirmed).
  Whether the facts value should be a struct with fields or keep method form on the
  value (`facts.NetworkName()`) affects the 262 textual call-site lines that mention
  these names (an upper bound: the search is by method name, not receiver type).
- **Status.** `ceph.FactsOf` and `ClusterFacts` serve the ten facts, and the
  change `146-static-facts-accessors` deleted the ten `Container` accessors and
  moved every caller to `FactsOf`. The 0.x line keeps no alias. All ten were
  purely static (no lock, command or Docker call), so none stayed. Code inside
  package `ceph` reads the same recorded sources through the unexported
  `staticFacts`, because it holds a cluster that may still be starting and has
  no use for the nil-cluster error.
- **Size effect.** `Container` exported methods 73 to 63. First extraction
  recommended, because it has no lock, no gate and no cleanup, and it clarifies
  which `settings` fields are public facts before the networking fields move.

### Read-only observation snapshots (16 methods)

> Done: the sixteen methods are on `ceph.Observer` only; the `Container` methods and the eight `Tenant` forwarders are removed, and `Pools` became `Observer.Pools`, which lists `*ceph.Pool` objects. The `Observer` stays in package `ceph` because it needs the captured control handle, the topology and configuration locks and the shared-cluster admission, which are internal. The text below is the design as proposed.

`BlocklistEntries`, `Configuration`, `DaemonAddresses`, `FullRatios`,
`HealthDetails`, `MGRModules`, `ManagerMetrics`, `ManagerServices`,
`ManagerStatus`, `MonitorBootstrapAddresses`, `OSDFlags`, `OSDStates`,
`ObjectWatchers`, `PGInconsistencies`, `QuorumStatus`, `Status`.

- **Shape.** One object built from a cluster or from a tenant, for example
  `ceph.Observe(c)` and `ceph.ObserveTenant(t)` returning the same type (names
  undecided). Its methods carry the same names and results as today.
- **Dependencies actually consumed.** Not one `cephCLI` contract. The execution
  audit under "Dependencies that every extraction must keep" lists five paths:
  `cephCLI`, a control lookup with a query helper, a captured control handle with
  the captured bootstrap identity (`HealthDetails`, `FullRatios`), a client
  container lookup running `rados` (`PGInconsistencies`, `ObjectWatchers`), and
  supervised execution (`ManagerMetrics`). Besides execution the contract has the
  topology lock in its two forms, the liveness check, the owned OSD set
  (`OSDStates`) and `configMu` with the config bytes (`HealthDetails`,
  `FullRatios`). The contract is split by what each method needs, so `Status`
  does not receive the lock it never takes, and the captured-control and identity
  fence of `HealthDetails` and `FullRatios` is carried over as it is.
  Which of the test seams that are fields today (`osdRunner` and the like) these
  methods reach was not inventoried (unconfirmed); the child issue lists the
  doubles that move with them.
- **Admission.** All `sharedOpen`. The tenant constructor keeps `Tenant.check()`
  (released or terminated tenant) on every call, as the eight forwarders do now.
- **State and lock owner.** The object holds a reference and no state of its own.
  The lock stays `Container.mu`; splitting it is a separate step.
- **Cleanup lifetime.** None. A call after termination keeps returning the same
  errors as today because the liveness check moves with the method.
- **Tenant effect.** Removes the eight one-by-one forwarders in `tenant.go`
  (`BlocklistEntries`, `HealthDetails`, `OSDFlags`, `ObjectWatchers`, `Status`,
  `Pools`, `PoolStatus`, `PoolUsage`) for the methods that move. `Pools`,
  `PoolStatus` and `PoolUsage` belong to the pool placement of #144 and need an
  explicit decision on whether they join this object.
- **Open questions.** Whether `QuorumStatus` and `ManagerStatus` (used by topology
  waits inside the package) keep an unexported twin on `Container`. Counted by
  name, `Status` appears on 277 lines of Go in 97 files, but many belong to other
  types, so the real migration size must be measured by receiver type in the child
  issue.
- **Size effect.** `Container` exported methods 63 to 47 after facts; the `Tenant`
  forwarders shrink accordingly.

### Cluster-wide reversible overrides (7 methods, implemented in `override`)

Implemented. The seven former `Container` methods are the package functions
`override.SetConfig`, `override.SetDebugLevel`, `override.SetFullRatios`,
`override.SetMGRModule`, `override.SetMessengerPolicy`, `override.SetOSDFlag` and
`override.AddBlocklist`, each returning a non-nil `*override.*Handle`. 0.x: old
names are removed without aliases. The decisions, contract and migration order
are in [override-package-boundary.md](override-package-boundary.md); the public
reference is [override.md](../override.md).

- **Shape (as built).** A public package `override` takes the cluster as the
  first argument (option A of the design: the handles and their `Restore` moved
  with the entry points). `ceph` never imports `override`; the package reaches
  `ceph` internals only through `internal/overridehook`, which `ceph` registers
  in `internal/hookreg` (`ceph/override_hooks.go`).
- **Dependencies actually consumed.** Admission, `mu` (or `lockTopology`), the
  liveness check, `faults` (the five tracked kinds), `debugLevels`
  (`override.SetDebugLevel`), `settings` (messenger mode, startup timeout) and
  `cephCLI`, each through a hook method.
- **Admission.** All `sharedExclusive`, checked before any effect through
  `sharedOverridePolicies`. `override/admission_test.go` parses the package
  source with `go/parser`, enumerates every exported top-level function and
  requires a policy for it (`TestEveryExportedFunctionIsClassified`), and calls
  each with zero arguments on a shared cluster to require `ErrSharedCluster`
  before any effect, so a new override cannot skip the gate; an import test
  pins that `override` imports only the permitted packages.
- **State and lock owner.** The registry kinds and `debugLevels` stay on
  `Container`; the three former maps are registry kinds.
- **Cleanup and restore lifetime (preserved unchanged by the move).**
  `Container.terminate` sets `closed` and calls `undoFaults`, which visits only
  the faults in the shaping, endpoint and freeze phases (network delays,
  interruptions, pauses). It never visits the tracked kinds (the five override
  kinds). The extraction added no automatic restoration and no second ownership
  path.

  | Kind | Who restores | After `Terminate` (read from the code) |
  | --- | --- | --- |
  | `override.SetConfig` (fault kind `faultConfigOverride`, phase `faultTracked`) | The caller, through `ConfigHandle.Restore` | `Restore` checks readiness, so it returns `errClusterTerminated` on a closed cluster. |
  | `override.SetMessengerPolicy` | The caller, through `MessengerPolicyHandle.Restore`, which restores its `ConfigHandle` values in reverse order | Same as `override.SetConfig`, because it is built from those handles. |
  | `override.SetOSDFlag` (fault kind `faultOSDFlag`, phase `faultTracked`) | The caller, through `Restore` | `Restore` checks readiness: `errClusterTerminated`. |
  | `override.SetMGRModule` (fault kind `faultMGRModule`, phase `faultTracked`) | The caller, through `Restore` | `Restore` checks readiness: `errClusterTerminated`. |
  | `override.AddBlocklist` (fault kind `faultBlocklist`, phase `faultTracked`) | The caller, through `BlocklistHandle.Restore` | Tracked kinds are bookkeeping only and `terminate` leaves them alone (`fault_registry.go`). `Restore` checks readiness: `errClusterTerminated`. |
  | `override.SetFullRatios` (fault kind `faultFullRatios`, phase `faultTracked`) | The caller, through `FullRatiosHandle.Restore` | Same bookkeeping rule. `Restore` goes through `fullRatiosSession`, which refuses a closed cluster. |
  | `override.SetDebugLevel` (`debugLevels`) | The caller, typically registered with `Finalization.TrackDebugLevel` and `Finalization.Cleanup` | Its `Restore` is driven by the handle's own state and generation checks; after `Terminate` the result is pinned by the window tests of `override`. |

  In every kind the handle is the only restoration path, and the cluster's
  termination ends the native state with the cluster rather than restoring it.
- **Not-applied handle.** A refused or never-applied call returns a handle whose
  `Restore` is a no-op; an uncertain outcome returns a real handle with the
  error. `Finalization.TrackDebugLevel` takes the narrow `ceph.DebugLevelReceipter`
  so that `ceph` does not import `override`.
- **Open questions (decided).** `override.SetMessengerPolicy` is in the group and
  calls `override.SetConfig`; `override.AddBlocklist` is in the group (the design
  note records both decisions).
- **Size effect.** Seven exported methods left `Container`. The measured
  exported count is in the Measurement section; it is information, not a target.

### Not in the issue's tables

The issue's size table quoted about 40 exported methods after the three groups.
The owner decided later that this figure is a qualitative indicator only and
that there is no method-count target. The remaining methods listed under
"Operations that fit none of the above" were not assigned a shape there; none
is proposed as a commitment here.

| Candidates | Observation |
| --- | --- |
| `WaitForClean`, `WaitForPGClean`, `WaitForQuorum`, `WaitMGRModuleReady` | Waits that poll observations. They could become methods of the observation object or stay as lifecycle helpers. |
| `DeepScrubPG`, `RepairPG` | PG operations. Principle 1 suggests a PG handle, which does not exist yet. |
| `RefreshMonitorConfig`, `RefreshClientMonitorConfig` | Client and monitor configuration refresh; related to the client and auth state split. |
| `CollectCrashMetadata`, `CollectDiagnostics`, `RedactSecrets` | Diagnostics group; `sharedOpen` today. Post-termination representation is compared in [post-termination-diagnostics.md](post-termination-diagnostics.md). |
| `RunNativeCommand`, `WithClient`, `WithClientIdentity` | Client and native-command surface that `Tenant` mirrors; tied to #155. |

These methods are what remains on `Container` besides the Docker surface,
membership and the listing methods; the measurement section records the count
as information.

## Internal state and lock split

The internal split moves fields and unexported methods without changing the
exported API. It is a separate step from moving exported methods, and splitting
a lock needs its own review of lock order. The current fields group as follows
(57 named fields). Counts are indicative; whether a field belongs to exactly one
component was not verified for every method that touches it.

| Component | Fields (current names) |
| --- | --- |
| Client and auth | `authFences`, `clientSecrets`, `clientKeys`, `strayAuthInputs`, `clientRevocations`, `clientMu`, `clientPublishMu`, `featureClient`, `featureClientReady`, `featureClientAttempted` |
| Native commands | `nativeMu`, `nativeInvocations`, `nativeClosed`, `nativeFenced`, `clientStartupCancel` |
| Networking | `network`, `clusterNetwork`, `publicSubnet`, `clusterSubnet`, `clusterNetworkRemoved`, `networkRemoved` |
| OSD storage | `osds`, `detachedOSDs`, `osdMemory`, `osdVolume`, `run` |
| Overrides and faults | `faults`, `debugLevels` |
| Test seams | `keyRotationDocker`, `ownedContainerDocker`, `osdRunner`, `osdVolumeDocker`, `osdVolumeRunner` |
| Membership and control | `mu`, `configMu`, `controlPlane` (a `controlPlaneState` since the eleventh slice, see [Historical ordinals](#historical-ordinals), holding the former `controlMu`), `manager`, `monitors`, `managers`, `services`, `extensionsMu`, `extensions`, `config`, `keyring`, `portLeases`, `closed`, `monitorTerminated`, `firstMonitorImageID`, `cephVersion`, `owner`, `keyTypeOption`, `settings`, `sharing` |

Rules for every field move:

- Lock acquisition order and the topology gate order stay as they are. A move that
  needs a different order is not part of the structural step.
- Test seams move with the dependency that uses them, so a unit test replaces a
  component's collaborator and not a field of the whole cluster.
- `Terminate` still reaches every component, so the order in which components are
  closed is recorded in the child issue and checked against the current order.

## Order of extractions

1. Facts, because it has no lock, no gate and no cleanup, and the value type
   fixes which settings are public.
2. Observations, which also removes the `Tenant` forwarders. The narrow command
   and lock contracts are introduced here and reused by the overrides.
3. Overrides, which depend on the contract from step 2 and on a decision about
   where the maps and registries live.
4. Internal split, component by component (networking first, since facts
   depend on it), each with its own lock-order review.

Each step proposes its exact exported signatures in its own child issue before
code changes. The override group of step 3 and six components of step 4 (networking,
native commands, feature client, auth records, OSD storage and the run
directory) are implemented; this note does not restate the status of steps 1
and 2. This note claims no
size target.

## Component records

Each internal state component has its own record, one Markdown file per
component under [container-decomposition/](container-decomposition/). This
document keeps the common principles, the measurements and the
cross-component constraints above; a component record keeps that component's
audit, lock-order review, guards and limits.

- **Discovery.** Records are found by reading the directory. No file lists the
  records that exist or will exist, counts them or aggregates them, so a
  component change adds or edits only its own record and never this document.
  (The table under [Historical ordinals](#historical-ordinals) is a closed
  account of the sixteen former sections, not a registry.) The Docker-free guards in
  `ceph/container_decomposition_records_test.go` read the directory; they do not
  consult a list.
- **Names.** A record is `<component>.md`: lowercase `a-z`, `0-9` and `-`, named
  after the component and not after a position in a queue. An entry that is not a
  regular file with such a name (a README, a backup, a subdirectory) is refused
  by the guard and never skipped. A record opens with a level-one title, and
  neither a name nor a heading carries an ordinal such as "Fourteenth slice".
- **Order and dependencies.** Neither the directory order nor the file names
  define the order of extractions or of implementation. A record states in its
  own text what it depends on, when that matters. The order that this document
  fixes is in [Order of extractions](#order-of-extractions).
- **Citations.** A source comment or a link that names a record by path must
  resolve; the guard fails when a cited record is removed or renamed.
- **Shared edits that stay.** A change that adds, moves or removes a field of
  `Container` still edits the Go ledger and the table in
  [residual-fields.md](container-decomposition/residual-fields.md), and a change
  to a shared principle or measurement still edits this document. Splitting
  the records removes only the shared append point, not genuine cross-component
  coordination.

## Historical ordinals

Before the records moved to the directory, this document held them as sections
named by ordinal ("First internal slice" and so on) in the order they merged.
The ordinals appear inside the moved text, for example in "as written in the
second slice", and in earlier documents. They are historical context only:
this table is closed, it is not extended, and a new component gets no ordinal.

| Former section | Record |
| --- | --- |
| First internal slice | [networking-state.md](container-decomposition/networking-state.md) |
| Second internal slice | [native-command-state.md](container-decomposition/native-command-state.md) |
| Third internal slice | [feature-client-state.md](container-decomposition/feature-client-state.md) |
| Fourth internal slice | [auth-records.md](container-decomposition/auth-records.md) |
| Fifth internal slice | [osd-storage-state.md](container-decomposition/osd-storage-state.md) |
| Sixth internal slice | [run-directory-state.md](container-decomposition/run-directory-state.md) |
| Seventh internal slice | [osd-handle-registry.md](container-decomposition/osd-handle-registry.md) |
| Eighth internal slice | [host-port-lease-state.md](container-decomposition/host-port-lease-state.md) |
| Ninth internal slice | [membership-control-test-hooks.md](container-decomposition/membership-control-test-hooks.md) |
| Tenth internal slice | [fault-registry.md](container-decomposition/fault-registry.md) |
| Eleventh internal slice | [control-plane-state.md](container-decomposition/control-plane-state.md) |
| Twelfth internal slice | [bootstrap-record.md](container-decomposition/bootstrap-record.md) |
| Thirteenth internal slice | [extension-store.md](container-decomposition/extension-store.md) |
| Fourteenth internal slice | [member-roster.md](container-decomposition/member-roster.md) |
| Fifteenth internal slice | [startup-identity.md](container-decomposition/startup-identity.md) |
| Sixteenth internal slice | [residual-fields.md](container-decomposition/residual-fields.md) |

## Limits of this note

- Source reading and a static parse of method declarations only. No native
  Ceph, Docker or timing behavior was run or measured for it.
- Call-site counts are textual (by method name) and overstate the number of
  `Container` call sites.
- go-ceph is referenced in the issue as a source comparison for dependency
  decomposition. It does not prove this module's lock order, restoration or
  Docker-generation safety, and no go-ceph dependency is introduced.
