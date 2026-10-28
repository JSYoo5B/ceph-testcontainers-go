# Reducing the size of ceph.Container

This note is the design record that the parent issue (#146) requires before any
further extraction from `ceph.Container`. It re-measures the type after the
accessor, handle-operation and typed-flag changes (#143, #144, #145), proposes a
shape for each remaining group, and lists the dependencies, admission, state
and cleanup ownership that each extraction has to preserve. It changes no code.
Status: the override group has been extracted (see the group section below and
[override-package-boundary.md](override-package-boundary.md)), and six internal
state components have been merged (networking, native commands, feature client,
auth records, OSD storage and the run directory; see the slices below). The
other shapes are still proposals.

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
`Container` for the package `override`) and the sixth internal slice, measured at
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
| Membership and control | `mu`, `configMu`, `controlPlane` (a `controlPlaneState` since the eleventh slice, holding the former `controlMu`), `manager`, `monitors`, `managers`, `services`, `extensionsMu`, `extensions`, `config`, `keyring`, `portLeases`, `closed`, `monitorTerminated`, `firstMonitorImageID`, `cephVersion`, `owner`, `keyTypeOption`, `settings`, `sharing` |

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

## First internal slice: networking state

Inspected at `8335f47` (main after #238). The first component moved out of
`Container` is the Docker bridge state, because the order above puts networking
first and because it is the smallest group with a single writer per field.

Moved: `network`, `clusterNetwork`, `publicSubnet`, `clusterSubnet`,
`networkRemoved` and `clusterNetworkRemoved` become the unexported
`networkState` (`ceph/network_state.go`), held as `Container.nets`. The
removal block of `terminate` becomes `networkState.removeBridges`, and the
duplicated "list the bridges" loops in diagnostics and native-command admission
become `bridges` and `bridgeNames`. All `Container` methods, their signatures
and their behavior are unchanged.

Why this slice first, and why it adds no mutex:

- Its fields have two kinds of access. The four recorded fields are written
  only while `Run` builds the cluster (`ceph/ceph.go:220-236`) and are read
  without a lock by `FactsOf` (`ceph/cluster_facts.go:47-60`), which
  #238 pinned with an AST write-site guard. The two removal flags are touched
  only by the removal block of `terminate` (`ceph/ceph.go:1152-1165`).
- A new mutex would add a lock to the order and would break the lock-free
  `FactsOf` reads that the merged tests depend on. The first slice therefore
  keeps the existing guards and writes them down on the type.

Lock-order rules preserved, unchanged by this move:

- `terminate` calls `cancelNativeInvocations` before waiting for the mutex,
  takes `lockTopology` (`ceph/ceph.go:1020` at `8335f47`, `1016` at `134a752`) and holds `c.mu` until it
  returns, with one exception: the fault undo phases
  (`ceph/fault_registry.go:193-216`) release `c.mu` around each
  network-shaping restore. `c.mu` is therefore not held continuously through
  teardown. The undo phases have returned, and `c.mu` is held again, when the
  bridges are removed, after every owned container is gone. The removal order
  is the replication bridge first, then the public bridge, and a bridge named
  by a failed interruption undo stays and is retried by the next `Terminate`.
- The removal flags are guarded by `Container.mu`, which `removeBridges` does
  not release. The recorded fields need no lock. `TestNetworkStateRemovalFlagsHaveOneWriter` and the extended
  `TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup` fail when
  another function writes them, when a method of `networkState` writes a
  recorded field, or when `terminate` calls `removeBridges` before it takes
  `lockTopology`.

Not covered by this slice: the other components of the table above (client and
auth, native commands, OSD storage, overrides and faults, test seams,
membership and control), any change to which lock guards what, any new lock,
the `sharedBackend` port that #155 needs (`containerBackend` does not use the
network fields), and any exported API. The `Terminate` order of the other
components stays as recorded in the code and is not re-reviewed here.

## Second internal slice: native-command state

Inspected at `134a752` (main after #239). The second component moved out of
`Container` is the native-command state, held as `Container.nativeCmds` of the
unexported type `nativeState` (`ceph/native_state.go`): the tracked invocations
(`nativeInvocations`), whether Terminate closed admission (`nativeClosed`), the
fenced OSD instances (`nativeFenced`) and the cancel function of a client-image
startup (`clientStartupCancel`). The lock moves with them: `nativeMu` becomes
`nativeState.mu`. All `Container` methods, their signatures and their behavior
are unchanged.

Why this component next, and why no new mutex:

- The table above names no order after networking. Of the remaining
  components, this one has a single, already documented lock: every
  production access to the five fields, all in `ceph/native_command.go`,
  `ceph/images.go` and `ceph/debug_level.go`, happened inside a
  `nativeMu` critical section, and `ceph/ceph.go` only declared them. Client
  and auth uses `clientMu`, `clientPublishMu` and maps whose guards were not
  audited for this note, and OSD storage is tied to the OSD lifecycle under
  `Container.mu`; both stay for later slices. Moving `nativeMu` with its
  fields keeps one lock for the same data, so the number of mutexes stays the
  same and no lock is added to the order.
- `nativeMu` is already a leaf lock. Every critical section is a few map or
  flag operations (`ceph/native_command.go:430-447`, `469-475`, `522-540`,
  `664-668`, `697-708`, `738-740`, `ceph/images.go:151-153`, `164-172`,
  `194-196`, `ceph/debug_level.go:646-669`, `678-680`); cancellations and
  stops run after the unlock. The component's methods preserve that and take
  `mu` exactly once.

Lock-order rules preserved, unchanged by this move:

- `terminate` calls `cancelNativeInvocations` (`ceph/ceph.go:1015`) before it
  waits for `c.mu` (`ceph/ceph.go:1016`), because an invocation's caller may
  hold `c.mu`. It calls `stopNativeInvocations` twice with `c.mu` held
  (`ceph/ceph.go:1031`, `1149`); `nativeMu` is below `c.mu` there.
- `fenceNativeTarget` is called from OSD removal (`ceph/ceph.go:813`,
  `ceph/osd_retained.go:239`) under the topology lock and takes `nativeMu`
  inside it. The client startup takes `clientMu` and then `nativeMu`
  (`ceph/images.go:146-151`). Nothing takes another lock while holding
  `nativeMu`, so the order `c.mu` or `clientMu`, then `nativeMu` is the only
  one that exists.
- Admission checks stay in one critical section and in the same order for
  the three callers: closed, then fenced, then (for `RunNativeCommand` with an
  input) the one-shot input claim. The refusals and their error text are the
  callers' own. An owned shell has no target instance, which no fence can
  name, so the shared fence check never refuses it.
- An unconfirmed end keeps the invocation registered with its marker, also
  when the marker is empty, as before.

Guards, in `ceph/native_state_test.go`:

- `TestNativeStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.nativeCmds` appears only as the receiver of a `nativeState`
  method, so no code touches a field or the lock directly.
- `TestNativeStateMethodsHoldOnlyTheLeafLock`: each method takes `n.mu`
  exactly once and calls nothing else while it runs but the claim hook,
  builtins and two collection helpers; the method set is pinned.
- `TestNativeStateAddsNoMutex`: `Container`, `nativeState` and `networkState`
  declare the same seven mutexes as before the split.
- `TestTerminateCancelsNativeInvocationsBeforeTheTopologyLock`.
- Behavior tests for the admission order, the unconfirmed end, the close and
  fence results, and a concurrent run under `-race`.

The `FactsOf` write-site guard is untouched: no field moved here is read by
`FactsOf`.

Not covered by this slice: client and auth, OSD storage, overrides and faults,
test seams and membership and control; any change to which lock guards what;
any new lock; the order in which `Terminate` closes the other components; the
`sharedBackend` port that #155 needs; and any exported API.

## Third internal slice: feature-client state

Inspected at `3dc0d13` (main after #242); path:line citations in this section
are at that revision unless they name the new file. The third component moved
out of `Container` is the cache of the lazily started feature client, held as
`Container.toolkit` of the unexported type `featureClientState`
(`ceph/feature_client_state.go`): the published client container (`featureClient`),
whether a startup began (`featureClientAttempted`) and whether it succeeded
(`featureClientReady`). The two locks move with them: `clientMu` becomes
`featureClientState.mu` and `clientPublishMu` becomes
`featureClientState.publishMu`. All `Container` methods, their signatures and
their behavior are unchanged.

The table under "Internal state and lock split" lists these fields in the
"Client and auth" row. The audit below splits that row in two. The feature-client
cache is moved here. The auth records (`authFences`, `clientSecrets`,
`clientKeys`, `strayAuthInputs`, `clientRevocations`, `keyRotationDocker`) stay
on `Container` and are in the audit table of the components not picked.

### Audit of the moved fields

Every production access to the three fields and the two locks, at `3dc0d13`:

| Field | Writers | Readers | Guard |
| --- | --- | --- | --- |
| `featureClient` | `clientContainerContext` publishes the started or partially started container (`ceph/images.go:171-173`); `terminate` clears it after a successful removal (`ceph/ceph.go:1114-1116`) | `clientContainerContext` returns it when a startup was attempted (`ceph/images.go:158`); `terminate` removes it (`ceph/ceph.go:1110-1111`); `featureClientSnapshot` (`ceph/images.go:198-204`), used by `monitorConfigTargets` (`ceph/topology.go:513`) and the native-command fence check (`ceph/native_command.go:626`); `diagnosticsSnapshot` (`ceph/diagnostics.go:277-283`) | Every writer holds `clientMu` and `clientPublishMu`. The readers in `clientContainerContext` and `terminate` hold `clientMu`; the snapshot readers hold `clientPublishMu` for reading. |
| `featureClientAttempted` | `clientContainerContext` (`ceph/images.go:166`) | `clientContainerContext` (`ceph/images.go:154`) | `clientMu` |
| `featureClientReady` | `clientContainerContext` (`ceph/images.go:193`); `terminate` (`ceph/ceph.go:1117`) | `clientContainerContext` (`ceph/images.go:155`) | `clientMu` |

Tests access the fields and `clientPublishMu` directly (`ceph/images_test.go`,
`ceph/monitor_config_test.go`); they moved to `Container.toolkit`.

### Why this component next, and why no new mutex

- It has exactly two locks, both existing, and no access outside three
  production files (`ceph/ceph.go`, `ceph/images.go`, `ceph/diagnostics.go`
  plus the two snapshot callers). Moving each lock with the fields it guards
  keeps the number of mutexes at seven and adds nothing to the order.
- Nothing here relies on a caller holding a lock: `clientContainerContext`,
  `terminate`, `featureClientSnapshot` and the diagnostics read each take the
  lock they need themselves. The auth records rely on a doc-comment contract
  and the override maps on an open decision (see the table below), so this
  guard can be checked structurally and theirs cannot yet.
- `clientMu` is not a leaf: it is held across the startup of a container
  (`ceph/images.go:147-148` to the end of the function). The component
  therefore exposes `lock` and `unlock` instead of wrapping the critical
  section, and the methods that need it are marked "mu held". Wrapping the
  startup inside one method would have moved container startup logic, which
  this slice does not do.

### Lock order preserved

- `Container.mu`, then `featureClientState.mu`: `ClientContainer` takes the
  topology lock first when the client image differs from the essential image
  (`ceph/images.go:127`) and then calls `clientContainerContext`; `terminate`
  holds the topology lock from `ceph/ceph.go:1010` and takes `clientMu` at
  `ceph/ceph.go:1107`, after the control container (`ceph/ceph.go:1098`) and
  before the removal of labeled containers (`ceph/ceph.go:1123`). The new code
  keeps these positions.
- Nothing that holds the startup lock takes `Container.mu`. Reading the calls
  in `clientContainerContext` between `ceph/images.go:147` and the end of the
  function, they reach `nativeState` (leaf), `configMu` read in `adminClient`
  (`ceph/ceph.go:505-507`), the lock-free accessors `Images`, `CephVersion` and
  `NetworkName`, and `testcontainers.Run`. This is source reading, not a
  proof for callees the guard does not follow. Service callers that reach
  `clientContainerContext` may already hold `Container.mu` (`ceph/service_hooks.go:41`,
  `ceph/observation_rados.go:59`), which is the same order.
- `publishMu` and `nativeState.mu` are leaves below `mu`. `publishMu` is held
  only around a pointer read or write.
- The client startup takes `mu` and then `nativeState.mu`
  (`ceph/images.go:147-161`), as written in the second slice.
- `terminate` removes the feature client only after the control container,
  and it keeps a client whose removal failed published and not ready, so the
  next `Terminate` retries it. `attempted` stays set, so a terminated cluster
  starts no second client.

### Guards, in `ceph/feature_client_state_test.go`

- `TestFeatureClientStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.toolkit` appears only as the receiver of a `featureClientState`
  method.
- `TestFeatureClientStateMethodsMakeOnlyTheirPinnedCalls`: the method set is
  pinned and each method makes exactly its listed calls, so `publishMu` stays
  a leaf, `mu` is taken only by `lock` and no method calls into `Container`.
- `TestFeatureClientStateCallersHoldTheStartupLockInOrder`: a function that
  uses a "mu held" method has called `toolkit.lock` before and `toolkit.unlock`
  after, takes `Container.mu` never once it holds the startup lock, and
  `terminate` takes the topology lock first. Only `terminate` and
  `clientContainerContext` may hold the startup lock.
- `TestNativeStateAddsNoMutex` now counts `featureClientState` too and still
  expects seven.
- Behavior tests: the lifecycle, the lock honoring the caller's context,
  readers that do not wait for a startup, the bounded diagnostics wait, a
  concurrent run under `-race`, and a real `Terminate` of a fixture that keeps a
  client whose removal failed and removes it on retry.

The `FactsOf` write-site guard is untouched: no field moved here is read by
`FactsOf`.

### Components not picked: audit for the next slice

Fields, writers, readers and locks as the code states them at `3dc0d13`. "By
contract" means the lock is named in a doc comment ("The caller holds c.mu")
and the caller chain was not checked here.

| Component | Fields | Writers | Readers | Locks | Why not now |
| --- | --- | --- | --- | --- | --- |
| Auth records | `authFences`, `clientSecrets`, `clientKeys`, `strayAuthInputs`, `clientRevocations`; seam `keyRotationDocker` | `settleClient` (`ceph/client_key.go:136,140`), `fenceAuth` (`ceph/daemon_key_rotation.go:369,374`), `settleEarlierRotation` (`:439`), `setClientKey` (`ceph/client_key.go:312-318`), `recordClientSecret` (`:330-332`), `strayAuthInput` (`ceph/daemon_key_rotation.go:152-154`), `createClient`/`deleteClient`/`revokeClient`/`ClientRevocationHandle.Restore` (`ceph/auth.go:306`, `ceph/client_revocation.go:59-69,107`) | The same files, plus `diagnosticsSnapshot` for `clientSecrets` (`ceph/diagnostics.go:263`, between the `TryLock` at `:219` and the unlock at `:267`) | `Container.mu`. The revocation code takes it itself (`lockTopology` before every access in `ceph/client_revocation.go` and `ceph/auth.go`). The fence and key helpers rely on a doc comment (`ceph/client_key.go:109,309,324`, `ceph/daemon_key_rotation.go:34,149,159`); `settleClient` alone has eight callers in five files (`ceph/auth.go:162,291`, `ceph/client_key.go:202,273`, `ceph/client_revocation.go:93`, `ceph/shared_backend.go:396,444`, `ceph/service_hooks.go:377`) | Guard by contract across five files and another object (`client.owner.setClientKey`, `ceph/service_hooks.go:368`). `authFences` serves both clients and daemons, so the rotation code moves with it. Needs a per-caller lock audit first. |
| OSD storage | `osds`, `detachedOSDs`, `osdMemory`, `osdVolume`, `run`; seams `osdRunner`, `osdVolumeDocker`, `osdVolumeRunner` | `osds`: `registerOSD` (`ceph/ceph.go:659`), `removeOSD` (`:863`), `terminate` (`:1046`), `detachOSD` (`ceph/osd_retained.go:274`), `reopenOSD` (`:322`), `serviceHooks.TestCluster` (`ceph/service_hooks.go:429,431`). `osdMemory`/`osdVolume`: `ensureOSDMemory` (`ceph/osd_memory_storage.go:109`), `ensureOSDVolume` (`ceph/osd_volume_storage.go:97`), `terminate` (`ceph/ceph.go:1051,1057,1128,1133`). `run`: once in `Run` (`ceph/ceph.go:234`) | `osds` is read in about twenty files (a name match that also finds the options field of the same name); `osdMemory`/`osdVolume` in `registerOSD`, `removeOSD`, `newOSDLaunchRecipe` (`ceph/osd_launch.go:47-54`) and `detachOSD`/`reopenOSD`; `run` in `ceph/admin_socket.go`, `ceph/debug_level.go:633`, `ceph/osd_retained.go:345`, `terminate` | `Container.mu` for the lifecycle writers (`removeOSD`, `detachOSD`, `reopenOSD`, `terminate` take `lockTopology`); `registerOSD` and `ensureOSD*` take no lock themselves (the caller chain was not checked); `newOSDLaunchRecipe` reads `run`, `osdMemory` and `osdVolume` (`ceph/osd_launch.go:47-54`) after its `configMu` read section and takes no other lock | `terminate` releases memory and volume in two phases around the container and labeled-container removal (`ceph/ceph.go:1049-1057`, `1126-1133`), so the close order of the component has to be reviewed. Largest reader set. Open PR #244 changes `ceph/osd_launch.go` and the `registerOSD` area. |
| Overrides and faults | `faults`; `debugLevels` | the configuration, OSD flag and MGR module overrides (at that revision methods in `ceph/config.go`, `ceph/osd_policy.go` and `ceph/mgr_modules.go`; now `override.SetConfig`, `override.SetOSDFlag` and `override.SetMGRModule`) reserve and settle their kind through `reserveFault` and `settleFault`; each handle's `finish` calls `releaseFault`. The former maps `configOverrides`, `flagOverrides` and `moduleOverrides` are gone | The same functions, `Restore` (`tracksFault`) and `deletePool` through `allowPoolDeleteHeld` (`ceph/pool_removal.go`, "called with c.mu held") | All of it under `Container.mu`, taken in every entry point and `Restore`; the registry guards by `Container.mu` by contract (`ceph/fault_registry.go`) and has outside users (`ceph/fault_hooks.go`, `ceph/osd_retained.go`). `debugLevels`: its own mutex (`ceph/debug_level.go`) | Nothing is left to move out of `Container` for the three override kinds: the registry is the one tracking record, as [override-package-boundary.md](override-package-boundary.md) section 6.1 decided, and it is already its own type. `faults` and `debugLevels` are already their own types; moving the field adds nothing. |
| Test seams | `keyRotationDocker`, `ownedContainerDocker`, `osdRunner`, `osdVolumeDocker`, `osdVolumeRunner` | None in production code; tests assign them | One read each at the point of use (`ceph/daemon_key_rotation.go:730-731`, `ceph/owned_containers.go:35`, `ceph/ceph.go:713`, `ceph/osd_volume_storage.go:96,101`) | None | The rule above moves a seam with the dependency that uses it: three belong to OSD storage, one to the auth records, and `ownedContainerDocker` serves `terminate`'s labeled-container removal, which has no component yet. Open PR #244 adds a sibling seam (`daemonRunner`) next to `osdRunner`. |
| Membership and control | `mu`, `controlMu`, `configMu`, `extensionsMu`, `controlPlane`, `manager`, `monitors`, `managers`, `services`, `extensions`, `config`, `keyring`, `portLeases`, `closed`, `monitorTerminated`, `firstMonitorImageID`, `cephVersion`, `owner`, `keyTypeOption`, `settings`, `sharing` | `controlPlane`: `ensureControlPlane` (`ceph/topology.go:75`, `controlMu`), `terminate` (`ceph/ceph.go:1103`, `controlMu` inside the topology lock). `monitors`/`managers`/`manager`: `AddMonitor` (`ceph/topology.go:180,182`), `startManager` (`:768-781`, no lock in the function), `removeMonitor`/`removeManager`, `terminate`. `services`: `startService` (`ceph/ceph.go:979`), `serviceHooks.SetService`/`DeleteService` (`ceph/service_hooks.go:179-187`), `terminate`. `config`: `Run` (`ceph/ceph.go:245`), `configureDefaultCRUSHRoot` (`ceph/placement.go:141`, `configMu`), `refreshMonitorConfigAfterRemoval` (`ceph/topology.go:439`, `configMu`). `closed`: `terminate` (`ceph/ceph.go:1014`). `portLeases`: `trackHostPortLease` (`ceph/ceph.go:475`, `mu`), `reserveTrackedHostPortsWith`, `AddMonitor`. `cephVersion`: `recordCephVersion` (`ceph/version.go:51`). `owner`, `keyTypeOption`, `firstMonitorImageID`: `Run` | `settings` is read in 181 places by name and written only while `Run` builds the cluster; `closed` and `monitors` throughout; `FactsOf` reads `settings` and `cephVersion` without a lock | Four lock domains: `mu`, `controlMu`, `configMu`, `extensionsMu`; some fields are written once before publication | Not one component. Four locks and the lock-free `FactsOf` reads depend on the layout. A split by lock domain is a separate review. |

The components remain in this order of audited risk: auth records (needs the
caller audit; moved in the fourth slice below), the override maps (needs the owner's decision), OSD storage
(close order and the open per-daemon image PR), membership and control (four
locks).

### Not covered by this slice

The auth records, OSD storage, overrides and faults, the other test seams and
membership and control; any change to which lock guards what; any new lock; the
order in which `Terminate` closes the other components; a proof that the
callees of `clientContainerContext` never take `Container.mu` (read from the
source, not enforced by a guard); the `sharedBackend` port that #155 needs; and
any exported API. No Docker, native Ceph or timing behavior was run.

## Fourth internal slice: auth records

Inspected at `7266eb7` (main after #247); path:line citations in this section
are at the revision of the change that adds `ceph/auth_state.go`. The fourth
component moved out of `Container` is the set of auth records, held as
`Container.authRecords` of the unexported type `authState`
(`ceph/auth_state.go`): the fences (`fences`, formerly `authFences`), the
client keys the module confirmed (`keys`, formerly `clientKeys`), every client
key kept for redaction (`secrets`, formerly `clientSecrets`), the key files
that earlier calls could not remove (`strays`, formerly `strayAuthInputs`) and
the temporary client revocations (`revocations`, formerly `clientRevocations`).
The fences serve clients and daemons alike, so the daemon key rotation uses the
same type and moved with it. The old field names are gone, with no alias.

Unlike the earlier slices, this component has no lock of its own and gets none:
`Container.mu` guards every part, as before. The previous slices moved a lock
together with its fields. Here the lock is the topology lock that guards much
more, so it stays on `Container` and the component documents "mu held" on every
method. What the split adds is a structural proof of that contract, which was
only a doc comment before ("The caller holds c.mu").

### Audit of the callers

Every function that reads or writes the records, and the lock it holds, at the
revision of this change. "Takes mu" means the function takes `Container.mu`
itself before the first use and does not release it before the last one.

| Function | Records | Lock |
| --- | --- | --- |
| `createClient` (`ceph/auth.go:146`) | `settleClient`, revocation read, secret, key | takes mu (`lockTopology`, `defer c.mu.Unlock()`) |
| `withClientIdentity` closure (`ceph/auth.go:245`) | `checkCurrentClient` | takes mu (`c.mu.Lock()`) |
| `deleteClient` (`ceph/auth.go:276`) | `settleClient`, key and revocation reads and writes, `changeClient` | takes mu |
| `replaceClientKey` (`ceph/client_key.go:190`) | `settleClient`, secrets, key, fence read | takes mu |
| `clientCapabilities` (`ceph/auth_policy.go:10`), `updateClientCaps` (`:26`) | `ownedClientRecord` and `changeClient` | take mu |
| `revokeClient` (`ceph/client_revocation.go:46`), `ClientRevocationHandle.Restore` (`:81`) | revocation, fence, `settleClient` | take mu |
| `containerBackend.createClient` (`ceph/shared_backend.go:384`), `containerBackend.deleteClient` (`:431`) | `settleClient`, secret, key | take mu |
| `rotateDaemonKey` (`ceph/daemon_key_rotation.go:218`) | fence reads and writes, stray inputs, `authCall`, `daemonExec` | takes mu; its deferred settling runs before the deferred unlock, because it was registered later |
| `diagnosticsSnapshot` (`ceph/diagnostics.go:218`) | `secretValues` (`:262`) | takes mu with `TryLock` and reads before the explicit unlock |
| `serviceHooks.SettleClient` (`ceph/service_hooks.go:377`), `SetClientRevoked` (`:368`) | `settleClient`, `confirmKey` | owner-held: "The caller holds the cluster mutex"; the only callers are in `cephfs` and are proved below |
| `settleClient`, `changeClient`, `ownedClientRecord`, `checkCurrentClient`, `ownedClientCapabilities`, `removeStrayAuthInputs`, `stopEarlierAuthCommands`, `settleEarlierRotation`, `daemonExec`, `writeDaemonKeyrings`, `placeDaemonKeyring`, and the `authCall` methods `send`, `mutate`, `keys`, `settle`, `removeInput` | the records | owner-held helpers; each is reached only from the functions above or from each other |

The eight callers of `settleClient`: `createClient` (`ceph/auth.go:162`),
`deleteClient` (`:291`), `replaceClientKey` (`ceph/client_key.go:202`),
`ownedClientRecord` (`:273`), `Restore` (`ceph/client_revocation.go:90`),
`containerBackend.createClient` (`ceph/shared_backend.go:396`),
`containerBackend.deleteClient` (`:444`) and `serviceHooks.SettleClient`
(`ceph/service_hooks.go:377`). Seven are on a path that takes mu first.
`serviceHooks.SettleClient` and `SetClientRevoked` are called through the hook
interface from `cephfs` (`cephfs/authorization.go:230`, `436`, `462`): their
functions are `authorizationClientCaps`, documented "must be called with
cluster.mu held" (`cephfs/authorization.go:221`), and `DeauthorizeSubvolume`.
The three functions that call `authorizationClientCaps`, one of them
`DeauthorizeSubvolume`, take `servicehook.LockTopology` first
(`cephfs/authorization.go:127`, `422`, `488`).

The audit found no caller that holds another lock or none. No locking was
changed.

Two things stay outside the component:

- `keyRotationDocker` (`ceph/ceph.go`) is a test seam read once, without a lock
  (`ceph/daemon_key_rotation.go:707`). It guards no record and no lock
  covers it, so moving it into a component whose rule is "mu held" would state
  a protection it does not have. It stays on `Container`.
- The `authFence` and `clientRevocationState` values are handed out as pointers
  and changed in place by their callers, under the same mu. The component
  guards the maps, and the callers' functions are covered by the same guard
  because they are the ones that hold mu.

### Lock order

Unchanged. The records add no lock and call no lock: `authState` methods only
read and write maps (`TestAuthStateMethodsMakeOnlyTheirPinnedCalls`). The
callers take mu first (`ceph/auth.go:146`, `ceph/client_key.go:190`,
`ceph/daemon_key_rotation.go:218`), then run native commands through the
control container, which reach `nativeState.mu` (a leaf) as before. Terminate
neither reads nor writes the records.

### Guards, in `ceph/auth_state_test.go`

- `TestAuthStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.authRecords` appears only as the receiver of an `authState` method
  and never in a composite literal.
- `TestAuthStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the field
  set are pinned, and each method makes exactly its listed calls, so none locks
  and none calls into `Container`.
- `TestAuthRecordsAreOnlyUsedWithTheClusterMutexHeld`: every use of
  `Container.authRecords`, every call of an owner-held helper and every
  construction of an `authCall` happens in a function that takes `Container.mu`
  before the use and has not released it, or in a function of the pinned
  `authOwnerHeld` list. The list is closed under callers, a stale entry fails,
  and a call by name needs the name to belong to one function.
- `TestAuthHookCallersInCephFSHoldTheClusterMutex`: the same rule for the
  `cephfs` callers of the two hooks.
- `TestNativeStateAddsNoMutex` counts `authState` too and still expects seven.
- Behavior tests for each method group and a concurrent run under `-race`.

Each guard was broken on purpose and failed: a caller without the lock, an
unpinned caller of `settleClient`, a field read outside a method, a lock inside
a method, a record read after the explicit unlock, a `cephfs` caller without
`LockTopology`, an unpinned helper, and an extra mutex.

The `FactsOf` write-site guard is untouched: no field moved here is read by
`FactsOf`.

### Limits of the guards

The guards are syntactic. They follow calls by name inside the package and in
`cephfs`; they do not follow function values, interface calls other than the two
hooks, or the order of lock operations across branches (the held state is the
last acquire or explicit release before the use, in source order). Closures are
judged by the function that contains them, so a closure that outlives its
function would escape. `applied` and `finish` callbacks run inside
`changeClient` or `Restore`, which hold mu. Behavior under real Docker or
native Ceph was not run.

### Not covered by this slice

OSD storage, the override maps, faults and debug levels, the other test seams
(including `keyRotationDocker`) and membership and control; any change to which
lock guards what; any new lock; the order in which `Terminate` closes the
other components; the `sharedBackend` port that #155 needs; and any exported
API.

## Fifth internal slice: OSD storage state

Inspected at `7266eb7` (main after #247, which includes the per-daemon image
option #244); path:line citations in this section are at that revision unless
they name a new file. The fifth component moved out of `Container` is the
owned storage of the OSDs, held as `Container.stores` of the unexported type
`osdStorageState` (`ceph/osd_storage_state.go`): the tmpfs storage of
`WithOSDInMemoryStorage` (`osdMemory`, now `memory`), the ordinary volume of
`WithOSDVolumeStorage` (`osdVolume`, now `volume`) and the three test seams that
replace their collaborators (`osdRunner` now `runner`, `osdVolumeDocker` now
`volumeDocker`, `osdVolumeRunner` now `volumeRunner`). The type has no mutex.
All `Container` methods, their signatures and their behavior are unchanged.

### What stayed on Container, and why

The audit row "OSD storage" above lists `osds`, `detachedOSDs`, `osdMemory`,
`osdVolume` and `run`. This slice moves only the two storage records and the
seams. `osds` and `detachedOSDs` are the registry of OSD handles: they are read in
about twenty files for handle identity, purge state and topology, not for
storage, and `detachedOSDs` pairs with `osds` in `detachOSD` and `reopenOSD`
(`ceph/osd_retained.go:274,321`). `run` is the admin-socket volume
(`runVolume`, `ceph/admin_socket.go`), written once by `Run`, which no storage
path owns. Moving the handle registry is a separate review.

### Audit of the moved fields

Every production access, at `7266eb7` (before the move; the "now" column is the
component method that replaced it):

| Field | Writers | Readers | Guard |
| --- | --- | --- | --- |
| `osdMemory` | `ensureOSDMemory` publishes the record before the allocation (`ceph/osd_memory_storage.go:109`, now `beginMemory`); `terminate` clears it after a successful release (`ceph/ceph.go:1065,1139`, now `releaseMemory` and `retryMemory`) | `ensureOSDMemory` (`:103`), `registerOSD` (`ceph/ceph.go:693`, `requireFresh`), `removeOSD` (`:867`, `discard`), `newOSDLaunchRecipe` (`ceph/osd_launch.go:54`, `launchVolumes`), `terminate` | `Container.mu`, taken by `startInitialOSDs` (`ceph/initial_daemons.go:26`) and `AddOSDWithConfig` (`ceph/ceph.go:585`) around `registerOSD`, and by `removeOSD` (`:765`) and `terminate` (`:1024`) |
| `osdVolume` | `ensureOSDVolume` (`ceph/osd_volume_storage.go:97`, now `beginVolume`); `terminate` (`ceph/ceph.go:1071,1144`) | the same functions, plus `detachOSD` (`ceph/osd_retained.go:211,263`), `reopenOSD` (`:302`) and `reopenPreflight` (`:381`, through `retainedStore`) | `Container.mu`: `detachOSD` (`:204`) and `reopenOSD` (`:285`) take it, and `reopenPreflight` runs inside `reopenOSD` |
| `osdRunner` | none in production; tests assign it | `startOSD` (`ceph/ceph.go:727`, now `runOSD`) | `startOSD` runs without `Container.mu` so that `Run` can start several OSDs at once, and only reads this function value, which nothing writes after construction |
| `osdVolumeDocker` | none in production | `ensureOSDVolume` copies it into the new record (now `beginVolume`) | `Container.mu` through `registerOSD` |
| `osdVolumeRunner` | none in production | `ensureOSDVolume` (`ceph/osd_volume_storage.go:101`, now `runVolumeHelper`) | `Container.mu` through `registerOSD` |

`FactsOf` reads none of these fields.

### Why no new mutex

Every writer already runs under `Container.mu`, taken by the function itself
or by its caller within the same call chain listed above, so the fields have a
single lock domain and the number of mutexes stays at seven. The only access
that does not hold `mu` is the read of `runner` in `startOSD`, which is safe
because no production code writes a seam (guarded below). The lock-free
`FactsOf` source (`ceph/cluster_facts.go`) is unaffected and the guard from #238
stays valid.

### Lock order and Terminate order preserved

Lines in this list are after the move.

- `terminate` takes `lockTopology` (`ceph/ceph.go:1002`) and holds `c.mu`
  through teardown, as before (the fault undo phases still release it around
  network restores, as written in the first slice).
- First release stage: `releaseMemory` and `releaseVolume` (`ceph/ceph.go:1040-1041`)
  run after the services and the OSD containers are removed and only when
  `len(c.osds) == 0`, memory first. A stage that fails keeps its record, so
  nothing is forgotten before the storage is gone. They run before the manager,
  the managers, the monitors, the control container and the feature client.
- Second stage: `retryMemory` and `retryVolume` (`ceph/ceph.go:1105-1106`) run
  after `removeLabeledContainers`, because that removal can take out a storage
  user whose create reply was lost (an OSD or a helper). They repeat the release
  only for a stage that failed in the first one and only while no OSD is left;
  otherwise they return the first stage's error unchanged, and both errors are
  appended to the result in the old order.
- `registerOSD` checks freshness in memory then volume order and `removeOSD`
  discards in the same order, each stopping at its first failure, as before.
- The component calls into no `Container` method and takes no lock, so it is
  below `Container.mu` and below every lock that `Container.mu` precedes.

### Guards, in `ceph/osd_storage_state_test.go`

- `TestOSDStorageStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.stores` appears only as the receiver of an `osdStorageState`
  method, and the `FactsOf` source never mentions it.
- `TestOSDStorageStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none takes a lock or calls into `Container`,
  and only the publish and release methods assign `memory` and `volume`.
- `TestOSDStorageStateSeamsHaveNoProductionWriter`: no production code assigns
  or sets `runner`, `volumeDocker` or `volumeRunner`.
- `TestOSDStorageStateCallersHoldTheTopologyLock`: the set of callers is pinned;
  `removeOSD`, `detachOSD`, `reopenOSD` and `terminate` call `lockTopology`
  before their first use, and the functions that rely on the caller are reached
  from functions that take it.
- `TestTerminateReleasesOSDStorageInItsTwoStagesInOrder`: lock, memory, volume,
  manager and monitor termination, labeled-container removal, retry memory, retry
  volume, each once and in that order.
- `TestNativeStateAddsNoMutex` counts `osdStorageState` too and still expects
  seven.
- Behavior tests: the release stages forget a record only after success and
  retry only a failed stage, the seams, freshness and discard order, and the
  existing memory and volume storage tests (renamed fields only).

Negative controls were run for each guard and for the behavior tests (a field
read outside the component, a call added to a method, a production seam write, a
lock call removed from `removeOSD`, the retry stage removed, a mutex added, a
failed release that forgets its record, a retry that ignores the first stage,
a diverging validation in the shared cleanup, and a seam that is not wired);
each failed against the broken version and passed after restoring it.

### The duplicated cleanup (issue #233)

`osdMemoryStorage.release` and `osdVolumeStorage.release` were line-for-line
the same sequence: terminate the helper container (`StopTimeout(time.Second)`,
tolerating a missing container), clear it on success, return when the volume is
already marked removed, open a Docker client, inspect the volume, treat not
found as removed, validate the identity and ownership, remove without force and
mark removed, and join the client's `Close` error. They differed only in the
fields, the validation function and the nouns in the error text. They are now
`ownedStorageRelease.run` (`ceph/osd_storage_release.go`) with those parts
passed in. `TestOSDStorageReleaseSemanticsAreIdenticalForMemoryAndVolume` runs
twelve scenarios against both storages (clean, helper termination fails, helper
already gone, no helper, volume already removed, volume missing, inspect fails,
foreign volume, remove fails, remove not found, close fails, client fails), each
with a failing first call and a clean retry, and requires the same error text
after the two nouns are normalized, the same errors, the same helper and removal
state, the same count of Docker calls and terminations and no forced removal.
The creation, freshness, discard and check paths of the two storages differ in
their semantics (a tmpfs keeper with a size and a start identity, versus a
volume with a creation identity and symlink checks) and stay separate.

### Not covered by this slice

The handle registry (`osds`, `detachedOSDs`), the admin-socket `run` volume, the
other components of the table (client and auth, overrides and faults, the two
other test seams, membership and control), any change to which lock guards what,
any new lock, the order in which `Terminate` closes the other components, the
caller chain beyond the functions listed above (read from source, enforced only
for the functions the caller guard names), and any exported API. No Docker or
native Ceph behavior was run.

## Sixth internal slice: run directory state

0.x: old names are removed without aliases. Inspected at `c74f0e5` (main after
#260); path:line citations are at that revision unless they name a new file.
The sixth component moved out of `Container` is the shared admin-socket run
directory, held as `Container.runDir` of the unexported type `runDirState`
(`ceph/run_dir_state.go`). It replaces the field `Container.run` (a
`*runVolume`) and the unexported helper `Container.runMount`, which is deleted.
The type has no mutex. Exported API and behavior are unchanged.

### Why this component, and what was not picked

The audit row "OSD storage" left `osds`, `detachedOSDs` and `run` on
`Container`. The OSD handle registry (`osds`, `detachedOSDs`) is not picked
yet: `osds` is read in about twenty production files across four lock domains,
the test hook publishes a pointer to the map itself (`OSDs: &c.osds`,
`ceph/service_hooks.go:473`) and builds it (`:438`), and about thirty test files
reference it directly. Moving it needs a decision about that pointer, which is the
service-hook seam, and is a review of its own. `run` is the opposite: one
field, twelve production call sites in twelve functions, and a lock behavior that
differs between them, which makes it the part of the audit row that can be
pinned today. The two test seams that remained on `Container` are not moved
either: `keyRotationDocker` (`ceph/daemon_key_rotation.go:707`) and
`ownedContainerDocker` (`ceph/owned_containers.go:35`) each have one reader and
no state of their own to join, and `daemonRunner` (`ceph/daemon_image.go:79`)
belongs to the membership registry. A seam moves with the state of the
dependency that uses it, so these wait for their components.

### Audit of the moved field

| Part | Writers | Readers | Guard |
| --- | --- | --- | --- |
| volume pointer | `Run` through `create` (`ceph/ceph.go:217`), before the cluster is returned; never assigned again | `mounts` in `runMonitor` (`ceph/ceph.go:348`), `ensureControlPlane` (`ceph/topology.go:74`), `AddMonitor` (`:227`), `startManager` (`:796`) and `clientContainerContext` (`ceph/images.go:162`); `published` in `clearAdminSocket` (`ceph/admin_socket.go:210`) and `clearStaleOSDSocket` (`ceph/osd_retained.go:346`); `volumeName` in `newOSDLaunchRecipe` (`ceph/osd_launch.go:51`) | none needed: the pointer and the volume name are immutable after `Run` |
| removed mark | `release` in `terminate` (`ceph/ceph.go:1065`) | `shared` in `adminSocket` (`ceph/admin_socket.go:133`) and `osdDebugTarget.set` (`ceph/debug_level.go:641`) | `Container.mu`: `terminate` and both readers call `lockTopology` first |

The finding that matters: `clientContainerContext` starts the feature client
while holding only the feature-client startup lock, not `Container.mu`
(`ceph/images.go:128-141`). It reads the volume through `mounts`. That is safe
only because the pointer is never reassigned after `Run` and `mounts` reads
only the immutable name, never the removed mark. The old code read `c.run`
there with no stated reason; the type now says why, and the guard below fails
when a second writer of the pointer or a lock-free reader of the removed mark
appears. `FactsOf` reads none of this.

### Why no new mutex

The removed mark has one lock domain, `Container.mu`, taken by every reader and
writer as listed. The pointer needs none. The number of mutexes stays at seven.

### Lock order and Terminate order preserved

- `terminate` still releases the volume once, after `removeLabeledContainers`
  (which takes out every container that mounts it) and the storage retry, and
  before the bridges are removed (`ceph/ceph.go:1061-1067`).
- A failed release keeps the removed mark unset, so the volume stays shared
  and the next `Terminate` retries it. A removed volume stays published, which
  is what the stale-socket cleanup keys on, and keeps its name for the launch
  recipe.
- The component calls into no `Container` method and takes no lock.

### Guards, in `ceph/run_dir_state_test.go`

- `TestRunDirStateFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.runDir` appears only as the receiver of a `runDirState` method;
  `FactsOf` never mentions it.
- `TestRunDirStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the calls
  of each method are pinned, none locks or calls into `Container`, and only
  `create` assigns the volume pointer.
- `TestRunDirStateCreatePublishesBeforeTheDockerCreate`: the record is assigned
  before the Docker create.
- `TestRunDirStateCallersAreAudited`: every caller and the method it calls is
  pinned with its guard kind (init, free, takes); `create` has the single caller
  `Run`; `shared` and `release` are called only from functions that call
  `lockTopology` first.
- `TestTerminateReleasesTheRunDirectoryAfterTheStorageRetryAndBeforeTheBridges`.
- `TestNativeStateAddsNoMutex` counts `runDirState` too and still expects seven.
- Behavior: an empty component is inert, and a failed release keeps the volume
  shared while a successful one does not and is not repeated.

Negative controls, each failing against the broken version and passing after
restoring it: a field read outside the type, a call added to `shared`, an
assignment in `release`, the lock removed from `adminSocket`, a mutex added, a
publish after the create, a second `release` call in `terminate`, a caller not
in the table, and a release that marks the volume removed after a failure.

### Not covered by this slice

The OSD handle registry (`osds`, `detachedOSDs`), the service-hook pointer to
`osds`, the three test seams above, membership and control, any change to which
lock guards what, any new lock, the `runVolume` type itself (its create,
remove and validation are unchanged), and any exported API. The creation of the
volume is pinned only by the order of its publish, because `runVolume.create`
has no Docker seam in `create`. No Docker or native Ceph behavior was run.

## Seventh internal slice: OSD handle registry

The seventh component moved out of `Container` is the OSD handle registry, held
as `Container.osdHandles` of the unexported type `osdRegistry`
(`ceph/osd_registry_state.go`). It replaces the fields `Container.osds` (the
owned descriptors by OSD ID) and `Container.detachedOSDs` (the retained stores
and registrations without a container). The type has no mutex. 0.x: the old
fields are removed without aliases, and every one of the roughly thirty test
files that read `c.osds` directly was migrated to the registry methods.

The registry methods are `get`, `count`, `each` (a range-over-func iterator in
map order), `sorted`, `register`, `forget`, `detachedOf`, `park` (the detach
transition: record the detached OSD and drop the owned descriptor in one call),
`unpark` (the reopen transition: drop the detached OSD and record the owned
descriptor in one call) and `replace` (for the unit-test hook only). A handle
is owned or detached, never both, and `park` and `unpark` are the only methods
that touch both maps, so a caller cannot break the pairing by forgetting half
of it.

### The service-hook pointer

The audit of the fifth slice found that `serviceHooks.TestState` published
`OSDs: &c.osds`, a pointer to the map itself, and that `TestCluster` built the
map. A test in `cephfs` or `fault` could therefore write the map without
`Container.mu`. The pointer is removed. The test cluster needs two things, and
only these two are exposed, as functions that take the cluster:

- `servicehook.TestReplaceOSDs(c, osds)` swaps the owned descriptors (the
  `cephfs` and `fault` fixtures set them to nil, to an empty map or to a map
  with one fake OSD);
- `servicehook.TestOSDCount(c)` reads the number of owned descriptors (the
  `cephfs` admission test checks that a refused call left both OSDs owned).

Both take no lock, like the pointer they replace: they are for a cluster value
that no other goroutine uses yet, or that the test holds through the mutex
that `TestState` still exposes. `TestCluster` registers its descriptors through
`register`. The exposed state view no longer has a member that points at OSD
descriptors.

### Audit of the callers

Every production access, after the move, with how `Container.mu` is held:

| Guard | Functions |
| --- | --- |
| takes `Container.mu` itself, before the first use | `removeOSD` (`ceph/ceph.go:706`), `terminate` (`:959`), `detachOSD` (`ceph/osd_retained.go:204`), `reopenOSD` (`:281`), `osdStates` (`ceph/osd_policy.go:27`), `WaitForClean` (`ceph/status.go:38`), `daemonsRunning` (`ceph/shared_backend.go`), `diagnosticsSnapshot` (`ceph/diagnostics.go`, through `TryLock`), `openDebugTarget` (`ceph/debug_level.go:175`, after `lockTopology`) |
| documented as called with `Container.mu` held, and every call comes from a function that takes it | `registerOSD` (from `AddOSDWithConfig`, `ceph/ceph.go:537`, and `startInitialOSDs`, `ceph/initial_daemons.go:26`), `currentOSD`, `ownedBy`, `ownsDaemon`, `daemonKeyrings`, `generationLocked`, `validateOSDPlacement`, `poolPlacementDomains`, `nativePoolDomains`, `ownedOSDState`, `ownsNativeTargetContext`, `monitorConfigTargets`, `osdsLocked`, and the functions between them and a lock |
| hooks for another package whose documentation requires the mutex | `OwnedOSD` and `Owns` (`ceph/fault_hooks.go`), `PoolPlacementDomains`, `TestReplaceOSDs`, `TestOSDCount` (`ceph/service_hooks.go`) |
| runs before the cluster is shared | `prepareInitialComposition` (`ceph/composition.go`, a throwaway `Container` that only plans), `TestCluster` |

The finding that matters is that the domain is single: every access is under
`Container.mu`, with no read that relies on the other locks. `FactsOf` reads
none of it. `registerOSD` takes no lock itself (as the audit row said); its two
callers take it before the call, and the guard below makes that a checked
fact for it and for the other "holds" functions.

### Why no new mutex

Every reader and writer already runs under `Container.mu`, taken by the
function itself (`lockTopology`, `ceph/topology.go:390`, or `mu.Lock`) or by a
caller within the same call chain, as listed. The registry therefore sits where
the fields sat in the lock order: after the shared-gate admission and the
topology gate, before `configMu`, `controlMu` and the leaf locks, and the
registry methods take no lock and call no `Container` method. The number of
mutexes stays at seven.

### Terminate order preserved

`terminate` still terminates each owned OSD container and forgets its
descriptor (`ceph/ceph.go:988-996`) before the first storage release stage,
which is told `count() == 0` (`:997-998`), and reads the count again for the
retry stage after `removeLabeledContainers` (`:1062-1063`). The loop keeps an
OSD whose container cannot be removed, as before.

### Guards, in `ceph/osd_registry_state_test.go`

- `TestOSDRegistryFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.osdHandles` appears only as the receiver of an `osdRegistry`
  method (so no code reads either map or takes its address), and `FactsOf`
  never mentions it.
- `TestOSDRegistryMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none locks or calls into `Container`, and
  each map is assigned only by its owning methods (`owned` by `register` and
  `replace`, `detached` by `park`).
- `TestOSDRegistryCallersAreAudited`: every function that uses the registry,
  the methods it calls and its guard kind (takes, holds, hook, init) are
  pinned. A "takes" function acquires the lock before its first use. A "holds"
  or "hook" function has a documentation comment that mentions the held lock,
  and every call to a "holds" function, found by name over the production
  files, comes from a function that acquires the lock before the call or is
  itself a pinned "holds", "hook" or "init" function. A stale row fails.
- `TestOSDRegistryHasNoAddressTakenAndTestHooksExposeNoMap`: nothing takes the
  address of the registry or one of its maps, and the exported test state
  view has no member that points at OSD descriptors.
- `TestOSDRegistryReplaceHasTheSingleCallerTheTestHook`: `replace` is called
  from production only by `TestReplaceOSDs`.
- `TestTerminateDropsTheOSDsBeforeTheStorageReleaseDecidesOnTheirCount`: the
  forget loop, the first release stage, the labeled-container removal and the
  retry stage keep their order and the count is read for each of the four
  release calls.
- `TestNativeStateAddsNoMutex` counts `osdRegistry` too and still expects
  seven.
- Behavior: the zero value is empty and writable, `sorted` orders by ID,
  `each` allows forgetting the visited key and stops early, `park` and
  `unpark` move a handle between the maps, `replace` leaves the detached map
  alone.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a map read outside the type, a call added to a
method, `forget` assigning the detached map, a caller not in the table, a
"takes" function without its lock, a "holds" function called from a function
without the lock, the address of the registry taken, the test state view
exposing the map again, a second caller of `replace`, a mutex added, `park`
keeping the owned handle, `unpark` keeping the detached handle, one count read
dropped from `terminate`, the forget loop moved after the release, `each`
ignoring an early stop, and `register` without the lazy map.

### Limits of the guards

The guards are syntactic. They follow calls by name inside the package, so two
functions with the same name are merged; they do not follow function values or
interface calls other than the ones listed; a "takes" function is judged by
the position of its acquire before its first use, not by where it releases, and
closures are judged by the function that contains them. The hooks `OwnedOSD`,
`Owns` and `PoolPlacementDomains` are called from other packages (`fault`,
`cephfs`), which are not scanned: their "caller holds the mutex" contract is
documentation, as before. Behavior under real Docker or native Ceph was not
run.

### Not covered by this slice

The remaining components (the override maps, faults and debug levels, the other
test seams, membership and control), any change to which lock guards what, any
new lock, the `osdMemory` and `osdVolume` records (already moved in the fifth
slice), the order in which `Terminate` closes the other components, the
`sharedBackend` port that #155 needs, and any exported API. The test hook
functions are not synchronized. No Docker or native Ceph behavior was run.

## Eighth internal slice: host-port lease state

0.x: old names are removed without aliases. Inspected at `39b26f9` (main after
#267); path:line citations are at the branch head unless they name another
revision. The eighth component moved out of `Container` is the host-port
reservation list of host network mode, held as `Container.portLeases` of the
unexported type `portLeaseState` (`ceph/port_lease_state.go`). It replaces the
field `Container.portLeases` (a `[]*hostPortLease`) of the same name. The type
has no mutex. Exported API and behavior are unchanged. This slice is not the
membership/control step: that group stays whole, see below.

### Fresh audit of the fields of `Container`

The earlier tables of this note are partly stale: the override maps, the OSD
maps, the run volume and the port list have since moved or been renamed. At
`39b26f9`, `Container` has 33 named fields (plus the embedded
`testcontainers.Container`), 39 exported and 189 unexported methods
(information only, no target). Fields by state, with this slice applied:

| State | Fields | Status |
| --- | --- | --- |
| inside a component | `nets`, `nativeCmds`, `toolkit`, `authRecords`, `stores`, `runDir`, `osdHandles`, `portLeases` | components with the guards of their own section |
| named registry, guarded as written | `faults` (`faultRegistry`, guarded by `mu`, `ceph/fault_registry.go:115`), `debugLevels` (`debugLevelRegistry`, own lock, `ceph/debug_level.go:135`) | already a type of their own; the undo and release methods that walk them are still methods on `Container` (`undoFaults` and the per-kind handles), so a state component needs the fault-handle seam of #146, not a field move |
| test seams | `keyRotationDocker` (`ceph/daemon_key_rotation.go:707`), `ownedContainerDocker` (`ceph/owned_containers.go:35`), `daemonRunner` (`ceph/daemon_image.go:79`) | one reader each and no state to join; they move with the component that owns their dependency |
| bootstrap record | `config`, `keyring`, `cephVersion`, `keyTypeOption`, `firstMonitorImageID`, `owner` | `config` is rewritten after `Run` under `configMu` (`ceph/placement.go:141`, `ceph/topology.go:449`), many readers were not checked for which lock they hold (unverified), and `service_hooks.go:473` publishes `&c.config`, `&c.keyring` and `&c.cephVersion` to other packages; not a clean single-domain move |
| membership and control | `mu`, `controlMu`, `configMu`, `controlPlane`, `manager`, `monitors`, `managers`, `services`, `extensionsMu`, `extensions`, `closed`, `monitorTerminated`, `settings`, `sharing` | four lock domains (`mu`, `controlMu`, `configMu`, `extensionsMu`) plus the `sharing` atomic; `closed` and `monitorTerminated` are read in about a dozen functions under `mu`, and `TestState` publishes pointers to `closed` and `controlPlane` (`ceph/service_hooks.go:472-473`); the first safe step needs the test-hook pointers removed as the OSD slice did, and is a slice of its own |

### Why this component, and what was not picked

`portLeases` is the one field that is written in three places, read in two, and
covered by a single lock domain with no outside pointer:

| Part | Writers | Readers | Guard |
| --- | --- | --- | --- |
| lease slice | `reserveTrackedHostPortsWith` (`ceph/host_port_tracking.go:32`, after `lockTopology` at `:23`), `trackHostPortLease` (`ceph/ceph.go:434`, with `mu.Lock`), `AddMonitor` (`ceph/topology.go:202`, after `lockTopology` at `:149`) | `terminate` (`ceph/ceph.go:977`, which holds `mu`), the exported test view (`ceph/service_hooks.go:476`, a count) | `Container.mu` |

The hook publishes a count, never a pointer, so no outside package can write the
slice. The membership/control group was not picked because the hooks above and
the four lock domains make any move there a review of its own. The fault and
debug-level registries were not picked because their undo logic still lives on
`Container`. The bootstrap record was not picked because `config` is rewritten after
`Run` under `configMu` while `keyring` is written once, and the hook publishes
pointers to both.

### Why no new mutex

Every access runs under `Container.mu`, taken by the function itself
(`lockTopology`, or `mu.Lock` in `trackHostPortLease`) before the first use.
Each lease has its own mutex, taken by `hostPortLease.Release`
(`ceph/host_network.go:69`) through `lockTopologyMutex`; that lock stays a leaf
below `mu` and is not touched. The component takes no lock and calls into no
`Container` method. The number of mutexes counted by `TestNativeStateAddsNoMutex`
stays at seven. That test lists the component types by name and does not cover
`debugLevelRegistry`, whose own `mu` makes eight in total; this note records
that gap and does not change it.

### Lock order and Terminate order preserved

`terminate` still undoes the faults (`ceph/ceph.go:972`), stops the native
commands (`:976`) and then releases the leases (`:977`), before it terminates
the services (`:979`), the OSDs and the monitors. `release` still tries every
lease, collects the failures and keeps every lease recorded, so a failed
release is retried by the next `Terminate` and a released one is a no-op. A
lease whose reservation failed or whose context ended is still published as
soon as its allocator exists, without a context gate.

### Guards, in `ceph/port_lease_state_test.go`

- `TestPortLeaseStateFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.portLeases` appears only as the receiver of a `portLeaseState`
  method, nothing reads the slice or takes an address, and `FactsOf` never
  mentions it.
- `TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none locks or calls into `Container`, and
  only `track` assigns the slice.
- `TestPortLeaseStateCallersAreAudited`: every caller, the methods it calls and
  its guard kind are pinned; a "takes" function acquires `mu` before its first
  use, the test hook documents its contract, and a stale row fails.
- `TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices`.
- `TestNativeStateAddsNoMutex` counts `portLeaseState` and still expects seven.
- Behavior: an empty state is inert, a nil lease is not recorded, `release`
  tries every lease and keeps a failed one for retry.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a slice read outside the type, the address taken, a
call added to a method, an assignment in `release`, a lock added to `track`, a
mutex added, a caller without the lock, a caller not in the table, `release`
moved after the services, `release` moved before the faults, a release that
stops at the first failure, a release that forgets its leases, and a `track`
that records a nil lease.

### Limits of the guards

The guards are syntactic. They follow calls by name inside the package and judge
a function by the position of its first acquire, not by where it releases. The
hook `TestState` is called from other packages, which are not scanned: its
"caller holds the lock or the cluster is not shared" contract is documentation.
No Docker or native Ceph behavior was run.

### Not covered by this slice

Membership and control (all four lock domains, `closed`, `monitorTerminated`,
the `TestState` pointers), the fault and debug-level registries as state
components, the bootstrap record, the three test seams, the debug-level
registry in the mutex count, any change to which lock guards what, any new
lock, `hostPortLease` itself (reservation, parsing and release are unchanged),
and any exported API.

## Ninth internal slice: membership/control test-hook pointers

0.x: old names are removed without aliases. Inspected at `4293511` (main after
#269); citations are at the branch head. This is the first safe step of the
membership/control group named in the audit of the eighth slice: it removes the
raw pointers that `serviceHooks.TestState` (`ceph/service_hooks.go:466`)
published to other packages. It moves no field, adds no component type and
changes no lock. Exported API and production behavior are unchanged.

### What was exposed

`TestState` returned pointers into the cluster. Those that reached the
membership/control group or its options were:

| Pointer | Field | Writers outside the package (tests) |
| --- | --- | --- |
| `Closed` | `closed` | 23 sites in `cephfs`, `fault`, `override`, `rgw`, `internal/overridehook` |
| `ControlPlane` | `controlPlane` | 2 (`cephfs`, `override`) |
| `InitialOSDs`, `PoolReplicas`, `PoolMinSize`, `StartupTimeout`, `CephFSImage`, `RGWImage` | `settings.*` | 8; `RGWImage` had no user |

All uses were plain writes made while a test held `Mutex`, or on a cluster value
that was not shared. No caller read through a pointer.

### What replaced them

Six hooks on `serviceHooks`, mirrored by functions in `internal/servicehook`
(the pattern of `TestReplaceOSDs` and `TestOSDCount` from the OSD registry
slice):

- `TestSetClosed(c, closed)`
- `TestSetControlPlane(c, control)`
- `TestSetInitialOSDs(c, n)`
- `TestSetPoolDefaults(c, replicas, minSize)`
- `TestSetStartupTimeout(c, d)`
- `TestSetCephFSImage(c, image)`

Each is one assignment, makes no call and takes no lock. Its doc comment names
the lock the caller holds (`Mutex` for the closed flag and the options,
`ControlMutex` for the control plane) or that the cluster value is not shared.
`TestState` keeps the three lock identities, `Config`, `Keyring`, `CephVersion`,
`NetworkRemoved` and the `PortLeases` count. The unused `RGWImage` pointer was
dropped. Every caller in the tests was migrated, including the lock-then-write
sequences (the lock stays in the test, the write is the hook).

### Why a hook per value and not a typed component

The audit showed that `closed` and `controlPlane` are written by a handful of
functions under three different locks (`mu`, `controlMu`, and the unshared
builder), so a component type would either need those locks or lie about them.
The hooks are the narrowest seam that keeps each write under the lock domain it
already has. A component type for the group stays a later step, after the
`closed` and `monitorTerminated` readers are audited.

### Guards, in `ceph/membership_control_test.go`

- `TestTestStateExposesNoMembershipControlPointer`: the exact field set of
  `testState` and of `servicehook.TestStateView` is pinned, so a new pointer
  needs a review.
- `TestMembershipControlStateIsNeverAddressed`: no production function takes
  the address of `closed`, `controlPlane`, `settings`, `monitorTerminated`,
  `manager`, `monitors`, `managers`, `services` or `extensions`, or of anything
  under them.
- `TestClosedAndControlPlaneWritersAreAudited`: the writers of `closed` and
  `controlPlane` are pinned by function and field (`terminate`, `TestCluster`,
  `ensureControlPlane` with `controlMu`, and the two hooks); a new writer or a
  stale row fails.
- `TestTestSetHooksAreSingleWritesWithoutCalls`: the hook set, the fields each
  assigns, no call, and the documented lock contract.
- `TestTestSetHooksAreCalledOnlyFromTests`: the cross-package caller table. No
  non-test file in the module calls a `servicehook.TestSet*` function, so the
  hooks cannot become a second production writer.
- `TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup` lists the four
  settings hooks as the only writers of `c.settings` after `Run`. Only
  `TestSetCephFSImage` writes a field `FactsOf` reads (`cephfsOverride`); the
  others are listed because the guard treats all of `settings` as recorded.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: the `Closed` pointer put back in both views, the
address of `c.closed` taken, the address of `c.settings.osds` taken, a new
`c.closed` writer in `ensureControlPlane`, a lock call in a hook, a hook writing
a second field, a hook doc without the contract, an unpinned `TestSet*` hook, a
non-test file calling `servicehook.TestSetClosed`, and a mutex added to
`portLeaseState`. Putting the pointer back in only the `ceph` view panics at
package initialization instead (the two views must be the same type), which is
also a failure.

### The mutex count is now eight

`TestNativeStateAddsNoMutex` listed the component types by name and left out
`debugLevelRegistry`, whose own `mu` (`ceph/debug_level.go:136`) made the real
total eight while the test expected seven. The test now includes
`debugLevelRegistry` and expects eight: `mu`, `controlMu`, `configMu`,
`extensionsMu`, `native.mu`, `toolkit.mu`, `toolkit.publishMu` and the
debug-level registry's `mu`. This slice adds no mutex and changes no lock order;
the number changed only because the test now counts what exists. Removing the
type from the list again makes the test fail (negative control).

### Limits of the guards

The guards are syntactic and follow names inside the package. The writer table
judges receivers by the identifiers the package uses for a cluster (`c`,
`cluster`, `owner`). Callers of the hooks in other packages are scanned only for
non-test use; their lock discipline is documentation. No Docker or native Ceph
behavior was run.

### Not covered by this slice

The group is not moved. The three lock identities (`Mutex`, `ControlMutex`,
`ConfigMutex`) are still published as pointers, because the lock queueing tests
need the lock itself and a different seam would be a lock-order review.
`Config`, `Keyring` and `CephVersion` (bootstrap record, `config` under
`configMu`) and `NetworkRemoved` (network component) are still pointers and
belong to their own slices. Also not covered: `monitorTerminated`, `monitors`,
`managers`, `services`, `extensions` and `sharing`, a typed component for the
group, the fault and debug-level registries as components, any lock change, and
any exported API.

## Tenth internal slice: fault registry and debug-level window registry

0.x: old names are removed without aliases. Inspected at `4293511` (main after
#269); path:line citations are at the branch head unless they name another
revision. The two registries that the eighth slice left on `Container` are now
components that carry their own logic. `Container.faults` is a `faultRegistry`
(`ceph/fault_registry.go:124`) whose methods hold the reservation, release and
Terminate undo logic that used to be methods on `Container`
(`reserveFault`, `settleFault`, `releaseFault`, `tracksFault`, `heldFaults` and
`undoFaults`; all removed). `Container.debugLevels` stays a `debugLevelRegistry`
(`ceph/debug_level.go:136`) and gets guards of its own. Exported API and
behavior are unchanged.

### What moved

| Before | After |
| --- | --- |
| `Container.reserveFault`, `settleFault`, `releaseFault`, `tracksFault` | `faultRegistry.reserve`, `settle`, `release`, `tracks` |
| `Container.heldFaults`, `Container.undoFaults` | `faultRegistry.held`, `faultRegistry.undo` |
| `c.faults.entries[...]` read in `allowPoolDeleteHeld`, `refuseInstanceFaults` and the two `TestFaults`/`TestHeld` hooks | `faultRegistry.holds`, `openInstanceFault`, `handles` |

Two seams changed shape so that the component reads no `Container` field and
calls no `Container` method:

- `settle` used to return `Container.closed`. It returns nothing now, and the
  two `Settle` hooks (`ceph/fault_hooks.go:55`, `ceph/override_hooks.go:89`)
  read `c.closed` themselves after it, in the same critical section, so a live
  settle, a handle-less settle and a stale token still report the flag exactly
  as before.
- `undo` used to unlock and relock `c.mu` around each network-shaping restore.
  It receives the cluster mutex as a `sync.Locker` argument (`terminate` passes
  `&c.mu`, `ceph/ceph.go:974`) and calls only `Unlock` and `Lock` on it
  (`ceph/fault_registry.go:268,270`), only in the shaping phase.

### Why no new mutex and no lock-order change

`faultRegistry` has no mutex. Every method runs under `Container.mu`, taken by
the caller before the first use (`terminate` through `lockTopology` at
`ceph/ceph.go:963`, which is followed by `c.closed = true` at `:967`; the hooks
document that the caller holds the mutex, the same contract as before). The one
release of `mu` is the existing one around a shaping restore, which still runs
before any bridge is removed, so the order stated in `ceph/network_state.go`
holds unchanged. `terminate` still sets `closed`, undoes the faults
(`ceph/ceph.go:974`), stops the native commands (`:978`) and then releases the
leases (`:979`).

`debugLevelRegistry` keeps its leaf lock (`ceph/debug_level.go:137`). `Reserve`
(`:142`) and `Release` (`:156`) take it first, defer the unlock, call only
`invalid`, `errors.New`, `make` and `delete`, and never name `Container`. The
override package calls them from its own window state
(`override/debug_level.go:208,236,286,464`). The lines read do not call them
inside a section of `Container.mu`, but those callers are in another package
and are not scanned, so that part is documentation and not a guard. The registry is handed out
once, by `overrideHooks.DebugWindows` (`ceph/override_hooks.go:152`), and
`terminate` never touches it: a window is released by its own handle.

### Mutex count, now honest

`TestNativeStateAddsNoMutex` used to list the component types by name and left
`debugLevelRegistry` out, so its count of seven understated the real eight.
It now scans every component type including `faultRegistry` and
`debugLevelRegistry`, collects each mutex as `Type.field`, and compares the
sorted list with the pinned `nativeStateMutexOwners` (`ceph/native_state_test.go:272`):

| Owner | Mutexes |
| --- | --- |
| `Container` | `mu`, `controlMu`, `configMu`, `extensionsMu` |
| `nativeState` | `mu` |
| `featureClientState` | `mu`, `publishMu` |
| `debugLevelRegistry` | `mu` |

That is eight. No mutex was added by this slice: the eighth is the one that the
old test did not scan. Mutexes of other types (a lease or a handle with its own
lock) are not in the scope of this test and are not part of the claim.

### Guards, in `ceph/fault_registry_state_test.go`

- `TestFaultRegistryFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.faults` is only the receiver of a `faultRegistry` method call; the
  entries map is never read, no address of the registry or the map is taken, and
  `FactsOf` never mentions it.
- `TestFaultRegistryMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned, only `undo` operates on a lock and only through its
  parameter, and no method names `Container`, `closed` or a mutex field.
- `TestFaultRegistryCallersAreAudited`: every caller, the methods it calls and
  its guard kind (takes the lock before the first use, or documents that the
  caller holds it); a stale row fails.
- `TestTerminateUndoesFaultsFirstUnderTheClusterMutex`: `faults.undo` runs once,
  after `closed` is set, before the native commands and the lease release, and
  receives `&c.mu`.
- `TestDebugLevelRegistryStaysALeafBehindItsOwnLock`: pinned calls, lock before
  the first access, deferred unlock, one named use of `Container.debugLevels`.
- Behavior: both `Settle` hooks report `closed` for a live, handle-less and stale
  token; the new accessors read what the callers need.
- `TestPortLeaseStateCallersAreAudited` and the lease Terminate-order test were
  updated to the new name `faults.undo`.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: an `entries` read outside the type, the address of
`faults` taken, a call added to `settle`, a `Container` field read in a method,
`undo` without its relock, a caller not in the table, a hook without its lock
documentation, `undo` removed from `terminate`, `undo` given another mutex, a
`Release` without its lock, a second use of `debugLevels`, a mutex added to
`faultRegistry`, the debug-level registry dropped from the scan, a `Settle` hook
that ignores `closed`, and an `openInstanceFault` that ignores the completion
check.

### Limits of the guards

The guards are syntactic: they follow calls by name inside the package and judge
a function by the position of its first acquire. The hooks are called from other
packages that are not scanned. No Docker or native Ceph behavior was run for
this slice.

### Not covered by this slice

Membership and control, the bootstrap record, the three test seams, any change
to which lock guards what, any new lock, the fault kinds table and the override
package, and any exported API. The override package keeps calling the same
hooks; the typed hook seam in `internal/overridehook` is unchanged.

## Eleventh internal slice: control-plane state

0.x: old names are removed without aliases. Inspected at `75a313b` (main after
#272); path:line citations are at the branch head unless they name another
revision. This is the first component of the membership and control group.
`Container.controlPlane` is now a `controlPlaneState`
(`ceph/control_plane_state.go:34`) that owns the control container handle and
the read-write lock that guards it, the former `Container.controlMu`. Exported
API and behavior are unchanged; `controlMu` and the raw handle field are gone.

### Fresh audit of the membership and control group by lock domain

Counts are textual uses by field name in non-test files of package `ceph`
before this change, so they overstate real call sites.

| Field(s) | Lock domain | Production uses | Verdict |
| --- | --- | --- | --- |
| `controlPlane` with `controlMu` | its own leaf lock | 19 uses of the handle and 17 of the lock in 8 functions: `terminate`, `ensureControlPlane`, `cliContainer`, `controlContainerContext`, `admitNativeTarget`, `sharedDaemons`, `diagnosticsSnapshot`, `TestState` | Picked. Two writers, no pointer published to other packages since #270, the lock identity is the only thing `TestState` still shares |
| `closed`, `monitorTerminated` | `mu` | 60 and 10 | Not picked. A flag type would add a method per read in about a dozen functions and still need the caller to hold `mu`; `closed` is also read by the gate and by the `Settle` hooks, so it is a separate review of the admission semantics |
| `manager`, `monitors`, `managers`, `services` | `mu` | 11, 17, 26 and 29 | Not picked. A roster like `mdsRoster`, but the maps hold `MonitorContainer` and `ManagerContainer` handles that `AddMonitor`, `startManager`, `removeMonitor`, `removeManager` and `serviceHooks.SetService`/`DeleteService` rewrite in place, and `sharedDaemons` and `terminate` iterate them; the callers are not yet pinned |
| `config`, `keyring`, `cephVersion` | `configMu` (bootstrap record) | 49, 10 and 3 | Not picked. `TestState` still publishes `Config`, `Keyring` and `CephVersion` as pointers; they need setters first, as #270 did for the closed flag |
| `extensions` | `extensionsMu` | 6 | Not picked. A generic keyed store (`extensionsWith`), small and independent; a later slice |
| `sharing` | atomic | 4 | Not picked. One atomic, owned by the gate; the shared-gate admission is its own review |
| `settings` | none after `Run` | 181 reads | Not picked. Immutable options read in 37 files |

The control plane is the one group of the list where the lock and the data move
together without a `mu` argument: every other row is guarded by `Container.mu`
or by a lock that also guards unrelated fields.

### What moved

| Before | After |
| --- | --- |
| `Container.controlMu`, `Container.controlPlane` (a `testcontainers.Container`) | `Container.controlPlane` (a `controlPlaneState`) with `mu` and `handle` |
| `cliContainer`, `sharedDaemons`: `controlMu.RLock`, read, `RUnlock` | `controlPlane.current()` |
| `controlContainerContext`: `lockTopologyReadMutex(ctx, &controlMu)`, read | `controlPlane.currentContext(ctx)` |
| `admitNativeTarget`: lock, compare container ID, unlock | `controlPlane.owns(ctx, id)` |
| `diagnosticsSnapshot`: `diagnosticLock(ctx, controlMu.TryRLock)`, read | `controlPlane.diagnosticHandle(ctx)` |
| `ensureControlPlane`: `controlMu.Lock`, assign, `Unlock` | `controlPlane.publish(ctr)` |
| `terminate`: `controlMu.Lock`, `Terminate`, forget on success, `Unlock` | `controlPlane.terminate(ctx, opts...)` |
| `TestState.ControlMutex: &c.controlMu` | `c.controlPlane.mutex()` (the same lock, now inside the component) |

The one conditional of `terminate` is preserved: the handle is forgotten when the
removal succeeded or the container was already gone, and kept (so the next
`Terminate` retries it) on any other error. The returned error is the same
value `terminate` appended before.

### Why no new mutex and no lock-order change

`controlMu` is the lock that moved; its type (`sync.RWMutex`), its holders and its
position are unchanged. The order stays: shared-gate admission, then
`Container.mu` (`lockTopology`, `ceph/topology.go:383`), then the control lock.
`ensureControlPlane` (`ceph/topology.go:62`) and `terminate` (`ceph/ceph.go:1038`)
run with `mu` held, and `terminate` still removes the control container after the
first MON and before the feature client (`ceph/ceph.go:1041`) and the labeled
containers (`:1054`). The readers `diagnosticsSnapshot` (`ceph/diagnostics.go:267`),
`admitNativeTarget` (`ceph/native_command.go:547`) and `sharedDaemons`
(`ceph/shared_cluster_port.go:70`) call it after releasing `mu`;
`cliContainer` and `controlContainerContext` may be called with `mu` held, which
is the same direction. No method of the type takes another lock or calls into
`Container`, so a holder of the control lock never waits for `mu`; the only wait
inside it is the Docker removal that `terminate` already made under the write
lock. `TestState` keeps publishing `ControlMutex` because the lock queueing tests
in `cephfs`, `override` and `rgw` need the lock itself; the pointer now comes from
`controlPlane.mutex()`, whose only caller is `TestState`.

### Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `controlPlaneState` as a component and pins
`controlPlaneState.mu` where it pinned `Container.controlMu`: still eight
mutexes (`Container.mu`, `configMu`, `extensionsMu`, `controlPlaneState.mu`,
`nativeState.mu`, `featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`). Adding a second mutex to the component fails it
(negative control).

### Guards, in `ceph/control_plane_state_test.go`

- `TestControlPlaneStateFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.controlPlane` is the receiver of a method call, or the owner of
  `handle` inside `TestSetControlPlane` only; no `Container` literal in production
  sets it; `FactsOf` never mentions it.
- `TestControlPlaneStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned, no method names `Container` or `closed`, and
  `handle` is assigned only by `publish` and `terminate`.
- `TestControlPlaneStateWritersAreAudited`: the production writers of `handle`
  are `publish` and `terminate` (both take the write lock) and the
  `TestSetControlPlane` hook (its doc names the lock contract).
- `TestControlPlaneStateCallersAreAudited`: the caller table, with the guard kind
  of each (`takes` the topology lock first, `holds` documented, `free`, `identity`),
  and the single caller of `mutex()`; a stale or new row fails.
- `TestTerminateRemovesTheControlPlaneUnderTheTopologyLockBeforeTheFeatureClient`:
  `lockTopology`, the first MON's `Terminate`, `controlPlane.terminate`, the
  feature-client lock and `removeLabeledContainers` each once, in that order.
- Behavior: an empty component is inert; `owns` compares IDs; a failed removal
  keeps the handle and a missing container or success forgets it; the bounded
  readers return the context error while the write lock is held; concurrent
  readers against a publisher and terminator, run under `-race`; `TestState`
  publishes the component's own lock.
- `TestClosedWritersAreAudited` (renamed from the closed-and-control-plane
  writer test) keeps the `closed` rows; `TestSetControlPlane` is pinned as
  assigning `handle`.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the component, `publish` without
the lock, a direct `handle` read in `cliContainer`, an extra call in `current`,
a method that names `Container`, a second caller of `mutex()`, a stale caller
row, `controlPlane.terminate` called twice in `terminate`, a `Container` literal
that sets `controlPlane`, a hook doc without the contract, a `terminate` that
forgets the handle on a real error, a lock identity that is a copy, and a
`currentContext` that ignores its context.

### Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The caller table does
not prove the position of the lock relative to `mu` for the `free` callers; that
part is the argument above. The other packages reach the lock only through the
`ControlMutex` pointer and the `TestSetControlPlane` hook, which are not scanned
for lock discipline. No Docker or native Ceph behavior was run for this slice.

### Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `configMu`,
`extensionsMu`, `manager`, `monitors`, `managers`, `services`, `closed`,
`monitorTerminated`, `config`, `keyring`, `cephVersion`, `extensions`, `sharing`,
`settings`, `firstMonitorImageID`, `owner` and `keyTypeOption`. Also not covered:
the `ControlMutex` pointer in `TestState`, any change to which lock guards what,
any new lock, the three test seams and any exported API.

## Twelfth internal slice: bootstrap record

0.x: old names are removed without aliases. Inspected at `2f1abd4` (main after
#274); path:line citations are at the branch head unless they name another
revision. Two independent changes in one slice. First, the last raw pointers
that `serviceHooks.TestState` published for the bootstrap record are replaced
by setters, as #270 did for the closed flag, the control plane and the options.
Second, `Container.config`, `Container.keyring` and `Container.cephVersion`
move into a `bootstrapRecord` (`ceph/bootstrap_record.go:35`, field
`ceph/ceph.go:53`) that also owns the read-write lock that guards the first
two, the former `Container.configMu`. Exported API and behavior are unchanged.

### Fresh audit of what is left of the membership and control group

Counts are textual uses by field name in non-test files of package `ceph`
before this change, so they overstate real call sites.

| Field(s) | Lock domain | Verdict |
| --- | --- | --- |
| `config`, `keyring` with `configMu`; `cephVersion` | `configMu` for the two byte slices; `cephVersion` has no lock (written once in `Run`, read lock-free by `FactsOf`) | Picked. The lock guards only these two fields, every reader is a method-sized section that calls no other lock, and the three pointers in `TestState` were the last raw pointers into the group |
| `extensions` with `extensionsMu` | its own leaf lock | Not picked. A generic keyed store (`extensionsWith`), six uses in two files; independent of this slice and the next candidate that needs no `mu` argument |
| `closed`, `monitorTerminated` | `mu` | Not picked, for the reasons given in the eleventh slice: a flag type would add a method per read in about a dozen functions and `closed` is read by the shared-gate admission |
| `manager`, `monitors`, `managers`, `services` | `mu` | Not picked. The maps hold daemon handles that several functions rewrite in place and `sharedDaemons` and `terminate` iterate; the callers are not yet pinned |
| `sharing` | atomic | Not picked. One atomic owned by the gate |
| `settings` | none after `Run` | Not picked. Immutable options read in 37 files |
| `keyTypeOption`, `firstMonitorImageID`, `owner` | none, written once in `Run` | Not picked for this component. They are bootstrap facts but have no lock and no `TestState` pointer, so putting them in the record would widen it without a lock-domain reason |

The earlier audit (ninth slice) listed `config`, `keyring`, `cephVersion`,
`keyTypeOption`, `firstMonitorImageID` and `owner` as one "bootstrap record" and
called it unclean because of the published pointers and unverified readers.
The pointers are now gone and the readers were all checked below, so the
subset that shares a lock domain is a clean move. The three fields that have no
lock stay where they are.

### Part 1: setters instead of pointers

`TestState` and `servicehook.TestStateView` no longer have `Config`, `Keyring`
and `CephVersion`. Their field set is now `ConfigMutex`, `ControlMutex`,
`Mutex`, `NetworkRemoved` and `PortLeases`. Three hooks replace them, mirrored
by functions in `internal/servicehook/testaccess.go`:

- `TestSetConfig(c, config)` and `TestSetKeyring(c, keyring)`: one assignment
  each, the caller holds `ConfigMutex` or the cluster value is not shared.
- `TestSetCephVersion(c, version)`: one assignment, the caller holds `Mutex` or
  the cluster value is not shared (the field has no lock of its own).

Every caller was migrated: 20 setter calls replace the pointer writes in `override`, `cephfs`, `rgw/multisite`,
`rbd/mirror` and `internal/overridehook`. All were plain writes; none read through a
pointer. `NetworkRemoved` is still a pointer, because it belongs to the network
component.

### Part 2: what moved

| Before | After |
| --- | --- |
| `Container.configMu`, `config`, `keyring`, `cephVersion` | `Container.bootstrap` (a `bootstrapRecord`) with `mu`, `config`, `keyring`, `cephVersion` |
| `Run`: `c.config, err = readFile(...)`, `c.keyring, err = ...` | `bootstrap.recordConfig`, `recordKeyring` (no lock, before the cluster is shared) |
| `recordCephVersion`: `c.cephVersion = version` | `bootstrap.recordVersion` |
| `adminClient`: `configMu.RLock`, clone both, `RUnlock` | `bootstrap.clones()` |
| `currentMonitorBootstrapAddresses`: lock, clone config, keyring present, unlock | `bootstrap.templateAndConfirmed()` |
| `newOSDLaunchRecipe`: lock, strings of both, unlock | `bootstrap.launchStrings()` |
| `refreshMonitorConfigAfterRemoval`: read lock and clone; write lock and assign | `bootstrap.template()`, `bootstrap.publishConfig(...)` |
| `configureDefaultCRUSHRoot`: write lock, rewrite, assign on success, unlock | `bootstrap.applyDefaultCRUSHRule(value)` |
| `HealthDetails`, `fullRatiosSession`, `poolPGs`, `poolUsage`, `ClusterIdentity`: `lockTopologyReadMutex(ctx, &configMu)`, FSID, keyring present, unlock | `bootstrap.identity(ctx)` returning a `bootstrapIdentity` |
| `ConnectionConfig`: bounded read lock, completeness check, clone both | `bootstrap.connection(ctx)` |
| `diagnosticsSnapshot`: `diagnosticLock(ctx, configMu.TryRLock)`, scan the keyring, unlock | `bootstrap.diagnosticKeyring(ctx)` (a copy) |
| `TestState.ConfigMutex: &c.configMu` | `c.bootstrap.mutex()` (the same lock, now inside the component) |

Readers that never took `configMu` keep reading without a lock, now through
methods whose name says so: `rawConfig`, `rawKeyring`, `rawVersion`,
`hasConfig`, `isComplete`. Their callers are `createClient`,
`withClientIdentity`, `createSharedClient`, `startService` and `AddMonitor`, each
after it took the topology lock (`lockTopology`, `ceph/topology.go:383`, or
`mu.Lock` directly), and the lock-free `staticFacts` that `FactsOf` reads. Making
those reads locked would be a lock-order change, so none was.

One incidental behavior is preserved on purpose: `Run` stored the result of
`readFile` before checking its error, and `recordConfig`/`recordKeyring` are
called before the check as well.

### Why no new mutex and no lock-order change

`configMu` is the lock that moved; its type (`sync.RWMutex`), its holders and its
position are unchanged. It is a leaf below `Container.mu`: `adminClient`
(`ceph/ceph.go:471`, inside `startService` with `mu` held),
`currentMonitorBootstrapAddresses` (`ceph/client_monitor_config.go:87`, "called
under c.mu"), `newOSDLaunchRecipe` (`ceph/osd_launch.go:37`, "the caller holds
c.mu"), `refreshMonitorConfigAfterRemoval` (`ceph/topology.go:425,439`) and
`ConnectionConfig` (`ceph/topology_snapshot_context.go:29`, after the
`lockTopology` call there) take it with `mu` held. `diagnosticsSnapshot`
(`ceph/diagnostics.go:283`), the health and pool readers and `ClusterIdentity`
(`ceph/service_hooks.go:352`) take it after releasing `mu`, and
`configureDefaultCRUSHRoot` (`ceph/placement.go:138`) takes it during startup.
No method of the type takes another lock or calls into `Container`: the only
calls out are the pure helpers `monitorConfigClusterIdentity` and
`defaultCRUSHRuleConfig` and the two bounded-lock helpers, so a holder of the
record lock never waits for `Container.mu` or any other lock. `cephVersion` is
in the component but outside its lock; it is written only in `Run` and read by
`staticFacts`, which the recorded-sources guard in
`cluster_facts_termination_test.go` already pins.

### Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `bootstrapRecord` as a component and pins
`bootstrapRecord.mu` where it pinned `Container.configMu`: still eight mutexes
(`Container.mu`, `extensionsMu`, `controlPlaneState.mu`, `bootstrapRecord.mu`,
`nativeState.mu`, `featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`). Adding a second mutex to the component fails it
(negative control).

### Guards, in `ceph/bootstrap_record_test.go`

- `TestContainerDeclaresNoBootstrapFieldsOrLock`: `Container` no longer
  declares `config`, `keyring`, `cephVersion` or `configMu`, and the record has
  exactly its four fields.
- `TestBootstrapRecordFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.bootstrap` is the receiver of a method call, or (the three
  `TestSet*` hooks only) the owner of a field the hook assigns; no `Container`
  literal in production sets it.
- `TestBootstrapRecordMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned, no method names `Container`, `closed`,
  `controlPlane` or `settings`, or another mutex.
- `TestBootstrapRecordWritersAreAudited`: the writers of the three fields are
  the three startup recorders (no lock), `applyDefaultCRUSHRule` and
  `publishConfig` (both take the write lock) and the three hooks (docs name the
  lock contract).
- `TestBootstrapRecordCallersAreAudited`: the caller table with the guard kind
  of each (`free`, `takes`, `startup`, `lockfree`, `identity`); a `takes` caller
  takes the topology lock before its first use, the single `lockfree` caller is
  `staticFacts`, the single caller of `mutex()` is `TestState`, and a stale or
  new row fails.
- The existing guards were updated, not weakened: the `TestState` field set and
  the hook table (`membership_control_test.go`), `bootstrap` in the no-address
  list, the mutex owner table, and the recorded-sources guard, which now flags
  both a direct write of `bootstrap.cephVersion` and a call of
  `bootstrap.recordVersion` outside `Run`, `recordCephVersion` and
  `TestSetCephVersion`.
- Behavior: an empty record is inert and incomplete; the locked readers return
  copies; `identity` separates the FSID, the identity error and the keyring
  presence; a failed `applyDefaultCRUSHRule` leaves the record unchanged; the
  three bounded readers return the context error while the write lock is held;
  concurrent readers against a writer under `-race`; `TestState` publishes the
  component's own lock.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the component, `publishConfig`
without the lock, a direct field read in `adminClient`, an extra call in
`template`, a method that names `Container`, a second caller of `mutex()`, an
unlisted caller, a `takes` caller that uses the record before the topology lock,
a `Container` literal that sets `bootstrap`, a hook doc without the contract, the
`Config` pointer put back in the view, `configMu` declared again on `Container`,
a `clones` that returns the stored slices, an `identity` that ignores its
context, `recordVersion` called from `staticFacts`, `bootstrap.cephVersion`
assigned directly, the address of `bootstrap.config` taken, and a lock identity
that is a copy.

### Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The `takes` check
proves the order of two calls in a function body, not that the topology lock is
still held at the use. The cross-package setters are scanned only for non-test
callers by the existing caller table; their lock discipline is documentation. No
Docker or native Ceph behavior was run for this slice.

### Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `extensionsMu`,
`manager`, `monitors`, `managers`, `services`, `closed`, `monitorTerminated`,
`extensions`, `sharing`, `settings`, `firstMonitorImageID`, `owner` and
`keyTypeOption`. Also not covered: the `Mutex`, `ControlMutex` and `ConfigMutex`
pointers in `TestState`, `NetworkRemoved` (network component), any change to which
lock guards what, any new lock and any exported API.

## Thirteenth internal slice: extension store

0.x: old names are removed without aliases. Inspected at `eccaad1` (main after
#277); path:line citations are at the branch head. Two independent changes in
one slice. First, `Container.extensions` and `Container.extensionsMu` move into
an `extensionStore` (`ceph/extension_store.go:33`, field `ceph/ceph.go:75`) that
owns the map and the lock that guards it. Second, the last raw pointer that
`serviceHooks.TestState` published to a non-lock field, `NetworkRemoved`, is
replaced by two hooks. Exported API and behavior are unchanged.

### Fresh audit of what is left of the membership and control group

Counts are textual uses by field name in non-test files of package `ceph`, so
they overstate real call sites.

| Field(s) | Lock domain | Verdict |
| --- | --- | --- |
| `extensions` with `extensionsMu` | its own leaf lock | Picked. Every access is in two functions (`serviceHooks.Extension` and the generic `extensionsWith`, the latter with three callers); no function takes another lock inside the lock except the `create` callback, and the two callbacks in the module only allocate a registry |
| `sharing` | atomic | Not picked as a second component. Four uses in three non-test files (`ceph/shared_gate.go:252,273`, `ceph/shared_cluster_port.go:77`, `ceph/service_hooks.go:422`), all `Load` or `Store` of one atomic that `sharedGate` already owns by name. A component would wrap one atomic and add no lock domain or guard |
| `settings` | none after `Run` | Not picked. Immutable options read in 39 files (149 lines); a component would add an accessor per option and hide nothing |
| `closed`, `monitorTerminated` | `mu` | Not picked, for the reasons in the eleventh slice: 61 and 11 uses, `closed` is read by the shared-gate admission |
| `manager`, `monitors`, `managers`, `services` | `mu` | Not picked. The maps hold daemon handles that several functions rewrite in place and `sharedDaemons` and `terminate` iterate (17, 26, 11 and 29 uses) |
| `keyTypeOption`, `firstMonitorImageID`, `owner` | none, written once in `Run` | Not picked. No lock domain; `owner` is read in 17 files |
| `TestState` pointers `Mutex`, `ControlMutex`, `ConfigMutex` | lock identities | Not picked. The lock queueing tests of the service packages need the identity, not a value |
| `TestState.NetworkRemoved` | `mu` (a field of `networkState`) | Picked. It needed no lock identity: its two users in `fault` set it once on a synthetic bridge and read it after `Terminate` |

### Part 1: what moved

| Before | After |
| --- | --- |
| `Container.extensionsMu`, `extensions` | `Container.extensions` (an `extensionStore`) with `mu`, `states` |
| `serviceHooks.Extension`: lock, look up, create on a miss, store, unlock | `extensions.getOrCreate(kind, create)` (`ceph/service_hooks.go:285`) |
| `extensionsWith[T]`: lock, sort the kinds, type-assert each state, unlock | `extensions.inKindOrder()` returns the states in kind order under the lock; the generic function asserts `T` after the lock is released (`ceph/service_bootstrap.go:81`) |

Methods cannot be generic, so `extensionsWith[T]` stays a function; it is the
only caller of `inKindOrder`. One difference in timing, no difference in result:
the type assertion used to run inside the lock and now runs on the snapshot
after it. The states are handed out unchanged, as before; the service packages
guard their own contents with the cluster mutex, as their docs say.

### Part 2: `NetworkRemoved` replaced by hooks

`TestState` and `servicehook.TestStateView` no longer have `NetworkRemoved`.
Their field set is now `ConfigMutex`, `ControlMutex`, `Mutex` and `PortLeases`:
three lock identities and a count. `TestSetNetworkRemoved(c, removed)` and
`TestNetworkRemoved(c)` (`ceph/service_hooks.go:490,496`, wrappers in
`internal/servicehook/testaccess.go`) replace the pointer. Both take no lock; the
caller holds `Mutex` or the cluster value is not shared, as for the other
`TestSet*` hooks. The three callers in `fault` were migrated
(`fault/network_test_helpers_test.go`, `fault/network_test.go`). The
removal-flag writer guard (`TestNetworkStateRemovalFlagsHaveOneWriter`) now
lists the hook in place of `TestState`, and the recorded-fact guard
(`TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup`) lists it as the
only new writer through `nets`; neither guard was loosened beyond that row.

### Why no new mutex and no lock-order change

`extensionsMu` is the lock that moved; its type (`sync.Mutex`), its holders and
its position are unchanged. It is a leaf: the only code that runs under it
besides map operations is the `create` callback of `getOrCreate`, as in the
former `serviceHooks.Extension`, and the two callbacks in the module
(`cephfs.filesystems`, `cephfs/entry.go:108`; `rgw.gatewaysFrom`,
`rgw/entry.go:95`) allocate a registry and call nothing. Callers may or may not hold the
cluster mutex, as the doc of `serviceHooks.Extension` says. `extensionsWith` in
`monitorConfigTargets` (`ceph/topology.go:546`, reached from
`refreshMonitorConfigAfterRemoval` at `:441`) and in `diagnosticsSnapshot`
(`ceph/diagnostics.go:257`, after `diagnosticLock(ctx, c.mu.TryLock)` at `:218`)
run with `Container.mu` held, and the tenant-port path
(`containerOps.extension`, `ceph/shared_ops.go:47`) is the caller that does not
promise it. Where both are held
the order is `Container.mu`, then the store lock, and nothing takes
`Container.mu` while holding the store lock. The store publishes no lock
identity (`TestState` never exposed `extensionsMu`), so the type has no
accessor for its mutex.

### Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `extensionStore` as a component and pins
`extensionStore.mu` where it pinned `Container.extensionsMu`: still eight
mutexes (`Container.mu`, `extensionStore.mu`, `controlPlaneState.mu`,
`bootstrapRecord.mu`, `nativeState.mu`, `featureClientState.mu`,
`featureClientState.publishMu`, `debugLevelRegistry.mu`). Adding a second mutex
to the component fails it (negative control). `Container` now declares one
mutex.

### Guards, in `ceph/extension_store_test.go`

- `TestContainerDeclaresNoExtensionsMapOrLock`: `Container` no longer declares
  `extensionsMu`, and the store has exactly the fields `mu` and `states`.
- `TestExtensionStoreFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.extensions` is only the receiver of a store method; its map is
  never reached; no production `Container` literal sets it; the guard is not
  vacuous.
- `TestExtensionStoreMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned; no method names `Container`, `closed`,
  `controlPlane`, `settings` or `bootstrap`, or another mutex.
- `TestExtensionStoreMapWritersAreAudited`: the only writer of the map is
  `getOrCreate`, and it takes the lock.
- `TestExtensionStoreCallersAreAudited`: the caller table (`Extension` calls
  `getOrCreate`, `extensionsWith` calls `inKindOrder`); a new or stale row
  fails.
- The existing guards were updated, not weakened: the `TestState` field set and
  the `TestSet*` hook table (`ceph/membership_control_test.go`), the mutex owner
  table and component list (`ceph/native_state_test.go`), and the two guards
  named above.
- Behavior: an empty store is inert; each kind is created once; a running
  `create` holds the lock against another kind; the snapshot is in kind order and
  does not change when a kind is added later; `extensionsWith` filters by
  interface in kind order; concurrent use under `-race`; the hook reaches the
  store; the removal-flag hooks reach `nets.publicRemoved` and only it.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the store, `extensionsMu` declared
again on `Container`, `getOrCreate` without the lock, a direct read of the map in
`extensionsWith`, an extra call in `inKindOrder`, a method that names
`Container`, an unlisted caller of the store, a `Container` literal that sets
`extensions`, the `NetworkRemoved` pointer put back in the view, and a hook that
makes a call or writes a second flag.

### Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The cross-package
hooks are scanned only for non-test callers by the existing caller table; their
lock discipline is documentation. The `create` callback runs under the lock by
contract, which no guard can check for callbacks in other packages; the
behavior test pins only that the store holds the lock while it runs. No Docker
or native Ceph behavior was run for this slice.

### Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `manager`,
`monitors`, `managers`, `services`, `closed`, `monitorTerminated`, `sharing`,
`settings`, `firstMonitorImageID`, `owner` and `keyTypeOption`. Also not
covered: the `Mutex`, `ControlMutex` and `ConfigMutex` pointers in `TestState`,
any change to which lock guards what, any new lock, the three test seams and any
exported API. `ceph.Container` has 229 method declarations before and after
this change (information only; it is not a target).

## Fourteenth internal slice: startup identity

0.x: old names are removed without aliases. Inspected at `eb933e7` (main after
#280); path:line citations are at the branch head. `Container.owner`,
`Container.keyTypeOption`, `Container.firstMonitorImageID` and the test seam
`Container.ownedContainerDocker` move into a `startupIdentity`
(`ceph/startup_identity.go`, field `Container.startup`, `ceph/ceph.go:63`). The
other two test seams and the remaining membership fields get a recorded
disposition below. Exported API and behavior are unchanged. The slice
overlaps `ceph/ceph.go` (the `Container` struct and `Run`) with the member
roster slice (#281); the textual conflict is expected and left to the owner.

### Disposition of the remaining startup fields and test seams

Counts are textual uses by field name in non-test files of package `ceph`
before this change.

| Field | Lock domain | Verdict |
| --- | --- | --- |
| `owner` | none: set by the composite literal in `Run` (`ceph/ceph.go:193`) | Moved. Its only users are the label (`ownedLabel`, 12 call sites in 7 files), the labeled-container removal, and three reads that copy it into a record (`ceph/osd_launch.go:45`, `ceph/verification.go:559`, `ceph/finalization.go:124`) |
| `keyTypeOption` | none: written once in `Run` (`ceph/ceph.go:235`) | Moved. One reader, `generateKeyArgs` (`ceph/auth_config.go:183,187`) |
| `firstMonitorImageID` | none: written once in `Run` (`ceph/ceph.go:239`) | Moved. Two readers that name it in an error (`ceph/auth_config.go:184`, `ceph/dual_stack_admission.go:39`) |
| `ownedContainerDocker` (test seam) | none: set by tests before the cluster starts | Moved. Its one reader is the labeled-container removal, which uses nothing else of `Container` but `owner`; the seam travels with the dependency it replaces |
| `keyRotationDocker` (test seam) | none | Stays on `Container`. One reader, `keyRotationExec` (`ceph/daemon_key_rotation.go:707`, called from `:717,751`). The rotation code is a set of `Container` methods that call `authRecords`, `bootstrap` and the control plane, not a component; moving the seam alone would create a one-field type whose owner has no state of its own |
| `daemonRunner` (test seam) | none | Stays on `Container`. One reader, `runDaemon` (`ceph/daemon_image.go:79`), called by `AddMonitor` and `startManager`. It replaces container creation for the membership functions, which stay whole on `Container` (the roster slice and the lock domains of `mu` are separate reviews) |
| `settings` | none after `Run` | Stays. Immutable options read across the package; a component would add an accessor per option and hide nothing (unchanged from the twelfth and thirteenth slices) |
| `sharing` | atomic | Stays. One atomic already owned by `sharedGate` by name (unchanged) |

### What moved

| Before | After |
| --- | --- |
| `Container.owner`, `keyTypeOption`, `firstMonitorImageID`, `ownedContainerDocker` | `Container.startup` (a `startupIdentity`) |
| `Container.ownedLabel()` | `startupIdentity.ownedLabel()` |
| `Container.removeLabeledContainers(ctx)` | `startupIdentity.removeLabeledContainers(ctx)` (`ceph/ceph.go:1053` is its one caller) |
| `Run`: `c.keyTypeOption, err = detect...`; `c.firstMonitorImageID = ...` | `recordKeyTypeOption` before the error check, as the field was assigned before it, then `recordFirstMonitorImage` |
| reads of the three fields | `ownerID()`, `supportsKeyType()`, `firstMonitorImage()` |

The old names are deleted; there are no aliases or wrappers. The test callers
were migrated (`ceph/owned_containers_test.go`, `ceph/verification_test.go`,
`ceph/finalization_test.go`, `ceph/osd_launch_test.go` and others).

### Why no lock, no new mutex and no lock-order change

The type has no mutex and `Container.mu` was not given a new job. Every fact is
written while the cluster value belongs to the goroutine that runs `Run`: the
owner in the literal at `ceph/ceph.go:193`, the other two after the first MON
started (`ceph/ceph.go:235,239`) and before any other daemon is started or the
value is returned. A cluster that fails part way is returned with what was
recorded, so `Terminate` can still list by the owner ID. After `Run` returns,
the fields are read only. No method of the type calls into `Container` or takes
a lock, so the type adds no edge to the lock order. The one place that runs
under a lock is `terminate`: `startup.removeLabeledContainers` is called with
`Container.mu` held (`ceph/ceph.go:1053`, after `lockTopology` at `:963`), as
`Container.removeLabeledContainers` was, and it takes no lock itself. Its
position in Terminate (after the OSD storage release and the control plane,
before the storage retry and the run directory) is pinned by the existing
order guards, which were changed only where they matched the full call
expression (`ceph/control_plane_state_test.go`).

### Mutex count unchanged

`TestNativeStateAddsNoMutex` now scans `startupIdentity` as a component. The
count stays at eight (`Container.mu`, `extensionStore.mu`,
`controlPlaneState.mu`, `bootstrapRecord.mu`, `nativeState.mu`,
`featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`); a mutex in the new type fails it.

### Guards, in `ceph/startup_identity_test.go`

- `TestContainerDeclaresNoStartupIdentityFields`: the four fields are gone from
  `Container`, and the component has exactly those four members.
- `TestStartupIdentityFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.startup` is only the receiver of a component method; no member is
  read, assigned or addressed; the only production `Container` literal that
  sets it is `Run`'s; no literal sets a removed field.
- `TestStartupIdentityMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned; no method names `Container`, `closed`,
  `controlPlane`, `settings` or `bootstrap`, or touches a mutex; a field is
  assigned only by its recorder.
- `TestStartupIdentityCallersAreAudited`: the caller table (function and the
  methods it calls); the two recorders are called exactly once each, by `Run`;
  a new or stale row fails.
- `TestStartupIdentitySourceIsInTheRightFile`.
- Behavior: a zero identity is inert and never reaches the Docker seam; each
  recorder changes only its own fact; the label carries the owner ID and two
  owners differ; the removal lists by its own label only and reports a client
  failure.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a mutex in the component, `owner` declared again on
`Container`, a method that names `Container`, a recorder called outside `Run`,
a direct read of `Container.startup.keyTypeOption`, an unlisted caller, a
second production `Container` literal that sets `startup`, and a recorder that
drops its write.

### Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. "Written before the
cluster is shared" is argued above and pinned only as far as the recorders have
one caller, `Run`; no guard proves that `Run` publishes the value after the
writes. No Docker or native Ceph behavior was run for this slice.

### Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `manager`,
`monitors`, `managers`, `services`, `closed`, `monitorTerminated`, `sharing`,
`settings`, and the test seams `keyRotationDocker` and `daemonRunner`. Also not
covered: any change to which lock guards what, any new lock, the member roster
(#281) and any exported API.

## Limits of this note

- Source reading and a static parse of method declarations only. No native
  Ceph, Docker or timing behavior was run or measured for it.
- Call-site counts are textual (by method name) and overstate the number of
  `Container` call sites.
- go-ceph is referenced in the issue as a source comparison for dependency
  decomposition. It does not prove this module's lock order, restoration or
  Docker-generation safety, and no go-ceph dependency is introduced.
