# Reducing the size of ceph.Container

This note is the design record that the parent issue (#146) requires before any
further extraction from `ceph.Container`. It re-measures the type after the
accessor, handle-operation and typed-flag changes (#143, #144, #145), proposes a
shape for each remaining group, and lists the dependencies, admission, state
and cleanup ownership that each extraction has to preserve. It changes no code.

Every shape below is a proposal. Each extraction still gets its own child issue
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

The exported count equals the issue's expected total after #145 (73). The
unexported count grew by 26. The likely cause is that helpers stayed on
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
  the observation files at `494d3bd`:

  | Path | Methods | What it needs |
  | --- | --- | --- |
  | `cephCLI` (selects the control container on every call through `controlContainerContext`, runs `ceph --connect-timeout 5`) | `Status`, `DaemonAddresses`, `ManagerServices`, `ManagerStatus`, `OSDFlags`, `OSDStates`, `Configuration`, `BlocklistEntries`, `MGRModules` | Control container lookup per call. Not gated. |
  | `controlContainerContext` then a query helper | `QuorumStatus` (`queryMonitorQuorum`); `MonitorBootstrapAddresses` (through `QuorumStatus`, the original bootstrap config for the FSID comparison, and `monitorClientRefreshContext`) | Control container and, for the second, the bootstrap identity. |
  | Captured control handle and captured bootstrap identity, queried directly | `HealthDetails` (FSID, health, FSID queries on one captured handle, with `configMu` read for the original FSID and keyring presence); `FullRatios` (through `fullRatiosSession`, which also serves `OverrideFullRatios`) | The control handle chosen once under the topology lock and never switched to a replacement, the original FSID, and the `closed` check. This fence is part of the behavior and must be preserved, not replaced by `cephCLI`. |
  | `clientContainerContext` then `rados` in that container | `PGInconsistencies` (`rados list-inconsistent-obj`), `ObjectWatchers` (`rados listwatchers`) | Client image role selection: the control container when the client image equals the essential image, otherwise the feature client under `clientMu` and `nativeMu` (refused after native commands are closed). Client lifecycle and configuration, not the control container. |
  | `ManagerServices` then `supervised.Run` in the control container | `ManagerMetrics` (python3 scrape of the advertised HTTP endpoint) | Supervised bounded execution (`managerMetricsTimeout`, `managerMetricsLimit`) plus the control container. |

  The extraction contract therefore needs, per method: the control lookup, the
  captured-control and identity fence, the client container lookup, and
  supervised execution, in addition to `cephCLI`. A `cephCLI`-only contract would
  drop collaborators or change which container runs the query. No override uses a
  path outside this list except `OverrideDebugLevel`, which runs admin socket
  commands through a native invocation and `supervised` (`debug_level.go`; not
  audited in detail, unconfirmed). Shared-cluster admission for observations is
  unaffected because they are `sharedOpen`; the executors themselves are not gated.
- **Admission.** `sharedMethodPolicies` is keyed by method name and checked by
  `TestSharedGateClassifiesEveryMethod`, which reflects over `*Container`, requires
  a policy for every method and calls every non-open method on a shared cluster
  to see it refused. `sharedHandlePolicies` and
  `TestSharedGateClassifiesEveryHandleOperation` apply the same scheme to the
  handle receivers through `sharedHandleReceivers`. `sharedFaultPolicies` covers
  the fault entry points. All 16 observations and 10 facts are `sharedOpen`; all
  7 overrides are `sharedExclusive`.
- **Fault registry.** `TemporaryBlocklist`, `OverrideFullRatios`, `OverrideConfig`,
  `OverrideOSDFlag` and `OverrideMGRModule` reserve and settle entries in
  `Container.faults` (`reserveFault`, `settleFault`), guarded by `mu`. The last
  three use the kinds `faultConfigOverride`, `faultOSDFlag` and `faultMGRModule`
  (phase `faultTracked`, keys the configuration key, the flag token and the
  module name). `OverrideDebugLevel` uses `debugLevels`, which has its own lock.
- **Override state.** The handles of `OverrideConfig`, `OverrideOSDFlag` and
  `OverrideMGRModule` hold a registry token in their state, `owner *Container`
  and run `Restore` against it; the former maps `configOverrides`,
  `flagOverrides` and `moduleOverrides` no longer exist. The current lifetime of every family is in the table
  under the overrides group below.
- **Messenger mode.** `OverrideConfig` reads `settings.messengerMode` to refuse
  options fixed by the bootstrap policy. `OverrideMessengerPolicy` builds its
  result from `OverrideConfig` calls.

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
- **Size effect.** `Container` exported methods 73 to 63. First extraction
  recommended, because it has no lock, no gate and no cleanup, and it clarifies
  which `settings` fields are public facts before the networking fields move.

### Read-only observation snapshots (16 methods)

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

### Cluster-wide reversible overrides (7 methods)

`OverrideConfig`, `OverrideDebugLevel`, `OverrideFullRatios`, `OverrideMGRModule`,
`OverrideMessengerPolicy`, `OverrideOSDFlag`, `TemporaryBlocklist`.

- **Shape.** Package functions that take the cluster and return the existing
  handles, for example `ceph.OverrideConfig(ctx, c, setting)`. The handles and
  their `Restore` behavior do not change; only the entry point moves, as the issue
  notes.
- **Dependencies actually consumed.** Admission, `mu` (or `lockTopology`), the
  liveness check, `faults` (`TemporaryBlocklist`, `OverrideFullRatios`),
  `debugLevels` (`OverrideDebugLevel`), the three override maps, `settings`
  (messenger mode, startup timeout) and `cephCLI`.
- **Admission.** All `sharedExclusive`. The function must call the gate before it
  touches the cluster, as the methods do. The policy keys move from
  `sharedMethodPolicies` to a table for package-level operations, and a reflection
  test must cover every exported function of the group so that a new override
  cannot skip the gate.
- **State and lock owner.** Unchanged: the maps and the registries stay on
  `Container` in this step. They move only with the internal split below.
- **Cleanup and restore lifetime (current behavior, preserved unchanged).**
  `Container.terminate` sets `closed` and calls `undoFaults`, which visits only
  the faults in the shaping, endpoint and freeze phases (network delays,
  interruptions, pauses). It never visits the tracked kinds (the five override kinds). The extraction is
  structural only: it adds no automatic restoration and no uncertain-outcome
  reporting, and it creates no second ownership path.

  | Family | Who restores | After `Terminate` (read from the code) |
  | --- | --- | --- |
  | `OverrideConfig` (fault kind `faultConfigOverride`, phase `faultTracked`) | The caller, through `ConfigOverrideHandle.Restore` | `Restore` calls `poolPolicyReady`, so it returns `errClusterTerminated` on a closed cluster. |
  | `OverrideMessengerPolicy` | The caller, through `MessengerPolicyOverrideHandle.Restore`, which restores its `ConfigOverrideHandle` values in reverse order | Same as `OverrideConfig`, because it is built from those handles. |
  | `OverrideOSDFlag` (fault kind `faultOSDFlag`, phase `faultTracked`) | The caller, through `Restore` | `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `OverrideMGRModule` (fault kind `faultMGRModule`, phase `faultTracked`) | The caller, through `Restore` | `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `TemporaryBlocklist` (fault kind `faultBlocklist`, phase `faultTracked`) | The caller, through `BlocklistOverride.Restore` | Tracked kinds are bookkeeping only and `terminate` leaves them alone (`fault_registry.go`). `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `OverrideFullRatios` (fault kind `faultFullRatios`, phase `faultTracked`) | The caller, through `FullRatiosOverride.Restore` | Same bookkeeping rule. `Restore` goes through `fullRatiosSession`, which refuses a closed cluster. |
  | `OverrideDebugLevel` (`debugLevels`) | The caller, typically registered with `Finalization.Cleanup` as the `OverrideDebugLevel` documentation describes | Its `Restore` is driven by the handle's own state and generation checks. Its behavior after `Terminate` was not traced (unconfirmed). |

  In every family the handle is the only restoration path, and the cluster's
  termination ends the native state with the cluster rather than restoring it.
- **Package boundary design.** The owner decided on 2026-10-27 to prefer a public
  `override` package with the handles moving with the entry points (option A).
  The decisions, the concrete proposal awaiting owner approval, the import graph
  and the migration order are in
  [override-package-boundary.md](override-package-boundary.md). The shape above
  (functions in `ceph`) is the fallback only if a structural import cycle were
  shown, and that note shows none.
- **Open questions.** `OverrideMessengerPolicy` delegates to `OverrideConfig`
  and takes no lock of its own, unlike the other six; it may be better placed with
  the messenger fixtures than with this group. `TemporaryBlocklist` is a time-bounded fence
  rather than a restore-to-previous override and may deserve its own placement.
- **Size effect.** `Container` exported methods 47 to 40, matching the issue's table.

### Not in the issue's tables

The issue's size table reaches about 40 exported methods after the three groups.
The remaining methods listed under "Operations that fit none of the above" were
not assigned a shape there. Each group below needs a decision before the target
is reachable; none is proposed as a commitment here.

| Candidates | Observation |
| --- | --- |
| `WaitForClean`, `WaitForPGClean`, `WaitForQuorum`, `WaitMGRModuleReady` | Waits that poll observations. They could become methods of the observation object or stay as lifecycle helpers. |
| `DeepScrubPG`, `RepairPG` | PG operations. Principle 1 suggests a PG handle, which does not exist yet. |
| `RefreshMonitorConfig`, `RefreshClientMonitorConfig` | Client and monitor configuration refresh; related to the client and auth state split. |
| `CollectCrashMetadata`, `CollectDiagnostics`, `RedactSecrets` | Diagnostics group; `sharedOpen` today. Post-termination representation is compared in [post-termination-diagnostics.md](post-termination-diagnostics.md). |
| `RunNativeCommand`, `WithClient`, `WithClientIdentity` | Client and native-command surface that `Tenant` mirrors; tied to #155. |

The 40-method target is therefore a lower bound on what the three groups alone
can achieve, not a measured prediction.

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
| Membership and control | `mu`, `controlMu`, `configMu`, `controlPlane`, `manager`, `monitors`, `managers`, `services`, `extensionsMu`, `extensions`, `config`, `keyring`, `portLeases`, `closed`, `monitorTerminated`, `firstMonitorImageID`, `cephVersion`, `owner`, `keyTypeOption`, `settings`, `sharing` |

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
code changes. This note does not claim that any step is implemented or that the
size targets are reached.

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
| Overrides and faults | `faults`; `debugLevels` | `OverrideConfig` (`ceph/config.go`), `OverrideOSDFlag` (`ceph/osd_policy.go`) and `OverrideMGRModule` (`ceph/mgr_modules.go`) reserve and settle their kind through `reserveFault` and `settleFault`; each handle's `finish` calls `releaseFault`. The former maps `configOverrides`, `flagOverrides` and `moduleOverrides` are gone | The same functions, `Restore` (`tracksFault`) and `deletePool` through `allowPoolDeleteHeld` (`ceph/pool_removal.go`, "called with c.mu held") | All of it under `Container.mu`, taken in every entry point and `Restore`; the registry guards by `Container.mu` by contract (`ceph/fault_registry.go`) and has outside users (`ceph/fault_hooks.go`, `ceph/osd_retained.go`). `debugLevels`: its own mutex (`ceph/debug_level.go`) | Nothing is left to move out of `Container` for the three override kinds: the registry is the one tracking record, as [override-package-boundary.md](override-package-boundary.md) section 6.1 decided, and it is already its own type. `faults` and `debugLevels` are already their own types; moving the field adds nothing. |
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

## Limits of this note

- Source reading and a static parse of method declarations only. No native
  Ceph, Docker or timing behavior was run or measured for it.
- Call-site counts are textual (by method name) and overstate the number of
  `Container` call sites.
- go-ceph is referenced in the issue as a source comparison for dependency
  decomposition. It does not prove this module's lock order, restoration or
  Docker-generation safety, and no go-ceph dependency is introduced.
