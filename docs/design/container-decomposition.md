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
- **Fault registry.** `TemporaryBlocklist` and `OverrideFullRatios` reserve and
  settle entries in `Container.faults` (`reserveFault`, `settleFault`), guarded by
  `mu`. `OverrideDebugLevel` uses `debugLevels`, which has its own lock.
- **Override state.** `OverrideConfig`, `OverrideOSDFlag` and `OverrideMGRModule`
  keep their active handles in the maps `configOverrides`, `flagOverrides` and
  `moduleOverrides`, guarded by `mu`. Handles hold `owner *Container` and run
  `Restore` against it. The current lifetime of every family is in the table
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
  interruptions, pauses). It never visits the override maps. The extraction is
  structural only: it adds no automatic restoration and no uncertain-outcome
  reporting, and it creates no second ownership path.

  | Family | Who restores | After `Terminate` (read from the code) |
  | --- | --- | --- |
  | `OverrideConfig` (`configOverrides`) | The caller, through `ConfigOverrideHandle.Restore` | `Restore` calls `poolPolicyReady`, so it returns `errClusterTerminated` on a closed cluster. |
  | `OverrideMessengerPolicy` | The caller, through `MessengerPolicyOverrideHandle.Restore`, which restores its `ConfigOverrideHandle` values in reverse order | Same as `OverrideConfig`, because it is built from those handles. |
  | `OverrideOSDFlag` (`flagOverrides`) | The caller, through `Restore` | `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `OverrideMGRModule` (`moduleOverrides`) | The caller, through `Restore` | `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `TemporaryBlocklist` (fault kind `faultBlocklist`, phase `faultTracked`) | The caller, through `BlocklistOverride.Restore` | Tracked kinds are bookkeeping only and `terminate` leaves them alone (`fault_registry.go`). `Restore` calls `poolPolicyReady`: `errClusterTerminated`. |
  | `OverrideFullRatios` (fault kind `faultFullRatios`, phase `faultTracked`) | The caller, through `FullRatiosOverride.Restore` | Same bookkeeping rule. `Restore` goes through `fullRatiosSession`, which refuses a closed cluster. |
  | `OverrideDebugLevel` (`debugLevels`) | The caller, typically registered with `Finalization.Cleanup` as the `OverrideDebugLevel` documentation describes | Its `Restore` is driven by the handle's own state and generation checks. Its behavior after `Terminate` was not traced (unconfirmed). |

  In every family the handle is the only restoration path, and the cluster's
  termination ends the native state with the cluster rather than restoring it.
- **Package boundary design.** The owner preference for a public `override`
  package, its package map, import graph and dependency analysis are in
  [override-package-boundary.md](override-package-boundary.md). The shape above
  (functions in `ceph`) is the fallback, not the current direction.
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
| Overrides and faults | `configOverrides`, `flagOverrides`, `moduleOverrides`, `faults`, `debugLevels` |
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

## Limits of this note

- Source reading and a static parse of method declarations only. No native
  Ceph, Docker or timing behavior was run or measured for it.
- Call-site counts are textual (by method name) and overstate the number of
  `Container` call sites.
- go-ceph is referenced in the issue as a source comparison for dependency
  decomposition. It does not prove this module's lock order, restoration or
  Docker-generation safety, and no go-ceph dependency is introduced.
