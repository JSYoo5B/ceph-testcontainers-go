# Feature-client state

Moved without rewriting from the section "Third internal slice: feature-client state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

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

## Audit of the moved fields

Every production access to the three fields and the two locks, at `3dc0d13`:

| Field | Writers | Readers | Guard |
| --- | --- | --- | --- |
| `featureClient` | `clientContainerContext` publishes the started or partially started container (`ceph/images.go:171-173`); `terminate` clears it after a successful removal (`ceph/ceph.go:1114-1116`) | `clientContainerContext` returns it when a startup was attempted (`ceph/images.go:158`); `terminate` removes it (`ceph/ceph.go:1110-1111`); `featureClientSnapshot` (`ceph/images.go:198-204`), used by `monitorConfigTargets` (`ceph/topology.go:513`) and the native-command fence check (`ceph/native_command.go:626`); `diagnosticsSnapshot` (`ceph/diagnostics.go:277-283`) | Every writer holds `clientMu` and `clientPublishMu`. The readers in `clientContainerContext` and `terminate` hold `clientMu`; the snapshot readers hold `clientPublishMu` for reading. |
| `featureClientAttempted` | `clientContainerContext` (`ceph/images.go:166`) | `clientContainerContext` (`ceph/images.go:154`) | `clientMu` |
| `featureClientReady` | `clientContainerContext` (`ceph/images.go:193`); `terminate` (`ceph/ceph.go:1117`) | `clientContainerContext` (`ceph/images.go:155`) | `clientMu` |

Tests access the fields and `clientPublishMu` directly (`ceph/images_test.go`,
`ceph/monitor_config_test.go`); they moved to `Container.toolkit`.

## Why this component next, and why no new mutex

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

## Lock order preserved

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

## Guards, in `ceph/feature_client_state_test.go`

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

## Components not picked: audit for the next slice

Fields, writers, readers and locks as the code states them at `3dc0d13`. "By
contract" means the lock is named in a doc comment ("The caller holds c.mu")
and the caller chain was not checked here.

| Component | Fields | Writers | Readers | Locks | Why not now |
| --- | --- | --- | --- | --- | --- |
| Auth records | `authFences`, `clientSecrets`, `clientKeys`, `strayAuthInputs`, `clientRevocations`; seam `keyRotationDocker` | `settleClient` (`ceph/client_key.go:136,140`), `fenceAuth` (`ceph/daemon_key_rotation.go:369,374`), `settleEarlierRotation` (`:439`), `setClientKey` (`ceph/client_key.go:312-318`), `recordClientSecret` (`:330-332`), `strayAuthInput` (`ceph/daemon_key_rotation.go:152-154`), `createClient`/`deleteClient`/`revokeClient`/`ClientRevocationHandle.Restore` (`ceph/auth.go:306`, `ceph/client_revocation.go:59-69,107`) | The same files, plus `diagnosticsSnapshot` for `clientSecrets` (`ceph/diagnostics.go:263`, between the `TryLock` at `:219` and the unlock at `:267`) | `Container.mu`. The revocation code takes it itself (`lockTopology` before every access in `ceph/client_revocation.go` and `ceph/auth.go`). The fence and key helpers rely on a doc comment (`ceph/client_key.go:109,309,324`, `ceph/daemon_key_rotation.go:34,149,159`); `settleClient` alone has eight callers in five files (`ceph/auth.go:162,291`, `ceph/client_key.go:202,273`, `ceph/client_revocation.go:93`, `ceph/shared_backend.go:396,444`, `ceph/service_hooks.go:377`) | Guard by contract across five files and another object (`client.owner.setClientKey`, `ceph/service_hooks.go:368`). `authFences` serves both clients and daemons, so the rotation code moves with it. Needs a per-caller lock audit first. |
| OSD storage | `osds`, `detachedOSDs`, `osdMemory`, `osdVolume`, `run`; seams `osdRunner`, `osdVolumeDocker`, `osdVolumeRunner` | `osds`: `registerOSD` (`ceph/ceph.go:659`), `removeOSD` (`:863`), `terminate` (`:1046`), `detachOSD` (`ceph/osd_retained.go:274`), `reopenOSD` (`:322`), `serviceHooks.TestCluster` (`ceph/service_hooks.go:429,431`). `osdMemory`/`osdVolume`: `ensureOSDMemory` (`ceph/osd_memory_storage.go:109`), `ensureOSDVolume` (`ceph/osd_volume_storage.go:97`), `terminate` (`ceph/ceph.go:1051,1057,1128,1133`). `run`: once in `Run` (`ceph/ceph.go:234`) | `osds` is read in about twenty files (a name match that also finds the options field of the same name); `osdMemory`/`osdVolume` in `registerOSD`, `removeOSD`, `newOSDLaunchRecipe` (`ceph/osd_launch.go:47-54`) and `detachOSD`/`reopenOSD`; `run` in `ceph/admin_socket.go`, `ceph/debug_level.go:633`, `ceph/osd_retained.go:345`, `terminate` | `Container.mu` for the lifecycle writers (`removeOSD`, `detachOSD`, `reopenOSD`, `terminate` take `lockTopology`); `registerOSD` and `ensureOSD*` take no lock themselves (the caller chain was not checked); `newOSDLaunchRecipe` reads `run`, `osdMemory` and `osdVolume` (`ceph/osd_launch.go:47-54`) after its `configMu` read section and takes no other lock | `terminate` releases memory and volume in two phases around the container and labeled-container removal (`ceph/ceph.go:1049-1057`, `1126-1133`), so the close order of the component has to be reviewed. Largest reader set. Open PR #244 changes `ceph/osd_launch.go` and the `registerOSD` area. |
| Overrides and faults | `faults`; `debugLevels` | the configuration, OSD flag and MGR module overrides (at that revision methods in `ceph/config.go`, `ceph/osd_policy.go` and `ceph/mgr_modules.go`; now `override.SetConfig`, `override.SetOSDFlag` and `override.SetMGRModule`) reserve and settle their kind through `reserveFault` and `settleFault`; each handle's `finish` calls `releaseFault`. The former maps `configOverrides`, `flagOverrides` and `moduleOverrides` are gone | The same functions, `Restore` (`tracksFault`) and `deletePool` through `allowPoolDeleteHeld` (`ceph/pool_removal.go`, "called with c.mu held") | All of it under `Container.mu`, taken in every entry point and `Restore`; the registry guards by `Container.mu` by contract (`ceph/fault_registry.go`) and has outside users (`ceph/fault_hooks.go`, `ceph/osd_retained.go`). `debugLevels`: its own mutex (`ceph/debug_level.go`) | Nothing is left to move out of `Container` for the three override kinds: the registry is the one tracking record, as [override-package-boundary.md](../override-package-boundary.md) section 6.1 decided, and it is already its own type. `faults` and `debugLevels` are already their own types; moving the field adds nothing. |
| Test seams | `keyRotationDocker`, `ownedContainerDocker`, `osdRunner`, `osdVolumeDocker`, `osdVolumeRunner` | None in production code; tests assign them | One read each at the point of use (`ceph/daemon_key_rotation.go:730-731`, `ceph/owned_containers.go:35`, `ceph/ceph.go:713`, `ceph/osd_volume_storage.go:96,101`) | None | The rule above moves a seam with the dependency that uses it: three belong to OSD storage, one to the auth records, and `ownedContainerDocker` serves `terminate`'s labeled-container removal, which has no component yet. Open PR #244 adds a sibling seam (`daemonRunner`) next to `osdRunner`. |
| Membership and control | `mu`, `controlMu`, `configMu`, `extensionsMu`, `controlPlane`, `manager`, `monitors`, `managers`, `services`, `extensions`, `config`, `keyring`, `portLeases`, `closed`, `monitorTerminated`, `firstMonitorImageID`, `cephVersion`, `owner`, `keyTypeOption`, `settings`, `sharing` | `controlPlane`: `ensureControlPlane` (`ceph/topology.go:75`, `controlMu`), `terminate` (`ceph/ceph.go:1103`, `controlMu` inside the topology lock). `monitors`/`managers`/`manager`: `AddMonitor` (`ceph/topology.go:180,182`), `startManager` (`:768-781`, no lock in the function), `removeMonitor`/`removeManager`, `terminate`. `services`: `startService` (`ceph/ceph.go:979`), `serviceHooks.SetService`/`DeleteService` (`ceph/service_hooks.go:179-187`), `terminate`. `config`: `Run` (`ceph/ceph.go:245`), `configureDefaultCRUSHRoot` (`ceph/placement.go:141`, `configMu`), `refreshMonitorConfigAfterRemoval` (`ceph/topology.go:439`, `configMu`). `closed`: `terminate` (`ceph/ceph.go:1014`). `portLeases`: `trackHostPortLease` (`ceph/ceph.go:475`, `mu`), `reserveTrackedHostPortsWith`, `AddMonitor`. `cephVersion`: `recordCephVersion` (`ceph/version.go:51`). `owner`, `keyTypeOption`, `firstMonitorImageID`: `Run` | `settings` is read in 181 places by name and written only while `Run` builds the cluster; `closed` and `monitors` throughout; `FactsOf` reads `settings` and `cephVersion` without a lock | Four lock domains: `mu`, `controlMu`, `configMu`, `extensionsMu`; some fields are written once before publication | Not one component. Four locks and the lock-free `FactsOf` reads depend on the layout. A split by lock domain is a separate review. |

The components remain in this order of audited risk: auth records (needs the
caller audit; moved in the fourth slice below), the override maps (needs the owner's decision), OSD storage
(close order and the open per-daemon image PR), membership and control (four
locks).

## Not covered by this slice

The auth records, OSD storage, overrides and faults, the other test seams and
membership and control; any change to which lock guards what; any new lock; the
order in which `Terminate` closes the other components; a proof that the
callees of `clientContainerContext` never take `Container.mu` (read from the
source, not enforced by a guard); the `sharedBackend` port that #155 needs; and
any exported API. No Docker, native Ceph or timing behavior was run.
