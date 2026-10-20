# Design: extracting fault injection into package `fault`

Status: stage 2's registry and internal hooks and stage 3's network extraction
are implemented, including container pause as `fault.Pause`, returning
`*fault.PauseLease`. Admission and phase-3 `Resume` cleanup are preserved.
Blocklist, fullness and object errors remain in `ceph` for subsequent
checkpoints. See the [migration guide](../fault.md) for the current API.

The inventory below records the original stage 1 design. It inventories every fault-injection API of the module, decides which package
each one belongs to, records the contracts that the move must preserve, and
proposes the internal boundary built in stage 2. Historical references use `file:symbol` for the APIs inventoried below,
including rate, queue-limit and packet-fault support.

Stages 2 to 5 start only after this inventory and its contracts are
reviewed. A decision here that turns out wrong is changed in this document
first, in the stage that discovers it.

## Decisions

| # | Decision |
| --- | --- |
| D1 | A new public package `github.com/JSYoo5B/ceph-testcontainers-go/fault`, in the same module and release stream. `fault` imports `ceph`; `ceph` never imports `fault`. There is no `fault.Run` and no second fixture lifetime: every cluster-bound fault takes the existing `*ceph.Container`. |
| D2 | Eight entry points move: `TemporaryNetworkDelay`, `TemporaryLinkDelay`, both `InterruptNetwork` forms, `PauseContainer`, `TemporaryBlocklist`, `TemporaryFullRatios` and `InjectObjectDataError`, with their request, status and handle types, constants and netem sentinel errors (42 exported identifiers in all, see [Moves to `fault`](#moves-to-fault)). |
| D3 | Queries, topology, lifecycle, normal scrub and recovery and every general `Temporary*` control stay in `ceph`, including `TemporaryConfig`, `TemporaryMGRModule`, `TemporaryMessengerPolicy`, `TemporaryOSDFlag` and `TemporaryClientRevocation` (see [Ambiguous `Temporary*` APIs](#ambiguous-temporary-apis)). |
| D4 | Service-package fault helpers (`rbd.Mirror.InterruptPeerLink`, `cephfs.Mirror.InterruptPeerLink`, `rgw.Multisite.InterruptZoneLink`, `rgw.Multisite.TemporaryZoneLinkDelay`) stay methods of their fixtures, because they select fixture-owned endpoints and register cleanup with the fixture. Only their return and parameter types change to `fault` types. |
| D5 | Reversible leases keep their own undo (`Restore` or `Resume`). The one-shot `InjectObjectDataError` keeps returning no handle: its recovery is a rewrite (`RepairPG`) or an OSD restart. No universal `Restore`, `Close` or cleanup interface is introduced. |
| D6 | Stage 2 adds a ceph-owned fault registry inside `*ceph.Container` and an internal typed facade `internal/faulthook`, registered through `internal/hookreg` the way `internal/servicehook` is. Terminate keeps calling the registered undo functions in today's fixed phase order. No mutex, map or general cleanup registration becomes public. |
| D7 | No transitional wrappers in `ceph`. A `ceph.Container` method that returns a `fault` type would need `ceph` to import `fault`. The break ships in one minor release with an upgrade guide (see [Transitional wrappers](#transitional-wrappers)). |
| D8 | Shared-cluster admission stays enforced in `ceph`: every cluster-bound `fault` function asks `ceph` to admit it by name, with the same policy its method has today. |

## Package boundary

```
fault  ──imports──▶  ceph  ──imports──▶  internal/hookreg
  │                                      ▲
  └──imports──▶ internal/faulthook ──────┘   (typed view of the hooks ceph registers)

rbd, cephfs, rgw, internal/multicluster ──imports──▶ fault, ceph
```

- `ceph` keeps `Run`, `Terminate`, topology changes (`AddOSD`, `RemoveOSD`,
  `SetOSDIn`, daemon `Stop`/`Start`), general configuration, status and
  query APIs, and normal scrub and recovery (`DeepScrubPG`, `RepairPG`,
  `PGInconsistencies`, `WaitForPGClean`).
- `fault` owns dedicated fault operations: netem delay, loss, rate and
  packet faults; endpoint interruption; container pause; blocklist fencing;
  temporary fullness; injected object errors.
- `fault` may reuse `ceph` controls (for example a fault recipe built on
  `TemporaryConfig`), but a `Temporary` prefix alone does not move an API.

## API inventory

### Moves to `fault`

Proposed names are the stage 3 and stage 4 starting point; final spelling
is decided in those stages. Cluster-bound functions take the cluster as the
second parameter. The pause extraction chooses `fault.Pause` and
`*fault.PauseLease` as the final public names, with `Resume` unchanged.
The tables below retain the original stage 1 names.

| Today (`ceph`) | Proposed (`fault`) | Source |
| --- | --- | --- |
| `(*Container).TemporaryNetworkDelay(ctx, target, NetworkDelay)` | `TemporaryNetworkDelay(ctx, cluster, target, NetworkDelay)` | `ceph/network_delay.go:TemporaryNetworkDelay` |
| `TemporaryLinkDelay(ctx, ctr, network, NetworkDelay)` | `TemporaryLinkDelay(ctx, ctr, network, NetworkDelay)` | `ceph/network_delay.go:TemporaryLinkDelay` |
| `(*Container).InterruptNetwork(ctx, ctr, NetworkPlane)` | `InterruptNetwork(ctx, cluster, ctr, ceph.NetworkPlane)` | `ceph/network.go:(*Container).InterruptNetwork` |
| `InterruptNetwork(ctx, ctr, name)` | `InterruptLink(ctx, ctr, network)` (renamed: one package cannot hold both forms) | `ceph/network.go:InterruptNetwork` |
| `(*Container).PauseContainer(ctx, ctr)` | `PauseContainer(ctx, cluster, ctr)` | `ceph/pause.go:PauseContainer` |
| `(*Container).TemporaryBlocklist(ctx, address, duration)` | `TemporaryBlocklist(ctx, cluster, address, duration)` | `ceph/fencing.go:TemporaryBlocklist` |
| `(*Container).TemporaryFullRatios(ctx, FullRatios)` | `TemporaryFullRatios(ctx, cluster, ceph.FullRatios)` | `ceph/full_ratios.go:TemporaryFullRatios` |
| `(*Container).InjectObjectDataError(ctx, pool, object, osdID)` | `InjectObjectDataError(ctx, cluster, pool, object, osdID)` | `ceph/scrub.go:InjectObjectDataError` |

Types, methods, constants and variables that move with them:

| Kind | Identifiers | Source |
| --- | --- | --- |
| Request types | `NetworkDelay` (all fields, including `Rate`, `QueueLimit`, `Reorder`, `Duplicate`, `Corrupt`), `NetworkPeer`, `NetworkRate`, `NetworkPacketFault` | `ceph/network_delay.go` |
| Status type | `NetworkDelayStatus` | `ceph/network_delay.go` |
| Handle types | `NetworkDelayLease`, `NetworkInterruption`, `ContainerPause`, `BlocklistOverride`, `FullRatiosOverride` | `network_delay.go`, `network.go`, `pause.go`, `fencing.go`, `full_ratios.go` |
| Handle methods | `NetworkDelayLease.Restore`, `.Status`; `NetworkInterruption.Restore`; `ContainerPause.Resume`; `BlocklistOverride.Restore`, `.Address`; `FullRatiosOverride.Restore` | same files |
| Value methods | `NetworkRate.String`, `NetworkPacketFault.IsZero`, `NetworkPacketFault.String` | `ceph/network_delay.go` |
| Constants | `NetemHelperImage`, `MaxNetworkDelay`, `MaxNetworkPeers`, `BitPerSecond`, `KilobitPerSecond`, `MegabitPerSecond`, `GigabitPerSecond`, `MinNetworkRate`, `MaxNetworkRate`, `DefaultNetworkQueueLimit`, `MaxNetworkQueueLimit` | `ceph/network_delay.go` |
| Sentinel errors | `ErrNetemImageUnsupported`, `ErrNetemPermissionDenied`, `ErrNetemKernelUnsupported` | `ceph/network_delay.go` |

Count: 8 entry points, 10 types, 10 methods, 11 constants, 3 variables.

The sentinels keep their categories: `errors.Is(err, ceph.ErrImageCapability)`
and `errors.Is(err, ceph.ErrEnvironmentUnavailable)` stay true. `fault`
defines its own unexported category error type with the same `Is` rule as
`ceph/errors.go:categoryError`; refusals use the public
`&ceph.OperationError{Kind: ceph.ErrInvalidRequest}` as
`internal/multicluster/resources.go:invalid` already does.

### Stays in `ceph`

| Identifier | Reason |
| --- | --- |
| `NetworkPlane`, `PublicNetworkPlane`, `ClusterNetworkPlane`, `NetworkName`, `ClusterNetworkName`, `HasSeparateClusterNetwork` | Topology of the cluster's bridges. `fault` request types refer to `ceph.NetworkPlane`. |
| `BlocklistEntry`, `(*Container).BlocklistEntries`, `(*Tenant).BlocklistEntries` | Read-only query with a tenant counterpart (`sharedOpen`). |
| `FullRatios`, `FullRatioSnapshot`, `(*Container).FullRatios` | Query of OSDMap policy. `FullRatios` is also the request type of `fault.TemporaryFullRatios`. |
| `DeepScrubPG`, `RepairPG`, `PGInconsistencies`, `InconsistentObject`, `InconsistentShard`, `WaitForPGClean`, `WaitForClean` | Normal scrub, repair and recovery. `RepairPG` is also the documented recovery of an injected error. |
| `SetOSDIn`, `RemoveOSD`, `AddOSD`, daemon `Stop`/`Start` | Topology and lifecycle. The fault guide lists them as techniques, which does not move them. |
| `DaemonAddresses`, `HealthDetails`, `PoolPGs`, `OSDFlags`, `OSDStates` | Observation used by fault tests. |
| `TemporaryConfig`, `TemporaryMGRModule`, `TemporaryMessengerPolicy`, `TemporaryOSDFlag`, `TemporaryClientRevocation` and their handles | See the next section. |

### Ambiguous `Temporary*` APIs

| API | Decision | Reason |
| --- | --- | --- |
| `TemporaryConfig` / `ConfigOverride` (`ceph/config.go`) | Stays | General central configuration. The fault guide's `ms_blackhole_*`, `ms_inject_delay_*` and `osd_debug_inject_dispatch_delay_*` recipes are uses of it, not a fault API. `TemporaryMessengerPolicy` and `pool_removal.go:RemovePool` (its `mon_allow_pool_delete` check) depend on `c.configOverrides`. |
| `TemporaryMGRModule` / `MGRModuleOverride` (`ceph/mgr_modules.go`) | Stays | Module membership, coupled to service bootstrap requirements through `extensionsWith[serviceModules]`. Not a failure condition. |
| `TemporaryMessengerPolicy` / `MessengerPolicyOverride` (`ceph/messenger_policy.go`) | Stays | Connection-mode policy composed from `TemporaryConfig` overrides; it changes which modes daemons accept, it does not inject a failure. |
| `TemporaryOSDFlag` / `OSDFlagOverride` (`ceph/osd_policy.go`) | Stays | Ordinary operational OSDMap flags (`noout`, `noscrub`, `norebalance` ...) also used for maintenance and recovery control. Pairing it with `PauseContainer` (`nodown`) is a recipe. |
| `TemporaryClientRevocation` / `ClientRevocation` (`ceph/client_revocation.go`) | Stays | Client identity lifecycle. It depends on `authFences`, `clientKeys` and `clientRevocations` and the fencing of `ReplaceClientKey`, and it is `sharedTenant`. Moving it means designing an auth boundary, which is outside this refactor. |
| `cephfs.Filesystem.TemporarySessionTimeouts`, `TemporarySnapshotSchedule`, `TemporarySubvolumePin`, `TemporarySubvolumeGroupPin` | Stay in `cephfs` | MDS and file system configuration. |
| `rbd.TemporaryImmutableObjectCache` | Stays in `rbd` | Client feature lease, not a fault. |

### Service-package fault helpers

| API | Today | After the move |
| --- | --- | --- |
| `rbd.Mirror.InterruptPeerLink` (`rbd/network.go`) | returns `*ceph.NetworkInterruption`; calls `internal/multicluster/network.go:InterruptOwnedPeerLink`, which calls `ceph.InterruptNetwork` and registers `link.Restore` with `multicluster.Resources.AddCleanup` | returns `*fault.NetworkInterruption`; `InterruptOwnedPeerLink` calls `fault.InterruptLink` |
| `cephfs.Mirror.InterruptPeerLink` (`cephfs/network.go`) | same | same |
| `rgw.Multisite.InterruptZoneLink` (`rgw/network.go`) | same, on the private multisite HTTP bridge | same |
| `rgw.Multisite.TemporaryZoneLinkDelay` (`rgw/network.go`) | takes `ceph.NetworkDelay`, returns `*ceph.NetworkDelayLease`; calls `ceph.TemporaryLinkDelay`, registers `lease.Restore` with `f.owned` | takes `fault.NetworkDelay`, returns `*fault.NetworkDelayLease`; calls `fault.TemporaryLinkDelay` |

`multicluster.Resources.Terminate` runs cleanups in reverse construction
order and keeps completed ones, so a failed restore stays retryable. This is
the caller-owned path and has nothing to do with `ceph.Container.Terminate`.

## Contracts per family

Shared policies are those of `ceph/shared_gate.go:sharedMethodPolicies`:
`sharedExclusive` is refused on a cluster shared with tenants and allowed
under `SharedCluster.Exclusive`; `sharedRecycle` likewise, and the cluster
is retired after the exclusive lease in either case
(`ceph/shared_exclusive.go:Exclusive`).

### Netem delay, loss, rate and packet faults (cluster-bound)

| Aspect | Current behavior (`ceph/network_delay.go`) |
| --- | --- |
| Apply | `TemporaryNetworkDelay`: `sharedGate`, `validateNetworkDelay`, `reserveNetworkDelay`, `applyNetworkDelay` without `c.mu`, `settleNetworkDelay`. `applyNetworkDelay` inspects the target, `admitNetemTarget` refuses stopped, paused and shared-namespace targets, checks IPv6 peers against the endpoint, runs `preflightNetem` in a network-less helper (image contract, `NET_ADMIN`, kernel netem, rate, each requested packet fault, prio and u32 for peers), starts the helper by image ID in `container:<id>`, finds the interface by address and MAC, requires root qdisc `0:`, re-checks the sandbox ID, then adds the netem root, or the prio root, netem child and one u32 filter per peer leaf. It reads back and compares delay, jitter, loss, rate, queue limit (`DefaultNetworkQueueLimit` when unset) and, through `checkFaults`, `Reorder`, `Duplicate`, `Corrupt` and the gap. |
| Status | `NetworkDelayLease.Status`: `settle` (replace a stopped helper, cancel a pending tc command) then `read`, which identifies the interface by index and namespace and reports `Gone` after a restart, removal or disconnect. `Applied` requires the exact owned hierarchy (`ownedPrefix`, `checkPeerFilter`). After `Restore` it is `ErrInvalidRequest`. |
| Undo | `NetworkDelayLease.Restore`: retryable, idempotent once complete. Removes the owned root only while everything under it is what the apply added; foreign qdiscs or filters, or a replaced root, are refused and the lease stays retryable. A gone interface needs no removal. Then removes the helper and calls `release`. |
| Shared policy | `sharedExclusive`. |
| Overlap and identity | One lease per `network/containerID` key in `c.delays`. A `nil` value is a reservation; a second request is refused (`ErrInvalidRequest`) with nil handle while reserved or held. Identity: sandbox ID, namespace name, interface index, MAC and address; helper image pinned by ID. Host-network clusters, unattached caller containers and non-default root qdiscs are refused. |
| Cancellation and uncertainty | Each tc step runs supervised; `supervised.Unconfirmed` stores the marker in `pending`, and the next `Status` or `Restore` cancels and confirms it before acting. A failed first step reads back and discards the lease only when the kernel proves nothing was installed. No mutation is replayed. |
| Errors with a non-nil handle | Whenever the change may have reached the kernel: an unconfirmed or failed later step, a readback error, or a readback that differs from the request. If `Terminate` started while the delay was being applied, `settleNetworkDelay` reports it and the call returns the lease with `errNativeTerminated` joined with the result of its own `Restore`. |
| Terminate | Restored first, with `c.mu` released around each `Restore`, while targets and helpers still run. A failed restore is reported and stays retryable through the lease; it does not block network removal. Helpers carry the cluster's owner label (`dockerNetemBackend.label`, `ownedLabel`), so `removeLabeledContainers` later removes leaked helpers. |

### Netem link delay (caller-owned)

| Aspect | Current behavior |
| --- | --- |
| Apply, status, undo | `TemporaryLinkDelay` → `temporaryLinkDelay` → `validateDelay(delay, false)` and the same `applyNetworkDelay`. `Plane` must stay empty; a network that is not a bridge is refused. Same `Status` and `Restore`. |
| Ownership | No cluster knows it: no reservation, no `release`, no Terminate restore, helper without a cluster label (`dockerNetemBackend{}`). The caller restores before removing the container or the network. The RGW fixture registers the restore with its own `multicluster.Resources`. |
| Shared policy | None: there is no cluster to gate. Moving it must not add one. |
| Overlap | Only the kernel check: a non-default root qdisc is refused. |

### Network interruption

| Aspect | Current behavior (`ceph/network.go`) |
| --- | --- |
| Apply | Cluster form: `sharedGate`, `lockTopology` held for the whole call, refuses closed, host-network, nil or unstarted containers and invalid planes (plain errors, not `ErrInvalidRequest`), checks overlap, then calls the standalone form and records any returned handle in `c.interruptions[network/id]`. Standalone form: refuses `host`, `none`, `bridge`; requires an IPv4 endpoint; captures `restoreEndpoint` (IPv4, dual-stack IPv6, aliases, gateway priority); disconnects. |
| Undo | `NetworkInterruption.Restore`: retryable, no-op when complete. Reconnects with the original addresses. An externally reattached endpoint with another address, IPv6 address, gateway priority or missing aliases is refused, not overwritten. A removed container completes the handle; a missing network does not. |
| Shared policy | Cluster form `sharedExclusive`; standalone none. |
| Overlap and identity | Cluster form: an incomplete previous handle for the key is returned **with** the error `restore the existing endpoint interruption first`. Completed handles stay in the map and are replaced. No cross-check against a netem lease on the same endpoint: an interruption removes the interface and the lease then reports `Gone`. |
| Cancellation and uncertainty | A Docker transport error after the handle exists returns the handle with the error: it cannot prove the disconnect did not happen. |
| Terminate | Restored second, with `c.mu` held. A failed restore is reported, stays retryable, and marks its network in `blockedNetworks`, so `terminate` does not remove that bridge. |

### Container pause

| Aspect | Current behavior (`ceph/pause.go`) |
| --- | --- |
| Apply | `sharedGate`, `lockTopology` held for the whole call, refuses a closed cluster and nil containers, refuses the control CLI container (`controlContainerContext`), checks overlap, inspects and refuses a container that is not running, already paused or restarting, then pauses. |
| Undo | `ContainerPause.Resume`: retryable and idempotent; a removed, stopped or unpaused container completes the handle without starting it. |
| Shared policy | `sharedExclusive`. Host networking is supported. |
| Overlap | An incomplete previous handle for the container ID is returned **with** the error `resume the existing container pause first`. |
| Cancellation and uncertainty | A pause transport error returns the handle with the error. |
| Terminate | Resumed third, with `c.mu` held, before native commands are stopped and before any container stops, because a frozen process cannot handle the stop signal. |

### Blocklist fencing

| Aspect | Current behavior (`ceph/fencing.go`) |
| --- | --- |
| Apply | `sharedGate`, `normalizeBlocklistSession` (exact `IP:port/nonce`, nonzero nonce, one address of a v1/v2 vector), duration of at least one second, `c.mu.Lock()` (not context-aware) for the whole call, `poolPolicyReady`, refuses an existing native entry for the session, records the handle in `c.blocklistOverrides` before `osd blocklist add`, then reads back with a 5 s context detached from cancellation and captures `Until`. |
| Undo | `BlocklistOverride.Restore`: refuses an untracked copy (`tracked.state != change.state`), treats a missing entry as natural expiry, refuses when the apply readback was unknown, refuses an entry whose `Until` changed (outside renewal), removes, and checks absence. Copies share state. |
| Shared policy | `sharedExclusive`. |
| Errors with a non-nil handle | Command or readback failure, or success without a stored entry. |
| Terminate | Not restored: the entry dies with the cluster. The map entry is bookkeeping for overlap and identity. |

### Temporary full ratios

| Aspect | Current behavior (`ceph/full_ratios.go`) |
| --- | --- |
| Apply | `sharedGate`, `normalizeFullRatios` (float32, ordered, `<= 1`), deadline from `startupTimeout`, `lockTopology` for the whole call, refuses an unrestored previous override, opens `fullRatiosSession` (control container captured once, FSID from `c.config` under `configMu`), requires ordered original ratios, records the handle in `c.fullRatiosOverride`, then `transition` in ordered single-threshold steps. |
| Undo | `FullRatiosOverride.Restore`: identity by shared state pointer and bootstrap FSID; `reconcile` accepts only the expected tuple or either side of one uncertain step (`pending`), refuses outside edits and older prefixes. |
| Shared policy | `sharedExclusive`. |
| Errors with a non-nil handle | Any failure inside `transition`, including canceled contexts and lost readbacks; the journaled `pending` tuple lets `Restore` reconcile. |
| Terminate | Not restored: map policy dies with the cluster. |

### Injected object data error (one-shot)

| Aspect | Current behavior (`ceph/scrub.go:InjectObjectDataError`) |
| --- | --- |
| Apply | `sharedGate`, requires `OSDBlueStore`, validates pool and object names, `lockTopology` for the whole call, `poolPolicyReady`, requires an owned, unpurged OSD, a replicated pool, the OSD in the object's acting set, then `tell osd.N config set bluestore_debug_inject_read_err true`, `checkPoolID`, and `tell osd.N injectdataerr`. |
| Recovery | No handle and no `Restore`. The injection lasts until the replica is rewritten (`RepairPG`) or the OSD restarts. The runtime setting `bluestore_debug_inject_read_err` stays true until the OSD restarts, also after `RepairPG`. |
| Shared policy | `sharedRecycle`. |
| Uncertainty | A failed command returns an error only; the caller observes with `DeepScrubPG`, `PGInconsistencies` and `HealthDetails`. Nothing is replayed. |
| Terminate | Nothing to do. |

## Dependencies on `*ceph.Container` private state

| State or helper (`ceph/ceph.go:Container` unless noted) | Used by | Guarded by |
| --- | --- | --- |
| `mu` via `lockTopology` (`topology.go`) / `lockTopologyMutex` (`context_lock.go`, `internal/clusterutil.LockMutex`) | all cluster-bound families; `TemporaryBlocklist` and `BlocklistOverride.Restore` use plain `c.mu.Lock()` | itself |
| `closed` | delay reservation and settlement, interruption, pause, full ratios session; set by `terminate` | `mu` |
| `delays map[string]*NetworkDelayLease` (nil value = reservation) | `reserveNetworkDelay`, `settleNetworkDelay`, lease `release`, `terminate` | `mu` |
| `interruptions map[string]*NetworkInterruption` | `InterruptNetwork`, `terminate` | `mu` |
| `pauses map[string]*ContainerPause` | `PauseContainer`, `terminate` | `mu` |
| `blocklistOverrides map[string]*BlocklistOverride` | `TemporaryBlocklist`, `Restore`, `finish` | `mu` |
| `fullRatiosOverride *FullRatiosOverride` | `TemporaryFullRatios`, `Restore` | `mu` |
| `ownsNativeTarget` (`native_command.go`) | delay reservation (skips the attachment check for owned daemons) | caller holds `mu` |
| `ownedLabel` (`owned_containers.go`), `newNetemBackend`, `netemBackend()` | helper labeling and the unit-test seam | none |
| `controlContainerContext` (`topology_snapshot_context.go`) / `controlMu` | pause refusal, full ratios session, `cephCLI` | `controlMu` (read) |
| `config`, `keyring` / `configMu` | full ratios FSID | `configMu` (read) |
| `cephCLI` (`ceph.go`), `poolPolicyReady` (`pool_policy.go`), `poolState`, `checkPoolID` | blocklist, object error | caller holds `mu` |
| `osds`, `OSDContainer.purged`, `settings.objectStore()`, `settings.startupTimeout` | object error, blocklist and full ratios deadlines | `mu` |
| `sharing` / `sharedGate` (`shared_gate.go`) | every cluster-bound entry point | atomic |
| `errClusterTerminated`, `errNativeTerminated` (`native_command.go`), `invalidRequest`, `classify`, `classifyDocker`, `categoryError` (`errors.go`), `onlyMissingHostResource` (`host_network.go`), `nativeFailure` | errors of every family | none |

## Lock ordering

| Order | Where | Rule that must survive |
| --- | --- | --- |
| `NetworkDelayLease.mu` → `c.mu` | `Restore` calls `release`, which takes `c.mu` | Never take a lease mutex while holding `c.mu`. `reserveNetworkDelay` deliberately does not inspect the held lease; `terminate` unlocks `c.mu` around each `delay.Restore`. |
| `c.mu` → `NetworkInterruption.mu` | `InterruptNetwork` (`isComplete`), `terminate` | `NetworkInterruption.Restore` never takes `c.mu`. |
| `c.mu` → `ContainerPause.mu` | `PauseContainer` (`isComplete`), `terminate` | `ContainerPause.Resume` never takes `c.mu`. |
| `c.mu` → `controlMu` (read), `c.mu` → `configMu` (read) | pause, full ratios, `cephCLI` | unchanged |
| Cancellation before `c.mu` (sequence, not nesting) | `terminate` calls `cancelNativeInvocations` before `lockTopology`; it takes `nativeMu`, collects and cancels the tracked invocations, and releases `nativeMu` before returning (`ceph/native_command.go:cancelNativeInvocations`) | Cancel native invocations before waiting for `c.mu`, whose holder may be such an invocation's caller, and never wait for `c.mu` while holding `nativeMu`. Faults run no native invocations of their own. |
| `c.mu` → `nativeMu` | `terminate` calls `stopNativeInvocations` with `c.mu` held, after resuming pauses and again at the end of teardown (`ceph/ceph.go:terminate`); it takes `nativeMu` through `cancelNativeInvocations` and when it deletes tracked invocations | The nested order is `c.mu` → `nativeMu`. Keep it: code holding `nativeMu` must not take `c.mu`. |

## Terminate cleanup ordering

`ceph/ceph.go:terminate`, after `cancelNativeInvocations` and with `c.mu`
held and `closed` set:

1. Netem leases from `c.delays` (reservations skipped), each restored with
   `c.mu` released. Errors are collected.
2. Interruptions, under `c.mu`. A failure is collected and blocks removal
   of its network.
3. Pauses, under `c.mu`. Errors are collected.
4. `stopNativeInvocations`, host port leases, service containers, OSDs, OSD
   memory, managers, monitors, primary MON, control container, labeled
   containers (including leaked netem helpers), run volume, then the
   cluster and public bridges unless blocked, then a final
   `stopNativeInvocations`.

Terminate is retryable: handles that failed stay in their maps, completed
interruptions and pauses are no-ops, and `closed` keeps refusing new faults.
`SharedCluster` exclusive release retires the cluster through the same
internal `terminate`. Blocklist entries, full ratios and injected errors are
not undone; they end with the cluster.

## Reversible leases and one-shot injections

| Operation | Class | Undo | Undone by Terminate |
| --- | --- | --- | --- |
| `TemporaryNetworkDelay` | reversible lease | `NetworkDelayLease.Restore` | yes, phase 1 |
| `TemporaryLinkDelay` | reversible lease, caller-owned | `NetworkDelayLease.Restore` | no; the caller or fixture restores |
| `InterruptNetwork` (cluster) | reversible lease | `NetworkInterruption.Restore` | yes, phase 2 |
| `InterruptNetwork` (standalone) | reversible lease, caller-owned | `NetworkInterruption.Restore` | no |
| `PauseContainer` | reversible lease | `ContainerPause.Resume` | yes, phase 3 |
| `TemporaryBlocklist` | reversible override, may also expire | `BlocklistOverride.Restore` | no, dies with the cluster |
| `TemporaryFullRatios` | reversible override | `FullRatiosOverride.Restore` | no, dies with the cluster |
| `InjectObjectDataError` | one-shot injection | none: `RepairPG` rewrites the replica, or an OSD restart clears it and the debug setting | no |

## Proposed stage 2 boundary

Stage 2 changes internals only. The existing `ceph` entry points stay and
are rewritten on top of the boundary, so stage 3 moves code without changing
how it reaches cluster state.

### Fault registry inside `ceph`

A new `ceph/fault_registry.go` replaces `delays`, `interruptions`, `pauses`,
`blocklistOverrides` and `fullRatiosOverride` with one table guarded by
`c.mu`:

```go
type faultPhase int

const (
	faultTracked  faultPhase = iota // overlap and identity only: blocklist, full ratios
	faultShaping                    // netem: undone first, with c.mu released
	faultEndpoint                   // interruptions: under c.mu; failure blocks its network
	faultFreeze                     // pauses: under c.mu, before native commands stop
)

type faultEntry struct {
	kind, key string
	phase     faultPhase
	network   string                               // faultEndpoint only
	handle    any                                  // the public handle, for overlap replies
	undo      func(context.Context) error          // nil while reserved
	complete  func(context.Context) (bool, error)  // nil means "held until released"
}
```

Operations, all under `c.mu`:

- `reserveFault(kind, key, phase)`: refuses after `closed`, refuses a held
  or reserved key, or returns the previous incomplete handle for kinds whose
  contract returns it (interruption, pause). The returned `*faultEntry` is
  the identity token.
- `settleFault(token, handle, undo, complete)`: records the handle and
  reports whether `terminate` started meanwhile.
- `releaseFault(token)`: deletes the key only when it still maps to this
  token (the `c.delays[key] == lease` and `tracked.state != change.state`
  checks of today).
- `terminate` snapshots entries per phase and runs phases in the order
  above, unlocking only for `faultShaping`, with the same error collection
  and `blockedNetworks` rule.

The phase is fixed per kind by `ceph`; callers cannot add phases, order or
arbitrary cleanups.

### `internal/faulthook`

A typed facade like `internal/servicehook`, registered by `ceph` in `init`
as a second value in `internal/hookreg` (`hookreg.Fault`), implemented by a
new unexported `faultHooks` type in `ceph/fault_hooks.go`. Its methods take
`*ceph.Container`:

| Hook | Wraps | Needed by |
| --- | --- | --- |
| `Admit(c, op)` | `sharedGate` with a `fault.<op>` entry in a new `sharedFaultPolicies` table | all cluster-bound functions |
| `Reserve`, `Settle`, `Release` | the registry above | delay, interruption, pause, blocklist, full ratios |
| `WithTopology(ctx, c, fn)` | `lockTopology`, `fn`, `Unlock` | interruption, pause, full ratios, object error (their whole call holds `c.mu` today) |
| `WithTopologyNoContext(c, fn)` | `c.mu.Lock` | blocklist, until a separate change makes it context-aware |
| `Owns(c, id)` | `ownsNativeTarget` | delay reservation |
| `OwnedLabel(c)` | `ownedLabel` | netem helpers |
| `Control(ctx, c)` | `controlContainerContext` | pause refusal, full ratios session |
| `CLI(ctx, c, args...)` | `cephCLI` | blocklist, full ratios, object error |
| `ClusterIdentity(ctx, c)` | existing `serviceHooks.ClusterIdentity` logic | full ratios |
| `PoolPolicyReady`, `PoolState`, `CheckPoolID`, `OwnedOSD(c, id)`, `BlueStore(c)`, `StartupTimeout(c)` | the private helpers of the same names | blocklist, object error, deadlines |
| `Terminated()` | `errNativeTerminated`, `errClusterTerminated` | settle after close |
| `ClassifyDocker`, `NativeFailure`, `OnlyMissingHostResource` | the error helpers | all families |

Rules for `fn` passed to `WithTopology`: it must not lock a
`NetworkDelayLease` mutex, must not call a public `ceph.Container` method
that takes `c.mu`, and returns before the hook unlocks.

Considered and rejected:

- Reusing `serviceHooks.Extension` with an interface discovered through
  `extensionsWith`: it suits per-service state that `ceph` reads, but it
  cannot make reservation atomic with `closed`, and `terminate` would have
  to hand lock control to `fault`.
- Adding the fault methods to `internal/servicehook`: it would give rbd,
  cephfs and rgw the registry and give `fault` the service bootstrap hooks.
- A public `Cleanup` interface or `Container.RegisterCleanup`: excluded by
  the issue, and it would let callers break the phase order.

### Stage 2 as implemented

Stage 2 follows the boundary above with these differences, each taken from
the code's behavior rather than from the proposal:

- **Closed is checked by each family, not by `reserveFault`.** The
  families refuse a closed cluster with different errors and in different
  places: delay with `errClusterTerminated` right before reserving,
  interruption and pause with their plain precondition error, blocklist
  through `poolPolicyReady`, and full ratios only inside
  `fullRatiosSession`, after its overlap refusal. A closed check inside
  `reserveFault` would change the full ratios error and its precedence.
  `reserveFault` therefore requires the caller to have refused `closed` in
  the same `c.mu` section, which keeps the reservation atomic with
  `closed`, and the `faulthook` facade gains a `Closed` hook.
- **Completion decides the overlap reply.** An entry with a `complete`
  probe (interruption, pause) is checked under `c.mu` and its handle is
  returned with the refusal while it is incomplete. An entry without one
  (delay, blocklist, full ratios) is held until `releaseFault` and refused
  with a nil handle. The blocklist and full ratios `restored` flags are set
  in the same `c.mu` section that removes them, so presence in the
  registry is the same test as `!restored`.
- **A failed replacement keeps the complete entry.** Today a complete
  interruption or pause stays in its map when a new attempt returns no
  handle. A reservation records the complete entry it replaced
  (`faultEntry.replaced`) and puts it back when it settles without a
  handle.
- **Identity checks use `tracksFault(token)`.** The blocklist and full
  ratios `Restore` refusals of an untracked handle compare the token stored
  in the shared state that copies share. `faulthook` exposes this as
  `Tracks`; `Settle` returns an error for a token that no cluster issued.
- **Terminate snapshots each phase.** `undoFaults` collects the held
  entries of a phase before undoing them instead of ranging over a live map
  across the unlocked shaping undo. A lease settled while `c.mu` is
  released is restored by its own caller, because `settleFault` reports
  `closed`; a lease released meanwhile is a no-op `Restore`.
- **The entry points use the registry directly.** The five tracked
  families call the unexported registry; `InjectObjectDataError`,
  `TemporaryLinkDelay` and the standalone `InterruptNetwork` hold no
  registry state and are unchanged. `internal/faulthook` and
  `hookreg.Fault` exist with the hooks listed above plus `Closed` and
  `Tracks`, and have no caller outside their own test until stage 3.
- **Validation.** Besides the unit tests, the families whose bookkeeping
  moved were run natively (delay, bandwidth, packet faults, interruption,
  pause, blocklist and full ratios tests) on the release and role image
  sets.

### Shared-cluster admission after the move

`TestSharedGateClassifiesEveryMethod` (`ceph/shared_gate_test.go`) reflects
over `*Container` methods, so moved functions would silently leave it. Stage
2 adds `sharedFaultPolicies` (keys `fault.TemporaryNetworkDelay` and so on,
with today's policies) and the `Admit` hook. From stage 3, an external test
in `fault` lists every exported function that takes a `*ceph.Container` with
`go/parser`, requires a policy for each, and calls each on a shared test
cluster (`servicehook.TestCluster`-style constructor) to check
`ErrSharedCluster`.

## Consumer migration

| Before | After |
| --- | --- |
| `lease, err := cluster.TemporaryNetworkDelay(ctx, osd, ceph.NetworkDelay{HelperImage: ceph.NetemHelperImage, Delay: 50 * time.Millisecond})` | `lease, err := fault.TemporaryNetworkDelay(ctx, cluster, osd, fault.NetworkDelay{HelperImage: fault.NetemHelperImage, Delay: 50 * time.Millisecond})` |
| `ceph.NetworkDelay{Plane: ceph.ClusterNetworkPlane, Rate: 10 * ceph.MegabitPerSecond, QueueLimit: 200}` | `fault.NetworkDelay{Plane: ceph.ClusterNetworkPlane, Rate: 10 * fault.MegabitPerSecond, QueueLimit: 200}` |
| `ceph.NetworkDelay{Delay: d, Reorder: ceph.NetworkPacketFault{Percent: 25}}` | `fault.NetworkDelay{Delay: d, Reorder: fault.NetworkPacketFault{Percent: 25}}` |
| `ceph.TemporaryLinkDelay(ctx, ctr, network, delay)` | `fault.TemporaryLinkDelay(ctx, ctr, network, delay)` |
| `cluster.InterruptNetwork(ctx, ctr, ceph.PublicNetworkPlane)` | `fault.InterruptNetwork(ctx, cluster, ctr, ceph.PublicNetworkPlane)` |
| `ceph.InterruptNetwork(ctx, ctr, name)` | `fault.InterruptLink(ctx, ctr, name)` |
| `cluster.PauseContainer(ctx, osd)` | `fault.Pause(ctx, cluster, osd)` |
| `cluster.TemporaryBlocklist(ctx, addr, time.Minute)` | `fault.TemporaryBlocklist(ctx, cluster, addr, time.Minute)` |
| `cluster.TemporaryFullRatios(ctx, ceph.FullRatios{...})` | `fault.TemporaryFullRatios(ctx, cluster, ceph.FullRatios{...})` |
| `cluster.InjectObjectDataError(ctx, pool, obj, 1)` | `fault.InjectObjectDataError(ctx, cluster, pool, obj, 1)` |
| `errors.Is(err, ceph.ErrNetemKernelUnsupported)` | `errors.Is(err, fault.ErrNetemKernelUnsupported)`; `errors.Is(err, ceph.ErrEnvironmentUnavailable)` unchanged |
| `var l *ceph.NetworkInterruption = mirror.InterruptPeerLink(...)` | `var l *fault.NetworkInterruption = mirror.InterruptPeerLink(...)` |
| `multisite.TemporaryZoneLinkDelay(ctx, a, b, ceph.NetworkDelay{...})` | `multisite.TemporaryZoneLinkDelay(ctx, a, b, fault.NetworkDelay{...})` |

Call sites in the pre-extraction inventory (`.Method(` occurrences): `ceph` unit tests 101
(`TemporaryNetworkDelay` 67, `TemporaryFullRatios` 13, `InjectObjectDataError`
9, `TemporaryBlocklist` 7, `PauseContainer` 3, `InterruptNetwork` 2),
`internal/integration` 59 in 34 files (`TemporaryNetworkDelay` 25,
`PauseContainer` 17, `InterruptNetwork` 7, `TemporaryBlocklist` 4,
`TemporaryFullRatios` 3, `InjectObjectDataError` 3), `internal/dockerbridge`
2, `internal/multicluster` 1, `rgw` 1. Documentation: `docs/ceph.md`,
`docs/rbd.md`, `docs/cephfs.md`, `docs/rgw.md`, `docs/guides/fault-injection.md`,
`contexts-and-cleanup.md`, `errors.md`, `host-network.md`,
`messenger-client-fixtures.md`, `native-commands.md`,
`releases-and-images.md`, `docs/contributing/ci.md`.

## Transitional wrappers

Keeping `(*ceph.Container).TemporaryNetworkDelay` as a deprecated wrapper
would need either `ceph` to import `fault` (a cycle) or the handle types to
stay in `ceph` with `fault` aliasing them, which keeps the implementation
and its state in `ceph` and defeats the extraction. An internal shared
implementation package that both alias would work, but it is a third
location for every type and outlives the transition. Recommendation: no
wrappers. Stages 3 and 4 break the API on main; the break is released once,
after stage 5, with `docs/guides/upgrading-to-v0.4.md` holding the table
above. Stage 5 confirms or overturns this explicitly.

## Stage checkpoints

| Stage | Moves | Must preserve | Validation |
| --- | --- | --- | --- |
| 2. Internal boundary | Nothing public. Adds `ceph/fault_registry.go`, `ceph/fault_hooks.go`, `internal/faulthook`, `hookreg.Fault`, `sharedFaultPolicies`; rewrites the eight entry points and `terminate` onto them. | Atomic reservation against `closed`; settle-after-close self-restore with `errNativeTerminated`; handles returned with errors; previous-handle replies of interruption and pause; nil replies of delay, blocklist and full ratios overlap; token identity on release; phase order and lock mode per phase; `blockedNetworks`; retryable Terminate; lock ordering table. | Existing unit tests unchanged and passing (`network_delay*_test.go`, `network_link_delay_test.go`, `network_packet_faults_test.go`, `network_rate_test.go`, `pause_test.go`, `fencing_test.go`, `full_ratios_test.go`, `scrub_test.go`, `ceph_test.go` Terminate cases, `context_lock_test.go`, `shared_gate_test.go`). New registry tests: reserve after close, double reserve, settle after close, release by stale token, phase order with a failing undo per phase, retry after failure, concurrent `Restore` and `Terminate` under `-race`. Full pre-push check. No native run needed: no Docker behavior changes. |
| 3. Network faults | `fault` package with netem delay (cluster and link), interruption (cluster and link), their types, constants and sentinels, netem backend and its test seam; unit tests move with them; rbd, cephfs, rgw, `internal/multicluster`, `internal/dockerbridge` and integration call sites change in the same change. | Helper capability and kernel admission, peer, port and address-family selection, exact qdisc and filter ownership, foreign-state refusal, `Gone` detection, IPv4-only standalone interruption, helper labels, caller-owned obligations of link faults. | Unit tests in `fault`; the `Admit` enumeration test; `go vet`, race, lint, script tests. Native: `TestNetworkDelay`, `TestNetworkDelayPeerIPv6`, `TestNetworkBandwidth`, `TestNetworkBandwidthIPv6`, `TestNetworkBandwidthSevere`, `TestNetworkPacketFaults`, `TestNetworkPacketFaultsIPv6`, `TestOSDReplicationPeerPathCut`, `TestDualStackFamilyCut`, `TestSeparateClusterNetworksAndInterruptions`, `TestSeparateIPv6ClusterNetworksAndInterruptions`, `TestOSDHeartbeatFailure`, `TestFiveMonitorQuorumAndNetworkRecovery`, `TestRBDMirrorBandwidthCatchUp`, `TestMultiClusterRBDPeerNetworkInterruption`, `TestMultiClusterRGWPeerNetworkTopology`, `TestMultiClusterRGWZoneLinkPathCut`, `TestRBDMirrorPeerPathCut`, `TestCephFSMirrorPeerPathCut`, `TestCephFSClientMDSPathCut`, `TestCephFSMDSOSDPathCut` and `TestCephFSMDSOSDsPathCut`, on bridge and host network where they run today, with source and image identities and cleanup recorded. |
| 4. Remaining families | Pause, blocklist, full ratios, object error, each in its own bounded change. | Control-container refusal and paused-state refusal; blocklist session normalization, refusal to adopt or renew, expiry-as-absence, `Until` identity, detached readback; full ratios ordered steps, `pending` reconciliation and FSID identity; object error preconditions, `sharedRecycle`, and its recovery by rewrite or restart. `TemporaryConfig`, `TemporaryOSDFlag` and the rest stay. | Moved unit tests; native `TestPausedOSDFaults`, `TestClientFencing`, `TestOSDFullRatios`, `TestScrubInconsistency`, and the CephFS and RBD fixtures that pause or fence. |
| 5. Public migration | Examples, `docs/fault.md` (new API page), `internal/apiinventory` gains `"fault": "docs/fault.md"`, `docs/ceph.md`, service pages, the fault guide, the upgrade guide and CHANGELOG. | Explicit wrapper decision; before and after call forms; scenario discovery and declaration selection still reach the moved code. | `internal/apiinventory` test; `tag_scenarios.py` plan and `replay_declaration_selection.py` show the netem and fault batches selected for a change under `fault/` (paths outside `internal/integration` fall back to all batches today, so a new public package must not narrow selection). |

## Risks

1. **Lock inversion during the move.** `NetworkDelayLease` takes `c.mu`
   inside its own mutex, while interruption and pause take theirs inside
   `c.mu`. A uniform "restore everything under `c.mu`" or "restore
   everything unlocked" would deadlock delays or race interruptions. The
   registry keeps the lock mode per phase, and stage 2 adds a race test.
2. **Admission bypass.** Moving methods off `*Container` removes them from
   the reflection-based gate test. Without `sharedFaultPolicies` and the
   enumeration test, a tenant's exclusive-only fault could run on a shared
   cluster.
3. **Silent contract drift.** The families disagree today (previous handle
   with error for interruption and pause, nil for delay; plain errors for
   interruption and pause, classified errors elsewhere; plain `c.mu.Lock`
   for blocklist). Normalizing them is out of scope; each stage keeps them
   and records any intended change as separate work.
4. **Test seam loss.** Netem unit tests inject a backend through the
   unexported `Container.newNetemBackend`. Moving the backend to `fault`
   needs an equivalent seam there, or the 67 delay unit calls become
   native-only.
5. **Breaking change size.** Every fault user, three service packages and
   their docs change at once at release. No wrappers are possible without a
   cycle, so the upgrade guide carries the migration.
6. **CI selection.** Declaration selection only narrows changes inside
   `internal/integration`; a new top-level package must keep forcing the
   fallback, and batch roots must still reach the moved declarations.

## Open questions for review

- Final names, in particular `InterruptLink` for the standalone
  interruption and whether `NetworkDelay` keeps that name now that it also
  carries rate and packet faults.
- Whether `TemporaryOSDFlag` should be re-exported from `fault` for
  discoverability (decision here: no, documentation links suffice).
- Whether stage 4 may switch `TemporaryBlocklist` to a context-aware lock;
  the default is to keep today's behavior.
