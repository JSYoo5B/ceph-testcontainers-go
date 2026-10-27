# Shared-cluster and tenant harness: inventory and boundary

This note is the measured inventory and the package boundary that #155 asks
for before any public move. The owner decided the package layout on
2026-10-27 (see "Decided layout"). The note changes no code, no public
signature and no dependency. Moving the exported types stays blocked on the
internal state, lock and admission decomposition of #146 (see
[container-decomposition.md](container-decomposition.md) and "Blocking
prerequisites" below); this note fixes what that move has to carry, the exact
old-to-new map and the order in which it can be done safely. Exported names
under the new packages are the map's proposal; exact signatures remain detailed
design work.

## Method and limits

- Source snapshot: the inventory counts below were measured at `1d4c937`
  (merge of #166). The adapter and caller sections were re-measured at
  `e22b728` (after the tenant port, #200, and `internal/sharedinvariants`).
  The "Refresh at `7266eb7`" section below re-measures everything against the
  merge of #247 plus the cluster-operations port of this change; where it
  differs from an older number the refresh wins.
- Container members used by the harness were measured with a throwaway
  `go/types` program over the non-test files of package `ceph` (type check
  without errors, selector expressions whose receiver is `Container` or
  `*Container`, by declaring file). Exported symbols, `sharedGate` call sites
  and importers were counted with `grep` over the tree. Counts are textual or
  type-resolved as stated; they are a baseline for review, not acceptance
  targets, and must be re-measured after #146.
- Source reading and static analysis only. No Docker, no native Ceph and no
  timing behavior was run, so nothing here is native evidence.

## Harness surface at the snapshot

Files (non-test lines) at the first snapshot: `shared.go` 668,
`shared_backend.go` 603, `shared_exclusive.go` 290, `shared_services.go` 112,
`tenant.go` 307, and the gate `shared_gate.go` 319 (which stays in core, see
below). Four test files (`shared_test.go`, `shared_exclusive_test.go`,
`shared_services_test.go`, `shared_gate_test.go`) hold 1,642 lines. The
current numbers are in the refresh section.

Exported symbols that would move (35):

| Kind | Symbols |
| --- | --- |
| Types (6) | `SharedCluster`, `SharedOption`, `LeaseOption`, `SharedStats`, `Tenant`, `TenantAccess` |
| Constants (3) | `SharedPool`, `TenantReadOnly`, `TenantReadWrite` |
| Functions (6) | `NewShared`, `RunTests`, `WithSharedRunOptions`, `WithMaxTenants`, `WithMaxRecycles`, `WithKeepOnTenantFailure` |
| `SharedCluster` methods (4) | `Lease`, `Exclusive`, `Stats`, `Close` |
| `Tenant` methods (16) | `ID`, `Pool`, `Namespace`, `Client`, `CreateClient`, `WithClient`, `WithClientIdentity`, `RunNativeCommand`, and eight read-only views: `Status`, `HealthDetails`, `Pools`, `PoolStatus`, `PoolUsage`, `ObjectWatchers`, `OSDFlags`, `BlocklistEntries` |

`ErrSharedCluster` is also exported but is a core error category used by the
gate; it stays in `ceph` (see the gate section).

External references to the moved symbols, outside `ceph/` (type-checked names
counted per file by `grep`): `internal/integration` 30 occurrences in 5 test
files, `rbd/tenant.go` 5, `cephfs/tenant.go` 4, `rgw/tenant.go` 4,
`internal/servicehook/servicehook.go` 6. Documentation that names them:
`docs/ceph.md`, `docs/rbd.md`, `docs/cephfs.md`, `docs/rgw.md`,
`docs/design/api-naming.md`, `CHANGELOG.md`, plus the generated
declaration entries for the 35 symbols in the `docs/design/api-naming/<package>/` catalogues.

## What the harness consumes from `Container` internals

Result of the type-resolved measurement, by file:

| File | Distinct `Container` members used | Members |
| --- | --- | --- |
| `shared.go`, `shared_exclusive.go`, `shared_services.go` | 0 | They reach the cluster only through the `sharedBackend` interface (12 methods) and `containerBackend` |
| `shared_backend.go` | 24 | Fields: embedded `Container`, `closed`, `config`, `controlMu`, `controlPlane`, `managers`, `monitorTerminated`, `monitors`, `mu`, `osds`, `services`, `settings`, `sharing`. Methods: `CreatePool`, `State`, `WaitForPGClean`, `WaitForQuorum`, `controlContainerContext`, `generateKeyArgs`, `lockTopology`, `recordClientSecret`, `setClientKey`, `settleClient`, `terminate` |
| `tenant.go` | 12 | `BlocklistEntries`, `GetContainerID`, `HealthDetails`, `Inspect`, `OSDFlags`, `ObjectWatchers`, `Pools`, `Status` (exported), and `poolStatus`, `poolUsage`, `runNativeCommand`, `withClientIdentity` (unexported) |
| Union | 36 | 13 fields and 23 methods (11 unexported) |

Reverse direction: the only non-harness file in `ceph` that names a
harness-declared identifier is `service_hooks.go` (`Tenant`,
`registerSharedService`, `sharedService`). The gate side is the other
direction: `Container.sharing` is written in three places
(`shared_gate.go` reads it, `shared_backend.go` and the `TestCluster` hook in
`service_hooks.go` write it), and 39 lines mention `sharedGate(` in 18
non-test files of core (38 calls plus its declaration); `admitHandle`
is called from 6 sites on the handle types.

Two consequences:

1. The coupling from the orchestrator (`SharedCluster`) to `Container` is
   already narrow and already an interface (`sharedBackend`, replaced by a
   model in the unit tests). Only `containerBackend` needs private state, and
   it needs a lot of it (13 fields, 11 unexported methods).
2. The coupling that blocks a package move is `Tenant`. It holds forwarders to
   eight `Container` operations, the `runNativeCommand`/`withClientIdentity`
   internals, and it is handed to the service adapters as a concrete type.

## Decided layout

The owner decided on 2026-10-27 (issue comments "Owner decision — public
harness packages (revised)" and "Final package-placement decision"), replacing
this note's earlier proposal of a top-level `cephtest` package and of keeping
the adapters in the service packages:

| Package | Responsibility |
| --- | --- |
| `ceph/harness` | Common `SharedCluster`, `Tenant`, `RunTests`, shared/lease options, statistics and orchestration |
| `rbd/harness` | RBD tenant and lease adapters |
| `cephfs/harness` | CephFS tenant and lease adapters |
| `rgw/harness` | RGW tenant and lease adapters |

Paths are relative to `github.com/JSYoo5B/ceph-testcontainers-go`. Consumers
may alias the packages (`cephharness`, `rbdharness`) because all four are named
`harness`.

### Import graph

Before (arrows point to the imported package):

```
rbd, cephfs, rgw --> ceph                 (Container, SharedCluster, Tenant)
rbd, cephfs, rgw --> internal/servicehook --> ceph
ceph --> internal/hookreg                 (registers serviceHooks in init)
ceph --> internal/sharedinvariants
internal/integration --> ceph, rbd, cephfs, rgw, internal/servicehook
```

After:

```
ceph/harness   --> ceph, internal/hookreg, internal/sharedinvariants
rbd/harness    --> rbd, ceph/harness, ceph
cephfs/harness --> cephfs, ceph/harness, ceph
rgw/harness    --> rgw, ceph/harness, ceph
rbd, cephfs, rgw --> ceph, internal/servicehook   (no harness import)
ceph --> internal/hookreg                          (no harness, no service import)
internal/integration --> the harness packages it tests, plus the above
```

Directory nesting is not an import constraint: `ceph/harness` imports `ceph`
and `ceph` does not import it. Core and service packages must not import their
harness children, and `ceph/harness` must not import any service harness.
Services reach the common harness only through registration made in each
service harness package's `init`, so a program that does not import a service
harness does not pay for its shared-cluster hooks.

### What stays in core

The gate stays with `Container` operations: `sharedGate`, `admitShared`,
`admitHandle`, `admitFault`, the policy tables, `sharingMode`, `Container.sharing`
and `ErrSharedCluster`. Its reflection tests enumerate `*Container` and the
handle receivers; moving it would force the harness to know every core method
name or to duplicate the tables. `shared_gate_test.go` stays. The harness
drives the gate through one narrow call that sets the mode (not shared, shared
with tenants, exclusive).

### What moves to `ceph/harness`

`SharedCluster`, its options, `SharedStats`, `RunTests`, the
lease/exclusive/waiter state machine, poisoning and recycling, the service
registry (`shared_services.go`), `Tenant`, `TenantAccess` and the tenant scope
list, with their model-backed unit tests (`shared_test.go`,
`shared_exclusive_test.go`, `shared_services_test.go`). `SharedPool` moves with
them as a harness constant unless the baseline invariants keep a core copy
(open question).

### Ports between `ceph/harness` and core

1. **Backend port.** Today `SharedCluster` reaches the cluster through the
   unexported `sharedBackend` interface (12 methods), and `containerBackend`
   uses 13 private fields and 11 private methods of `Container`. After the move
   the implementation stays in `ceph` and is reached through a behavior port
   registered in `internal/hookreg` as a second slot next to the service hooks:
   start a cluster (returning the partial cluster on failure so the harness can
   terminate it), and per cluster: create pool, wait ready, read the
   invariants, check daemons, create and delete a scoped client, purge a
   namespace, set the sharing mode and terminate. It exposes no lock, field or
   executor. The decoders that `invariants` needs (`decodePoolStates`,
   `decodeOSDPolicyDump`, `decodeConfiguration`, `decodeBlocklistEntries`,
   `decodeHealthDetails`, `pgsAreClean`) stay private to `ceph`; only the
   resulting `sharedinvariants.Invariants` crosses the port. The service
   `prepare` and `probe` loops move to the harness and run on the cluster
   returned by the port.
2. **Tenant port.** The tenant port added in `ceph/service_hooks.go` (`ID`,
   `Live`, `StartupTimeout`, `Command`, `CreateClient`, `AttachScope`,
   `WithClientIdentity`, `WithTopology`, `Extension`) is what the adapters
   already use. After the move its implementation (`tenantLease`) is built over
   the harness `Tenant`, so it moves to `ceph/harness`, and the service
   harnesses obtain it through an internal registry that takes the tenant as an
   untyped value. This adds no public method to `Tenant`.
3. **Private pass-throughs of `Tenant`.** `Tenant` calls four private
   `Container` paths: `poolStatus`, `poolUsage`, `runNativeCommand` and
   `withClientIdentity`. Each needs a port method or leaves `Tenant` through the
   observation object (next section). The eight read-only forwarders call
   exported `Container` methods and need no port.
4. **Errors.** `invalidRequest`, `classify`, `classifyDocker` and the client ID
   pattern are private to `ceph`. The harness needs equivalents that preserve
   the `*OperationError` kind and message text. The port or an exported helper
   decision belongs to the extraction contract; neither may weaken
   `errors.Is` behavior.
5. **`TenantCluster`.** It remains only for the native gate and release
   controls in `internal/integration` (3 call sites in 2 files). After the move
   it becomes a test-only internal registry entry that returns the gated
   cluster of a live tenant, not a method on any public type.

### Tenant read-only views

The eight forwarders (`Status`, `HealthDetails`, `Pools`, `PoolStatus`,
`PoolUsage`, `ObjectWatchers`, `OSDFlags`, `BlocklistEntries`) are not migrated
twice. The owner decided in #146 that live observations sit behind an
observation object bound to a cluster or a tenant, with the same query shape for
both, validated on every query for a tenant, and that the applicable Tenant
forwarders move into it. The harness therefore adopts that object when it
exists; pool views follow its placement decision. Until then the forwarders
stay on `Tenant` and move with it unchanged.

## Service adapters

Re-measured at `e22b728`. After #200 no adapter obtains the shared cluster
through `TenantCluster`; each uses the tenant port.

| Adapter | Port use | Registration |
| --- | --- | --- |
| `rbd` | `Tenant`, `Command`, `StartupTimeout`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `RegisterSharedService(bootstrapKind, nil, probeTenantNamespaces)` |
| `cephfs` | `Tenant`, `Command`, `StartupTimeout`, `Live`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `RegisterSharedService(cephFSBootstrapKind, prepareShared, probeShared)` |
| `rgw` | `Tenant`, `Live`, `WithTopology`, `Extension`, `AttachScope` | `RegisterSharedService(rgwBootstrapKind, prepareShared, probeShared)` |

One residue remains in `rgw`: the gateway release path takes the topology
mutex of the gateway's owning cluster with `servicehook.Lock`/`Unlock` (4
pairs). That cluster is the gateway's own `*ceph.Container`, held by the
gateway, not one obtained from a tenant. The extraction must keep that path
unchanged and must not widen it to a tenant-provided cluster.

After the move each adapter's `Lease`, `TenantNamespace`, `TenantGroup` and
`TenantUser` entry points live in the service's `harness` package and take
`*harness.Tenant` from `ceph/harness`. The service package keeps the service
API that does not need a tenant. The registration call moves into the service
harness `init`; the service package no longer calls it. No adapter obtains a
cluster from `TenantCluster` any more, so the adapters keep no tenant-derived
raw-cluster path.

## Old-to-new map

| Old | New |
| --- | --- |
| `ceph.SharedCluster`, `ceph.NewShared`, `ceph.RunTests` | `harness.SharedCluster`, `harness.NewShared`, `harness.RunTests` in `ceph/harness` |
| `ceph.SharedOption`, `ceph.LeaseOption`, `ceph.WithSharedRunOptions`, `ceph.WithMaxTenants`, `ceph.WithMaxRecycles`, `ceph.WithKeepOnTenantFailure` | same names in `ceph/harness` |
| `ceph.SharedStats` | `harness.SharedStats` |
| `ceph.Tenant`, `ceph.TenantAccess`, `ceph.TenantReadOnly`, `ceph.TenantReadWrite` | same names in `ceph/harness` |
| `ceph.SharedPool` | `harness.SharedPool` (or kept in core, see open questions) |
| `ceph.ErrSharedCluster` | unchanged, stays in `ceph` |
| `rbd.Lease`, `rbd.TenantNamespace` | `rbd/harness` |
| `cephfs.Lease`, `cephfs.TenantGroup` | `cephfs/harness` |
| `rgw.Lease`, `rgw.TenantUser` | `rgw/harness` |

Direct v0.x migration applies: no deprecated aliases, no forwarding layer.
Callers to migrate: `internal/integration` (30 type references in 5 test
files), the adapters, `internal/servicehook`, `docs/ceph.md`, `docs/rbd.md`,
`docs/cephfs.md`, `docs/rgw.md`, `docs/design/api-naming.md`, `CHANGELOG.md`,
the package examples and `TestMain` snippets, and the v0.4 upgrade mappings.
The package-scoped API-naming catalogues are regenerated with
`-update-api-naming` and approved with the separate approve command (see
`api-naming.md`), and the move adds its own shard under
`.github/scripts/fixtures/package-split/`, deliberately and in the move's own
change (see `docs/contributing/package-split-shards.md`).

## Preserved behavior to carry

| Property | Where it lives now |
| --- | --- |
| Live-lease and generation admission, release invalidation | `Tenant.check`, `released` (guarded by `SharedCluster.mu`), `sharedInstance.terminated` |
| Owner-scoped cleanup in reverse order before client deletion, remainder poisons the cluster | `releaseScopes`, `Tenant.takeScopes`, `SharedCluster.release` |
| Exclusive fairness, self-wait refusal, waiter queue | `shared_exclusive.go` (`acquire`, `admissibleLocked`, `selfWaitLocked`) |
| Poison, recycle bound, non-counting exclusive restarts | `SharedCluster.poison`, `defaultMaxRecycles` |
| Namespace and Cephx isolation, baseline and release invariants | `internal/sharedinvariants` (`AdmissionDiff`, `ReleaseDiff`), `containerBackend.invariants` |
| Refusal of raw Docker/admin/topology access on a shared cluster | gate and tables (stay in core) |
| Topology mutex and gate order, uncertain-outcome ownership | `tenantLease.WithTopology`, `lockTopology` in `containerBackend`; unchanged by a pure move |

## Blocking prerequisites

Status of the safe extraction order at `e22b728` (superseded by the refresh
section below, which keeps this table for history):

| Step | State |
| --- | --- |
| Pure logic split into `internal/sharedinvariants` | Done |
| Tenant port in `ceph`, adapters on it, `TenantCluster` retired from adapters | Done (#200) |
| Observation object and removal of the Tenant forwarders | Blocked: #146 item 2, public contract and remaining implementation undecided |
| Backend port (second `hookreg` slot, factory, `SharedCluster` on the port only) | Blocked: needs #146 item 4, the internal state and lock split, because `containerBackend` uses 13 private fields and 11 private methods and the port would freeze that shape; also touches the facts and termination state that #146 is still moving |
| Move to `ceph/harness` and the three service harnesses | Blocked on the two rows above and on #146's acceptance review |
| Verification (`make check`, tagged compilation, native shared-RADOS and service-tenant cases on the Linux runner) | Runs on the move's change |

#146 states that its internal state, lock and admission decomposition is not
complete (remaining: facts extraction, the observation object, overrides, the
internal state and lock split, final measurement), and #155 keeps that as its
gate. Neither the backend port nor the exported move is started here.

## Refresh at `7266eb7` plus the cluster-operations port

Measured against `7266eb7` (merge of #247) with the change that adds
`ceph/shared_ops.go` applied. Source reading, `go/types` measurement and
`grep` only; no Docker, no native Ceph, nothing here is native evidence.

### Counts

| Item | Earlier | Now |
| --- | --- | --- |
| `shared.go` / `shared_backend.go` / `shared_exclusive.go` / `shared_services.go` / `tenant.go` / `shared_gate.go` / `shared_ops.go` (non-test lines) | 668 / 603 / 290 / 112 / 307 / 319 / not present | 668 / 482 / 294 / 90 / 307 / 342 / 93 |
| Exported symbols that would move | 35 | 35 (unchanged; the 35 are still all declared in `ceph/shared.go`, `ceph/tenant.go`) |
| `sharedBackend` methods | 12 | 12 (`cluster() *Container` replaced by `ops() clusterOps`, `ceph/shared_backend.go:21`) |
| Container members named by `shared.go`, `shared_exclusive.go`, `shared_services.go` | 0 | 0 |
| Container members named by `tenant.go` | 12 (8 exported views, 4 private paths) | 0 (all reached through `clusterOps`, `ceph/shared_ops.go:18`) |
| Container members named by `shared_backend.go` (`containerBackend`) | 24 | 23 (the embedded `Container` type, `CreatePool`, `WaitForPGClean`, `WaitForQuorum`, `closed`, `config`, `controlContainerContext`, `controlMu`, `controlPlane`, `generateKeyArgs`, `lockTopology`, `managers`, `monitorTerminated`, `monitors`, `mu`, `osds`, `recordClientSecret`, `services`, `setClientKey`, `settings`, `settleClient`, `sharing`, `terminate`) |
| Container members named by `shared_ops.go` (`containerOps`) | not present | 12 (`BlocklistEntries`, `HealthDetails`, `OSDFlags`, `ObjectWatchers`, `Pools`, `Status`, `mu`, `poolStatus`, `poolUsage`, `runNativeCommand`, `settings`, `withClientIdentity`) |
| Union of Container members used by the harness files | 36 | 33 in two files, of which the 12 of `shared_ops.go` disappear when the Tenant forwarders move to the observation object |
| `sharedGate(` lines / files / `sharedGate("` call sites | 39 / 18 / 38 | 39 / 18 / 38 |
| `admitHandle(` / `admitFault(` call expressions | 6 / not counted | 6 / 0 outside declarations |
| References to the 16 moved package-level names as `ceph.X` outside `ceph/` (regexp over the tree) | `internal/integration` 30 in 5 files; `rbd/tenant.go` 5; `cephfs/tenant.go` 4; `rgw/tenant.go` 4; `internal/servicehook/servicehook.go` 6 | `internal/integration` 30 in 5 files; `rbd/tenant.go` 4; `cephfs/tenant.go` 3; `rgw/tenant.go` 4; `internal/servicehook/servicehook.go` 4 (the regexp counts qualified `ceph.Tenant`-style names only, so it is lower than the earlier textual count for the three adapters and the hook package; the files and the 30 integration references agree) |
| `TenantCluster` call sites | 3 in 2 files | 4 in 2 files (`internal/integration/shared_cluster_integration_test.go:83,146`, `internal/integration/shared_services_integration_test.go:165,198`) plus the hook (`ceph/service_hooks.go:56`) and its declaration (`internal/servicehook/servicehook.go:42`) |

The earlier "3 call sites" figure undercounted by one: the integration
helpers call it in four places. No adapter calls it.

### Import graph check

Measured with `go list` at the commit above (internal imports only):

```
ceph             -> internal/clusterutil, dockerbridge, hookreg, naming, sharedinvariants, supervised
rbd              -> ceph, internal/multicluster, naming, servicehook
cephfs           -> ceph, internal/clusterutil, naming, servicehook
rgw              -> ceph, internal/clusterutil, naming, servicehook
internal/servicehook -> ceph, internal/hookreg, supervised
internal/hookreg, internal/sharedinvariants -> (nothing)
```

Cycle check for the planned graph: `ceph/harness` imports `ceph`,
`internal/hookreg` and `internal/sharedinvariants`; none of them imports a
harness package. `internal/servicehook` is the one package that imports `ceph`
and names `*ceph.Tenant` and `*ceph.Container` in its hook interface
(`internal/servicehook/servicehook.go:42-43`). After the move its tenant half
has to import `ceph/harness`, so `ceph/harness` must not import
`internal/servicehook`. It does not need to: it registers through
`internal/hookreg` only. The service harnesses import `internal/servicehook`
and `ceph/harness`, which is acyclic. The check that must hold after the move:
`go list -deps ./ceph` and `go list -deps ./ceph/harness` list no `rbd`, `cephfs`,
`rgw`, `servicehook` or any `.../harness` package other than itself.

One reverse dependency remains in core and is new in this refresh:
`ObserveTenant(tenant *Tenant)` (`ceph/observer.go:43`) names the harness
`Tenant`. The owner decided that the `Observer` stays in `ceph` (#146, decision
1 of 2026-10-27), so after the move core cannot name `Tenant`. The move has to
turn the tenant form into a harness-side constructor that hands the `Observer`
a target function through an internal seam (the same shape as `override`), or
into an `Observe` overload on a narrow interface. This is a decision of the
move, not of the port.

### What the cluster-operations port changes

The note's item 1 of "Ports between `ceph/harness` and core" describes the
future cross-package registration. The part of it that is internal and
behavior preserving is done: the orchestration no longer holds a `*Container`.

- `sharedBackend` no longer has `cluster() *Container`; it has `ops()
  clusterOps` (`ceph/shared_backend.go:21`, `ceph/shared_backend.go:482`),
  nil for a model.
- `clusterOps` (`ceph/shared_ops.go:18`) carries exactly what `Tenant` and
  `tenantLease` used: the startup timeout, a CLI command, `WithTopology`, the
  service-extension slot, the eight read-only views, the two private
  pass-throughs (`runNativeCommand`, `withClientIdentity`) and
  `poolStatus`/`poolUsage`. It exposes no lock, field or executor.
- `Tenant` reaches the cluster only through it (`ceph/tenant.go:298`) and
  `tenantLease` holds it instead of a `*Container` (`ceph/service_hooks.go:109`).
  The admission check (`Tenant.check`, `ceph/tenant.go:256`) is unchanged and
  runs before `ops()`.
- Two places still obtain the gated `*Container` from `clusterOps.raw()`:
  `SharedCluster.Exclusive` (`ceph/shared_exclusive.go:31`, the deliberate raw
  handle of the open question below) and `Tenant.container` (`ceph/service_hooks.go:67`),
  used by the `Observer` and the test-only `TenantCluster` hook
  (`ceph/service_hooks.go:56`). `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList`
  pins that `shared.go`, `shared_exclusive.go`, `shared_services.go` and
  `tenant.go` name `Container` only in `Exclusive` and in the registered
  service hook types (`sharedService.prepare` and `probe`,
  `ceph/shared_services.go:21,26`), and that `tenantLease` holds no `Container`.

What the port does not do: it does not yet have the cross-package form (a
`hookreg` slot, a start factory returning the partial cluster, the invariants
decoders behind it). That form freezes the shape of `containerBackend`'s 13
fields and 11 private methods, which the remaining internal state and lock
split is still moving.

### Which prerequisites are met

| Prerequisite | State at `7266eb7` | Evidence |
| --- | --- | --- |
| Pure invariant logic outside `ceph` | Met | `internal/sharedinvariants` (imported by `ceph/shared_backend.go`) |
| Tenant port and adapters off `TenantCluster` | Met (#200) | no call in `rbd/`, `cephfs/`, `rgw/`; port at `ceph/service_hooks.go:80` |
| Orchestration and `Tenant` free of `*Container` | Met by this change | `ceph/shared_ops.go:18`, guard test in `ceph/shared_ops_test.go` |
| Networking, native-command and feature-client state split from `Container` | Met (#239, #241, #246) | `ceph/network_state.go`, `ceph/native_state.go`, `ceph/feature_client_state.go` |
| Static facts extraction | Partly met: `FactsOf` exists (`ceph/cluster_facts.go:49`) but the old exported fact accessors are still on `Container` | owner decision 1 of 2026-10-27 says to delete them; in flight |
| Observation object and removal of the Tenant forwarders | Not met: `Observe` and `ObserveTenant` exist (`ceph/observer.go:33,43`), but the eight views are still `Container` methods (`ceph/status.go:30`, `ceph/health_details.go:55`, `ceph/pool_policy.go:45`, `ceph/object_watchers.go:26`, `ceph/osd_policy.go:115`, `ceph/fencing.go:47`) and still forwarded by `Tenant` (`ceph/tenant.go:181-253`); the forwarders go through `clusterOps` now, and are deleted when the views move | #146 decision 1, in flight |
| Auth records split (`authFences`, `clientKeys`, `clientSecrets`, `strayAuthInputs`, `clientRevocations`) | Not met | `containerBackend` calls `generateKeyArgs`, `settleClient`, `recordClientSecret` and `setClientKey` (`ceph/shared_backend.go:377,397,411,416,445,457`); audit in `docs/design/container-decomposition.md`, "Components not picked" |
| OSD storage state split (`osds`, `detachedOSDs`, `osdMemory`, `osdVolume`) | Not met | `containerBackend.daemonsRunning` walks the daemon containers (`ceph/shared_backend.go:323`; `monitors` at `:331`, OSDs through `osds`) |
| Override maps and fault registry | Not met | the three maps are guarded by `Container.mu`; the `override` package is designed (`docs/design/override-package-boundary.md`, owner answers of 2026-10-27) and not implemented |
| Membership and control (`mu`, `controlMu`, `controlPlane`, `monitors`, `managers`, `services`, `sharing`, `closed`) | Not met, and not a single component | `containerBackend` uses all of these (`ceph/shared_backend.go`); split by lock domain is a separate review |
| `ObserveTenant` independent of the harness `Tenant` | Not met | `ceph/observer.go:43` |

Conclusion: the internal sharing orchestration is now independent of
`Container`, which was the part of the backend port that could be done without
moving state. The public move is still blocked by #146 slices that are in
flight or not started: the auth records, OSD storage, the override registry,
and facts/observation removal (with the `ObserveTenant` seam). The
cross-package backend registration (second `hookreg` slot) waits for the
membership and control split because `containerBackend` would otherwise freeze
13 fields and 11 private methods in its port.

## Open questions

- Whether `SharedPool` stays a core constant because the baseline checks depend
  on it.
- Whether the service registry stays an `init`-time registration in each
  service harness or becomes an explicit argument of `NewShared`.
- Whether `Exclusive` keeps returning `*ceph.Container`, a deliberate raw
  handle, from a package that is not `ceph`.
- How the private errors helpers are made available to the harness without a
  public API (port method versus equivalent code with equal `errors.Is`
  behavior).
- Exact exported signatures of the four packages and the consumer import
  aliases to document.

## Not covered

No exported symbol was moved, no public signature was added and no package
import changed. The only code change tied to this note is the unexported
`clusterOps` port, covered by Docker-free unit tests and a structural guard;
no Docker or native Ceph test was run. The counts of the first half are from the
older snapshot; the refresh section holds the current ones and must be
re-measured after the remaining #146 slices.
