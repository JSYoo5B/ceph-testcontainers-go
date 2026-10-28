# Shared-cluster and tenant harness: inventory and boundary

This note is the measured inventory and the package boundary that #155 asks
for before any public move. The owner decided the package layout on
2026-10-27 (see "Decided layout"). The note changes no production code, no
public signature and no dependency. The internal state, lock and admission
decomposition of #146 that blocked the move is complete (see
[container-decomposition.md](container-decomposition.md) and "Inventory at
`83de22a`" below); what is left is the cross-package registration contract,
which "Cross-package registration design" specifies and "Decisions for the
owner" lists the open choices of. This note fixes what the move has to carry,
the exact old-to-new map and the order in which it can be done safely.

How to read the sections: "Inventory at `83de22a`" is the current, guarded
state and wins over every older number. The sections from "Method and limits"
to "Blocking prerequisites" and the three "Refresh at" sections are the history
of how the boundary was found; they keep their measured commits and are not
maintained. "Cross-package registration design" and "Decisions for the owner"
are the design.

## Method and limits

- Source snapshot of the historical sections: the inventory counts below were measured at `1d4c937`
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
A service's shared-cluster support is not registered through `init` or any
process-global registry (owner decision of 2026-10-28, see "Decisions for the
owner"). The program supplies it explicitly when it constructs each shared
harness instance, so the selected services are visible in construction code.

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
3. **Private pass-throughs of `Tenant`.** (Historical: `Tenant` called four
   private `Container` paths. Today it reaches the cluster only through
   `clusterOps`, whose six non-raw methods become the `Ops` port below.)
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

Superseded: the eight forwarders (`Status`, `HealthDetails`, `Pools`,
`PoolStatus`, `PoolUsage`, `ObjectWatchers`, `OSDFlags`, `BlocklistEntries`) are
no longer `Tenant` methods. They are `Observer` queries (`ceph/observer.go`),
and `Tenant` has eight exported methods. The Observer stays in `ceph`; its
tenant constructor is the one cross-package seam, specified in "Observer
constructor" below.

## Service adapters

Re-measured at `e22b728`. After #200 no adapter obtains the shared cluster
through `TenantCluster`; each uses the tenant port.

| Adapter | Port use | Registration |
| --- | --- | --- |
| `rbd` | `Tenant`, `Command`, `StartupTimeout`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `RegisterSharedService(bootstrapKind, nil, probeTenantNamespaces)` at `init` today; explicit service support after the move |
| `cephfs` | `Tenant`, `Command`, `StartupTimeout`, `Live`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `RegisterSharedService(cephFSBootstrapKind, prepareShared, probeShared)` at `init` today; explicit service support after the move |
| `rgw` | `Tenant`, `Live`, `WithTopology`, `Extension`, `AttachScope` | `RegisterSharedService(rgwBootstrapKind, prepareShared, probeShared)` at `init` today; explicit service support after the move |

One residue remains in `rgw`: the gateway release path takes the topology
mutex of the gateway's owning cluster with `servicehook.Lock`/`Unlock` (4
pairs). That cluster is the gateway's own `*ceph.Container`, held by the
gateway, not one obtained from a tenant. The extraction must keep that path
unchanged and must not widen it to a tenant-provided cluster.

After the move each adapter's `Lease`, `TenantNamespace`, `TenantGroup` and
`TenantUser` entry points live in the service's `harness` package and take
`*harness.Tenant` from `ceph/harness`. The service package keeps the service
API that does not need a tenant. The `init` registration call is removed, not
moved: the service harness offers its support as a value that the program passes
when it constructs the shared harness ("Service support"). No adapter obtains a
cluster from `TenantCluster` any more, so the adapters keep no tenant-derived
raw-cluster path.

## Old-to-new map

| Old | New |
| --- | --- |
| `ceph.SharedCluster`, `ceph.NewShared`, `ceph.RunTests` | `harness.SharedCluster`, `harness.NewShared`, `harness.RunTests` in `ceph/harness` |
| `ceph.SharedOption`, `ceph.LeaseOption`, `ceph.WithSharedRunOptions`, `ceph.WithMaxTenants`, `ceph.WithMaxRecycles`, `ceph.WithKeepOnTenantFailure` | same names in `ceph/harness` |
| `ceph.SharedStats` | `harness.SharedStats` |
| `ceph.Tenant`, `ceph.TenantAccess`, `ceph.TenantReadOnly`, `ceph.TenantReadWrite` | same names in `ceph/harness` |
| `ceph.ObserveTenant` | `harness.ObserveTenant` in `ceph/harness`; it returns `*ceph.Observer` |
| `ceph.SharedPool` | `harness.SharedPool` in `ceph/harness`; no `ceph.SharedPool` alias (settled) |
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
`go run ./internal/tools/apinaming regenerate <package>` and approved with the
separate `approve` command, a move of API between packages needs a record under
`docs/design/api-naming/moves/`, and the move adds its own shard under
`.github/scripts/fixtures/package-split/`, deliberately and in the move's own
change (see AGENTS.md and `docs/contributing/package-split-shards.md`).

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

## Blocking prerequisites (history)

Status of the safe extraction order at `e22b728` (superseded by "Inventory at
`83de22a`", which holds the current table):

| Step | State |
| --- | --- |
| Pure logic split into `internal/sharedinvariants` | Done |
| Tenant port in `ceph`, adapters on it, `TenantCluster` retired from adapters | Done (#200) |
| Observation object and removal of the Tenant forwarders | Blocked: #146 item 2, public contract and remaining implementation undecided |
| Backend port (second `hookreg` slot, factory, `SharedCluster` on the port only) | Blocked: needs #146 item 4, the internal state and lock split, because `containerBackend` uses 13 private fields and 11 private methods and the port would freeze that shape; also touches the facts and termination state that #146 is still moving |
| Move to `ceph/harness` and the three service harnesses | Blocked on the two rows above and on #146's acceptance review |
| Verification (`make check`, tagged compilation, native shared-RADOS and service-tenant cases on the Linux runner) | Runs on the move's change |

At `e22b728`, #146 still listed facts extraction, the observation object,
overrides, the internal state and lock split and the final measurement as
remaining. All of them have since merged.

## Refresh at `7266eb7` plus the cluster-operations port (history)

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
`ObserveTenant(tenant *Tenant)` named the harness `Tenant` in
`ceph/observer.go` (resolved inside `ceph` since: see the prerequisite table). The owner decided that the `Observer` stays in `ceph` (#146, decision
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

### Which prerequisites are met (history, superseded by "Refresh at `1a158b9`")

| Prerequisite | State at `7266eb7` | Evidence |
| --- | --- | --- |
| Pure invariant logic outside `ceph` | Met | `internal/sharedinvariants` (imported by `ceph/shared_backend.go`) |
| Tenant port and adapters off `TenantCluster` | Met (#200) | no call in `rbd/`, `cephfs/`, `rgw/`; port at `ceph/service_hooks.go:80` |
| Orchestration and `Tenant` free of `*Container` | Met by this change | `ceph/shared_ops.go:18`, guard test in `ceph/shared_ops_test.go` |
| Networking, native-command and feature-client state split from `Container` | Met (#239, #241, #246) | `ceph/network_state.go`, `ceph/native_state.go`, `ceph/feature_client_state.go` |
| Static facts extraction | Partly met: `FactsOf` exists (`ceph/cluster_facts.go:49`) but the old exported fact accessors are still on `Container` | owner decision 1 of 2026-10-27 says to delete them; in flight |
| Observation object and removal of the Tenant forwarders | Not met: `Observe` and `ObserveTenant` exist (`ceph/observer.go:33,43`), but the eight views are still `Container` methods (`ceph/status.go:30`, `ceph/health_details.go:55`, `ceph/pool_policy.go:45`, `ceph/object_watchers.go:26`, `ceph/osd_policy.go:115`, `ceph/fencing.go:47`) and still forwarded by `Tenant` (`ceph/tenant.go:181-253`); the forwarders go through `clusterOps` now, and are deleted when the views move | #146 decision 1, in flight |
| Auth records split (`authFences`, `clientKeys`, `clientSecrets`, `strayAuthInputs`, `clientRevocations`) | Not met | `containerBackend` calls `generateKeyArgs`, `settleClient`, `recordClientSecret` and `setClientKey` (`ceph/shared_backend.go:377,397,411,416,445,457`); audit in `docs/design/container-decomposition/feature-client-state.md`, "Components not picked" |
| OSD storage state split (`osds`, `detachedOSDs`, `osdMemory`, `osdVolume`) | Not met | `containerBackend.daemonsRunning` walks the daemon containers (`ceph/shared_backend.go:323`; `monitors` at `:331`, OSDs through `osds`) |
| Override maps and fault registry | Not met | the three maps are guarded by `Container.mu`; the `override` package is designed (`docs/design/override-package-boundary.md`, owner answers of 2026-10-27) and not implemented |
| Membership and control (`mu`, `controlMu`, `controlPlane`, `monitors`, `managers`, `services`, `sharing`, `closed`) | Not met, and not a single component | `containerBackend` uses all of these (`ceph/shared_backend.go`); split by lock domain is a separate review |
| `ObserveTenant` independent of the harness `Tenant` | Met inside `ceph` (internal seam, no public move): `observer.go`, `pool.go` and `pool_handle.go` name no `Tenant`; `ObserveTenant` lives in `ceph/tenant_observe.go` and hands the `Observer` a cluster resolver and a lease check through `newLeasedObserver`. Pinned by `TestObserverAndPoolLeaseNameNoTenant`. The cross-package form (a `hookreg` slot that the harness calls) is part of the public move | `ceph/tenant_observe.go`, `ceph/observer.go` |

Conclusion: the internal sharing orchestration is now independent of
`Container`, which was the part of the backend port that could be done without
moving state. The public move is still blocked by #146 slices that are in
flight or not started: the auth records, OSD storage, the override registry,
and facts/observation removal (the `ObserveTenant` seam inside `ceph` is done). The
cross-package backend registration (second `hookreg` slot) waits for the
membership and control split because `containerBackend` would otherwise freeze
13 fields and 11 private methods in its port.

## Refresh at `1a158b9`: backend port narrowing (history)

Measured against `1a158b9` (merge of #270) with the change that adds
`ceph/shared_cluster_port.go` applied. Source reading, `grep` and a Docker-free
AST guard only; no Docker, no native Ceph, nothing here is native evidence. The
first backend port is still internal. No exported symbol moved.

### Which prerequisites are met

| Prerequisite | State at `1a158b9` | Evidence |
| --- | --- | --- |
| Pure invariant logic outside `ceph` | Met | `internal/sharedinvariants` |
| Tenant port; adapters off `TenantCluster` | Met (#200) | no call in `rbd/`, `cephfs/`, `rgw/`; port at `ceph/service_hooks.go:146` |
| Orchestration and `Tenant` free of `*Container` | Met | `ceph/shared_ops.go:18`; `tenant.go` is 232 lines and names no Container member; `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList` |
| Networking, native-command and feature-client state | Met (#239, #241, #246) | `ceph/ceph.go:60`, `:92`, `:49` |
| Static facts | Met: `FactsOf` is the accessor (`ceph/cluster_facts.go:49`) and no old exported fact accessor is left on `Container` | `ceph/cluster_facts_migration_test.go:48` |
| Observation views and Tenant forwarders | Met: the eight views are no longer `Container` or `Tenant` methods; they are `Observer` queries (`ceph/observer.go:42`, `ceph/tenant_observe.go:10`) | `ceph/shared_ops.go` carries no view |
| `ObserveTenant` independent of the harness `Tenant` inside `ceph` | Met (#266). The cross-package form of the seam is part of the public move | `ceph/observer.go:54`, `TestObserverAndPoolLeaseNameNoTenant` (`ceph/observer_seam_test.go:47`) |
| Auth records split | Met | `authState` (`ceph/auth_state.go:22`), field `authRecords` (`ceph/ceph.go:53`); `containerBackend` no longer calls `recordClientSecret`/`setClientKey` |
| OSD storage state and handle registry | Met | `osdStorageState` (`ceph/osd_storage_state.go:32`), `osdRegistry` (`ceph/osd_registry_state.go:29`) |
| Run directory and port leases | Met | `ceph/run_dir_state.go`, `ceph/port_lease_state.go`; fields `ceph/ceph.go:81,84` |
| Override package and fault registry | Partly met: the `override` and `fault` packages exist, `faultRegistry` holds the bookkeeping (`ceph/fault_registry.go:115`, field `ceph/ceph.go:97`). The move of the registry logic into the component is open as #271 (owner review) | `override/`, `fault/`; open PR #271 |
| Membership and control (`mu`, `controlPlane`, `bootstrap`, `extensions`, `monitors`, `managers`, `services`, `closed`, `monitorTerminated`, `sharing`) | Partly met: two lock domains are components. `controlPlane` with the former `controlMu` is `controlPlaneState` (`ceph/control_plane_state.go:34`; record `container-decomposition/control-plane-state.md`, formerly the eleventh slice), and `config`, `keyring` and `cephVersion` with the former `configMu` are `bootstrapRecord` (`ceph/bootstrap_record.go:35`, field `ceph/ceph.go:53`; twelfth slice). `extensions` with the former `extensionsMu` is `extensionStore` (`ceph/extension_store.go:33`, field `ceph/ceph.go:75`; thirteenth slice). `monitors`, `managers`, `services` and the first MGR's container are `memberRoster` (`ceph/member_roster.go:35`, field `ceph/ceph.go:71`; fourteenth slice), guarded by `mu`. The rest is not met as components. #270 and the twelfth slice replaced the test-state pointers with setters (`ceph/service_hooks.go:484-494`, `ceph/membership_control_test.go`), so `TestState` now publishes only lock identities and a count (the thirteenth slice replaced the network removal flag pointer with `TestSetNetworkRemoved` and `TestNetworkRemoved`), and the other fields stay on `Container` where nothing outside `ceph` can address them. They are not split by lock domain | `ceph/ceph.go:34-100` |
| Backend port shape (`containerBackend` reach into `Container`) | Narrowed by this change; see below | `ceph/shared_cluster_port.go`, `ceph/shared_backend_reach_test.go` |
| Move to `ceph/harness` and the service harnesses | Not started, see "Is the move unambiguous" | |

### What this change narrows

`containerBackend` used to name 14 `Container` fields (the 13 of the earlier
measurement, with `osdHandles` for `osds`, plus `authRecords`: `closed`,
`config`, `controlMu`, `controlPlane`, `managers`, `monitorTerminated`,
`monitors`, `mu`, `osdHandles`, `services`, `settings`, `sharing`,
`authRecords` and the embedded `Container`) and 8 `Container` methods
(`CreatePool`, `WaitForPGClean`, `WaitForQuorum`, `controlContainerContext`,
`generateKeyArgs`, `lockTopology`, `settleClient`, `terminate`; the earlier "11"
counted `State` and two auth methods that have since moved into `authState`).
It also held the topology mutex itself in `createClient`, `deleteClient` and
`daemonsRunning`. After this change it names no `Container` field and no lock.
Its whole reach is:

| Port operation | `Container` method | Kind |
| --- | --- | --- |
| `createPool` | `CreatePool` | exported, unchanged |
| `ready` | `WaitForQuorum`, `WaitForPGClean` | exported, unchanged |
| `invariants`, `purgeNamespace` | `runSharedScript` | new, purpose-named |
| `daemonsRunning` | `sharedDaemons` | new: the daemon walk under the topology mutex, then the control lock |
| `createClient` | `createSharedClient` | new: the former body, with the key, secret and settle logic |
| `deleteClient` | `deleteSharedClient` | new: the former body |
| `setSharing` | `setSharingMode` | new |
| `terminate` | `terminate` | unchanged |
| `prepareServices`, `resources` | the pointer itself | handed to the registered service `prepare` and `probe` functions, which take the cluster |

That is 9 methods (3 exported, 6 private) and 0 fields. The remaining private
methods of the earlier list (`controlContainerContext`, `generateKeyArgs`,
`lockTopology`, `settleClient`) are called only by the new `Container` methods.
The 14 fields now appear only in the `Container` methods of
`ceph/shared_cluster_port.go`, so they stay private to `ceph` and are not part
of a cross-package port. The `clusterOps` reach (`ceph/shared_ops.go`) is
unchanged: `settings` and `mu` through `startupTimeout` and `withTopology`,
plus `runNativeCommand` and `withClientIdentity`.

The change moves code and adds no behavior: the bodies of `createClient`,
`deleteClient`, `daemonsRunning` and `run` are the same statements with
`b.c` replaced by the receiver, and the lock order is as before (topology
mutex, then the control lock after it is released). `TestContainerBackendReach`
(`ceph/shared_backend_reach_test.go`) pins this reach with an AST check of
`shared_backend.go` and `shared_services.go`: any other selected member, a lock,
or a copy of the pointer fails it, and
`TestBackendReachCheckerSeesARegression` shows the checker reporting those
cases. The count of `Container` methods grows by five; the owner has set no
numeric target and this is information only.

### Is the move unambiguous

No. The prerequisites that gated the backend port are met except the
membership and control component split, which the narrowed port no longer
depends on, and #271 is open. But the public move itself still has
undecided parts that this note lists as open questions: the exported
signatures, whether `SharedPool` and `Exclusive`'s raw `*Container` stay,
how the private error helpers reach the harness, and the cross-package form of
the `ObserveTenant` seam. Registering the second `hookreg` slot would also fix
the shape of `createSharedClient` and `deleteSharedClient`, whose return type
`*ClientConfig` is a core type, and the start factory that returns a partial
cluster. None of this is decided by the merged design, so only the
audit and the narrowing are done here.

## Inventory at `83de22a`

Pinned at `83de22a8fe310d612612479d1941d2e3a3d9dabf` (main after the owner-listed
merges #266, #272, #281, #282 and #284, and after #285, #286, #287 and #290). The rows below are
measured from the files by `ceph/shared_harness_inventory_test.go` and compared
with this table, so a drifting row fails the test; the pinned commit is the only
part that is not checked and is refreshed by hand. Source reading and Docker-free
tests only; nothing here is native evidence.

To refresh, run the guard verbosely, copy the logged rows and update the commit:

```
env -u GOROOT go test ./ceph -run '^TestSharedHarnessInventoryMatchesTheNote$' -v
```

The measurement is lexical. "Harness files" are `ceph/shared.go`,
`ceph/shared_exclusive.go`, `ceph/shared_services.go`, `ceph/tenant.go` and
`ceph/tenant_observe.go`. A "private name" is an unexported package-level name
declared in another non-test file of the package that a harness file uses without
declaring it (the parser's unresolved identifiers, so a local variable that
shadows a package-level name is not counted). Reference counts are regexp matches
of qualified `ceph.X` uses of the 16 package-level names that move, in all Go
files of the directory including tests.

| Measure | Value |
| --- | --- |
| exported types that move | 6 |
| exported constants that move | 3 |
| exported package functions that move | 7 |
| exported SharedCluster methods that move | 4 |
| exported Tenant methods that move | 8 |
| exported declarations that move | 28 |
| private names of package ceph that the harness files name | classify, classifyDocker, clientIDPattern, clusterOps, containerBackend, invalidRequest, newLeasedObserver, sharedBackend, sharedExclusively, sharedInvariants, sharedStarter, sharedWithTenants, startSharedContainer |
| private names of package rbd that its tenant.go names | bootstrapKind, invalid, validateResourceName |
| private names of package cephfs that its tenant.go names | cephFSBootstrapKind, invalid |
| private names of package rgw that its tenant.go names | gateways, gatewaysFrom, invalid, rgwBootstrapKind |
| references to moved names in rbd | 4 in 1 files |
| references to moved names in cephfs | 3 in 1 files |
| references to moved names in rgw | 4 in 1 files |
| references to moved names in internal/servicehook | 2 in 1 files |
| references to moved names in internal/integration | 36 in 6 files |
| testhelper.TestTenantCluster calls in internal/integration | 4 in 2 files |
| servicehook.RegisterSharedService calls in rbd, cephfs and rgw | 3 |

What changed against the older tables, so none of them is carried forward:

- The 28 declarations that move are the older 35 less the eight `Tenant` read-only
  forwarders (now `Observer` queries) plus `ObserveTenant`, which was counted
  apart as a reverse dependency. The 16 package-level names are unchanged.
- The private reach of the harness files is no longer a set of `Container`
  members. They name no `Container` field or method (`TestContainerBackendReach`,
  `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList`); what remains is
  the 13 package-level names above: four error helpers, the backend port types
  and its starter, the two gate modes, and `newLeasedObserver`.
- `internal/integration` uses the moved names 36 times in 6 files, not 30 in 5.
  The test-only tenant-cluster lookup is no longer a `servicehook` function
  (#286): it is `testhelper.TestTenantCluster`, served by
  `unitAccess.UnitTenantCluster` through the `hookreg.Test` seam, with four call
  sites outside `ceph`, all native controls. `internal/servicehook` names the
  moved names twice, not four times, for that reason.
- The adapters' private reach is new: each service's `tenant.go` names two to
  four unexported helpers of its own package. They are the inputs of decision 8.

### Prerequisites at `83de22a`

| Prerequisite | State | Evidence |
| --- | --- | --- |
| Pure invariant logic outside `ceph` | Met | `internal/sharedinvariants` |
| Tenant port; adapters off `TenantCluster` | Met (#200) | rbd, cephfs and rgw call no `TenantCluster`; the row above counts only `internal/integration` |
| Orchestration and `Tenant` free of `*Container` | Met (#253) | `ceph/shared_ops.go`, `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList` |
| Backend port: 12-method `sharedBackend`, nine core methods, zero `Container` fields | Met (#272) | `ceph/shared_cluster_port.go`, `TestContainerBackendReach` |
| `Observer` and `poolLease` free of `*Tenant` | Met (#266) | `ceph/tenant_observe.go`, `TestObserverAndPoolLeaseNameNoTenant` |
| Facts, observation views, auth records, OSD storage and registry, run directory, port leases, override and fault registries | Met | the records under `container-decomposition/` (formerly the internal slices before the control-plane slice, see the historical ordinals in `container-decomposition.md`) |
| Membership and control: control plane, bootstrap record, extension store, member roster, startup identity | Met (#281, #282) | `container-decomposition/` records `control-plane-state.md`, `bootstrap-record.md`, `extension-store.md`, `member-roster.md` and `startup-identity.md` (formerly slices eleven to fifteen) |
| Disposition of every remaining `Container` field | Met (#284) | `ceph/container_residual_fields_test.go`, sixteenth slice |
| Cross-package registration contract | Specified below, not implemented | this note |
| Public move, exported signatures, API catalogue moves, direct caller migration, native acceptance | Not started | |

The older "waits for the membership and control split" condition no longer
applies. Whether #146 as a whole is accepted is the owner's review, not this
note's claim.

## Cross-package registration design

Goal: `ceph/harness` and the three service harnesses compile against `ceph`
without any of them importing a package that imports them, expose no raw cluster,
lock, credential or executor, and keep the behavior that "Preserved behavior to
carry" lists. Only the private names in the inventory have to cross the
boundary; this section assigns each one a route.

### Packages and import graph

Two new internal packages follow the existing typed-hook pattern
(`internal/faulthook`, `internal/overridehook`): `internal/hookreg` stays free of
imports and holds an untyped slot, and a typed package names the interface.

```
ceph                 --> internal/hookreg (sets Harness), no harness import
internal/harnesshook --> ceph, internal/hookreg, internal/sharedinvariants
ceph/harness         --> ceph, internal/harnesshook, internal/hookreg,
                         internal/naming, internal/sharedinvariants
internal/servicehook --> ceph, ceph/harness, internal/hookreg, supervised
rbd/harness, cephfs/harness, rgw/harness
                     --> their service package, ceph, ceph/harness, internal/servicehook
```

`internal/servicehook` cannot host the backend slot. After the move it names
`*harness.Tenant`, so it imports `ceph/harness`, and `ceph/harness` must then not
import it. That is why the backend slot gets its own package, which `ceph/harness`
may import because it imports `ceph` and not `ceph/harness`.

Two untyped slots in `internal/hookreg`:

| Slot | Set by | Set when | Typed by | Holds |
| --- | --- | --- | --- | --- |
| `Harness` | package `ceph`, in its `init` (as `Value`, `Fault`, `Override` are) | before any importer's `init` | `internal/harnesshook` | `Core`: start, tenant observer, lend |
| `HarnessTenant` | package `ceph/harness`, in its `init` | before `internal/servicehook` and the service harnesses initialize, because both import `ceph/harness` | `internal/servicehook` | the tenant port lookup (typed by `internal/servicehook`) and the test-only cluster lookup (typed by `internal/testhelper`); each consumer asserts only the method it uses |

These two slots are the internal seams of `ceph`'s own backend and of the harness's
tenant port; they are not service registration, which the owner removed (see
"Service support"). Each typed accessor panics with a message naming the missing registrant when its
slot is empty, as `servicehook.get` does today. Go initializes an imported package
before its importer, so no registration depends on call order. A Docker-free
control imports the harness and asserts that both slots are set and that
`go list -deps` of `ceph` and `ceph/harness` list none of the forbidden packages
(see "Guards").

### What `ceph` registers: `harnesshook.Core`

The registered value is implemented by package `ceph`, so every method signature
of the registered interfaces may use only types that `ceph` itself can spell:
its own types, the standard library, `testcontainers-go`, `internal/hookreg` and
`internal/sharedinvariants` (a leaf). It must not name a type declared in
`internal/harnesshook`, which imports `ceph`. Go requires identical result types
for an interface to be satisfied, so a ceph-owned interface with the same methods
is not accepted where `harnesshook.Backend` is expected, and importing
`harnesshook` from `ceph` to spell the type is an import cycle. A scratch module
with `hookreg`, `ceph` and `harnesshook` packages shows both failures with that
shape (the typed assertion on the registered value fails at run time, and the
import is rejected as a cycle) and compiles and runs with the shape below.

The boundary therefore has two layers in `internal/harnesshook`:

1. Port interfaces that the registered values satisfy structurally. They use
   `any` wherever the harness-facing type would be a `harnesshook` type, and
   `int32` for the sharing mode.
2. Harness-facing types (`Backend`, `Ops`, `Mode`) and typed accessors. They wrap
   an asserted port value, so the harness sees typed methods and `ceph` never
   names them.

```go
// Layer 1: satisfied structurally by values that package ceph registers.
type core interface {
    // Start starts a cluster. On failure it still returns the partial
    // backend, which the harness owns and terminates (sharedRunResult today).
    Start(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (any, error)
    // NewTenantObserver builds the tenant form of an Observer. target returns
    // the tenant's backend value after its admission check.
    NewTenantObserver(target func() (any, error), live func() error) *ceph.Observer
    // Lend returns the gated cluster of a backend value. It is called only by
    // SharedCluster.Exclusive, after SetSharing(Exclusive), and by the
    // test-only cluster lookup; a model backend is nil here and gives nil.
    Lend(backend any) *ceph.Container
    // The error contract, see "Error handling".
    InvalidRequest(format string, args ...any) error
    Classify(kind, err error) error
    ClassifyDocker(ctx context.Context, err error) error
}

type backendValue interface { // the returned backend, today's 12-method sharedBackend
    CreatePool(ctx context.Context, name string) error
    PrepareServices(ctx context.Context, prepare []func(context.Context, *ceph.Container) error) error
    Ready(ctx context.Context) error
    Invariants(ctx context.Context, pool string) (sharedinvariants.Invariants, error)
    Resources(ctx context.Context, probe []func(context.Context, *ceph.Container) (map[string]string, error)) (map[string]string, error)
    DaemonsRunning(ctx context.Context) error
    CreateClient(ctx context.Context, entity string, caps ceph.ClientCaps) (client *ceph.ClientConfig, created bool, err error)
    DeleteClient(ctx context.Context, client *ceph.ClientConfig) error
    PurgeNamespace(ctx context.Context, pool, namespace string) error
    SetSharing(mode int32)
    Terminate(ctx context.Context) error
    Ops() any // nil for a model
}

type opsValue interface { // clusterOps without raw
    StartupTimeout() time.Duration
    Command(ctx context.Context, args ...string) ([]byte, error)
    WithTopology(fn func() error) error
    Extension(kind string, create func() any) any
    RunNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
    WithClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
}

// Layer 2: what ceph/harness imports. Backend has the methods of backendValue
// with SetSharing(mode Mode) and Ops() Ops; Ops has those of opsValue.
// Start, NewTenantObserver and Lend assert the values of layer 1 and panic with
// a message naming the registrant if one does not implement its port.
type Mode int32 // NotShared, SharedWithTenants, SharedExclusively; the gate keeps its policy tables
```

`ceph` implements the layer-1 interfaces on unexported types with exported method
names (as `serviceHooks` and `unitAccess` do), so it spells no `harnesshook`
type. The changes against today, each removing one use of a private name:

1. `CreateClient` returns `created`. `Tenant.createClient` reads the private field
   `ClientConfig.created` to decide whether the release must delete the client; a
   cross-package tenant cannot read it, and the backend already knows
   (`createSharedClient` sets it).
2. `Mode` replaces `sharingMode`. The values map one to one; the policy tables,
   `Container.sharing` and `ErrSharedCluster` stay in core (decided above).
3. `raw()` leaves `Ops`. The raw pointer is reachable only through `Core.Lend`,
   which keeps `Ops` safe to hand to service harnesses.
4. The three error helpers are reached through the `core` interface, not copied
   (see "Error handling").

`Start`, `NewTenantObserver` and `Lend` replace `startSharedContainer`,
`newLeasedObserver` and `clusterOps.raw`. The unit-test seam `sharedSettings.start`
stays a private field of the harness with the type
`func(ctx, image, opts) (Backend, error)`, so the model-backed tests
(`shared_test.go`, `shared_exclusive_test.go`, `shared_services_test.go`) move with
the orchestration and need no public option.

### Service support: prepare and probe callbacks

Owner decision of 2026-10-28 ([comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469630041)): no `init` or import side
effect registers service harness support, and no explicitly called `Register`
function mutates a process-global registry. The shared harness instance owns the
service support it was constructed with and its lifecycle. This supersedes this
note's earlier proposal (a registry in `ceph` filled by each service harness's
`init`). The decision fixes the policy; exact option, interface and function names
and the duplicate and missing-support validation are design work, and the shapes
below are not frozen signatures.

The callbacks still take `*ceph.Container` (`sharedService.prepare` and `probe`
today). The cluster pointer is handled as follows, and the harness never receives
it:

- A service support value carries a kind and the two callbacks. It is an opaque
  type of `ceph/harness` with unexported fields, so the public API shows no
  callback that receives a cluster. A service harness builds it through an
  internal route (the `HarnessTenant` value offers a constructor that
  `internal/servicehook` types), and offers it to the program, for example as
  `rbdharness.Support()`. A consumer then reads
  `cephharness.NewShared(image, cephharness.WithServices(rbdharness.Support()))` or
  an equivalent; that spelling is illustrative.
- The instance stores the supports in its settings. Nothing is global: two shared
  harness instances in one test binary can select different services, and a test
  binary that selects none prepares and probes none.
- The ports take the callbacks as arguments instead of reading a registry:
  `Backend.PrepareServices(ctx, prepare []func(context.Context, *ceph.Container) error)`
  and `Backend.Resources(ctx, probe []func(context.Context, *ceph.Container) (map[string]string, error))`.
  Both parameter types use only `ceph` types and the standard library, so the
  layer-1 interfaces can spell them. `ceph` calls each callback with the pointer of
  the backend's own cluster, in the order given. The harness sorts by kind, as
  `registeredSharedServices` does today, and wraps each callback so its error keeps
  the `prepare %s for a shared cluster` and `list the %s resources` context.
  `ceph` keeps no registry: `registerSharedService`, `registeredSharedServices`,
  `servicehook.RegisterSharedService` and the `RegisterSharedService` hook method
  are removed without an alias (0.x).
- Validation belongs to construction: a duplicate kind, or a support that was
  never constructed by a service harness, is an `ErrInvalidRequest` recorded in the
  settings and returned by the first `Lease` or `Exclusive`, as an invalid
  `WithMaxTenants` is today. A service harness `Lease` on an instance that was not
  given its support fails before it creates anything, through a check on the tenant
  port (for example a `RequireService(kind)` method, name not frozen), instead of
  creating a resource that no probe would attribute.
- Lifecycle and cleanup: supports hold no process resource of their own beyond what
  a tenant attaches through `AttachScope`. Partial-failure behavior is unchanged: a
  failed `PrepareServices` fails the start of that cluster, whose partial backend
  stays owned and is terminated; a failed probe is an unverifiable release and
  poisons the cluster; tenant scopes are released in reverse order before clients
  are deleted. Callers connect the existing `Close` lifecycle to `TestMain`,
  `t.Cleanup` or `defer`; `RunTests` keeps closing every instance, which is the
  complete cleanup contract, not `defer` alone.
- Handing the pointer to the harness is rejected: it would give `ceph/harness` a raw
  cluster.

`releaseScopes` and `tenantScope` are tenant state and move to `ceph/harness`.

### Tenant port for the service harnesses

`tenantPort` and `tenantLease` move to `ceph/harness`, built over the harness
`Tenant` and `Ops`. The service harnesses reach them through `internal/servicehook`,
which keeps its `TenantPort` interface and changes `TenantPort(t *ceph.Tenant)` to
`TenantPort(t *harness.Tenant)`; the lookup goes through the `HarnessTenant`
slot, so `ceph/harness` does not import `servicehook`. The port exposes no
cluster, as today: `ID`, `Live`, `StartupTimeout`, `Command`, `CreateClient`,
`AttachScope`, `WithClientIdentity`, `WithTopology`, `Extension`. rgw keeps the
four `servicehook.Lock`/`Unlock` pairs on the gateway's own `*ceph.Container`
unchanged.

The test-only lookup is `testhelper.TestTenantCluster(t)`, which today reaches
`unitAccess.UnitTenantCluster(t *Tenant)` through the `hookreg.Test` seam
(`ceph/unit_access.go`, `internal/testhelper/testhelper.go`; #286). After the move
`ceph` cannot name `Tenant`, so `UnitTenantCluster` leaves `unitAccess`. Its
replacement is a second method of the value that `ceph/harness` registers in the
`HarnessTenant` slot: it checks that the tenant is live and returns
`Core.Lend(backend)`, or nil for a model. `internal/testhelper` asserts only that
method from the slot and takes `*harness.Tenant` in `TestTenantCluster`, so it
imports `ceph/harness` and `ceph/harness` does not import it. The unit tests of
that lookup in `ceph/shared_ops_test.go` move to `ceph/harness`. Its four call
sites are native controls in `internal/integration` and stay the only non-test
users (guard below).

### Observer constructor

Settled by the owner on 2026-10-28 ([comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469688158)); the properties below follow it. The query implementation and the public `Observer` type stay in `ceph`, and only the Tenant-specific entry point moves.

`harness.ObserveTenant(t *harness.Tenant) (*ceph.Observer, error)` replaces
`ceph.ObserveTenant`. A nil tenant returns the same `ErrInvalidRequest` error and a
nil Observer. Otherwise it calls `Core.NewTenantObserver(target, live)` where
`target` runs the tenant's admission check and returns the tenant's `Backend`, and
`live` is `Tenant.check`. Inside `ceph`, `NewTenantObserver` unwraps the backend to
the gated cluster (a model fails with the text it fails with today) and calls
`newLeasedObserver`. Properties carried over unchanged:

- constructing captures nothing, and every query resolves the target again and
  re-checks the lease;
- a listed `Pool` carries the same `live` check, which runs before the tenant
  admission policy on every operation;
- the Observer exposes only queries and never the cluster; `Observer`, `Pool` and
  `poolLease` stay in `ceph` and name no `Tenant`;
- the bridge is internal: `NewTenantObserver` is a method of the registered core, not
  public API, and no public executor, resolver or authorization callback is added;
- a retained Observer or Pool cannot gain access after release or acquire the
  privileges of a later exclusive interval, because the original tenant is
  re-checked on every query and every Pool operation;
- callers of `ceph.ObserveTenant` migrate directly, with no forwarding function.

The closures retain the original `*harness.Tenant` receiver exactly as the bound
methods do today (#266 states this lifetime relationship): the dependency is not
removed, only its type direction.

### SharedPool and Exclusive

`SharedPool` is settled by the owner on 2026-10-28 ([comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469654249)):

- The public constant moves to `ceph/harness` with the orchestration. There is no
  `ceph.SharedPool` alias and no forwarding declaration; callers and documentation
  migrate directly to `harness.SharedPool` (with an import alias where needed). The
  value `"tenants"` and the isolation and cleanup behavior do not change.
- Non-test code uses it only in `shared.go` and `tenant.go`; `observer.go` mentions
  it in a comment. Core checks do not import the harness. Today they receive the
  pool name as an argument (`Backend.Invariants(ctx, pool)`,
  `PurgeNamespace(ctx, pool, ns)`, `sharedinvariants.ReleaseDiff`), which is the
  explicit input the owner allows. If a core check ever needs the value without an
  argument, it takes it from one internal definition (recommended: a constant in the
  leaf `internal/sharedinvariants` that the public constant is declared from), not
  from a second literal. The mechanism is implementation design.

`Exclusive` is settled by the owner on 2026-10-28 ([comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469536710)):

- `Exclusive(t testing.TB) *ceph.Container` keeps returning the core cluster handle
  after the move. No second public exclusive-only facade is added. The harness
  obtains the pointer only through `Core.Lend`, with the gate opened by
  `SetSharing(SharedExclusively)`.
- The decision is lease-bounded authorization, not authority attached to a retained
  pointer. Cluster-wide use is allowed only while that exclusive interval is active;
  a retained `Container` or resource handle must not exercise it after the interval
  ends, including during a later tenant or exclusive interval. The move must show
  the admission and lifetime enforcement for stale handles and for generation reuse,
  and must audit direct, derived and embedded access paths (the promoted Docker
  methods of the embedded container, `Pool` values, `ClientConfig`, `OSDContainer`
  and other resource handles) and name any enforcement gap before the extraction is
  accepted. This note does not claim that audit: the current gate decides by the
  cluster's sharing mode (`admitHandleAs`), and whether it distinguishes one
  exclusive interval from the next after release is part of the audit.
- The separate restrictions of ordinary tenant handles, ownership, cancellation,
  lock order and uncertain-outcome handling are unchanged.
- A guard pins that `Lend` is called from `Exclusive` and from the test-only lookup
  and nowhere else, that no harness field stores a `*ceph.Container`, and that `Ops`
  has no method returning one (the successor of
  `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList`). It checks where the
  pointer is handed out, not the stale-handle enforcement above.
- `Exclusive` still sets the mode before it lends and the release still skips
  verification for a retired instance; those state-machine rules move unchanged.

### Error handling

Settled by the owner on 2026-10-28 ([comment](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469722063)): the harness shares `OperationError`
construction and classification through a narrow internal contract. It adds no
public error-helper API, copies no core classification into the harness, keeps
every category, cause, `errors.Is`/`errors.As` result and required diagnostic
message, and keeps the dependency direction (no core-to-harness import).

- The contract is the three error methods of the `core` interface above:
  `InvalidRequest`, `Classify` and `ClassifyDocker`. `ceph` implements each with
  the code it has today (`invalidRequest`, `classify`, `classifyDocker`), so there
  is one implementation. Their signatures use only `error`, `string`, `context` and
  `any`, which `ceph` can spell. `ceph/harness` calls them through
  `internal/harnesshook`; the exact internal names remain implementation design.
- `Tenant.RunNativeCommand` keeps its container inspection in the harness and
  classifies a failed `Inspect` with `ClassifyDocker`. No `Ops.Label` is needed.
- `clientIDPattern` is `naming.ClientID`, the one definition both packages
  already import.
- Local contextual messages (`"create tenant client: %w"`, the `fmt.Errorf` wraps
  around a backend error) stay local, as the owner allows.
- Errors that cross `Backend` (a failed start, scripts, `CreateClient`) are
  returned as `ceph` built them and wrapped with `%w`, so the kind is not rebuilt.
  A partial `Start` result still comes with the error.
- Verification at the move, against the migrated paths and not by compilation:
  invalid input (an invalid tenant client name, an unknown access, a nil tenant,
  a released tenant), cancellation and deadline (`Lease`, a started backend,
  `RunNativeCommand`), shared admission (`ErrSharedCluster` through a leased
  Observer and a retained Pool) and a Docker failure of the inspection (not found
  or conflict, transport error, closed context). Each asserts the kind with
  `errors.Is`, the type with `errors.As` and the message text.

### Service harness private reach

The inventory shows what each adapter file names in its own service package:
`bootstrapKind`, `validateResourceName` and `invalid` in rbd, `cephFSBootstrapKind`
and `invalid` in cephfs, `gateways`, `gatewaysFrom`, `invalid` and
`rgwBootstrapKind` in rgw. The recommendation is to move each into a
`<service>/internal/...` package that the service and its `harness` child both
import (the internal-directory rule admits any package rooted at `rbd/`), so no
new public name appears. This is a source move inside each service, independent
of `ceph/harness`.

### Order of work

1. Internal, no public API: `Backend.CreateClient` returns `created`. It touches
   `sharedBackend`, its test model and `tenant.go`, and is the only code-level
   preparation that the design needs before the move. It is left to a separate
   change. The error contract needs no preparation: it is added with the move
   because it has no caller before then.
2. Internal: move the service-private helpers into `<service>/internal/...`
   (decision 8), one change per service.
3. The move: `ceph/harness`, `internal/harnesshook`, the two slots, the three
   service harnesses, `servicehook`, `ObserveTenant`, direct caller migration,
   catalogue regeneration and the `moves` record, the package-split shard, the
   release fragment and the v0.4 upgrade mapping, then extraction-specific native
   acceptance on the Linux runner.

### Guards the move adds

Docker-free, with negative controls in the style of `TestContainerBackendReach`:
the graph test over `go list -deps` (both directions above); the `Lend` allow-list;
that `harnesshook.Backend` names no `Container` type and no lock; that each
consumer of the `HarnessTenant` slot (`servicehook`, `testhelper`) asserts only its
own method; a compile example, kept as a test, that the registered `ceph` values
satisfy the layer-1 interfaces; the error-equivalence checks of "Error handling"; and the inventory test of this note, retargeted at the new files.

## Decisions for the owner

Owner decisions of 2026-10-28 on #155 are recorded below with their comment links
and are followed by the design text, which this note no longer proposes to
reverse. Only the items that the owner has not decided stay open, each with a
recommendation. Nothing here is implemented.

### Settled by the owner

| Topic | Decision | Comment |
| --- | --- | --- |
| `Exclusive` handle | `*ceph.Container` stays the handle during an active exclusive interval; no second facade; authority is lease-bounded and the move must show stale-handle and generation-reuse enforcement and audit the access paths | [12:55Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469536710) |
| Service harness registration | No `init` or import-side-effect registration and no process-global registry behind a `Register` function; the shared harness instance is given its service support at construction and owns its lifecycle; option names and duplicate or missing-support validation are design work | [13:00Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469630041) |
| `SharedPool` | The public constant moves to `ceph/harness`; no `ceph.SharedPool` alias; core checks use an internal shared definition or an explicit input, never a second literal | [13:01Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469654249) |
| Observer construction | `Observer` and its queries stay in `ceph`; `ObserveTenant` moves to `ceph/harness`; core names no `Tenant` and imports no harness; the original tenant is revalidated on every query; no public executor, resolver or authorization callback; direct caller migration | [13:03Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469688158) |
| Error helpers | One narrow internal contract; no public helper, no copy of core classification; categories, causes, `errors.Is`/`errors.As` and diagnostics preserved; classification verified on the migrated paths | [13:04Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469722063) |

The earlier layout decision (2026-10-27: `ceph/harness`, `rbd/harness`,
`cephfs/harness`, `rgw/harness`, direct v0.x migration) is unchanged. Decisions 1
to 4 and 6 of the first draft of this list are therefore closed, and the first
draft's `init`-time recommendation for decision 1 is withdrawn.

### Still open

| # | Decision | Recommendation | Alternative and its cost |
| --- | --- | --- | --- |
| 5 | Typed hook placement and the signature boundary between `ceph` and the harness (the owner left the internal package and port mechanism to implementation) | Top-level `internal/harnesshook` beside `faulthook` and `overridehook`, with port interfaces in `any`/`int32` terms that `ceph` satisfies structurally and harness-facing wrappers on the other side | `ceph/internal/...` is not importable by `internal/servicehook`; a shared leaf package for the port types cannot name `ceph.ClientCaps` and the other public types without importing `ceph` |
| 7 | Test-only tenant-cluster lookup (`testhelper.TestTenantCluster`, today `unitAccess.UnitTenantCluster`) | Keep it test-only: a second method of the `HarnessTenant` value, asserted by `testhelper`, four known native call sites, guarded | Replacing the four native controls with Observer-based checks weakens what they prove about the gate |
| 8 | Service-private helpers used by the adapters | `<service>/internal/...` packages, moved before the harness | Exporting them adds public names to rbd, cephfs and rgw |
| 9 | Exported names and signatures beyond the settled ones | The old-to-new map: same names in the new packages, no renames, no new options other than the service-support input; any rename is a later change | Renaming during the move mixes a mechanical migration with an API review |
| 10 | Shape of the service-support input and its validation | An opaque `ceph/harness` value built by each service harness through an internal route and passed to `NewShared` as an option, validated at construction (duplicate kind, foreign value) and checked by a tenant-port method before a service `Lease` creates anything | A positional `NewShared` argument changes the signature for every caller; a plain callback struct would show the cluster-receiving callbacks in public API |
| 11 | Stale-handle audit result for `Exclusive` | Do the audit as the first step of the move, as a Docker-free test over the access paths plus a native control, and record every gap | Deferring it until after the move would accept a handle whose enforcement is unproven |

Overlap: #285 and #286 (renames and the test-only lookup move) are merged and
composed into this note.

## Not covered

No exported symbol was moved, no public signature was added and no package
import changed. The only code in this change is the Docker-free inventory guard
and the CI classifier entry that makes an edit of this note run it; no production
code changed. No Docker or native Ceph test was run. The sections from "Method
and limits" to "Refresh at `1a158b9`" are history and keep their older counts;
"Inventory at `83de22a`" is the current state and must be refreshed, with its
pinned commit, whenever the guard fails or the move starts.
