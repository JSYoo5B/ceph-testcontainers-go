# Shared-cluster and tenant harness: inventory and boundary

This note is the measured inventory and the package boundary of the shared-cluster
and tenant harness (#155). The owner decided the package layout on 2026-10-27
(see "Decided layout"), and the move itself is implemented by the change that
adds "Implementation of the move" below: `ceph/harness`, `rbd/harness`,
`cephfs/harness` and `rgw/harness` exist, the old names are removed without
aliases (0.x), and service support is selected explicitly with
`ceph/harness.WithServices`. The sections that describe the state before the move
keep their wording as the design record and say so; "Inventory at `55d60e5`"
is the current, guarded state. The internal state, lock and admission
decomposition of #146 that blocked the move is complete (see
[container-decomposition.md](container-decomposition.md)).

How to read the sections: "Inventory at `55d60e5`" is the current, guarded
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
| `rbd` | `Tenant`, `Command`, `StartupTimeout`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `rbd/harness.Support()`, built with `servicehook.Support(servicekind.RBD, nil, probeTenantNamespaces)`; no registration (history: an `init` registration into a transitional registry) |
| `cephfs` | `Tenant`, `Command`, `StartupTimeout`, `Live`, `AttachScope`, `CreateClient`, `WithClientIdentity` | `cephfs/harness.Support()`, built with `servicehook.Support(servicekind.CephFS, prepareShared, probeShared)`; no registration |
| `rgw` | `Tenant`, `Live`, `WithTopology`, `Extension`, `AttachScope` | `rgw/harness.Support()`, built with `servicehook.Support(servicekind.RGW, prepareShared, probeShared)`; no registration |

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
| `prepareServices`, `resources` | the pointer itself | handed to the `prepare` and `probe` callbacks that the `SharedCluster` passes as arguments (its own service set), which take the cluster |

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

## Inventory at `55d60e5` (after the move)

Measured on the change that moves the harness (issue 155), on top of the audit
base `55d60e5` (main at `ca24db6` plus the Exclusive stale-handle audit). The
rows below are measured from the files by `ceph/shared_harness_inventory_test.go`
and compared with this table, so a drifting row fails the test; the pinned
commit is the only part that is not checked and is refreshed by hand. Source
reading and Docker-free tests only; nothing here is native evidence.

To refresh, run the guard verbosely, copy the logged rows and update the commit:

```
env -u GOROOT go test ./ceph -run '^TestSharedHarnessInventoryMatchesTheNote$' -v
```

The measurement is lexical. "Exported" rows count the declarations of the
non-test files of `ceph/harness`. A "private name" is an unexported
package-level name declared in another non-test file of the service package
that its harness `tenant.go` uses without declaring it (the parser's unresolved
identifiers, so a local variable that shadows a package-level name is not
counted); the harness packages cannot name such a name at all, so the row is a
guard that no helper is declared twice. Reference counts are regexp matches of
qualified `ceph.X`, `rbd.Lease` and similar uses of the moved names under their
old packages; after the move they are the removed names, so the expected value
is zero everywhere, tests and documentation prose included (a file that must
spell one, such as a negative control, builds it from parts).

| Measure | Value |
| --- | --- |
| exported SharedCluster methods in ceph/harness | 4 |
| exported Tenant methods in ceph/harness | 8 |
| exported constants in ceph/harness | 3 |
| exported declarations in ceph/harness | 30 |
| exported package functions in ceph/harness | 8 |
| exported types in ceph/harness | 7 |
| module packages that ceph/harness imports | ceph, internal/harnesshook, internal/hookreg, internal/naming, internal/sharedinvariants |
| module packages that import ceph/harness | cephfs/harness, internal/servicehook, internal/testhelper, rbd/harness, rgw/harness |
| port RequireService calls in rbd, cephfs and rgw harness packages | 3 |
| private names of package cephfs that its harness tenant.go names |  |
| private names of package rbd that its harness tenant.go names |  |
| private names of package rgw that its harness tenant.go names |  |
| references to moved names under the old package in ceph | 0 in 0 files |
| references to moved names under the old package in cephfs | 0 in 0 files |
| references to moved names under the old package in internal/integration | 0 in 0 files |
| references to moved names under the old package in internal/servicehook | 0 in 0 files |
| references to moved names under the old package in rbd | 0 in 0 files |
| references to moved names under the old package in rgw | 0 in 0 files |
| servicehook.RegisterSharedService calls in rbd, cephfs and rgw | 0 |
| servicehook.Support calls in rbd, cephfs and rgw harness packages | 3 |
| testhelper.TestTenantCluster calls in internal/integration | 4 in 2 files |

What changed against the table of `83de22a`, so none of it is carried forward:

- The 28 declarations that move became 30 in `ceph/harness`: the same 28 plus the
  two new ones, `ServiceSupport` and `WithServices`. `SharedPool` is declared
  there from `internal/sharedinvariants`; the `ceph` declaration is deleted
  without an alias.
- The old private reach of the harness files is gone by construction: the files
  are in another package and name `ceph` only through its exported names and
  `internal/harnesshook`. The twelve names of the old row (the error helpers,
  the backend port types and its starter, the gate modes, `newLeasedObserver`)
  are now the harness-side adapters of `ceph/harness/core.go`.
- `internal/integration` uses the moved names under their new packages; the
  four `testhelper.TestTenantCluster` calls are unchanged native controls.
- `servicehook.RegisterSharedService` has no caller or declaration; each service
  harness calls `servicehook.Support` once from its `Support` function.

### Prerequisites after the move

| Prerequisite | State | Evidence |
| --- | --- | --- |
| Pure invariant logic outside `ceph` | Met | `internal/sharedinvariants` |
| Tenant port; adapters off `TenantCluster` | Met (#200) | the harness packages call no `TenantCluster` |
| Orchestration and `Tenant` free of `*Container` | Met (#253) | `ceph/harness/shared_ops_test.go`, `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList` |
| Backend port | Met (#272), now the `harnesshook.Backend` contract | `ceph/shared_backend.go`, `TestContainerBackendReach` |
| `Observer` and `poolLease` free of `*Tenant` | Met (#266) | `ceph/observer.go`, `TestObserverAndPoolLeaseNameNoTenant` |
| Cross-package registration contract | Implemented | `internal/harnesshook`, `ceph/harness/core.go`, `ceph/harness/tenant_port.go` |
| Service-private reach of the adapters | Met (decision 8) and one new seam | the leaf packages of "Service harness private reach", `rgw/internal/gatewayadmin` |
| Public move, direct caller migration, catalogue moves | Done by this change | `docs/design/api-naming/moves/155-harness-package-move.json`, `docs/guides/upgrading-harness-packages.md` |
| Native acceptance of the extraction | Not run here | see "Not covered" |

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

### Instance-owned service set: first slice

Implemented in `ceph` behind unexported names; no public name is chosen and no
package moves.

- `sharedServiceSet` (`ceph/shared_services.go`) is the service support one
  `SharedCluster` owns. `NewShared` builds it once into `sharedSettings.services`,
  sorted by kind, with the wrapped callbacks (`prepare %s for a shared cluster`,
  `list the %s resources`, same text and `errors.Is` chain as before). It is
  immutable afterwards and holds no lock; no mutex is added (guard:
  `TestSharedServiceSetHoldsNoLock`). Two `SharedCluster` values in one process
  can own different sets.
- The backend port takes the callbacks as arguments:
  `prepareServices(ctx, []sharedPrepare)` and `resources(ctx, []sharedProbe)`,
  the form specified above. `containerBackend` hands its own `*Container` to each
  callback; the registry is no longer read by the backend or the orchestration.
- Validation at construction, reported as `ErrInvalidRequest` by the first
  `Lease` or `Exclusive` before any cluster starts: a service without a kind (the
  zero value) and a kind given twice. A nil `prepare` or `probe` stays allowed (rbd
  has no `prepare`).
- Missing support: `tenantPort.RequireService(kind)` (also on
  `servicehook.TenantPort`) fails with `ErrInvalidRequest` when the instance was
  not given the kind. `rbd.Lease`, `cephfs.Lease` and `rgw.Lease` call it first,
  after taking the port and before any command or `AttachScope`
  (`TestServiceAdaptersRequireTheirSupportBeforeCreating`; inventory row).
- Cleanup and partial failure are unchanged and characterized
  (`ceph/shared_services_registry_test.go`, `ceph/shared_service_support_test.go`):
  a failed prepare stops the later ones, fails the start, and the partial cluster
  stays owned and is terminated; a failed probe at release is an unverifiable
  release and poisons the cluster. The set owns no process resource, so it has no
  cleanup of its own.
- Transitional: until the exported option exists, a service package cannot hand
  its support to `NewShared`, so `init` still fills the process-global registry
  (`registerSharedService`, `servicehook.RegisterSharedService`, 3 calls) and
  `NewShared` snapshots it once when no `withSharedServices` option was given.
  `NewShared` is the only reader (`TestSharedServiceRegistryHasOneReader`). One
  deliberate difference from the call-time registry: a service registered after
  `NewShared` is not seen by that cluster (`TestRegistryIsReadOnceByNewShared`);
  `init` registration runs before any `NewShared`, so no in-repo caller changes.
  The zero-kind and repeated-kind checks also apply to a registration that
  reaches `NewShared`, where the registry used to accept it.
- Model clusters in the unit tests pass an explicit empty set
  (`newFakeShared`), because the test binary links the real service packages.

Proposal for the exported form (not implemented, owner choice 10):

| Piece | Proposed spelling |
| --- | --- |
| Opaque value | `type ServiceSupport struct{ ... }` in `ceph/harness`, unexported fields, built only through an internal route typed by `internal/servicehook` |
| Option | `func WithServices(supports ...ServiceSupport) SharedOption`; validation as above, recorded in the settings |
| Service harness | `func Support() cephharness.ServiceSupport` in `rbd/harness`, `cephfs/harness` and `rgw/harness` |
| Callers | `internal/integration` (2 `NewShared` calls), `docs/ceph.md` (3 examples), `docs/rbd.md`, `docs/cephfs.md`, `docs/rgw.md`; the 3 `init` registrations, `servicehook.RegisterSharedService`, `registerSharedService`, `registeredSharedServices` and the registry are deleted without an alias (0.x) |

Link effect: today importing `rbd`, `cephfs` or `rgw` registers its probe for
every shared cluster of the binary. After the exported option, importing a
service harness registers nothing; a cluster that was not given the support
prepares and probes nothing, and the service `Lease` fails with
`ErrInvalidRequest` at `RequireService` before it creates anything. A program
that relied on the import now lists `WithServices(...)`; the migration is
mechanical (one option per service the binary leases).

Open choices that remain the owner's, with a recommendation: (1) the option and
type names above (recommend them: they keep `WithServices` next to the other
`With...` options and name no cluster); (2) whether the unconfigured service
`Lease` error is `ErrInvalidRequest` (recommend yes, as an invalid
`WithMaxTenants` is); (3) whether to land the exported option before the package
move with the service packages' own `Support()` (recommend no: it would add a
public name to `rbd`, `cephfs` and `rgw` that the move renames; do it with the
move).

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
  explicit input the owner allows.
- Done as internal preparation (no public change): the leaf
  `internal/sharedinvariants` declares `SharedPool = "tenants"` as the one
  definition, and `ceph.SharedPool` is declared from it
  (`const SharedPool = sharedinvariants.SharedPool`). At the move the public constant
  is declared in `ceph/harness` from the same internal definition and the `ceph`
  declaration is deleted without an alias; the pool value is never spelled twice.
  `TestSharedPoolHasOneDefinitionInNonTestCode` scans every non-test Go file of the
  module for a second literal of the name (also as `pool=<name>` or `pool <name>`) and
  pins which file declares `SharedPool` from what; `TestSharedPoolValueIsTheExistingPoolIdentity`
  pins the value so a placement change cannot rename the pool; a negative control
  shows the readers report a copied literal and a redeclared constant. The scan
  needs no change at the move except the declaring-file row.

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

Decision 8 recommends moving each helper that an adapter's `tenant.go` names
into a leaf package that the service and its `harness` child both import (the
internal-directory rule admits any package rooted at the module or at the
service directory), so no new public name appears. The recommendation needs no
public name, so it is carried out in steps. The inventory below lists every
private name as measured at `8bff17d`, where it is defined, who else calls it
(non-test call sites outside `tenant.go`, then test files) and the step.

| Service | Name | Defined | Other callers | Destination | State |
| --- | --- | --- | --- | --- | --- |
| rbd | `bootstrapKind` (`"rbd"`) | `rbd/bootstrap.go` | `bootstrap.go` (3: `Kind`, `WithPools`, `defaultPool`) | `servicekind.RBD` in `internal/servicekind` | Moved; the old name is removed |
| rbd | `validateResourceName` | `rbd/pool.go` | `pool.go` (4), `image_clients.go` (3) | `rbd/internal/resourcename.Check`; each caller wraps the error with its own `invalid` | Moved; the old name is removed |
| cephfs | `cephFSBootstrapKind` (`"cephfs"`) | `cephfs/bootstrap.go` | `bootstrap.go` (1), `entry.go` (3) | `servicekind.CephFS` | Moved; the old name is removed |
| rgw | `rgwBootstrapKind` (`"rgw"`) | `rgw/bootstrap.go` | `bootstrap.go` (1), `entry.go` (3), one test | `servicekind.RGW` | Moved; the old name is removed |
| rbd, cephfs, rgw, rbd/mirror, cephfs/mirror | `invalid` | one copy per package: `&ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err}` | non-test call sites measured by AST: rbd 32, cephfs 67, rgw 62, `rbd/mirror` 28, `cephfs/mirror` 15 (an earlier count of this row differed because it counted another way); all kept | `harnesshook.Invalid` in `internal/harnesshook`, which reaches the one construction `newInvalid` in `ceph` through the `Harness` slot of `internal/hookreg` | Moved (decision 5 settled by the 2026-10-29 owner delegation); the five copies are removed without an alias |
| rgw | `gateways`, `gatewaysFrom`, `gatewayRegistry` | `rgw/entry.go` | `gateway.go`, `tenant.go`, tests | `internal/namedregistry.Registry[T any]`, instantiated as `Registry[*Gateway]`; `gatewaysFrom` and the `gatewayRegistry` type are removed; `tenant.go` reads it through `namedregistry.From[*Gateway]` | Moved (settled by the 2026-10-29 owner delegation on #155); `gateways` stays in `rgw` as the writer-side accessor that describes secrets and config targets |

The `servicekind` package is shared by the three services instead of one package
per service because each part is one string constant and the three kinds are the
keys of one instance-owned service set (`RequireService`, bootstrap kind,
extension store); a single leaf keeps them in one place for the harness packages.
`rbd/internal/resourcename` is rbd-owned because only rbd validates RBD names.
The move is old-name removal under the 0.x policy: no alias, wrapper or forward
remains, and `ceph/service_helper_moves_test.go` fails if a moved name reappears,
if a leaf package imports a service package, or if a `tenant.go` names a
private name outside the table above.

The registry row was settled by the owner delegation of 2026-10-29 on #155: a
small generic `Registry[T any]` in a leaf package, with the value type staying in
`rgw` and no new public name. The credentials and config targets that the cluster
reads from a service state are described by the owning package with
`Registry.Describe`, so a registry that a tenant created first reports them once
the owner takes it for writing (`TestGatewayRegistryCreatedByAReaderIsDescribedByTheOwner`).
Sorting is by registry key, which `startGateway` sets to the gateway name.

The `invalid` row follows decision 5 as settled by the owner delegation of
2026-10-29 on #155: the shared construction lives in `internal/harnesshook` beside
`faulthook` and `overridehook`, and `ceph` registers it in `internal/hookreg`
(`Harness`) when it is initialized, so there is no copy of the `OperationError`
construction and no core-to-harness import. Today the contract has one member,
`Invalid(err error) error`. `ceph.invalidRequest` builds through the same
`newInvalid`. The result keeps the cause as it is, also when it is nil, exactly as
the former copies did; `classify` is not used because it returns nil for a nil
error. The remaining members (`Classify`, `ClassifyDocker`, the backend entry
points) join the contract with the package move.
Two further copies outside the five packages (`fault` and `internal/multicluster`)
are not part of this row and are unchanged.

### Order of work

0. Done, internal, no public API: the single internal definition of the shared
   pool name (see "SharedPool and Exclusive"). The error contract has no caller
   before the move and is therefore not prepared separately (see "Error handling").
1. Internal, no public API (done): the private backend method `createClient`
   (`Backend.CreateClient` after the move) returns `created`. `sharedBackend`,
   `containerBackend`, `createSharedClient`, the model backend of the tests and
   `Tenant.createClient` changed; the tenant registers a client for deletion on
   the backend's report and no longer reads `ClientConfig.created`. The report is
   true from the point where the MON accepted the entity, including a later
   keyring failure that still returns the partial client, and false before it.
   Docker-free controls pin both the tenant's use of the report and the
   backend's value at each return. Not done and not independent of the open
   items: removing `raw()` from the operations port. Its callers are
   `SharedCluster.Exclusive`, the Observer path `Tenant.container` and the
   test-only `UnitTenantCluster` (decision 7), so it needs the `Lend` route,
   hence the placement of decision 5, the stale-handle audit of decision 11 and
   the decision 7 lookup.
   The error contract needs no preparation: it is added with the move because it
   has no caller before then.
2. Internal: move the service-private helpers into leaf internal packages
   (decision 8). Done for the three service kinds and rbd's resource-name check
   (see "Service harness private reach"); the rgw gateway registry and `invalid` are moved
   (the latter through the first member of the error contract).
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

## Implementation of the move

The change that moves the harness is a relocation. The owner chose one large
change over a series (chat answer to the proposal's decision 1), and decided that
the hardening of the audit's gaps stays a separate change (slice 2 below). This
section records what was built and where it differs from the design text above.

What moved, with `git mv` where the file kept its content:

| From | To |
| --- | --- |
| `ceph/shared.go`, `shared_exclusive.go`, `shared_services.go`, `tenant.go`, `tenant_observe.go` | `ceph/harness/` (same names) |
| `tenantPort` and `tenantLease` of `ceph/service_hooks.go` | `ceph/harness/tenant_port.go` |
| `sharedBackend`, `clusterOps` (interfaces), `sharingMode` | `ceph/harness/core.go`, with adapters over the contract |
| `rbd/tenant.go`, `cephfs/tenant.go`, `rgw/tenant.go` | `rbd/harness/`, `cephfs/harness/`, `rgw/harness/` (`tenant.go`) |
| the model-backed tests (`shared_test.go`, `shared_exclusive_test.go`, `shared_services_test.go`, `shared_service_support_test.go`, `tenant_port_test.go`, `shared_ops_test.go`, the audit lifecycle controls) | `ceph/harness/` |

What stays in `ceph`: the gate and its tables, `containerBackend` (the script,
the invariants read, the client create and delete), `containerOps`, the Observer
and its queries, and the error construction. Package `ceph` registers
`harnessHooks` in `internal/hookreg` (`Harness`); package `ceph/harness`
registers `tenantHooks` (`HarnessTenant`) from its `init`. The latter is an
internal seam that holds no service state: it serves the tenant port lookup, the
opaque `ServiceSupport` constructor and the test-only cluster lookup to
`internal/servicehook` and `internal/testhelper`, each of which asserts only the
method it uses.

Where the build differs from the design above, each for a stated reason:

- `internal/harnesshook` exports the interfaces `Backend` and `Ops` and the
  function types `Prepare` and `Probe`, which `ceph` satisfies structurally,
  instead of a layer that wraps `any` values. `Backend.Ops` returns `any`
  (asserted by `harnesshook.AsOps`) and `SetSharing` takes an `int32` (a `Mode`),
  because `ceph` cannot name a `harnesshook` type. The harness keeps its own
  lowercase `sharedBackend` and `clusterOps` interfaces and adapts the contract
  to them, so the model backends of the unit tests are unchanged.
- `NewTenantObserver` takes the target as `func() (*ceph.Container, error)`, not as
  a backend value: the harness resolves the cluster with `clusterOps.raw`, which
  calls `harnesshook.Lend`, in `Tenant.container`. `TestLendHasOnlyTheAllowedCallers`
  pins that `Exclusive`, `Tenant.container`, the test-only lookup and the adapter are
  the only callers, and `TestLeaseOrchestrationNamesNoContainerBeyondTheAllowList`
  that no other harness code names `ceph.Container`.
- `ServiceSupport` is built by `servicehook.Support`, which asks the
  `HarnessTenant` slot for the opaque value. The service harnesses do not import
  the harness's unexported fields, and `ServiceSupport` has no method.
- The RGW tenant adapter used four unexported members of `rgw.Gateway` (the owner
  cluster, the ownership check, the administration context and the command). They
  are reached through `rgw/internal/gatewayadmin`, an internal leaf that `rgw` fills
  when it is initialized; the inventory of private reach counted package-level names
  and did not see members. It adds no public name and is not service registration.
- Tests that need private `ceph` state stay in `ceph` and tests that need a harness
  `Tenant` moved: the Observer controls use a stand-in lease
  (`ceph/observer_lease_test.go`), and the real-tenant seam is covered in
  `ceph/harness/observe_tenant_test.go`. `createSharedClient` and the service callback
  run keep their tests in `ceph`.
- The documentation page of the shared cluster moved from `docs/ceph.md` to
  `docs/ceph-harness.md`, the service tenants to `docs/rbd-harness.md`,
  `docs/cephfs-harness.md` and `docs/rgw-harness.md`. The examples of
  `ceph/harness/examples_test.go` compile the snippets, and
  `TestDocsNameNoRemovedHarnessName` scans the pages for a removed name.

The move carried the audit's gap 1 and gap 2 over unchanged. The hardening of
"Exclusive stale-handle audit" (slice 2) closes them for the handles that reach
the gate; `docs/ceph-harness.md` states what remains under "Limits".

## Exclusive stale-handle audit

Slice 1 of the implementation plan below found the gaps; slice 2 (this section
as it now stands) hardens them. The findings are kept as the record of the
code before slice 2, and "Hardening of slice 2" states what the code does now.
The Docker-free controls are in `ceph/shared_exclusive_audit_test.go` and
`ceph/harness/shared_exclusive_audit_test.go`.

What the owner decision requires: `SharedCluster.Exclusive` keeps returning the
core `*ceph.Container`, and a retained Container or derived handle must not keep
an interval's authority, including during a later tenant or exclusive interval.

How the lifecycle is built, as the controls show it:

- A cluster's sharing mode moves only forward: shared with tenants when the
  cluster is started, exclusive when an `Exclusive` takes it, retired when that
  interval's release begins, and never lowered. The only writers are
  `shared.go`, `shared_exclusive.go`, the backend method and core setter they
  call, and the unit-test cluster builder
  (`TestExclusiveAuditSharingModeOnlyMovesForward`, with a scanner control that
  reports a demoting release).
- An `Exclusive` takes the cluster that tenants used last (the current
  instance), marks it so no later lease finds it, and its release retires and
  terminates it. A later `Lease` or `Exclusive` starts a fresh cluster. So one
  `*Container` is never the cluster of a tenant interval after an exclusive
  interval, and the same handle cannot meet two exclusive intervals
  (`TestExclusiveAuditInstanceOfAnExclusiveLeaseIsNeverReused`).

Access paths audited:

| Path | How the gate reads the mode | Result |
| --- | --- | --- |
| Methods of `*Container`, including the promoted ones | the cluster's current mode on every call | Before slice 2: admitted for the whole interval and for as long as the cluster kept the exclusive mode (gap 1). Now: refused once the release retires the cluster, but for the methods that only read state |
| Embedded `Container.Container` (the primary MON handle) | not gated: a field access bypasses the method overrides | Reachable from the lent pointer, so it carries the same lifetime as the pointer; slice 2 does not change it (see "What slice 2 does not close"); it is not reachable from a `Tenant` |
| `OSDContainer`, `MonitorContainer`, `ManagerContainer`, `DetachedOSD` and a `ClientConfig` from `Container.CreateClient` | the owner's current mode on every operation | Admitted exactly when the owner is exclusive; refused after the retirement (gap 1 closed) |
| `ClientConfig` from `Tenant.Client`, `Tenant.CreateClient` or a service tenant | before slice 2: the owner's current mode. Now: shared-with-tenants, whatever the owner's mode | Refused in every mode (gap 2 closed); the descriptor's copies from `ReplaceKey` keep the mark |
| `Pool` from `Container.Pool` or `CreatePool` | the owner's current mode | As the `OSDContainer` row |
| `Pool` listed by a tenant `Observer` | tenant admission is fixed to shared-with-tenants and the tenant lease is checked first | Does not gain an interval's authority, and an expired lease is refused before admission |
| `Observer` of a tenant | every query re-resolves the target and re-checks the lease | Does not gain an interval's authority |

Findings:

- Gap 1, retired cluster keeps the exclusive mode (found by slice 1). The
  release of an exclusive interval terminated the cluster but never lowered its
  mode. When the termination failed, the cluster stayed owned by `Close` and
  kept admitting every retained Container or handle until `Close` ended it. In
  the normal case the terminated containers refuse the operations, so the
  protection was the absence of a running cluster, not admission.
- Gap 2, tenant-obtained client descriptors read the live mode (found by slice
  1). `Tenant.Client` and `Tenant.CreateClient` return `*ClientConfig` handles
  whose `ClientConfig.*` operations are refused to tenants, but they read the
  owner's current mode, and the cluster that tenants used becomes the exclusive
  cluster. A descriptor retained past its lease was therefore admitted by the
  gate once an `Exclusive` took that cluster; what ended its use was identity
  state (the tenant release deletes the identity and marks the descriptor
  revoked), not the lease. The tenant's `Pool` and `Observer` do not have this
  gap.
- Not a gap: `Terminate` is refused under the exclusive mode, and the tenant
  cluster is never reused after an exclusive interval.

### Hardening of slice 2

This is an implementation-side proposal made under the owner's delegation of
2026-10-29. It is not owner approval: the owner has said that neither the
relocation nor green native consumers approves a hardening mechanism, so the
mechanism below needs the owner's acceptance.

- Gap 1: a fourth gate mode, retired (`sharedRetired` in the core,
  `harnesshook.SharedRetired` in the internal contract). `releaseExclusive`
  sets it first, before the termination
  (`TestExclusiveAuditReleaseRetiresBeforeTerminating`, with a control for a
  release that terminates first or never retires). A retired cluster admits only
  the operations that only read state (`sharedOpen`) and refuses the others
  with `ErrSharedCluster`, naming the operation
  (`TestExclusiveAuditRetiredClusterAdmitsOnlyOpenOperations` covers every
  classified method, handle, fault and override operation). A termination that
  fails now leaves a running cluster in the retired mode until `Close`
  ends it
  (`TestExclusiveAuditRetiredClusterRefusesAfterFailedTermination`).
- Gap 2: a `ClientConfig` created through `createSharedClient`, the only
  creation that a tenant's release deletes, carries a `tenant` mark that its
  `ReplaceKey` copies keep. `ClientConfig.cluster` admits a marked descriptor as
  for a cluster shared with tenants whatever the owner's mode is, as the tenant's
  leased `Pool` already is. Every `ClientConfig.*` operation has the tenant
  policy, so the marked descriptor is refused in every mode. No lease check was
  added: a descriptor that is refused in every mode needs none
  (`TestExclusiveAuditHandleAdmissionFollowsTheLiveMode`,
  `TestExclusiveAuditOnlyTheTenantCreationMarksADescriptor`).
- `TestExclusiveAuditModeValuesMatchTheHarnessContract` ties the harness
  contract's mode numbers to the gate's.

Behavior that changes: a `*Container`, handle or fault/override operation that
an exclusive interval lent is refused by the gate after the interval's release,
except the read-only operations; a client descriptor that a tenant obtained is
refused by the gate during an exclusive interval of its cluster.

### What slice 2 does not close

The embedded `Container.Container` field is a plain field of the lent
`*Container`. A caller that retained it calls the testcontainers container
directly, past every shared gate, so a retired mode cannot revoke it. After a
normal release the terminated container refuses the call; after a termination
that failed, the running container still answers until `Close` ends it. Closing
that needs a different design (a facade or a lease-bound handle instead of the
raw pointer), which the owner's `Exclusive` decision (the core `*Container`
stays the handle) excluded. The read-only operations also still pass the gate
on a retired cluster and rely on the terminated cluster to fail. Handles that
are not reachable through any gate (a value copied out of one, such as a
retained keyring or config file) are outside this mechanism.

### Native observation

`TestSharedClusterRetainedHandles` (`internal/integration`, batch
`shared_cluster_retained_handles`, header `//ci:`) retains a tenant's client
descriptors and pool, takes the same cluster with an `Exclusive`, retains the
lent Container and handles derived from it, and calls them during the interval
and after the release, logging each outcome. The embedded container's outcome is
recorded, with the only requirement that a terminated cluster does not run the
command. Written without Docker: it was compiled under every tag set and was not
executed by the author. Native acceptance needs a run of the exact head; one
run on one runner is an observation, not stability.

Not established: the audit does not show that every operation behind the gate
also fails safely on a terminated cluster, and the native scenario has not run.

## Implementation plan (proposal)

This section is an implementation-side proposal made under the owner's
delegation of 2026-10-29 on issue 155 (decisions 5 and 10, the generic registry).
It is not owner approval of the slice order, the exported names or the behavior
changes of slice 2. Call-site counts are those of the inventory at `83de22a` before the move
(guarded by the inventory test) and a fresh search for what that table does not
count.

Constraints that shape the order:

- `ceph/harness` imports `ceph`, so nothing that `SharedCluster` or `Tenant` use
  can move first: the moved set is one strongly connected group (`SharedCluster`,
  `Tenant`, `SharedStats`, the options, `TenantAccess`). A partial move would need
  a public alias or forwarding declaration in `ceph`, which the 0.x policy forbids.
  The public move is therefore one change; what can be sliced is the work before it.
- Choice (3) of decision 10 lands `ServiceSupport`, `WithServices` and each
  `Support()` together with the move, and deletes the registry and the three
  `init` registrations in the same change. Landing `Support()` in `rbd`, `cephfs`
  and `rgw` first would add public names that the move renames.
- The note rejects an uncalled internal port ("`raw()` removal coupled to real
  callers"). `Core.Start`, `NewTenantObserver`, `Lend`, `Classify` and
  `ClassifyDocker` have no caller before the move, so they join with it.

| Slice | Content | Behavior | Files | Size | Guards | Build stays green because |
| --- | --- | --- | --- | --- | --- | --- |
| 1 (this change) | Exclusive stale-handle audit and this plan | Preserved; no production change | `ceph/shared_exclusive_audit_test.go`, this note | about 230 test lines, no generated catalogue | 5 audit tests (one is the scanner's negative control; the others were also shown to fail against a deliberately broken release or pool admission); `TestSharedHarnessInventoryMatchesTheNote` unchanged | Tests and a note only |
| 2 (implementation-side proposal, delivered for the owner's acceptance; see "Hardening of slice 2") | Close gap 1 and gap 2: a retired mode that refuses every handle operation but the open ones, set at the start of `releaseExclusive` before the termination; tenant-obtained `ClientConfig` descriptors admitted as shared-with-tenants (no lease check, they are refused in every mode) | Hardening: a retained handle is refused after the interval and a tenant descriptor is refused in an exclusive interval. Needs the owner's yes | `ceph/shared_gate.go`, `ceph/shared_exclusive.go`, `ceph/client_handle.go`, `ceph/tenant.go`, `ceph/shared_backend.go`; `docs/changes` fragment | about 250 lines | The two characterization controls are inverted; `TestSharedGateClassifiesEveryMethod` extended to the new mode; a native control retains a handle across a real interval | The change is inside `ceph`, public names unchanged |
| 3 (done by the change that adds "Implementation of the move") | The move: `ceph/harness` with `SharedCluster`, `Tenant`, the options, `SharedStats`, `RunTests`, `SharedPool`, `TenantAccess`, `ObserveTenant`, `ServiceSupport`, `WithServices`; `internal/harnesshook` grows `Core`, `Backend`, `Ops`, `Mode` and the two `hookreg` slots; `rbd/harness`, `cephfs/harness` and `rgw/harness` with `Lease`, the namespace/group/user entry points and `Support()`; `internal/servicehook` retargeted at `*harness.Tenant`; the registry, the three `init` registrations and `RegisterSharedService` deleted | Preserved, except the documented link effect: a binary that relied on importing a service now lists `WithServices(...)` | 28 declarations; `ceph/shared*.go`, `tenant*.go` and their model tests (about 2,600 prod and 2,600 test lines, relocated); rbd 4, cephfs 3, rgw 4 and `internal/servicehook` 2 references; `internal/integration` 36 references in 6 files, 2 `NewShared`, 1 `RunTests`, 2 `Exclusive`, 10 `ceph.SharedPool`, 4 `TestTenantCluster` calls in 2 files; `docs/ceph.md`, `docs/rbd.md`, `docs/cephfs.md`, `docs/rgw.md`; catalogues for `ceph`, `rbd`, `cephfs`, `rgw` and the new `ceph/harness` page, then a separate approve commit; a `moves` record; a package-split shard; the release fragment and the v0.4 upgrade mapping | Mostly relocation; well above 600 changed lines by diff, and small by content once renames are followed | The guards of "Guards the move adds"; the inventory test retargeted; the error-equivalence checks; every moved model test unchanged except its package name | Everything that names `ceph.Tenant` moves in the same change, because `ceph` cannot keep a forwarder. Inside the change the order of commits is: `internal/harnesshook` contract and `ceph` registration (additive), then `ceph/harness`, `servicehook` and the three service harnesses together (they cannot compile apart), then callers and docs |
| 4 | Extraction-specific native acceptance on the Linux runner: the shared-cluster and service-package jobs retargeted at the new packages, plus the stale-handle control of slice 2 | Evidence only | `internal/integration` scenarios with their `//ci:` header | small | Real CI of the exact head; one run on one runner is not stability | Not a code change of its own |

Why slice 1 is first: it is the only item that makes progress without a public
decision (the audit is owed before the extraction is accepted), it needs no
Docker, and its findings decide whether slice 2 is needed before the move.
Slice 3 cannot be reduced further without a forwarding layer, so its review
cost is stated here instead of hidden.

Decisions this proposal could not take, and how they were answered:

1. Whether the move lands as one pull request, as a series on an integration
   branch, or with a temporary `Support()` in the service packages. Answered by the
   owner in chat: one large change, no series, no temporary `Support()`.
2. Whether slice 2 is wanted before the move and whether a retired mode is the
   mechanism the owner accepts for gap 1. Answered: the move is strictly a
   relocation; slice 2 stays a separate follow-up that needs the owner's yes. Gap 1
   and gap 2 were recorded as carried-over behavior. Slice 2 is now implemented as
   a proposal that awaits that yes.
3. Whether tenant `ClientConfig` descriptors should be lease-bound like `Pool`
   (gap 2) or the identity-state protection is accepted. Slice 2 proposes a fixed
   tenant admission instead of a lease check; the owner's acceptance of that
   choice is open.

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
| `SharedPool` | The public constant moves to `ceph/harness`; no `ceph.SharedPool` alias; core checks use an internal shared definition or an explicit input, never a second literal. The internal definition and its guard are in place; the constant move itself is not | [13:01Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469654249) |
| Observer construction | `Observer` and its queries stay in `ceph`; `ObserveTenant` moves to `ceph/harness`; core names no `Tenant` and imports no harness; the original tenant is revalidated on every query; no public executor, resolver or authorization callback; direct caller migration | [13:03Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469688158) |
| Error helpers | One narrow internal contract; no public helper, no copy of core classification; categories, causes, `errors.Is`/`errors.As` and diagnostics preserved; classification verified on the migrated paths | [13:04Z](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6469722063) |
| Decision 5, internal contract placement | Settled by the owner delegation of 2026-10-29: top-level `internal/harnesshook` beside `faulthook` and `overridehook`, ports in `any`/`int32` terms that `ceph` satisfies structurally; the `invalid` copies move onto its one construction | [2026-10-29](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6484975606) |
| rgw gateway registry | Settled by the same delegation: a generic `Registry[T any]` in a leaf internal package (`internal/namedregistry`); the value type stays in `rgw`; no public name | [2026-10-29](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/155#issuecomment-6484975606) |

The earlier layout decision (2026-10-27: `ceph/harness`, `rbd/harness`,
`cephfs/harness`, `rgw/harness`, direct v0.x migration) is unchanged. Decisions 1
to 4 and 6 of the first draft of this list are therefore closed, and the first
draft's `init`-time recommendation for decision 1 is withdrawn.

### Status of the numbered decisions

Decisions 7 to 10 are taken as recommended under the owner's delegation of
2026-10-29 and implemented by the move; decision 11 is open for its enforcement
part.

| # | Decision | Recommendation (implemented unless noted) | Alternative and its cost |
| --- | --- | --- | --- |
| 7 (done) | Test-only tenant-cluster lookup (`testhelper.TestTenantCluster`, today `unitAccess.UnitTenantCluster`) | Keep it test-only: a second method of the `HarnessTenant` value, asserted by `testhelper`, four known native call sites, guarded | Replacing the four native controls with Observer-based checks weakens what they prove about the gate |
| 8 (done) | Service-private helpers used by the adapters | Leaf internal packages, moved before the harness; the moves are done (service kinds, the RBD name check, `invalid`, the rgw gateway registry) | Exporting them adds public names to rbd, cephfs and rgw |
| 9 (done) | Exported names and signatures beyond the settled ones | The old-to-new map: same names in the new packages, no renames, no new options other than the service-support input; any rename is a later change | Renaming during the move mixes a mechanical migration with an API review |
| 10 (done) | Shape of the service-support input and its validation (the instance-owned set, its validation and `RequireService` are implemented behind unexported names; see "Instance-owned service set: first slice" for the exact exported spelling proposed) | An opaque `ceph/harness` value built by each service harness through an internal route and passed to `NewShared` as an option, validated at construction (duplicate kind, foreign value) and checked by a tenant-port method before a service `Lease` creates anything | A positional `NewShared` argument changes the signature for every caller; a plain callback struct would show the cluster-receiving callbacks in public API |
| 11 (audit done, enforcement proposed, not accepted) | Stale-handle audit result for `Exclusive` | Done as slice 1 ("Exclusive stale-handle audit": Docker-free controls, two gaps recorded); the enforcement of the gaps is slice 2, implemented as an unaccepted proposal, and its native scenario is unexecuted until a run of the exact head. Do the audit as the first step of the move, as a Docker-free test over the access paths plus a native control, and record every gap | Deferring it until after the move would accept a handle whose enforcement is unproven |

Overlap: #285 and #286 (renames and the test-only lookup move) are merged and
composed into this note.

## Not covered

The move is implemented and verified without Docker: unit and model tests, the
compile of every tag set, the API catalogues and the documentation guards. No
native Ceph or Docker test was run by this change. Native evidence for the
extraction (the shared-cluster and service-package jobs on the Linux runner, and a
control that retains a stale handle across a real interval) is the CI of the exact
head of the pull request and slice 4 of the plan; one run on one runner is not a
claim of stability.

Not changed by the move: the two enforcement gaps of "Exclusive stale-handle
audit". Slice 2 closes them for the handles that reach the gate, as a proposal
that needs the owner's yes, and leaves the raw embedded container open. The sections from
"Method and limits" to "Refresh at `1a158b9`" and the design sections that describe
the state before the move are history and keep their older counts and names; the
"Cross-package registration design" text names the shape that was proposed, and
"Implementation of the move" lists where the build differs.
