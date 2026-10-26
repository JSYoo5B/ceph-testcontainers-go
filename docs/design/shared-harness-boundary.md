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

Files (non-test lines): `shared.go` 668, `shared_backend.go` 603,
`shared_exclusive.go` 290, `shared_services.go` 112, `tenant.go` 307, and the
gate `shared_gate.go` 319 (which stays in core, see below). Four test files
(`shared_test.go`, `shared_exclusive_test.go`, `shared_services_test.go`,
`shared_gate_test.go`) hold 1,642 lines.

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
`docs/design/api-naming-inventory.json` entries for the 35 symbols.

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
The API-naming inventory and `package-split.json` are regenerated with
`-update-api-naming`, deliberately and in the move's own change.

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

Status of the safe extraction order at `e22b728`:

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

No code was extracted, no signature was added, no import changed and no test
was run. The counts of the first half are from the older snapshot and are
superseded by the re-measurement that the move requires after #146.
