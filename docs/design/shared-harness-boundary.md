# Shared-cluster and tenant harness: inventory and boundary

This note is the measured inventory and the proposed package boundary that
#155 asks for before any public move. It changes no code, no public signature
and no dependency. Moving the exported types stays blocked on the internal
state, lock and admission decomposition of #146 (see
[container-decomposition.md](container-decomposition.md)); this note only
fixes what that move has to carry and in which order it can be done safely.
Every package name and signature below is a proposal for review, not a
decision.

## Method and limits

- Source snapshot: `main` at `1d4c937` (merge of #166).
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

## Service adapter coupling

`rbd.Lease`/`TenantNamespace`, `cephfs.Lease`/`TenantGroup` and
`rgw.Lease`/`TenantUser` take `*ceph.Tenant` and use `internal/servicehook`.
Calls per adapter (counted in `tenant.go` of each package):

| Hook | rbd | cephfs | rgw | What it provides |
| --- | --- | --- | --- | --- |
| `TenantCluster` | 1 | 2 | 1 | Gated `*Container` of a live tenant, or nil |
| `AttachTenantScope` | 1 | 1 | 1 | Scope registration released before the tenant's clients are deleted |
| `TenantClient` | 1 | 1 | 0 | Scoped Cephx client with service caps; deleted by the release |
| `RegisterSharedService` | 1 | 1 | 1 | `prepare`/`probe` hooks run by the orchestrator on a cluster that is not shared yet |
| `ClientCommand` | 4 | 1 | 0 | CLI command in the control or client container |
| `StartupTimeout` | 1 | 2 | 0 | Bound for one fixture operation |
| `Lock` / `Unlock` | 0 | 0 | 5 / 5 | Topology mutex around read-modify-write sequences |

The adapters therefore need five distinct capabilities from a tenant, not one
commander: (1) liveness (is the tenant released or its cluster terminated),
(2) a scoped client factory, (3) scope registration with owner-scoped
release, (4) bounded command execution on the cluster, and (5) for RGW the
topology lock. `rgw/tenant.go` also calls `Pools` and `CreatePool` on the
gated `*Container` it obtains from `TenantCluster`. The `*Container` returned
by `TenantCluster` is the shared one, so today the gate is the only thing that
stops an adapter from using broad admin paths. This is the coupling the
issue's go-ceph comparison warns about: handing the adapter a narrower Go type
does not by itself narrow what the command can do.

## Proposed boundary

Candidate (for review): a new top-level package `cephtest`
(`github.com/JSYoo5B/ceph-testcontainers-go/cephtest`), sibling of `fault`,
`rbd`, `cephfs`, `rgw` and `capture`. Alternatives considered: a subpackage
`ceph/shared` (keeps the import path under `ceph`, but a package under `ceph`
cannot be imported by `ceph` and invites the same ambiguity about which
direction may import which) and keeping the harness in `ceph` with only an
internal split (does not meet the issue's acceptance that core no longer
exposes the moved declarations).

### Import graph

Before (all arrows point to the imported package):

```
rbd, cephfs, rgw --> ceph (SharedCluster, Tenant, Container)
rbd, cephfs, rgw --> internal/servicehook --> ceph (hookreg.Value)
ceph --> internal/hookreg      (registers serviceHooks in init)
internal/integration --> ceph, rbd, cephfs, rgw
```

After:

```
cephtest --> ceph                     (public types: Container, ClientConfig, ...)
cephtest --> internal/hookreg         (reads the core backend port)
rbd, cephfs, rgw --> cephtest         (Tenant, via a narrow tenant port, see below)
rbd, cephfs, rgw --> ceph
ceph --> internal/hookreg             (registers serviceHooks and the backend port)
ceph does not import cephtest, rbd, cephfs or rgw
internal/integration --> cephtest, ceph, rbd, cephfs, rgw
```

Core never imports the harness. The service adapters keep importing `ceph`
for cluster types and add `cephtest` for the tenant. Adapters stay in the
service packages (their `Lease` functions are service API and their
`prepare`/`probe` hooks run against `*ceph.Container`); moving them into a
fourth set of packages would not remove a dependency and would double the
package count. The adapter-to-harness import is one-way and acyclic because
`cephtest` never imports a service package: services register with it by the
existing `RegisterSharedService` hook, which moves from `servicehook` to a
harness-owned registry (the `sharedServices` registry in `shared_services.go`
moves with the harness and is fed through `internal/servicehook`).

### What stays in core

- `sharedGate`, `admitShared`, `admitHandle`, `admitFault`, the policy tables
  (`sharedMethodPolicies` 93 entries, `sharedHandlePolicies` 25,
  `sharedFaultPolicies` 4), `sharingMode`, the `Container.sharing` field and
  `ErrSharedCluster`. The gate is a property of `Container` operations, and
  its reflection tests enumerate `*Container` and the handle receivers.
  Moving it would force the harness to depend on every method name of core, or
  duplicate the tables, which the issue forbids.
- The harness drives the gate through one port: `setSharing(mode)` with three
  modes. That is the whole admission contract between orchestration and core.

### What moves

`SharedCluster`, its options, stats, `RunTests`, the lease/exclusive/waiter
state machine, poisoning and recycling, `sharedInvariants` and its diff
functions, the service-probe registry, `Tenant` and its scope list.

### Ports the harness needs from core

Replace the 13 fields and 11 unexported methods by a port registered through
`internal/hookreg` (a second slot, next to `Value` and `Fault`), typed in an
internal package. The port is the current `sharedBackend` interface (12
methods) plus a factory `Start(ctx, image, opts) (Backend, error)` that
returns it; its implementation, `containerBackend`, stays in `ceph` and keeps
using private state. This is a port on behavior, not an accessor on state: it
exposes no `mu`, `controlPlane` or other field. `cluster() *Container` stays
because `Exclusive` returns the cluster by contract.

### Tenant port for the adapters

Replace the concrete `*ceph.Tenant` coupling of `internal/servicehook` by an
interface owned by the harness, with the five capabilities from the adapter
table:

- live check that returns an error instead of a raw cluster;
- scoped client creation (`TenantClient`) and scope attachment
  (`AttachTenantScope`), unchanged in behavior;
- a bounded command method that runs in the tenant's cluster and does not
  return it;
- a topology-locked section `WithTopology(ctx, func(...) error)` for RGW, in
  place of `Lock`/`Unlock` on a `*Container`.

`TenantCluster` is the member to retire first: it is the only hook that hands
an adapter the shared `*Container`. RGW's `Pools`/`CreatePool` calls on it
need either pool operations on the port or an explicit statement that they run
in `prepare`, before the cluster is shared (to be verified; not run here).

### Tenant read-only views

The eight forwarders in `tenant.go` should not be migrated twice. #146 plans an
observation object built from a cluster or a tenant (`Observe(c)`,
`ObserveTenant(t)`); the harness should adopt that object for the views that
move into it, and `PoolStatus`, `PoolUsage`, `Pools` follow #144's pool
placement decision. Until #146's observation step lands, the `Tenant`
forwarders stay as they are.

## Preserved behavior to carry (checklist for the move)

Each item is a requirement from the issue, listed with where it lives now:

| Property | Where it lives now |
| --- | --- |
| Live-lease and generation admission, release invalidation | `Tenant.check`, `released` (guarded by `SharedCluster.mu`), `sharedInstance.terminated` |
| Owner-scoped cleanup in reverse order before client deletion, remainder poisons the cluster | `releaseScopes`, `Tenant.takeScopes`, `SharedCluster.release` |
| Exclusive fairness, self-wait refusal, waiter queue | `shared_exclusive.go` (`acquire`, `admissibleLocked`, `selfWaitLocked`) |
| Poison, recycle bound, non-counting exclusive restarts | `SharedCluster.poison`, `defaultMaxRecycles` |
| Namespace and Cephx isolation, baseline and release invariants | `sharedInvariants.admissionDiff`/`releaseDiff`, `containerBackend.invariants` |
| Refusal of raw Docker/admin/topology access on a shared cluster | gate and tables (stay in core) |
| Lock and gate order, uncertain-outcome ownership | `lockTopology` use in `containerBackend`; unchanged by a pure move |

The unit tests that control these (the model-backed tests in
`shared_test.go`, `shared_exclusive_test.go`, `shared_services_test.go`) use
the `sharedBackend` model and move with the harness. `shared_gate_test.go`
tests the core gate and stays.

## Safe extraction order

Steps 1 and 2 can be done before #146 finishes and do not touch the
`Container` state, facts or option files that #146 changes. Steps 3 to 6 wait.

1. **Pure logic split, in place.** Move `sharedInvariants`, `admissionDiff`,
   `releaseDiff`, `mapDiff`, `resourceDiff`, `releaseScopes`' pure parts and
   `sortedKeys` (about 180 lines, no `Container` reference in the measured
   range) to a package under `internal/`, with their existing Docker-free
   tests. This removes a third of `shared_backend.go` from the later move and
   is verifiable without a cluster. It edits `shared_backend.go` and
   `shared_services.go` only; check with #146 that neither file is in its
   write set before starting.
2. **Tenant port inside `ceph`.** Introduce the tenant interface, change
   `internal/servicehook` and the three adapters to use it, and make
   `TenantCluster` unnecessary. `*ceph.Tenant` still implements it. This
   migrates the three adapters once, before the package changes, and keeps
   the diff to hooks and adapter files plus `service_hooks.go` and `tenant.go`.
   The unit tests of each adapter that use `servicehook.TestCluster` keep
   working through the hook.
3. **After #146's observation step:** drop the eight view forwarders from
   `Tenant` in favor of the observation object.
4. **Backend port.** Add the hookreg slot, the typed internal package and the
   factory; make `SharedCluster` use only the port. Still in `ceph`.
5. **Move.** Create `cephtest`, move the 35 exported symbols and the harness
   state, update the 49 external Go references and the importers, with no
   aliases or forwarding, as #146's v0.x policy requires. Update package docs,
   `TestMain` examples, `docs/*.md`, `CHANGELOG.md`, the v0.4 migration guide
   and regenerate the API inventory.
6. **Verification.** `make check`, tagged compilation (reported separately from
   native execution) and the existing native shared-RADOS and service-tenant
   cases on the Linux runner, with escaped/expired-handle and partial-failure
   controls unchanged.

## Open questions

- Package name and path (`cephtest` versus alternatives) and whether
  `SharedPool` should stay a core constant because the baseline checks depend
  on it.
- Whether the service registry (`RegisterSharedService`) is one more hook slot
  or is passed explicitly at `NewShared` (explicit registration would remove
  an `init`-time global, but changes how `rbd`/`cephfs`/`rgw` opt in).
- Whether RGW's pool operations on the shared cluster run only before sharing
  starts (not verified).
- Whether `Exclusive` should keep returning `*ceph.Container`, which is a
  deliberate raw handle, from a package that is not `ceph`.

## Not covered

No code was extracted, no signature was added, no import changed and no test
was run. The numbers are from one snapshot and are superseded by the
re-measurement the issue requires after #146. The order above is a proposal
and has not been agreed with the owner of #146's write set.
