# Control-plane state

Moved without rewriting from the section "Eleventh internal slice: control-plane state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `75a313b` (main after
#272); path:line citations are at the branch head unless they name another
revision. This is the first component of the membership and control group.
`Container.controlPlane` is now a `controlPlaneState`
(`ceph/control_plane_state.go:34`) that owns the control container handle and
the read-write lock that guards it, the former `Container.controlMu`. Exported
API and behavior are unchanged; `controlMu` and the raw handle field are gone.

## Fresh audit of the membership and control group by lock domain

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

## What moved

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

## Why no new mutex and no lock-order change

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

## Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `controlPlaneState` as a component and pins
`controlPlaneState.mu` where it pinned `Container.controlMu`: still eight
mutexes (`Container.mu`, `configMu`, `extensionsMu`, `controlPlaneState.mu`,
`nativeState.mu`, `featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`). Adding a second mutex to the component fails it
(negative control).

## Guards, in `ceph/control_plane_state_test.go`

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

## Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The caller table does
not prove the position of the lock relative to `mu` for the `free` callers; that
part is the argument above. The other packages reach the lock only through the
`ControlMutex` pointer and the `TestSetControlPlane` hook, which are not scanned
for lock discipline. No Docker or native Ceph behavior was run for this slice.

## Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `configMu`,
`extensionsMu`, `manager`, `monitors`, `managers`, `services`, `closed`,
`monitorTerminated`, `config`, `keyring`, `cephVersion`, `extensions`, `sharing`,
`settings`, `firstMonitorImageID`, `owner` and `keyTypeOption`. Also not covered:
the `ControlMutex` pointer in `TestState`, any change to which lock guards what,
any new lock, the three test seams and any exported API.
