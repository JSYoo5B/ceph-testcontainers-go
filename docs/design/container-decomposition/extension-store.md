# Extension store

> Current boundary: the shared-cluster/tenant harness and its core-only policy,
> lease and hook plumbing have been removed. Harness references and source
> locations below are historical design evidence, not retained APIs or future
> extraction requirements. See [independent fixtures](../../ceph.md#usage-example).

Moved without rewriting from the section "Thirteenth internal slice: extension store" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `eccaad1` (main after
#277); path:line citations are at the branch head. Two independent changes in
one slice. First, `Container.extensions` and `Container.extensionsMu` move into
an `extensionStore` (`ceph/extension_store.go:33`, field `ceph/ceph.go:75`) that
owns the map and the lock that guards it. Second, the last raw pointer that
`serviceHooks.TestState` published to a non-lock field, `NetworkRemoved`, is
replaced by two hooks. Exported API and behavior are unchanged.

## Fresh audit of what is left of the membership and control group

Counts are textual uses by field name in non-test files of package `ceph`, so
they overstate real call sites.

| Field(s) | Lock domain | Verdict |
| --- | --- | --- |
| `extensions` with `extensionsMu` | its own leaf lock | Picked. Every access is in two functions (`serviceHooks.Extension` and the generic `extensionsWith`, the latter with three callers); no function takes another lock inside the lock except the `create` callback, and the two callbacks in the module only allocate a registry |
| `sharing` | atomic | Not picked as a second component. Four uses in three non-test files (`ceph/shared_gate.go:252,273`, `ceph/shared_cluster_port.go:77`, `ceph/service_hooks.go:422`), all `Load` or `Store` of one atomic that `sharedGate` already owns by name. A component would wrap one atomic and add no lock domain or guard |
| `settings` | none after `Run` | Not picked. Immutable options read in 39 files (149 lines); a component would add an accessor per option and hide nothing |
| `closed`, `monitorTerminated` | `mu` | Not picked, for the reasons in the eleventh slice: 61 and 11 uses, `closed` is read by the shared-gate admission |
| `manager`, `monitors`, `managers`, `services` | `mu` | Not picked. The maps hold daemon handles that several functions rewrite in place and `sharedDaemons` and `terminate` iterate (17, 26, 11 and 29 uses) |
| `keyTypeOption`, `firstMonitorImageID`, `owner` | none, written once in `Run` | Not picked. No lock domain; `owner` is read in 17 files |
| `TestState` pointers `Mutex`, `ControlMutex`, `ConfigMutex` | lock identities | Not picked. The lock queueing tests of the service packages need the identity, not a value |
| `TestState.NetworkRemoved` | `mu` (a field of `networkState`) | Picked. It needed no lock identity: its two users in `fault` set it once on a synthetic bridge and read it after `Terminate` |

## Part 1: what moved

| Before | After |
| --- | --- |
| `Container.extensionsMu`, `extensions` | `Container.extensions` (an `extensionStore`) with `mu`, `states` |
| `serviceHooks.Extension`: lock, look up, create on a miss, store, unlock | `extensions.getOrCreate(kind, create)` (`ceph/service_hooks.go:285`) |
| `extensionsWith[T]`: lock, sort the kinds, type-assert each state, unlock | `extensions.inKindOrder()` returns the states in kind order under the lock; the generic function asserts `T` after the lock is released (`ceph/service_bootstrap.go:81`) |

Methods cannot be generic, so `extensionsWith[T]` stays a function; it is the
only caller of `inKindOrder`. One difference in timing, no difference in result:
the type assertion used to run inside the lock and now runs on the snapshot
after it. The states are handed out unchanged, as before; the service packages
guard their own contents with the cluster mutex, as their docs say.

## Part 2: `NetworkRemoved` replaced by hooks

`TestState` and `servicehook.TestStateView` no longer have `NetworkRemoved`.
Their field set is now `ConfigMutex`, `ControlMutex`, `Mutex` and `PortLeases`:
three lock identities and a count. `TestSetNetworkRemoved(c, removed)` and
`TestNetworkRemoved(c)` (`ceph/service_hooks.go:490,496`, wrappers in
`internal/servicehook/testaccess.go`) replace the pointer. Both take no lock; the
caller holds `Mutex` or the cluster value is not shared, as for the other
`TestSet*` hooks. The three callers in `fault` were migrated
(`fault/network_test_helpers_test.go`, `fault/network_test.go`). The
removal-flag writer guard (`TestNetworkStateRemovalFlagsHaveOneWriter`) now
lists the hook in place of `TestState`, and the recorded-fact guard
(`TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup`) lists it as the
only new writer through `nets`; neither guard was loosened beyond that row.

## Why no new mutex and no lock-order change

`extensionsMu` is the lock that moved; its type (`sync.Mutex`), its holders and
its position are unchanged. It is a leaf: the only code that runs under it
besides map operations is the `create` callback of `getOrCreate`, as in the
former `serviceHooks.Extension`, and the two callbacks in the module
(`cephfs.filesystems`, `cephfs/entry.go:108`; `rgw.gatewaysFrom`,
`rgw/entry.go:95`) allocate a registry and call nothing. Callers may or may not hold the
cluster mutex, as the doc of `serviceHooks.Extension` says. `extensionsWith` in
`monitorConfigTargets` (`ceph/topology.go:546`, reached from
`refreshMonitorConfigAfterRemoval` at `:441`) and in `diagnosticsSnapshot`
(`ceph/diagnostics.go:257`, after `diagnosticLock(ctx, c.mu.TryLock)` at `:218`)
run with `Container.mu` held, and the tenant-port path
(`containerOps.extension`, `ceph/shared_ops.go:47`) is the caller that does not
promise it. Where both are held
the order is `Container.mu`, then the store lock, and nothing takes
`Container.mu` while holding the store lock. The store publishes no lock
identity (`TestState` never exposed `extensionsMu`), so the type has no
accessor for its mutex.

## Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `extensionStore` as a component and pins
`extensionStore.mu` where it pinned `Container.extensionsMu`: still eight
mutexes (`Container.mu`, `extensionStore.mu`, `controlPlaneState.mu`,
`bootstrapRecord.mu`, `nativeState.mu`, `featureClientState.mu`,
`featureClientState.publishMu`, `debugLevelRegistry.mu`). Adding a second mutex
to the component fails it (negative control). `Container` now declares one
mutex.

## Guards, in `ceph/extension_store_test.go`

- `TestContainerDeclaresNoExtensionsMapOrLock`: `Container` no longer declares
  `extensionsMu`, and the store has exactly the fields `mu` and `states`.
- `TestExtensionStoreFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.extensions` is only the receiver of a store method; its map is
  never reached; no production `Container` literal sets it; the guard is not
  vacuous.
- `TestExtensionStoreMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned; no method names `Container`, `closed`,
  `controlPlane`, `settings` or `bootstrap`, or another mutex.
- `TestExtensionStoreMapWritersAreAudited`: the only writer of the map is
  `getOrCreate`, and it takes the lock.
- `TestExtensionStoreCallersAreAudited`: the caller table (`Extension` calls
  `getOrCreate`, `extensionsWith` calls `inKindOrder`); a new or stale row
  fails.
- The existing guards were updated, not weakened: the `TestState` field set and
  the `TestSet*` hook table (`ceph/membership_control_test.go`), the mutex owner
  table and component list (`ceph/native_state_test.go`), and the two guards
  named above.
- Behavior: an empty store is inert; each kind is created once; a running
  `create` holds the lock against another kind; the snapshot is in kind order and
  does not change when a kind is added later; `extensionsWith` filters by
  interface in kind order; concurrent use under `-race`; the hook reaches the
  store; the removal-flag hooks reach `nets.publicRemoved` and only it.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the store, `extensionsMu` declared
again on `Container`, `getOrCreate` without the lock, a direct read of the map in
`extensionsWith`, an extra call in `inKindOrder`, a method that names
`Container`, an unlisted caller of the store, a `Container` literal that sets
`extensions`, the `NetworkRemoved` pointer put back in the view, and a hook that
makes a call or writes a second flag.

## Limits of the guards

The guards are syntactic and follow names inside the package; the receiver
identifiers `c`, `cluster` and `owner` stand for a cluster. The cross-package
hooks are scanned only for non-test callers by the existing caller table; their
lock discipline is documentation. The `create` callback runs under the lock by
contract, which no guard can check for callbacks in other packages; the
behavior test pins only that the store holds the lock while it runs. No Docker
or native Ceph behavior was run for this slice.

## Not covered by this slice

The rest of membership and control stays on `Container`: `mu`, `manager`,
`monitors`, `managers`, `services`, `closed`, `monitorTerminated`, `sharing`,
`settings`, `firstMonitorImageID`, `owner` and `keyTypeOption`. Also not
covered: the `Mutex`, `ControlMutex` and `ConfigMutex` pointers in `TestState`,
any change to which lock guards what, any new lock, the three test seams and any
exported API. `ceph.Container` has 229 method declarations before and after
this change (information only; it is not a target).
