# Auth records

Moved without rewriting from the section "Fourth internal slice: auth records" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

Inspected at `7266eb7` (main after #247); path:line citations in this section
are at the revision of the change that adds `ceph/auth_state.go`. The fourth
component moved out of `Container` is the set of auth records, held as
`Container.authRecords` of the unexported type `authState`
(`ceph/auth_state.go`): the fences (`fences`, formerly `authFences`), the
client keys the module confirmed (`keys`, formerly `clientKeys`), every client
key kept for redaction (`secrets`, formerly `clientSecrets`), the key files
that earlier calls could not remove (`strays`, formerly `strayAuthInputs`) and
the temporary client revocations (`revocations`, formerly `clientRevocations`).
The fences serve clients and daemons alike, so the daemon key rotation uses the
same type and moved with it. The old field names are gone, with no alias.

Unlike the earlier slices, this component has no lock of its own and gets none:
`Container.mu` guards every part, as before. The previous slices moved a lock
together with its fields. Here the lock is the topology lock that guards much
more, so it stays on `Container` and the component documents "mu held" on every
method. What the split adds is a structural proof of that contract, which was
only a doc comment before ("The caller holds c.mu").

## Audit of the callers

Every function that reads or writes the records, and the lock it holds, at the
revision of this change. "Takes mu" means the function takes `Container.mu`
itself before the first use and does not release it before the last one.

| Function | Records | Lock |
| --- | --- | --- |
| `createClient` (`ceph/auth.go:146`) | `settleClient`, revocation read, secret, key | takes mu (`lockTopology`, `defer c.mu.Unlock()`) |
| `withClientIdentity` closure (`ceph/auth.go:245`) | `checkCurrentClient` | takes mu (`c.mu.Lock()`) |
| `deleteClient` (`ceph/auth.go:276`) | `settleClient`, key and revocation reads and writes, `changeClient` | takes mu |
| `replaceClientKey` (`ceph/client_key.go:190`) | `settleClient`, secrets, key, fence read | takes mu |
| `clientCapabilities` (`ceph/auth_policy.go:10`), `updateClientCaps` (`:26`) | `ownedClientRecord` and `changeClient` | take mu |
| `revokeClient` (`ceph/client_revocation.go:46`), `ClientRevocationHandle.Restore` (`:81`) | revocation, fence, `settleClient` | take mu |
| `containerBackend.createClient` (`ceph/shared_backend.go:384`), `containerBackend.deleteClient` (`:431`) | `settleClient`, secret, key | take mu |
| `rotateDaemonKey` (`ceph/daemon_key_rotation.go:218`) | fence reads and writes, stray inputs, `authCall`, `daemonExec` | takes mu; its deferred settling runs before the deferred unlock, because it was registered later |
| `diagnosticsSnapshot` (`ceph/diagnostics.go:218`) | `secretValues` (`:262`) | takes mu with `TryLock` and reads before the explicit unlock |
| `serviceHooks.SettleClient` (`ceph/service_hooks.go:377`), `SetClientRevoked` (`:368`) | `settleClient`, `confirmKey` | owner-held: "The caller holds the cluster mutex"; the only callers are in `cephfs` and are proved below |
| `settleClient`, `changeClient`, `ownedClientRecord`, `checkCurrentClient`, `ownedClientCapabilities`, `removeStrayAuthInputs`, `stopEarlierAuthCommands`, `settleEarlierRotation`, `daemonExec`, `writeDaemonKeyrings`, `placeDaemonKeyring`, and the `authCall` methods `send`, `mutate`, `keys`, `settle`, `removeInput` | the records | owner-held helpers; each is reached only from the functions above or from each other |

The eight callers of `settleClient`: `createClient` (`ceph/auth.go:162`),
`deleteClient` (`:291`), `replaceClientKey` (`ceph/client_key.go:202`),
`ownedClientRecord` (`:273`), `Restore` (`ceph/client_revocation.go:90`),
`containerBackend.createClient` (`ceph/shared_backend.go:396`),
`containerBackend.deleteClient` (`:444`) and `serviceHooks.SettleClient`
(`ceph/service_hooks.go:377`). Seven are on a path that takes mu first.
`serviceHooks.SettleClient` and `SetClientRevoked` are called through the hook
interface from `cephfs` (`cephfs/authorization.go:230`, `436`, `462`): their
functions are `authorizationClientCaps`, documented "must be called with
cluster.mu held" (`cephfs/authorization.go:221`), and `DeauthorizeSubvolume`.
The three functions that call `authorizationClientCaps`, one of them
`DeauthorizeSubvolume`, take `servicehook.LockTopology` first
(`cephfs/authorization.go:127`, `422`, `488`).

The audit found no caller that holds another lock or none. No locking was
changed.

Two things stay outside the component:

- `keyRotationDocker` (`ceph/ceph.go`) is a test seam read once, without a lock
  (`ceph/daemon_key_rotation.go:707`). It guards no record and no lock
  covers it, so moving it into a component whose rule is "mu held" would state
  a protection it does not have. It stays on `Container`.
- The `authFence` and `clientRevocationState` values are handed out as pointers
  and changed in place by their callers, under the same mu. The component
  guards the maps, and the callers' functions are covered by the same guard
  because they are the ones that hold mu.

## Lock order

Unchanged. The records add no lock and call no lock: `authState` methods only
read and write maps (`TestAuthStateMethodsMakeOnlyTheirPinnedCalls`). The
callers take mu first (`ceph/auth.go:146`, `ceph/client_key.go:190`,
`ceph/daemon_key_rotation.go:218`), then run native commands through the
control container, which reach `nativeState.mu` (a leaf) as before. Terminate
neither reads nor writes the records.

## Guards, in `ceph/auth_state_test.go`

- `TestAuthStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.authRecords` appears only as the receiver of an `authState` method
  and never in a composite literal.
- `TestAuthStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the field
  set are pinned, and each method makes exactly its listed calls, so none locks
  and none calls into `Container`.
- `TestAuthRecordsAreOnlyUsedWithTheClusterMutexHeld`: every use of
  `Container.authRecords`, every call of an owner-held helper and every
  construction of an `authCall` happens in a function that takes `Container.mu`
  before the use and has not released it, or in a function of the pinned
  `authOwnerHeld` list. The list is closed under callers, a stale entry fails,
  and a call by name needs the name to belong to one function.
- `TestAuthHookCallersInCephFSHoldTheClusterMutex`: the same rule for the
  `cephfs` callers of the two hooks.
- `TestNativeStateAddsNoMutex` counts `authState` too and still expects seven.
- Behavior tests for each method group and a concurrent run under `-race`.

Each guard was broken on purpose and failed: a caller without the lock, an
unpinned caller of `settleClient`, a field read outside a method, a lock inside
a method, a record read after the explicit unlock, a `cephfs` caller without
`LockTopology`, an unpinned helper, and an extra mutex.

The `FactsOf` write-site guard is untouched: no field moved here is read by
`FactsOf`.

## Limits of the guards

The guards are syntactic. They follow calls by name inside the package and in
`cephfs`; they do not follow function values, interface calls other than the two
hooks, or the order of lock operations across branches (the held state is the
last acquire or explicit release before the use, in source order). Closures are
judged by the function that contains them, so a closure that outlives its
function would escape. `applied` and `finish` callbacks run inside
`changeClient` or `Restore`, which hold mu. Behavior under real Docker or
native Ceph was not run.

## Not covered by this slice

OSD storage, the override maps, faults and debug levels, the other test seams
(including `keyRotationDocker`) and membership and control; any change to which
lock guards what; any new lock; the order in which `Terminate` closes the
other components; the `sharedBackend` port that #155 needs; and any exported
API.
