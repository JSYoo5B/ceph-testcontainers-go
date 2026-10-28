# Fault registry and debug-level window registry

Moved without rewriting from the section "Tenth internal slice: fault registry and debug-level window registry" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `4293511` (main after
#269); path:line citations are at the branch head unless they name another
revision. The two registries that the eighth slice left on `Container` are now
components that carry their own logic. `Container.faults` is a `faultRegistry`
(`ceph/fault_registry.go:124`) whose methods hold the reservation, release and
Terminate undo logic that used to be methods on `Container`
(`reserveFault`, `settleFault`, `releaseFault`, `tracksFault`, `heldFaults` and
`undoFaults`; all removed). `Container.debugLevels` stays a `debugLevelRegistry`
(`ceph/debug_level.go:136`) and gets guards of its own. Exported API and
behavior are unchanged.

## What moved

| Before | After |
| --- | --- |
| `Container.reserveFault`, `settleFault`, `releaseFault`, `tracksFault` | `faultRegistry.reserve`, `settle`, `release`, `tracks` |
| `Container.heldFaults`, `Container.undoFaults` | `faultRegistry.held`, `faultRegistry.undo` |
| `c.faults.entries[...]` read in `allowPoolDeleteHeld`, `refuseInstanceFaults` and the two `TestFaults`/`TestHeld` hooks | `faultRegistry.holds`, `openInstanceFault`, `handles` |

Two seams changed shape so that the component reads no `Container` field and
calls no `Container` method:

- `settle` used to return `Container.closed`. It returns nothing now, and the
  two `Settle` hooks (`ceph/fault_hooks.go:55`, `ceph/override_hooks.go:89`)
  read `c.closed` themselves after it, in the same critical section, so a live
  settle, a handle-less settle and a stale token still report the flag exactly
  as before.
- `undo` used to unlock and relock `c.mu` around each network-shaping restore.
  It receives the cluster mutex as a `sync.Locker` argument (`terminate` passes
  `&c.mu`, `ceph/ceph.go:974`) and calls only `Unlock` and `Lock` on it
  (`ceph/fault_registry.go:268,270`), only in the shaping phase.

## Why no new mutex and no lock-order change

`faultRegistry` has no mutex. Every method runs under `Container.mu`, taken by
the caller before the first use (`terminate` through `lockTopology` at
`ceph/ceph.go:963`, which is followed by `c.closed = true` at `:967`; the hooks
document that the caller holds the mutex, the same contract as before). The one
release of `mu` is the existing one around a shaping restore, which still runs
before any bridge is removed, so the order stated in `ceph/network_state.go`
holds unchanged. `terminate` still sets `closed`, undoes the faults
(`ceph/ceph.go:974`), stops the native commands (`:978`) and then releases the
leases (`:979`).

`debugLevelRegistry` keeps its leaf lock (`ceph/debug_level.go:137`). `Reserve`
(`:142`) and `Release` (`:156`) take it first, defer the unlock, call only
`invalid`, `errors.New`, `make` and `delete`, and never name `Container`. The
override package calls them from its own window state
(`override/debug_level.go:208,236,286,464`). The lines read do not call them
inside a section of `Container.mu`, but those callers are in another package
and are not scanned, so that part is documentation and not a guard. The registry is handed out
once, by `overrideHooks.DebugWindows` (`ceph/override_hooks.go:152`), and
`terminate` never touches it: a window is released by its own handle.

## Mutex count, now honest

`TestNativeStateAddsNoMutex` used to list the component types by name and left
`debugLevelRegistry` out, so its count of seven understated the real eight.
It now scans every component type including `faultRegistry` and
`debugLevelRegistry`, collects each mutex as `Type.field`, and compares the
sorted list with the pinned `nativeStateMutexOwners` (`ceph/native_state_test.go:272`):

| Owner | Mutexes |
| --- | --- |
| `Container` | `mu`, `controlMu`, `configMu`, `extensionsMu` |
| `nativeState` | `mu` |
| `featureClientState` | `mu`, `publishMu` |
| `debugLevelRegistry` | `mu` |

That is eight. No mutex was added by this slice: the eighth is the one that the
old test did not scan. Mutexes of other types (a lease or a handle with its own
lock) are not in the scope of this test and are not part of the claim.

## Guards, in `ceph/fault_registry_state_test.go`

- `TestFaultRegistryFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.faults` is only the receiver of a `faultRegistry` method call; the
  entries map is never read, no address of the registry or the map is taken, and
  `FactsOf` never mentions it.
- `TestFaultRegistryMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each are pinned, only `undo` operates on a lock and only through its
  parameter, and no method names `Container`, `closed` or a mutex field.
- `TestFaultRegistryCallersAreAudited`: every caller, the methods it calls and
  its guard kind (takes the lock before the first use, or documents that the
  caller holds it); a stale row fails.
- `TestTerminateUndoesFaultsFirstUnderTheClusterMutex`: `faults.undo` runs once,
  after `closed` is set, before the native commands and the lease release, and
  receives `&c.mu`.
- `TestDebugLevelRegistryStaysALeafBehindItsOwnLock`: pinned calls, lock before
  the first access, deferred unlock, one named use of `Container.debugLevels`.
- Behavior: both `Settle` hooks report `closed` for a live, handle-less and stale
  token; the new accessors read what the callers need.
- `TestPortLeaseStateCallersAreAudited` and the lease Terminate-order test were
  updated to the new name `faults.undo`.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: an `entries` read outside the type, the address of
`faults` taken, a call added to `settle`, a `Container` field read in a method,
`undo` without its relock, a caller not in the table, a hook without its lock
documentation, `undo` removed from `terminate`, `undo` given another mutex, a
`Release` without its lock, a second use of `debugLevels`, a mutex added to
`faultRegistry`, the debug-level registry dropped from the scan, a `Settle` hook
that ignores `closed`, and an `openInstanceFault` that ignores the completion
check.

## Limits of the guards

The guards are syntactic: they follow calls by name inside the package and judge
a function by the position of its first acquire. The hooks are called from other
packages that are not scanned. No Docker or native Ceph behavior was run for
this slice.

## Not covered by this slice

Membership and control, the bootstrap record, the three test seams, any change
to which lock guards what, any new lock, the fault kinds table and the override
package, and any exported API. The override package keeps calling the same
hooks; the typed hook seam in `internal/overridehook` is unchanged.
