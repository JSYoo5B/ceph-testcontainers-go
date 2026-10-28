# OSD handle registry

Moved without rewriting from the section "Seventh internal slice: OSD handle registry" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

The seventh component moved out of `Container` is the OSD handle registry, held
as `Container.osdHandles` of the unexported type `osdRegistry`
(`ceph/osd_registry_state.go`). It replaces the fields `Container.osds` (the
owned descriptors by OSD ID) and `Container.detachedOSDs` (the retained stores
and registrations without a container). The type has no mutex. 0.x: the old
fields are removed without aliases, and every one of the roughly thirty test
files that read `c.osds` directly was migrated to the registry methods.

The registry methods are `get`, `count`, `each` (a range-over-func iterator in
map order), `sorted`, `register`, `forget`, `detachedOf`, `park` (the detach
transition: record the detached OSD and drop the owned descriptor in one call),
`unpark` (the reopen transition: drop the detached OSD and record the owned
descriptor in one call) and `replace` (for the unit-test hook only). A handle
is owned or detached, never both, and `park` and `unpark` are the only methods
that touch both maps, so a caller cannot break the pairing by forgetting half
of it.

## The service-hook pointer

The audit of the fifth slice found that `serviceHooks.TestState` published
`OSDs: &c.osds`, a pointer to the map itself, and that `TestCluster` built the
map. A test in `cephfs` or `fault` could therefore write the map without
`Container.mu`. The pointer is removed. The test cluster needs two things, and
only these two are exposed, as functions that take the cluster:

- `servicehook.TestReplaceOSDs(c, osds)` swaps the owned descriptors (the
  `cephfs` and `fault` fixtures set them to nil, to an empty map or to a map
  with one fake OSD);
- `servicehook.TestOSDCount(c)` reads the number of owned descriptors (the
  `cephfs` admission test checks that a refused call left both OSDs owned).

Both take no lock, like the pointer they replace: they are for a cluster value
that no other goroutine uses yet, or that the test holds through the mutex
that `TestState` still exposes. `TestCluster` registers its descriptors through
`register`. The exposed state view no longer has a member that points at OSD
descriptors.

## Audit of the callers

Every production access, after the move, with how `Container.mu` is held:

| Guard | Functions |
| --- | --- |
| takes `Container.mu` itself, before the first use | `removeOSD` (`ceph/ceph.go:706`), `terminate` (`:959`), `detachOSD` (`ceph/osd_retained.go:204`), `reopenOSD` (`:281`), `osdStates` (`ceph/osd_policy.go:27`), `WaitForClean` (`ceph/status.go:38`), `daemonsRunning` (`ceph/shared_backend.go`), `diagnosticsSnapshot` (`ceph/diagnostics.go`, through `TryLock`), `openDebugTarget` (`ceph/debug_level.go:175`, after `lockTopology`) |
| documented as called with `Container.mu` held, and every call comes from a function that takes it | `registerOSD` (from `AddOSDWithConfig`, `ceph/ceph.go:537`, and `startInitialOSDs`, `ceph/initial_daemons.go:26`), `currentOSD`, `ownedBy`, `ownsDaemon`, `daemonKeyrings`, `generationLocked`, `validateOSDPlacement`, `poolPlacementDomains`, `nativePoolDomains`, `ownedOSDState`, `ownsNativeTargetContext`, `monitorConfigTargets`, `osdsLocked`, and the functions between them and a lock |
| hooks for another package whose documentation requires the mutex | `OwnedOSD` and `Owns` (`ceph/fault_hooks.go`), `PoolPlacementDomains`, `TestReplaceOSDs`, `TestOSDCount` (`ceph/service_hooks.go`) |
| runs before the cluster is shared | `prepareInitialComposition` (`ceph/composition.go`, a throwaway `Container` that only plans), `TestCluster` |

The finding that matters is that the domain is single: every access is under
`Container.mu`, with no read that relies on the other locks. `FactsOf` reads
none of it. `registerOSD` takes no lock itself (as the audit row said); its two
callers take it before the call, and the guard below makes that a checked
fact for it and for the other "holds" functions.

## Why no new mutex

Every reader and writer already runs under `Container.mu`, taken by the
function itself (`lockTopology`, `ceph/topology.go:390`, or `mu.Lock`) or by a
caller within the same call chain, as listed. The registry therefore sits where
the fields sat in the lock order: after the shared-gate admission and the
topology gate, before `configMu`, `controlMu` and the leaf locks, and the
registry methods take no lock and call no `Container` method. The number of
mutexes stays at seven.

## Terminate order preserved

`terminate` still terminates each owned OSD container and forgets its
descriptor (`ceph/ceph.go:988-996`) before the first storage release stage,
which is told `count() == 0` (`:997-998`), and reads the count again for the
retry stage after `removeLabeledContainers` (`:1062-1063`). The loop keeps an
OSD whose container cannot be removed, as before.

## Guards, in `ceph/osd_registry_state_test.go`

- `TestOSDRegistryFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.osdHandles` appears only as the receiver of an `osdRegistry`
  method (so no code reads either map or takes its address), and `FactsOf`
  never mentions it.
- `TestOSDRegistryMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none locks or calls into `Container`, and
  each map is assigned only by its owning methods (`owned` by `register` and
  `replace`, `detached` by `park`).
- `TestOSDRegistryCallersAreAudited`: every function that uses the registry,
  the methods it calls and its guard kind (takes, holds, hook, init) are
  pinned. A "takes" function acquires the lock before its first use. A "holds"
  or "hook" function has a documentation comment that mentions the held lock,
  and every call to a "holds" function, found by name over the production
  files, comes from a function that acquires the lock before the call or is
  itself a pinned "holds", "hook" or "init" function. A stale row fails.
- `TestOSDRegistryHasNoAddressTakenAndTestHooksExposeNoMap`: nothing takes the
  address of the registry or one of its maps, and the exported test state
  view has no member that points at OSD descriptors.
- `TestOSDRegistryReplaceHasTheSingleCallerTheTestHook`: `replace` is called
  from production only by `TestReplaceOSDs`.
- `TestTerminateDropsTheOSDsBeforeTheStorageReleaseDecidesOnTheirCount`: the
  forget loop, the first release stage, the labeled-container removal and the
  retry stage keep their order and the count is read for each of the four
  release calls.
- `TestNativeStateAddsNoMutex` counts `osdRegistry` too and still expects
  seven.
- Behavior: the zero value is empty and writable, `sorted` orders by ID,
  `each` allows forgetting the visited key and stops early, `park` and
  `unpark` move a handle between the maps, `replace` leaves the detached map
  alone.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a map read outside the type, a call added to a
method, `forget` assigning the detached map, a caller not in the table, a
"takes" function without its lock, a "holds" function called from a function
without the lock, the address of the registry taken, the test state view
exposing the map again, a second caller of `replace`, a mutex added, `park`
keeping the owned handle, `unpark` keeping the detached handle, one count read
dropped from `terminate`, the forget loop moved after the release, `each`
ignoring an early stop, and `register` without the lazy map.

## Limits of the guards

The guards are syntactic. They follow calls by name inside the package, so two
functions with the same name are merged; they do not follow function values or
interface calls other than the ones listed; a "takes" function is judged by
the position of its acquire before its first use, not by where it releases, and
closures are judged by the function that contains them. The hooks `OwnedOSD`,
`Owns` and `PoolPlacementDomains` are called from other packages (`fault`,
`cephfs`), which are not scanned: their "caller holds the mutex" contract is
documentation, as before. Behavior under real Docker or native Ceph was not
run.

## Not covered by this slice

The remaining components (the override maps, faults and debug levels, the other
test seams, membership and control), any change to which lock guards what, any
new lock, the `osdMemory` and `osdVolume` records (already moved in the fifth
slice), the order in which `Terminate` closes the other components, the
`sharedBackend` port that #155 needs, and any exported API. The test hook
functions are not synchronized. No Docker or native Ceph behavior was run.
