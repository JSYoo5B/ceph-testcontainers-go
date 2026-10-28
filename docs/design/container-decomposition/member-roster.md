# Member roster

Moved without rewriting from the section "Fourteenth internal slice: member roster" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `1fec584` (main after
#278); path:line citations are at the branch head. The added MONs, the MGR
candidates, the first MGR's container and the optional services move from four
`Container` fields into a `memberRoster` (`ceph/member_roster.go:35`, field
`ceph/ceph.go:71`). Exported API and behavior are unchanged.

## Fresh audit of what is left of the membership group

Counts are textual uses of `receiver.field` in non-test files of package `ceph`
at `1fec584`, so they overstate real call sites.

| Field(s) | Lock domain | Disposition |
| --- | --- | --- |
| `manager`, `monitors`, `managers`, `services` | `mu` | Owner: `memberRoster` (11, 15, 21 and 17 uses in 4, 7, 9 and 6 files). The thirteenth slice left them because several functions rewrite the maps in place and `sharedDaemons` and `terminate` iterate them. That is a reason to give the maps one owner, not to keep them open: every one of those uses is already under `Container.mu`, so the component moves exactly the data that one lock guards, and the in-place rewrites and the iteration become methods |
| `mu` | the lock | Stays. It guards the roster, the OSD registry, the fault registry and most of the other components; it is the position every lock-order citation is measured from |
| `closed` | `mu` | Stays. 44 uses in 26 files. It is the admission flag that the shared-gate path, every entry point and the fault registry's undo read in the same critical section as other state (`ceph/ceph.go:962` writes it at the top of `terminate`); a roster component that owned it would have to be called by the gate, which is the dependency the eleventh slice avoided |
| `monitorTerminated` | `mu` | Stays. It does not describe the roster. It records whether the first MON, the embedded `testcontainers.Container` of `Container` (the control container of public API), was removed or terminated (`ceph/ceph.go:1029`, `ceph/topology.go:335`), and the roster does not own that container. Moving the flag next to maps that do not contain the first MON would split one fact (is the first MON alive) from its container |
| the first MON (embedded `Container`) | `mu` | Stays. It is part of the public type, so it cannot move into an unexported component |
| `sharing`, `settings`, `keyTypeOption`, `firstMonitorImageID`, `owner` | atomic, none after `Run`, or written once | Unchanged from the thirteenth slice: no lock domain to move |

## What moved

| Before | After |
| --- | --- |
| `Container.manager` | `roster.firstMgr`, read with `initialManager`, written with `recordInitialManager`, `clearInitialManager` and `dropManager` |
| `Container.monitors` (added MONs, not the first) | `roster.monitors`: `monitor`, `monitorCount`, `eachMonitor`, `addMonitor`, `dropMonitor` |
| `Container.managers` (every MGR candidate, including a) | `roster.managers`: `manager`, `managerCount`, `eachManager`, `addManager`, `dropManager` |
| `Container.services` | `roster.services`: `service`, `hasService`, `serviceCount`, `eachService`, `sortedServices`, `serviceCopy`, `setService`, `dropService` |
| lazy `make` before each map write (`AddMonitor`, `startManager`, `SetService`) and eager `make` in `Run` | the adder allocates; `Run` leaves the roster empty |

The one invariant the component owns is that `firstMgr` mirrors the entry `a`
of `managers`. `dropManager` is the only way to forget a MGR entry and clears the
mirror with the entry `a`, as `removeManager` did by hand with two statements
(`ceph/topology.go:884`); `clearInitialManager` exists because `terminate`
clears the mirror when the first MGR's container is gone and keeps the entry
until its loop reaches it (`ceph/ceph.go:994-1005`). The iterators are
range-over-func in map order, like `osdRegistry.each`, and allow `drop*` of the
visited key, which `terminate` relies on (`ceph/ceph.go:976,1001,1016`). The
service-package hooks `Service`, `SetService`, `DeleteService` and `Services`
(`ceph/service_hooks.go:181-197`) are thin callers of the roster; the
signatures are unchanged.

## Why no new mutex and no lock-order change

The roster has no lock. Every access is under `Container.mu`, taken by the
function (`lockTopology`, `ceph/topology.go:380`, or `c.mu.Lock` in
`ManagerContainer` `ceph/ceph.go:453` and `ServiceContainers` `ceph/ceph.go:897`,
or `TryLock` in `diagnosticsSnapshot` `ceph/diagnostics.go:218`) or by a caller
in the same call chain, as the caller table below pins. The component sits where
the fields sat in the lock order: after the shared-gate admission and the
topology gate, before `configMu`, `controlMu` and the leaf locks, and no method
takes a lock or calls into `Container`. `sharedDaemons` still copies the roster
into a map under `Container.mu` and inspects the containers after it is
released (`ceph/shared_cluster_port.go:43-73`), so a slow Docker call never
holds the lock. The service hooks are documented as called with the cluster
mutex held, as before; the service packages are not scanned.

## Terminate order preserved

`terminate` still stops the services first, while their OSDs are alive
(`ceph/ceph.go:976`), then the OSDs, then the storage release, then the first MGR
(`:994`), the other MGRs (`:1001`), the added MONs (`:1016`) and the first MON
(`:1025`), and keeps a member whose container cannot be removed so a retry
finds it. The port-lease and the OSD-storage order guards were updated for the
new spelling and not loosened.

## Mutex count unchanged

`TestNativeStateAddsNoMutex` scans `memberRoster` as a component and still
expects the same eight named mutexes (`Container.mu`, `extensionStore.mu`,
`controlPlaneState.mu`, `bootstrapRecord.mu`, `nativeState.mu`,
`featureClientState.mu`, `featureClientState.publishMu`,
`debugLevelRegistry.mu`). A mutex added to the roster fails it (negative
control). `Container` declares one mutex.

## Guards, in `ceph/member_roster_test.go`

- `TestContainerDeclaresNoRosterFields`: `Container` no longer declares
  `manager`, `monitors`, `managers` or `services`, and the roster has exactly
  the fields `firstMgr`, `monitors`, `managers` and `services`.
- `TestMemberRosterFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.roster` is only the receiver of a roster method (so no code reads or
  writes a map); no production `Container` literal sets it; `FactsOf` never
  mentions it; the guard is not vacuous.
- `TestMemberRosterMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned (no lock, no `Container` call), and each
  field, also through an index assignment, is written only by its owning
  methods.
- `TestMemberRosterCallersAreAudited`: the caller table. Each function that uses
  the roster, the methods it calls and its guard kind (takes, holds, hook) are
  pinned. A "takes" function acquires the lock before its first use; a "holds"
  or "hook" function has a doc comment that mentions the held lock, and every
  call to a "holds" function comes from an acquiring function or from a pinned
  "holds", "hook" or "init" function. A new, stale or unguarded row fails.
- `TestMemberRosterHasNoAddressTaken`: nothing takes the address of the roster
  or of `firstMgr`; `TestMembershipControlStateIsNeverAddressed` lists `roster`
  in place of the four fields.
- `TestTerminateStopsServicesBeforeOSDsAndMembersAfterThem`: the order above,
  and that the first MGR is read twice (to terminate it, then to decide whether
  the entry `a` goes).
- Behavior: the zero value is empty and writable; a service recorded without a
  container is present; `dropManager("a")` clears the mirror and any other name
  does not; `clearInitialManager` leaves the entry; each iterator allows
  dropping the visited key and stops early; `sortedServices` orders by name; a
  change to `serviceCopy` does not reach the roster; the service hooks reach the
  roster.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a second mutex in the roster, one of the old fields
declared again on `Container`, `dropManager` that keeps the mirror, a call added
to a method, a method that writes another field, a method that calls into
`Container`, a function outside the table that uses the roster, a "takes"
function without its lock, a "holds" function called from an unguarded function,
a hook whose doc loses the lock sentence, the address of the roster taken, a map
read outside the type, a production `Container` literal that sets `roster`,
`FactsOf` reading the roster, `terminate` reading the first MGR once,
`terminate` stopping the monitors before the managers, and `terminate` without
the services loop.

## Limits of the guards

The guards are syntactic and follow calls by name inside the package, so two
functions with the same name are merged for the "holds" check; they do not
follow function values or interface calls other than the ones listed. The
service hooks are called from other packages (`rgw`, `cephfs`, `fault`), which
are not scanned: their "caller holds the mutex" contract is documentation, as
before. The behavior tests are unit tests with fake containers. No Docker or
native Ceph behavior was run for this slice.

## Not covered by this slice

`mu`, `closed`, `monitorTerminated` and the first MON stay on `Container` for
the reasons in the audit table. Also not covered: `sharing`, `settings`,
`firstMonitorImageID`, `owner`, `keyTypeOption`, the `Mutex`, `ControlMutex` and
`ConfigMutex` pointers in `TestState`, the three dependency-specific test seams,
startup identity, the override maps, any change to which lock guards what, any
new lock and any exported API. `ceph.Container` has 229 method declarations
before and after this change (information only; it is not a target).
