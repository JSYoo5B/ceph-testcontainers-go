# Host-port lease state

Moved without rewriting from the section "Eighth internal slice: host-port lease state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `39b26f9` (main after
#267); path:line citations are at the branch head unless they name another
revision. The eighth component moved out of `Container` is the host-port
reservation list of host network mode, held as `Container.portLeases` of the
unexported type `portLeaseState` (`ceph/port_lease_state.go`). It replaces the
field `Container.portLeases` (a `[]*hostPortLease`) of the same name. The type
has no mutex. Exported API and behavior are unchanged. This slice is not the
membership/control step: that group stays whole, see below.

## Fresh audit of the fields of `Container`

The earlier tables of this note are partly stale: the override maps, the OSD
maps, the run volume and the port list have since moved or been renamed. At
`39b26f9`, `Container` has 33 named fields (plus the embedded
`testcontainers.Container`), 39 exported and 189 unexported methods
(information only, no target). Fields by state, with this slice applied:

| State | Fields | Status |
| --- | --- | --- |
| inside a component | `nets`, `nativeCmds`, `toolkit`, `authRecords`, `stores`, `runDir`, `osdHandles`, `portLeases` | components with the guards of their own section |
| named registry, guarded as written | `faults` (`faultRegistry`, guarded by `mu`, `ceph/fault_registry.go:115`), `debugLevels` (`debugLevelRegistry`, own lock, `ceph/debug_level.go:135`) | already a type of their own; the undo and release methods that walk them are still methods on `Container` (`undoFaults` and the per-kind handles), so a state component needs the fault-handle seam of #146, not a field move |
| test seams | `keyRotationDocker` (`ceph/daemon_key_rotation.go:707`), `ownedContainerDocker` (`ceph/owned_containers.go:35`), `daemonRunner` (`ceph/daemon_image.go:79`) | one reader each and no state to join; they move with the component that owns their dependency |
| bootstrap record | `config`, `keyring`, `cephVersion`, `keyTypeOption`, `firstMonitorImageID`, `owner` | `config` is rewritten after `Run` under `configMu` (`ceph/placement.go:141`, `ceph/topology.go:449`), many readers were not checked for which lock they hold (unverified), and `service_hooks.go:473` publishes `&c.config`, `&c.keyring` and `&c.cephVersion` to other packages; not a clean single-domain move |
| membership and control | `mu`, `controlMu`, `configMu`, `controlPlane`, `manager`, `monitors`, `managers`, `services`, `extensionsMu`, `extensions`, `closed`, `monitorTerminated`, `settings`, `sharing` | four lock domains (`mu`, `controlMu`, `configMu`, `extensionsMu`) plus the `sharing` atomic; `closed` and `monitorTerminated` are read in about a dozen functions under `mu`, and `TestState` publishes pointers to `closed` and `controlPlane` (`ceph/service_hooks.go:472-473`); the first safe step needs the test-hook pointers removed as the OSD slice did, and is a slice of its own |

## Why this component, and what was not picked

`portLeases` is the one field that is written in three places, read in two, and
covered by a single lock domain with no outside pointer:

| Part | Writers | Readers | Guard |
| --- | --- | --- | --- |
| lease slice | `reserveTrackedHostPortsWith` (`ceph/host_port_tracking.go:32`, after `lockTopology` at `:23`), `trackHostPortLease` (`ceph/ceph.go:434`, with `mu.Lock`), `AddMonitor` (`ceph/topology.go:202`, after `lockTopology` at `:149`) | `terminate` (`ceph/ceph.go:977`, which holds `mu`), the exported test view (`ceph/service_hooks.go:476`, a count) | `Container.mu` |

The hook publishes a count, never a pointer, so no outside package can write the
slice. The membership/control group was not picked because the hooks above and
the four lock domains make any move there a review of its own. The fault and
debug-level registries were not picked because their undo logic still lives on
`Container`. The bootstrap record was not picked because `config` is rewritten after
`Run` under `configMu` while `keyring` is written once, and the hook publishes
pointers to both.

## Why no new mutex

Every access runs under `Container.mu`, taken by the function itself
(`lockTopology`, or `mu.Lock` in `trackHostPortLease`) before the first use.
Each lease has its own mutex, taken by `hostPortLease.Release`
(`ceph/host_network.go:69`) through `lockTopologyMutex`; that lock stays a leaf
below `mu` and is not touched. The component takes no lock and calls into no
`Container` method. The number of mutexes counted by `TestNativeStateAddsNoMutex`
stays at seven. That test lists the component types by name and does not cover
`debugLevelRegistry`, whose own `mu` makes eight in total; this note records
that gap and does not change it.

## Lock order and Terminate order preserved

`terminate` still undoes the faults (`ceph/ceph.go:972`), stops the native
commands (`:976`) and then releases the leases (`:977`), before it terminates
the services (`:979`), the OSDs and the monitors. `release` still tries every
lease, collects the failures and keeps every lease recorded, so a failed
release is retried by the next `Terminate` and a released one is a no-op. A
lease whose reservation failed or whose context ended is still published as
soon as its allocator exists, without a context gate.

## Guards, in `ceph/port_lease_state_test.go`

- `TestPortLeaseStateFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.portLeases` appears only as the receiver of a `portLeaseState`
  method, nothing reads the slice or takes an address, and `FactsOf` never
  mentions it.
- `TestPortLeaseStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none locks or calls into `Container`, and
  only `track` assigns the slice.
- `TestPortLeaseStateCallersAreAudited`: every caller, the methods it calls and
  its guard kind are pinned; a "takes" function acquires `mu` before its first
  use, the test hook documents its contract, and a stale row fails.
- `TestTerminateReleasesPortLeasesAfterNativeCommandsAndBeforeTheServices`.
- `TestNativeStateAddsNoMutex` counts `portLeaseState` and still expects seven.
- Behavior: an empty state is inert, a nil lease is not recorded, `release`
  tries every lease and keeps a failed one for retry.

Negative controls, each failing against the deliberately broken version and
passing after restoring it: a slice read outside the type, the address taken, a
call added to a method, an assignment in `release`, a lock added to `track`, a
mutex added, a caller without the lock, a caller not in the table, `release`
moved after the services, `release` moved before the faults, a release that
stops at the first failure, a release that forgets its leases, and a `track`
that records a nil lease.

## Limits of the guards

The guards are syntactic. They follow calls by name inside the package and judge
a function by the position of its first acquire, not by where it releases. The
hook `TestState` is called from other packages, which are not scanned: its
"caller holds the lock or the cluster is not shared" contract is documentation.
No Docker or native Ceph behavior was run.

## Not covered by this slice

Membership and control (all four lock domains, `closed`, `monitorTerminated`,
the `TestState` pointers), the fault and debug-level registries as state
components, the bootstrap record, the three test seams, the debug-level
registry in the mutex count, any change to which lock guards what, any new
lock, `hostPortLease` itself (reservation, parsing and release are unchanged),
and any exported API.
