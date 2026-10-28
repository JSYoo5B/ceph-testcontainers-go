# Run directory state

Moved without rewriting from the section "Sixth internal slice: run directory state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

0.x: old names are removed without aliases. Inspected at `c74f0e5` (main after
#260); path:line citations are at that revision unless they name a new file.
The sixth component moved out of `Container` is the shared admin-socket run
directory, held as `Container.runDir` of the unexported type `runDirState`
(`ceph/run_dir_state.go`). It replaces the field `Container.run` (a
`*runVolume`) and the unexported helper `Container.runMount`, which is deleted.
The type has no mutex. Exported API and behavior are unchanged.

## Why this component, and what was not picked

The audit row "OSD storage" left `osds`, `detachedOSDs` and `run` on
`Container`. The OSD handle registry (`osds`, `detachedOSDs`) is not picked
yet: `osds` is read in about twenty production files across four lock domains,
the test hook publishes a pointer to the map itself (`OSDs: &c.osds`,
`ceph/service_hooks.go:473`) and builds it (`:438`), and about thirty test files
reference it directly. Moving it needs a decision about that pointer, which is the
service-hook seam, and is a review of its own. `run` is the opposite: one
field, twelve production call sites in twelve functions, and a lock behavior that
differs between them, which makes it the part of the audit row that can be
pinned today. The two test seams that remained on `Container` are not moved
either: `keyRotationDocker` (`ceph/daemon_key_rotation.go:707`) and
`ownedContainerDocker` (`ceph/owned_containers.go:35`) each have one reader and
no state of their own to join, and `daemonRunner` (`ceph/daemon_image.go:79`)
belongs to the membership registry. A seam moves with the state of the
dependency that uses it, so these wait for their components.

## Audit of the moved field

| Part | Writers | Readers | Guard |
| --- | --- | --- | --- |
| volume pointer | `Run` through `create` (`ceph/ceph.go:217`), before the cluster is returned; never assigned again | `mounts` in `runMonitor` (`ceph/ceph.go:348`), `ensureControlPlane` (`ceph/topology.go:74`), `AddMonitor` (`:227`), `startManager` (`:796`) and `clientContainerContext` (`ceph/images.go:162`); `published` in `clearAdminSocket` (`ceph/admin_socket.go:210`) and `clearStaleOSDSocket` (`ceph/osd_retained.go:346`); `volumeName` in `newOSDLaunchRecipe` (`ceph/osd_launch.go:51`) | none needed: the pointer and the volume name are immutable after `Run` |
| removed mark | `release` in `terminate` (`ceph/ceph.go:1065`) | `shared` in `adminSocket` (`ceph/admin_socket.go:133`) and `osdDebugTarget.set` (`ceph/debug_level.go:641`) | `Container.mu`: `terminate` and both readers call `lockTopology` first |

The finding that matters: `clientContainerContext` starts the feature client
while holding only the feature-client startup lock, not `Container.mu`
(`ceph/images.go:128-141`). It reads the volume through `mounts`. That is safe
only because the pointer is never reassigned after `Run` and `mounts` reads
only the immutable name, never the removed mark. The old code read `c.run`
there with no stated reason; the type now says why, and the guard below fails
when a second writer of the pointer or a lock-free reader of the removed mark
appears. `FactsOf` reads none of this.

## Why no new mutex

The removed mark has one lock domain, `Container.mu`, taken by every reader and
writer as listed. The pointer needs none. The number of mutexes stays at seven.

## Lock order and Terminate order preserved

- `terminate` still releases the volume once, after `removeLabeledContainers`
  (which takes out every container that mounts it) and the storage retry, and
  before the bridges are removed (`ceph/ceph.go:1061-1067`).
- A failed release keeps the removed mark unset, so the volume stays shared
  and the next `Terminate` retries it. A removed volume stays published, which
  is what the stale-socket cleanup keys on, and keeps its name for the launch
  recipe.
- The component calls into no `Container` method and takes no lock.

## Guards, in `ceph/run_dir_state_test.go`

- `TestRunDirStateFieldIsOnlyTouchedByItsMethods`: outside the type,
  `Container.runDir` appears only as the receiver of a `runDirState` method;
  `FactsOf` never mentions it.
- `TestRunDirStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the calls
  of each method are pinned, none locks or calls into `Container`, and only
  `create` assigns the volume pointer.
- `TestRunDirStateCreatePublishesBeforeTheDockerCreate`: the record is assigned
  before the Docker create.
- `TestRunDirStateCallersAreAudited`: every caller and the method it calls is
  pinned with its guard kind (init, free, takes); `create` has the single caller
  `Run`; `shared` and `release` are called only from functions that call
  `lockTopology` first.
- `TestTerminateReleasesTheRunDirectoryAfterTheStorageRetryAndBeforeTheBridges`.
- `TestNativeStateAddsNoMutex` counts `runDirState` too and still expects seven.
- Behavior: an empty component is inert, and a failed release keeps the volume
  shared while a successful one does not and is not repeated.

Negative controls, each failing against the broken version and passing after
restoring it: a field read outside the type, a call added to `shared`, an
assignment in `release`, the lock removed from `adminSocket`, a mutex added, a
publish after the create, a second `release` call in `terminate`, a caller not
in the table, and a release that marks the volume removed after a failure.

## Not covered by this slice

The OSD handle registry (`osds`, `detachedOSDs`), the service-hook pointer to
`osds`, the three test seams above, membership and control, any change to which
lock guards what, any new lock, the `runVolume` type itself (its create,
remove and validation are unchanged), and any exported API. The creation of the
volume is pinned only by the order of its publish, because `runVolume.create`
has no Docker seam in `create`. No Docker or native Ceph behavior was run.
