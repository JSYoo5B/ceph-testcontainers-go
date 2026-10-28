# OSD storage state

Moved without rewriting from the section "Fifth internal slice: OSD storage state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

Inspected at `7266eb7` (main after #247, which includes the per-daemon image
option #244); path:line citations in this section are at that revision unless
they name a new file. The fifth component moved out of `Container` is the
owned storage of the OSDs, held as `Container.stores` of the unexported type
`osdStorageState` (`ceph/osd_storage_state.go`): the tmpfs storage of
`WithOSDInMemoryStorage` (`osdMemory`, now `memory`), the ordinary volume of
`WithOSDVolumeStorage` (`osdVolume`, now `volume`) and the three test seams that
replace their collaborators (`osdRunner` now `runner`, `osdVolumeDocker` now
`volumeDocker`, `osdVolumeRunner` now `volumeRunner`). The type has no mutex.
All `Container` methods, their signatures and their behavior are unchanged.

## What stayed on Container, and why

The audit row "OSD storage" above lists `osds`, `detachedOSDs`, `osdMemory`,
`osdVolume` and `run`. This slice moves only the two storage records and the
seams. `osds` and `detachedOSDs` are the registry of OSD handles: they are read in
about twenty files for handle identity, purge state and topology, not for
storage, and `detachedOSDs` pairs with `osds` in `detachOSD` and `reopenOSD`
(`ceph/osd_retained.go:274,321`). `run` is the admin-socket volume
(`runVolume`, `ceph/admin_socket.go`), written once by `Run`, which no storage
path owns. Moving the handle registry is a separate review.

## Audit of the moved fields

Every production access, at `7266eb7` (before the move; the "now" column is the
component method that replaced it):

| Field | Writers | Readers | Guard |
| --- | --- | --- | --- |
| `osdMemory` | `ensureOSDMemory` publishes the record before the allocation (`ceph/osd_memory_storage.go:109`, now `beginMemory`); `terminate` clears it after a successful release (`ceph/ceph.go:1065,1139`, now `releaseMemory` and `retryMemory`) | `ensureOSDMemory` (`:103`), `registerOSD` (`ceph/ceph.go:693`, `requireFresh`), `removeOSD` (`:867`, `discard`), `newOSDLaunchRecipe` (`ceph/osd_launch.go:54`, `launchVolumes`), `terminate` | `Container.mu`, taken by `startInitialOSDs` (`ceph/initial_daemons.go:26`) and `AddOSDWithConfig` (`ceph/ceph.go:585`) around `registerOSD`, and by `removeOSD` (`:765`) and `terminate` (`:1024`) |
| `osdVolume` | `ensureOSDVolume` (`ceph/osd_volume_storage.go:97`, now `beginVolume`); `terminate` (`ceph/ceph.go:1071,1144`) | the same functions, plus `detachOSD` (`ceph/osd_retained.go:211,263`), `reopenOSD` (`:302`) and `reopenPreflight` (`:381`, through `retainedStore`) | `Container.mu`: `detachOSD` (`:204`) and `reopenOSD` (`:285`) take it, and `reopenPreflight` runs inside `reopenOSD` |
| `osdRunner` | none in production; tests assign it | `startOSD` (`ceph/ceph.go:727`, now `runOSD`) | `startOSD` runs without `Container.mu` so that `Run` can start several OSDs at once, and only reads this function value, which nothing writes after construction |
| `osdVolumeDocker` | none in production | `ensureOSDVolume` copies it into the new record (now `beginVolume`) | `Container.mu` through `registerOSD` |
| `osdVolumeRunner` | none in production | `ensureOSDVolume` (`ceph/osd_volume_storage.go:101`, now `runVolumeHelper`) | `Container.mu` through `registerOSD` |

`FactsOf` reads none of these fields.

## Why no new mutex

Every writer already runs under `Container.mu`, taken by the function itself
or by its caller within the same call chain listed above, so the fields have a
single lock domain and the number of mutexes stays at seven. The only access
that does not hold `mu` is the read of `runner` in `startOSD`, which is safe
because no production code writes a seam (guarded below). The lock-free
`FactsOf` source (`ceph/cluster_facts.go`) is unaffected and the guard from #238
stays valid.

## Lock order and Terminate order preserved

Lines in this list are after the move.

- `terminate` takes `lockTopology` (`ceph/ceph.go:1002`) and holds `c.mu`
  through teardown, as before (the fault undo phases still release it around
  network restores, as written in the first slice).
- First release stage: `releaseMemory` and `releaseVolume` (`ceph/ceph.go:1040-1041`)
  run after the services and the OSD containers are removed and only when
  `len(c.osds) == 0`, memory first. A stage that fails keeps its record, so
  nothing is forgotten before the storage is gone. They run before the manager,
  the managers, the monitors, the control container and the feature client.
- Second stage: `retryMemory` and `retryVolume` (`ceph/ceph.go:1105-1106`) run
  after `removeLabeledContainers`, because that removal can take out a storage
  user whose create reply was lost (an OSD or a helper). They repeat the release
  only for a stage that failed in the first one and only while no OSD is left;
  otherwise they return the first stage's error unchanged, and both errors are
  appended to the result in the old order.
- `registerOSD` checks freshness in memory then volume order and `removeOSD`
  discards in the same order, each stopping at its first failure, as before.
- The component calls into no `Container` method and takes no lock, so it is
  below `Container.mu` and below every lock that `Container.mu` precedes.

## Guards, in `ceph/osd_storage_state_test.go`

- `TestOSDStorageStateFieldsAreOnlyTouchedByItsMethods`: outside the type,
  `Container.stores` appears only as the receiver of an `osdStorageState`
  method, and the `FactsOf` source never mentions it.
- `TestOSDStorageStateMethodsMakeOnlyTheirPinnedCalls`: the method set and the
  calls of each method are pinned, none takes a lock or calls into `Container`,
  and only the publish and release methods assign `memory` and `volume`.
- `TestOSDStorageStateSeamsHaveNoProductionWriter`: no production code assigns
  or sets `runner`, `volumeDocker` or `volumeRunner`.
- `TestOSDStorageStateCallersHoldTheTopologyLock`: the set of callers is pinned;
  `removeOSD`, `detachOSD`, `reopenOSD` and `terminate` call `lockTopology`
  before their first use, and the functions that rely on the caller are reached
  from functions that take it.
- `TestTerminateReleasesOSDStorageInItsTwoStagesInOrder`: lock, memory, volume,
  manager and monitor termination, labeled-container removal, retry memory, retry
  volume, each once and in that order.
- `TestNativeStateAddsNoMutex` counts `osdStorageState` too and still expects
  seven.
- Behavior tests: the release stages forget a record only after success and
  retry only a failed stage, the seams, freshness and discard order, and the
  existing memory and volume storage tests (renamed fields only).

Negative controls were run for each guard and for the behavior tests (a field
read outside the component, a call added to a method, a production seam write, a
lock call removed from `removeOSD`, the retry stage removed, a mutex added, a
failed release that forgets its record, a retry that ignores the first stage,
a diverging validation in the shared cleanup, and a seam that is not wired);
each failed against the broken version and passed after restoring it.

## The duplicated cleanup (issue #233)

`osdMemoryStorage.release` and `osdVolumeStorage.release` were line-for-line
the same sequence: terminate the helper container (`StopTimeout(time.Second)`,
tolerating a missing container), clear it on success, return when the volume is
already marked removed, open a Docker client, inspect the volume, treat not
found as removed, validate the identity and ownership, remove without force and
mark removed, and join the client's `Close` error. They differed only in the
fields, the validation function and the nouns in the error text. They are now
`ownedStorageRelease.run` (`ceph/osd_storage_release.go`) with those parts
passed in. `TestOSDStorageReleaseSemanticsAreIdenticalForMemoryAndVolume` runs
twelve scenarios against both storages (clean, helper termination fails, helper
already gone, no helper, volume already removed, volume missing, inspect fails,
foreign volume, remove fails, remove not found, close fails, client fails), each
with a failing first call and a clean retry, and requires the same error text
after the two nouns are normalized, the same errors, the same helper and removal
state, the same count of Docker calls and terminations and no forced removal.
The creation, freshness, discard and check paths of the two storages differ in
their semantics (a tmpfs keeper with a size and a start identity, versus a
volume with a creation identity and symlink checks) and stay separate.

## Not covered by this slice

The handle registry (`osds`, `detachedOSDs`), the admin-socket `run` volume, the
other components of the table (client and auth, overrides and faults, the two
other test seams, membership and control), any change to which lock guards what,
any new lock, the order in which `Terminate` closes the other components, the
caller chain beyond the functions listed above (read from source, enforced only
for the functions the caller guard names), and any exported API. No Docker or
native Ceph behavior was run.
