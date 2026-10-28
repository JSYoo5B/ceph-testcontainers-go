# Networking state

Moved without rewriting from the section "First internal slice: networking state" of [container-decomposition.md](../container-decomposition.md) when the decomposition records became one file per component (issue #216, owner decision of 2026-10-28). The ordinal is historical: statements below that cite another slice by its number use the [historical ordinals](../container-decomposition.md#historical-ordinals) of the shared document.

Inspected at `8335f47` (main after #238). The first component moved out of
`Container` is the Docker bridge state, because the order above puts networking
first and because it is the smallest group with a single writer per field.

Moved: `network`, `clusterNetwork`, `publicSubnet`, `clusterSubnet`,
`networkRemoved` and `clusterNetworkRemoved` become the unexported
`networkState` (`ceph/network_state.go`), held as `Container.nets`. The
removal block of `terminate` becomes `networkState.removeBridges`, and the
duplicated "list the bridges" loops in diagnostics and native-command admission
become `bridges` and `bridgeNames`. All `Container` methods, their signatures
and their behavior are unchanged.

Why this slice first, and why it adds no mutex:

- Its fields have two kinds of access. The four recorded fields are written
  only while `Run` builds the cluster (`ceph/ceph.go:220-236`) and are read
  without a lock by `FactsOf` (`ceph/cluster_facts.go:47-60`), which
  #238 pinned with an AST write-site guard. The two removal flags are touched
  only by the removal block of `terminate` (`ceph/ceph.go:1152-1165`).
- A new mutex would add a lock to the order and would break the lock-free
  `FactsOf` reads that the merged tests depend on. The first slice therefore
  keeps the existing guards and writes them down on the type.

Lock-order rules preserved, unchanged by this move:

- `terminate` calls `cancelNativeInvocations` before waiting for the mutex,
  takes `lockTopology` (`ceph/ceph.go:1020` at `8335f47`, `1016` at `134a752`) and holds `c.mu` until it
  returns, with one exception: the fault undo phases
  (`ceph/fault_registry.go:193-216`) release `c.mu` around each
  network-shaping restore. `c.mu` is therefore not held continuously through
  teardown. The undo phases have returned, and `c.mu` is held again, when the
  bridges are removed, after every owned container is gone. The removal order
  is the replication bridge first, then the public bridge, and a bridge named
  by a failed interruption undo stays and is retried by the next `Terminate`.
- The removal flags are guarded by `Container.mu`, which `removeBridges` does
  not release. The recorded fields need no lock. `TestNetworkStateRemovalFlagsHaveOneWriter` and the extended
  `TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup` fail when
  another function writes them, when a method of `networkState` writes a
  recorded field, or when `terminate` calls `removeBridges` before it takes
  `lockTopology`.

Not covered by this slice: the other components of the table above (client and
auth, native commands, OSD storage, overrides and faults, test seams,
membership and control), any change to which lock guards what, any new lock,
the `sharedBackend` port that #155 needs (`containerBackend` does not use the
network fields), and any exported API. The `Terminate` order of the other
components stays as recorded in the code and is not re-reviewed here.
