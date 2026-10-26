# Mirror package boundaries (RBD and CephFS)

This note records the comparison required before moving or merging mirror code,
and the decision it supports. Matching names (`Mirror`, `MirrorConfig`,
`MirrorDaemon`, `RunMirror`, `RunMirrorWithImages`) are not treated as evidence
of shared behavior. Only mechanics that were compared line by line and found to
behave identically were extracted.

Inventory source: the tree this note was written against (merge of #151 into
`main`). Counts are measured at that tree and are not acceptance targets.

## Inventory

| Item | `rbd` | `cephfs` |
| --- | --- | --- |
| Non-test mirror files (`mirror*.go`) | 8 | 8 |
| Lines in those files | 3407 | 4766 |
| Exported types declared there | 13 | 17 |
| Exported package-level functions and methods there | 23 | 32 |
| Mirror unit test files (`mirror*_test.go`) | 9 | 11 |
| Go files calling `RunMirror`/`RunMirrorWithImages` | 16 | 13 |
| Markdown files naming those constructors | 6 | 3 |

Module imports of the mirror files: `ceph` and `internal/multicluster` in both,
plus `internal/clusterutil` in `rbd`. Parent-package dependencies were measured
by compiling the non-test mirror files of each package as a standalone package
in a scratch copy (not committed): `cephfs` needed only the three-line
`invalid` error helper; `rbd` needed `invalid` and `ListNamespaces` from
`rbd/pool.go`. Only `network.go` (`InterruptPeerLink`) in each package attaches
further methods to `Mirror` outside the mirror files. This is a compile-level
measurement, not a design review of the resulting API.

## Mechanics compared

| Mechanic | Finding | Verdict |
| --- | --- | --- |
| Initial daemon count admission | Already shared: `multicluster.NormalizeInitialMirrorDaemonCount`. | Shared before this change |
| Cluster pair validation, setup CLI client, resource ownership, bounded lock, missing-resource handling, peer-link interruption primitive | Already shared in `internal/multicluster`. | Shared before this change |
| Initial daemon name sequence (`a` to `z`, then `node-27` onward) | Identical sequence in both `RunMirror` loops, documented identically. | Extracted: `multicluster.InitialMirrorDaemonName` |
| `MirrorDaemon.Terminate` | Same lock, remembered-success flag, missing-resource handling, nil-container no-op and retry-on-failure semantics. The flag is `terminated` in `rbd` and `removed` in `cephfs`. | Extracted: `multicluster.TerminateOnce`; each daemon keeps its own mutex and flag |
| Daemon name validation | Different rules: `rbd` forbids dots and a leading hyphen, `cephfs` allows dots. Different messages. | Not duplicated |
| `AddDaemon` | Different credentials (per-daemon `client.rbd-mirror.*` against the destination, versus one shared source entity), commands, sockets, startup timeouts, and peer network (`rbd` joins the source network, `cephfs` the destination network). Only the "append customizers, run, register partial result" skeleton is alike. `cephfs` also binds an original-process observer. | Domain-specific |
| `RemoveDaemon` | Parallel shape, but the initial-handle clear is keyed by name in `rbd` and by container ID in `cephfs`, and unknown-daemon errors differ in kind. | Not extracted |
| `Daemons` snapshot | Four lines over different element types. A generic helper would be larger than the code it removes. | Not extracted |
| `InterruptPeerLink` | The shared primitive already exists. The remaining wrapper selects a different network (source versus destination), cluster field, mutex-protected state and error text. | Not extracted |
| Monitor-config refresh | `rbd` refreshes two setup clients and all destination daemons with pool/namespace policy guards. `cephfs` refreshes only source daemons with filesystem identity guards and has a separate peer config-key refresh. Only the two-minute bound and lock are alike. | Not duplicated |
| Readiness, status, enrollment, receiver identity and election (`rbd`); directory, snapshot, peer, MGR reassignment and process quiescence (`cephfs`) | Distinct domain operations with no counterpart in the other family. | Domain-specific |

## Layout alternatives

1. Service-specific `cephfs/mirror` and `rbd/mirror` packages. Technically
   plausible given the compile-level measurement above, but it moves 30 exported
   types (13 + 17) and every constructor and method of each family, changes 29 caller
   files and the naming inventory, and leaves the mirror types needing a
   narrow bridge for `invalid` (and `ListNamespaces` for `rbd`). #144 and #146
   are still reshaping handle signatures and internal dependencies, so a move
   now would be re-done. Not chosen in this change. The benefit (smaller
   `rbd` and `cephfs` package surface) is real and unconfirmed against the
   cost; revisit after #144 and #146 settle, with a package-by-package
   before/after API inventory.
2. Proven shared helpers with the current public layout. Chosen: it removes the
   duplication that was demonstrated and changes no exported signature,
   behavior or import edge.
3. Separate public packages plus a shared internal implementation. Rejected as
   a starting point: the only shared mechanics are small, already live in
   `internal/multicluster`, and a second internal framework would duplicate it.

No generic public mirror abstraction is introduced.

## Import graph

```
rbd ------> internal/multicluster ---> ceph, fault, internal/servicehook
cephfs ---> internal/multicluster
```

The edges are unchanged. `internal/multicluster` imports neither `rbd` nor
`cephfs`, so no cycle is possible from this change.

## Behavior preserved

Exported API, error values, locking order, image admission, release handling,
initial-daemon compatibility handles, daemon identity and cleanup semantics are
unchanged. The unit tests that observe `terminated`/`removed` under the daemon
mutex still exercise the original fields. This change adds deterministic unit
controls only; it has no native Ceph evidence of its own. Native mirror
scenarios exercise the same code paths in CI and are reported separately on the
pull request.

## Not covered

The decision step for moving packages is deferred as described above. No
CHANGELOG or migration entry is added because no public name, signature or
behavior changes.
