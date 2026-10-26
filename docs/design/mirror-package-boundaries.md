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
| `Mirror.Terminate` | Same sequence in both families: bounded lock, set the closed flag, run the owned cleanup actions, return their error without touching membership, and only on success drop the daemon list and the initial-daemon handle. The fields (`closed`, `daemons`, `Container`, `owned`, `mu`) belong to each fixture type. | Extracted: `multicluster.TerminateFixture`; each fixture keeps its own fields and passes a callback that clears them |
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

## Follow-up: fixture termination

Issue #144 is closed, but #146 (reducing `ceph.Container`) is still open and
changes the internal dependencies of the mirror families. The package move
therefore stays deferred. The next proven duplication was the fixture-level
`Terminate`, extracted as `multicluster.TerminateFixture` without changing any
exported name, error value, lock order or retry behavior. Deterministic unit
controls cover retry after a failed cleanup (membership kept, closed flag set)
and a lock wait bounded by the context. Existing `rbd` and `cephfs` unit tests
that terminate a fixture were rerun unchanged. No native evidence is claimed.

## Not covered

The decision step for moving packages is deferred as described above. No
CHANGELOG or migration entry is added because no public name, signature or
behavior changes.

## Owner decision and RBD move

The layout question above is settled: the public mirror families live in
service-specific subpackages, `rbd/mirror` and `cephfs/mirror`, with no common
public Mirror abstraction and no new shared framework. The proven shared
mechanics stay in `internal/multicluster`. This change moves the RBD family
only; the CephFS family remains in `cephfs` and is moved by a following change
with its own inventory.

### Old to new (RBD)

Moved files: `rbd/mirror*.go`, `rbd/network.go` (`InterruptPeerLink`) and their
tests (`mirror*_test.go`, `context_lock_test.go`, `wait_context_test.go`) to
`rbd/mirror/`, plus the role-image integration scenario
`TestRoleImagesMirrorRefresh`, whose test name and `//ci:` batch name are
unchanged. Exported names: `RunMirror` becomes `Run`, `RunMirrorWithImages`
becomes `RunWithImages`, `Mirror` is kept, and the `Mirror` prefix is dropped
from `MirrorConfig`, `MirrorMode` (and its two constants), `MirrorScope` (and its
two constants), `MirrorDaemon`, `MirrorDaemonStatus`, `MirrorPoolReplayerStatus`,
`MirrorImageStatus`, `MirrorNamespace`, `MirrorNamespaceState`, `MirrorPolicies`,
`MirrorReceiverStatus` and `MirrorReceiverDaemonStatus`. All 74 members keep
their names, receivers and signature shapes; only the type names inside the
signatures follow the table. The full table is in
[the upgrade guide](../guides/upgrading-rbd-mirror-package.md). Callers: 18 Go
files under `internal/integration` and the unit tests moved with the package.
Docs: `docs/rbd.md` keeps pool, namespace, client and backup reference; the
mirror reference, examples, limitations and pitfalls moved to
`docs/rbd-mirror.md`.

### Parent-owned declarations and bridges

The compile-level measurement above held: the moved files needed the three-line
`invalid` helper (copied as an unexported helper in `rbd/mirror/errors.go`, same
error kind) and `ListNamespaces`, which the mirror package calls through the
exported `rbd.ListNamespaces`. No unexported parent state is shared, and no
method needed to be added to a type declared in the parent.

### Import graph

```
rbd/mirror --> rbd ----------> ceph
rbd/mirror --> internal/multicluster --> ceph, fault, internal/servicehook
rbd/mirror --> internal/clusterutil, ceph, fault
rbd ----------> ceph, internal/multicluster (no import of rbd/mirror)
cephfs -------> internal/multicluster (unchanged, not yet moved)
```

`rbd` does not import `rbd/mirror` (checked with `go list -deps ./rbd`), so the
new edge `rbd/mirror` to `rbd` is acyclic.

### Behavior preserved

Declaration bodies, locking, admission, scoped identity, generation and
lifecycle ownership, cancellation, uncertain outcomes, retryable cleanup and the
independent native data checks are byte for byte as before except for the
renamed identifiers and the package clause. The unit tests moved with their
assertions unchanged. This change adds no native evidence of its own.

### Remaining

`cephfs/mirror` (17 exported types and 32 functions and methods at the
inventory above, 13 caller files, 3 Markdown pages) is not moved here, and no
CephFS name is renamed.
