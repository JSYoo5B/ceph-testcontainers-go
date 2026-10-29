# Setup-gate-then-topology acquisition prefix

Component record for the next bounded slice of Step 2 in
[filesystem-decomposition.md](../filesystem-decomposition.md) (issue #313),
after [admission-lifetime.md](admission-lifetime.md) (#316). Inspected at
`aaa0769` (main after #318); citations name the files of this change. Internal to
package `cephfs`: no exported name, signature, error text or catalogue row
changed.

## Which site and why

Outside the three `begin*Operation` flows, four operations wrote out the same
acquisition before their own ownership predicate: `removeFilesystem`
(`cephfs/removal.go:23`), `ScaleMDS` (`cephfs/filesystem.go:271`),
`removeStoppedMDS` (`cephfs/stopped_mds.go:96`) and `addMDSReplacement`
(`cephfs/mds_replacement.go:62`). Each took `setupMu` context-aware, deferred its
release, then took the topology mutex. That prefix is only acquisition, with no
predicate in it, so it is the lowest-risk remaining site.

`lockSetupThenTopology` (`cephfs/setup_topology_lock.go`) is that prefix: it takes
`setupMu` with `clusterutil.LockMutex` (`:24`) and then `servicehook.LockTopology`
(`:27`), the package order (`cephfs/entry.go:100-102`, R1 of the parent record). On
error no lock is held (a topology failure releases `setupMu` before returning).
On success both are held and the caller gets a `release` function for `setupMu`
that it defers.

## What stays in each operation

- The topology mutex release. `removeFilesystem` and `removeStoppedMDS` defer it;
  `ScaleMDS` and `addMDSReplacement` release it at several points of their flow
  and, in `ScaleMDS`, take it a second time later. None of that moved.
- Every ownership predicate and refusal text: the registry membership and
  `Closed` check of `ScaleMDS`, the stale-descriptor refusal of `removeFilesystem`,
  `checkStoppedMDSOwner`, `checkMDSReplacementOwner` and the cohort checks.
- Each operation's own context handling: `removeStoppedMDS` and
  `addMDSReplacement` create their startup-timeout context before locking, the
  other two do not. The helper takes the context it is given.

The helper reads no registry, identity or exported field and has no notion of a
valid filesystem; F1 (which name is authoritative) is neither decided nor
touched.

## What did not change

- Lock order, and the lifetime of `setupMu` (until the function returns) for all
  four. No lock-held I/O section was shortened or lengthened. The only ordering
  difference is where the defer is registered: after both locks are held instead
  of between them, which cannot be observed because nothing runs between.
- No new mutex. `TestSetupThenTopologyPrefixHasOneOwnerAndKeepsPredicatesLocal`
  pins that the helper's file imports no `sync`.
- `startFilesystem` (setup gate only, topology later at `:158`),
  `startMDSWithAdmission`, the cold-start flow, `drainFilesystemForRemoval`, pin
  and session flows were not touched, and `admitOperation` still has its own
  copy because it rechecks cancellation between the two locks.
- Nothing is claimed about F5 (`config` access) or native behavior.

## Before and after

`structmetrics delta -packages cephfs -base-ref origin/main` reports no measured
structural change (no struct owner, method or field is added). Line spans from
the source:

| Item | Before | After |
| --- | ---: | ---: |
| `removeFilesystem` lines | 57 | 55 |
| `removeStoppedMDS` lines | 97 | 95 |
| `addMDSReplacement` lines | 145 | 143 |
| `ScaleMDS` lines | 164 | 162 |
| New helper lines (with doc comment) | | 33 |
| Functions that take `setupMu` then the topology mutex by hand | 5 (the four and `admitOperation`) | 2 (`admitOperation` and the helper) |

Each operation shrinks by two lines and the helper adds more than it saves: this
is relocation, not removed complexity. The roster lock guards
(`TestMDSRosterWritersAreTheFourPinnedFunctionsUnderBothLocks`,
`TestMDSRosterReadersHoldTheirLock`) now treat a call to the helper as taking
`setupMu` and the cluster mutex at that position.

## Guards (Docker-free, in `cephfs/`)

- `TestSetupThenTopologyAcquisitionKeepsItsOrderAndRelease`
  (`setup_topology_lock_test.go`): for each of the four real operations over the
  replacement fixture, a held setup gate returns the deadline error without the
  topology mutex being taken, a held topology mutex returns it with the setup gate
  released again, a canceled context retains neither, and refusals of a stale
  handle release both. It passes against `aaa0769` before the change (first
  commit of this change).
- `TestSetupThenTopologyPrefixHasOneOwnerAndKeepsPredicatesLocal`: the caller
  table (the four functions only), exactly one deferred release and no direct
  `setupMu` unlock in each, the helper's order (`setupMu`, then topology), no
  registry or ownership read and no `sync` import in the helper, and that no
  function other than `admitOperation` and the helper takes both locks itself.

Negative controls (applied, run, reverted): not releasing `setupMu` when the
topology mutex fails, and taking the topology mutex first, each fail the
characterization or the guard; a missing deferred release, a direct unlock in a
caller, a registry read in the helper, a fifth hand-written prefix and a fifth
caller each fail the guard.

## Not covered

No Docker or native Ceph ran. Still open in Step 2: the other registry-predicate
and ownership sites (`ScaleMDS` and `startMDSWithAdmission` predicates, the stopped
and replacement owner checks), the 29 `beginSubvolumeOperation` call sites, F1, F5
and F6, and everything under Steps 4 and 5.
