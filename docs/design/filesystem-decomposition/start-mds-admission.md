# startMDSWithAdmission phases

Component record for the next bounded slice of Step 2 in
[filesystem-decomposition.md](../filesystem-decomposition.md) (issue #313), after
[admission-lifetime.md](admission-lifetime.md) and
[setup-topology-lock.md](setup-topology-lock.md). Inspected at `017a307` (main
after #319); citations name the files of this change. Internal to package
`cephfs`: no exported name, signature, error text or catalogue row changed.

## Why this site, and what was not shared

`startMDSWithAdmission` (`cephfs/filesystem.go:461`) was a 98-line method that
interleaved three start modes (ordinary, first cold MDS, explicit replacement)
across three phases that differ in lock state. It does not share a prefix with
another operation, so nothing is deduplicated here. The change only names the
phases so each can be read, tested and pinned on its own. A shared
"valid filesystem" step was deliberately not attempted: its ownership refusal
(registry membership by `FilesystemName`, F1) is a different predicate from the
cold, replacement and stopped-MDS owner checks, and merging them would change
which edits are refused.

The split was judged safe because the phases communicate only through values
(`mdsStartAdmission`, the keyring, the started handle), and every early topology
release, the reservation order and the partial-failure ownership stay inside the
phase that had them.

## The phases

| Phase | Function | Lock state |
| --- | --- | --- |
| Admission | `admitMDSStart` (`cephfs/filesystem.go:499`) | takes the topology mutex (`:501`), releases it on each of its seven returns and after reserving the index (`:543`); `setupMu` is held by the caller (R1, `cephfs/entry.go:100-102`) |
| Credentials | `createMDSCredentials` (`:552`) | none |
| Service start | the caller-supplied `start`, still in `startMDSWithAdmission` | none |
| Publication | `publishStartedMDS` (`:579`) | takes the cluster mutex (`:584`), publishes (`:587`), releases once |

`admitMDSStart` keeps, in the same order: the cluster-open and registry-membership
refusal, the name and customizer reads, the cold ownership check, control
selection, the selected-control check, the last cancellation check, consumption of
cold permission (`attempted = true`, `:533`), the replacement `admit` hook and the
index reservation. `createMDSCredentials` keeps the three failure texts
(replacement, initial, ordinary `create mds.<id> credentials`). `publishStartedMDS`
publishes only a real handle, sets the embedded `Container` only if unset, and runs
the `published` hook under the mutex. The post-start result classification
("start initial MDS service", "start replacement MDS service", the returned
partial daemon) stays in `startMDSWithAdmission`.

## What did not change

- Lock order and every lock-held region: the admission section, the unlocked auth
  command and service start, and the publication section are the same spans as
  before (R3 of the parent record). Early release on refusal is unchanged.
- No new mutex. The consumption of cold permission and the index reservation are
  still the single places that do those things.
- The roster writer pins moved with their code: `reserveIndex` is now pinned to
  `admitMDSStart` and `publish` to `publishStartedMDS`, each still under both locks.
- F1 (the registry predicate keys on `FilesystemName`), F5 and F6 are untouched.

## Before and after

`structmetrics delta -packages cephfs -base-ref origin/main`, and line spans from
the source. Relocation, not removed complexity.

| Item | Before | After |
| --- | ---: | ---: |
| Package `cephfs` private methods | 95 | 98 (all on `Filesystem`) |
| `startMDSWithAdmission` lines | 98 | 31 |
| New `admitMDSStart` / `createMDSCredentials` / `publishStartedMDS` lines | | 48 / 20 / 18 |
| New value type `mdsStartAdmission` fields | | 4 |
| Package `cephfs` owners / fields | 72 / 372 | 73 / 376 |

The three methods plus the new type add up to more lines than they remove. The
control-flow complexity of the admission phase is moved, not reduced.

## Guards (Docker-free, in `cephfs/start_mds_admission_test.go`)

- `TestStartMDSAdmissionKeepsEachModesRefusalsAndOwnership`: the ownership refusal,
  the cold identity and embedded-container refusals, selected-control drift and
  read failure, cancellation before permission is consumed, single consumption of
  cold permission, the replacement admission hook (failure and nil control: index
  not reserved) and the three credentials failure texts, each with no retained
  topology mutex. It passes against `017a307` before the split (first commit).
- `TestStartMDSPublicationAndResultClassificationPerMode`: typed-nil handles are
  not published, the embedded `Container` is not replaced by a later daemon, the
  `published` hook runs once with the returned daemon, and each mode's start
  failure text and returned daemon (ordinary returns the partial handle with the
  native error, cold returns none, replacement returns it). Same baseline.
- `TestStartMDSWithAdmissionHasThreePhasesInOrder`: caller table, phase order
  (admission, credentials, start, publication), lock counts per phase, one
  consumption of cold permission placed after the last cancellation check of the
  admission phase, no lock or registry read in the credentials phase, no
  `setupMu` acquisition and no mutex type in any of them. The existing roster lock
  guards cover the moved writers.

Negative controls (applied, run, reverted): leaking the topology mutex on a cold
refusal, moving the consumption of cold permission before the cancellation check
(only the AST guard sees this; see below), dropping the index reservation,
changing the cold credentials text, dropping the replacement flag, running
credentials after the start, publishing a nil handle, a registry read in the
credentials phase and a second caller of the admission phase each fail a test.

One guard is order-only: no fixture isolates the final cancellation check of the
admission phase, because a canceled context already fails the selected-control
read before it. It is pinned by position, and stays as defense in depth.

## Not covered

No Docker or native Ceph ran. Still open: the cold-start and stopped/replacement
owner predicates, the 29 `beginSubvolumeOperation` call sites and the other
admission families, F1, F5, F6, Step 4 and the public-handle decisions.
