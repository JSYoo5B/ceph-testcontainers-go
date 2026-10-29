# Admission lifetime of the begin*Operation family

Component record for Step 2 of [filesystem-decomposition.md](../filesystem-decomposition.md)
(issue #313), narrowed as the owner's comment of 2026-10-29 on #313 asks: the
shared part owns the acquisition, timeout and release lifetime only. Inspected at
`fb2e760`; citations name the files of this change. Internal to package
`cephfs`: no exported name, signature, error text or catalogue row changed.

## What it is

`Filesystem.admitOperation` (`cephfs/admission.go`) does what the three flows
`beginSubvolumeOperation`, `beginDataPoolOperation` and `beginSessionOperation`
each wrote out:

1. refuse a canceled context, then a nil handle or cluster
   ("CephFS filesystem is unavailable");
2. take `setupMu` context-aware (`cephfs/admission.go:49`), optionally recheck
   cancellation, then take the cluster topology mutex (`:58`). The order is
   `setupMu` before the cluster mutex, as in the parent record's R1
   (`cephfs/entry.go:100-102`);
3. read the startup timeout, run the operation's own `check`, release the
   cluster mutex (`:64`);
4. on a refusal or error release `setupMu` and return a no-op `done`;
5. on success create the timeout context (`:73`) and return a `done` that
   cancels it and releases `setupMu`.

An `admissionSpec` carries the three things that differ between the flows and are
not predicates: `recheckCanceled` (subvolume and data pool yes, session no), the
refusal text, and `check`.

## What stays in each operation

`check` is each flow's own predicate, run under both locks, and the flow keeps
its post-admission work:

| Flow | Predicate in `check` | After admission |
| --- | --- | --- |
| subvolume | not closed, registered under `config.Name` as this handle, `FilesystemName == config.Name`, native identity present, then the control container must exist | reads native pools, ensures the `volumes` module, returns the FSMap id |
| data pool | not closed, registered under `config.Name` as this handle, native identity present (no `FilesystemName` check), then the control container must exist | nothing |
| session | not closed, registered under `config.Name` as this handle, `FilesystemName == config.Name`, native identity present | returns the FSMap id read under the lock |

There is deliberately no shared "valid filesystem" Boolean: the data-pool column
differs from the other two, and `MDSStatus` follows `FilesystemName` and takes no
ownership admission at all. Which name is authoritative is a separate owner
decision (proposal P2 of the parent record); this change does not decide it.
`admitOperation` itself reads no registry, identity or exported field.

## What did not change

- Lock order and the lifetime of each lock: `setupMu` is held for the whole
  operation and released by `done`; the cluster mutex is held only for the
  predicate, the timeout read and the control selection. No lock-held I/O section
  was shortened or lengthened (R3 of the parent record).
- No new mutex. `TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal` also
  pins that `admission.go` imports no `sync`.
- The roster and its cleanup ownership are untouched.
- `recheckCanceled` is kept per flow. `clusterutil.LockMutex` already tests the
  context after a successful `TryLock`, so no test can tell the recheck apart
  from that; it is preserved only because the change is not allowed to drop it.
- Nothing is claimed about concurrent access to `config` (F5).

## Before and after

`structmetrics delta -packages cephfs -base-ref fb2e760`, and line spans from the
source. This is relocation, not removed complexity.

| Item | Before | After |
| --- | ---: | ---: |
| `Filesystem` exported / private methods / fields | 40 / 72 / 14 | 40 / 73 / 14 |
| `beginSubvolumeOperation` lines | 48 | 30 |
| `beginDataPoolOperation` lines | 38 | 17 |
| `beginSessionOperation` lines | 29 | 18 |
| `admitOperation` lines (new) | | 35 |
| Places that take `setupMu` then the topology mutex for this family | 3 | 1 |
| Package `cephfs` private methods / fields / owners | 93 / 348 / 66 | 94 / 351 / 67 |

The three predicates, the three refusal texts and the two post-admission
sequences are all still there. `Filesystem` gains one private method and no
field; `admissionSpec` adds an owner with 3 fields and no method.

## Guards (Docker-free, `cephfs/admission_test.go`)

- `TestAdmissionKeepsEachOperationsOwnNameAuthority`: the owner's
  characterization with a synthetic FSMap of an owned filesystem41 and a sibling99.
  The original descriptor reports the owned daemon; with only `FilesystemName`
  changed to the sibling, `MDSStatus` follows it while the stored config and
  native identity stay; the session admission refuses; the data-pool admission is
  admitted with no native command; restoring the name admits the session again.
  It also passes against `fb2e760` before the change. It shows the admission
  prefix only, not that a whole data-pool operation succeeds.
- `TestAdmissionKeepsRefusalTextsAndNoOpDone`: the three refusal texts, the nil
  handle text, cancellation, and that no gate is retained after any refusal.
- `TestAdmissionLifetimeHasOneOwnerAndKeepsPredicatesLocal`: the caller table
  (`admitOperation` only from the three flows), the lock acquisitions of the
  family (`setupMu`, then the topology mutex, only inside `admitOperation`), no
  direct unlock in a flow, exactly one own `check` per flow, and no predicate
  state read in `admitOperation`.

Negative controls (each applied to a copy of the source, run, reverted): adding
the session `FilesystemName` predicate to data-pool admission fails the
characterization (owner's counterfactual 1); making `MDSStatus` use `config.Name`
fails it (counterfactual 2); removing the session predicate fails it; changing a
refusal text fails the text test; reading the registry in `admitOperation`, a
direct unlock in a flow, and taking the topology mutex before `setupMu` each fail
the AST guard.

## Not covered

No Docker or native Ceph ran; nothing here is native evidence. The registry
predicate sites outside the family (`ScaleMDS`, `startMDSWithAdmission`, the
stopped-MDS and replacement owner checks), the subvolume/auth flows that call
`beginSubvolumeOperation` (29 call sites, unchanged), the embedded `Container`,
public handles, F1, F5 and F6 are untouched.
