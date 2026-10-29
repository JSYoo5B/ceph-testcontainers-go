# Responsibility inventory and decomposition design for cephfs.Filesystem

> Current boundary: the shared-cluster/tenant harness and its core-only policy,
> lease and hook plumbing have been removed. Harness references and source
> locations below are historical design evidence, not retained APIs or future
> extraction requirements. See [independent fixtures](../guides/remove-shared-harness.md).

This note is the design record that follow-up item 2 of #233 asks for: a
responsibility inventory of `cephfs.Filesystem` and a decision framework for
which boundaries deserve an internal component and which, if any, deserve a
public handle. It is the analogue of
[container-decomposition.md](container-decomposition.md) (#146) for the CephFS
package. It changes no code, no public signature, no catalogue and no
dependency.

Status (issue #313): Step 1 is delivered, and the stopped-MDS decoder slice of
Step 3 is delivered. [Refresh at fb2e760](#refresh-at-fb2e760-issue-313) reconciles
this record with main and lists what remains. The sections before it keep the
findings of their snapshots; a count or citation there is historical unless the
refresh restates it.

Every shape below is a proposal. The final boundaries and any public API are
not decided by counts. Each extraction step gets its own child issue with exact
signatures, call-site counts, caller migration and acceptance evidence before
code changes. The owner decisions this note needs are listed under
[Open questions](#open-questions-for-the-owner), and the recommendation is
marked in [Recommendation](#recommendation).

## Method and limits

- Source snapshot: `main` at `134a7524a499b0a596b86d8868977bc73a1da3ab` (merge
  of #239). All `path:line` citations are at that commit. The line numbers of
  the functions that #233 quotes at `7df1094` (`ScaleMDS`,
  `decodeStoppedMDSScope`, `readMDSReplacementScope`) are unchanged.
- Counts come from two throwaway programs built on the standard `go/parser`
  and `go/ast` packages, run over the non-test files of package `cephfs`
  (`*_test.go` skipped, no generated files in the package). The first counts
  `FuncDecl` values whose receiver is `Filesystem` or `*Filesystem`, split by
  exported and unexported name, and counts the fields of the `Filesystem`
  struct (an embedded field counts as one). The second walks each such method
  body and records which `Filesystem` fields and which other `Filesystem`
  methods it names through the receiver variable. That is a direct, syntactic
  measurement: a method that only calls other methods shows no field access,
  and aliasing through a local variable is not followed. Function spans are
  end line minus start line plus one. The programs are not committed.
- Caller counts use `grep` by method name over `internal/integration` and are
  upper bounds, because they do not resolve the receiver type.
- Source reading and static analysis only. No Docker, no native Ceph and no
  timing, race or lifecycle behavior was run, so nothing here is native
  evidence and no unit or race test was executed for this note.
- Facts about what Ceph prints (FSMap field names, daemon state names) were
  checked against the pinned Ceph source (`7f793731f1b39eb4f465e960113d2363c311b964`,
  Tentacle 20.2.4), file and function named where used.

## Measurement

| Item | #233 at `7df1094` | This note at `134a752` |
| --- | --- | --- |
| Exported methods declared on `*Filesystem` | 40 | 40 |
| Unexported methods declared on `*Filesystem` | 72 | 72 |
| Total | 112 | 112 |
| Fields (15 named plus the embedded `testcontainers.Container`) | 16 | 16 |
| Non-test files in `cephfs` | 21 | 21 |
| Non-test lines in `cephfs` | 7,045 | 7,047 |
| Test files / lines / `Test` functions in `cephfs` | not recorded | 24 / 7,657 / 176 |

The counts match the issue, so the baseline did not drift between the two
snapshots. They are a baseline for review, not acceptance targets.

Direct field access of the 112 methods (second program; categories are
exclusive, in the order shown):

| Direct access | Methods | Meaning |
| --- | ---: | --- |
| Roster or cold state: `mdss`, embedded `Container`, `nextMDSIndex`, `mdsOpts`, `coldMDS` | 17 | `MDSs`, `mdsSnapshot`, `ScaleMDS`, `startMDSWithAdmission`, `retireMDS`, `addMDSReplacement`, `completedMDSReplacementCohort`, `checkMDSReplacementOwner`, `checkMDSReplacementPartial`, `captureColdMDS`, `scaleColdMDS`, `verifyColdMDS`, `checkColdMDSAuth`, `checkColdSelectedControl`, `removeStoppedMDS`, `checkStoppedMDSOwner`, `decodeStoppedMDSScope` |
| Handle-registry state: `pinOverrides`, `sessionTimeouts`, `scheduledPaths` with `snapshotScheduleMu` | 3 | `temporaryPin`, `OverrideSessionTimeouts`, `AddSnapshotSchedule` |
| `nativeIdentity` and nothing mutable besides | 10 | `captureNativePoolIdentity`, `readNativePools`, `beginDataPoolOperation`, `AddDataPool`, `checkSubvolumeDataPool`, `checkSubvolumeCreationPool`, `RemoveUnusedDataPool`, `beginSessionOperation`, `captureMDSIdentity`, `beginSubvolumeOperation` |
| Only `cluster`, `config`, `FilesystemName`, `MetadataPool`, `DataPool` | 51 | Native command wrappers and operations on a handle |
| No direct field access (they delegate) | 31 | Handle validators and thin exported readers |

So 82 of the 112 methods touch no mutable `Filesystem` state directly, and
only 30 touch the roster, cold-start, identity or handle-registry fields. The
method count measures how many operations hang off the type more than how much
state it owns. That is the main difference from `Container`, which carries 58
fields next to 245 methods. The consequence is that moving fields is a small
part of the work here, and the larger part is admission, locking and the
native-state decoders.

## Inventory of Filesystem state

`Filesystem` (`cephfs/filesystem.go:57-76`) is one of the objects the cluster
owns for a filesystem. The cluster registry holds it by name
(`filesystemRegistry.byName`, `cephfs/entry.go:97-103`, created lazily by
`filesystems(c)` at `cephfs/entry.go:107`). Two locks and one leaf lock guard
its state; [Locks and admission](#locks-and-admission) gives their order.

`c.mu` below is the cluster mutex (`servicehook.Lock`/`LockTopology`,
`ceph/service_hooks.go:30` and `:169`, `ceph/topology.go:380`). `setupMu` is the
per-cluster registry mutex (`cephfs/entry.go:98-100`).

| Field | Kind | Written at | Guard of writes | Readers and their guard |
| --- | --- | --- | --- | --- |
| `FilesystemName`, `MetadataPool`, `DataPool`, `AdditionalDataPools` | Exported descriptive copies | Constructed before publication (`cephfs/filesystem.go:141-147`) | None after construction. A caller can edit them | Both names are in wide use: 55 textual lines mention `FilesystemName` and 99 mention `config.Name`. `ScaleMDS` keys the registry and issues its `fs set` commands with `FilesystemName` (`cephfs/filesystem.go:279`, `:332`, `:373`), `MDSStatus` parses with it (`:662`), and the session readers use it (`cephfs/session.go:195`, `:276`). The `begin*` flows key by `config.Name`. `checkStoppedMDSOwner` compares the exported copies with the private `config` (`cephfs/stopped_mds.go:188-190`) |
| `cluster` | Back reference | Constructor (`cephfs/filesystem.go:143`) | Immutable | Everywhere, unlocked |
| `config` | Private `Config` copy | Constructor (`:143`); replaced as a whole by `ScaleMDS` (`:326`) | `c.mu`, with `setupMu` held for the whole of `ScaleMDS` (`:268`) | `c.mu` readers (`WaitReady` `:681-685`, `checkStoppedMDSOwner`, `decodeStoppedMDSScope`, `checkMDSReplacementOwner`), `setupMu`-only readers (every `begin*Operation` flow, `readNativePools` `cephfs/data_pools.go:93`), and readers with neither lock (see finding F5) |
| `mdss` | Owned `[]*MDS`, creation order | Publish (`cephfs/filesystem.go:525`), `retireMDS` (`:583`), `removeFilesystem` (`cephfs/removal.go:67`), `removeStoppedMDS` (`cephfs/stopped_mds.go:179`) | `c.mu`, and `setupMu` at all four writers | `MDSs` (`servicehook.Lock`, `:229`), `mdsSnapshot` (`LockTopology`, `:241`), `filesystemRegistry.ConfigTargets` (`cephfs/entry.go:126`, called by `ceph/topology.go:547`), owner and cohort checks under `c.mu` |
| Embedded `Container` | First owned MDS container, for `Start` compatibility | Publish (`:526-528`), `retireMDS` (`:584-589`, from `fs.mdss[0].Container`), `removeFilesystem` (`cephfs/removal.go:69`), `removeStoppedMDS` (`cephfs/stopped_mds.go:180-182`, from `identity.container`) | `c.mu` and `setupMu` | Exported: callers use the promoted methods without any lock. Internally read under `c.mu` (`ScaleMDS` `:279`) |
| `mdsOpts` | Customizers for every MDS | Constructor (`:143`, cloned) | Immutable | `startMDSWithAdmission` under `c.mu` (`:454`) |
| `nextMDSIndex` | Next daemon name index | `startMDSWithAdmission` (`:488`) | `c.mu`, with `setupMu` held by every caller of that function (`startFilesystem` `:119`, `ScaleMDS` `:268`, `addMDSReplacement` `cephfs/mds_replacement.go:64`) | Under `c.mu` (`:453`, `cephfs/mds_replacement.go:108`) or under `setupMu` alone (`cephfs/no_initial_mds.go:175`, `:196`) |
| `nativeIdentity` | `*cephFSNativeIdentity`: FSMap id, pool ids, attachments map | Once, by `captureNativePoolIdentity` (`cephfs/data_pools.go:75-78`) | `c.mu` (and `setupMu` held by `startFilesystem`). Pointer immutable afterwards. Its `attachments` map is edited under `setupMu` (`cephfs/data_pools.go:284`) | 22 non-test lines in 7 files, mostly validity predicates |
| `coldMDS` | `*cephFSColdMDS`: no-initial-MDS authority (`cephfs/no_initial_mds.go:23`) | Once, `captureColdMDS` (`:70`); its `attempted` flag is set by `startMDSWithAdmission` (`cephfs/filesystem.go:478`) | `c.mu` | `ScaleMDS` (`:275`), `scaleColdMDS`, `startMDSWithAdmission` |
| `pinOverrides` | Active pin override per directory and field | `temporaryPin` (`cephfs/pin.go:166-168`), `Restore` (`:217`, `:234`) | `setupMu`, through `beginSubvolumeOperation` | `cephfs/pin.go` only |
| `sessionTimeouts` | Active session-timeout override | `OverrideSessionTimeouts` (`cephfs/session.go:120`), `Restore` (`:171`) | `setupMu`, through `beginSessionOperation` | `cephfs/session.go` only |
| `snapshotScheduleMu`, `scheduledPaths` | Directories with a schedule lease | `AddSnapshotSchedule` (`cephfs/snapshot_schedule.go:152-161`), `release` (`:215-219`) | `snapshotScheduleMu` only | `cephfs/snapshot_schedule.go` only |

State that hangs off `Filesystem` but lives in separate records, all mutated
under `setupMu` (the `begin*Operation` family) unless noted:

- `cephFSMDSIdentity` per owned daemon, with `removalAttempted`, `removed`,
  `scope`, `replacement` and `createdByReplacement` (`cephfs/stopped_mds.go:23`),
  and `cephFSMDSReplacement` (`cephfs/mds_replacement.go:20`). Publication and
  the replacement reservation run under `c.mu`
  (`cephfs/filesystem.go:522-532`, `cephfs/mds_replacement.go:153`,
  `cephfs/stopped_mds.go:151`, `:183`).
- `cephFSDataPoolIdentity` per attachment (`cephfs/data_pools.go:40`).
- The identities inside `Subvolume`, `SubvolumeGroup`, `SubvolumeSnapshot`,
  `SubvolumeClone` and `SubvolumeAuthorization` handles. Copies of a handle
  share them, which is what makes `Restore` and removal idempotent across
  copies.

The `MDS` handle already routes into the filesystem. `MDS.AddReplacement`
(`cephfs/mds_replacement.go:43-52`) and `MDS.RemoveStopped`
(`cephfs/stopped_mds.go:80-87`) are exported on `*MDS` and call unexported
`Filesystem` methods through `identity.filesystem`. So for the stopped-MDS and
replacement scope the handle-first shape exists; only the implementation sits
on the filesystem.

`Filesystem` has no termination hook. The cluster owns the MDS containers
through its service map (`servicehook.StartService`, `Service`, `DeleteService`)
and terminates them; the registry entry and the descriptors are not cleared
there. This note does not change that and did not trace the cluster side.

## Locks and admission

Lock inventory:

| Lock | Defined | Held by | Notes |
| --- | --- | --- | --- |
| `setupMu` | `cephfs/entry.go:100` | Filesystem creation, removal, MDS topology changes, replacement, stopped removal, and every `begin*Operation` flow | Documented as "taken before the cluster mutex" (`:98-99`). Not reentrant. Acquired context-aware with `clusterutil.LockMutex` |
| `c.mu` (cluster mutex) | `ceph/service_hooks.go:30`, `ceph/topology.go:380` | Registry reads and writes, roster edits, ownership predicates | Two acquisitions exist: `servicehook.Lock` (blocking, ignores the context) and `servicehook.LockTopology(ctx, c)` (context-aware). `MDSs()` and `Filesystems()` use the first (`cephfs/filesystem.go:229`, `cephfs/entry.go:44`); everything that takes a context uses the second. `mdsSnapshot` exists to bound admission by the context (`cephfs/filesystem.go:235`) |
| `snapshotScheduleMu` | `cephfs/filesystem.go:74` | Only `AddSnapshotSchedule` and `release` | Leaf lock. No other lock is taken or held while it is held (`cephfs/snapshot_schedule.go:152-161`, `:215-219`) |
| `Container.configMu`, `controlMu`, `clientMu`, `nativeMu` | `ceph` | Reached through `servicehook.ClusterIdentity`, `AuthCommand`, `ControlContainer` | Internal to `ceph`; not re-traced here. `c.Ceph` runs the shared-gate check and then the control selection (`ceph/ceph.go:536-549`); `removeFilesystem` and `removeStoppedMDS` call it under `c.mu`, so it does not take `c.mu` (inferred from those calls, not traced) |

Order and admission rules, each pinned to the code:

- **R1. `setupMu` before `c.mu`.** Every path takes `setupMu` first and then
  `LockTopology`: `startFilesystem` (`cephfs/filesystem.go:119`, `:155`),
  `ScaleMDS` (`:268`, `:272`), `removeFilesystem` (`cephfs/removal.go:23`,
  `:27`), `addMDSReplacement` (`cephfs/mds_replacement.go:64`, `:68`),
  `removeStoppedMDS` (`cephfs/stopped_mds.go:99`, `:103`),
  `beginSubvolumeOperation` (`cephfs/subvolume.go:151`, `:158`),
  `beginDataPoolOperation` (`cephfs/data_pools.go:154`, `:161`) and
  `beginSessionOperation` (`cephfs/session.go:253`, `:256`). No path read for this
  note takes `setupMu` while holding `c.mu`. Paths that run under `c.mu`
  (`admit` and `published` of `startMDSWithAdmission`, documented at
  `cephfs/filesystem.go:441-443`) call only owner checks.
- **R2. The public MDS readers do not take `setupMu`.** `MDSs`, `MDSStatus` and
  `WaitReady` take only the cluster lock (`cephfs/filesystem.go:224-233`,
  `:650-663`, `:670-694`). `ScaleMDS` can therefore call `MDSStatus`,
  `mdsSnapshot` and `WaitReady` while it holds `setupMu`
  (`:295`, `:299`, `:422`). The converse trap is recorded in
  `drainFilesystemForRemoval`: it "deliberately does not call the public
  MDSStatus method (which takes them)" because removal already holds both locks
  (`cephfs/removal_shutdown.go:46-48`). Any component that gives `MDSStatus` an
  admission step would break `ScaleMDS`.
- **R3. Short critical sections around I/O, with two exceptions.**
  `startMDSWithAdmission` releases `c.mu` before the auth command and the
  container start (`cephfs/filesystem.go:489-517`) and retakes it to publish
  (`:522-532`). `ScaleMDS` releases it between admission and the loop
  (`:289`, `:327`). The exceptions hold `c.mu` across Docker and native calls:
  `removeFilesystem` for its whole body (`cephfs/removal.go:27-30`, I/O at
  `:40-71`) and `removeStoppedMDS` (`cephfs/stopped_mds.go:103-106`, Docker
  terminate at `:152`). Both also hold `setupMu`. A component must not change
  which of these sections hold the lock.
- **R4. The `begin*Operation` family is the admission contract.** Each takes
  `setupMu`, then `LockTopology`, checks ownership, reads the startup timeout,
  releases `c.mu`, and returns a context bounded by that timeout plus a `done`
  func that releases `setupMu`: `beginSubvolumeOperation`
  (`cephfs/subvolume.go:143-190`, 29 call sites), `beginDataPoolOperation`
  (`cephfs/data_pools.go:146-183`, 3 call sites), `beginSessionOperation`
  (`cephfs/session.go:245-273`, 4 call sites). They differ on purpose or by accident:

  | Step | subvolume | data pool | session |
  | --- | --- | --- | --- |
  | `ctx.Err()` after `LockMutex` | yes (`:154`) | yes (`:157`) | no |
  | Control container must exist | yes (`:165`) | yes (`:168`) | no |
  | Ownership predicate | `byName[config.Name] == fs && FilesystemName == config.Name && nativeIdentity != nil` | `byName[config.Name] == fs && nativeIdentity != nil` (no `FilesystemName` check) | same as subvolume |
  | Native work in the begin | reads pools, ensures the `volumes` module | none | none (returns `nativeIdentity.id`) |
  | Returns | context, FSMap id, `done` | context, `done` | context, FSMap id, `done` |

  `done` cancels the timeout and releases `setupMu`; callers `defer` it. A
  refactor that merges the three must keep each column, or record the change as
  a behavior change.
- **R5. Ownership predicates are not interchangeable.** The registry is read at
  11 sites with different conjuncts: `startFilesystem` rejects an
  existing name (`cephfs/filesystem.go:158`); `ScaleMDS` and
  `startMDSWithAdmission` key by `FilesystemName` (`:279`, `:318`, `:449`) while
  the `begin*` flows key by `config.Name`; `ScaleMDS` requires a non-nil
  embedded `Container` (`:279`) while `scaleColdMDS` requires it nil
  (`cephfs/no_initial_mds.go:87`); `checkStoppedMDSOwner` additionally compares
  the exported fields with `config` and the registered container with the
  identity record (`cephfs/stopped_mds.go:188-190`). The variation encodes
  "which edits of an exported field are refused", so a shared predicate must
  take the variation as parameters.
- **R6. Owner checks run again after every unlocked gap.** `ScaleMDS` repeats the
  full predicate and `completedMDSReplacementCohort` after re-locking
  (`cephfs/filesystem.go:315-325`). The stopped-MDS and replacement flows
  re-read native state and re-run owner checks before and after each Docker
  attempt (`cephfs/stopped_mds.go:99-185`, `cephfs/mds_replacement.go:296-337`).
  Moving code must not drop a recheck.
- **R7. Uncertain outcomes stay owned.** A partial daemon is published as soon
  as a real container handle exists, before the start error is judged
  (`cephfs/filesystem.go:518-533`), and a replacement attempt is cleanup-only
  after the auth or start attempt (`cephfs/mds_replacement.go:18-19`,
  `:203-208`). `retireMDS` removes ownership only after the container is gone
  and the daemon left the FSMap (`cephfs/filesystem.go:566-590`).
- **R8. Snapshot-schedule operations are outside the contract.**
  `AddSnapshotSchedule` and the lease methods never call a `begin*Operation`
  and never check `Closed`, registry ownership or the native identity
  (`cephfs/snapshot_schedule.go:122-204`, `:398-492`). They guard only
  `scheduledPaths`. This is a current-state observation. A decomposition must
  not silently add admission, and whether to add it is an owner question.

## Methods by responsibility

Grouping used for the extraction analysis (112 methods; the exported entries of
`MDS` are listed separately):

| Group | Files | Exported | Unexported | State touched directly |
| --- | --- | ---: | ---: | --- |
| MDS lifecycle and scale | `filesystem.go` | 4 (`MDSs`, `ScaleMDS`, `MDSStatus`, `WaitReady`) | 6 | roster, embedded `Container`, `config`, `nativeIdentity` |
| Cold start (no initial MDS) | `no_initial_mds.go` | 0 | 7 | `coldMDS`, roster |
| Stopped-MDS retirement and scope | `stopped_mds.go` | 0 (`MDS.RemoveStopped`) | 6 | roster, `config`, `nativeIdentity` |
| MDS replacement | `mds_replacement.go` | 0 (`MDS.AddReplacement`) | 7 | roster, `nextMDSIndex`, `config` |
| Filesystem removal | `entry.go`, `removal.go`, `removal_shutdown.go` | 1 (`Remove`) | 0 (package functions) | roster, embedded `Container`, `nativeIdentity` |
| Data pools and native identity | `data_pools.go` | 3 | 8 | `nativeIdentity` |
| Subvolume, group and quota | `subvolume.go` | 10 | 9 | `cluster`, `config` |
| Snapshot, clone, schedule, quiesce | `snapshot.go`, `clone_lifecycle.go`, `snapshot_schedule.go`, `quiesce.go` | 11 | 13 | `cluster`, `config`; `scheduledPaths` |
| Authorization | `authorization.go` | 4 | 6 | `cluster`, `config` |
| Pin policy and override | `pin.go` | 4 | 7 | `pinOverrides` |
| Session limits and listing | `session.go` | 3 | 3 | `sessionTimeouts` |
| Total | | 40 | 72 | |

Notes on the groups the issue names:

- **Client and mount.** `Filesystem` has no client or mount method. Clients
  use libcephfs through `ceph.Container.WithClient` (`cephfs/filesystem.go:55-56`),
  and the FUSE client path is a separate integration batch. `Authorize...`
  creates a `ceph.ClientConfig` through `CreateClient` (`cephfs/authorization.go:115`)
  and `cephfs/tenant.go` creates tenant clients through the tenant port. Both
  are subvolume and tenant concerns, not a mount responsibility of this type.
- **Quota.** There is no separate quota state. It is the `SizeBytes` of a
  subvolume or group (`cephfs/subvolume.go:19`, validated at `:123`) and
  `ResizeSubvolume` / `ResizeSubvolumeGroup` (`:528`, `:560`). It belongs to the
  subvolume group.
- **Status and observation.** 16 exported methods are read-only: `MDSs`,
  `MDSStatus`, `WaitReady` (a poll), `DataPools`, `SessionTimeouts`, `Sessions`,
  `SubvolumeGroups`, `Subvolumes`, `SubvolumeGroupInfo`, `SubvolumeInfo`,
  `SubvolumeSnapshots`, `SubvolumeSnapshotInfo`, `SubvolumeCloneStatus`,
  `SubvolumePinPolicy`, `SubvolumeGroupPinPolicy` and
  `SubvolumeAuthorizedClients`. All but `MDSs`, `MDSStatus` and `WaitReady` go
  through a `begin*Operation` admission and so hold `setupMu` for the read.
- **Mirror hooks.** No `Filesystem` method mentions mirroring, and
  `cephfs/mirror` does not import `cephfs` (its imports are `ceph`, `fault` and
  `internal/multicluster`). Mirror fixtures receive only the filesystem name
  string (`internal/integration/cephfs_multicluster_integration_helpers_test.go:71`
  passes `sourceFS.FilesystemName`) and read the native identity themselves
  (`cephfs/mirror/mirror.go:304`). The only coupling is documented behavior:
  `Remove` asks the caller to stop mirrors first (`cephfs/entry.go:54-56`).
  The package-level hooks that the cluster calls on the registry are
  `ConfigTargets` (`cephfs/entry.go:120`) and `RequiresMGRModule`
  (`cephfs/entry.go:136`), read by `ceph/topology.go:547` and
  `ceph/mgr_modules.go:325` through the interfaces at
  `ceph/service_bootstrap.go:71-77`.
- **Cleanup.** Container termination belongs to the cluster (see the state
  inventory). The filesystem-level cleanup is `Remove` (graceful drain, terminate
  daemons, delete keys, remove pools it created), `retireMDS`, and
  `RemoveStopped`; plus the idempotent `Restore` methods of the override handles.

## Native-state decoders

The package decodes native state at three strictness levels. They are different
acceptance contracts and are not interchangeable.

| Tier | Decoders | Rule |
| --- | --- | --- |
| Lenient | `parseCephFSMDSStatus` (`cephfs/filesystem.go:741`), `cephFSMDSAbsent` (`:627`), `readNativePools` (`cephfs/data_pools.go:82`) | `json.Unmarshal` into a plain struct. Only "filesystems missing" and a few identity comparisons are errors. A missing field decodes to its zero value |
| Pointer-strict | `parseRemovalShutdownState` (`cephfs/removal_shutdown.go:23`) | Every field is a pointer; a missing or negative id, wrong name, missing `max_mds` or `up`, or a malformed `mds_<n>` rank key is an error |
| Strict with duplicate-key rejection | `decodeColdMDSMap` (`cephfs/cold_mds_decode.go`, formerly `validateColdMDSMap`), `decodeStoppedMDSMap` (`cephfs/stopped_mds_decode.go`), `decodeMDSReplacementMap` (`cephfs/mds_replacement_decode.go`, split from `readMDSReplacementScope`) | All via `coldMDSJSON` (`cephfs/no_initial_mds.go`): input limited to 4 MiB, duplicate object keys and trailing documents rejected, raw decode errors never returned (native auth input can contain credentials). Pointer fields so absent differs from zero. Cross-checks: unique names and GIDs, `gid_<n>` keys, `up` versus `info` agreement, policy flags |

Invariants that any change to a decoder must keep, from the code and the
pinned Ceph source:

- The daemon-state allowlist is written out three times
  (`cephfs/no_initial_mds.go:247`, `cephfs/stopped_mds.go:305`,
  `cephfs/mds_replacement.go:379`) with the same twelve names. They are the
  `up:` names of `ceph_mds_state_name`
  (`src/common/ceph_strings.cc:239-265`) except `up:oneshot-replay`. The
  `down:*` names and `???` are also absent. A row in any of those states is
  rejected today. Whether `up:oneshot-replay` is excluded on purpose is not
  documented in the code; the behavior is what is preserved.
- The three row validators are near copies, not equal. The replacement
  validator rejects the original name with a dedicated message before the
  generic malformed check (`cephfs/mds_replacement.go:376-378`), the stopped one
  keys uniqueness by name through a map (`cephfs/stopped_mds.go:305`) and the
  cold one adds a reserved name (`cephfs/no_initial_mds.go:247`). Sharing the
  allowlist as one value is safe if a test pins equality with the three current
  lists. Sharing the validator needs paired controls for each difference.
- `laggy_since` is dumped only while the daemon is laggy
  (`src/mds/MDSMap.cc`, `mds_info_t::dump`), so "key present" is the signal the
  `Laggy json.RawMessage` field encodes (`cephfs/stopped_mds.go:42-45`). The
  `joinable` flag is the inverse of `CEPH_MDSMAP_NOT_JOINABLE`
  (`MDSMap::dump_flags_state`), and the code reads it as dumped.
- Missing, null or wrong-typed fields stay inconclusive errors, never an empty
  value that counts as evidence. `decodeStoppedMDSScope` ends with a
  survivor check that inspects Docker containers
  (`cephfs/stopped_mds.go:396-421`): decoding and survivor verification are
  separate concerns inside one 150-line function (`:274-423`). A split must keep
  the survivor check on the same side of the final scope return.
- High cognitive and cyclomatic scores of these functions (92 and 127 for
  `decodeStoppedMDSScope` in #233) are an investigation signal, not evidence that
  a guard is redundant. Every guard stays unless a failing paired control shows it
  unreachable.

Triage item 3 of #233 owns the internal review of these decoders. This note only
fixes where their boundary sits and in which order they move.

## Cross-package dependencies

| Consumer or provider | Dependency on Filesystem | Consequence |
| --- | --- | --- |
| `cephfs/mirror` | None at the Go level. Receives the filesystem name string and reads FSMap identity itself | Independent of this decomposition. #156 already moved the package; no mirror change is needed |
| `ceph.Observer` (`ceph/observer.go`) | None. It is cluster-scoped, has no filesystem or MDS query, and `ceph` cannot import `cephfs` | A filesystem observer would be a separate type in `cephfs`, not an extension of `ceph.Observer`. Filesystem reads need the `setupMu` admission, which `ceph.Observer` queries never take |
| `ceph.Container` (provider) | `servicehook`: `Lock`/`Unlock`/`LockTopology`/`Closed`/`Service`/`SetService`/`DeleteService`, `StartService`, `ClusterIdentity`, `AuthCommand`, `PoolStates`, `RemovePool`, `PoolPolicyReady` and others (35 distinct functions in `cephfs`) plus about 60 `c.Ceph` calls, 6 `ControlContainer`, 3 `ClientContainer`, 1 `CreatePool`, 1 `CreateClient` | The seam is `internal/servicehook`, which stays. A component takes only the subset it uses, as in the other #146 steps |
| `ceph` extension hooks | `ConfigTargets`, `RequiresMGRModule`, `Bootstrap` registered through `servicehook.WithBootstrap` (`cephfs/entry.go:79`) | They read `byName` and `mdss`. Moving `mdss` changes `ConfigTargets` (`cephfs/entry.go:122-131`) and its caller assumptions about the lock |
| Harness (#155, `cephfs/harness` planned) | `cephfs/tenant.go` does not use `*Filesystem`; it works through `servicehook.TenantPort` and `tenantScript` | The harness move and this decomposition are independent. Neither blocks the other |
| `internal/integration` | 62 files import `cephfs`. Upper-bound file counts by method name: `Remove` 38, `MDSStatus` 22, `MDSs` 20, `WaitReady` 13, `CreateSubvolume` 9, `CreateSubvolumeGroup` 7, `RemoveSubvolume` 7, `Sessions` 6, `SubvolumeInfo` 6, `ScaleMDS` 3; the remaining exported methods are used in 0 to 5 files | A public signature change touches up to 62 test files. An internal component touches none, because they call exported methods |
| Native scenarios for the MDS scope | `TestStoppedMDSRetirementTopology`, `TestLastMDSReplacementTopology`, `TestCephFSMDSScaleTopology`, `TestCephFSMDSScaleStandbyReplayTopology`, `TestNoInitialMDSTopology`, `TestDualStackReplacement` (files `internal/integration/*_integration_test.go`, each with its own `//ci:` header) | These are the native evidence a step on the MDS group has to re-run on its exact head |

## Boundary assessment

A boundary is a meaningful component only if state, operations, lock and cleanup
move together and the result has a narrow dependency list. "Internal" means an
unexported type in package `cephfs` (the shape of `networkState` in
`ceph/network_state.go`, the first #146 internal slice). "Public handle" means an
exported type with its own methods.

| Candidate | State | Operations | Lock | Verdict |
| --- | --- | --- | --- | --- |
| MDS roster | `mdss`, `nextMDSIndex`, `mdsOpts` (the embedded `Container` stays) | publish, retire, snapshot, `ConfigTargets` view | none of its own; `c.mu` for access, `setupMu` for writers | **Internal component, meaningful.** Four writers and one clear guard, like `networkState`. 21 non-test lines in 6 files plus 61 test lines (`mdss` 39, `nextMDSIndex` 22) |
| Embedded `Container` re-seat | embedded `Container` | four sites choose the next first daemon | same | **Do not unify.** `retireMDS` reads the exported `MDS.Container` (`:587`); `removeStoppedMDS` reads the private `identity.container` (`cephfs/stopped_mds.go:181`) and `checkStoppedMDSOwner` rejects an edited descriptor (`:188-190`, `:193-195`). A shared helper would change which edits are refused. Keep the four sites until the embedded field's fate is decided |
| Admission (`begin*Operation` and the registry predicate) | none | `setupMu` + `LockTopology` + predicate + control + timeout | owns the acquisition order | **Internal component, meaningful, medium risk.** 29 + 3 + 4 call sites and 11 registry lookup sites, but the variation table (R4, R5) must be kept parameter by parameter |
| FSMap value functions (status, settle, ready, retirement candidate, absence, shutdown state) | none | pure functions of `[]byte` and values | none | Already free functions. Moving them to their own file changes nothing structural; do it only together with a decoder change |
| Strict global-map decoders (cold, stopped, replacement) | none | decode, then compare with expected identity | none | **Meaningful, owned by triage item 3.** Split decode from survivor verification; keep the three acceptance contracts; share only the state allowlist |
| MDS ownership proofs (`cephFSMDSIdentity`, `cephFSMDSReplacement`, `cephFSColdMDS`, owner and cluster checks, `inspectStoppedMDS`) | identity records | `checkStoppedMDSOwner`, `checkStoppedMDSCluster`, `checkMDSReplacement*`, `captureMDSIdentity` | caller holds `c.mu` | **Internal component, meaningful, higher risk.** 20 methods, about 875 method lines, shared helpers (`coldMDSJSON`, row types). Do it after the roster and admission steps |
| Stateless native readers (subvolume, snapshot, session, pin leaf readers) | none | one native command and one parse | none | **Optional.** Many are `(cluster, name)` functions but they use two name sources (`config.Name` and `FilesystemName`). Low value until the name source is decided (open question 4) |
| Override registries (`pinOverrides`, `sessionTimeouts`, `scheduledPaths`) | already one file and one handle type each | one writer file each | `setupMu` or `snapshotScheduleMu` | **Do not extract.** Already cohesive. A new type would add indirection without removing a coupling |
| Public subvolume handles | identities already inside the handles | 20 exported methods take a handle as the target | n/a | **Owner decision.** See below |
| Public MDS topology handle | n/a | `ScaleMDS`, `MDSStatus`, `WaitReady`, `MDSs` | n/a | **Not warranted now.** `MDS` is already a handle for the two per-daemon operations; a topology object would only rename four methods |
| Filesystem observer (analogue of `ceph.Observer`) | n/a | 16 read-only methods | n/a | **Not warranted now.** All but three reads share the `setupMu` admission, so the type would only forward |

Public-handle question in numbers: 20 of the 40 exported methods take a single
handle as their target (`AuthorizeSubvolume`, `DeauthorizeSubvolume`,
`EvictSubvolumeClients`, `CancelSubvolumeClone`, `RemovePartialSubvolumeClone`,
`RemoveUnusedDataPool`, the four pin methods, `SubvolumeSnapshots`,
`CreateSubvolumeSnapshot`, `RemoveSubvolumeSnapshot`, `CloneSubvolumeSnapshot`,
`SubvolumeCloneStatus`, `WaitForSubvolumeClone`, and the four resize/remove
methods). Handles already carry `identity.filesystem`. Moving them onto the
handles (`volume.Resize(ctx, n)`) would leave about 20 exported methods on
`Filesystem` and follows the container note's use of principle 1 for handle
operations. It is a source-breaking change for up to 62 test files and the docs,
under the direct v0.x policy it needs no alias, and it is the owner's decision
(open question 2). It also does not reduce the 72 unexported methods or any
lock, so it is independent of the internal steps and not a prerequisite for them.

## Relation to existing decisions

- **#146 order.** The container note orders facts, observations, overrides, then
  the internal state split, networking first. For `Filesystem` the facts and
  observation groups are already values or read-only methods, and the override
  handles stay in `cephfs` (the override note says the CephFS pin and session
  overrides "are different operations and are not part of" the Container override
  move, `override-package-boundary.md:85-88`). So the remaining work is the
  internal split, and it follows the accepted shape of the first #146 slice:
  fields move without a new lock, the guard is written on the type, and an AST
  guard test pins single-writer rules.
- **#155 harness packages.** `ceph/harness` and `cephfs/harness` own the tenant
  adapters. `cephfs/tenant.go` does not touch `*Filesystem`, so the filesystem
  decomposition does not block it and is not blocked by it. The gate and the
  admission tables stay in core, and `Filesystem` methods are not entries in
  `sharedMethodPolicies` (their cluster calls go through `c.Ceph`). Whether
  filesystem operations on a shared cluster are refused by that gate was not
  traced here.
- **#156 mirror boundary.** `cephfs/mirror` is a separate package with no import
  of `cephfs`; nothing in this note moves it.
- **0.x no-shim policy.** Any public change in this series is a direct migration
  in one minor release with no deprecated alias and no forwarding method
  (`shared-harness-boundary.md:246`, `override-package-boundary.md:279-283`).
  Internal steps change no exported signature and need no migration.
- **Catalogue and release notes.** An internal step changes no API catalogue
  row under `docs/design/api-naming/cephfs/`. This note adds no fragment under
  `docs/changes/`, as with the earlier design notes: the release-note guide says
  internal refactors need none. A public handle move needs a catalogue move
  record and an `api-break` fragment with a migration link.

## Findings that the extraction has to respect

- **F1. Two sources for one name.** The `begin*` flows and the native readers
  use the private `config.Name`; `ScaleMDS`, `startMDSWithAdmission`,
  `MDSStatus`, `Sessions` and the session readers use the exported, caller
  editable `FilesystemName`. `MDSStatus` has no ownership admission at all
  (`cephfs/filesystem.go:650-663`), so after an edit of the field it would parse
  another filesystem's entry. That was read from the code, not tested. A
  component must keep each method's source or change it deliberately.
- **F2. The embedded `Container` is mutable shared state.** Callers read it
  without any lock; four sites write it under the cluster lock. It is the one
  field that makes a clean "roster owns its state" shape impossible without
  deciding the embed's future (open question 3).
- **F3. Two-key invariant.** Writers of `mdss`, `Container`, `nextMDSIndex`,
  `config` and `coldMDS` hold both `setupMu` and `c.mu`; readers hold at least
  one. The invariant holds for the sites read here but is stated nowhere in the
  code. A component would write it on the type, as `networkState` did.
- **F4. Duplicated state allowlist and admission.** Three copies of the daemon
  state list; three `begin*` functions and 11 registry lookup sites. Both are
  drift risks, and both have differences that must not be flattened.
- **F5. Possible unlocked read of `config`.** `AddSnapshotSchedule` and the lease
  methods read `fs.config.Name` without `setupMu` or `c.mu`
  (`cephfs/snapshot_schedule.go:174`, `:192`, `:224`, `:342`, `:421`, `:473`) while `ScaleMDS`
  replaces the whole `config` struct (`cephfs/filesystem.go:326`). The `Name`
  value never changes, but the assignment writes the struct. No race run was made
  and no test is known that overlaps the two. Storing the name as a separate
  immutable value in a component would remove the question; whether to treat it
  as a defect is the owner's call.
- **F6. `Remove` and `RemoveStopped` hold the cluster lock across I/O** (R3).
  Any change that shortens those sections is a behavior change.

## Proposed extraction order

Each step is internal to package `cephfs`, changes no exported signature and gets
its own child issue. Order is by risk, lowest first, and every step is independent
of the later ones.

**Step 1 (lowest risk): the MDS roster component.**

- Move `mdss`, `nextMDSIndex` and `mdsOpts` into an unexported `mdsRoster`, held
  by `Filesystem`. It owns no lock; every method documents that the caller holds
  `c.mu`, and the writer rule of F3 is written on the type. The embedded
  `Container` stays on `Filesystem`, and its four re-seat sites stay as they are
  (finding F2). `MDSs`, `mdsSnapshot` and `ConfigTargets` read through the
  roster.
- Not covered: the `begin*` admission, the decoders, the embedded `Container`,
  any lock, any exported method.
- Invariants: R1 to R3 unchanged; `MDSs` still uses the blocking lock and
  `mdsSnapshot` the context-aware one; creation order and descriptor identity are
  kept, including retained cleanup handles; `ConfigTargets` returns the same map;
  partial real handles are published and typed-nil handles are rejected as today (R7).
- Acceptance evidence: `go vet`, `make check`; existing unit tests unchanged apart
  from field references (61 test lines move); an AST guard test, modeled on
  `TestNetworkStateRemovalFlagsHaveOneWriter`, that fails when a function outside
  the four writers assigns the roster fields, shown to fail against a deliberately
  added writer; `TestCephFSContextMDSSnapshotPreservesOwnedDescriptors`
  (`cephfs/context_admission_test.go:189`) and
  `TestStoppedMDSWholeClusterCleanupRetainsPartialAndAmbiguousHandles`
  (`cephfs/stopped_mds_test.go:705`) pass unchanged. Native evidence is not
  required for a pure field move, but if the owner wants it, the scale and
  stopped-MDS scenarios above run on the exact head, and are claimed only for
  that head and attempt.

**Step 2: shared admission.**

- One unexported admission helper that implements the common prefix (cancel
  check, `setupMu`, `LockTopology`, `Closed` plus registry membership, timeout,
  release) with the variation of R4 and R5 as explicit parameters or small named
  predicates. The three `begin*` functions keep their names and their results.
- Invariants: lock order R1, the `done` contract (releases `setupMu` once, cancels
  the timeout), the three columns of the R4 table, and the key used for the
  registry lookup per site.
- Acceptance evidence: paired controls per site for each predicate conjunct
  (exported-field edit refused, closed cluster refused, replaced registry entry
  refused) failing against a deliberately flattened version;
  `TestCephFSContextPublicAdmissionDeadline`, `...SetupAdmissionDeadline`,
  `...PinAdmissionDeadline` and `...CanceledFreeAdmissionHasNoEffects`
  (`cephfs/context_admission_test.go:21`, `:88`, `:147`, `:116`) unchanged.

**Step 3: decoder boundary (coordinated with triage item 3).**

- First a single shared state-name list used by the three strict decoders, with a
  test that pins it to the current twelve names and that rejects
  `up:oneshot-replay`, a `down:*` name and an empty string in each of the three.
  Then split `decodeStoppedMDSScope` into a decode-and-compare function that takes
  explicit identity and policy values and returns the scope, and the survivor
  verification that keeps its Docker inspection.
- Invariants: the three strictness tiers stay; `coldMDSJSON` stays the entry for
  the strict tier (duplicate keys, trailing data, size limit, no raw errors); no
  guard is removed.
- Acceptance evidence: the existing rejection tables
  (`TestStoppedMDSRejectsMalformedGlobalMaps` `cephfs/stopped_mds_test.go:485`,
  `TestStoppedMDSDecoderDoesNotPanicOnMissingNativeRow` `:726`,
  `TestMDSReplacementRequiresExactFailedRankAndGlobalAbsence`
  `cephfs/mds_replacement_test.go:272`, `TestNoInitialMDSStrictNativeAbsenceSchema`
  `cephfs/no_initial_mds_test.go:352`, `TestRemovalShutdownMap`
  `cephfs/removal_shutdown_test.go:18`) pass unchanged; for each existing guard a
  paired missing, null, wrong-type and empty control; a mutation check that
  removing any one guard in the new function fails a named case.

**Step 4: MDS ownership proofs.**

- Move the identity records and the owner, cluster and replacement checks
  (`stopped_mds.go`, `mds_replacement.go`, the identity part of
  `no_initial_mds.go`) behind one internal type that receives the roster, the
  admission helper and a decoder interface from the earlier steps. `MDS.RemoveStopped`
  and `MDS.AddReplacement` keep their signatures.
- Invariants: R3 (the cluster lock stays held across the Docker terminate), R6
  (recheck before and after every gap), R7 (attempted means cleanup-only), and the
  exported-field-edit refusals of `checkStoppedMDSOwner`.
- Acceptance evidence: the 18 `Test` functions of `cephfs/stopped_mds_test.go`
  and the 17 of `cephfs/mds_replacement_test.go` pass unchanged;
  the native scenarios `TestStoppedMDSRetirementTopology`,
  `TestLastMDSReplacementTopology` and `TestDualStackReplacement` run on the exact
  head of that PR, reported for that head and attempt only.

**Step 5 (separate decision): public handles and the embedded field.** Not
scheduled by this note; see open questions 2 and 3.

Not planned: the override registries, the stateless readers (until open question
4 is answered), and `Remove`/`removal*.go`, which already form one cohesive
cleanup unit.

## Behavior invariants for every step

- No exported signature, error text, error category (`ErrInvalidRequest` wrapping
  through `invalid`) or documented ordering changes.
- Lock order R1 and the R2 rule that public MDS readers never take `setupMu`.
- Which sections hold `c.mu` across I/O (R3) and which release it.
- Strict acceptance and rejection of every native-state decoder; fields that are
  missing, null or wrong-typed remain errors, never empty evidence.
- Partial resources stay owned and reachable by the cluster's cleanup; attempted
  operations stay cleanup-only; an error never promotes a later readback to
  success.
- Handle copies share identity and `Restore` stays idempotent across copies.
- `FilesystemName`, `MetadataPool`, `DataPool`, `AdditionalDataPools` and the
  embedded `Container` stay exported with the same meaning.
- Each step's tests are the existing tests plus the controls named for that step;
  a new regression test is shown to fail against a deliberately broken version.

## Open questions for the owner

1. Is the decomposition internal only for now? The note recommends that, with no
   public change in steps 1 to 4.
2. Should the 20 handle-targeted operations move onto `Subvolume`,
   `SubvolumeGroup`, `SubvolumeSnapshot`, `SubvolumeClone`,
   `SubvolumeAuthorization` and `DataPool` handles? That is a source-breaking
   change for up to 62 test files and the guides, and needs a catalogue move
   record and an `api-break` fragment. If yes, before or after steps 1 to 4?
3. What is the future of the embedded `testcontainers.Container`? It is mutable
   shared state (F2). Options: keep it as documented compatibility, deprecate it
   under the 0.x policy by removing it in one minor release, or replace it by an
   explicit accessor. The roster cannot own its state cleanly until this is
   decided.
4. Which name is authoritative for native commands and for the exported-field-edit
   refusals: the private `config.Name` or the exported `FilesystemName` (F1)? The
   answer decides whether `MDSStatus` stays without ownership admission.
5. Are snapshot-schedule operations meant to bypass the ownership and `Closed`
   admission (R8, F5), or is that an oversight to fix in its own change?
6. Is `up:oneshot-replay` meant to be rejected by the three strict decoders (the
   allowlist excludes it), or only absent from the list by omission? The note
   preserves the current rejection either way.
7. Should step 3 be a child of this issue or of triage item 3? The note
   recommends item 3 owns the decoder internals and this issue owns only the
   boundary.
8. May `Remove` and `RemoveStopped` keep holding the cluster lock across Docker
   and native calls (F6), or is shortening them a goal? The note assumes they
   stay as they are.

## Recommendation

This section is the recommendation of the note, not a decision.

1. Do the internal decomposition first and keep the public API unchanged through
   step 4. The type's state is small (82 of 112 methods touch no mutable state
   directly); the real coupling is admission, locking and the decoders, and none
   of that needs a public change.
2. Start with step 1, the MDS roster, as the lowest-risk slice: three fields, four
   writers, one guard, no new lock, and an accepted precedent in `networkState`.
   Do not move the embedded `Container` or unify its four re-seat sites.
3. Follow with step 2 (admission) and step 3 (decoder boundary, coordinated with
   triage item 3), then step 4 (ownership proofs) once the earlier steps have
   merged. Each step gets its own child issue and PR.
4. Treat the public-handle question (open question 2) as a separate owner
   decision that does not gate the internal steps, and decide the embedded
   `Container` (open question 3) before step 4 if the owner wants the roster to
   own its state completely.
5. Do not extract the override registries, add a filesystem observer, or create a
   topology handle until a measured need appears.

## Step 1 as implemented: the MDS roster

Inspected at `7266eb7` (main after #247); path:line citations in this section
are at that revision unless they name a new file. The first slice of the
extraction order moved `mdss`, `nextMDSIndex` and `mdsOpts` out of `Filesystem`
into the unexported `mdsRoster` (`cephfs/mds_roster.go`), held as
`Filesystem.roster`. The fields are now `mdsRoster.daemons`, `.nextIndex` and
`.opts`. No exported name, signature, error text or catalogue row changed.

### What was re-verified, and where main differs from the design

Everything below was checked again in the source at `7266eb7` before the move.

- Counts reproduce: 112 methods (40 exported, 72 unexported), 16 fields, 21
  non-test files and 7,047 non-test lines in `cephfs`. A throwaway `go/ast`
  program (not committed) also reproduced 17 methods that touch the roster or
  cold-start fields directly and 3 that touch the handle-registry fields.
- Not reproduced: the "82 of 112 methods touch no mutable state directly". The
  same program finds only 3 methods that name `nativeIdentity` directly
  (`beginSessionOperation`, `beginSubvolumeOperation`, `captureMDSIdentity`),
  not 10. Counting `config` as mutable (`ScaleMDS` replaces it), 65 methods
  touch no mutable field. Not counting it, 89 do. The 82 is between the two and
  could not be derived, so this note does not rely on it. The direction of the
  argument is unchanged: most methods do not own roster state.
- Not reproduced: "21 non-test lines in 6 files plus 61 test lines". `mdss`,
  `nextMDSIndex` and `mdsOpts` occur on 32 non-test lines in 6 files and on 64
  test lines (41, 23 and none) of 7 test files. The `mdss` match in
  `internal/integration/monitor_bootstrap_multicluster_integration_test.go` is an
  unrelated local variable.
- Line drift only: `ConfigTargets` is called at `ceph/topology.go:557` (the doc
  cites `:547`). Its caller `monitorConfigTargets` documents that the topology
  caller holds `c.mu` (`ceph/topology.go:518-519`), and `ConfigTargets` itself
  takes no lock (`cephfs/entry.go:120-131`). That is unchanged.
- Writers confirmed, all under `setupMu` and `c.mu`: `startMDSWithAdmission`
  publishes (`cephfs/filesystem.go:522-532`) and consumes the name index
  (`:488`, after `LockTopology` at `:446`); `retireMDS` removes (`:578-590`,
  `servicehook.Lock`, a blocking acquisition); `removeFilesystem` removes
  (`cephfs/removal.go:67`, locks at `:23` and `:27`); `removeStoppedMDS` removes
  (`cephfs/stopped_mds.go:179`, locks at `:99` and `:103`). `mdsOpts` is written
  only by the constructor (`cephfs/filesystem.go:143`).
- `setupMu` reaches `startMDSWithAdmission` and `retireMDS` from the callers:
  `startFilesystem` (`:119`, calls `startMDS` at `:213` and `captureColdMDS` at
  `:205`), `ScaleMDS` (`:268`, calls `scaleColdMDS` at `:275`, `startMDS` at
  `:368` and `retireMDS` at `:412`) and `addMDSReplacement`
  (`cephfs/mds_replacement.go:64`, calls `startMDSWithAdmission` at `:160`).
- Readers confirmed. Under `c.mu` they are `MDSs` (`servicehook.Lock`, `:229`),
  `mdsSnapshot` (`LockTopology`, `:241`), the `startMDSWithAdmission` reads
  (`:453-458`), `addMDSReplacement` (`cephfs/mds_replacement.go:108`), the owner
  and cohort checks (`:211-266`, `cephfs/stopped_mds.go:188-205`, `:398`), and
  the cold-start admission (`cephfs/no_initial_mds.go:67`, `:87`). Under
  `setupMu` alone, between the two `c.mu` sections, are the two cold-start
  reads of the name index (`cephfs/no_initial_mds.go:175`, `:196`); no writer can
  run then because every writer holds `setupMu`.
- Two things the design did not list: the cold-start admission (`:67`, `:87`)
  also reads the roster length, and `removeFilesystem` iterates a clone of the
  slice while it deletes from the live one (`cephfs/removal.go:54-67`).
- Interactions kept as they are: the cold-start sequence (`captureColdMDS`,
  `scaleColdMDS`, the `coldMDS.attempted` flag set before the auth command); the
  `ScaleMDS` gap between its two `c.mu` sections (`:289`, `:315`); the partial
  daemon that stays published once a real handle exists (`:518-533`); the
  replacement cohort checks of length 1 and 2; the stopped-MDS removal that reads
  `mdss[0].identity.container` after removing the daemon
  (`cephfs/stopped_mds.go:179-182`); and `Remove`, which keeps `c.mu` across
  Docker and native calls.

### What moved and how it is protected

| Field | Now | Writers | Readers |
| --- | --- | --- | --- |
| `mdss` | `mdsRoster.daemons` | `publish` from `startMDSWithAdmission`; `remove` from `retireMDS`, `removeFilesystem`, `removeStoppedMDS` | `members`, `snapshot`, `count`, `at` |
| `nextMDSIndex` | `mdsRoster.nextIndex` | `reserveIndex` from `startMDSWithAdmission` | `issued`, `nextID` |
| `mdsOpts` | `mdsRoster.opts` | `newMDSRoster` only (clone of the caller's slice) | `customizers` (a clone, as before) |

Rules preserved, unchanged from the code they came from:

- A reader holds `c.mu` (`servicehook.Lock` or `LockTopology`), or `setupMu` for
  the two cold-start reads. A writer holds `setupMu` and `c.mu`. The roster
  states this on the type (F3) and takes no lock.
- Order is still `setupMu`, then `c.mu` (R1). `MDSs` still uses the blocking
  lock and `mdsSnapshot` the context-aware one (R2), so `ScaleMDS` can still
  call them under `setupMu`. No section changed which locks it holds across
  I/O (R3). Owner checks still run after every unlocked gap (R6).
- Creation order, descriptor identity and retained cleanup handles are kept:
  `remove` matches by pointer, `snapshot` is a copy, and a partial real handle is
  still published before the start error is judged (R7).
- The embedded `Container` stays on `Filesystem`. Its four re-seat sites
  (publication, `retireMDS`, `removeStoppedMDS`, `removeFilesystem`) were not
  unified (F2). The two that pick the next first daemon now read the roster
  through `at(0)` where they read `mdss[0]` before, and keep their different
  sources (`MDS.Container` and `identity.container`).
- `ConfigTargets` returns the same map and still relies on its caller for the
  lock.

### Why no new mutex

The fields already had a guard that the roster does not change, and the guard is
the pair of locks that all writers take. A lock inside the roster would either
duplicate `c.mu`, which `MDSs` and `ConfigTargets` readers already take, or
introduce a second order against `setupMu` and `c.mu` that `startMDSWithAdmission`
(unlocked around auth and container start) and `removeFilesystem` (locked across
I/O) would have to respect. The package keeps exactly three mutexes
(`setupMu`, `snapshotScheduleMu`, the schedule lease mutex).

### Guards, in `cephfs/mds_roster_test.go`

- `TestMDSRosterFieldsAreOnlyReachedThroughItsMethods`: outside the type,
  `Filesystem.roster` appears only as the receiver of a pinned method, a
  `mdsRoster` literal appears only in `newMDSRoster`, and the `Filesystem`
  literal takes its roster from that constructor.
- `TestMDSRosterMethodsMakeOnlyTheirPinnedCalls`: the method set is pinned, each
  method makes exactly its listed calls (no lock, no call into `Filesystem`),
  and only the three writer methods assign a field.
- `TestMDSRosterWritersAreTheFourPinnedFunctionsUnderBothLocks`: the writer
  methods are called only by the four pinned functions, each after the cluster
  mutex in the same function, and after `setupMu` unless the function is one
  whose callers hold it.
- `TestMDSRosterReadersHoldTheirLock`: every roster read follows a cluster-mutex
  acquisition, or sits in a pinned "owner held" function (the checks documented
  that way, `readStoppedMDSScope`, `ConfigTargets`) or a pinned setup-only
  reader; every reference to such a function follows the lock it relies on.
- `TestMDSRosterAddsNoMutex`: package `cephfs` declares three mutexes and the
  roster none.
- Behavior tests for order and descriptor identity, copies, name reservation, and
  a `-race` run of the public readers against writers under the cluster lock.

The guards read the source lexically. They do not follow a lock across an
unlock or into a callee in another package. The existing tests
(`TestCephFSContextMDSSnapshotPreservesOwnedDescriptors` and
`TestStoppedMDSWholeClusterCleanupRetainsPartialAndAmbiguousHandles` among them)
pass with only their field references changed.

### The `config.Name` observation (F5)

This slice does not touch `config`. The unlocked `fs.config.Name` reads of the
snapshot-schedule methods and the whole-struct assignment in `ScaleMDS`
(`cephfs/filesystem.go:326`) are exactly as before, and the lines in the moved
paths that read `config` (for example `cephfs/mds_replacement.go:264` and
`cephfs/no_initial_mds.go:175`) keep their lock and their order. No race was
shown or claimed.

### Not covered by this slice

Steps 2 to 5: the shared admission, the decoder boundary, the ownership proofs,
and public handles; the embedded `Container` and its re-seat sites; the
`begin*Operation` family and the registry predicates; every lock and every
exported method; `config` and F1, F5, F6. No Docker, no native Ceph and no native
MDS scenario was run, so nothing here is native evidence. Native scale and
stopped-MDS scenarios are for the owner to request on the exact head.

## Refresh at fb2e760 (issue #313)

Source snapshot: `main` at `fb2e76034d132d7d09c846a338b82b76b671928c` (merge of
#311). Citations in this section are at that commit unless they name a file this
change adds. Source reading and Docker-free unit tests only; nothing here is
native evidence.

### Measured with the repository tool

`go run ./internal/tools/structmetrics measure -packages cephfs` (lexical,
untyped; see [Structural metrics](../contributing/structural-metrics.md)):

| Owner | Exported methods | Private methods | Fields | Where |
| --- | ---: | ---: | ---: | --- |
| `cephfs.Filesystem` | 40 | 72 | 14 | `cephfs/filesystem.go:58` |
| `cephfs.mdsRoster` | 0 | 10 | 3 | `cephfs/mds_roster.go:37` |
| `cephfs.MDS` | 2 | 0 | 4 | `cephfs/filesystem.go:47` |
| `cephfs.cephFSMDSIdentity` | 0 | 0 | 13 | `cephfs/stopped_mds.go:24` |
| package `cephfs` (66 owners, 21 files) | 68 | 93 | 348 | |

### Reconciliation with the measurements above

- The method totals match the first snapshot (40 exported, 72 unexported, 112).
  The field count is 14, not 16: Step 1 moved `mdss`, `nextMDSIndex` and `mdsOpts`
  into `mdsRoster` and added `roster` (16 - 3 + 1). That is a relocation of three
  fields behind one, not removal of the state; the roster still holds the same
  three values under the same two locks.
- Historical, not reused: "82 of 112 methods touch no mutable state" was not
  reproducible (see Step 1 as implemented) and is not an acceptance basis. The
  cognitive and cyclomatic scores of `decodeStoppedMDSScope` (92/127), `ScaleMDS`
  (74/54) and `readMDSReplacementScope` (81/101) are pinned observations of an
  older tool run on `7df1094`/`0b6e6c2`. They were not re-measured here: the
  repository has no complexity tool and none was added.
- Admission call sites by textual search over non-test `cephfs` files:
  `beginSubvolumeOperation` 29, `beginDataPoolOperation` 3,
  `beginSessionOperation` 4, and 12 lines that read `byName[`. Unchanged from R4
  and R5.
- R1 to R8 and F1 to F6 were not re-derived. Step 3 touched none of the lock
  paths they cite; the line numbers in them have drifted and are not restated.
- Owner characterization of F1 at `964aaea` (issue #313, comment of 2026-10-29): with
  only the exported `FilesystemName` changed to a sibling name, `MDSStatus`
  follows the sibling while the stored config and native identity stay put, the
  session admission refuses the edited descriptor, and the data-pool admission
  admits it. This is the offline control that a shared admission step must keep
  per operation. It does not establish that a whole data-pool operation
  succeeds, and F5 stays untested.

### Status of the extraction order

| Step | Status | Where |
| --- | --- | --- |
| 1. MDS roster | Delivered | [Step 1 as implemented](#step-1-as-implemented-the-mds-roster) |
| 2. Shared admission | Partly delivered: the acquisition, timeout and release lifetime of the three `begin*Operation` flows, and the setup-gate-then-topology acquisition prefix of four more operations. The per-operation predicates, the other registry-predicate and ownership sites remain; `startMDSWithAdmission` is split into admission, credentials and publication phases without sharing anything | [Admission lifetime](filesystem-decomposition/admission-lifetime.md), [Setup-then-topology prefix](filesystem-decomposition/setup-topology-lock.md), [startMDSWithAdmission phases](filesystem-decomposition/start-mds-admission.md) |
| 3. Decoder boundary | Partly delivered: one shared state list, and the stopped-MDS decoder split from survivor verification | [Stopped-MDS native-state decoder](filesystem-decomposition/stopped-mds-decoder.md) |
| 3, remaining | Delivered: the cold-start decoder is an argument-only function and the replacement decoder is split from its two native reads | [Cold-start and replacement decoders](filesystem-decomposition/cold-replacement-decoder.md) |
| 4. Ownership proofs | Not started | |
| 5. Public handles, embedded `Container` | Owner decision | |

### Proposals that need an owner decision (not approvals)

These are labelled proposals from the implementation side. None changes code in
this slice.

- P1, embedded `Container`: keep it as documented compatibility until a public
  change is chosen; do not unify the four re-seat sites. Unchanged from the
  recommendation above; the decoder slice does not depend on it.
- P2, name authority (F1): keep both sources until the owner decides which is
  authoritative. A shared admission step takes the predicate as a parameter per
  operation, so deciding later does not block it.
- P3, public handles: no signature changes are proposed in the internal slices.
  Moving the 20 handle-targeted operations needs the old-to-new map from the
  Public-handle numbers above and its own issue.
- P4, `up:oneshot-replay`: it is refused today by all three strict decoders and the
  shared list preserves that. Supporting it needs evidence from the pinned Ceph
  source of when a filesystem can report it and a separate semantic decision.
- P5, admission and locking: any change to which section holds `c.mu` across I/O
  (R3) or to the admission predicates stays a separate behavior change.

## Limits of this note

- Source reading and a static parse of method declarations and receiver field
  access only. No Docker, no native Ceph, no race detector and no unit test was
  run for it. Nothing here is native or timing evidence.
- The direct-access table is syntactic. It does not follow aliases or the
  transitive state a delegating method reaches, and the lock claims were read from
  the call sites listed, not proved exhaustively. A method not named here may take
  a lock the note does not mention.
- Caller counts are textual and overstate the number of `Filesystem` call sites.
- The cluster side (`ceph.Container` termination, shared-gate treatment of
  `c.Ceph`, `controlContainerContext` locking) was read only where cited.
- No behavior after a caller edits an exported field was tested (F1, F5).
