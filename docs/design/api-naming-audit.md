# Public naming retention audit

This finishes #14's staged naming pass at the source captured by
[api-naming-review.json](api-naming-review.json). The last selected operation is
`rbd.StartImmutableObjectCache`; its [migration](../guides/upgrading-rbd-cache-name.md)
retains the existing lease and runtime contract. No further rename is selected
for the remaining names in this snapshot.

## Scope and evidence

The declaration catalogue contains **792** functions, declared methods on
exported receivers, types, constants and variables. The separate member
catalogue contains **1086** exported field/nested-record/embedding entries in
**180** types. It scans exported declared interfaces as well; the only one in the
current public packages is `ceph.CaptureSettler`, with two methods. Its synthetic scope test includes interface
methods/embeddings, grouped fields, pointer/private embeddings and nested
anonymous array/map records, and refuses to expose ordinary private fields.

| Package | Declaration entries | Member/embedding entries |
| --- | ---: | ---: |
| `ceph` | 365 | 422 |
| `fault` | 34 | 64 |
| `rbd` | 77 | 94 |
| `cephfs` | 155 | 247 |
| `rgw` | 126 | 204 |
| `capture` | 35 | 55 |
| Total | 792 | 1086 |

This includes the dependency-role migration through #32: `ImageSet`,
`RunWithImages`, `WithImages`, `Images`, `ClientContainerContext` and the
mirror variants, the context-bounded `MonitorsContext` and `OSDsContext`
getters added for #4, and the retained OSD lifecycle (`DetachOSD`, `ReopenOSD`
and `DetachedOSD` with its `ID`, `UUID`, `ClusterFSID` and `Placement`
accessors) added for #6, the test finalization added for #49
(`TerminateOnCleanup`, `Finalization` with `Check` and `Cleanup`, and
`FinalizationConfig` with its eight fields), the opt-in post-test verification
added for #48 (see [Post-test verification](#post-test-verification) below),
the `capture` package added for #52 (see
[Packet capture](#packet-capture-package) below), and the first runtime debug
level window added for #50 (see [Runtime debug level window](#runtime-debug-level-window)
below). It uses the current source, not the earlier 691/692-entry, 707-entry,
712-entry, 745-entry, 768-entry, 787-entry or 788-entry checkpoints. The receipt binds canonical catalogue bytes and per-package counts.
The generation flag cannot update the receipt: a change to captured names or signatures
requires an explicit retention review even after inventory regeneration.
Digest matching establishes scope agreement, not the quality of a naming review.

The review covers module-owned declarations and explicit embedding boundaries.
It does not recursively adopt dependency APIs, unexported implementation names,
function parameter/local names, protobuf/upstream schemas or every possible
consumer wrapper. Nested anonymous record paths are recorded; named records
have their own entries. JSON tags are retained as source metadata, not renamed.
The receipt is not a stability guarantee or approval of later APIs.

## Retention decisions

| Family examined | Decision and reason |
| --- | --- |
| Cluster construction, topology and resource operations | Retain `Run`, `RunWithImages`, `Add*`, `Create*`, `Remove*`, `Start`, `Set*` and `Update*`. Their verbs distinguish allocating an owned resource, attaching a daemon to an existing cluster, and changing existing policy. `cephfs.Start`/`rgw.Start` attach services; package `Run` owns a cluster. No blanket Create/Add or Set/Apply conversion is justified. |
| Role selection and feature executors | Retain `ImageSet` and `Essential`, `Client`, `CephFS`, `RBDAux`, `RGW`, `All`: they match the current dependency-role contract. `WithImages` selects configuration; `Images` reports effective selection; `ClientContainerContext` names the lazy toolkit executor. `ControlImage`/`ControlContainer*` retain their control-plane meaning rather than restoring old daemon-role image options. |
| Context-bounded getters | Retain `ConnectionConfigContext`, `ControlContainerContext`, `ManagersContext`, `MonitorsContext` and `OSDsContext`. The `Context` suffix names the variant of an existing getter that returns the same owned result but waits for the topology only while `ctx` is active; the context-free getters remain. It does not promise that a returned descriptor stays live. |
| Queries and waits | Retain domain nouns (`Pools`, `OSDStates`, `Sessions`, `Zonegroups`, `ImageStatus`, `DataPools`), plus List where the standalone namespace CLI uses it. Receiver/package context resolves subject names. `WaitReady`, `WaitFor*`, `Wait*Ready` and `Wait*Synced` identify actual readiness/sync predicates; a naming pass must not turn one observation into a broader completion promise. |
| Requests, observations and value/resource types | Retain `Config`, `Setting`, `Policy`, `Status`, `State`, `Info`, `Snapshot`, `Report` and domain resource nouns. Request and returned observation distinctions are explicit in their signatures/reference pages. Repeated `Filesystem`, `Mirror`, `Pool`, `Client` or `Network` words remain where needed to distinguish data pools, host/cluster networks, source/destination and daemon/client roles. |
| Saved policies and revocation handles | Retain the completed `Override*` and management `*Handle` names from #18/#19. Retain `FullRatiosOverride` and `BlocklistOverride` as the explicit #15/#16 exceptions: their domain/Restore methods already identify the saved policy. This does not move either policy into `fault`, add automatic undo, or claim an uncertain handle succeeded. |
| Owned additions and disruption leases | Retain `PauseLease`, `NetworkProfileLease`, `SnapshotScheduleLease`, and `ImmutableObjectCacheLease`. Preserve `Pause`/`Resume`, Apply/Restore and Add/Restore. `TemporaryBlocklist` retains its real native TTL; the cache's Start name deliberately drops Temporary because it has none. `Lease` identifies ownership, not universal expiry or automatic restoration. |
| Quiesce and staged removal/addition | Retain `QuiesceSubvolumes`/`Quiesce.Release`, `MirrorDirectoryAddition`, `MirrorDirectoryRemoval`, `MirrorPeerRemoval`, their Begin/Status/Wait/Acknowledge methods and observation types. They represent native set or phased transition ownership, including partial/uncertain progress; a past-participle name or common Undo would misstate their contract. |
| Retained OSD lifecycle | Retain `DetachOSD`, `ReopenOSD` and `DetachedOSD`. Detach names releasing a daemon's container while keeping its store and MON registration, and is distinct from `RemoveOSD` (drain and purge) and from container Stop/Start (same container). Reopen names starting a new container on that retained store; it neither allocates a registration like `Add*` nor formats like a fresh start. `DetachedOSD` exposes read-only accessors instead of mutable fields, because ReopenOSD validates against private copies; `ClusterFSID` qualifies which FSID it is. |
| Test finalization | Retain `TerminateOnCleanup`, `Finalization`, `Finalization.Check`, `Finalization.Cleanup` and `FinalizationConfig`. `TerminateOnCleanup` names what the call owns: it registers the cluster's termination with test cleanup, like `testcontainers.CleanupContainer`, and orders the checks, failure archive and cleanups before it. `Finalization` is that registered phase sequence, not a lease or restore handle; `Check` and `Cleanup` add steps to its two phases and keep `testing.TB.Cleanup`'s meaning of a function that runs at test end. `Archive` opts into the failure archive; `ArchiveDirectory`, `ArchiveTimeout`, `CheckTimeout` and `CleanupTimeout` qualify the phase they bound, and `Diagnostics` and `CrashMetadata` take the existing collector configurations unchanged. No generic cleanup interface or automatic archive for other fixtures follows. |
| Lifetime and recovery operations | Retain `Terminate` for owned clusters/fixtures, `Close` for streams/shared fixtures, explicit Restore for policies/files, Stop for a process that may retain files, and Resume for thawing. `RestoreBackup`, `RestoreIncremental`, `Rebootstrap*`, `RepairPG` and clone cancel/remove are domain recovery operations, not interchangeable cleanup handles. |
| Debug and fault operations | Retain `InjectObjectDataError`, `InterruptNetwork`, `InterruptLink` and peer/zone interruption helpers with their established package placement. A one-shot injection has no generic Restore contract; caller-bound links differ from cluster-selected networks. No package move or new fault guarantee follows from the naming pass. |
| Options, constants and errors | Retain `With*`, `Default*`, `Max*`, `Min*`, typed domain constants and `Err*`. These express configuration, bounds and errors.Is categories. `WithOSDInMemoryStorage`, `WithOSDVolumeStorage` and `WithOSDObjectStore` distinguish backing storage from the object-store engine. Prefixes alone do not assert support or require a global alias. |
| Identity and native projection fields | Retain `ID`, `UUID`, `FSID`, `GID`, `PID`, `PGID`, `MDS`, `MGR`, `OSD`, `RADOSReads`, `CRUSH*`, `URL` and context-specific `Name`/`Path`. Preserve native map spellings such as `MgrMap`, `MonMap`, `Addrs` and `Addrvec`: these are recognizable protocol projections with existing JSON tags, not requests to normalize upstream schemas. |
| Bounds, quantities and role/policy fields | Retain existing unit-bearing `*Bytes`, `*Percent`, `*Probability`, durations, ranks/counts and source/destination qualifiers. Domain `K`/`M` denote erasure-code geometry, and pin `Value` is qualified by `Type`; no untyped universal value API is introduced. `Complete`, `Ready`, `Confirmed`, `Owned`, `Gone` and truncation/error fields keep their documented observation limits. |
| Credentials, callbacks and mutable public descriptors | Retain `AccessKey`, `SecretKey`, `PrivateKeyPEM`, client identities, `OriginalProcessClientFactory` and configuration fields such as lease `Config`. Clear spelling does not authorize logging secrets, mutate captured identity or convert a descriptor into proof of current ownership; the existing validation/redaction contracts remain. |

## Post-test verification

The first #48 increment adds 23 `ceph` declarations and 73 member entries in
`VerificationConfig`, `LogExpectation`, `CrashExpectation`,
`VerificationReport`, `VerificationWindow`, `VerificationSource`,
`VerificationCheck`, `VerificationFinding`, `ExpectationResult` and
`VerificationGap`. It was reviewed at the call site against the convention
above:

| Name | Decision and reason |
| --- | --- |
| `Finalization.Verify` / `Verification.Evaluate` | `Verify` activates a verification and adds it to the finalization's checks, beside `Check` and `Cleanup`; it records a baseline and changes nothing in the cluster, so neither `Start` nor a lease name fits, and the returned `Verification` has nothing to stop or restore. `Evaluate` observes the window until now and returns a report without failing the test, which only the finalization's check does. |
| `VerificationConfig`, `VerificationReport`, `VerificationWindow`, `VerificationSource`, `VerificationCheck`, `VerificationFinding`, `VerificationGap` | Request and observation types of the retained `Config`/`Report` family. The `Verification` prefix keeps them apart from the diagnostics and crash metadata reports, whose collectors they reuse. |
| `LogExpectation`, `CrashExpectation`, `ExpectationResult` | Separate log and crash expectation types, so that a log pattern cannot waive a crash. `Required` with `MinCount` distinguishes requiring an event from permitting it; `MaxCount` bounds a permission. `ExpectationResult` reports each one's matches. |
| `VerificationOutcome` with `VerificationClean`, `VerificationUnexpected`, `VerificationIncomplete` | The three outcomes keep their type as a prefix, so that clean, an unexpected finding and an incomplete observation stay distinct at the call site. |
| `LogEvent` with `LogAssertion`, `LogFatalSignal`, `LogClusterError`, `LogClusterSecurity`, `LogClusterWarning` | The fixed line classes. `Cluster*` names the cluster log levels (ERR, SEC, WRN) rather than a generic severity, and none of them implies an allowance. |

## Runtime debug level window

The first #50 increment adds 19 `ceph` declarations and 16 member entries in
`DebugLevelSetting`, `DebugLevelReceipt` and `DebugLevelTransition`. It covers
one OSD and one `debug_ms` or `debug_osd` runtime value at a time; the final
spelling for more roles, options or replacement-spanning windows stays open
until native evidence and overhead measurements exist. It was reviewed at the
call site against the convention above:

| Name | Decision and reason |
| --- | --- |
| `Container.OverrideDebugLevel` / `DebugLevelOverrideHandle.Restore` | The `Override*`/`*OverrideHandle`/`Restore` family: the call saves the daemon's prior effective value and returns an explicit restoration handle that may hold an uncertain apply. It has no native expiry, so `Temporary` does not fit. `DebugLevel` rather than `Config` keeps it apart from `OverrideConfig`, which changes stored central entries, not a daemon's runtime value. |
| `DebugLevelSetting` | Request input of the retained `Setting` family, beside `ConfigSetting`; `Daemon`, `Name` and `Level` name the selected target, option and requested text. |
| `DebugLevelOverrideHandle.Receipt`, `DebugLevelReceipt`, `DebugLevelTransition` | `Receipt` returns a copy of the window's evidence, as the existing native receipts do, rather than a live status; `Transition` names one recorded state change with its time, readback and error. |
| `DebugLevelState` with `DebugLevelPriorRead`, `DebugLevelApplyAttempted`, `DebugLevelApplied`, `DebugLevelApplyUncertain`, `DebugLevelRestoreAttempted`, `DebugLevelRestored`, `DebugLevelConflict`, `DebugLevelTargetChanged`, `DebugLevelUnconfirmed` | The states of #50's state progression. The constants keep the `DebugLevel` prefix so that they do not collide with other state names; `Applied` and `Restored` mean verified readback, and `ApplyUncertain` and `Unconfirmed` keep an unknown outcome apart from success. |

`Finalization.TrackDebugLevel` registers a handle for the failure manifest.
`Track` is the retained verb for handing an existing resource to a reporter
without taking ownership or changing it (it neither restores nor removes the
window, unlike `Cleanup`); `DebugLevel` names the handle's family. It adds one
declaration and no member entry, because the manifest records are unexported.

`Finalization.Capture` registers a running packet capture for settlement
before the failure archive. `Capture` is the retained noun-verb for handing a
caller-owned resource to the finalization's settlement phase without the
finalization owning its creation; `CaptureSettler` is the interface that
`capture.Session` implements (the `capture` package imports `ceph`, so `ceph`
cannot name the session type), with `Settle`, `Stop` for a finalization, and
`SettlementAllowance`, the admitted bound. `Settle` and `SettlementAllowance`
add two declarations to `capture`, `CaptureSettler` and `Capture` two to
`ceph`, and two interface member entries.

## Packet capture package

The first #52 increment adds the sixth public package, `capture`, with 32
declarations and 55 member entries in `Request`, `Report`, `Statistics` and
`FileReport`. It was reviewed at the call site against the convention above:

| Name | Decision and reason |
| --- | --- |
| `capture.Start` / `capture.Session.Stop` | `Start` owns a new resource (a helper running `tcpdump` in the target's namespace) and has no native expiry, so neither `Apply`/`Override` nor `Temporary` fits. `Stop` ends a process whose file is retained and delivered, the existing meaning of Stop; it is not a `Restore`, because nothing of the target is changed. `Session` is the caller-owned handle: no cluster `Terminate` knows it, which a `Lease` beside the cluster-registered leases would blur. |
| `capture.Request`, `capture.Report`, `capture.Statistics`, `capture.FileReport` | Request input, the delivery report and its observed parts, as in the retained `Config`/`Report`/`Status` family. The package name already says capture, so no type repeats it. `FileReport` mirrors the internal pcap check field for field without exposing the internal package. |
| `capture.EndReason`, `TargetState`, `FileStatus` and their constants | Typed observations whose constants keep their type as a prefix (`EndStopped`, `EndPremature`, `TargetGone`, `FileValid`), so separate outcomes stay distinct at the call site; `End*`, `Target*` and `File*` do not collide. Nil `Statistics` counters keep unknown apart from zero. |
| `capture.ErrImageUnsupported`, `ErrPermissionDenied`, `ErrFailed` | The netem sentinels' roles without their `Netem` qualifier, which the package name replaces; each matches its `ceph` category with `errors.Is`. |
| `capture.MaxSnapLen`, `MaxFileBytes`, `MaxFilterBytes`, `MaxTimeout`, `DefaultReadyTimeout`, `DefaultStopGrace` | The retained `Max*`/`Default*` bound convention. There is no helper-image constant: no capture helper is published, and the request never selects one implicitly. |

## Embeddings and externally defined interfaces

The member catalogue records **16** explicit embeddings. Cluster/service/daemon
wrappers embed `testcontainers.Container` or `*ceph.Container`; `Pool` embeds
`PoolConfig`, and `PlacementState` embeds `PlacementConfig`. The local parent
fields/methods are reviewed in their own catalogue entries. Testcontainers and
standard-library method names in public signatures are retained without
module-local aliases or duplicate child renames. This source declares no new
exported interface requiring method-name changes.

Inherited raw container Start/Stop/Exec methods continue to address their actual
container; they are not renamed into whole-cluster lifecycle claims. Existing
module overrides, shared-tenant admission and native-command fences stay where
they are. Embedding does not establish that every dependency method is safe for
every tenant or that a removed raw handle has been revoked; those are behavioral
contracts and remain outside this source-only naming completion.

## Validation and future changes

Normal API tests verify declarations, members and receipt binding without
writing files. The member-scope fixture checks that interface and nested fields
are captured while ordinary private fields are excluded. Unit/race tests and
all tag combinations check the maintained method/caller sets. The focused RBD
cache native recipe separately checks cache promotion/counters, repeated read,
Stop fallback and Restore cleanup using the current selected dependency roles.
Compilation, digest agreement and zero remaining proposals are not native proof.

Future additions do not inherit an approval merely by matching a prefix. Review
their role at the call site, ownership, undo/lifetime meaning and affected
consumers, regenerate the inventories, then explicitly refresh the receipt and
this assessment as needed. Keep published tags immutable; the selected source
breaks target the next minor release without automatic permanent aliases.
