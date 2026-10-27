# Public naming retention audit

This finishes #14's staged naming pass at the source captured by
the per-package receipts in [api-naming/](api-naming/) (one `review.json` per
public package). The last selected operation is
`rbd.StartImmutableObjectCache`; its [migration](../guides/upgrading-rbd-cache-name.md)
retains the existing lease and runtime contract. No further rename is selected
for the remaining names in this snapshot.

## Scope and evidence

The declaration catalogues contain functions, declared methods on
exported receivers, types, constants and variables. The separate member
catalogues contain exported field/nested-record/embedding entries. They scan exported declared interfaces as well; the current public packages declare `ceph.CaptureSettler` (two methods) and
`ceph.CaptureFileSource` (that embedding plus one read-only file method). Its synthetic scope test includes interface
methods/embeddings, grouped fields, pointer/private embeddings and nested
anonymous array/map records, and refuses to expose ordinary private fields.

The per-package counts and the totals are no longer kept in this note by hand.
Print them from the committed per-package files with
`go run ./internal/tools/apinaming aggregate`; the view is computed on demand
and never committed.

This includes the dependency-role migration through #32: `ImageSet`,
`RunWithImages`, `WithImages`, `Images`, `ClientContainer` and the
mirror variants, the context-bounded `Monitors` and `OSDs`
getters added for #4, and the retained OSD lifecycle (`OSDContainer.Detach`, `DetachedOSD.Reopen`
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
| Role selection and feature executors | Retain `ImageSet` and `Essential`, `Client`, `CephFS`, `RBDAux`, `RGW`, `All`: they match the current dependency-role contract. `WithImages` selects configuration; `Images` reports effective selection; `ClientContainer` names the lazy toolkit executor. `EssentialImage` names the essential image that starts the daemons and the control-plane executor, and `ControlContainer` retains its control-plane meaning; the retired accessor and the old daemon-role image options are gone. |
| Context-taking getters | Retain `ConnectionConfig`, `ControlContainer`, `Managers`, `Monitors` and `OSDs` of `ceph.Container` and `rgw.Gateways`. Each exists once, takes `ctx` first and returns an error. The former no-context getters and the `Context` suffix variants are gone, with no alias ([migration](../guides/upgrading-context-accessors.md)). The call waits for the topology only while `ctx` is active and does not promise that a returned descriptor stays live. |
| Queries and waits | Retain domain nouns (`Pools`, `OSDStates`, `Sessions`, `Zonegroups`, `ImageStatus`, `DataPools`), plus List where the standalone namespace CLI uses it. Receiver/package context resolves subject names. `WaitReady`, `WaitFor*`, `Wait*Ready` and `Wait*Synced` identify actual readiness/sync predicates; a naming pass must not turn one observation into a broader completion promise. |
| Requests, observations and value/resource types | Retain `Config`, `Setting`, `Policy`, `Status`, `State`, `Info`, `Snapshot`, `Report` and domain resource nouns. Request and returned observation distinctions are explicit in their signatures/reference pages. Repeated `Filesystem`, `Mirror`, `Pool`, `Client` or `Network` words remain where needed to distinguish data pools, host/cluster networks, source/destination and daemon/client roles. |
| Saved policies and revocation handles | Retain the completed `Override*` and management `*Handle` names from #18/#19. Retain `FullRatiosOverride` and `BlocklistOverride` as the explicit #15/#16 exceptions: their domain/Restore methods already identify the saved policy. This does not move either policy into `fault`, add automatic undo, or claim an uncertain handle succeeded. |
| Owned additions and disruption leases | Retain `PauseLease`, `NetworkProfileLease`, `SnapshotScheduleLease`, and `ImmutableObjectCacheLease`. Preserve `Pause`/`Resume`, Apply/Restore and Add/Restore. `TemporaryBlocklist` retains its real native TTL; the cache's Start name deliberately drops Temporary because it has none. `Lease` identifies ownership, not universal expiry or automatic restoration. |
| Quiesce and staged removal/addition | Retain `QuiesceSubvolumes`/`Quiesce.Release`, `MirrorDirectoryAddition`, `MirrorDirectoryRemoval`, `MirrorPeerRemoval`, their Begin/Status/Wait/Acknowledge methods and observation types. They represent native set or phased transition ownership, including partial/uncertain progress; a past-participle name or common Undo would misstate their contract. |
| Operations on handles | Retain `OSDContainer.Remove`, `SetIn`, `Detach`, `AdminSocket` and `RotateKey`, `DetachedOSD.Reopen`, `MonitorContainer.Remove` and `AdminSocket`, `ManagerContainer.Remove`, `AdminSocket` and `RotateKey`, the `Pool` operations `Status`, `Usage`, `PGs`, `SetPGCount`, `WaitForPGCount`, `SetPlacement`, `SetQuota`, `SetReplication` and `Remove`, the `ClientConfig` operations `Capabilities`, `UpdateCaps`, `ReplaceKey`, `Revoke` and `Delete`, `cephfs.Filesystem.Remove`, `cephfs.MDS.RemoveStopped` and `AddReplacement`, `rgw.Gateway.Remove` and the lookup `Container.Pool`. The receiver names the object, so the method drops the noun (`Remove`, not `RemoveOSD`) and takes no name or ID; the verbs keep their meaning. Creating operations (`AddOSD`, `AddMonitor`, `AddManager`, `CreatePool`, `CreateClient`, `cephfs.Start`, `rgw.Start`) have no handle yet and stay where they are. The former `Container` methods and package functions are gone, with no alias ([migration](../guides/upgrading-handle-operations.md)). |
| Typed values | Retain `OSDFlag` with its eleven constants, `PGID` with `ParsePGID` and `String`, and the result type `ConnectionConfig` (fields `Conf` and `Keyring`) for `Container.ConnectionConfig` and `ClientConfig.ConnectionConfig`. A value type names what a plain `string` or an unlabeled `[]byte` pair hid, so a swapped conf/keyring assignment no longer compiles. `OSDFlag` stays an open string type because `OSDFlags` reports flags that have no constant; `OverrideOSDFlag` and the PG methods keep their run-time refusal. An untyped string constant still converts to these types. The method and the result type share the name `ConnectionConfig`, which Go allows. The former `string` and `[]byte, []byte` shapes are gone, with no alias ([migration](../guides/upgrading-typed-values.md)). |
| Retained OSD lifecycle | Retain `OSDContainer.Detach`, `DetachedOSD.Reopen` and `DetachedOSD`. Detach names releasing a daemon's container while keeping its store and MON registration, and is distinct from `Remove` (drain and purge) and from container Stop/Start (same container). Reopen names starting a new container on that retained store; it neither allocates a registration like `Add*` nor formats like a fresh start. `DetachedOSD` exposes read-only accessors instead of mutable fields, because Reopen validates against private copies; `ClusterFSID` qualifies which FSID it is. |
| Test finalization | Retain `TerminateOnCleanup`, `Finalization`, `Finalization.Check`, `Finalization.Cleanup` and `FinalizationConfig`. `TerminateOnCleanup` names what the call owns: it registers the cluster's termination with test cleanup, like `testcontainers.CleanupContainer`, and orders the checks, failure archive and cleanups before it. `Finalization` is that registered phase sequence, not a lease or restore handle; `Check` and `Cleanup` add steps to its two phases and keep `testing.TB.Cleanup`'s meaning of a function that runs at test end. `Archive` opts into the failure archive; `ArchiveDirectory`, `ArchiveTimeout`, `CheckTimeout` and `CleanupTimeout` qualify the phase they bound, and `Diagnostics` and `CrashMetadata` take the existing collector configurations unchanged. No generic cleanup interface or automatic archive for other fixtures follows. |
| Lifetime and recovery operations | Retain `Terminate` for owned clusters/fixtures, `Close` for streams/shared fixtures, explicit Restore for policies/files, Stop for a process that may retain files, and Resume for thawing. `RestoreBackup`, `RestoreIncremental`, `Rebootstrap*`, `RepairPG` and clone cancel/remove are domain recovery operations, not interchangeable cleanup handles. |
| Debug and fault operations | Retain `fault.InjectObjectDataError` (moved from `ceph.Container` by the #9 checkpoint under the maintainer decision that dedicated synthetic-error injection belongs in `fault`), `InterruptNetwork`, `InterruptLink` and peer/zone interruption helpers. The name and signature otherwise follow the pre-move method. A one-shot injection has no generic Restore contract; caller-bound links differ from cluster-selected networks. No new fault guarantee follows from the naming pass. |
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

Change `146-debug-level-receipter` makes `TrackDebugLevel` take the interface
`DebugLevelReceipter` (`Receipt() DebugLevelReceipt`), named for the one method
it requires and for the existing `Receipt` family, and adds the state
`DebugLevelNotApplied` for a window refused before any native step. The
`DebugLevelReceipt.Reason` field is a member of an existing declaration row.

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

The member catalogue records **17** explicit embeddings. Cluster/service/daemon
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

Normal API tests verify the per-package declarations, members and receipt
binding without writing files. The member-scope fixture checks that interface and nested fields
are captured while ordinary private fields are excluded. Unit/race tests and
all tag combinations check the maintained method/caller sets. The focused RBD
cache native recipe separately checks cache promotion/counters, repeated read,
Stop fallback and Restore cleanup using the current selected dependency roles.
Compilation, digest agreement and zero remaining proposals are not native proof.

Future additions do not inherit an approval merely by matching a prefix. Review
their role at the call site, ownership, undo/lifetime meaning and affected
consumers, regenerate the affected package's catalogues
(`go run ./internal/tools/apinaming regenerate <package>`), then approve that
package explicitly (`go run ./internal/tools/apinaming approve <package>`, a
separate step that writes only its `review.json`) and refresh this assessment as
needed. Keep published tags immutable; the selected source
breaks target the next minor release without automatic permanent aliases.

## Explicit raw capture archive addition

`Finalization.CaptureWithFile` names the deliberate raw-file variant next to
metadata-only `Capture`; its byte limit is explicit at the call site.
`CaptureFileSource` extends `CaptureSettler` only for that opt-in and its
`CaptureFile` method opens a finalized read-only source without transferring
ownership. `Session.CaptureFile` implements the same contract, rather than
adding a second settlement name. `Report.RetrievedSHA256` pairs with
`RetrievedBytes` and distinguishes the complete retrieved file from
`File.SHA256`'s possibly incomplete structural-validation prefix. These three
new declarations, the interface embedding/method and the report member were
reviewed together; existing `Capture`/`Settle` names and behavior are retained.

## RGW multisite package move

The 56 multisite declarations (2 functions, 21 types, 3 constants and 30
methods) moved from `rgw` to the new `rgw/multisite` package, following the
[boundary note](rgw-multisite-boundary.md). The catalogues label the package by
its directory (`rgw/multisite.Name`), so the label is unambiguous next to
`rgw`. Names are retained except three that would stutter or collide in the new
package: `RunMultisite` is `Run`, `MultisiteConfig` is `Config` and `Multisite`
is `Fixture`. Every method keeps its name, signature and meaning on the renamed
receiver, and `rgw` keeps 69 declarations (125 before). The move adds no
declaration and no member: the combined `rgw` and `rgw/multisite` counts equal
the previous `rgw` counts (125 declarations, 204 members). No alias remains in
`rgw`. The receipt was refreshed by hand after checking this one-to-one
mapping.

## RBD mirror package move

The 41 exported declarations and 74 members of the RBD mirror family moved from
`rbd` to the service-specific package `rbd/mirror` (the declaration count of
`rbd` and `rbd/mirror` together is the previous 77, the member count the
previous 94). The inventory labels the package by its directory,
`rbd/mirror.`. The package name is `mirror`, so the repeated `Mirror` prefix was
dropped from the types, constants and `RunMirror` functions (`Config`, `Mode`,
`Scope`, `Daemon`, `ImageStatus`, `Namespace`, `Run`, `RunWithImages` and so
on), while `Mirror` itself and every method name, receiver and signature
shape were retained. No operation verb, handle or undo name changed, and no
alias remains. The reviewed mapping is in
[the upgrade guide](../guides/upgrading-rbd-mirror-package.md). The receipt was
refreshed for this deliberate move only. CephFS mirror names are not yet part
of it.

## CephFS mirror package move

The 50 exported declarations and 94 members of the CephFS mirror family moved
from `cephfs` to the service-specific package `cephfs/mirror`; `cephfs` keeps
105 declarations (155 before) and 153 members (247 before), so the two
packages together equal the previous counts one to one. The catalogues label
the package by its directory, `cephfs/mirror`. As for the RBD move, the repeated
`Mirror` prefix was dropped from the types and the `RunMirror` functions
(`Run`, `RunWithImages`, `Config`, `Daemon`, `DirectoryAddition`,
`DirectoryRemoval`, `PeerRemoval`, `Snapshot`, `DirectoryStatus` and the status
types), while `Mirror` and every method name, receiver and signature shape were
retained. No operation verb, handle or undo name changed and no alias remains.
The reviewed mapping is in
[the upgrade guide](../guides/upgrading-cephfs-mirror-package.md). The receipts
of `cephfs` and `cephfs/mirror` were approved separately for this move only.

## 227-image-role-overhaul

The role overhaul deliberately removes the former broad image accessor and adds
`Container.EssentialImage`. The package catalogue also removes the obsolete
`ClusterFacts` image member while retaining the three Observer queries merged
on main. The removal/addition record is historical API accounting only; it
provides no alias, compatibility mapping or retained runtime behavior.

## 64-per-daemon-image-option

The change adds the `ceph.DaemonOption` type with `ceph.WithDaemonImage`, a
variadic parameter on `AddMonitor`, `AddManager`, `AddOSD` and
`AddOSDWithConfig`, an `Image` accessor on the three daemon handles and on
`DetachedOSD`, and a `Version` query on the MON, MGR and OSD handles. The
option follows the `With...` idiom of the other options and is not an alias or
a rename; no existing name, receiver or operation verb changed. `OSDConfig`
was left as the CRUSH placement descriptor. The record lists the added
rows as historical API accounting only.

## 146-observer-only-queries

The sixteen read-only observations are methods of `ceph.Observer` only, so the
catalogue removes the sixteen `Container` methods of those names, `Container.Pools`
and the eight `Tenant` forwarders (`Status`, `HealthDetails`, `Pools`,
`PoolStatus`, `PoolUsage`, `ObjectWatchers`, `OSDFlags`, `BlocklistEntries`).
The `Observer` methods keep the removed names, receivers and signature shapes.
The one addition is `Observer.Pools`, which lists pools as `*ceph.Pool`
objects instead of the `PoolState` slice of `Container.Pools`; the per-pool
reads stay the methods of `Pool`. The record is historical API accounting only;
no alias or compatibility mapping remains. The mapping is in
[the upgrade guide](../guides/upgrading-observer-queries.md).

## 146-static-facts-accessors

The change deletes the ten `Container` accessors that returned static cluster
facts: `AddressFamily`, `CephVersion`, `ClusterNetworkName`, `EssentialImage`,
`HasSeparateClusterNetwork`, `Images`, `MessengerMode`, `NetworkName`,
`PublicAddress` and `UsesHostNetwork`. `FactsOf` and `ClusterFacts`, merged
earlier, serve every one of them, so no name is added and none is renamed. The
essential image is read as `ClusterFacts.Images.Essential`. No alias, shim or
mapping remains. The record lists the removed rows as historical API accounting
only.
