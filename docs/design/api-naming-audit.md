# Public naming retention audit

This finishes #14's staged naming pass at the source captured by
[api-naming-review.json](api-naming-review.json). The last selected operation is
`rbd.StartImmutableObjectCache`; its [migration](../guides/upgrading-rbd-cache-name.md)
retains the existing lease and runtime contract. No further rename is selected
for the remaining names in this snapshot.

## Scope and evidence

The declaration catalogue contains **705** functions, declared methods on
exported receivers, types, constants and variables. The separate member
catalogue contains **932** exported field/nested-record/embedding entries in
**161** types. It scans exported declared interfaces as well; there are none
in the current public packages. Its synthetic scope test includes interface
methods/embeddings, grouped fields, pointer/private embeddings and nested
anonymous array/map records, and refuses to expose ordinary private fields.

| Package | Declaration entries | Member/embedding entries |
| --- | ---: | ---: |
| `ceph` | 313 | 323 |
| `fault` | 34 | 64 |
| `rbd` | 77 | 94 |
| `cephfs` | 155 | 247 |
| `rgw` | 126 | 204 |
| Total | 705 | 932 |

This includes the dependency-role migration through #32: `ImageSet`,
`RunWithImages`, `WithImages`, `Images`, `ClientContainerContext` and the
mirror variants, and the retained OSD lifecycle (`DetachOSD`, `ReopenOSD` and
`DetachedOSD` with its `ID`, `UUID`, `ClusterFSID` and `Placement` accessors)
added for #6. It uses the current source, not the earlier 691/692-entry
checkpoint. The receipt binds canonical catalogue bytes and per-package counts.
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
| Queries and waits | Retain domain nouns (`Pools`, `OSDStates`, `Sessions`, `Zonegroups`, `ImageStatus`, `DataPools`), plus List where the standalone namespace CLI uses it. Receiver/package context resolves subject names. `WaitReady`, `WaitFor*`, `Wait*Ready` and `Wait*Synced` identify actual readiness/sync predicates; a naming pass must not turn one observation into a broader completion promise. |
| Requests, observations and value/resource types | Retain `Config`, `Setting`, `Policy`, `Status`, `State`, `Info`, `Snapshot`, `Report` and domain resource nouns. Request and returned observation distinctions are explicit in their signatures/reference pages. Repeated `Filesystem`, `Mirror`, `Pool`, `Client` or `Network` words remain where needed to distinguish data pools, host/cluster networks, source/destination and daemon/client roles. |
| Saved policies and revocation handles | Retain the completed `Override*` and management `*Handle` names from #18/#19. Retain `FullRatiosOverride` and `BlocklistOverride` as the explicit #15/#16 exceptions: their domain/Restore methods already identify the saved policy. This does not move either policy into `fault`, add automatic undo, or claim an uncertain handle succeeded. |
| Owned additions and disruption leases | Retain `PauseLease`, `NetworkProfileLease`, `SnapshotScheduleLease`, and `ImmutableObjectCacheLease`. Preserve `Pause`/`Resume`, Apply/Restore and Add/Restore. `TemporaryBlocklist` retains its real native TTL; the cache's Start name deliberately drops Temporary because it has none. `Lease` identifies ownership, not universal expiry or automatic restoration. |
| Quiesce and staged removal/addition | Retain `QuiesceSubvolumes`/`Quiesce.Release`, `MirrorDirectoryAddition`, `MirrorDirectoryRemoval`, `MirrorPeerRemoval`, their Begin/Status/Wait/Acknowledge methods and observation types. They represent native set or phased transition ownership, including partial/uncertain progress; a past-participle name or common Undo would misstate their contract. |
| Retained OSD lifecycle | Retain `DetachOSD`, `ReopenOSD` and `DetachedOSD`. Detach names releasing a daemon's container while keeping its store and MON registration, and is distinct from `RemoveOSD` (drain and purge) and from container Stop/Start (same container). Reopen names starting a new container on that retained store; it neither allocates a registration like `Add*` nor formats like a fresh start. `DetachedOSD` exposes read-only accessors instead of mutable fields, because ReopenOSD validates against private copies; `ClusterFSID` qualifies which FSID it is. |
| Lifetime and recovery operations | Retain `Terminate` for owned clusters/fixtures, `Close` for streams/shared fixtures, explicit Restore for policies/files, Stop for a process that may retain files, and Resume for thawing. `RestoreBackup`, `RestoreIncremental`, `Rebootstrap*`, `RepairPG` and clone cancel/remove are domain recovery operations, not interchangeable cleanup handles. |
| Debug and fault operations | Retain `InjectObjectDataError`, `InterruptNetwork`, `InterruptLink` and peer/zone interruption helpers with their established package placement. A one-shot injection has no generic Restore contract; caller-bound links differ from cluster-selected networks. No package move or new fault guarantee follows from the naming pass. |
| Options, constants and errors | Retain `With*`, `Default*`, `Max*`, `Min*`, typed domain constants and `Err*`. These express configuration, bounds and errors.Is categories. `WithOSDInMemoryStorage`, `WithOSDVolumeStorage` and `WithOSDObjectStore` distinguish backing storage from the object-store engine. Prefixes alone do not assert support or require a global alias. |
| Identity and native projection fields | Retain `ID`, `UUID`, `FSID`, `GID`, `PID`, `PGID`, `MDS`, `MGR`, `OSD`, `RADOSReads`, `CRUSH*`, `URL` and context-specific `Name`/`Path`. Preserve native map spellings such as `MgrMap`, `MonMap`, `Addrs` and `Addrvec`: these are recognizable protocol projections with existing JSON tags, not requests to normalize upstream schemas. |
| Bounds, quantities and role/policy fields | Retain existing unit-bearing `*Bytes`, `*Percent`, `*Probability`, durations, ranks/counts and source/destination qualifiers. Domain `K`/`M` denote erasure-code geometry, and pin `Value` is qualified by `Type`; no untyped universal value API is introduced. `Complete`, `Ready`, `Confirmed`, `Owned`, `Gone` and truncation/error fields keep their documented observation limits. |
| Credentials, callbacks and mutable public descriptors | Retain `AccessKey`, `SecretKey`, `PrivateKeyPEM`, client identities, `OriginalProcessClientFactory` and configuration fields such as lease `Config`. Clear spelling does not authorize logging secrets, mutate captured identity or convert a descriptor into proof of current ownership; the existing validation/redaction contracts remain. |

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
