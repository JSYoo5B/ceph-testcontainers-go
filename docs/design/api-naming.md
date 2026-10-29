# Public API naming: inventory and proposed conventions

Status: #14's staged naming implementation and current-source retention audit
are complete in this revision. The initial catalogue was introduced by #17;
the final RBD helper change and [retention audit](api-naming-audit.md) complete
the remaining scope. This is a naming assessment of the captured source, not
approval of future APIs or a new runtime-support claim.
Per the maintainer decision recorded on #9, dedicated object-error injection
is `fault.InjectObjectDataError`. Session fencing, fullness and the other
reversible overrides were first kept in `ceph` and later moved to the package
`override` under #146 ([design](override-package-boundary.md)); the
`ceph.Container.Temporary*` and `Override*` names and handle types that this
note inventories are the historical record of the naming pass, not current API.
The [package boundary](fault-package.md) remains independent of naming.

## Source inventory and limits

The `declarations.json` file of each public package in [api-naming/](api-naming/)
is a source-derived catalogue of package-level exports and declared methods on exported receiver
types in the public packages. Every row names
the current identifier, kind, proposed identifier and decision. Functions and
methods include their **current** Go signature, including returned handles; methods also record their pointer/value receiver.
A proposal for a type also propagates to its declared method names in the
catalogue; signatures are not rewritten to pretend the proposed API exists.

| Package | Declared exports |
| --- | ---: |
| `ceph` | 306 |
| `fault` | 34 |
| `rbd` | 77 |
| `cephfs` | 155 |
| `rgw` | 126 |
| Total | 698 |

These are the counts of that initial catalogue. The current counts, including
the `capture` package that #52 added, the post-test verification of #48
and the runtime debug level window of #50, are in the
[retention audit](api-naming-audit.md#scope-and-evidence) and its receipt.

The catalogue includes functions, methods on exported receiver types, types,
constants and variables. It scans all non-test Go source files, including
build-tag alternatives, and refuses conflicting duplicate declarations.
The separate per-package `members.json` catalogues record the
exported field/nested-record/embedding entries. They also scan
declared interface methods; this source has no exported declared interfaces.
Promoted dependency methods are not duplicated: the audit separately records
embedding boundaries and retains the inherited contracts. It does not rename
Testcontainers or standard-library APIs. Implementing a rename still requires
compilation of the resulting method sets and affected callers.

`baseline` means the current name was recorded, **not** that it has received a
complete naming review. The [final audit](api-naming-audit.md) records the
retention decisions for this exact declaration and member surface, including
query/provisioning, options, values and embeddings. Each package's separately issued
receipt (`api-naming/<package>/review.json`) binds that package's two catalogues;
regenerating them cannot silently extend that review to future names. `proposal` means a candidate source
rename, not a compatibility alias or approval to change behavior.

The existing API reference inventory test now also checks this catalogue
against source names and signatures, catches stale proposal keys, and refuses
proposals that collide with each other or a retained declared name. Regenerate
only after inspecting a source/proposal change:

```sh
go run ./internal/tools/apinaming regenerate rgw/multisite
# equivalent test flag:
go test ./internal/apiinventory -run '^TestPublicNaming(Inventory|Members)$' -update-api-naming -api-naming-packages=rgw/multisite
```

Regeneration rewrites only the `declarations.json` and `members.json` of the
named packages (all packages when none is named) and never reads or writes a
receipt. Approval is a separate, explicit command that writes only the named
packages' `review.json`, and only for a catalogue that matches the source:

```sh
go run ./internal/tools/apinaming approve rgw/multisite
```

It refuses a stale catalogue, a pending `proposal` row, and a missing review
note or reference page. The receipt names the review note
(`api-naming-audit.md` by default, `-review` selects another) and the API
reference page of the package (`docs/<package>.md`, with `/` written as `-`).
The other commands are `check` (what the tests verify) and `aggregate` (the
per-package counts and totals, computed on demand and never committed).

Normal `go test ./internal/apiinventory` verifies without modifying files. The
proposal map is `catalogue.Proposals` in
`internal/apiinventory/catalogue/proposals.go`; rationale lives here.
Updating the map/catalogues does not implement any runtime rename. A changed
surface also requires reviewing the names and explicitly approving it; the
regeneration command never edits a receipt.

The public package list is discovered: every directory outside `internal`,
`testdata`, `vendor` and hidden directories with a non-test Go source is a
public package and needs a catalogue directory with all three files. A catalogue
directory without a package fails as well.

## Naming convention

1. Read the full call, including the package and receiver. Use an operation
   verb for a mutation and a noun for its request, observation or returned
   handle. Keep a domain word where the package covers multiple fault or
   service kinds; do not remove every repeated word mechanically.
2. `OverrideX` saves prior policy and gives the caller an explicit `Restore`
   contract. It implies no expiry, context-cancellation rollback or scope-exit
   cleanup. `Container.OverrideFullRatios` is the established example.
3. Use `AddX`, `StartX` or `ApplyX` when the operation owns a new resource or
   disruption rather than adopting arbitrary existing state. The returned
   lease records ownership; its documentation must say who restores it and
   when fixture teardown does so. `Lease` alone does not promise automatic
   restoration, a timer, or interchangeability with another lease type.
4. Keep `Temporary` where native time actually bounds the effect, and document
   that clock independently of explicit restoration. A Restore handle alone
   is insufficient reason for the prefix.
5. Preserve specific undo verbs. `Resume` thaws a process; `Restore` restores
   owned state; `Close` and `Terminate` describe resource or owner lifetimes.
   Do not add a generic cleanup interface or rename these methods into equivalence without reviewing their distinct contracts.
6. Keep native domain operations such as backup restore and one-shot error
   injection distinct from fixture rollback. An injection does not acquire a
   reversible handle merely to match the naming convention.
7. Name stateful management/restoration objects so callers can distinguish them
   from configuration input and observed values. The five ceph operations below
   return explicit `Handle` types retaining prior state, ownership and possibly
   partial or uncertain application. `Handle` promises no successful apply, timer
   or automatic cleanup. Verb-object operations and noun-phrase types are valid;
   shared words or their order alone are not defects. Do not force past-participle
   names or mechanically suffix unrelated Lease/resource types. Other management
   types, including `FullRatiosOverride`, remain for the later #14 audit.
8. Keep standard Testcontainers `Run`, `Container` and functional `WithX`
   options unless a concrete conflict justifies a separate proposal. A
   descriptive query name does not need an added `Get` prefix by default.

## Implemented ceph policy/auth checkpoint

These source renames preserve their implementations, handle state and undo methods.
They add no expiry, context-cancellation rollback, background restoration or
new package move. RevokeClient remains a Container operation; no Tenant revoke
method is introduced. Its shared admission remains the existing tenant-restricted
policy, while the four policy overrides remain exclusive-only.

| Previous operation | Current operation | Result / undo | Preserved contract |
| --- | --- | --- | --- |
| `ceph.Container.TemporaryConfig` | `ceph.Container.OverrideConfig` | `*ceph.ConfigOverrideHandle` / `Restore` | Captures/restores one central config key, including prior absence; no TTL. [config.go](../../ceph/config.go) |
| `ceph.Container.TemporaryOSDFlag` | `ceph.Container.OverrideOSDFlag` | `*ceph.OSDFlagOverrideHandle` / `Restore` | Saves global OSDMap flag state; does not schedule restoration. [osd_policy.go](../../ceph/osd_policy.go) |
| `ceph.Container.TemporaryMGRModule` | `ceph.Container.OverrideMGRModule` | `*ceph.MGRModuleOverrideHandle` / `Restore` | Owns a change to module enablement, not an expiring module. [mgr_modules.go](../../ceph/mgr_modules.go) |
| `ceph.Container.TemporaryMessengerPolicy` | `ceph.Container.OverrideMessengerPolicy` | `*ceph.MessengerPolicyOverrideHandle` / `Restore` | Changes allowed runtime policy; does not replace the fixture's fixed bootstrap mode. [messenger_policy.go](../../ceph/messenger_policy.go) |
| `ceph.Container.TemporaryClientRevocation` | `ceph.Container.RevokeClient` | `*ceph.ClientRevocationHandle` / `Restore` | Removes an owned auth identity, with fenced restoration of its original key/caps; existing authenticated sessions are not all killed immediately. `DeleteClient` remains the permanent deletion operation. [client_revocation.go](../../ceph/client_revocation.go) | This checkpoint fulfills five proposal rows; at that checkpoint the catalogue contained
13 remaining proposal rows and 678 baseline rows. Baseline still does
not mean a complete naming approval. At that point the broader audit and
service/network stages kept #14 open; the final audit below now records those
remaining decisions.

## Implemented CephFS policy and owned schedule names

This checkpoint changes source names only. Explicit restoration, ownership,
non-nil error handles, retryability and existing cleanup remain unchanged.
No expiry, automatic rollback, compatibility alias or package move is added.

| Previous name | Current name | Contract |
| --- | --- | --- |
| `cephfs.Filesystem.TemporarySessionTimeouts` | `cephfs.Filesystem.OverrideSessionTimeouts` | `*cephfs.SessionTimeoutsOverrideHandle` / `Restore`; Overrides timeout policy, not the lifetime of this override. [session.go](../../cephfs/session.go) |
| `cephfs.Filesystem.TemporarySubvolumePin` | `cephfs.Filesystem.OverrideSubvolumePin` | `*cephfs.PinOverrideHandle` / `Restore`; Restores a captured local policy field on the same owned inode; does not guarantee immediate MDS placement. [pin.go](../../cephfs/pin.go) |
| `cephfs.Filesystem.TemporarySubvolumeGroupPin` | `cephfs.Filesystem.OverrideSubvolumeGroupPin` | `*cephfs.PinOverrideHandle` / `Restore`; Same field/identity contract for an owned group. [pin.go](../../cephfs/pin.go) |
| `cephfs.Filesystem.TemporarySnapshotSchedule` | `cephfs.Filesystem.AddSnapshotSchedule` | `*cephfs.SnapshotScheduleLease` / `Restore`; Adds an owned schedule/retention set and refuses existing schedules; Restore removes owned additions rather than overwriting an arbitrary prior schedule. [snapshot_schedule.go](../../cephfs/snapshot_schedule.go) |
| `cephfs.SessionTimeoutsOverride` | `cephfs.SessionTimeoutsOverrideHandle` | Stateful management/restoration handle; no successful-apply guarantee. |
| `cephfs.PinOverride` | `cephfs.PinOverrideHandle` | Stateful management/restoration handle; no successful-apply guarantee. |

## Implemented network profile family

The three operations and three types are migrated together. Profile describes
all supported shaping fields rather than only Delay; it adds no new netem
capability. Error categories, shared/exclusive admission, partial-error leases,
identity/external-edit checks, retryability and cleanup order are preserved.

| Previous name | Current name | Contract |
| --- | --- | --- |
| `fault.TemporaryNetworkDelay` | `fault.ApplyNetworkProfile` | Applies an owned profile to a cluster-plane endpoint; cluster teardown restores held profiles. |
| `fault.TemporaryLinkDelay` | `fault.ApplyLinkProfile` | Applies the same profile on a caller-owned named bridge; the caller restores before removing the target/bridge. |
| `rgw.Multisite.TemporaryZoneLinkDelay` | `rgw/multisite.Fixture.ApplyZoneLinkProfile` | Selects the owned zone endpoint/peer and preserves multisite-owned cleanup. |
| `fault.NetworkDelay` | `fault.NetworkProfile` | Request with unchanged fields for delay/jitter, loss, rate/queue and packet faults/peers. |
| `fault.NetworkDelayLease` | `fault.NetworkProfileLease` | Owns the installed profile/helper; retains `Status` and retryable `Restore`, including uncertain results. |
| `fault.NetworkDelayStatus` | `fault.NetworkProfileStatus` | Observed native state and counters, not achieved throughput or proof that a packet was affected. |

Keep `MaxNetworkDelay`: it limits the Delay field. `NetworkRate`, rate units,
`NetworkPeer`, `NetworkPacketFault`, `NetemHelperImage` and netem sentinels keep
their names and error categories. No timer, cancellation rollback, permanent
alias, package move or generic cleanup interface is introduced.

## Completed RBD helper and retention audit

The cache operation is now `rbd.StartImmutableObjectCache`, returning the same
`*rbd.ImmutableObjectCacheLease`. It starts an owned daemon in a caller-owned
client container without native expiry. `Stop` keeps cached files and `Restore`
removes the owned daemon/files; those distinct contracts remain unchanged.

Current branch catalogue: 698 declarations/methods, zero remaining proposal
rows and 698 baseline rows. The separate 932-entry member catalogue and bound
[retention audit](api-naming-audit.md) complete the current-source assessment.
Baseline alone still means inventory, not naming approval.

## Reviewed retention and undo exceptions

| Current call/type | Decision and reason |
| --- | --- |
| `ceph.Container.OverrideFullRatios` / `ceph.FullRatiosOverride.Restore` | Retain #15's explicit override naming and `ceph` policy placement, normalized ordered tuples, pending-write journal and FSID/epoch checks. No per-threshold Terminate undo. |
| `ceph.Container.TemporaryBlocklist` / `ceph.BlocklistOverride.Restore` | Retain `ceph` placement and `Temporary`: native TTL and natural expiry are real. Restore checks captured expiration before removing the exact session, and does not infer ownership from a listing. No `fault.TemporaryBlocklist`. |
| `fault.Pause` / `fault.PauseLease.Resume` | Retain the selected operation/lease pair; Resume thaws without restarting. A non-nil uncertain result still owns a possible pause. |
| `fault.InterruptNetwork`, `fault.InterruptLink` / `fault.NetworkInterruption.Restore` | Retain the explicit cluster-plane versus standalone named-link call forms for this checkpoint. Restore reconciles an endpoint's identity/address; it is not daemon restart. Do not invent a type suffix change just for uniformity. |
| `rbd.Mirror.InterruptPeerLink`, `cephfs.Mirror.InterruptPeerLink`, `rgw/multisite.Fixture.InterruptZoneLink` | Retain domain-selected peer/zone names and service-owned restoration. These are not global network-wide cuts. |
| `fault.InjectObjectDataError` | Record the one-shot debug mutation in `fault` (moved from `ceph.Container` by the #9 checkpoint); retain `Inject` and no universal Restore handle. Recovery by replica rewrite/repair or OSD restart is distinct from removing a policy override. |
| `ceph.NativeInput.Close` | Retain stream-resource lifetime; not cluster termination or policy rollback. [native_command.go](../../ceph/native_command.go) |
| `rbd.RestoreBackup`, `rbd.RestoreIncremental` | Retain domain data-restore verbs. They consume backup input; they are not lease cleanup and must not be converted into a generic cleanup contract. [backup.go](../../rbd/backup.go) |

## Implementation sequence and evidence

1. Review this inventory/convention checkpoint. No source rename is applied by
   regenerating the catalogue. The final retention audit now covers baseline
   query/provisioning, option, data-type and constant names, plus the expanded
   field/interface/embedding catalogue.
2. The ceph policy/auth operation checkpoint is implemented in this revision,
   preserving shared/exclusive admission keys and their reflection tests. Its
   callers, examples, references and migration table use the current names.
3. The CephFS policy, owned-schedule and RBD cache-helper checkpoints are
   implemented with their original ownership contracts.
4. The complete network profile family is migrated together, including types,
   service helpers, signatures, native callers, discovery tooling, docs and
   sentinels' error-category behavior. Bandwidth/packet/peer/family/plane
   semantics, severe-rate and replication recovery oracles remain intact.
5. Refresh the catalogues after each implementation checkpoint and remove
   fulfilled proposal keys. All selected naming changes and the captured-source
   audit are now implemented. Future API changes need their own review and
   explicit receipt update; refreshing an inventory is not that review.

Names marked `baseline` remain inventory entries; the bound audit records why
this revision retains them.
These selected source breaks target the next minor release; never rewrite a
published tag. Do not introduce permanent compatibility aliases automatically.
No implementation PR may change timers, background workers, admission,
uncertainty, cleanup, or recovery semantics merely to make its new name true.

For each naming checkpoint, verify the inventory, affected links, JSON
determinism, privacy, affected unit tests and tag compilation. A source rename
creates no native capability. Run native tests where
changed implementation, ownership or discovery warrants them; reuse sufficient
same-source evidence with its exact commit/scope instead of repeating runtime
work to turn a naming proposal into a native-support claim.
