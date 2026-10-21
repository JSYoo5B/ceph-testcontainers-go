# Public API naming: inventory and proposed conventions

Status: first review checkpoint for #14, based on the public surface after
#15 merged. This checkpoint records current declarations and proposed names;
it changes no runtime API. #9 is closed, but its remaining object-error wording
does not authorize another package move. This audit preserves the current
placement of `ceph.Container.InjectObjectDataError`, session fencing and fullness.
The [package boundary](fault-package.md) remains independent of naming.

## Source inventory and limits

[api-naming-inventory.json](api-naming-inventory.json) is a source-derived
catalogue of package-level exports and declared methods on exported receiver
types in the five public packages. Every row names
the current identifier, kind, proposed identifier and decision. Functions and
methods include their **current** Go signature, including returned handles; methods also record their pointer/value receiver.
A proposal for a type also propagates to its declared method names in the
catalogue; signatures are not rewritten to pretend the proposed API exists.

| Package | Declared exports |
| --- | ---: |
| `ceph` | 301 |
| `fault` | 34 |
| `rbd` | 76 |
| `cephfs` | 154 |
| `rgw` | 126 |
| Total | 691 |

The catalogue includes functions, methods on exported receiver types, types,
constants and variables. It scans all non-test Go source files, including
build-tag alternatives, and refuses conflicting duplicate declarations.
Struct fields, interface members, promoted/inherited methods and dependency
exports are outside this snapshot. In particular, service containers inherit
methods from `ceph.Container` and Testcontainers; the catalogue does not invent
independent service renames for those promoted methods. Implementing a proposal
still requires compilation of the resulting method sets and affected callers.

`baseline` means the current name was recorded, **not** that it has received a
complete naming review. The decisions and exceptions below cover the initial
operation/handle families; the remaining query, provisioning, option and value
names remain to be reviewed under #14. `proposal` means a candidate source
rename, not a compatibility alias or approval to change behavior.

The existing API reference inventory test now also checks this catalogue
against source names and signatures, catches stale proposal keys, and refuses
proposals that collide with each other or a retained declared name. Regenerate
only after inspecting a source/proposal change:

```sh
go test ./internal/apiinventory -run '^TestPublicNamingInventory$' -update-api-naming
```

Normal `go test ./internal/apiinventory` verifies without modifying files. The
proposal map is in `internal/apiinventory/naming_test.go`; rationale lives here.
Updating the map/catalogue does not implement any runtime rename.

## Proposed convention

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
   Existing shared `Lease`/`Exclusive` calls register release with `testing.TB.Cleanup`;
   do not invent a public `Tenant.Release` just to match another handle.
   Do not add a generic cleanup interface or rename these methods into equivalence without reviewing their distinct contracts.
6. Keep native domain operations such as backup restore and one-shot error
   injection distinct from fixture rollback. An injection does not acquire a
   reversible handle merely to match the naming convention.
7. Keep standard Testcontainers `Run`, `Container` and functional `WithX`
   options unless a concrete conflict justifies a separate proposal. A
   descriptive query name does not need an added `Get` prefix by default.

## Proposed operation and handle mappings

All rows below are proposals for later implementation PRs. Handles and undo
methods stay named as shown unless an explicit type rename appears. Preserve
non-nil error handles, cancellation fencing, uncertain outcomes, identity,
external-edit refusal, retryability and cleanup order during every rename.

| Current operation | Proposed operation | Result / undo | Reason and source contract |
| --- | --- | --- | --- |
| `ceph.Container.TemporaryConfig` | `ceph.Container.OverrideConfig` | `*ceph.ConfigOverride` / `Restore` | Captures/restores one central config key, including prior absence; no TTL. [config.go](../../ceph/config.go) |
| `ceph.Container.TemporaryOSDFlag` | `ceph.Container.OverrideOSDFlag` | `*ceph.OSDFlagOverride` / `Restore` | Saves global OSDMap flag state; does not schedule restoration. [osd_policy.go](../../ceph/osd_policy.go) |
| `ceph.Container.TemporaryMGRModule` | `ceph.Container.OverrideMGRModule` | `*ceph.MGRModuleOverride` / `Restore` | Owns a change to module enablement, not an expiring module. [mgr_modules.go](../../ceph/mgr_modules.go) |
| `ceph.Container.TemporaryMessengerPolicy` | `ceph.Container.OverrideMessengerPolicy` | `*ceph.MessengerPolicyOverride` / `Restore` | Changes allowed runtime policy; does not replace the fixture's fixed bootstrap mode. [messenger_policy.go](../../ceph/messenger_policy.go) |
| `ceph.Container.TemporaryClientRevocation` | `ceph.Container.RevokeClient` | `*ceph.ClientRevocation` / `Restore` | Removes an owned auth identity, with fenced restoration of its original key/caps; existing authenticated sessions are not all killed immediately. `DeleteClient` remains the permanent deletion operation. [client_revocation.go](../../ceph/client_revocation.go) |
| `cephfs.Filesystem.TemporarySessionTimeouts` | `cephfs.Filesystem.OverrideSessionTimeouts` | `*cephfs.SessionTimeoutsOverride` / `Restore` | Overrides timeout policy, not the lifetime of this override. [session.go](../../cephfs/session.go) |
| `cephfs.Filesystem.TemporarySubvolumePin` | `cephfs.Filesystem.OverrideSubvolumePin` | `*cephfs.PinOverride` / `Restore` | Restores a captured local policy field on the same owned inode; does not guarantee immediate MDS placement. [pin.go](../../cephfs/pin.go) |
| `cephfs.Filesystem.TemporarySubvolumeGroupPin` | `cephfs.Filesystem.OverrideSubvolumeGroupPin` | `*cephfs.PinOverride` / `Restore` | Same field/identity contract for an owned group. [pin.go](../../cephfs/pin.go) |
| `cephfs.Filesystem.TemporarySnapshotSchedule` | `cephfs.Filesystem.AddSnapshotSchedule` | `*cephfs.SnapshotScheduleLease` / `Restore` | Adds an owned schedule/retention set and refuses existing schedules; Restore removes owned additions rather than overwriting an arbitrary prior schedule. [snapshot_schedule.go](../../cephfs/snapshot_schedule.go) |
| `rbd.TemporaryImmutableObjectCache` | `rbd.StartImmutableObjectCache` | `*rbd.ImmutableObjectCacheLease` / `Restore` | Starts an owned helper in a caller-owned client container; Restore stops the owned daemon and removes its owned files. Applying `ClientOptions` to a native client remains the caller's responsibility; there is no native expiry. [immutable_object_cache.go](../../rbd/immutable_object_cache.go) |
| `fault.TemporaryNetworkDelay` | `fault.ApplyNetworkProfile` | `*fault.NetworkProfileLease` / `Restore` | The request supports rate, queue, loss, reorder, duplicate and corrupt as well as delay; refuses foreign/overlapping state instead of capturing arbitrary qdiscs. Cluster owns teardown. [network_delay.go](../../fault/network_delay.go) |
| `fault.TemporaryLinkDelay` | `fault.ApplyLinkProfile` | `*fault.NetworkProfileLease` / `Restore` | Same profile on a named link for caller-owned/standalone targets; caller restores before removing the target or bridge. [network_delay.go](../../fault/network_delay.go) |
| `rgw.Multisite.TemporaryZoneLinkDelay` | `rgw.Multisite.ApplyZoneLinkProfile` | `*fault.NetworkProfileLease` / `Restore` | The zone helper forwards the full profile and retains multisite-owned cleanup. [network.go](../../rgw/network.go) |

The network operation rows require one coordinated type migration:

| Current type | Proposed type | Reason |
| --- | --- | --- |
| `fault.NetworkDelay` | `fault.NetworkProfile` | Describes the complete request, not just its delay field. |
| `fault.NetworkDelayLease` | `fault.NetworkProfileLease` | Owns the complete installed profile; retains `Restore` and `Status`. |
| `fault.NetworkDelayStatus` | `fault.NetworkProfileStatus` | Observes the complete installed profile. |

Keep `MaxNetworkDelay`: it limits the delay field, not every kind of profile
fault. Keep `NetworkRate`, rate units, `NetworkPeer`, `NetworkPacketFault`,
`NetemHelperImage` and netem sentinel names where they describe their actual
values/dependency. A broad type-prefix replacement would obscure those limits.

## Reviewed retention and undo exceptions

| Current call/type | Decision and reason |
| --- | --- |
| `ceph.Container.OverrideFullRatios` / `ceph.FullRatiosOverride.Restore` | Retain #15's explicit override naming and `ceph` policy placement, normalized ordered tuples, pending-write journal and FSID/epoch checks. No per-threshold Terminate undo. |
| `ceph.Container.TemporaryBlocklist` / `ceph.BlocklistOverride.Restore` | Retain `ceph` placement and `Temporary`: native TTL and natural expiry are real. Restore checks captured expiration before removing the exact session, and does not infer ownership from a listing. No `fault.TemporaryBlocklist`. |
| `fault.Pause` / `fault.PauseLease.Resume` | Retain the selected operation/lease pair; Resume thaws without restarting. A non-nil uncertain result still owns a possible pause. |
| `fault.InterruptNetwork`, `fault.InterruptLink` / `fault.NetworkInterruption.Restore` | Retain the explicit cluster-plane versus standalone named-link call forms for this checkpoint. Restore reconciles an endpoint's identity/address; it is not daemon restart. Do not invent a type suffix change just for uniformity. |
| `rbd.Mirror.InterruptPeerLink`, `cephfs.Mirror.InterruptPeerLink`, `rgw.Multisite.InterruptZoneLink` | Retain domain-selected peer/zone names and service-owned restoration. These are not global network-wide cuts. |
| `ceph.Container.InjectObjectDataError` | Record the current one-shot debug mutation in `ceph`; retain `Inject` and no universal Restore handle. Recovery by replica rewrite/repair or OSD restart is distinct from removing a policy override. This naming proposal decides no package move. |
| `ceph.NativeInput.Close` | Retain stream-resource lifetime; not cluster termination or policy rollback. [native_command.go](../../ceph/native_command.go) |
| `ceph.SharedCluster.Close`, `ceph.SharedCluster.Lease`, `ceph.SharedCluster.Exclusive`, `ceph.Container.Terminate` | Retain distinct owner/tenant/fixture lifetimes. Lease/Exclusive register release with test cleanup; there is no public Tenant.Release method. A tenant cleanup does not mean every shared cluster is immediately terminated. [shared.go](../../ceph/shared.go), [shared_exclusive.go](../../ceph/shared_exclusive.go), [tenant.go](../../ceph/tenant.go) |
| `rbd.RestoreBackup`, `rbd.RestoreIncremental` | Retain domain data-restore verbs. They consume backup input; they are not lease cleanup and must not be converted into a generic cleanup contract. [backup.go](../../rbd/backup.go) |

## Implementation sequence and evidence

1. Review this inventory/convention checkpoint. No source rename is applied by
   regenerating the catalogue. Review the remaining baseline query/provisioning,
   option, data-type and constant names before claiming the whole naming audit
   complete; field/interface-member naming needs its own expanded inventory.
2. Implement ceph policy/auth operation names as one bounded PR, preserving
   shared/exclusive admission keys and their reflection tests. Update callers,
   examples, references and an explicit old-to-new migration table together.
3. Implement CephFS policy and owned-schedule names and the RBD cache helper in
   separate service-focused PRs with their actual undo/ownership contracts.
4. Migrate the complete network profile family together, including types,
   service helpers, signatures, native callers, discovery tooling, docs and
   sentinels' error-category behavior. Keep bandwidth/packet/peer/family/plane
   semantics, severe-rate and replication recovery oracles intact.
5. Refresh the catalogue after each implementation checkpoint; remove fulfilled
   proposal keys or record the resulting new baseline rather than keeping stale
   aliases in the proposal map. Keep #14 open until its full scope is satisfied.

Names marked `baseline` are pending audit unless covered by the retention table.
These proposed source breaks target the next minor release; never rewrite a
published tag. Do not introduce permanent compatibility aliases automatically.
No implementation PR may change timers, background workers, admission,
uncertainty, cleanup, or recovery semantics merely to make its new name true.

For this documentation/test-only checkpoint, verify the inventory, affected
links, JSON determinism and privacy. It creates no native capability. Later
rename PRs need affected unit tests and tag compilation. Run native tests where
changed implementation, ownership or discovery warrants them; reuse sufficient
same-source evidence with its exact commit/scope instead of repeating runtime
work to turn a naming proposal into a native-support claim.
