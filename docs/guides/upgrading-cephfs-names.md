# Upgrading CephFS policy and schedule names

These source breaks target the next minor release. Published tags keep their
original APIs; main adds no compatibility aliases. Methods remain on
`*cephfs.Filesystem`.

| Previous method | Current method | Current result / unchanged undo |
| --- | --- | --- |
| `TemporarySessionTimeouts` | `OverrideSessionTimeouts` | `*SessionTimeoutsOverrideHandle` / `Restore` |
| `TemporarySubvolumePin` | `OverrideSubvolumePin` | `*PinOverrideHandle` / `Restore` |
| `TemporarySubvolumeGroupPin` | `OverrideSubvolumeGroupPin` | `*PinOverrideHandle` / `Restore` |
| `TemporarySnapshotSchedule` | `AddSnapshotSchedule` | `*SnapshotScheduleLease` / `Restore` |

Update explicit types, interface declarations, mocks and wrappers:
`cephfs.SessionTimeoutsOverride` becomes `cephfs.SessionTimeoutsOverrideHandle`,
and `cephfs.PinOverride` becomes `cephfs.PinOverrideHandle`. The inputs
`SessionTimeouts`, `PinSetting` and `SnapshotSchedule` are unchanged.

The two Handle types retain prior state, ownership and partial/uncertain
application. Keep a non-nil result even when an error is returned. Copies share
restoration state. Retry Restore with a fresh bounded context as needed; no
successful-apply guarantee, timer, cancellation rollback or scope cleanup is
introduced. Session limits still use non-atomic native commands, filesystem
identity checks and outside-edit refusal. Pin restoration still checks the
owned directory/inode and canonical local policy value, without promising
immediate MDS placement.

`AddSnapshotSchedule` adds an owned schedule and retention rules and refuses an
existing schedule; it does not overwrite an arbitrary saved schedule. Restore
removes the owned additions and preserves snapshots. `SnapshotScheduleLease`
keeps its existing ownership contract and is not renamed. No package boundary,
admission, native test selector or fixture cleanup behavior changes.

See the [naming inventory](../design/api-naming.md) and
[CephFS reference](../cephfs.md). This checkpoint does not complete #14's broader
baseline/field/interface audit.
