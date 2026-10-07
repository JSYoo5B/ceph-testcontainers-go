# Retire an original stopped CephFS MDS

`fs.RemoveStoppedMDS(ctx, daemon)` removes one original owned Docker container
and its filesystem/service registry entries after its immutable daemon name
has disappeared from every native filesystem and the global standby list.
Healthy original owned members must still serve all desired active ranks.
The descriptor must come from a successful original startup in `fs.MDSs()`;
partial startups remain owned for whole-cluster cleanup.

The method does not stop a worker, fail a native GID, delete credentials, or
change desired active/standby capacity. Docker CID ownership and a native
name/GID observation are separate authorities. `MDSStatus` reports native
name-based ownership; this operation does not establish CID-to-GID process
attribution. A caller may wait for native expiry or explicitly fail an observed
native GID from the control container before invoking it.

For an ordinary 1 active / 1 standby filesystem, the recovery sequence is:

```go
// Save original handles before injecting a failure. The cluster owns cleanup.
old := fs.MDSs()[0]
// Stop/kill old using its original handle. Separately observe native failover,
// all settled owned ranks, and global absence of old.ID.
if err := fs.RemoveStoppedMDS(ctx, old); err != nil {
    return err
}
// Desired standby capacity remains 1, so restore it explicitly.
if err := fs.ScaleMDS(ctx, 1, 1); err != nil {
    return err
}
return fs.WaitReady(ctx)
```

Do not run direct `Start`/`Stop`, external FSMap/failure/policy changes, or edit
exported descriptor fields concurrently with removal. Gates and fresh rechecks
protect fixture ownership; they are not a Docker/Ceph transaction. Exported
name/filesystem/container changes cannot redirect removal to another process.

The operation retains original cluster FSID, filesystem ID, metadata/default
pool IDs and the private creation handle/full CID. It snapshots and rechecks
current data-pool attachments and policy during removal; legal completed
additional-pool changes before a fresh call are allowed. Ordinary filesystem
construction does not permanently capture every initial additional-pool ID.
A stopped member that remains native-registered is refused before termination.
Foreign active ranks, incomplete maps, an unavailable original handle, or no
healthy original survivor also prevent removal.

A Docker error or lost reply retains the original descriptor/service entries
and attempt identity. Retry the same handle, or an exact copied handle, after
resolving the reported condition. Only exact original-CID absence following
this owned termination attempt can reconcile removal; an arbitrary first-call
404 is not success. Final authority/context drift leaves ownership pending.
A completed retry performs fresh checks and does not repeat termination.
Whole-cluster `Terminate` continues to own pending/partial cleanup.

The old auth entity, key and caps are preserved. Initial `auth get-or-create`
does not establish exclusive ownership of that principal. Existing
`ScaleMDS(1, 1)` creates a new indexed name/container rather than adopting the
retired CID; it does not reclaim the old credential. The compatibility embedded
container is redirected to an original surviving member when necessary.
`WaitReady` still checks requested standby capacity, so a recovered active rank
alone does not complete that wait.

The native case is
[`TestStoppedMDSRetirementTopology`](../internal/integration/stopped_mds_integration_test.go),
built with `integration,topology`. It uses one fresh cluster per bridge/Linux
host case, 2 OSDs and dedicated target/sibling pools with replicas 2/min 1.
The target initially owns 1 active and 1 ordinary standby; the sibling owns a
separate active worker. It preserves original cluster/FS/pool identities,
sibling GID/full CID/task, and distinct nonce files through takeover, original
CID removal and explicit replacement. A completed copied retirement handle is
retried both before and after replacement; it must preserve the new cohort,
compatibility embed and replacement task. No replay-standby or foreign-standby
native variant is claimed by this case.

To make the stopped-but-still-registered rejection deterministic, the test
uses the existing initial MON customizer with
`CEPH_ARGS=--mds-beacon-grace=3600`. It requires fresh effective
`ceph config show mon.a mds_beacon_grace` readback of 3600 and exact MON CID/env
inspection before and after rejection. The entire setup/operation context is
15 minutes, shorter than this startup grace. Strict native maps must retain
the exact old name/GID on both sides of the refused call, while fresh raw Docker
inspection retains the original exited CID. It then explicitly invokes
control-side `ceph mds fail <observed GID>` and observes the original standby
promotion and global old-name/GID absence. This fixture setting is disposed
with the cluster; it adds no module image feature or application config API.
Pinned [MON expiry code](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/MDSMonitor.cc#L2114-L2209)
reads this value; the [option definition](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/mds.yaml.in#L233-L242)
and [startup environment parser](https://github.com/ceph/ceph/blob/v20.2.4/src/common/config.cc#L446-L561)
support this fixture condition. Runtime configuration changes are not assumed.

Independent raw Docker inspection proves the full original CID absent on the
retained original engine. Complete raw resource inventories before failure,
after retirement, and after replacement exclude hidden launches. Fresh
libcephfs sessions verify 128 KiB nonce payloads in the original additional data
pool and the sibling's original data pool. The final stage requires clean PGs,
strict unmuted `HEALTH_OK` and enabled/always-on required MGR catalog closure.
No MDS-role CLI, Python, socket or `/proc` observer is used. The fixed image
payload policy remains mandatory; observed required payload/module failures
stop the case for classification rather than receiving a warning waiver.

Expected source-derived records across both successful cases are 2 each of
`STOPPED_MDS_REFUSED`, `TAKEOVER`, `RETIRED`, `REPLACEMENT`, and `COMPLETE`;
4 `GRACE`, 4 `EXITED`, 6 `REMOVED`, 14 `MAP`, and 20 `BYTES` records. The byte
records describe 8 independent nonce datasets, including repeated verification
of retained files. Reused readers emit 8 `NO_INITIAL_MDS_AUTH`, 6 resource,
4 health and 4 module records, plus 2 `NO_INITIAL_OSD_CLEANUP` records.
Native preservation of CID/map/inventory is observable; internal attempt-state
and zero termination-call behavior are unit/source assertions, not new logged
native fields.

Each network case starts a 15-minute context before all cluster/client/FS
setup. Explicit client + cluster + raw absence cleanup shares a separate
3-minute context. Failure fallback bounds are 1 minute for the client and
3 minutes for the cluster; successful explicit client cleanup suppresses its
fallback only after a successful receipt. Failure logs use at most 5 seconds
for each of 7 returned daemon handles per case, at most 70 seconds across both.
Thus the conservative source budget is 44 minutes plus failure logs and raw
client Close overhead, within the dedicated Go 50-minute/job 60-minute limits.
The failed-child parent returns before starting the next network. These are
source ceilings, not measured runtime or an absolute Docker deadline.

The actual original Quay Linux ARM64 bridge/host run completed with 3 RUN/PASS,
no FAIL/SKIP, and package time 207.328 seconds (parent 206.97 seconds; bridge
103.81 seconds, host 103.16 seconds). Session 48592 exited 0. The 259 runtime
files were unchanged before/after, with manifest SHA
`c2c0133003f6f5659127632c7bc8980b6ba49786a7841e64c058edfafc0b5ef9`, and the fixed
image policy was unchanged. All source-derived marker counts above were
observed. The 20 full-reader records retain 8 distinct nonce datasets; the
four raw health details were HEALTH_OK with empty checks and mutes, with
required MGR module closure. The separate outer cleanup exited 0 on the
original engine with no new containers or networks.

[Native log](../artifacts/stopped-mds-retirement-20261008/native.log),
[actual terminal](../artifacts/stopped-mds-retirement-20261008/native-terminal.json),
[checks and provenance](../artifacts/stopped-mds-retirement-20261008/checks-provenance.json),
[source before](../artifacts/stopped-mds-retirement-20261008/root-source-before.json),
[source after](../artifacts/stopped-mds-retirement-20261008/root-source-after.json),
[outer cleanup](../artifacts/stopped-mds-retirement-20261008/runtime-cleanup/after.json),
and [independent direct review](../artifacts/stopped-mds-retirement-20261008/PRIMARY_NATIVE_REVIEW.md)
are retained in the validating checkout. Raw map name/GIDs and original
container ownership remain independent observations. Sibling pool identity,
current task/private handle rechecks, and internal attempt-state assertions
come from the frozen source and unit checks; no unlogged native field is invented.

The same source's existing cold-MDS profile passed separately: session 58408
exited 0 with 3 RUN/PASS and package time 201.502 seconds (parent 200.89 seconds;
bridge 102.33 seconds, host 98.56 seconds). Its 10 byte records retain 4 nonce
datasets; they are not added to the primary 20/8 totals. Its original policy
and 259-file source remained unchanged, and its own outer cleanup reported no
new containers or networks. See the [cold regression log](../artifacts/stopped-mds-retirement-20261008/cold-regression.log),
[terminal](../artifacts/stopped-mds-retirement-20261008/cold-regression-terminal.json),
[source after](../artifacts/stopped-mds-retirement-20261008/root-source-after-cold-regression.json),
and [own cleanup](../artifacts/stopped-mds-retirement-20261008/cold-regression-cleanup/after.json).

Root `make check` session 2950 passed before native execution. The actual
[compiled selection](../artifacts/stopped-mds-retirement-20261008/compiled-selection.json)
is 118 required names, adding only this parent to P117, with 116 selected
internal names and 2 SDK runtime names. The default profile remains 14 names
and the full SDK package remains 11 names. Compiled selection and these two
focused native episodes do not certify the whole 118-name CI, another image
or platform, every standby combination, native partial recovery, or
CID-to-GID process binding. Existing ordinary scaling/replay and P's first
cleanup failure retain their original evidence and scope.
