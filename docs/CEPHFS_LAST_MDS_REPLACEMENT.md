# Replace the only stopped CephFS MDS

`fs.AddMDSReplacement(ctx, originalStopped) (*MDSContainer, error)` starts one
new indexed owned MDS after the only original confirmed worker has stopped and
its immutable name has disappeared from the complete native FSMap. The initial
scope is desired active 1 / standby 0, no replay, with the established rank 0
failed and no eligible standby. Pools, filesystem identity, desired counts and
old credentials are preserved. This is separate from ordinary `ScaleMDS`, cold
`NoInitialMDS`, and [stopped CID retirement](CEPHFS_STOPPED_MDS.md).

The caller stops/kills the original worker and separately observes native
failure, or explicitly fails an observed GID from the control container. Add
never performs native fail, clears damage, removes the old CID, deletes auth,
changes max_mds, or recreates storage. Docker CID ownership and native logical
name/GID are separate evidence; this API does not establish CID-to-GID process
attribution. A first arbitrary old-CID 404 is not permission to add.

```go
// The cluster owns original and partial replacement cleanup.
old := fs.MDSs()[0]
// Inject failure through the original handle, then separately observe global
// old-name absence, original rank 0 failed, and no eligible standby.
replacement, err := fs.AddMDSReplacement(ctx, old)
if err != nil {
    // A nonnil replacement is useful cleanup ownership, not completion.
    return err
}
// A healthy new owned rank now permits unchanged original-CID retirement.
if err := fs.RemoveStoppedMDS(ctx, old); err != nil {
    return err
}
_ = replacement
return fs.WaitReady(ctx)
```

Fresh admission retains the original successful descriptor/private full CID,
cluster FSID, filesystem and base-pool IDs, and current admitted attached-pool
IDs/policy. It requires coherent complete native maps, joinable/non-replay
policy, max 1/in [0]/failed [0], empty target up/info/damaged/stopped, global
old-name absence, and no target-assigned or otherwise assignable foreign
standby. The reserved new indexed name/auth must be absent before an attempt.
Unsupported original ownership, drift, busy admission or cancellation before
the actual attempt does not silently consume a new indexed launch.

Once auth/start is attempted, any auth/start, logical-ready, final authority or
final-context error makes this request cleanup-only. Retain any returned exact
partial descriptor and both original cleanup handles. A retry returns that
useful partial with a fixed error and performs no new launch or implicit
completion; generic `WaitReady` success cannot promote a failed attempt. Whole
cluster cleanup owns these resources. No partial-completion/recovery API is
provided by this operation. Q retirement and ordinary Scale remain blocked for
a failed attempted request, including auth/start failure followed by an original
worker restart or a new worker becoming logically active.

For completed exact/copy retries, return only the same original replacement
handle after fresh full-CID/running task, original scope, healthy logical
rank 0/name/GID, old global-name absence and final authority checks. Before
retirement the original CID must remain positively exited; afterward an exact
old 404 is accepted only with Q's confirmed original removal receipt. Unchanged
Q `RemoveStoppedMDS` removes the old member after the new healthy rank exists,
and redirects the compatibility embedded container. It preserves old auth and
desired counts. Completed retries must preserve this new cohort and embed.
External Start/Stop, native policy/failure changes and exported-field edits must
not race these fixture operations. The gates are not a Ceph/Docker transaction.

The native parent
[`TestLastMDSReplacementTopology`](../internal/integration/last_mds_replacement_integration_test.go)
uses `integration,topology`, one fresh original Quay cluster each for bridge and
Linux host, sequentially, with no next network after a failed child. Two OSDs
and metadata/default/additional/sibling pools use replicas 2/min 1. The ordinary
target starts 1 active / 0 standby; an active sibling protects original IDs,
GID/full CID/task and nonce bytes. Initial MON-local startup grace 3600 seconds,
actual effective readback and exact MON CID/env make stopped-but-registered
negative Add preflight deterministic within the 15-minute operation umbrella.
The caller then explicitly fails the saved GID and proves global absence and
established rank0 failed, independently of the old exited CID.

The unavailable phase requires exactly three target-qualified native health
checks: FS_WITH_FAILED_MDS and FS_DEGRADED at HEALTH_WARN, MDS_ALL_DOWN at
HEALTH_ERR, with no mutes or unrelated sibling/storage/module check. Pinned
[FSMap health](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/FSMap.cc#L555-L581)
and [MDSMap health](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/MDSMap.cc#L500-L560)
justify the failed-target checks; [erase](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/FSMap.cc#L947-L980)
and [promotion](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/FSMap.cc#L896-L912)
retain/recover the established rank. MDS_UP_LESS_THAN_MAX is not expected for
in [0]/max 1. A bounded fresh libcephfs mount records a completed native errno
110/112 with return_code 0/not killed, or separately an admitted outer timeout
with killed=true and no invented native errno. Setup/import/auth failures are
failures and are not accepted as availability evidence.

The new worker has a distinct indexed name/full CID/positive task and separately
matching healthy logical rank. Completed copied-old Add retries before and
after Q retirement must return the same new handle, with customizer count 1→2
exactly once and no hidden launch. Fresh libcephfs sessions retain original
additional-pool bytes, write/read new nonce files before and after retirement,
and preserve sibling controls. Complete original-engine resource reads cover
7 initial, 7 registered/failed, 8 with new worker and old exited CID, and 7 after
retirement; only the exact positive original exited task may be non-running.
Cleanup covers the union of 8 original/new CIDs and the owned bridge network.
Auth logs contain names only; private credentials are not emitted. Original and
final stages require clean PGs, strict unmuted HEALTH_OK and required MGR
catalog closure. No MDS-role CLI, Python, socket, tell or process observer is used.

Source-derived successful record expectations across both cases are 2 each of
LAST_MDS_REFUSED, FAILED, ACTIVE, RETIRED and COMPLETE; 14 LAST_MDS_MAP;
6 LAST_MDS_RESOURCES; 2 LAST_MDS_HEALTH and 2 LAST_MDS_MODULES. Reused readers
emit 24 STOPPED_MDS_BYTES for 8 independent nonce datasets, 4 GRACE, 4 EXITED,
4 REMOVED, 10 NO_INITIAL_MDS_AUTH, 4 all-live resource, 4 strict health, 4 module,
2 availability and 2 NO_INITIAL_OSD_CLEANUP records. Repeated byte verifications
are not additional datasets; potential extra bounded health polls are preserved
as returned observations, not discarded to force source call counts.

Each network starts its 15-minute context before all setup. Explicit client,
cluster and raw removal proof share a separate 3-minute cleanup context. Client
fallback is 1 minute and cluster fallback 3 minutes, with the client fallback
skipped only after actual explicit success. Seven returned daemon handles have
at most five-second failure log bounds each: at most 70 seconds total. The
conservative two-network budget is 44 minutes plus failure logs and raw Close
within registered Go 50-minute/job 60-minute limits. These are source ceilings,
not measured completion or an absolute Docker deadline. Existing helper files
remain unchanged. Actual joint compile with the final real API passes for
minimal and all eight tags without selecting tests. Actual native primary and measured cleanup evidence are recorded below. Partial/lost-reply/race and malformed/foreign
admission tests are units unless an actual native barrier is provided. No full
CI, other platform/image, damaged/multi-rank/replay recovery, or in-flight
client-session survival claim follows from this focused case.

## Actual validation

Actual primary session 25476 exits 0: three RUN/PASS, package 223.931 seconds,
parent 223.62, bridge 113.12 / host 110.49, no FAIL/SKIP. The two original Quay
Linux ARM64 cases produce exactly 24 full 128 KiB reader records for eight
independent nonce datasets, 14 maps and 10 names-only auth records. Resource
path 7→7→7→8→7 and union-eight cleanup retain the exact old exited CID and
new indexed task; completed copied retries preserve the new cohort/embed.
Both unavailable mounts completed with native errno 110, return_code 0 and
not killed. Admitted outer watchdog timeout remains a separate contract branch,
not this measured outcome. Two exact failed-target health records, four strict
initial/final HEALTH_OK/checks {} observations and six selected required-module
closures pass. Own outer cleanup has zero new containers/networks.

Root check 86223 exits 0. The actual compiled selection adds only this parent
to Q118, giving 119 required names: 117 internal and two SDK selections,
default 14/full SDK 11 unchanged. All 262 runtime files before/after/current
match SHA `5428132b983b740825e9218abbb33b3d505ef71b664451e01f16d1c7c97808d1`,
and the fixed image policy remains unchanged. These measurements cover this
primary. Existing Q stopped-MDS regression 71135 separately passes three
RUN/PASS in 212.955 seconds (parent 212.59, bridge 105.00 / host 107.59); P
cold-MDS regression 33769 separately passes three in 197.593 seconds (parent
197.31, bridge 98.97 / host 98.34). Each has its own fresh baseline, zero new
resources after cleanup and unchanged current 262-file source/policy. Their
20 readers/eight datasets and ten readers/four datasets remain separate from
the primary 24/eight. Historical Q source 259 and P source 256 timings remain
their original records; Q118/P117
and historical full CI101 remain their original source results. Native logical
name/GID and original Docker CID are independent, not process-to-GID binding.
Partial/lost-reply/race/foreign and damaged/replay cases remain unit/source scope;
no full 119-test CI, all-platform/image or active-session survival claim follows.

- [Actual native terminal](../artifacts/last-mds-replacement-20261008/native-terminal.json)
- [Native log](../artifacts/last-mds-replacement-20261008/native.log)
- [Actual root checks and source](../artifacts/last-mds-replacement-20261008/checks-provenance.json), [source before](../artifacts/last-mds-replacement-20261008/root-source-before.json), [source after](../artifacts/last-mds-replacement-20261008/root-source-after.json)
- [Own cleanup](../artifacts/last-mds-replacement-20261008/runtime-cleanup/after.json)
- [Actual compiled selection](../artifacts/last-mds-replacement-20261008/compiled-selection.json)

- [Current Q regression terminal](../artifacts/last-mds-replacement-20261008/stopped-regression-terminal.json), [log](../artifacts/last-mds-replacement-20261008/stopped-regression.log), [own cleanup](../artifacts/last-mds-replacement-20261008/stopped-regression-cleanup/after.json)
- [Current P regression terminal](../artifacts/last-mds-replacement-20261008/cold-regression-terminal.json), [log](../artifacts/last-mds-replacement-20261008/cold-regression.log), [own cleanup](../artifacts/last-mds-replacement-20261008/cold-regression-cleanup/after.json)
