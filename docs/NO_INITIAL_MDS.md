# Create CephFS before its first MDS

`cephfs.Config.NoInitialMDS` creates an owned filesystem and its pools without
starting its first metadata service. Use it for deferred provisioning and client
availability tests, then call the retained descriptor's existing
`ScaleMDS(ctx, 1, 0)` when the first MDS should start.

The focused bridge/host fixture passed on original Quay Linux ARM64 after a
narrow client cleanup correction. The first failed episode remains recorded
below. Filesystem creation and compilation still establish different evidence
from actual fresh client I/O.

## Construction and first start

The flag is accepted by `cephfs.WithFilesystems` and `cephfs.Start`. With the flag
set, leave `ActiveMDS` and `StandbyMDS` zero and `StandbyReplay` false. Conflicting
counts or replay are rejected before allocation or mutation. The prospective
native `max_mds` remains one; zero owned MDSs does not mean `max_mds=0`.

A confirmed cold descriptor has `Container == nil` and an empty `MDSs()` result.
The cluster owns its lifetime. Callers must not invoke promoted running-container
methods while the embedded `Container` is nil. Its MDS auth entity has not been created and retained MDS customizers have not
run. Metadata, default data and configured additional data pools remain owned
by the original cluster. `WaitReady` still requires active ranks and therefore
returns a caller deadline while the filesystem is cold. The constructor skips
that wait; it does not claim that a client can mount the filesystem.

```go
cluster, err := ceph.Run(ctx, image, ceph.WithOSDCount(2))
if cluster != nil {
    defer func() {
        cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
        defer cancel()
        _ = cluster.Terminate(cleanup)
    }()
}
if err != nil {
    return err
}
fs, err := cephfs.Start(ctx, cluster, cephfs.Config{
    Name:         "deferred",
    NoInitialMDS: true,
    MetadataPool: ceph.PoolConfig{Name: "deferred-meta", Replicas: 2, MinSize: 1},
    DataPool:     ceph.PoolConfig{Name: "deferred-data", Replicas: 2, MinSize: 1},
})
if err != nil {
    return err
}
// Perform the test's cold-client phase before explicitly starting the MDS.
if err := fs.ScaleMDS(ctx, 1, 0); err != nil {
    return err
}
return fs.WaitReady(ctx)
```

The confirmed capability retains the original cluster FSID, named filesystem
ID, metadata/default/additional pool IDs, attachments and prospective MDS
policy. First start reads that original native state again under the existing
setup and owner gates. Recreated pools/filesystems, changed policy, owned worker
inventory, a foreign active/replay worker or an assignable standby cannot be
adopted as a new first start. The reserved first daemon/auth name must remain
absent. For a named filesystem `deferred`, the first owned daemon is
`deferred-0`; the existing default filesystem retains its special first name.
The guard permits an independently bound sibling standby only when the original
target refusal policy protects it from assignment.

Busy, canceled and preflight-rejected calls which issue no mutation preserve
cold admission. Immediately before the first actual auth/start mutation, that
permission is consumed. An error or lost reply does not rearm an empty
descriptor. A returned partial MDS remains in the existing owned inventories
for whole-cluster cleanup; inspect a nonnil descriptor returned with an error.
An unconfirmed partial constructor cannot later adopt externally completed
native state. Automatic recovery of an attempted start with no returned worker
is outside this contract.

After a successful first start, the ordinary scale, readiness, handoff and
partial-worker rules apply. With the flag omitted, existing default active count
one, standby/replay validation and constructors remain unchanged. Initial
CephFS composition still needs the storage and MGR required by the existing
`WithNoInitialOSDs` and `WithNoInitialManagers` guards. Add those resources first
and use the runtime filesystem constructor when testing a combined bootstrap.

## Focused native fixture

Required CI runs this parent with the no-initial-OSD and no-initial-manager
parents in `scenario-empty-bootstrap`, using published official role images.
The three parents run sequentially with fresh fixtures and per-fixture cleanup,
under a shared Go 80-minute/job 90-minute ceiling. The grouped target omits
global `-failfast`; each parent's existing failed-child guard remains. A package
timeout can still prevent later parents from running. For an isolated local run,
`make scenario-mds-bootstrap` retains minimal topology tags, `-failfast` and its
Go 50-minute budget. Historical validation below keeps its original source,
image and CI selection.

[TestNoInitialMDSTopology](../internal/integration/no_initial_mds_integration_test.go)
uses one fresh cluster per bridge/host child, runs sequentially and stops before
starting the next child after a failure. Each cluster has two OSDs, an ordinary
active sibling and a cold target. All sibling/target metadata, default data and
additional data pools use two replicas and minimum size one. The ordinary `.mgr`
pool also uses the two-OSD default. No redundancy warning is waived or muted.

The fixture preserves a live sibling's original filesystem/pool IDs, active
GID, full container CID, StartedAt/PID and nonce bytes. It independently reads
strict native target `info`/`up`/rank arrays and policy, MDS auth entity names and
fresh raw Docker Info/List/exact Inspect/Info on the retained engine. Auth dumps
are private parser input; only entity names are logged. The narrower fresh
fixture has no global standbys. Production handling of independently bound
sibling standbys and deterministic partial/racing admission is covered by units.

Before first scale, a completed bounded `WaitReady` must return canonical
`context.DeadlineExceeded`. A fresh libcephfs mount has an eight-second native
mount timeout and a separate Python subprocess watchdog which kills and waits
for its child after 20 seconds. Only native ETIMEDOUT/EHOSTDOWN from a successfully completed child, or an outer
watchdog deadline after positive native import/setup and mount admission, count
as cold availability observations. The latter is not a native errno claim.
Successful mount, auth denial, import/setup failure and unexpected native error
remain distinct failures. A fresh sibling session still reads exact bytes.
This requires the existing Python payload and utilities, without adding a
`timeout` executable requirement.

First `ScaleMDS(ctx, 1, 0)` must produce the original named rank zero with a
positive native GID and one exact returned live container; its customizer runs
once. Public MDS status is compared with the independent FSMap. Fresh userspace
sessions write, fsync and verify a unique 128 KiB target nonce using the original
additional data pool. Sibling identities, task and bytes remain unchanged.
Every byte result includes a Go-checked SHA-256 and native layout pool name;
readiness observations alone do not establish data delivery.

Cold health accepts only exact original-target absence details from pinned
[v20.2.4 MDSMap::get_health_checks](https://github.com/ceph/ceph/blob/v20.2.4/src/mds/MDSMap.cc):
`MDS_ALL_DOWN` (HEALTH_ERR), `MDS_UP_LESS_THAN_MAX` (HEALTH_WARN) or `FS_DEGRADED`
(HEALTH_WARN), with the expected target name and single matching detail. Missing
or null map collections cannot prove absence. PG-clean readiness precedes that
check. Other filesystem, damage, laggy, pool or module warnings fail. Final data
readiness requires unmuted raw `HEALTH_OK`, empty checks and usable required,
enabled and published always-on MGR modules. A substantiated image payload,
dependency or role conflict stops dependent work for classification against the
fixed [image requirements](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md).
No image, policy, suppression or skip/PASS workaround is authorized.

## Bounds and evidence

Each child's 15-minute operation context starts before raw engine capture,
cluster construction and client creation, and is passed through setup and all
operations. A separate three-minute cleanup context is shared by client,
cluster and retained original raw resource-removal proof. Fallback client
cleanup is bounded to one minute and whole-cluster cleanup to three minutes,
including partial construction. On failure, existing daemon logs are read before
cluster removal with at most five seconds per returned MON/MGR/OSD/MDS container;
no auth dump or keyring is queried. Two children therefore have 30 minutes of
operation ceilings, six minutes of explicit cleanup and eight minutes of
conservative fallback cleanup, plus at most 60 seconds of failure logs and small local Close/test overhead. These are
source bounds, not observed runtime or an absolute Docker wall-clock guarantee.

The inherited raw cleanup marker is `NO_INITIAL_OSD_CLEANUP`; it proves exact
owned CID/network removal on the original engine. The separate outer resource
baseline remains necessary. The accepted run recorded two ZERO,
two ACTIVE, ten BYTES, two AVAILABILITY and two COMPLETE records. MAP/AUTH/resource
and health/module observations describe the actual calls rather than a new
ledger. The ten byte records cover four independently generated nonce datasets;
repeated fresh verification does not create ten distinct datasets. Final source/policy equality, own outer cleanup and measured timing are
recorded below. No full CI matrix, cold partial-start recovery, kernel mount,
metadata-corruption or image certification is claimed.

## Measured validation

The real API joint compile sessions 35530/26535 exited 0 without selected tests.
The accepted root production unit/race/vet/tag checks are session 3645 EXIT 0; that
check predates the later five-line native cleanup receipt correction. The initial
root check 50698 rejected historical ignored Go evidence accidentally discovered
by the all-tag compile. Its log and a byte-preserving .go.txt storage correction
remain separate; production source was unchanged. Final2 minimal/eight-tag
compile sessions 53085/47272 exited 0 on the changed native fixture. See
[compile/check provenance](../artifacts/no-initial-mds-20261008/checks-provenance.json)
and [artifact correction](../artifacts/no-initial-mds-20261008/artifact-storage-correction.json).

Actual compiled selection is 117 names: 115 required internal tests plus two
SDK tests. O's prior 116 gains only `TestNoInitialMDSTopology`; default 14, full
SDK 11/selected 2 and old named profiles remain unchanged. At that source, the
dedicated `scenario-mds-bootstrap` used minimal topology tags, -failfast,
Go 50/job 60 minutes and its own baseline/always cleanup. [Completed inventory](../artifacts/no-initial-mds-20261008/compiled-selection.json)
is separate from runtime. O116/N115/M114 and whole CI101 remain their original
source outcomes, not a new full 117 runtime result.

Native session 59387 exited 0 with three RUN/PASS and no FAIL/SKIP. Package time
was 203.141 seconds; parent 202.77, bridge 102.22 and host 100.55. Both fresh cold
mount children completed with errno 110, return_code 0, prepared=true, mounted=false
and killed=false; no outer watchdog availability result was needed in this run.
Two ZERO/ACTIVE/AVAILABILITY/COMPLETE records, 14 MAP, 6 AUTH, 8 RESOURCES, 6 MODULES
and 10 byte records cover four independently generated 128 KiB nonce datasets.
Fresh verification records do not create additional datasets or imply throughput.

Six raw health observations comprise four HEALTH_OK/checks{}/mutes[] (before cold
construction and after first scale) and two expected cold HEALTH_ERR observations
with only MDS_ALL_DOWN/MDS_UP_LESS_THAN_MAX target absence details. All six module
closure observations passed. The final two data-stage health observations are
strict HEALTH_OK; the cold health exception is not a general warning waiver.
The native uses only the fixed existing payload/image contract.

All 256 runtime inputs were identical before/after/current under manifest SHA
`2f4c6974015a64bdd6200ff24c783db68eff55d31cad4a11e60a05951045fb09`; native source
is `a64f1a84e9abd42d9fca1a32c8ffe1e0d17db6ca695960cfa664fb8ad52ea378`. Fixed policy
SHA 4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62 remained
unchanged. [Image identity](../artifacts/no-initial-mds-20261008/image-inspect.json),
[raw native log](../artifacts/no-initial-mds-20261008/native.log),
[source before](../artifacts/no-initial-mds-20261008/root-source-final2-before.json) and
[source after](../artifacts/no-initial-mds-20261008/root-source-final2-after.json) retain
the actual Quay 20.2.4/Linux ARM64 scope. Two inherited original-engine raw removal
markers passed. A separate actual outer cleanup exited 0 on the same engine with
new containers/networks 0; [its saved result](../artifacts/no-initial-mds-20261008/runtime-cleanup-final2/after.json)
is required alongside test PASS. No other image/platform, kernel mount, cold
partial-start recovery or full CI matrix is certified.

A separate unchanged bridge-only ordinary MDS scale/standby-replay regression,
session 5038, exited 0 with two RUN/PASS and no FAIL/SKIP after 202.702 seconds
(ordinary 96.07, replay 106.07). Its existing rank/map/container, original FS/pool
and client-data assertions remained unchanged. The same 256 source inputs/policy
and separate outer cleanup with zero new resources were recorded in the
[regression log](../artifacts/no-initial-mds-20261008/scale-regression.log) and
[cleanup result](../artifacts/no-initial-mds-20261008/scale-regression-cleanup/after.json).
The evidence covers bridge-only ordinary and standby-replay with their original
assertions. Host-mode scale/replay, partial-start, all-parameter and whole-image
coverage remain outside this episode. Historical scale results stay attached
to their original source.

### First native failure retained

Session 20463 exited 2 after 103.921 seconds. Bridge completed original first-MDS
identity, native mount errno 110, target/sibling nonce data, final strict health
and module closure, original-engine resource removal and COMPLETE observations.
Its fallback then repeated the successfully completed explicit client removal
and reported Docker NoSuchContainer. The test remained FAIL; host never started.
Separate outer cleanup exited 0 with zero new containers/networks. The
[classification](../artifacts/no-initial-mds-20261008/native-final1-rejected/classification.json),
[raw log](../artifacts/no-initial-mds-20261008/native-final1-rejected/native.log) and
[saved cleanup](../artifacts/no-initial-mds-20261008/native-final1-rejected/runtime-cleanup/after.json)
remain evidence for that rejected source, rather than a cold-MDS PASS.

The native-only correction records success immediately after explicit client
Terminate returns nil; fallback skips only that receipt. Partial setup and
uncertain/failed termination retain fallback and its errors. There is no general
NotFound suppression, policy/image change or health waiver. The successful Final2
rerun and outer cleanup above are a separate episode. No required image conflict
was observed in the saved first failure; this narrow classification does not certify the whole image.
