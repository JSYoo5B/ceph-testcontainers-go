# RGW multisite propagation of a bucket's object lock configuration

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestMultiClusterRGWBucketObjectLockSync`, optional batch
`rgw_object_lock_sync`. Pure tests of the configuration reader, of the
judgement, of the versioning prerequisite and of the raw body kept as
evidence (`TestParseLockConfig`, `TestJudgeLockReads`, `TestJudgeVersioning`
and `TestRawBody`, batch `pure`).

## Use case and native contract

A service writes to one zone of a multisite realm and relies on a bucket's
default retention in every zone: a bucket made with object lock gets a
default retention rule (`PUT ?object-lock`), and an application or an
auditor that asks another zone for the bucket's configuration
(`GET ?object-lock`) must get the same answer: enabled, the same retention
mode and the same period. A successful call in the zone that was written to
does not show that.

The realm has two zones on two clusters, `a` (the metadata master) and `b`.
Everything is written to zone `a`:

| Bucket | Made with | Role |
| --- | --- | --- |
| `tc-lock` | `x-amz-bucket-object-lock-enabled: true` | trigger; never holds an object |
| `tc-plain` | no object lock | control; holds one canary object |
| `tc-lock-marker-1`, `tc-lock-marker-2` | nothing | metadata checkpoints |

The case applies `GOVERNANCE`, 1 day, to `tc-lock`, and once both zones
answer it, replaces it with `COMPLIANCE`, 1 year.

The trigger is the bucket instance record that metadata sync transfers. On
an affected build the record's JSON form leaves the object lock
configuration out: the flag "object lock enabled" travels with the record's
flags, the rule does not, and the zone that writes the record from that JSON
stores a bucket that is enabled without a rule. The fix writes and reads the
configuration in that JSON form
([fix](https://github.com/ceph/ceph/pull/63089),
[Tentacle backport](https://github.com/ceph/ceph/pull/70500), in Tentacle
since 20.2.5 and in Umbrella). The case shows the record with `radosgw-admin metadata get` in both
zones (`metadata-has-obj_lock`).

## Requirements

- Two clusters with one OSD each and a realm of two zones in one zonegroup
  (`multisite.Run` with `WithTopology`), one gateway per zone, plain HTTP.
  Both clusters and both gateways run the package's `image`, one aggregate
  image. Bridge networking.
- Requests are signed by the test's own SigV4 client with each gateway's
  generated user. The bucket record is read with `Fixture.ZoneAdmin`.
- The test bounds itself to 20 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps fail
the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rgw_multisite_object_lock_sync result=<REPRODUCED|NOT_REPRODUCED>`,
for both steps, and passes either way. No automatic run includes it (the
default checks of a pull request, pushes to `main`, periodic samples), and it
never gates anything. A maintainer can run it on demand, also on a pull
request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_object_lock_sync`.
- Comment: `/ci run rgw_object_lock_sync` or
  `/ci run rgw_object_lock_sync on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterRGWBucketObjectLockSync$' \
  ./internal/integration/experiments/rgw_multisite_object_lock_sync
```

On a fixed image, injected into the package's `image` variable, which must
print `result=NOT_REPRODUCED` (Tentacle 20.2.5 here; Umbrella 21.2.0,
`quay.io/ceph/ceph:v21.2.0`,
is fixed too):

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_multisite_object_lock_sync_test.image=quay.io/ceph/ceph:v20.2.5" \
  -run '^TestMultiClusterRGWBucketObjectLockSync$' \
  ./internal/integration/experiments/rgw_multisite_object_lock_sync
```

The role image variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RGW_IMAGE`, ...) do
not select the case's images. The log names the image (`RGW_LOCK image=`),
the Ceph version of both clusters (`RGW_LOCK ceph=`), each zone's ID and
gateway binary (`RGW_LOCK zone=`), the canary (`RGW_LOCK canary`), every
configuration read (`RGW_LOCK read step=`) with the raw body of the reads
that are judged (`RGW_LOCK raw`), the versioning read-back
(`RGW_LOCK versioning`), each metadata checkpoint (`RGW_LOCK checkpoint`) and
the bucket record in each zone (`RGW_LOCK stored`).

## Observations and controls

1. Condition: zone `b`'s sync becomes ready, both buckets exist in zone `b`,
   and the canary object written to `tc-plain` in zone `a` is read from zone
   `b` with its bytes. That shows the data path apart from any
   configuration. Before a rule is applied, `tc-lock` answers "enabled, no
   rule" in zone `a`, and zone `a` reads back its versioning as `Enabled`
   (`GET ?versioning`, `judgeVersioning`): object lock requires versioning,
   and a bucket that reports object lock without it is not the prerequisite
   the zones are compared for. Zone `b`'s versioning answer is recorded.
   Otherwise the test fails with `condition not established`.
2. Per step (`initial`, then `update`): the configuration is applied in zone
   `a` and zone `a` must read it back (`judgeSource`), again
   `condition not established` otherwise.
3. Metadata checkpoint, per step: a marker bucket is created in zone `a`
   after the configuration; zone `b` must report its metadata caught up
   (`Fixture.WaitSyncReady`) and must answer the marker bucket within 3
   minutes. Metadata written after the configuration has then arrived. A
   checkpoint that is not reached is `another fault`, not a result.
4. Oracle, per step: zone `b` is read until it answers the same
   configuration, for at most 2 minutes after the checkpoint. The last read
   is judged by meaning (enabled, mode, days, years), not by its XML
   (`judgeDestination`). Only an answer about the configuration is the
   defect: `404 ObjectLockConfigurationNotFoundError`, or another
   configuration. A transport failure, a refusal such as `403 AccessDenied`,
   `NoSuchBucket` or an unsupported operation is `another fault`. The
   response body of zone `a`'s read-back and of zone `b`'s last read is
   logged as received, cut to 1024 bytes and without any request header
   (`RGW_LOCK raw`), because the parsed fields cannot tell every body
   apart: a rule in a layout the reader does not know parses like no rule.
   The bodies of the other polls are not logged.
5. Control, per step: `tc-plain` must answer
   `404 ObjectLockConfigurationNotFoundError` in both zones, and zone `a` is
   read once more after the wait. Otherwise `control failed` or
   `condition not established`.
6. The `update` step runs only after the first configuration was answered by
   both zones, so that it tests a change and not a second initial sync. When
   it does not run, the log says so (`RGW_LOCK step=update not run`).

## Reading the result

- **REPRODUCED:** a step's symptom starts with `observed defect`: zone b
  answers no configuration (`404 ObjectLockConfigurationNotFoundError`, or
  `200` enabled without a rule, which is what it answered before the rule was
  applied) or another one, after the checkpoint was reached. When the
  `initial` step reproduces, the `update` step is not run
  (`RGW_LOCK step=update not run`). The bucket record then holds no object
  lock configuration (`metadata-has-obj_lock=false`).
- **NOT_REPRODUCED:** zone b answers `GOVERNANCE`, 1 day, after the first
  checkpoint and `COMPLIANCE`, 1 year, after the second. Check that both
  gateways are of the build under test (`RGW_LOCK zone=`).
- **Test failure (no result):** `condition not established` (the data path,
  the source read-back or the versioning prerequisite does not hold),
  `control failed` (the control bucket does not answer
  `404 ObjectLockConfigurationNotFoundError`), `another fault` (a checkpoint
  is not reached, or zone b does not answer about the configuration at all),
  or the clusters or the realm do not start.

Native results (Linux amd64 host, Docker, aggregate images, one run each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded) | `REPRODUCED`: zone b answers `200` enabled without a rule after the `initial` checkpoint; `update` not run | 216 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph:v20.2.5@sha256:4337ac8ac81d…` | `NOT_REPRODUCED` | 101 s |
| Umbrella 21.2.0, `quay.io/ceph/ceph:v21.2.0@sha256:c9e6ed4ac3fc…` | `NOT_REPRODUCED` | 79 s |

Earlier, failing-test results on the pinned role images of the shared
recipe agree: 20.2.4 (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw
`aac25fc48b53…`, 217 s) and 19.2.5 (essential `43ee23624e7a…`, client
`a69e1319ce88…`, rgw `1fbe1bd4ecc0…`, 224 s) reproduced; 21.2.0 (essential
`119180026952…`, client `e5f7edab88b6…`, rgw `9ef3fb34f7c4…`, 80 s) and the
20.2.5 official image (100 s) did not.

## Cleanup and limits

The cleanup is registered before the first bucket is created and deletes, in
zone `a`, the canary object and exactly the buckets whose creation answered
200; a failure is reported as `RGW_LOCK cleanup failed` separately from the
result. `tc-lock` never holds an object, so no retained object version is
created and nothing has to be bypassed to remove it. The realm and both
clusters are terminated by their own cleanups.

The case compares configuration only. It says nothing about retention being
enforced in either zone, about the retention of replicated object versions,
or about a configuration applied in the zone that is not the metadata
master. Not run: arm64, the host network profile, more than two zones, a
zone added after the configuration, and a gateway restart between the
configuration and the read.
