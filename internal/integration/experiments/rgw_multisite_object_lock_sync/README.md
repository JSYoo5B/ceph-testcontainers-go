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
  Image roles `essential`, `client` and `rgw`, all of one release, or one
  aggregate image (`CEPH_TEST_IMAGE`). Bridge networking.
- Requests are signed by the test's own SigV4 client with each gateway's
  generated user. The bucket record is read with `Fixture.ZoneAdmin`.
- The test bounds itself to 20 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Unpatched-defect reproducer: it reproduces on the published role images
of Tentacle 20.2.4 and Squid 19.2.5. It passes on the 20.2.4 and 21.2.0 role
images by the verdicts its catalogue lists, and a reproduction fails it on
images the catalogue does not list, 19.2.5 included. No automatic run includes it (the default
checks of a pull request, pushes to `main`, periodic samples), and it never
gates anything. A maintainer can run it on demand, also on a pull request's
head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_object_lock_sync -f release=20.2.4`
  for the affected build, `-f release=21.2.0` for the fixed one.
- Comment: `/ci run rgw_object_lock_sync` or
  `/ci run rgw_object_lock_sync on umbrella`.

## Running it

Export the images of one release first, as
[Images for the RGW S3 cases](../../../../docs/contributing/testing.md#images-for-the-rgw-s3-cases)
describes, then:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterRGWBucketObjectLockSync$' \
  ./internal/integration/experiments/rgw_multisite_object_lock_sync
```

Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_object_lock_sync \
  --package ./internal/integration/experiments/rgw_multisite_object_lock_sync \
  --directory "artifacts/rgw-object-lock-sync-$(date -u +%Y%m%dT%H%M%SZ)"
```

With the 20.2.4 or the 21.2.0 role images exported, the same commands check
the reproducer against the affected or the fixed build that its catalogue
lists (see [Verdict](#verdict)); only the exported image variables differ
between the two runs.

Record the image digests with the result; the log names the Ceph version of
both clusters (`RGW_LOCK ceph=`), each zone's ID and gateway binary
(`RGW_LOCK zone=`), the canary (`RGW_LOCK canary`), every configuration read
(`RGW_LOCK read step=`) with the raw body of the reads that are judged
(`RGW_LOCK raw`), the versioning read-back (`RGW_LOCK versioning`), each
metadata checkpoint (`RGW_LOCK checkpoint`) and the bucket record in each
zone (`RGW_LOCK stored`).

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
   checkpoint that is not reached is `another fault`, not a verdict.
4. Oracle, per step: zone `b` is read until it answers the same
   configuration, for at most 2 minutes after the checkpoint. The last read
   is judged by meaning (enabled, mode, days, years), not by its XML
   (`judgeDestination`). Only an answer about the configuration is the
   symptom: `404 ObjectLockConfigurationNotFoundError`, or another
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
6. The `update` step tests a change of a configuration both zones answered,
   so it runs only after zone `b` answered the `initial` one. When the
   `initial` step finds the symptom, the update is not attempted
   (`RGW_LOCK step=update not run`): zone `b` has no rule for it to replace.

## Verdict

The test reports the unit `rgw_multisite_object_lock_sync` in one
`REPRODUCTION_VERDICT` line, with the observations `initial` and `update`
(steps 2 to 5 for each configuration).

- **REPRODUCED:** zone `b`'s last read of a step answers no configuration
  (`404 ObjectLockConfigurationNotFoundError`) or another one than zone
  `a`'s. A symptom of `initial` concludes the verdict with `update` not
  recorded.
- **NOT_REPRODUCED:** zone `b` answers each configuration of both steps.
- **INCONCLUSIVE:** the reason names the category. Unavailable
  prerequisite: the realm does not start (the clusters not starting ends the
  test before a verdict). Condition not established: step 1 or a source
  read-back does not hold. Control failed: `tc-plain` does not answer
  `404 ObjectLockConfigurationNotFoundError` in a zone. Another fault: a
  checkpoint is not reached, or zone `b` does not answer about the
  configuration at all.

The expected verdict comes from [`reproductions.json`](reproductions.json)
beside the test. The combination whose platform and images equal the ones
the run resolves selects it, and only that verdict passes; a skip or
`INCONCLUSIVE` never does. It lists `20.2.4-roles` (`REPRODUCED`) and
`21.2.0-roles` (`NOT_REPRODUCED`), both on linux/amd64. On any other images
the run is an investigation (`expected=none combination=none`): a
reproduction fails the test, absence passes it and an inconclusive run fails
it.

## Recorded results

The results below ran on the pinned role images of the shared recipe: 20.2.4
(essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw `aac25fc48b53…`),
21.2.0 (essential `119180026952…`, client `e5f7edab88b6…`, rgw
`9ef3fb34f7c4…`) and 19.2.5 (essential `43ee23624e7a…`, client
`a69e1319ce88…`, rgw `1fbe1bd4ecc0…`), and on the 20.2.5 official image
`sha256:4337ac8ac81d…` as the aggregate. Linux amd64, bridge, one run per
release.

- **REPRODUCED** on Tentacle 20.2.4 and Squid 19.2.5. Zone `a` reads back
  `GOVERNANCE`, 1 day; the checkpoint is reached at once (metadata caught
  up, the marker bucket answered by zone `b`); zone `b` answers `200` with
  object lock enabled and no rule in every read over 2 minutes, which is
  also what it answered before the rule was applied. The bucket record holds
  no object lock configuration in either zone (`metadata-has-obj_lock=false`),
  and the control bucket answers `404 ObjectLockConfigurationNotFoundError`
  in both. The `update` step is then not attempted.
- **NOT_REPRODUCED** on Tentacle 20.2.5 and Umbrella 21.2.0: zone `b` answers
  `GOVERNANCE`, 1 day, in the first read after the checkpoint, and
  `COMPLIANCE`, 1 year, in the first read after the second; the bucket record
  holds the configuration in both zones (about 80 s on 21.2.0).

A NOT_REPRODUCED on a build claimed to be affected needs both gateways to be
of that build (`RGW_LOCK zone=`).

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
