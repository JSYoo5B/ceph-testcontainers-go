# RGW lifecycle expiration header for plain and versionId requests

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWLifecycleExpirationHeader`, optional batch
`rgw_expiration_header`. Pure tests of its helpers: `TestExpHeaderParsing`
and `TestExpExpectedRoundsToTheFollowingMidnight` (batch `pure`).

## Use case and native contract

An application reads `x-amz-expiration` from GET or HEAD to tell when the
lifecycle configuration will expire an object. In S3 the header reports the
current-version `Expiration` action that applies to the object, for a request
that names no version. A request for an explicit `versionId`, even of the
latest version, gets no header, and a `NoncurrentVersionExpiration` action
never produces one:

| Bucket rule (prefix `docs/`) | Request | S3 semantics |
| --- | --- | --- |
| `NoncurrentVersionExpiration` 30 days only | plain key | no header |
| | `versionId` of the current version | no header |
| | `versionId` of the older version | no header |
| `Expiration` 7 days and `NoncurrentVersionExpiration` 90 days | plain key | `expiry-date` = the midnight (UTC) after the current version's modification time plus 7 days, `rule-id` = the rule |
| | `versionId` of the current version | no header |
| | `versionId` of the older version | no header |
| | plain key outside the prefix | no header |

The defect: Tentacle 20.2.4 builds the header from the object key that RGW
resolved, which on a versioned bucket carries the current version's instance
even for a plain request. `s3_expiration_header` then takes every request
for a versionId request and reports the `NoncurrentVersionExpiration` action
with the version's modification time: the plain and the explicit-version
requests all report the noncurrent days (30, or 90 next to the 7-day
`Expiration`). Tentacle 20.2.5 passes the request's own `versionId` (empty
for a plain request), returns no header for an explicit version, and uses
only the `Expiration` action. Umbrella 21.2.0 has the same behaviour (it
passes the request's object key, `s->object_key`).

Upstream references:

- Release note of [Tentacle 20.2.5](https://docs.ceph.com/en/latest/releases/tentacle/)
  (`x-amz-expiration` correction).
- `rgw::lc::s3_expiration_header` in
  [`src/rgw/rgw_lc.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/rgw_lc.cc)
  and `get_s3_expiration_header` in
  [`src/rgw/rgw_rest_s3.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/rgw_rest_s3.cc);
  the unit test `ExpHdr.S3ExpirationHeaderCurrentVersionOnly` in
  `src/test/rgw/test_rgw_lc.cc` covers the rule combinations without HTTP.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user), plain HTTP. Image roles `essential`, `client` and `rgw`, all of one
  release. Bridge networking.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- No lifecycle processing, clock change or deletion is needed: the case
  reads response metadata only, with day counts far in the future.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: it fails on the published Tentacle 20.2.4
images, the pull request default. It never runs on a pull request, on `main`
or in periodic samples, and never gates anything.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_expiration_header`.
- Comment: `/ci run rgw_expiration_header` or
  `/ci run rgw_expiration_header on umbrella`.

## Running it

```sh
CEPH_TEST_ESSENTIAL_IMAGE=<essential role@sha256:...> \
CEPH_TEST_CLIENT_IMAGE=<client role@sha256:...> \
CEPH_TEST_RGW_IMAGE=<rgw role@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWLifecycleExpirationHeader$' \
  ./internal/integration/experiments/rgw_lifecycle_expiration_header
```

Resolve the role images with `run_image_matrix.py --variant official
--layout roles --release <release> --roles essential,client,rgw
--prepare-only` (see [Testing](../../../../docs/contributing/testing.md)).
Tentacle 20.2.5 has no published role images; run it with
`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v20.2.5@sha256:<index digest>` instead of
the role variables. Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_expiration_header \
  --package ./internal/integration/experiments/rgw_lifecycle_expiration_header \
  --directory artifacts/rgw-expiration-header
```

Record the image digests with the result; the log names the Ceph version
(`RGW_EXP ceph=` and the daemon versions), every version ID with its
modification time, the read-back rules and the GET and HEAD header of every
request.

## Observations and controls

1. Condition: versioning reads back `Enabled`, each key has two distinct
   non-null versions, and the lifecycle configuration reads back exactly the
   installed rule IDs, prefixes and day counts.
2. Every request is sent as GET and as HEAD: GET must return the requested
   version's exact bytes and both must name that version; HEAD must carry
   the same header as GET.
3. The header is parsed, not compared as a string: the date must be a GMT
   midnight, and it must equal the rounding that S3 and RGW use (drop the
   time of day of the modification time, add the days and one day).
4. Control: on the bucket with an `Expiration` action, the plain request
   must report it (the rule is eligible) and a key outside the prefix must
   get no header.

## Reading the result

- **Pass:** every request followed the table. Tentacle 20.2.5 and Umbrella
  21.2.0 pass.
- **Observed bug:** every request for a `docs/` key, plain or explicit,
  carries a header with the rule ID whose date is the modification time plus
  the `NoncurrentVersionExpiration` days (30 on the first bucket, 90 instead
  of 7 on the second); the key outside the prefix has none. Tentacle 20.2.4
  and Squid 19.2.5 do this.
- **Not reproduced:** a pass on a build claimed to be affected.
- **Unavailable prerequisite:** the cluster or the gateway does not start, or
  the lifecycle configuration is refused.
- **Condition not established:** versioning, a version ID, the read-back
  rules, or a version's bytes do not match. The test fails with
  `condition not established`.

## Cleanup and limits

The cleanup deletes every listed version and delete marker by version ID,
then each bucket's lifecycle configuration and the buckets, and reports a
failure as `RGW_EXP cleanup failed` separately from the result. The cluster is
removed by its cleanup. Not run: arm64, `Date`-based and tag-filtered rules,
several overlapping rules, versioning-suspended buckets and null versions,
delete markers, and `x-amz-expiration` on PUT, COPY and multipart completion
responses.
