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
  user), plain HTTP, all on the image `image` (see [Running it](#running-it)).
  Bridge networking.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- No lifecycle processing, clock change or deletion is needed: the case
  reads response metadata only, with day counts far in the future.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Known-defect reproducer, optional batch `rgw_expiration_header` with a
20-minute budget; no automatic run includes it. It is an `Experiments` case;
Umbrella 21.2.0 (`quay.io/ceph/ceph:v21.2.0`) is a fixed image for it, and
`/ci run rgw_expiration_header on umbrella` runs the batch there. Its
Docker-free header parsing tests (batch `code/pure`) run in Code checks on
every change. The triggers, the result checks and the image injection are the
shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, the Tentacle 20.2.4 aggregate
(`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`), which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWLifecycleExpirationHeader$' \
  ./internal/integration/experiments/rgw_lifecycle_expiration_header
```

On a fixed image, for example Umbrella 21.2.0:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_lifecycle_expiration_header_test.image=quay.io/ceph/ceph:v21.2.0" \
  -run '^TestRGWLifecycleExpirationHeader$' \
  ./internal/integration/experiments/rgw_lifecycle_expiration_header
```

The log names the image (`image=` in each result line) and the Ceph version
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

## Result line

The test passes whatever the gateway did and prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines),
`case=rgw_lifecycle_expiration_header`.

`REPRODUCED`: a request that must carry no `x-amz-expiration` carries one
(the plain and explicit-version requests of `noncurrent-only`, the
explicit-version requests of `current-and-noncurrent`), or the plain request
of `current-and-noncurrent` reports another rule or date than its 7-day
`Expiration`; `symptoms` lists each such request with its header.
`NOT_REPRODUCED`: every request follows the table. The controls must hold
for any result to print: HEAD carries the header GET carries, the eligible
plain request carries a well-formed header, and the key outside the prefix
carries none.

Standalone runs, Linux amd64 host, Docker, bridge, one run each:

| Image | Result | Test time |
| --- | --- | --- |
| 20.2.4 aggregate `sha256:2e57651a5aa7…`, the default image when recorded | `REPRODUCED`: six symptoms, every `docs/` request carries the noncurrent days (30, or 90 instead of 7) | 30.9 s |
| 21.2.0 official `sha256:c9e6ed4ac3fc…` | `NOT_REPRODUCED` | 30.8 s |

## Reading the result

The results below ran on the pinned images of the shared recipe: the 20.2.4
role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw
`aac25fc48b53…`), the 20.2.5 aggregate `sha256:4337ac8ac81d…`, the 21.2.0
role images (essential `119180026952…`, client `e5f7edab88b6…`, rgw
`9ef3fb34f7c4…`) and the 19.2.5 role images (essential `43ee23624e7a…`,
client `a69e1319ce88…`, rgw `1fbe1bd4ecc0…`).

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

The cleanup is registered before the first bucket is created and receives
only the buckets whose creation answered 200. It deletes every listed
version and delete marker by version ID,
then each bucket's lifecycle configuration and the buckets, and reports a
failure as `RGW_EXP cleanup failed` separately from the result. The cluster is
removed by its cleanup. Not run: arm64, `Date`-based and tag-filtered rules,
several overlapping rules, versioning-suspended buckets and null versions,
delete markers, and `x-amz-expiration` on PUT, COPY and multipart completion
responses.
