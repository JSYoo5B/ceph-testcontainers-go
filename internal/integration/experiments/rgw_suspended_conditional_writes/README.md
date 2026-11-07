# RGW conditional writes on a versioning-suspended bucket

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWSuspendedConditionalWrites`, optional batch
`rgw_conditional_writes`.

## Use case and native contract

An application uses `If-None-Match: *` for create-once and `If-Match: <ETag>`
for compare-and-swap. After the bucket's versioning is suspended, keys written
while it was enabled must follow the same AWS semantics as keys of a bucket
that was never versioned:

| Step | AWS semantics |
| --- | --- |
| `If-None-Match: *` on the existing key | 412 `PreconditionFailed` |
| `If-Match: <current ETag>` | 200, the null version holds the new bytes |
| `If-Match` with a wrong ETag | 412 `PreconditionFailed` |
| DELETE | 204, a null delete marker on the suspended bucket |
| `If-Match: <last ETag>` after the delete | 404 `NoSuchKey` |
| `If-None-Match: *` after the delete | 200 |
| `If-None-Match: *` again | 412 `PreconditionFailed` |
| `If-Match: <new ETag>` | 200 |

The sequence runs twice per bucket: with an ordinary PUT, and with a
single-part multipart upload whose `CompleteMultipartUpload` carries the
precondition.

The defect: `RGWRados::Object::Write::_do_write_meta` resolves the current
version through the OLH only when the write names an instance, and a write on
a suspended bucket never does. Once the head is an OLH (a key written while
versioning was enabled, or a key with a delete marker), the precondition is
checked against the OLH head instead of the current version. Tentacle 20.2.4
has it; Umbrella 21.2.0 follows the OLH whenever the head is one and checks an
empty state when there is no current version. Tentacle 20.2.5 has the same
code as 21.2.0 (source only, not run).

Upstream references:

- [tracker 80325](https://tracker.ceph.com/issues/80325), fix
  [ceph/ceph#71584](https://github.com/ceph/ceph/pull/71584), Umbrella
  [ceph/ceph#72048](https://github.com/ceph/ceph/pull/72048).

Squid 19.2.5 cannot run the contract at all, independently of versioning: it
compares the `If-Match` header with the stored ETag without removing the
quotes, so a quoted ETag never matches; with an unquoted ETag it answers 412
instead of 404 for a missing key; and it reads no precondition for
`CompleteMultipartUpload`. The never-versioned control fails there.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user). Image roles `essential`, `client` and `rgw`, all of one release.
  Bridge networking; nothing depends on the network mode.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: one run takes about 35 seconds and fails on
the published Tentacle 20.2.4 images, the pull request default. No automatic
run includes it (the default checks of a pull request, pushes to `main`,
periodic samples), and it never gates anything. A maintainer can run it on
demand, also on a pull request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_conditional_writes`.
- Comment: `/ci run rgw_conditional_writes` or
  `/ci run rgw_conditional_writes on umbrella`.

## Running it

```sh
CEPH_TEST_ESSENTIAL_IMAGE=<essential role@sha256:...> \
CEPH_TEST_CLIENT_IMAGE=<client role@sha256:...> \
CEPH_TEST_RGW_IMAGE=<rgw role@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWSuspendedConditionalWrites$' \
  ./internal/integration/experiments/rgw_suspended_conditional_writes
```

Resolve the role images with `run_image_matrix.py --variant official
--layout roles --release <release> --roles essential,client,rgw
--prepare-only` (see [Testing](../../../../docs/contributing/testing.md)),
or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_conditional_writes \
  --package ./internal/integration/experiments/rgw_suspended_conditional_writes \
  --directory "artifacts/rgw-conditional-writes-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Record the image digests with the result; the log names the Ceph version
(`RGW_COND ceph=`), every step's native status and error code, and the
version listings.

## Observations and controls

1. Control `never-versioned`: the same sequence, for both writers, on a bucket
   that was never versioned. It must pass for the result to mean anything.
2. Positive control on each bucket and writer: a key first written by
   `If-None-Match: *` is created, and the duplicate is refused with 412.
3. Condition: on the suspended bucket, `?versioning` reads `Enabled` and then
   `Suspended`; each key is a single latest version with the non-null version
   ID returned while versioning was enabled; the DELETE writes a null delete
   marker.
4. Effects: after every write a fresh signed GET reads the key, and a 200
   must carry a quoted strong ETag, since the next `If-Match` step sends it
   as its validator (`TestCondValidator` in the pure batch covers the
   accepted forms). An accepted write must read back its exact bytes, and a
   refused one must leave the key unchanged (or absent); either violation
   fails the step at once as a different defect. A wrong status fails the step, and the sequence goes on
   from the state the GET shows.
5. The final version listing must keep the version written while versioning
   was enabled, not latest, and show one latest null entry consistent with
   the GET: a null version with the current ETag, or a null delete marker.
   No multipart upload of the test may stay pending.

## Reading the result

- **Pass:** every step on both buckets followed AWS semantics. Umbrella
  21.2.0 passes (about 36 seconds).
- **Observed bug:** `never-versioned` passes and `suspended/put` and
  `suspended/multipart` fail at `existing-if-match` (412), `deleted-if-match`
  (412 instead of 404) and `deleted-if-none-match` (412); the later steps are
  not reached. Tentacle 20.2.4 does this (about 35 seconds). The refused writes
  change nothing: the listing keeps the original version and the null delete
  marker.
- **Not reproduced:** a pass on a build claimed to be affected. Check that
  the condition in step 3 was logged.
- **Unavailable prerequisite:** a gateway or user does not start, or the
  `never-versioned` control fails. Squid 19.2.5 fails the control at
  `existing-if-match` (put) and at the multipart create-once control (the
  duplicate completion answers 200).
- **Condition not established:** versioning does not read back as set, a key
  has no non-null version ID, a GET answers 200 without a quoted strong ETag,
  or the DELETE writes no null delete marker. The
  test fails with `condition not established`.

## Cleanup and limits

Refused multipart uploads are aborted at once; objects stay in the test-owned
buckets and the cluster is removed by its cleanup. Not run: arm64, Tentacle
20.2.5 (source only), multipart uploads of several parts, `If-Match` on a key
whose OLH points at an older version, and copy or tagging writes with
preconditions.
