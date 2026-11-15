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
  user), all on the image `image` (see [Running it](#running-it)). Bridge
  networking; nothing depends on the network mode.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- The case's steps share a 10-minute deadline that starts before cluster
  startup. Startup (`harness.NewSingleImageCluster`) runs on the test context,
  so the deadline does not cut it short, but the time it takes is spent from
  the 10 minutes and the steps after it get what remains. The cluster's
  termination in cleanup has its own two-minute deadline, so cleanup is not
  bounded by the deadline and can take the test past 10 minutes. The outer
  bound is the batch's 20-minute `go test` timeout.

## Tier and triggers

Known-defect reproducer, optional batch `rgw_conditional_writes` with a
20-minute budget; no automatic run includes it. One run takes about 35
seconds and, on its default image, passes and reports `REPRODUCED` (see
[Result line](#result-line)). It is an `Experiments` case
(`case=rgw_suspended_conditional_writes`); Tentacle 20.2.5
(`quay.io/ceph/ceph:v20.2.5`) and Umbrella 21.2.0
(`quay.io/ceph/ceph:v21.2.0`) are fixed images for it.
`/ci run rgw_conditional_writes` runs the default image. The triggers, the
result checks and the image injection are the shared ones
of [Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case). Its validator tests are Docker-free `code/pure` tests
that Code checks runs on every change.

## Running it

The default image is the Tentacle 20.2.4 aggregate
(`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all`), which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWSuspendedConditionalWrites$' \
  ./internal/integration/experiments/rgw_suspended_conditional_writes
```

On a fixed image, Umbrella 21.2.0:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_suspended_conditional_writes_test.image=quay.io/ceph/ceph:v21.2.0" \
  -run '^TestRGWSuspendedConditionalWrites$' \
  ./internal/integration/experiments/rgw_suspended_conditional_writes
```

The batch runs the same test through the tag runner with
`tag_scenarios.py optional --batch rgw_conditional_writes`.

The log names the image (`image=` in each result line) and the Ceph version
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

## Result line

The test prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines), `case=rgw_suspended_conditional_writes`.

`REPRODUCED`: a step of the sequence on the suspended bucket, with either
writer, answers another status than AWS semantics want; `symptoms` lists
each such step with its status. `NOT_REPRODUCED`: every step on the
suspended bucket does. The `never-versioned` control, the create-once
controls, the effect checks of step 4 and the final listing of step 5 must
hold for any result to print.

Standalone runs, Linux amd64 host, Docker, bridge, one run each:

| Image | Result | Test time |
| --- | --- | --- |
| 20.2.4 aggregate `sha256:2e57651a5aa7…`, the default image when recorded | `REPRODUCED`: `existing-if-match` (412), `deleted-if-match` (412 instead of 404) and `deleted-if-none-match` (412) for both writers, six symptoms | 34.6 s |
| 21.2.0 official `sha256:c9e6ed4ac3fc…` | `NOT_REPRODUCED` | 35.3 s |
| 20.2.5 official `sha256:4337ac8ac81d…` | `NOT_REPRODUCED` | 35.1 s |

## Reading the result

- **Pass:** every step on both buckets followed AWS semantics. Tentacle
  20.2.5 and Umbrella 21.2.0 pass (about 35 seconds).
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
buckets and the cluster is removed by its cleanup. Not run: arm64, multipart uploads of several parts, `If-Match` on a key
whose OLH points at an older version, and copy or tagging writes with
preconditions.
