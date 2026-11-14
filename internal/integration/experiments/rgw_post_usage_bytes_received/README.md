# RGW usage log bytes received for a browser form upload

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWPostUploadUsageBytesReceived`, optional batch
`rgw_post_usage_bytes_received`. Pure tests of the usage log reader, of the
comparison of two samples, of the logging prerequisite, of the judgement and
of the policy document (`TestParseUsageReadsTheBucketOfTheUser`,
`TestParseUsageRefusesWhatItCannotRead`,
`TestUsageGrewRefusesTotalsThatWentDown`,
`TestRequireLoggingNeedsTheCreationAndNoUpload`,
`TestUploadedReturnsTheCategoryOfTheBucket`, `TestJudgeUploads` and
`TestPostPolicyNamesEveryCondition`, batch `pure`).

## Use case and native contract

A service bills or limits its tenants by the gateway's usage log
(`radosgw-admin usage show`, the admin API's `/admin/usage`): per user,
bucket and operation it holds the operations counted and the bytes the
gateway sent and received. An upload is expected to add its payload to
`bytes_received` whichever S3 upload it used. S3 has two uploads that a
holder of a signed grant, without credentials, can send: a presigned `PUT`
of the object, and a browser form upload, a `POST` to the bucket whose
`multipart/form-data` body carries a signed policy and the file.

The trigger is the form upload. The gateway counts received bytes only
while its accounting is switched on, and an affected build switches it on
around the body read of a `PUT` but leaves it off, as it is when a request
starts, for the reads of a form body, so the usage log holds `post_obj`
with one successful operation and `bytes_received` 0. The fix switches the accounting on around every read of
the form body, so the log holds what the gateway read of the request body:
the payload and the form around it
([tracker](https://tracker.ceph.com/issues/76157), in Tentacle since
20.2.5 and in Umbrella 21.2.0).

| User and bucket | Upload | Role |
| --- | --- | --- |
| `tc-usage-put` | presigned `PUT` of `payload`, 1 MiB | control |
| `tc-usage-post` | presigned `POST` with the same 1 MiB as its file | trigger |

Each upload has its own user and bucket, so the two usage entries differ by
user, bucket and category (`put_obj`, `post_obj`).

## Requirements

- One cluster with one OSD and one gateway, plain HTTP, both from the
  package's `image`, one aggregate image. Bridge networking.
- Usage logging is off by default. The case sets `rgw_enable_usage_log`,
  `rgw_usage_log_tick_interval` 1 and `rgw_usage_log_flush_threshold` 1 for
  the gateway before it starts (`override.SetConfig`).
- The presigned requests are built by the test (SigV4 in the query for the
  `PUT`, a signed policy for the `POST`) and sent without an `Authorization`
  header. The usage log is read with `Gateway.Admin`
  (`usage show --uid <user> --show-log-entries=true`).
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps fail
the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rgw_post_usage_bytes_received result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it (the default checks of a
pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Experiments" -f case=rgw_post_usage_bytes_received`
  requires `REPRODUCED`; adding `-f image=quay.io/ceph/ceph:v20.2.5`
  requires `NOT_REPRODUCED` on Tentacle 20.2.5 afterwards.
  `gh workflow run "Native regressions" -f batch=rgw_post_usage_bytes_received`
  reports its pass or failure only.
- Comment: `/ci run rgw_post_usage_bytes_received` or
  `/ci run rgw_post_usage_bytes_received on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWPostUploadUsageBytesReceived$' \
  ./internal/integration/experiments/rgw_post_usage_bytes_received
```

On a fixed image, injected into the package's `image` variable, which must
print `result=NOT_REPRODUCED` (Tentacle 20.2.5 here; Umbrella 21.2.0,
`quay.io/ceph/ceph:v21.2.0`, is fixed too):

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_post_usage_bytes_received_test.image=quay.io/ceph/ceph:v20.2.5" \
  -run '^TestRGWPostUploadUsageBytesReceived$' \
  ./internal/integration/experiments/rgw_post_usage_bytes_received
```

The role image variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RGW_IMAGE`, ...) do
not select the case's image. The log names the image
(`RGW_POST_USAGE image=`), the gateway binary (`RGW_POST_USAGE gateway=`),
the usage log of each bucket before the uploads
(`RGW_POST_USAGE usage step=created`), the sizes of the payload and of the
`POST` body (`RGW_POST_USAGE uploads`) and the usage log of each bucket once
it holds the upload (`RGW_POST_USAGE usage step=uploaded`).

## Observations and controls

1. Condition, usage logging: after each user created its bucket, the usage
   log of that bucket is read until it holds `create_bucket`, for at most 2
   minutes. It must hold exactly one successful creation and neither
   `put_obj` nor `post_obj` (`requireLogging`). That shows that usage
   logging is enabled and that its flush reaches the log before an upload is
   sent; otherwise the test fails with `condition not established`.
2. When: the `PUT` must answer 200 and the `POST` 204, and a signed `GET` of
   each object must return the payload byte for byte, again
   `condition not established` otherwise.
3. The usage log of each bucket is read until it holds its upload's
   category, for at most 2 minutes. A read is a condition that is waited
   for, not a delay. Every read of a bucket is compared with the one before
   it (`usageGrew`): a category that is gone or a counter that went down
   means the log was trimmed or is not the same log, and the test fails with
   `usage not observed`. So does a bucket that holds the other upload's
   category.
4. The reader (`parseUsage`) takes a counter only when the report prints it:
   a category without `bytes_received`, `bytes_sent`, `ops` or
   `successful_ops`, or with a null one, is refused and never read as zero.
   An entry of another user is refused; the records of one bucket, one per
   hour, are added up.
5. Control: `put_obj` must hold one successful operation and exactly the
   payload, 1048576, as `bytes_received`. That shows that the log records
   what an upload receives; otherwise `control not established`.
6. Oracle (`judgeUploads`): `post_obj` must hold one successful operation.
   Its `bytes_received` is the symptom when it is 0, and the fixed form when
   it is at least the payload and at most the body of the `POST` request,
   which is the payload plus the form (1512 bytes more in the runs below). Any other
   count (part of the payload, the form without the file, more than was
   sent) is `another state` and fails the test.

Rule basis:

| Form | Native | Source |
| --- | --- | --- |
| `put_obj`: `bytes_received` equals the payload | 19.2.5, 20.2.4, 20.2.5 and 21.2.0: 1048576 | `RGWPutObj_ObjStore::get_data`, `src/rgw/rgw_rest.cc` lines 1087 to 1098 at v20.2.4: accounting is on around the body read only |
| `post_obj`: `bytes_received` 0 | 19.2.5 and 20.2.4 | `RGWPostObj_ObjStore::read_with_boundary`, `src/rgw/rgw_rest.cc` lines 1235 and 1267 at v20.2.4: the two body reads do not switch accounting on, and `AccountingFilter` (`src/rgw/rgw_client_io_filters.h` line 28) starts with it off |
| `post_obj`: `bytes_received` between the payload and the request body | 20.2.5 and 21.2.0: 1050088, the whole body | the same function at v20.2.5, lines 1240 to 1242 and 1274 to 1276: accounting is on around both reads |
| `entries[].user`, `entries[].buckets[].bucket` and `.categories[]` with `category`, `bytes_sent`, `bytes_received`, `ops`, `successful_ops` | 19.2.5, 20.2.4, 20.2.5 and 21.2.0 | `RGWUsage::show` and `dump_usage_categories_info`, `src/rgw/rgw_usage.cc` at v20.2.4 |
| categories `create_bucket`, `put_obj`, `post_obj` | 19.2.5, 20.2.4, 20.2.5 and 21.2.0 | the operations' `name()`, `src/rgw/rgw_op.h` lines 1205, 1393 and 1449 at v20.2.4 |
| `create_bucket` with one successful operation before any upload | 19.2.5, 20.2.4, 20.2.5 and 21.2.0 | not read in source |

## Reading the result

- **REPRODUCED:** the usage log holds one successful `post_obj` with
  `bytes_received` 0 for the form upload, beside one successful `put_obj`
  with the payload for the `PUT`.
- **NOT_REPRODUCED:** `post_obj` holds at least the payload and at most the
  request body. Check that the gateway is of the build under test
  (`RGW_POST_USAGE gateway=`).
- **Test failure (no result):** `condition not established` (usage logging
  is not enabled or not flushed, an upload was refused or not read back, or
  the log counts another number of operations), `control not established`
  (the `PUT` does not show its payload), `usage not observed` (the log does
  not hold an upload within the bound, or went down between two reads),
  `another state` (a `POST` count that is neither form), or the cluster or
  the gateway does not start.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all` | `REPRODUCED`: `post_obj` 0, `put_obj` 1048576 | 33 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph:v20.2.5` | `NOT_REPRODUCED`: `post_obj` 1050088, `put_obj` 1048576 | 33 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all` | `REPRODUCED`: `post_obj` 0, `put_obj` 1048576 | 39 s |
| Umbrella 21.2.0, `quay.io/ceph/ceph:v21.2.0` | `NOT_REPRODUCED`: `post_obj` 1050088, `put_obj` 1048576 | 34 s |

## Cleanup and limits

The case creates two users, two buckets and two objects in a cluster of its
own and leaves the usage log as it is; the cluster and the gateway are
terminated by their cleanups, and nothing outside them is changed.

The case reads `bytes_received` of one form upload of 1 MiB. It says
nothing about `bytes_sent`, about the operations log, about a form upload
that is refused, about Swift's form POST, or about anything else that
counts a request's bytes. Not run: arm64,
the host network profile, HTTPS and a payload larger than the gateway reads
at once (`rgw_max_chunk_size`, 4 MiB by default).
