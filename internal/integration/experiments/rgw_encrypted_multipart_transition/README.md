# Reading encrypted multipart objects after lifecycle transition

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWEncryptedMultipartTransition`, optional batch
`rgw_encrypted_multipart_transition`. Pure tests of the read judgement
(`TestFirstDifference` and `TestExact`, batch `pure`).

## Use case and native contract

An application stores SSE-C objects with multipart uploads and lets a
lifecycle rule move them to a colder storage class on another pool. After
the transition every GET with the customer key must return the original
plaintext: the whole object, and a range that crosses a part boundary.

RGW encrypts each part of a multipart upload on its own, so decryption needs
the part lengths. It reads them from the object's manifest, or from the
crypt attribute `user.rgw.crypt.part-lengths` when the manifest no longer
shows them. A lifecycle transition copies the stored (encrypted) bytes into
a new object in the target pool, whose manifest has no parts. Before the
correction the transition did not write the attribute, so the gateway
decrypts the copy as a single part: the plaintext is wrong from the last
incomplete cipher block of the first part on, while the status, the length
and the Content-Range stay correct. The correction stores the part lengths
from the source manifest in the attribute during the transition. See
[Ceph Tracker 23264](https://tracker.ceph.com/issues/23264) and the
[Tentacle correction](https://github.com/ceph/ceph/commit/ec11f6284d0810708ff65d5c5b8dc29294c0b672),
first tagged in 20.2.2 (Squid: first tagged in 19.2.7; 20.2.1 and 19.2.6
lack it).

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with the generated
  user and the test's own certificate), all on the image `image` (see
  [Running it](#running-it)). SSE-C requests go over HTTPS, and the test
  trusts the fixture's certificate authority.
- The case disables the background lifecycle worker, sets a two-second
  lifecycle debug day, creates the pool `lc.cold`, adds the storage class
  `COLD` on it to `default-placement` in the zonegroup and the zone, and
  restarts the gateway so that it loads them. No realm or period is
  involved.
- The case's steps share a six-minute deadline that starts before cluster
  startup. Startup (`harness.NewSingleImageCluster`) runs on the test context,
  so the deadline does not cut it short, but the time it takes is spent from
  the six minutes and the steps after it get what remains. In cleanup the
  purge of the bucket has its own 30-second deadline and the cluster's
  termination its own two-minute one, so cleanup is not bounded by the
  deadline and can take the test past six minutes. The outer bound is the
  batch's 20-minute `go test` timeout.

## Tier and triggers

Known-defect reproducer, optional batch `rgw_encrypted_multipart_transition`
with a 20-minute budget; it edits the zone and restarts the gateway. No
automatic run includes it. It is an `Experiments` case: dispatch
`gh workflow run Experiments -f case=rgw_encrypted_multipart_transition -f image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all`
to check that the default image reproduces and the fixed one does not. On a
pull request's head, `/ci run rgw_encrypted_multipart_transition` runs the
batch and takes only its pass or failure. Its Docker-free judgement tests
(batch `code/pure`) run in Code checks on every change. The triggers, the
result checks and the image injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, the Tentacle 20.2.1 aggregate
(`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all`), which
reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWEncryptedMultipartTransition$' \
  ./internal/integration/experiments/rgw_encrypted_multipart_transition
```

On the fixed Tentacle 20.2.2 aggregate, or any other image:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_encrypted_multipart_transition_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all" \
  -run '^TestRGWEncryptedMultipartTransition$' \
  ./internal/integration/experiments/rgw_encrypted_multipart_transition
```

The case ignores the image variables of the testing guide. The log names
the Ceph version and the image (`RGW_ET ceph=`), every HEAD and GET
(`RGW_ET before` and `RGW_ET after`), the part layout (`RGW_ET layout`) and
whether each object carries the part-lengths attribute (`RGW_ET ... stat`).

## Observations and controls

Three objects share one layout: a first part of 5,245,951 bytes (3,071
bytes past 5 MiB, so it ends inside a 4 KiB encryption chunk) and a second
part of 81,000 bytes, with distinguishable contents (5,326,951 bytes in all).

| Object | Encryption | Lifecycle |
| --- | --- | --- |
| `move/encrypted`, the subject | SSE-C (AES256) | moved to `COLD` |
| `move/plain`, plaintext control | none | moved to `COLD` |
| `keep/encrypted`, encrypted control | SSE-C with the same key | stays `STANDARD` |

Each object is read with a full GET, which must return 200 and every
original byte, and a GET of bytes 5,245,919-5,246,079 (32 bytes before the
part boundary to 128 after), which must return 206, the exact Content-Range
and those 161 bytes (`exact`).

1. Given: each upload's initiation confirms the requested encryption mode
   and its completion returns a successful result. Before the transition,
   HEAD of every object reports `STANDARD` and its encryption mode, and both
   reads of every object are exact. Otherwise the test fails.
2. When: a lifecycle rule for the prefix `move/` transitions after one debug
   day; after four seconds a manual `radosgw-admin lc process` runs. HEAD
   must report `COLD` for both `move/` objects and `STANDARD` for the
   encrypted control, and both controls must still read exactly. Otherwise
   the transition or the control is not established and the test fails.
3. Then: both reads of the subject are compared with the original bytes.

## Result line

The test passes whatever the subject returned and prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines),
`case=rgw_encrypted_multipart_transition`.

`REPRODUCED`: a read of the transitioned SSE-C object is not exact;
`symptoms` lists each such read with its status, Content-Range, length and
the object offset of the first wrong byte (or a transport failure).
`NOT_REPRODUCED`: both reads of the transitioned SSE-C object are exact.
A failed upload, unavailable placement, a transition that did not happen or
a failed control fails the test and prints no result: that is a missing
prerequisite, not `NOT_REPRODUCED`.

Linux amd64 host, Docker 29.9.1, bridge, one standalone run per image.

| Image | Result | Test time |
| --- | --- | --- |
| `official-20.2.1-all`, the default | `REPRODUCED`: both reads answer 200 and 206 with the right length and Content-Range, wrong from offset 5,245,936 (15 bytes before the boundary) on | 49.0 s, 48.4 s |
| `official-20.2.2-all`, the fixed image | `NOT_REPRODUCED` | 49.0 s, 47.9 s |
| `official-20.2.4-all` | `NOT_REPRODUCED` | 47.6 s |
| `official-19.2.6-all` | `REPRODUCED`, the same symptom at the same offset | 76.3 s |
| `official-19.2.7-all` | `NOT_REPRODUCED` | 74.4 s |

The 20.2.1 and 20.2.2 rows are also two runs through
`run_experiment.py` with the fixed image, as `Experiments` runs them. On
every image the plaintext control and the encrypted control read exactly
after the pass. After the transition the subject carries
`user.rgw.crypt.part-lengths` on 20.2.2, 20.2.4 and 19.2.7 and does not on
20.2.1 and 19.2.6; no object carries it before the transition.

## Cleanup and limits

The bucket cleanup is registered before the bucket is created and purges
its objects with `radosgw-admin bucket rm --purge-objects`; a failure is
reported as `RGW_ET cleanup failed`. The zone edits and the pool belong to
the cluster, which its cleanup removes. Not run: arm64, the host network
profile, SSE-S3 and SSE-KMS, more than two parts, a part that ends on a
chunk boundary, transitions to a cloud tier, multisite replication of the
transitioned object and the background lifecycle worker.
