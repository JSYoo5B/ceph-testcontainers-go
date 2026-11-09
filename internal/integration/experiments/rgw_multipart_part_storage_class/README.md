# RGW part reads of a multipart object in a non-default storage class

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWMultipartPartStorageClass`, optional batch
`rgw_multipart_part_class`. Pure tests of the part read classification
and of the out-of-range control (`TestPartOK` and `TestBeyondRefused`, batch
`pure`).

## Use case and native contract

An application stores large objects with a multipart upload in a storage
class other than `STANDARD`, for example a colder tier on its own pool, and
later reads ranges of them by part, as parallel downloaders do with
`GET ?partNumber=N` and `HEAD ?partNumber=N`. Every valid part number of a
completed upload must answer that part: its length, the object's part count
(`x-amz-mp-parts-count`) and, for a GET, its bytes. A part number beyond the
last part must be refused as out of range.

The zone of the case adds the storage class `COLD` to `default-placement` on
its own data pool, `tc-mp.cold.data`. Two buckets each hold one object made
of two parts with distinguishable bytes: a 5 MiB first part (the smallest
valid non-final part) and a 64 KiB second part.

| Subtest | Upload | Contract |
| --- | --- | --- |
| `cold-storage-class` | `x-amz-storage-class: COLD` on the initiation | GET and HEAD of parts 1 and 2 answer the part (200 or 206, its length, parts count 2, GET its bytes); part 3 is refused |
| `standard-control` | no storage class (`STANDARD`) | the same |

The trigger: the parts of an upload in `COLD` are written by the
`default-placement/COLD` placement rule, in its data pool; the case reads
that rule from the object's manifest. A whole-object GET follows the
manifest and reads every part. The reported mechanism, not confirmed here
in the source, is that a read by part number looks up the head of that part
in the pool of the default placement, where a `COLD` part head is not, so
RGW answers `400 InvalidPart` for a part that exists. The data itself is
intact, and the `STANDARD` object, whose parts are in the default pool,
answers every part.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user), plain HTTP. Image roles `essential`, `client` and `rgw`, all of one
  release, or one aggregate image (`CEPH_TEST_IMAGE`). Bridge networking.
- The case creates the pool `tc-mp.cold.data` (`ceph.Container.CreatePool`)
  and edits the gateway's own zone and zonegroup with `radosgw-admin
  zonegroup placement add` and `zone placement add` (`Gateway.Admin`), then
  restarts the gateway (`Stop` and `Start`) so that it loads them. No realm
  or period is involved.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: it fails on every published release image
and on the Tentacle 20.2.5 official image. No automatic run includes it (the
default checks of a pull request, pushes to `main`, periodic samples), and it
never gates anything. A maintainer can run it on demand, also on a pull
request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_multipart_part_class`.
- Comment: `/ci run rgw_multipart_part_class` or
  `/ci run rgw_multipart_part_class on umbrella`.

## Running it

Export the images of one release first, as
[Images for the RGW S3 cases](../../../../docs/contributing/testing.md#images-for-the-rgw-s3-cases)
describes, then:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWMultipartPartStorageClass$' \
  ./internal/integration/experiments/rgw_multipart_part_storage_class
```

Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_multipart_part_class \
  --package ./internal/integration/experiments/rgw_multipart_part_storage_class \
  --directory "artifacts/rgw-multipart-part-class-$(date -u +%Y%m%dT%H%M%SZ)"
```

Record the image digests with the result; the log names the Ceph version
(`RGW_MP ceph=`), the zone placement read back (`RGW_MP zone=`), each
object's head and tail placement rules (`RGW_MP placement`), its whole read
(`RGW_MP whole`) and every part read (`RGW_MP read`).

## Observations and controls

1. Condition: the zone reads back `COLD` on `tc-mp.cold.data` and `STANDARD`
   on another pool. The `COLD` object's manifest names the tail placement
   rule `default-placement/COLD`; the `STANDARD` object's does not name
   `COLD`. Each object answers HEAD 200 with its storage class and a
   whole-object GET of exactly the bytes written. Otherwise the test fails
   with `condition not established` and reads no part.
2. Oracle, per object: GET and HEAD of parts 1 and 2 must answer 200 or 206
   with the part's length and `x-amz-mp-parts-count: 2`; a GET must return
   the part's bytes (`partOK`).
3. Refusal control: GET of part 3 must answer `400` with the S3 code
   `InvalidPart`, which both objects answer on every measured build
   (`beyondRefused`). Another error, such as `403 AccessDenied` or
   `503 ServiceUnavailable`, does not show that the part number was out of
   range and fails the control.

## Reading the result

The results below ran on the pinned role images of the shared recipe: 20.2.4
(essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw `aac25fc48b53…`),
21.2.0 (essential `119180026952…`, client `e5f7edab88b6…`, rgw
`9ef3fb34f7c4…`) and 19.2.5 (essential `43ee23624e7a…`, client
`a69e1319ce88…`, rgw `1fbe1bd4ecc0…`), and on the 20.2.5 official image
`sha256:4337ac8ac81d…` as the aggregate. Linux amd64, bridge.

- **Observed bug:** `cold-storage-class` answers `400 InvalidPart` for GET of
  parts 1 and 2 and 400 for their HEAD, with the 238-byte error document as
  the length and no parts count, while the whole-object GET returns all
  5,308,416 bytes; `standard-control` passes with 200, the exact lengths
  (5,242,880 and 65,536), parts count 2 and the exact bytes. Squid 19.2.5,
  Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0 all do this (about 38 s
  each).
- **Pass:** both subtests meet the contract. No tested build passes.
- **Not reproduced:** a pass on a build claimed to be affected. Check the
  placement evidence in step 1.
- **Unavailable prerequisite:** the cluster or the gateway does not start,
  the zone edits are refused, or the gateway does not restart.

## Cleanup and limits

The cleanup is registered before the first bucket is created and receives
only the buckets whose creation answered 200. It aborts an upload left open,
deletes the objects and the buckets, and reports a failure as
`RGW_MP cleanup failed` separately from the result. The zone edits and the
pool belong to the cluster, which is removed by its cleanup. Not run: arm64,
the host network profile, ranged GETs, more than two parts, objects copied
or transitioned into the class, other placement targets, encryption and
compression.
