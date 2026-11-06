# RGW logical size of a copy from a compressed object

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWCompressedCopyAccounting`, optional batch `rgw_compressed_copy`.
Pure tests of its bucket ownership and copy result classification
(`TestOwnBuckets*`, `TestClassifyCopyResult`, batch `pure`).

## Use case and native contract

An application copies objects between storage classes or buckets whose
placement compresses differently, and relies on listing sizes, bucket
statistics or quotas. Whatever the source's compression, the destination's
bucket index entry must account the object's logical size, the size S3 lists
and GET returns.

The zone of the case compresses the `STANDARD` storage class of
`default-placement` with zlib and adds an uncompressed storage class `COLD`
on its own data pool. Each object has a bucket of its own:

| Subtest | Source | Destination | Contract |
| --- | --- | --- | --- |
| `compressed-source-uncompressed-destination` | `STANDARD`, zlib | `COLD` (`x-amz-storage-class` on the copy) | the copy is uncompressed; index `accounted_size`, listed `Size` and bucket `size` equal the logical length |
| `compressed-destination-control` | `STANDARD`, zlib | `STANDARD` | the copy is zlib-compressed with its own metadata; the same sizes equal the logical length |
| `uncompressed-source-control` | `COLD` | `COLD` | uncompressed; the same sizes equal the logical length |

The trigger: a copy to another placement rule, or of an object without a
tail (anything up to the head size, 4 MiB by default), rewrites the data in
`RGWRados::copy_obj_data`. The copy filter decompresses the source, removes
its compression attribute and compresses again only when the destination's
storage class compresses. Without a destination compression attribute,
Tentacle 20.2.4 accounts the copy with the number of bytes it read from the
source, which for a compressed source is the compressed length. Tentacle
20.2.5 passes the source's logical (`accounted_size`) size to
`copy_obj_data` and uses it in that case. Umbrella 21.2.0 has the same code as
20.2.5. The data itself is intact: GET returns every byte, and the index
entry's raw `size` is the logical length.

Upstream references:

- Release note of [Tentacle 20.2.5](https://docs.ceph.com/en/latest/releases/tentacle/)
  (accounted size of copies from compressed objects).
- `RGWRados::copy_obj` and `RGWRados::copy_obj_data` in
  [`src/rgw/driver/rados/rgw_rados.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/driver/rados/rgw_rados.cc),
  and `RGWCopyObjDPF` in
  [`src/rgw/rgw_op.cc`](https://github.com/ceph/ceph/blob/v20.2.5/src/rgw/rgw_op.cc).

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user), plain HTTP. Image roles `essential`, `client` and `rgw`, all of one
  release; the RGW image must ship the zlib compressor plugin (the published
  role images do). Bridge networking.
- The case creates the pool `tc-copy.cold.data` (`ceph.Container.CreatePool`)
  and edits the gateway's own zone and zonegroup with `radosgw-admin zonegroup
  placement add`, `zone placement add` and `zone placement modify`
  (`Gateway.Admin`), then restarts the gateway (`Stop` and `Start`) so that it
  loads them. No realm or period is involved.
- Requests are signed by the test's own SigV4 client (`harness.RGWBackendSigned`).
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: it fails on the published Tentacle 20.2.4
images, the pull request default. It never runs on a pull request, on `main`
or in periodic samples, and never gates anything.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_compressed_copy`.
- Comment: `/ci run rgw_compressed_copy` or
  `/ci run rgw_compressed_copy on umbrella`.

## Running it

```sh
CEPH_TEST_ESSENTIAL_IMAGE=<essential role@sha256:...> \
CEPH_TEST_CLIENT_IMAGE=<client role@sha256:...> \
CEPH_TEST_RGW_IMAGE=<rgw role@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWCompressedCopyAccounting$' \
  ./internal/integration/experiments/rgw_compressed_copy_accounting
```

Resolve the role images with `run_image_matrix.py --variant official
--layout roles --release <release> --roles essential,client,rgw
--prepare-only` (see [Testing](../../../../docs/contributing/testing.md)).
Tentacle 20.2.5 has no published role images; run it with
`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v20.2.5@sha256:<index digest>` instead of
the role variables. Or run the batch exactly as CI does:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_compressed_copy \
  --package ./internal/integration/experiments/rgw_compressed_copy_accounting \
  --directory artifacts/rgw-compressed-copy
```

Record the image digests with the result; the log names the Ceph version
(`RGW_CC ceph=` and the daemon versions), the zone placement read back, every
object's observation (`RGW_CC observe`) and the placement rules and pools
each copy resolved (`RGW_CC trace`).

## Observations and controls

1. Condition: the zone reads back zlib for `STANDARD` and the `COLD` class
   on its pool without compression. The source has a zlib compression
   attribute (`radosgw-admin object stat`) whose original size is the
   logical length and whose blocks, like the index entry's raw `size`, are
   smaller; the plain source has no compression attribute and is `COLD`.
2. Trigger evidence: the destination of the trigger copy has no compression
   attribute, storage class `COLD`, and an index `size` and bucket
   `size_utilized` equal to the logical length. With `debug_rgw` 20, set for
   the copies only, `copy_obj` logs the source rule `default-placement` and
   the destination rule `default-placement/COLD` on different pools; the
   test records these lines and does not depend on them.
3. Oracle, per destination: the index entry's `accounted_size` (`bi list`),
   the listed `Size` (S3 `ListObjects`) and the bucket statistics' `size`
   (`bucket stats`, read until they count the one object, at most 30
   seconds) must all equal the logical length, and a fresh GET must return
   the exact bytes. `size_actual` is the rounded size and is only logged.
4. Each copy's response must have the XML root `CopyObjectResult`; a 200
   whose root is `Error`, in any namespace, fails with its code and message.

## Reading the result

- **Pass:** every subtest met the contract. Tentacle 20.2.5 and Umbrella
  21.2.0 pass.
- **Observed bug:** `compressed-source-uncompressed-destination` reports the
  compressed length (48905 bytes for the 1048630-byte payload) as index
  `accounted_size`, listed `Size` and bucket `size`, while the raw `size`,
  `size_utilized` and GET are the logical length; both controls pass.
  Tentacle 20.2.4 does this.
- **Not reproduced:** a pass on a build claimed to be affected. Check the
  trigger evidence in step 2.
- **Unavailable prerequisite:** the cluster or the gateway does not start,
  the zone edits are refused, or the gateway does not restart.
- **Condition not established:** the zone does not read back the edits, the
  source is not compressed, or the trigger destination carries compression
  metadata. The test fails with `condition not established`. Squid 19.2.5
  keeps the source's zlib metadata and compressed bytes in the `COLD` copy,
  so the trigger is not reached there (its accounted size is the logical
  length).

## Cleanup and limits

The cleanup is registered before the first bucket is created and receives
only the buckets whose creation answered 200, so a failed intermediate
creation still removes the buckets made before it. It deletes their objects
and the buckets and reports a failure as
`RGW_CC cleanup failed` separately from the result. The zone edits and the
pool belong to the cluster, which is removed by its cleanup. No index repair
command runs. Not run: arm64, objects with a tail (larger than the head
size), multipart sources, copies between placement targets, other
compressors, compression combined with encryption, and quota admission.
