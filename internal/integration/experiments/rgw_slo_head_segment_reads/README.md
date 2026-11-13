# A HEAD of a Swift static large object reads every segment

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRGWSLOHeadSegmentReads`, optional batch
`rgw_slo_head_segment_reads`. Its readers of the OSD's op history, of the
read counter and of the bucket marker, its window rules and its judgement
have Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A client asks for the size or the metadata of a large object with HEAD. The
answer has no body, so the gateway needs the object's manifest and nothing of
its payload. For three of the four ways a large payload is stored that is
what happens; for a Swift Static Large Object (SLO) the gateway reads the
whole payload of every segment from RADOS and discards it:

| Layout | HEAD reads from the data pool |
| --- | --- |
| S3 single PUT (head object and tail stripes) | attributes and size of the head object |
| S3 multipart upload | attributes and size of the head object |
| Swift Dynamic Large Object (DLO, `X-Object-Manifest`) | attributes and size of the manifest object; the segments are listed in the index pool |
| Swift Static Large Object (`?multipart-manifest=put`) | attributes and size of the manifest object, and **every byte of every segment** |

The defect: `RGWGetObj::execute` (`src/rgw/rgw_op.cc`) handles a DLO and an
SLO manifest before its ordinary exit for a request without a body
(`if (!get_data || ofs > end)`). `RGWGetObj::handle_user_manifest`, for the
DLO, returns after its length pass when `get_data` is false.
`RGWGetObj::handle_slo_manifest` has no such check: it calls
`iterate_slo_parts` over the requested range, which for a HEAD without a
`Range` is the whole object, and every part is read through
`read_user_manifest_part`. The cost of a HEAD therefore grows with the
payload, and it is paid on the OSDs, not in the response.

Squid 19.2.5, Tentacle 20.2.4, Tentacle 20.2.5 and Umbrella 21.2.0 have the
same code, and no release contains a correction.

Upstream references:

- `RGWGetObj::handle_slo_manifest` and `RGWGetObj::handle_user_manifest` in
  [`src/rgw/rgw_op.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_op.cc).
- The form of an op history entry: `MOSDOp::print` in
  [`src/messages/MOSDOp.h`](https://github.com/ceph/ceph/blob/v20.2.4/src/messages/MOSDOp.h)
  and `operator<<(ostream&, const OSDOp&)` in
  [`src/osd/osd_types.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc).

## Requirements

- One cluster with one OSD, one gateway (`rgw.Start` with the default
  configuration, which enables the Swift API and its authentication) and the
  client toolkit, whose `rados` closes each observation window. Every
  container runs the package's `image`, by default the Tentacle 20.2.4
  aggregate. Image roles `essential`, `client` and `rgw`. Bridge networking,
  plain HTTP.
- One user (`gateway.CreateUser`) with its S3 key and a Swift subuser
  (`radosgw-admin subuser create --key-type swift`). The Swift requests are
  plain `net/http` requests with the token of `/auth/1.0`; the S3 requests
  are signed by the test's own SigV4 client. The client opens a connection
  per request, so the HTTP library never sends a request a second time over
  a connection the gateway closed.
- 36 MiB of object data. The test bounds itself to 15 minutes; the batch
  timeout is 20 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps and
its controls fail the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rgw_slo_head_segment_reads result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it, and it never gates
anything.

- Manual: `gh workflow run "Experiments" -f case=rgw_slo_head_segment_reads`,
  or `gh workflow run "Native regressions" -f batch=rgw_slo_head_segment_reads`
  for its pass or failure only.
- Comment: `/ci run rgw_slo_head_segment_reads` or
  `/ci run rgw_slo_head_segment_reads on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRGWSLOHeadSegmentReads$' \
  ./internal/integration/experiments/rgw_slo_head_segment_reads
```

On another image, injected into the package's `image` variable; a fixed
image must print `result=NOT_REPRODUCED`. No release fixes the defect yet,
so the example names Umbrella 21.2.0, which reproduces it too:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_slo_head_segment_reads_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623a5e6c9f7dc7dd13b356dcb1f71b0b4d74bf7cd2c175cd802735f" \
  -run '^TestRGWSLOHeadSegmentReads$' \
  ./internal/integration/experiments/rgw_slo_head_segment_reads
```

The cluster, its gateway and its client run `image`; the role image
variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RGW_IMAGE`) do not select it. The
test logs the gateway's `radosgw --version`.

## Observations and controls

Every layout carries the same 9 MiB of generated bytes, and each has its own
bucket, so its objects have their own prefix (the bucket marker) in the data
pool `default.rgw.buckets.data`:

| Object | Layout | Objects in the data pool |
| --- | --- | --- |
| `tc-slo/slo` | SLO over `tc-slo-segments/segment-0000` to `segment-0002`, 3 MiB each | the manifest object and one object per segment |
| `tc-dlo/dlo` | DLO over the prefix `tc-dlo-segments/segment-` with the same three segments | the manifest object and one object per segment |
| `tc-s3/single` | S3 single PUT | a head object and tail stripes |
| `tc-s3/multipart` | S3 multipart upload, 5 MiB and 4 MiB | a head object and the parts' objects |

Before anything is measured, every object is read back exactly, the SLO
answers `X-Static-Large-Object: True`, the DLO answers its
`X-Object-Manifest`, and the data pool lists the manifest objects, the head
objects and the three SLO segment objects under the names the test derives
from the bucket markers (`radosgw-admin bucket stats`).

The observation is on the OSD, so it does not depend on the response or on
timing. The OSD's op history is set to keep 20000 operations for an hour
(`osd_op_history_size`, `osd_op_history_duration` through the admin socket),
and every report must name that size and hold fewer entries than it. A
**window** is everything the history gained since the window before it. The
test sends at most one request, waits for its response, then creates a new
empty object in the data pool with `rados create` and reads
`dump_historic_ops` until the history holds that operation: the window is
closed by an operation that was sent after the response. Operations of
other pools are not looked at. Each client
operation of the window is attributed by its pool and its object name; its
sub-operations are data reads (`read <offset>~<length>`), metadata reads
(`call`, `getxattrs`, `getxattr`, `stat`) or unclassified. The second
observation is the growth of `op_r_out_bytes` in `perf dump osd` over the
window, the bytes the OSD returned for reads to all its clients.

1. Idle control, three times (before the first request, between the full
   GET and the HEAD of the SLO, and after it): a window without a request
   must hold no operation in the data pool. Nothing but the test's requests
   touches the data pool, and no operation of a request arrives after its
   window.
2. HEAD of the S3 single PUT, of the S3 multipart upload (signed S3
   requests) and of the DLO (Swift): each must be answered 200 with
   `Content-Length` 9437184 and no body, and its window must hold at least
   one operation on its head or manifest object, operations on objects of
   its own buckets only, and no data read. A deviation fails the test:
   - The single PUT and the multipart upload show that the method tells a
     HEAD from a GET. If a HEAD of an ordinary object showed data reads,
     either the window had caught operations of another request or the
     build read data for every HEAD, and the SLO window could not be read
     as a property of the SLO.
   - The DLO is the neighbour of the SLO in the code: the same manifest
     branch with the check for a request without a body. Its window is the
     only native example of a large object HEAD without segment reads, which
     is what `NOT_REPRODUCED` claims for the SLO. A DLO HEAD that read its
     segments would be another defect, which this case does not document.
3. Positive control: a full Swift GET of the SLO must return the exact bytes,
   and its window must hold data reads that cover every byte of every
   segment object and at least 9437184 returned bytes. The method sees
   segment reads when they happen.
4. Trigger: one Swift HEAD of the SLO, without `Range` and without a query.
   It must be answered 200 with `Content-Length` 9437184 and no body.
5. The HEAD window is judged against the GET window.

## Reading the result

- **REPRODUCED:** the HEAD window holds an operation on the manifest object
  and data reads that cover all 3145728 bytes of each of the three segment
  objects, and the OSD returned at least the payload during it. The
  symptoms give the read extents of each segment and the returned bytes of
  the HEAD and of the GET.
- **NOT_REPRODUCED:** the HEAD window holds an operation on the manifest
  object, so the request reached the OSD, and no data read on any segment
  object, and the OSD returned less than one segment (3145728 bytes) during
  it, while the full GET of the same object showed the reads. Metadata
  reads of the segments, or no operation on them at all, are both this
  result.
- **Test failure (no result):**
  - `condition not established`: an object does not read back, is not the
    layout it is meant to be or has no object of the expected name in the
    data pool; Swift authentication fails; the op history does not keep
    20000 entries; a HEAD is not answered 200 with the length and no body;
    or a window of a request holds no operation on the request's manifest or
    head object, so the request did not reach the OSD.
  - `control not established`: an idle window holds an operation in the
    data pool, a control HEAD read data, or the full GET does not show reads
    that cover every segment and return the payload.
  - `window not observed`: the history is full, the closing operation of a
    window does not appear in it within a minute, a window holds an
    operation on an object that is not the request's or on a PG of the data
    pool (an object listing), a sub-operation is neither a data read nor a
    metadata read (a write, or a read in another form such as
    `sparse-read`), or the history does not describe the object or the
    sub-operations of a new operation of the data pool (such as one the OSD
    recorded undecoded).
  - `another state:`: the HEAD read some segments but not all, or part of a
    segment. That is neither the documented symptom nor its absence.
  - `outcome not observed`: the operations and the byte counter disagree
    (reads that cover every segment with fewer returned bytes than the
    payload, or no segment read with a segment's worth of returned bytes).
  - A report lacks a field: the history its size or its op list, an entry
    its description, a client operation its request ID or its pool, the
    counter report `osd.op_r_out_bytes`, the bucket report its marker. An
    absent field is never read as an empty history or a count of zero.
  - The cluster, the gateway or the client does not start, or any other
    command or request of the case fails.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (default) | `REPRODUCED`: the HEAD window holds `[call, getxattrs, stat]` on the manifest and `[call, getxattrs, stat, read 0~3145728]` on each segment; the OSD returned 9441231 bytes during the HEAD and during the full GET | 38 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:d0fcbea14a2a…` | `REPRODUCED`, the same operations; 9441145 bytes for both | 45 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph@sha256:4337ac8ac81d…` | `REPRODUCED`, the same operations; 9441231 bytes for both | 38 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623…` | `REPRODUCED`, the same operations; 9441229 bytes for both | 40 s |

On every image the three idle windows held no operation and no returned
byte, and the control HEADs held one `[call, getxattrs, stat]` on their head
or manifest object: 912 to 1037 returned bytes for the single PUT, 956 to
1110 for the multipart upload and 4672 to 4837 for the DLO. The full GET
also holds a `read 0~0` on the manifest object, which the HEAD does not. A
run with the HEAD of the SLO left out, so the trigger was never sent, failed
with `condition not established` (no operation on the manifest) and printed
no result.

No fixed image exists to run, so `NOT_REPRODUCED` has not been observed
natively for the SLO. A HEAD window without data reads is observed in every
run for the single PUT, the multipart upload and the DLO.

## Cleanup and limits

The user, the buckets and the objects end with the cluster; nothing outside
it is changed.

Not covered: arm64, an S3 HEAD of the Swift-created SLO, a ranged GET of an
SLO, segments larger than one 4 MiB RADOS object, nested SLOs, HTTPS, more
than one OSD or gateway, and physical disk reads: the read extents and the
counter show what the OSD returned to the gateway, not what BlueStore read
from its device.
