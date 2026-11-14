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
- When an entry is described: `TrackedOp::mark_event` and
  `TrackedOp::get_desc` in
  [`src/common/TrackedOp.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/TrackedOp.cc)
  and
  [`src/common/TrackedOp.h`](https://github.com/ceph/ceph/blob/v20.2.4/src/common/TrackedOp.h).
- A read with a length of zero, and a read beyond the end of the object:
  `PrimaryLogPG::do_read` in
  [`src/osd/PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc).
- The class method of the metadata reads: `cls_version_read` in
  [`src/cls/version/cls_version.cc`](https://github.com/ceph/ceph/blob/v20.2.4/src/cls/version/cls_version.cc).

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
objects and the segment objects of the SLO and of the DLO under the names
the test derives from the bucket markers (`radosgw-admin bucket stats`).

The observation is on the OSD, so it does not depend on the response or on
timing. Through the OSD's admin socket the op history is set to keep 20000
operations for an hour (`osd_op_history_size`, `osd_op_history_duration`)
and the op tracker's messages are gathered in memory (`debug_optracker
0/6`). Every report must name that size, hold fewer entries than it and
still hold every operation an earlier report held. A **window** is
everything the history gained since the window before it. The test sends at
most one request, waits for its response, then creates a new empty object in
the data pool with `rados create` and reads `dump_historic_ops` until the
history holds that operation: the window is closed by an operation that was
sent after the response. Operations of other pools are not looked at.

The OSD writes the description of an operation the first time something
asks for it and keeps that text. With the op tracker's messages gathered,
that is when the operation arrives, before it runs: the description then
names the method of a class call and the extent a read asks for
(`call version.read in=11b`, `read 0~4194304`). Without the setting it is
written when the history is dumped, after the operation ran, and shows a
bare `call` and the extent the OSD had cut to the object's size
(`read 0~3145728`, or `read 0~0` for the empty manifest object).

Each client operation of the window is attributed by its pool and its object
name, and each sub-operation is classified by its whole text:

- `read <offset>~<length>`, with nothing after it, is a data read. The
  bytes it returns are worked out with the segment's size: a length of zero
  asks for everything from the offset to the end of the object, a read is
  cut off at the end of the object, and a read that starts at or beyond the
  end returns nothing.
- `getxattrs`, `stat` and `call version.read`, the last with or without the
  size of its input (`in=<n>b`), return no object data. `version.read`
  reads the object's version from one attribute.
- Everything else is unclassified and fails the window: a call of any other
  method, or a bare `call`, because a class method can read the object's
  data inside the OSD without returning it; a read with anything after its
  extent or in another form (`sparse-read`, `sync_read`); `getxattr`; and
  every write.

The second observation is the growth of `op_r_out_bytes` in `perf dump osd`
over the window, the bytes the OSD returned for reads to all its clients. A
counter that is lower after the request than before it was reset, and fails
the window.

1. Idle control, three times (before the first request, between the full
   GET and the HEAD of the SLO, and after it): a window without a request
   must hold no operation in the data pool. Nothing but the test's requests
   touches the data pool, and no operation of a request arrives after its
   window.
2. HEAD of the S3 single PUT, of the S3 multipart upload (signed S3
   requests) and of the DLO (Swift): each must be answered 200 with
   `Content-Length` 9437184 and no body, and its window must hold at least
   one operation on its head or manifest object, no operation on any other
   object, and no data read. A deviation fails the test:
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
  and data reads that return all 3145728 bytes of each of the three segment
  objects, and the OSD returned at least the payload during it. The
  symptoms give the read extents of each segment and the returned bytes of
  the HEAD and of the GET.
- **NOT_REPRODUCED:** the HEAD window holds an operation on the manifest
  object, so the request reached the OSD, and no data read on any segment
  object, and the OSD returned less than one segment (3145728 bytes) during
  it, while the full GET of the same object showed the reads. The three
  classified metadata reads of the segments, or no operation on them at
  all, are both this result; a segment that the window holds no operation
  on was not touched, because the window is complete.
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
  - `window not observed`: the history is full or has lost an operation it
    held before, the closing operation of a window does not appear in it
    within a minute, the byte counter went down during the window, a window
    holds an operation on an object that is not the request's or on a PG of
    the data pool (an object listing), a sub-operation is unclassified (see
    above), or the history does not describe the object or the
    sub-operations of a new operation of the data pool (such as one the OSD
    recorded undecoded).
  - `another state:`: the HEAD read some segments but not all, part of a
    segment, or from an offset at or beyond a segment's end. That is neither
    the documented symptom nor its absence.
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
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (default) | `REPRODUCED`: the HEAD window holds `[call version.read in=11b, getxattrs, stat]` on the manifest and `[call version.read in=11b, getxattrs, stat, read 0~4194304]` on each segment; the OSD returned 9441229 bytes during the HEAD and during the full GET | 39 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:d0fcbea14a2a…` | `REPRODUCED`, the same operations; 9441145 bytes for both | 43 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph@sha256:4337ac8ac81d…` | `REPRODUCED`, the same operations; 9441233 bytes for both | 40 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623…` | `REPRODUCED`, the same operations; 9441223 bytes for both | 42 s |

The gateway asks for 4194304 bytes of each 3145728-byte segment, and the
OSD returns the segment. On every image the three idle windows held no
operation and no returned byte, and the control HEADs held one
`[call version.read in=11b, getxattrs, stat]` on their head or manifest
object and nothing else: 912 to 1037 returned bytes for the single PUT, 956
to 1110 for the multipart upload and 4670 to 4835 for the DLO. The full GET
also asks for `read 0~4194304` of the empty manifest object, which the HEAD
does not. No read asked for a length of zero.

Two runs with a temporary change show the refusals. With the HEAD of the
SLO left out, so the trigger was never sent, the test failed with
`condition not established` (no operation on the manifest). Without
`debug_optracker 0/6`, so every call read as a bare `call`, the first
control HEAD failed with `window not observed: unclassified sub-operations
["call"]`. Neither printed a result.

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
