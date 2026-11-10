# RGW numbered reads of a multipart object that has one part

Owner: RGW. Kind: cross-build characterization of a fixed upstream defect.
Test: `TestRGWSinglePartMultipartNumberedRead`, batch
`topology/rgw_single_part_read`. Pure tests of the judgement and of the
process reading, and of the phases of a read against a stub transport
(`TestJudgeOnlyPart`, `TestJudgeControls`, `TestParseProcessStat` and
`TestSendPartReadPhases`, batch `pure`).

## Use case and native contract

An application uploads every object above a size threshold with a multipart
upload, and an object just above the threshold ends up with exactly one
part. A downloader that reads objects by part sends `GET ?partNumber=1` and
`HEAD ?partNumber=1` to it like to any other multipart object. The read must
answer that part: its length, a part count of 1 (`x-amz-mp-parts-count`)
and, for a GET, its bytes. Part number 2 must be refused as out of range.

One bucket in the default storage class holds three completed multipart
uploads with distinguishable bytes:

| Object | Parts | Role |
| --- | --- | --- |
| `single-small` | one part of 64 KiB (one 4 MiB stripe) | trigger |
| `single-large` | one part of 9 MiB (three stripes) | trigger |
| `two-parts` | 5 MiB (the smallest valid non-final part) and 64 KiB | control |

The trigger is the manifest of a single-part upload: the part id that the
end of its manifest reports is 1, where a manifest of N > 1 parts reports
N + 1. A gateway that takes that id for one more than the part count
computes a part count of 0, and its walk over the stripes of the requested
part, which stops at the first stripe of a later part, never ends, because
no later part exists. That is the mechanism the upstream fix describes; it
returns a count of at least 1 and stops the walk at the end of the manifest
([tracker 66705](https://tracker.ceph.com/issues/66705),
[fix](https://github.com/ceph/ceph/pull/58288)). The defect was in the
Squid release candidate 19.1.0; 19.2.0 and every later release contain the
fix, so the case passes on all supported releases and guards the contract.
Reef 18.2.7 does not read by part number at all (see the results).
[`rgw_multipart_part_storage_class`](../rgw_multipart_part_storage_class/README.md)
covers numbered reads in a non-default storage class.

## Requirements

- One cluster with one OSD and one gateway (`rgw.Start` with its generated
  user), plain HTTP, no zone edits. Image roles `essential`, `client` and
  `rgw`, all of one release, or one aggregate image (`CEPH_TEST_IMAGE`).
  Bridge networking.
- Requests are signed by the test's own SigV4 client. A numbered read uses
  an HTTP client without a timeout of its own and a 30 second deadline per
  request.
- The gateway container must have `getconf` and a readable `/proc/1/stat`;
  process 1 of the container is `radosgw`.
- The test bounds itself to 10 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Cheap and deterministic: one run takes about 31 seconds on 20.2.4 (37 on
19.2.5, 32 on 21.2.0).

- Pull request: only when a file of this directory changes, or with the
  `ci:full` label (a required `topology` batch).
- Comment: `/ci run rgw_single_part_read [on squid|umbrella]`.
- Periodic: `rotate` can draw it on the supported releases.

## Running it

Export the images of one release first, as
[Images for the RGW S3 cases](../../../../docs/contributing/testing.md#images-for-the-rgw-s3-cases)
describes, then:

```sh
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=20m \
  -run '^TestRGWSinglePartMultipartNumberedRead$' \
  ./internal/integration/experiments/rgw_single_part_multipart_read
```

or exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py run --category topology \
  --batch rgw_single_part_read \
  --package ./internal/integration/experiments/rgw_single_part_multipart_read \
  --directory "artifacts/rgw-single-part-read-$(date -u +%Y%m%dT%H%M%SZ)"
```

Record the image digests with the result; the log names the Ceph version
(`RGW_SP ceph=`), the gateway binary (`RGW_SP gateway=`), each object as
uploaded (`RGW_SP object=`), every numbered read, sent or not
(`RGW_SP read`) and, after a read that did not end, the gateway process
(`RGW_SP stall`).

## Observations and controls

1. Condition, per object: the ETag ends in `-<number of parts>`, the
   manifest that `radosgw-admin object stat` prints has rules and its first
   rule starts at part 1, and a whole-object GET returns exactly the bytes
   written. Otherwise the test fails with `condition not established` and
   reads no part.
2. Control, before any single-part read: GET and HEAD of parts 1 and 2 of
   `two-parts` answer 200 or 206 with the part's length, a part count of 2
   and, for a GET, the part's bytes, and GET of part 3 answers
   `400 InvalidPart`. Otherwise the test fails with `control failed` and
   does not read the single-part objects, since numbered reads do not work
   on that gateway at all.
3. Oracle, per single-part object: GET and then HEAD of part 1 must answer
   200 or 206 with the part's length, a part count of 1 and, for a GET, the
   part's bytes (`judgeOnlyPart`).
4. A read that does not end within its deadline is recorded with the phase
   the client had reached: the request was not written to a connection; it
   was written and no response header arrived; or the header arrived and
   the body did not end, in which case the status and header values
   received are kept. A request that was never written shows nothing about
   the gateway and is `another fault`. For the other two the test reads the
   start time and the CPU time of the gateway's process 1 before the read,
   after the deadline and three seconds later: with the same process still
   running the read is `observed defect`, with a restarted or unreadable
   gateway `another fault`. The verdict names no cause. The CPU share in
   its message is counted for the whole process, earlier unended reads of
   the run included (`earlier-unanswered`), and is no call trace of the
   request. The subtest of that object stops at such a read, because the
   request may still hold a gateway thread, and prints a `not sent` line
   for each read it leaves out (the HEAD of part 1 after a GET, and the GET
   of part 2); the other object is still read.
5. Health, after each answered read of part 1: process 1 has the start time
   it had before the read, and a whole-object GET returns the object.
   Otherwise `another fault`.
6. Refusal control, per single-part object: GET of part 2 must answer
   `400 InvalidPart`. Another error, such as `403 AccessDenied`, does not
   show that the part number was out of range.

## Reading the result

The results below ran on the pinned role images of the shared recipe: 20.2.4
(essential `3c7a6d0ae445…`, client `56b5c03d450c…`, rgw `aac25fc48b53…`),
21.2.0 (essential `119180026952…`, client `e5f7edab88b6…`, rgw
`9ef3fb34f7c4…`) and 19.2.5 (essential `43ee23624e7a…`, client
`a69e1319ce88…`, rgw `1fbe1bd4ecc0…`), and on the official images of 19.2.0
(`sha256:200087c35811…`), 20.2.5 (`sha256:4337ac8ac81d…`), the release
candidate 19.1.0 (`sha256:56d1542f98c9…`) and Reef 18.2.7
(`quay.io/ceph/ceph:v18.2.7`) as aggregates. Neither 19.1.0 nor 18.2.7 is a
supported release; they were run once each to see the answers of builds
without the fix. Linux amd64, bridge, one run per build.

- **Pass:** every read of part 1 answers 200 with the part's length (65,536
  and 9,437,184), a part count of 1 and, for a GET, the exact bytes; part 2
  is refused with `400 InvalidPart`; the two-part control answers with a
  part count of 2. Squid 19.2.0 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and
  Umbrella 21.2.0 pass (31 to 38 s each).
- **Observed defect:** a read of part 1 is answered with another status,
  length, body or part count, or is not answered at all. The release
  candidate 19.1.0 does not answer the GET: the two-part control passes,
  then GET of part 1 of `single-small` and GET of part 1 of `single-large`
  are each written to a connection to the gateway and get no response
  header in 30 s, with the same gateway process running. HEAD of part 1
  and GET of part 2 were not sent for either object on that build, so the
  result is about GET only. In the three seconds after each deadline the
  whole process used 1.00 of a core after the first and 2.00 after the
  second, which includes whatever the first request left running. No call
  trace was taken, so the runs do not show where in the gateway the
  requests are; that they loop in the manifest walk is the upstream
  description, not a finding of this case. The run takes about 156 s and
  the cleanup succeeds. No supported release does this.
- **Control failed:** numbered reads do not work on the gateway. Reef 18.2.7
  answers every `?partNumber` read of the two-part object, part 3 included,
  with 200 and the whole object (5,308,416 bytes, no part count), so it does
  not read by part number and the single-part objects are not read.
- **Another fault:** a read was not written to a connection within its
  deadline, a read does not end and the gateway was restarted or
  cannot be read, the request fails in transport, or the gateway is not the
  same healthy process after a read.
- **Not reproduced:** a pass on a build claimed to be affected. Check the
  condition line of step 1: the ETag suffix and the manifest rules show that
  the object has one part.
- **Unavailable prerequisite:** the cluster or the gateway does not start,
  or the gateway's clock tick rate cannot be read.

## Cleanup and limits

The cleanup is registered before the bucket is created. It aborts an upload
left open, deletes the three objects and the bucket, and reports a failure as
`RGW_SP cleanup failed` separately from the result. Not run: arm64, the host
network profile, ranged GETs combined with a part number, versioned buckets,
encrypted or compressed objects, copies of a single-part object and other
storage classes. On the affected build each object's subtest stops at the
GET, so HEAD of part 1 is untested there. The case takes no native call
trace (the images carry no debugger), so it never names the cause of a
read that did not end. A read that the client never wrote, one whose body
stalled after the header, and one that did not end on a restarted gateway
were not produced natively; those are covered by the Docker-free tests
only.
