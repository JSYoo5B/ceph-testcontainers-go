# RBD mirror snapshot copy resumed past its checkpoint object

Owner: RBD mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDMirrorResumedCopyRecopiesCheckpointObject`, optional batch
`rbd_resume_checkpoint`. Its kill-window prerequisite and its judgement
have Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A test kills the receiver (`rbd-mirror`) while it copies a mirror snapshot
and starts the same container again. The receiver resumes the copy from the
progress it persisted in the destination snapshot,
`last_copied_object_number`, and marks the snapshot complete. The
destination must then hold the snapshot's exact bytes, so that a planned
failover from it (demotion, non-forced promotion, reverse replay) keeps the
data.

The persisted value is a count, but the restart reads it as an index:

- `src/librbd/deep_copy/ImageCopyRequest.cc`, `handle_object_copy`: once the
  contiguous objects up to index N are copied, the handler reports N+1.
  `src/tools/rbd_mirror/image_replayer/snapshot/Replayer.cc`,
  `handle_copy_image_progress`, stores that count as
  `last_copied_object_number`.
- On restart `Replayer.cc` passes a positive `last_copied_object_number`
  unchanged as `ObjectNumber`, the index of the last copied object, and
  `ImageCopyRequest::send_object_copies` starts at that index plus one. For
  a checkpoint of 20, objects 0 to 19 are copied and the first object
  scheduled is 21. Object 20 keeps the previous snapshot's bytes if its copy
  had not finished at the kill. A zero checkpoint starts at object 0.

The same lines are in every published release that has a native image here
and in the upstream main branch of October 2026: tags v19.2.3 to v19.2.7,
v20.2.0 to v20.2.5, v21.2.0 and v21.3.0. **No release fixes it.** It is not
specific to demotion: the
[`rbd_mirror_demote_restart`](../rbd_mirror_demote_restart/README.md) case
saw the same object lost in a resumed demotion copy (its `resumed-bytes`
result). This case copies an ordinary mirror snapshot, so the orphaned
demotion snapshot of Squid 19.2.3 does not mask it, and it establishes the
kill window explicitly.

Upstream reference: none records this defect yet. The demotion-state fix it
was found next to is tracked as
[tracker 73528](https://tracker.ceph.com/issues/73528).

## Requirements

- Two clusters, one OSD each, bridge mode only; forward and reverse
  `rbdmirror.Run` links in snapshot mode. The clusters, their clients and
  both receivers run the package's `image`, one aggregate image.
- The netem helper image (`CEPH_TEST_NETEM_IMAGE`, default
  `fault.NetemHelperImage`) limits only the source OSD's packets to the
  forward receiver to 2 Mbit/s, so a copy of the 16 MiB image (64 objects of
  256 KiB) takes about a minute.
- The case's steps share a 25-minute deadline that starts before cluster
  startup. Startup (`harness.NewSingleImageCluster`, once per cluster) runs on
  the test context, so the deadline does not cut it short, but the time it
  takes is spent from the 25 minutes and the steps after it get what remains.
  In cleanup the restoration of each network limit has its own one-minute
  deadline, the link's termination its own two-minute one and each cluster's
  termination its own two-minute one, so cleanup is not bounded by the
  deadline and can take the test past 25 minutes. The outer bound is the
  batch's 30-minute `go test` timeout.

## Tier and triggers

Known-defect reproducer, optional batch `rbd_resume_checkpoint` with a
30-minute budget; no automatic run includes it. The default image is
Tentacle 20.2.4. It prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines),
`case=rbd_mirror_resume_checkpoint`. It is an `Experiments` case without a
fixed image: no release fixes the defect. The judgement tests are the
package's `code/pure` batch and run in Code checks for every change.
`/ci run rbd_resume_checkpoint` runs the default image. The triggers, the result
checks and
the image injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestRBDMirrorResumedCopyRecopiesCheckpointObject$' \
  ./internal/integration/experiments/rbd_mirror_resume_checkpoint
```

On another image, such as a patched build, which must print
`result=NOT_REPRODUCED` and then complete the failover:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_resume_checkpoint_test.image=<image>" \
  -run '^TestRBDMirrorResumedCopyRecopiesCheckpointObject$' \
  ./internal/integration/experiments/rbd_mirror_resume_checkpoint
```

The netem helper image (`CEPH_TEST_NETEM_IMAGE`) is not a Ceph image and
keeps its own selection. The test logs the image and the clusters' Ceph
versions.

## Observations and controls

1. Control: the image, enabled for snapshot mirroring at full speed, reads
   back exactly on the destination.
2. Trigger, one attempt: under the rate limit the source image is rewritten
   (every object changes), its bytes are verified on the source, and one
   mirror snapshot is taken. The test polls the destination's copy of that
   snapshot (`rbd snap ls --all --format json`, matched by
   `primary_snap_id`) every half second. When the copy persisted a
   checkpoint of at least 8, the test exports the destination image and
   kills the receiver (SIGKILL) only if the object at the checkpoint still
   holds the old bytes. The receiver often persists a checkpoint whose
   object already arrived, because it skips a progress update while another
   is in flight; the wait passes over such a checkpoint and logs it.
3. Condition, read while the receiver is down: the copy is incomplete, its
   persisted checkpoint is positive and inside the image, every object below
   it holds the new bytes, and the object at it holds the old bytes. This
   read, not the one before the kill, decides whether the attempt counts.
4. The rate limit is lifted, the same container is started, and the same
   snapshot copy must complete for the same primary snapshot.
5. Result: the destination is exported and compared with the source object
   by object.
6. Only when not reproduced: the source is demoted, the destination promoted
   without force, and its next write and snapshot replicate back through the
   reverse link. A failure here fails the test.

An attempt whose condition does not hold (the kill missed the copy of the
checkpoint object) is logged as `attempt N missed the kill window`; its
completed copy must read back exactly, and the next attempt rewrites the
image and takes a new snapshot. After three missed attempts the test fails.
Every attempt prints its persisted checkpoint and the snapshot listing.

## Reading the result

- **REPRODUCED:** the completed copy left the object at the checkpoint with
  the previous snapshot's bytes; the symptom names the object, the
  checkpoint and the classification of every object (`new=63 old=[20]`).
- **NOT_REPRODUCED:** the destination holds exactly the snapshot's bytes,
  and the failover that follows succeeded.
- **Test failure (no result):** the netem helper is missing, a cluster or
  link does not start, the control read-back fails, the copy completed under
  the limit, no attempt interrupted the copy of the checkpoint object
  (`no attempt interrupted the copy of the checkpoint object`, with every
  attempt's reason), a missed attempt's copy did not read back exactly, the
  restarted receiver did not complete the copy, or the destination differs
  elsewhere than at the checkpoint object (`another fault`).

Native results: Linux amd64 host, Docker 29.9.1, the published aggregate
tags. One run each.

| Image | Result | Checkpoint at the kill | Attempts | Time |
| --- | --- | --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all` | `REPRODUCED`: object 20 old, 63 new | 20; checkpoints 8, 11, 13, 16 and 18 passed over as lagging | 1 | 152 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all` | `REPRODUCED`: object 11 old, 63 new | 11 | 1 | 149 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-21.2.0-all` | `REPRODUCED`: object 13 old, 63 new | 13; checkpoint 11 passed over as lagging | 1 | 146 s |

An earlier revision of the test killed the receiver as soon as the
checkpoint reached 8, without the read before the kill. On 20.2.4 its first
two attempts missed the window (checkpoint 8 while object 8 already held the
new bytes; both completed copies read back exactly), and the third
reproduced at checkpoint 11 (287 s). That is why the test now reads the
checkpoint object before the kill.

## Cleanup and limits

The network profile lease is restored before every restart and again by its
cleanup; the links, clusters and helper containers are terminated by their
cleanups. Not run: arm64, journal mode, object-map/fast-diff images (a
fast-diff copy skips unchanged objects and may hide or move the lost
object), a graceful stop, and the failover steps after an exact resume,
which no published image reaches.
