# RBD mirror demotion copy resumed after a receiver restart

Owner: RBD mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDMirrorDemotionResumesAfterReceiverRestart`, optional batch
`rbd_demote_restart`.

## Use case and native contract

A planned failover demotes the primary image; the receiver (`rbd-mirror`)
copies the demotion snapshot to the destination. When the receiver is killed
during that copy and its container is started again, the same snapshot must
complete, the destination must hold the primary's exact bytes and must then be
promotable without force. The replica of a demoted primary that was removed
while the receiver was down must be removed too.

Two native defects are involved:

1. Squid 19.2.3 (and Tentacle 20.2.0) classify an incomplete non-primary
   demotion snapshot as orphaned (`calc_promotion_state` in
   `src/librbd/mirror/GetInfoRequest.cc`), so the restarted receiver never
   resumes the copy. 19.2.4, 20.2.1 and later resume it.
2. On every qualified image that resumes (19.2.4, 19.2.5, 20.2.4, 21.2.0) the
   resumed copy reports complete while the object numbered
   `last_copied_object_number` at the kill keeps the previous snapshot's
   bytes. An ordinary mirror snapshot copy shows the same loss, so it is not
   specific to demotion. No upstream issue records this one yet.

Upstream references for the first defect:

- [tracker 73528](https://tracker.ceph.com/issues/73528), fix
  [ceph/ceph#65953](https://github.com/ceph/ceph/pull/65953), Squid backport
  [ceph/ceph#66165](https://github.com/ceph/ceph/pull/66165), Tentacle
  backport [ceph/ceph#66164](https://github.com/ceph/ceph/pull/66164).

The case reports the two defects as two units, `demotion-resume` and
`resumed-bytes`. No image reproduces both: `resumed-bytes` can be judged
only after the copy resumed, which an image with defect 1 never does.

## Requirements

- Two clusters, one OSD each, bridge mode only; forward and reverse
  `rbdmirror.Run` links in snapshot mode. The clusters, their clients and
  both receivers run the package's `image`, one aggregate image.
- The netem helper image (`CEPH_TEST_NETEM_IMAGE`, default
  `fault.NetemHelperImage`) limits only the source OSD's packets to the
  forward receiver to 2 Mbit/s, so each 16 MiB image (256 KiB objects) takes
  about a minute to copy.
- The test bounds itself to 25 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Squid
19.2.3 by default, which reproduces `demotion-resume`. Its given and when
steps (steps 1 to 3 below) fail the test; the then step does not. It prints
`EXPERIMENT_RESULT case=rbd_mirror_demote_restart/demotion-resume result=...`,
and, only when the copy resumed,
`EXPERIMENT_RESULT case=rbd_mirror_demote_restart/resumed-bytes result=...`.
It passes either way. No automatic run includes it (the default checks of a
pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rbd_demote_restart`.
- Comment: `/ci run rbd_demote_restart` or `/ci run rbd_demote_restart on squid`.

## Running it

On the default image, which must print `demotion-resume result=REPRODUCED`
and no `resumed-bytes` line:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestRBDMirrorDemotionResumesAfterReceiverRestart$' \
  ./internal/integration/experiments/rbd_mirror_demote_restart
```

On another image, injected into the package's `image` variable. A release
that fixes defect 1 (Squid 19.2.4 here; Tentacle 20.2.1 and later too) must
print `demotion-resume result=NOT_REPRODUCED`; every such release qualified
so far prints `resumed-bytes result=REPRODUCED`, because no release fixes
defect 2:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_demote_restart_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:1f6b01ecd6d8deb5fc287f8c5caeffd3af9b93d57bf7bced2482a0c201d5cac0" \
  -run '^TestRBDMirrorDemotionResumesAfterReceiverRestart$' \
  ./internal/integration/experiments/rbd_mirror_demote_restart
```

The role image variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RBD_AUX_IMAGE`, ...)
do not select the case's images. The netem helper image
(`CEPH_TEST_NETEM_IMAGE`) is not a Ceph image and keeps its own selection.
The test logs the image and the clusters' Ceph versions.

## Observations and controls

1. Control: both images read back exactly on the destination before any
   fault.
2. Condition: the newest destination snapshot of each image is
   `type=mirror state=demoted complete=false` with a `primary_mirror_uuid`,
   observed before and again after the SIGKILL of the same receiver
   container. Without this the trigger is not established.
3. While the receiver is down the demoted `removed` primary is force-disabled
   and deleted, the rate limit is lifted and the same container is started.
4. `demotion-resume`: the same snapshot ID completes with the same
   `primary_snap_id`, and the destination `removed` image disappears.
5. `resumed-bytes`, only when the snapshot completed: the `resumed` image
   exports the source's exact bytes.
6. Only when neither unit reproduced: non-forced promotion succeeds, and the
   next write and snapshot replicate back through the reverse link. A
   failure here fails the test.

On a byte mismatch the `resumed-bytes` symptom classifies every 256 KiB
object as new, old or neither, so the skipped object number is in the log.

## Reading the result

- **`demotion-resume` REPRODUCED:** the copy does not complete after the
  restart (19.2.3: status `up+unknown`, "remote image is not primary"), or
  the destination keeps the `removed` replica. When the copy did not
  complete, `resumed-bytes` is not evaluated (`resumed-bytes not evaluated`).
- **`demotion-resume` NOT_REPRODUCED:** the same snapshot completed and the
  `removed` replica is gone.
- **`resumed-bytes` REPRODUCED:** the copy completed, but the destination
  does not read back the demotion snapshot's bytes; the symptom names one
  old object at the recorded progress number.
- **`resumed-bytes` NOT_REPRODUCED:** the destination holds the exact bytes.
  No published image does this today.
- **Test failure (no result):** the netem helper is missing, a cluster or
  link does not start, the control read-back fails, or the copy was already
  complete, or not yet started, at the kill (step 2; a timeout there is not
  a reproduction).

Native results (Linux amd64 host, Docker, aggregate images, one run each):

| Image | `demotion-resume` | `resumed-bytes` | Time |
| --- | --- | --- | --- |
| Squid 19.2.3, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:20c0e5b9d77c…` (default) | `REPRODUCED`: copy never completed (`up+unknown`, "remote image is not primary"); the `removed` replica stayed | not evaluated | 760 s |
| Squid 19.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:1f6b01ecd6d8…` | `NOT_REPRODUCED` | `REPRODUCED`: object 37 old, 63 new | 426 s |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` | `NOT_REPRODUCED` | `REPRODUCED`: object 44 old, 63 new | 430 s |

So the default image reproduces `demotion-resume` only, and a run on a
release that fixes it still prints `resumed-bytes result=REPRODUCED`: the
Experiments workflow's rule that every line of a fixed image is
`NOT_REPRODUCED` cannot hold for this case until defect 2 is fixed.

## Cleanup and limits

The network profile lease is restored before the restart; the links,
clusters and helper containers are terminated by their cleanups. Not run:
arm64, Tentacle 20.2.0/20.2.1 (source boundary only), a graceful stop,
journal mode, object-map/fast-diff images, and the promotion and reverse
replay steps after a correct resume.
