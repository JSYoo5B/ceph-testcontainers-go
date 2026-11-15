# RBD mirror receiver stopped by an unlink from a trashed snapshot

Owner: RBD mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDMirrorUnlinkFromTrashedSnapshot`, optional batch
`rbd_unlink_trashed_snapshot`. Its judgement of the receiver log and its
check of the trashed snapshot record have Docker-free tests in the same
package (`code/pure`).

## Use case and native contract

In snapshot-mode mirroring the receiver (`rbd-mirror`) keeps its peer
linked to the primary mirror snapshot it last copied, the start of its next
sync. After it copies the next mirror snapshot it unlinks itself from that
start snapshot (`Replayer::unlink_peer` in
`src/tools/rbd_mirror/image_replayer/snapshot/Replayer.cc`, through
`librbd::mirror::snapshot::UnlinkPeerRequest`). A start snapshot that is
gone or being removed by then needs no unlink, and the receiver must go on
replaying.

A removal first moves the snapshot to the trash namespace
(`SnapshotRemoveRequest::trash_snap`), and the snapshot stays there while a
clone v2 child still uses it. `UnlinkPeerRequest::unlink_peer` in Squid
19.2.3 and Tentacle 20.2.0 and 20.2.1 finds the trashed snapshot by its ID,
sees that its namespace is not the mirror namespace and fails with `EINVAL`
(`not mirror snapshot (snap_id=N)`). The replayer accepts only `ENOENT`
there, so the image replayer stops with `failed to unlink local peer from
remote image` and the image status turns `up+error`, until the daemon
starts the replayer again. Upstream this happens when the receiver's unlink
races with another removal of the same snapshot, for example a user taking
a mirror snapshot that prunes an old one. Squid 19.2.4 and Tentacle 20.2.2
treat a trashed snapshot like a missing one (`missing or trashed snapshot:
snap_id=N`, then `ENOENT`).

Upstream references: [tracker 68279](https://tracker.ceph.com/issues/68279),
fix [ceph/ceph#67482](https://github.com/ceph/ceph/pull/67482), Squid
backport [ceph/ceph#67582](https://github.com/ceph/ceph/pull/67582), Tentacle
backport [ceph/ceph#67583](https://github.com/ceph/ceph/pull/67583).

The case does not race. A clone of the start snapshot holds its removal in
the trash namespace for as long as the case needs, so the unlink meets the
snapshot in exactly the state the race produces, and both the state and the
unlink are tied to the snapshot's ID.

## Requirements

- Two clusters, one OSD each, bridge mode only; one forward `rbdmirror.Run`
  link in snapshot mode. The clusters, their clients and the receiver run
  the package's `image`, one aggregate image.
- Before the link starts, the destination sets `debug_rbd_mirror 10` for
  its clients (the receiver names the snapshot it unlinks from) and the
  source sets `debug_rbd 15` for `client.rbd-mirror-peer`, the bootstrap
  peer user. The receiver's unlink request runs on its connection to the
  source as that user and takes that user's settings, so it logs the skip of
  a trashed snapshot. The refusal is logged at any level.
- The case's steps share a 15-minute deadline that starts before cluster
  startup. Startup of the two clusters (`harness.NewSingleImageCluster`) runs
  on the test context, so the deadline does not cut it short, but the time it
  takes is spent from the 15 minutes and the steps after it get what remains.
  In cleanup the link's diagnostics have their own five-second deadline, its
  termination its own two-minute one and each cluster's termination its own
  two-minute one, so cleanup is not bounded by the deadline and can take the
  test past 15 minutes. The outer bound is the batch's 20-minute `go test`
  timeout.

## Tier and triggers

Known-defect reproducer, optional batch `rbd_unlink_trashed_snapshot` with a
20-minute budget; no automatic run includes it. The default image is
Tentacle 20.2.1, which prints `REPRODUCED`. It is an `Experiments` case; its
fixed image is
`ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all`, which
prints `NOT_REPRODUCED`. A comment run can name a release, for example
`/ci run rbd_unlink_trashed_snapshot on squid`, but it runs the default
image whatever release the batch names. The judgement tests run in
`code/pure` with every change (under 1 s). The triggers, the result checks
and the image injection are the shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestRBDMirrorUnlinkFromTrashedSnapshot$' \
  ./internal/integration/experiments/rbd_mirror_unlink_trashed_snapshot
```

On a fixed image, which must print `result=NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_mirror_unlink_trashed_snapshot_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all" \
  -run '^TestRBDMirrorUnlinkFromTrashedSnapshot$' \
  ./internal/integration/experiments/rbd_mirror_unlink_trashed_snapshot
```

The test logs the image and the clusters' Ceph versions.

## Observations and controls

The image `tc-rbd-unlink/primary` is 4 MiB with `layering,exclusive-lock`.
Each write rewrites it whole with a distinct payload.

1. Control: the first write and the mirror snapshot taken by enabling
   mirroring read back exactly on the destination.
2. S1: after a second write, `rbd mirror image snapshot` takes the mirror
   snapshot S1. The destination reads back its bytes, its newest snapshot is
   the complete non-primary copy of S1 (`primary_snap_id` is S1's ID), and
   S1 on the primary still lists the destination's peer UUID. S1 is now the
   receiver's sync start.
3. `rbd clone --rbd-default-clone-format 2 --snap-id <S1>` makes a clone of
   S1, and `rbd snap rm --snap-id <S1>` removes S1. `rbd snap ls --all` must
   list S1's ID as `type=trash` with `original_namespace_type=mirror` and
   S1's original name. Without this the trigger is not established.
4. Trigger: a third write and the next mirror snapshot. The receiver copies
   it and unlinks itself from S1.
5. The receiver's log is read for S1's ID (judgement below). S1 must still
   be trashed afterwards, so it was trashed when the unlink read it. The
   image status is logged.
6. A fourth write and mirror snapshot must replicate (within 4 minutes),
   on a reproducing image after the daemon started the replayer again. The
   time is logged.

## Reading the result

The test prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines),
`case=rbd_mirror_unlink_trashed_snapshot`. The receiver log lines it reads
are the replayer's `unlink_peer: remote_snap_id=<S1>` and
`handle_unlink_peer: failed to unlink local peer from remote image: (<errno>)`,
and the unlink request's `unlink_peer: not mirror snapshot (snap_id=<S1>)`,
`missing or trashed snapshot: snap_id=<S1>` and `missing snapshot:
snap_id=<S1>`. A failure line counts for S1 only between the replayer's
unlink from S1 and its next unlink. The request's lines come from another
logging thread than the replayer's, so their order against the replayer's
is not used.

- **REPRODUCED:** the replayer started the unlink from S1, the request
  refused S1 as not a mirror snapshot and the replayer failed with `(22)
  Invalid argument`. In every reproducing run the image status read right
  after it was `up+error`, "failed to unlink local peer from remote image",
  and the fourth write arrived about 19 s after its snapshot, once the
  daemon had started the replayer again.
- **NOT_REPRODUCED:** the replayer started the unlink from S1, the request
  skipped S1 as missing or trashed and the replayer did not fail.
- **Test failure (no result):**
  - `trigger not established`: the request found S1 fully removed
    (`missing snapshot`), so it did not meet a trashed snapshot; or a step
    of 1 to 3 failed, for example S1 is not trashed after its removal.
  - `another fault`: the request both refused and skipped S1, the
    replayer's unlink after S1 failed with another errno, or it failed after
    the request skipped S1.
  - No outcome within 4 minutes: the replayer never unlinked from S1, or the
    request logged neither line, for example because the source's debug
    level did not reach it.
  - S1 is no longer trashed after the unlink, or the fourth write does not
    replicate.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each; the time includes fixture teardown):

| Image | Result | Fourth write | Time |
| --- | --- | --- | --- |
| Tentacle 20.2.1, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all` (the default image when recorded) | `REPRODUCED`: `not mirror snapshot (snap_id=4)`, replayer `(22) Invalid argument`, status `up+error` | 19 s | 82 s |
| Tentacle 20.2.2, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all` | `NOT_REPRODUCED`: `missing or trashed snapshot: snap_id=4`, status `up+replaying` | 2 s | 60 s |
| Squid 19.2.3, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.3-all` | `REPRODUCED`, the same as 20.2.1 | 19 s | 90 s |
| Squid 19.2.4, `quay.io/ceph/ceph:v19.2.4` | `NOT_REPRODUCED`, the same as 20.2.2 | 2 s | 76 s |

In the 20.2.2 run the request's skip line came before the replayer's unlink
line in the receiver log, which is why the judgement does not use their
order. Every run ended with no testcontainers container left on the host.

The table is from separate `go test` runs. The scripted pair as
`Experiments` runs it (`run_experiment.py --image
ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.2-all`) also passed
on the same host: the default run printed `REPRODUCED` (80 s) and the fixed
run `NOT_REPRODUCED` (63 s).

## Cleanup and limits

The pools, images, clone and the trashed S1 end with the clusters; the link
and the clusters are terminated by their cleanups. The debug settings live
in the clusters' configuration and end with them.

Not covered: arm64, Tentacle 20.2.0 and Squid releases before 19.2.3
(source boundary only), the upstream race itself (a concurrent removal that
trashes the snapshot only during the unlink), two-way mirroring, journal
mode, and how long the image replayer stays stopped on a reproducing image
beyond the one fourth-write time recorded.
