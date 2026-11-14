# CephFS mirror blockdiff across two empty object versions aborts the MDS

Owner: CephFS mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions`, optional batch
`cephfs_mirror_blockdiff_empty`. Its judgement of the outcome and its readers
of object versions, of the request counter and of the snapshot report have
Docker-free tests in the same package (`code/pure`).

## Use case and native contract

`cephfs-mirror` synchronizes a new snapshot of a directory incrementally
when the previous snapshot is still on the source. For a file that changed
between the two snapshots it asks the source MDS which blocks changed (a
blockdiff request). The MDS lists the versions of each of the file's data
objects and aggregates the extents that differ between the version that
holds the previous snapshot and the one that holds the current snapshot.
Whatever the versions look like, the MDS must answer and stay up.

The defect: when the version that follows is empty, the aggregation treats
the object as a hole and inserts the length of the previous version into the
set of changed extents. When the previous version is empty too, that length
is zero. On Squid 19.2.5 inserting an interval of length zero fails
`ceph_assert(len > 0)` (`src/include/interval_set.h`), called from
`MDCache::aggregate_snap_sets`, and the MDS aborts. The directory then stays
at `syncing`, and with a single MDS the filesystem has no metadata server;
the MONs keep listing the dead daemon as active for minutes. Tentacle 20.2.4
and Umbrella 21.2.0 run the same aggregation, but their interval set ignores
an insert of length zero, so the request returns no changed extent and the
snapshot arrives.

The request is reached only for a file with a nonzero size, so the trigger
file is 4096 bytes long without data: its first object exists, empty, in
both snapshots.

Upstream reference: [tracker 82726](https://tracker.ceph.com/issues/82726).

## Requirements

- Two clusters, one OSD each, one CephFS filesystem with one MDS and no
  standby in each, and a `cephfsmirror.Run` link with one daemon and one
  directory. Both clusters, their client containers and the mirror daemon
  run the package variable `image`, an aggregate image whose `python3` has
  the `cephfs` module.
- The test bounds itself to 20 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Squid
19.2.5 by default, which reproduces the defect. Its given and when steps
fail the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=cephfs_mirror_blockdiff_empty_versions result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it (the default checks of a
pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Experiments" -f case=cephfs_mirror_blockdiff_empty_versions`,
  with `-f image=<fixed image>` for the second run, or
  `gh workflow run "Native regressions" -f batch=cephfs_mirror_blockdiff_empty`
  for its pass or failure only.
- Comment: `/ci run cephfs_mirror_blockdiff_empty`.

Every container the test starts uses `image`, so the role images that a
batch exports, and the release a comment names, do not reach it.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions$' \
  ./internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions
```

On another image, injected into the package's `image` variable. Tentacle
20.2.4 is fixed and must print `result=NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" \
  -run '^TestCephFSMirrorBlockdiffAcrossEmptyObjectVersions$' \
  ./internal/integration/experiments/cephfs_mirror_blockdiff_empty_versions
```

The test logs the client's `ceph --version`. The
`CEPHFS_MIRROR_BLOCKDIFF_EMPTY_VERSIONS_RECEIPT` line gives the MDS process
before and after the trigger, the blockdiff request counter, the mirror's
last status and whether the snapshot is on the destination.

## Observations and controls

The mirrored directory `/probe` holds `f`, the trigger file, and `g`, a file
that always has data. A Python process in a client container changes the
files through libcephfs, takes each snapshot and reports the size and
SHA-256 of both files in it; the same script reads the snapshots on the
destination. The versions of a file's first data object come from
`rados listsnaps` in the source client container. The number of blockdiff
requests the source MDS served is `req_blockdiff_latency.avgcount` of
`ceph tell mds.<id> perf dump mds_server`. The MDS process is identified by
its container's start time and, while the container runs, the GID of the
filesystem's one active daemon in the FSMap.

Every value a decision rests on must be in what was read: the counter's
count as a number, each object version's ID and size and each clone's
snapshot IDs, each snapshot report's presence, ID and files with inode
number, size and SHA-256. A count or a size of 0 is an observation; a
missing or null one fails the test and is never read as 0.

An image whose mirror daemon has `cephfs_mirror_blockdiff_min_file_size`
(Umbrella) copies files up to that size whole and never asks for their
changed blocks. The test sets it to 0 for clients of the source cluster
before the daemon starts, when `ceph config help` knows the setting; Squid
and Tentacle have none and always ask.

1. Baseline `s1`: both files hold 1 MiB of data. The mirror starts, and the
   snapshot must arrive with the source's sizes and content.
2. Control `s2`: `f` is truncated to nothing and extended to 4096 bytes
   without data, and the first 64 KiB of `g` are rewritten. The snapshot
   must arrive with the source's sizes and content, the MDS must have served
   more blockdiff requests than before, and the same MDS process must be
   live. The first object of `f` must have data in the version of `s1` and
   an empty head, and that of `g` data in both. A blockdiff over a hole
   after a version with data, and one over two versions with data,
   therefore work on the image.
3. Trigger `s3`, with the mirror daemon stopped: one byte is written to `f`
   and flushed, which makes the object store keep the empty object as the
   version of `s2`; then `f` is truncated and extended as in step 2. `g` is
   not changed, so `f` is the only changed file of `s3`. The first object of
   `f` must have an empty version for `s2` and an empty head within a
   minute, and the MDS process must still be the one of step 2.
4. The daemon is started again. The test waits until the mirror reports
   `s3` as last synchronized, the MDS container stops running, or four
   minutes pass.

## Reading the result

- **REPRODUCED:** the MDS container of step 2 was read as stopped, with the
  start time of step 2 and a nonzero exit code, its log was read to the end,
  and the log has `FAILED ceph_assert(len > 0)` followed by a backtrace line
  with `MDCache::aggregate_snap_sets(`. The symptoms give the container's
  state, those two log lines, and the mirror's last status with whether `s3`
  is on the destination. Both log lines and the exit code were observed on
  the default image, whose MDS prints a backtrace with function names; the
  assertion text is that of `src/common/assert.cc` for the condition in
  `src/include/interval_set.h`. An image whose backtrace has no function
  names would fail the test as `another fault` instead of printing a
  result.
- **NOT_REPRODUCED:** the MDS container was read as running with the start
  time and the GID of step 2, its blockdiff request counter was read again
  and rose during step 4, the mirror reported `s3` as last synchronized, and
  `s3` was read on the destination with both files at the source's sizes and
  SHA-256. Since only `f` changed in `s3`, the further
  request was for `f`.
- **Test failure (no result):**
  - `condition not established` or `control failed`: the baseline or the
    control snapshot does not arrive intact, the control was synchronized
    without a blockdiff request, the objects of the control do not have the
    expected versions, or the MDS process changed before the trigger.
  - `trigger not established`: the first object of `f` does not get the two
    empty versions, or the MDS stayed live and its counter did not rise in
    step 4. An image that copies the file whole ends here instead of
    printing `NOT_REPRODUCED` for a question it was not asked.
  - `outcome not observed`: after the trigger the state of the MDS
    container or the FSMap could not be read, the log of a stopped
    container could not be read to its end, or the counter of a live MDS
    could not be read. A failed observation decides nothing, also when the
    part of the log that was read has the assertion.
  - `another fault:`: the MDS container stopped without the assertion
    followed by the aggregation in its log, with exit code 0, or after a
    restart; it runs another process or GID; or the same process is live
    and served the request but `s3` did not arrive within four minutes or
    differs from the source.
  - A cluster, a filesystem, a client or the mirror does not start, a
    command of the case fails, or something read lacks a value (see above),
    such as an MDS without a blockdiff request counter.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each unless noted):

| Image | Result | Time |
| --- | --- | --- |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:d0fcbea14a2a…` (the default image when recorded) | `REPRODUCED`: 4 blockdiff requests in the control; in step 4 the MDS container exited with code 1, its log has `interval_set.h: 493: FAILED ceph_assert(len > 0)` under `MDCache::aggregate_snap_sets`; the mirror stayed at `syncing` with `s2` last synchronized and `s3` absent on the destination | 148 s |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` | `NOT_REPRODUCED`: 4 blockdiff requests in the control and 2 more in step 4, the same MDS process and GID, `s3` last synchronized and equal on the destination | 131 s |
| Umbrella 21.2.0, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:7fce1d2de623…` | `NOT_REPRODUCED`: 4 blockdiff requests in the control and 2 more in step 4, the same MDS process and GID, `s3` last synchronized and equal on the destination; the threshold was set to 0 | 140 s |

## Cleanup and limits

The mirror daemon, the client containers and both clusters are terminated by
their cleanups, also after the source MDS aborted; the snapshots and the
`client` configuration entry stay in the discarded clusters.

Not covered: arm64, Squid releases other than 19.2.5, a filesystem with a
standby MDS (whether the daemon that takes over aborts on the mirror's
retry), a host-network topology, Umbrella with its default threshold and a
trigger file above it, and trigger files whose empty object is not the
first.
