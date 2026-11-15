# Subvolume removal with retained snapshots on a full cluster

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSRetainedSnapshotRemovalOnAFullPool`, optional batch
`cephfs_retained_snapshot_full_pool`. The judgement of the reports has
Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A service removes a subvolume with `ceph fs subvolume rm --retain-snapshots`
and expects either a `snapshot-retained` subvolume whose snapshots it can
still read and clone, or an error that leaves the subvolume as it was, so that
it can retry. A v2 subvolume keeps its live data in an incarnation directory
(`/volumes/<group>/<name>/<uuid>`) and its state in a metadata file
(`/volumes/<group>/<name>/.meta`) whose `GLOBAL` section names that directory
as `path` with `state = complete`.

Before the correction, `SubvolumeV2.remove`
(`src/pybind/mgr/volumes/fs/operations/versions/subvolume_v2.py`) first renames
the incarnation directory into the subvolume's `.trash` and only then writes
the metadata with an empty `path` and `state = snapshot-retained`. When the
cluster is full, the rename succeeds (the MGR's MDS caps include the full
permission) but the metadata write fails with `ENOSPC`, usually at `fsync`. The
metadata keeps naming the incarnation, which is gone, as complete: every
later volumes call on the subvolume opens it by that path and fails with
`ENOENT` ("mount path missing"). `getpath`, `info`, `snapshot getpath` and a
retried removal all fail, so the retained snapshot is no longer reachable
through the volumes interface, although its bytes are still in the snapshot
directory. The command itself returns `EINVAL` with the Python traceback of
the unhandled `ENOSPC` of the metadata write.

The correction writes the metadata first and renames the directory only
after the write succeeded: on a full cluster the removal fails with
`ENOSPC: fsync failed` and changes nothing, and once the cluster has space
again a retry retains the snapshot. Upstream references:
[tracker 67330](https://tracker.ceph.com/issues/67330), the correction
[ceph/ceph#59676](https://github.com/ceph/ceph/pull/59676), backported to
Tentacle 20.2.1 and Squid 19.2.7. In the release sources, Squid 19.2.0 to
19.2.6 and Tentacle 20.2.0 have the old order.

## Requirements

- One cluster with one filesystem (`cephfs.Run`) and its client toolkit,
  whose `python3` has the `cephfs` module of the same release, all from the
  case's image. Image roles `essential`, `client` and `cephfs`, of one
  release.
- The cluster is disposable: the case lowers its global full thresholds
  (`override.SetFullRatios`) and restores them.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Known-defect reproducer, optional batch `cephfs_retained_snapshot_full_pool`
with a 20-minute budget; no automatic run includes it. On its default image,
Tentacle 20.2.0, it prints `REPRODUCED`; Tentacle 20.2.1 is fixed and prints
`NOT_REPRODUCED`, so it is the fixed image of its `Experiments` dispatch
(`-f image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all`).
The measured times are under [Reading the result](#reading-the-result). A
comment run is `/ci run cephfs_retained_snapshot_full_pool`; it runs the
default image whatever release the batch names. The judgement tests
run in `code/pure` with every change (under 1 s). The shared triggers, result
checks and image injection are in [Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSRetainedSnapshotRemovalOnAFullPool$' \
  ./internal/integration/experiments/cephfs_retained_snapshot_full_pool
```

A fixed image, Tentacle 20.2.1 (or Squid 19.2.7), prints `NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_retained_snapshot_full_pool_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.1-all" \
  -run '^TestCephFSRetainedSnapshotRemovalOnAFullPool$' \
  ./internal/integration/experiments/cephfs_retained_snapshot_full_pool
```

The test logs the client's `ceph --version`; the
`CEPHFS_RETAINED_SNAPSHOT_FULL_POOL_RECEIPT` lines give what the client read
before and after the failed removal (the metadata file, the live and
snapshot files, the subvolume and `.trash` directories), the health checks,
the removal's error and the volumes interface's answers.

## Observations and controls

1. Given: a subvolume `retained-full` in the default group holds a 256 KiB
   file; a snapshot `checkpoint` is taken, and the live file is then
   replaced with different content, so the two byte sequences tell the live
   incarnation and the snapshot apart.
2. Controls: `getpath` and `snapshot getpath` return the typed handles'
   paths; a libcephfs client reads the snapshot content at the snapshot path
   and the live content at the live path; the metadata file says version 2,
   type `subvolume`, the incarnation path and `state = complete`. A
   subvolume that is not v2 does not establish the condition.
3. Trigger: the thresholds are lowered to 0.0001, 0.0002 and 0.0003. The
   case waits for `OSD_FULL`, then until every pool carries the `full` flag
   and every OSD reports that osdmap as its newest map (`ceph tell osd.N
   status`), so that no OSD that has not seen the full map can accept the
   metadata write. Without this wait a removal on 20.2.0 once succeeded
   right after `OSD_FULL` appeared. It then runs
   `fs subvolume rm --retain-snapshots`, which must fail; a success does not
   establish the condition.
4. The thresholds are restored and read back equal to the originals, and the
   case waits until no fullness check (`OSD_FULL`, `OSD_NEARFULL`,
   `OSD_BACKFILLFULL`, `POOL_FULL` and the pool variants) remains.
5. Then: a fresh libcephfs mount reads the metadata file, the live and
   snapshot files and the directories; `getpath`, `snapshot getpath` and
   `info` are asked, and the removal is retried. After a successful retry,
   `info` must report `snapshot-retained`, `getpath` must fail, and
   `snapshot getpath` must return the same snapshot path, where the
   snapshot content is read again.

## Reading the result

Standalone runs, Linux amd64 host, Docker 29.9.1, bridge networking, the
published aggregate images, one cluster per run:

| Release | Aggregate image | Result | Runs and test times |
| --- | --- | --- | --- |
| Tentacle 20.2.0 (default) | `@sha256:2ca7d48354d2…` | `REPRODUCED` | 3 runs: 70 s, 64 s, 67 s |
| Tentacle 20.2.1 | `@sha256:95c4afd5c8f5…` | `NOT_REPRODUCED` | 2 runs: 72 s, 72 s |
| Squid 19.2.6 | `@sha256:d6e6222e6900…` | `REPRODUCED` | 1 run: 73 s |
| Squid 19.2.7 | `@sha256:17bb5a8e830d…` | `NOT_REPRODUCED` | 1 run: 79 s |

The images are `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-<release>-all`,
with the digests they had when the runs were recorded. Every run cleaned up
and left no test containers.

On the affected releases the removal returned `EINVAL` with the traceback of
the unhandled `cephfs.NoSpace` from the metadata write (at `fsync`, or once at
the write itself). After the thresholds were restored the
metadata still named the incarnation as complete, the incarnation was in the
subvolume's `.trash`, `getpath`, `snapshot getpath`, `info` and the retry
failed with `ENOENT` (mount path missing), and the snapshot content was still
readable at the snapshot directory through libcephfs. On the fixed releases
the removal returned `ENOSPC: fsync failed`, the metadata, the incarnation and
both contents were unchanged, and the retry retained the snapshot with its
content.

- **NOT_REPRODUCED:** the failed removal changed nothing that the volumes
  interface or a client sees, and the retry retained the snapshot.
- **REPRODUCED:** the symptoms name what was lost: a metadata file that names
  a gone incarnation, a failing `getpath` or `snapshot getpath`, unreadable
  snapshot content, a failing retry, or a retry that does not leave a
  `snapshot-retained` subvolume with the snapshot readable at its path.
- **Unavailable prerequisite:** the failure starts with
  `unavailable prerequisite`: the cluster, the filesystem, the subvolume or
  its snapshot does not start or cannot be made, or the client's `python3`
  cannot write or read.
- **Condition not established:** the failure starts with
  `condition not established`: the subvolume is not a complete v2 subvolume,
  the thresholds could not be lowered or restored, the fullness checks did
  not appear or clear, the full map did not reach every OSD, or the removal
  succeeded on the full cluster. It is not a reproduction and not a pass.
- **Control failed:** the failure starts with `control failed`: the paths or
  contents before the removal were not the ones written.
- **Another fault:** the failure starts with `another fault:`: after the
  failed removal the metadata is unreadable or names another state or path,
  the live file is unreadable for another reason or holds other bytes, or a
  getpath answers another path. It is not the defect of this case.

## Cleanup and limits

The threshold override is restored in the test and again by a cleanup,
also after a failure; a failed restoration is reported in addition to the
test's own failure. On a fixed release the case removes the retained
snapshot, which removes the retained subvolume, and checks that the name is
no longer listed. A subvolume the defect left inconsistent cannot be removed
through the volumes interface; it goes with the cluster, which is terminated
after every run.

Not covered: subvolume groups other than the default one, v1 subvolumes,
clones of the retained snapshot, a full condition reached by real data
instead of lowered thresholds, pool quotas, arm64, host networking, and the
MGR's asynchronous purge of the trashed incarnation.
