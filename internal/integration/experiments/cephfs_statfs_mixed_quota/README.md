# libcephfs statfs below a byte quota and a nearer file quota

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSStatfsWithNestedByteAndFileQuotas`, optional batch
`cephfs_statfs_mixed_quota`. The judgement of a report has Docker-free tests
in the same package (`code/pure`).

## Use case and native contract

A service reads the capacity of the directory it works in with statfs
through libcephfs and decides from it whether a quota-scoped environment has
room. With `client_quota_df` (on by default), statfs at a directory below a
byte quota reports the quota as the capacity and the quota minus the
recursive bytes of the quota's directory as free space. A file quota on a
directory in between does not limit bytes, so it must not change that.

Tentacle 20.2.4 picks one quota root for both observations: the nearest
directory that has any quota (`Client::_statfs`, `src/client/Client.cc`).
At a directory that has only `ceph.quota.max_files`, below a directory with
`ceph.quota.max_bytes`, that root has no byte quota, and statfs reports the
capacity and the free space of the data pool. Squid 19.2.5, Tentacle 20.2.5
and Umbrella 21.2.0 look up the nearest byte quota and the nearest file quota
separately and report the parent's byte quota.

Upstream references: the
[Tentacle 20.2.5 release notes](https://docs.ceph.com/en/latest/releases/tentacle/)
list the mixed-quota statfs handling; the two forms of `Client::_statfs` are
in [`v20.2.4`](https://github.com/ceph/ceph/blob/v20.2.4/src/client/Client.cc)
and [`v20.2.5`](https://github.com/ceph/ceph/blob/v20.2.5/src/client/Client.cc).

This is a capacity observation only. Enforcement of either quota is not
part of the case, and nothing here shows it broken.

The defect is reproduced on Tentacle 20.2.4 and not on Tentacle 20.2.5.

## Requirements

- One cluster with one filesystem (`cephfs.Run`) and its client toolkit,
  whose `python3` has the `cephfs` module of the same release. Image roles
  `essential`, `client` and `cephfs`, of one release.
- The client runs with its defaults; the test reads `client_quota` and
  `client_quota_df` from the mounted client and needs both `true`.
- The data pool must report more than 64 MiB of capacity, so that it can be
  told apart from the quota. A fixture cluster reported 944 MiB.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer. The test fails only when its setup, a condition or a
control does not hold; the documented symptom does not fail it. It prints
one `EXPERIMENT_RESULT` line instead, `REPRODUCED` on its default image,
Tentacle 20.2.4. No automatic run includes it (the default checks of a pull
request, pushes to `main`, periodic samples), and it never gates anything. A
maintainer can run it on demand, also on a pull request's head with the
comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_statfs_mixed_quota`.
- Comment: `/ci run cephfs_statfs_mixed_quota` or
  `/ci run cephfs_statfs_mixed_quota on umbrella`.

Every container the test starts uses the package variable `image`, so the
role images that a batch exports do not reach it.

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSStatfsWithNestedByteAndFileQuotas$' \
  ./internal/integration/experiments/cephfs_statfs_mixed_quota
```

A fixed image, Tentacle 20.2.5 (or Umbrella 21.2.0), prints `NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_statfs_mixed_quota_test.image=quay.io/ceph/ceph:v20.2.5" \
  -run '^TestCephFSStatfsWithNestedByteAndFileQuotas$' \
  ./internal/integration/experiments/cephfs_statfs_mixed_quota
```

The test logs the client's `ceph --version` and the release its `cephfs`
module reports, and the `CEPHFS_STATFS_MIXED_QUOTA_RECEIPT` line gives the
fresh mount's report.

## Observations and controls

One libcephfs client makes the layout below `/statfs-mixed-quota`, sets the
quotas and writes one 4 MiB file into each directory. Byte quotas are 64 MiB
(16 blocks of the 4 MiB that libcephfs reports), file quotas 100.

| Directory | Quotas | Byte quota that applies | Role |
| --- | --- | --- | --- |
| `bytes-only` | bytes | its own | control |
| `same` | bytes and files | its own | control |
| `parent` | bytes | its own | control |
| `parent/plain` | none | `parent` | control: inherited without a nearer quota |
| `parent/child` | files | `parent` | trigger |
| `unlimited` | none | none | shows the data pool's capacity |

A second, fresh libcephfs mount then observes. It reads every directory's
`ceph.dir.rbytes` every two seconds, for at most 90 seconds, until all are
the layout's, and keeps every reading. Then it reads each directory once:
its inode, both quota attributes with whether each is present, its recursive
statistics and statfs.

1. Conditions: the observing client is not the preparing one and runs with
   `client_quota` and `client_quota_df`; the recursive bytes converged;
   every directory has an inode of its own, the quota values of the layout
   and its recursive bytes; statfs reports 4 MiB blocks; `unlimited` reports
   more than 16 blocks. A directory with one quota reports the other
   attribute as present with the value 0, and a directory with none reports
   both as absent; the conditions require exactly that.
2. Controls: `bytes-only`, `same`, `parent` and `parent/plain` report 16
   blocks, with 15 free for the first two and 13 free below `parent`, which
   holds three files.
3. Trigger: `parent/child` must report 16 blocks with 13 free.

`f_files` is recorded in the receipt and not judged: it is a count of
entries in use, not the file quota, and the releases differ in which
directory they count (20.2.4 reported 6 at `parent` and `parent/plain`,
the other releases 2 everywhere).

## Reading the result

Standalone runs, Linux amd64 host, bridge networking, one run per image:

- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded): `REPRODUCED` in 33 s; `parent/child` reported 236 blocks
  with 236 free, as the directory without a byte quota does.
- Tentacle 20.2.5 `quay.io/ceph/ceph:v20.2.5@sha256:4337ac8ac81d…`:
  `NOT_REPRODUCED` in 34 s.
- Umbrella 21.2.0 `quay.io/ceph/ceph:v21.2.0@sha256:c9e6ed4ac3fc…`:
  `NOT_REPRODUCED` in 34 s.

The release comparison ran on the 20.2.4 role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, cephfs `9566841eba0a…`), the
19.2.5 role images (essential `43ee23624e7a…`, client `a69e1319ce88…`, cephfs `99f0ff6e827c…`), the 21.2.0 role images
(essential `119180026952…`, client `e5f7edab88b6…`, cephfs `7ae447d4d414…`) and the 20.2.5 aggregate `sha256:4337ac8ac81d…`, each with
the client module of its own release.

- **NOT_REPRODUCED:** every directory below a byte quota reported that
  quota. Squid 19.2.5 (40 s), Tentacle 20.2.5 (34 s) and Umbrella 21.2.0
  (34 s) do this.
- **REPRODUCED:** the symptom names `parent/child` with the blocks it reported. Tentacle 20.2.4 (33 s) reports
  236 blocks with 236 free there, which is what `unlimited` reports, while
  every control reports its quota.
- **NOT_REPRODUCED on a build claimed to be affected:** check in the
  receipt (`CEPHFS_STATFS_MIXED_QUOTA_RECEIPT`) that `parent/child` has
  `max_files` 100 and `max_bytes` 0 and that the client reports the release.
- **Unavailable prerequisite:** the failure starts with
  `unavailable prerequisite`: the cluster, the filesystem or the client does
  not start, or the client's `python3` cannot mount or prepare the layout.
- **Condition not established:** the failure starts with
  `condition not established` and names the condition of the list above
  that did not hold. It is not a reproduction and not a pass.
- **Control failed:** the failure starts with `control failed`: a directory
  other than `parent/child` did not report its byte quota. The trigger's
  result then says nothing about the nested quotas.

## Cleanup and limits

A third libcephfs mount unlinks the files, removes the quotas and the
directories and checks that `/statfs-mixed-quota` is gone, before the client
and the cluster are terminated, also after a failure. A failed removal is
reported in addition to the test's own failure.

Not covered: the kernel client and `ceph-fuse`, arm64, a mount rooted below
the quota directories, quotas that are nearly or wholly used, snapshots,
several data pools, and the reverse nesting (a file quota above a byte
quota). By the source the affected release reports the byte capacity
correctly there, because the nearest quota is then the byte quota; that was
not run.
