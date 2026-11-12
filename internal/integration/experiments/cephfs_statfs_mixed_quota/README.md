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

The test fails on Tentacle 20.2.4. That is the expected result for an
unpatched defect; it is not a fixture failure.

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

Deliberately failing reproducer: one run takes about 35 seconds and
reproduces on Tentacle 20.2.4, so without an expectation it fails there. No automatic run includes it (the default checks of a pull
request, pushes to `main`, periodic samples), and it never gates anything. A
maintainer can run it on demand, also on a pull request's head with the
comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_statfs_mixed_quota`.
- Comment: `/ci run cephfs_statfs_mixed_quota` or
  `/ci run cephfs_statfs_mixed_quota on umbrella`.

## Running it

Prepare the pinned role images of one supported release (20.2.4, 21.2.0 or
19.2.5) and export them:

```sh
release=20.2.4
dir="artifacts/roles-$release-$(date -u +%Y%m%dT%H%M%SZ)"
python3 .github/scripts/run_image_matrix.py --variant official --layout roles \
  --release "$release" --roles essential,client,cephfs --prepare-only --output-dir "$dir" \
  --github-env "$dir/role-images.env" &&
  . "./$dir/role-images.env" &&
  export CEPH_TEST_ESSENTIAL_IMAGE CEPH_TEST_CLIENT_IMAGE CEPH_TEST_CEPHFS_IMAGE
```

Tentacle 20.2.5 has no published role images. Run it on the upstream
aggregate that `.github/ceph-discovery-images.json` pins, with no role
variable set (`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v20.2.5@sha256:...`).

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSStatfsWithNestedByteAndFileQuotas$' \
  ./internal/integration/experiments/cephfs_statfs_mixed_quota
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch cephfs_statfs_mixed_quota \
  --package ./internal/integration/experiments/cephfs_statfs_mixed_quota \
  --directory "artifacts/cephfs-statfs-mixed-quota-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

The test logs the client's `ceph --version` and the release its `cephfs`
module reports; record the image digests with the result.

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

The results below ran on the 20.2.4 role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, cephfs `9566841eba0a…`), the
19.2.5 role images (essential `43ee23624e7a…`, client `a69e1319ce88…`, cephfs `99f0ff6e827c…`), the 21.2.0 role images
(essential `119180026952…`, client `e5f7edab88b6…`, cephfs `7ae447d4d414…`) and the 20.2.5 aggregate `sha256:4337ac8ac81d…`, each with
the client module of its own release.

Tentacle 20.2.4 (33 s) reproduces the defect (`REPRODUCED`): it reports 236
blocks with 236 free at `parent/child`, which is what `unlimited` reports,
while every control reports its quota. Squid 19.2.5 (40 s), Tentacle 20.2.5
(34 s) and Umbrella 21.2.0 (34 s) do not (`NOT_REPRODUCED`): every directory
below a byte quota reported that quota.

## Verdict

The test is the reproduction unit `cephfs_statfs_mixed_quota` with one
planned observation, `statfs-below-byte-quota`, which judges every
directory below a byte quota. It prints one line that starts with
`REPRODUCTION_VERDICT case=cephfs_statfs_mixed_quota`.

- `REPRODUCED`: every condition held, every control directory reported its
  byte quota, and `parent/child` did not. The observation names
  `parent/child` with the blocks it reported. Without an expectation the
  test fails with `observed defect: statfs-below-byte-quota:`.
- `NOT_REPRODUCED`: every condition held and every directory below a byte
  quota reported that quota, `parent/child` included. Without an
  expectation the test passes. On a build claimed to be affected, check in
  the receipt (`CEPHFS_STATFS_MIXED_QUOTA_RECEIPT`) that `parent/child` has
  `max_files` 100 and `max_bytes` 0 and that the client reports the release.
- `INCONCLUSIVE`: no verdict on the symptom; the test fails and the reason
  starts with its category. `unavailable prerequisite:` the cluster, the
  filesystem or the client does not start, or the client's `python3` cannot
  mount or prepare the layout. `condition not established:` a condition of
  the list above did not hold. `control failed:` a directory other than
  `parent/child` did not report its byte quota, so the trigger's result says
  nothing about the nested quotas.

An expectation turns the verdict into the result: only the expected verdict
passes, and a skip or `INCONCLUSIVE` never does. With the role images of a
release exported as above, 20.2.4 expects `REPRODUCED`:

```sh
CEPH_TEST_EXPECTED_VERDICT=cephfs_statfs_mixed_quota=REPRODUCED \
  python3 .github/scripts/tag_scenarios.py optional --batch cephfs_statfs_mixed_quota \
  --package ./internal/integration/experiments/cephfs_statfs_mixed_quota \
  --directory "artifacts/cephfs-statfs-mixed-quota-$(date -u +%Y%m%dT%H%M%SZ)"
```

and 21.2.0 or 19.2.5, the fixed builds, expect
`CEPH_TEST_EXPECTED_VERDICT=cephfs_statfs_mixed_quota=NOT_REPRODUCED`.

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
