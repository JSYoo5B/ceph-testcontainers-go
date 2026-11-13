# libcephfs lists the live directory through an open snapshot handle after its cache is trimmed

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSSnapshotListingAcrossCacheEviction`, optional batch
`cephfs_snapshot_readdir_eviction`. The judgement of the listings has
Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A service walks a snapshot of a large directory through libcephfs
(`D/.snap/s1`) while it also touches other files on the same mount, for
example to compare the snapshot with another tree. A directory handle of a
snapshot must return the snapshot's entries, each once, whatever else the
client does and however small its cache is.

A snapshot directory with more entries than one readdir reply holds is read
with several requests. For each one the client rebuilds the request path
from the directory inode's dentries up to the snapshot directory
(`Client::_readdir_r_cb`, `Inode::make_nosnap_relative_path`,
`src/client/`). The open handle pins the inode but not those dentries. When
the client's cache holds more than `client_cache_size` dentries, its
periodic trim drops the snapshot's child dentries and then the dentry of
the snapshot itself. The inode is left without a dentry, the rebuilt path
is the bare inode number, and the MDS answers the next request with the
live directory from the requested offset. The caller gets no error: the
rest of the listing holds live names, and most of the snapshot's names are
missing.

The trim runs on the client's tick (once a second by default), so the
window opens only when a tick passes between two requests of the handle
while the cache is over its limit. With the default cache of 16384 entries
the case's 5000 dentries are never trimmed.

Squid 19.2.2 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0
all do this; no release that was run lists the snapshot. Upstream
reference:
[tracker 82652](https://tracker.ceph.com/issues/82652).

The defect is reproduced on every release it ran on.

## Requirements

- One cluster with one filesystem (`cephfs.Run`) that allows snapshots, and
  its client toolkit, whose `python3` has the `cephfs` module of the same
  release. Image roles `essential`, `client` and `cephfs`, of one release.
- The listing clients mount as `client.admin`, write their own log with
  `debug_client` 15 to a file in the client container's `/tmp` (about
  100 MB per listing) and remove it after reading it.
- The kernel client and `ceph-fuse` are not used: there the kernel's lookup
  references keep the dentries pinned, which hides this path.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer. The test fails only when its setup, a condition or a
control does not hold; the documented symptom does not fail it. It prints
one `EXPERIMENT_RESULT` line instead, `REPRODUCED` on its default image,
Tentacle 20.2.4. No automatic run includes it (the default checks of a pull
request, pushes to `main`, periodic samples), and it never gates anything. A
maintainer can run it on demand, also on a pull request's head with the
comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_snapshot_readdir_eviction`.
- Comment: `/ci run cephfs_snapshot_readdir_eviction` or
  `/ci run cephfs_snapshot_readdir_eviction on umbrella`.

Every container the test starts uses the package variable `image`, so the
role images that a batch exports do not reach it.

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSSnapshotListingAcrossCacheEviction$' \
  ./internal/integration/experiments/cephfs_snapshot_readdir_eviction
```

No release is fixed. Another image runs the same way, for example Umbrella 21.2.0:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_snapshot_readdir_eviction_test.image=quay.io/ceph/ceph:v21.2.0@sha256:c9e6ed4ac3fcf67b28f38358bb9619dee4d1dfb96ddd4b9b583eee316a15f04d" \
  -run '^TestCephFSSnapshotListingAcrossCacheEviction$' \
  ./internal/integration/experiments/cephfs_snapshot_readdir_eviction
```

The test logs the client's `ceph --version` and the release its `cephfs`
module reports, and one `CEPHFS_SNAPSHOT_READDIR_EVICTION_RECEIPT` line per
listing client.

## Observations and controls

One libcephfs client prepares `/snapshot-readdir-eviction`: 3000 files
`snap-0000` to `snap-2999` in `dir`, 2000 files in `other`, the snapshot
`dir/.snap/s1`, and then `dir`'s files replaced by 3000 files `live-0000`
to `live-2999`. The two sets of names are disjoint, so every name a listing
returns says which state of `dir` it came from.

Two more clients, each a mount of its own, then do the same: open
`dir/.snap/s1`, read 50 entries, look up every file of `other`, wait four
seconds (several client ticks), read the rest through the same handle, and
list `dir`. The first runs with the default `client_cache_size`, the second
with 100. After its unmount each reads from its own log, by timestamp, the
readdir requests it sent while the handle was open, how many of them did
not name the snapshot in their path, and whether it dropped its dentry of
`s1` between the first entries and the rest.

1. Conditions, both clients: 50 entries were read first, 2000 files looked
   up, and the handle needed at least two readdir requests.
2. Conditions, control client: its cache has room for all 5000 entries, and
   it did not drop its dentry of `s1`.
3. Conditions, trigger client: its cache is 100 entries, and its log shows
   the dentry of `s1` dropped between the two reads. Without that the
   window did not open, and the run says nothing.
4. Control: the client with the default cache lists exactly the snapshot's
   3000 names through the interrupted handle. Reading on after lookups and
   a wait is not what breaks the listing.
5. Control: both clients list exactly the 3000 live names in `dir`. The
   trimmed client reads the live directory correctly, and the snapshot did
   not change it.
6. Trigger: the client with 100 entries must list exactly the snapshot's
   3000 names through its handle.

## Reading the result

Standalone runs, Linux amd64 host, bridge networking, one run per image:

- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (default): `REPRODUCED` in 47 s; the trigger handle returned 2987
  entries, 372 snapshot names and 2615 live names, and 7 of its 8 readdir
  requests did not name the snapshot.

The release comparison ran on the 20.2.4 role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, cephfs `9566841eba0a…`), the
19.2.5 role images (essential `43ee23624e7a…`, client `a69e1319ce88…`, cephfs `99f0ff6e827c…`), the 21.2.0 role images
(essential `119180026952…`, client `e5f7edab88b6…`, cephfs `7ae447d4d414…`) and the upstream aggregates of 20.2.5
(`sha256:4337ac8ac81d…`) and 19.2.2 (`sha256:8214ebff6133…`), each with the client module of its own release.

All five showed the defect with the same numbers, after every
condition and control held: Tentacle 20.2.4 (47 s), Tentacle 20.2.5 (47 s),
Umbrella 21.2.0 (47 s), Squid 19.2.5 (53 s) and Squid 19.2.2 (51 s). The
trigger client dropped its dentry of `s1` during the wait and then sent 7
of its 8 readdir requests without the snapshot in the path. Its handle
returned 2987 entries: 372 of the 3000 snapshot names and 2615 live names.
The client with the default cache returned exactly the 3000 snapshot names
with 2 to 5 requests, all naming the snapshot, and both clients listed
exactly the 3000 live names in `dir`.

With the wait removed (a temporary edit, on 21.2.0) the trigger client read
all 3000 snapshot names with 2 requests, its log showed no dropped dentry,
and the test stopped with `condition not established`: lookups alone, faster
than a tick, do not open the window.

- **NOT_REPRODUCED:** the trigger client dropped its dentry of the snapshot and still
  listed exactly the snapshot.
- **REPRODUCED:** the first
  50 entries are distinct names of the snapshot, and the entries read after
  the dentry was dropped hold live names. It gives the counts of the trigger's snapshot listing: entries returned, snapshot
  names found and missing, and live names, with a few of each, and how many
  of the handle's readdir requests did not name the snapshot (the symptom). The receipt
  (`CEPHFS_SNAPSHOT_READDIR_EVICTION_RECEIPT`) quotes the client's log line
  for the dropped dentry and the first two such requests.
- **NOT_REPRODUCED on a build claimed to be affected:** check in
  the trigger's receipt that the dentry was dropped and that the handle
  sent several requests.
- **Unavailable prerequisite:** the failure starts with
  `unavailable prerequisite`: the cluster, the filesystem or the client does
  not start, or the client's `python3` cannot mount, prepare or list.
- **Condition not established:** the failure starts with
  `condition not established` and names the condition of the list above
  that did not hold, such as a trigger client that kept its dentry of the
  snapshot. It is not a reproduction and not a pass.
- **Control failed:** the failure starts with `control failed`: the client
  with the default cache did not list the snapshot exactly, or a live
  listing is wrong. The trigger's result then says nothing.
- **Another fault:** the failure starts with `another fault:`: one of the
  trigger's first 50 entries, which it read before it dropped the dentry,
  is not a name of the snapshot or repeats one, or its snapshot listing is
  wrong (a missing, repeated or unknown name) without any live name in it.
  It is not the defect of this case: a live name among the first 50 entries
  came before the window opened.

## Cleanup and limits

A last libcephfs mount removes the snapshot, the files and the directories
and checks that `/snapshot-readdir-eviction` is gone, before the client and
the cluster are terminated, also after a failure. A failed removal is
reported in addition to the test's own failure. Each listing client removes
its log file after reading it; a listing that fails before that leaves the
file in the client container, which is terminated with the test.

Not covered: the kernel client and `ceph-fuse`, arm64, `readdir_snapdiff`
(which rebuilds its path the same way), a snapshot directory in several
fragments, other operations on a snapshot inode whose dentries were trimmed
(getattr, lookup, open), and cache pressure from another source than
lookups, such as a memory limit.
