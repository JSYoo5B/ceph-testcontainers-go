# libcephfs listing of a snapshot after a snapdiff on the same mount

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSListingAfterSnapdiff`, optional batch
`cephfs_readdir_after_snapdiff`. Pure tests of the judgement and of the
position reading (`TestJudgeAfterSnapdiff` and `TestDirEntryHash`, batch
`pure`).

## Use case and native contract

A backup or replication tool lists a snapshot through libcephfs, asks the
same mount which entries changed since the previous snapshot (a snapdiff),
and lists the snapshot again, for example to verify what it copied. A
listing of a snapshot must hold exactly the snapshot's names, each once,
whatever the mount did before.

The trigger is a directory with two names of one dentry hash, of which the
second changed between the snapshots. The mechanism below is read from the
client source of 20.2.4, not traced in a run; the runs show the layout and
the loss. The client keeps a listing it has
read in full as an ordered cache and resumes a later listing from the
position of the last entry returned. A position is the name's dentry hash
and a count of the entries of that hash; the two names get counts one
apart. A snapdiff reply holds only the changed entries, and the client
numbers them as if they were a whole listing, so the changed entry, alone
of its hash in the reply, gets the first count of the hash and the cached
entry's position is overwritten with it. The next listing from the cache
returns the first name, looks for an entry after its position and finds
none: the changed name is missing, and nothing reports an error
([tracker 82534](https://tracker.ceph.com/issues/82534)). The report used
the snapdiff variant with an attribute mask, which no supported release's
client has; the snapdiff without a mask, which they all have, takes the
same path.

`/readdir-after-snapdiff` holds two directories. Each has the snapshots
`s1` and `s2` and a subdirectory `sub` with two empty files; the second
file is written to between the snapshots.

| Directory | Files of `sub` | Role |
| --- | --- | --- |
| `trigger` | `collision_2379`, `collision_2690` (one dentry hash under the default hash) | trigger |
| `control` | `plain_first`, `plain_second` (two hashes) | control |

## Requirements

- One cluster with one filesystem (`harness.CEPHFSRun`) and its client
  container, whose `python3-cephfs` provides `LibCephFS.opensnapdiff`.
  Image roles `essential`, `client` and `cephfs`, all of one release, or one
  aggregate image (`CEPH_TEST_IMAGE`). Bridge networking.
- The default directory hash (rjenkins) and unfragmented directories, as a
  new filesystem has them.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Standalone reproducer. The test fails only when its setup, a condition or a
control does not hold; the documented symptom does not fail it. It prints
one `EXPERIMENT_RESULT` line instead, `REPRODUCED` on its default image,
Tentacle 20.2.4. No automatic run includes it (the default checks of a pull
request, pushes to `main`, periodic samples), and it never gates anything. A
maintainer can run it on demand, also on a pull request's head with the
comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_readdir_after_snapdiff`.
- Comment: `/ci run cephfs_readdir_after_snapdiff` or
  `/ci run cephfs_readdir_after_snapdiff on umbrella`.

Every container the test starts uses the package variable `image`, so the
role images that a batch exports do not reach it.

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSListingAfterSnapdiff$' \
  ./internal/integration/experiments/cephfs_readdir_after_snapdiff
```

No release is fixed. Another image runs the same way, for example Umbrella 21.2.0:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_readdir_after_snapdiff_test.image=quay.io/ceph/ceph:v21.2.0" \
  -run '^TestCephFSListingAfterSnapdiff$' \
  ./internal/integration/experiments/cephfs_readdir_after_snapdiff
```

The test logs the client's `ceph --version`, the release its `cephfs`
module reports and whether the module has `opensnapdiff`. One `CEPHFS_READDIR_AFTER_SNAPDIFF_RECEIPT` line
per directory gives every listing with its positions
(`name->hash+count (raw 0x…)`, or `name->frag …` for a position without the
hash-order marker) and the snapdiff's entries.

## Observations and controls

One libcephfs client prepares the two directories. A second client, the
warm mount, does for each directory: list `<dir>/.snap/s2/sub` with
single-entry `readdir` calls, read the snapdiff of `sub` between `s1` and
`s2` to its end, and list `<dir>/.snap/s2/sub` again through a new handle.
A third client, a fresh mount, lists both snapshots once.

1. Condition, per directory: the warm mount's first listing and the fresh
   mount's listing are the two names, each once, and the snapdiff names the
   changed file and nothing else. Otherwise `condition not established`.
2. Condition, the layout: the positions of both first listings carry the
   client's marker of a hash-ordered listing (all eight bits above the 24
   of the hash are set). Without it the same bits name the fragment the
   entry was read from, and two entries of one fragment would look like a
   shared hash. With it, the two names of `trigger` have the same hash and
   the changed one is the second of them, and the two names of `control`
   have different hashes. The test reads all of this from the positions
   the client reports (`d_off`) and computes no hash itself; the receipt
   prints every position decoded and raw.
3. Oracle: the warm mount's second listing of `trigger` holds exactly the
   two names. A missing name, a repeated one or a foreign one is
   `observed defect`.
4. Control: the warm mount's second listing of `control` holds exactly its
   two names. If it does not, the result is `another fault`, whatever the
   trigger did: the damage is then not confined to names of one hash.

## Reading the result

Standalone runs, Linux amd64 host, bridge networking, one run per image:

- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded): `REPRODUCED` in 38 s; the second listing of `trigger`
  held `collision_2379` only, and `control` was listed in full.

The release comparison ran on the pinned role images of 20.2.4 (essential
`3c7a6d0ae445…`), 21.2.0 (essential `119180026952…`) and 19.2.5 (essential
`43ee23624e7a…`) with the `client` and `cephfs` roles of the same releases,
and on the official images of 20.2.5 (`sha256:4337ac8ac81d…`) and 19.2.2
(`sha256:8214ebff6133…`) as aggregates. Linux amd64, bridge, one run per
release.

- **REPRODUCED:** the symptom starts with `after a snapdiff that named
  only collision_2690`. In the release comparison the warm mount's first listing of `trigger` holds both
  names at hash-ordered positions of one hash (`7f4abf`, counts 3 and 4,
  raw `0xff7f4abf0000003` and `0xff7f4abf0000004`); the snapdiff
  names `collision_2690` alone; the second listing holds `collision_2379`
  only. The fresh mount lists both names, and `control`, whose names have
  the hashes `6769c5` and `f51016`, is listed in full after its snapdiff.
  Squid 19.2.2 (43 s) and 19.2.5 (44 s), Tentacle 20.2.4 (38 s) and 20.2.5
  (38 s) and Umbrella 21.2.0 (38 s) all do this.
- **NOT_REPRODUCED:** every listing holds its two names. No tested build
  does this.
- **Failure, another fault:** the control's second listing is not its two
  names.
- **Failure, condition not established:** a condition of the list above
  did not hold; no line is printed.
- **NOT_REPRODUCED on a build claimed to be affected:** check the
  receipt line: the two names must share a hash and the snapdiff must name
  the second one.
- **Failure, unavailable prerequisite:** the cluster or the filesystem does not
  start, or a client run fails, for example because the client's `cephfs`
  module has no `opensnapdiff`.

## Cleanup and limits

The cleanup is registered before anything is created. It removes both
snapshots of each directory, the files, the directories and the root with a
client of its own, and fails the test if anything is left. The cluster and
the client container are removed by their own cleanups.

The case shows the loss for one pair of names and one changed entry. Not
run: more than two names of one hash, a changed first name, fragmented
directories, a directory larger than one readdir reply, the kernel client
and `ceph-fuse`, a snapdiff followed by a listing of the live directory or
of the first snapshot, arm64 and the host network profile. The snapdiff
variant with an attribute mask does not exist in the clients of the
releases above and was not run.
