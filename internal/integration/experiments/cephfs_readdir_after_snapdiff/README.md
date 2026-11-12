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

Unpatched-defect reproducer: it reproduces on every release it was run
on. It passes on the 20.2.4 role images, which its catalogue expects to
reproduce, and fails on images the catalogue does not list.
No automatic run includes it (the default checks of a pull request, pushes
to `main`, periodic samples), and it never gates anything. A maintainer can
run it on demand, also on a pull request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_readdir_after_snapdiff`.
- Comment: `/ci run cephfs_readdir_after_snapdiff` or
  `/ci run cephfs_readdir_after_snapdiff on umbrella`.

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

Releases without published role images run on the upstream aggregate that
`.github/ceph-discovery-images.json` pins, with no role variable set
(`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v20.2.5@sha256:...`).

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSListingAfterSnapdiff$' \
  ./internal/integration/experiments/cephfs_readdir_after_snapdiff
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch cephfs_readdir_after_snapdiff \
  --package ./internal/integration/experiments/cephfs_readdir_after_snapdiff \
  --directory "artifacts/cephfs-readdir-after-snapdiff-$(date -u +%Y%m%dT%H%M%SZ)"
```

The test logs the client's `ceph --version`, the release its `cephfs`
module reports and whether the module has `opensnapdiff`; record the image
digests with the result. One `CEPHFS_READDIR_AFTER_SNAPDIFF_RECEIPT` line
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
   two names. A missing name, a repeated one or a foreign one is the
   symptom.
4. Control: the warm mount's second listing of `control` holds exactly its
   two names. If it does not, the result is `another fault`, whatever the
   trigger did: the damage is then not confined to names of one hash.

## Reading the result

The results below ran on the pinned role images of 20.2.4 (essential
`3c7a6d0ae445…`), 21.2.0 (essential `119180026952…`) and 19.2.5 (essential
`43ee23624e7a…`) with the `client` and `cephfs` roles of the same releases,
and on the official images of 20.2.5 (`sha256:4337ac8ac81d…`) and 19.2.2
(`sha256:8214ebff6133…`) as aggregates. Linux amd64, bridge, one run per
release.

The warm mount's first listing of `trigger` holds both names at
hash-ordered positions of one hash (`7f4abf`, counts 3 and 4, raw
`0xff7f4abf0000003` and `0xff7f4abf0000004`); the snapdiff names
`collision_2690` alone; the second listing holds `collision_2379` only. The
fresh mount lists both names, and `control`, whose names have the hashes
`6769c5` and `f51016`, is listed in full after its snapdiff. Squid 19.2.2
(43 s) and 19.2.5 (44 s), Tentacle 20.2.4 (38 s) and 20.2.5 (38 s) and
Umbrella 21.2.0 (38 s) all do this: every tested build reproduces the
defect.

## Verdict

The test is the reproduction unit `cephfs_readdir_after_snapdiff` with one
planned observation, `listing-after-snapdiff`. It prints one line that
starts with `REPRODUCTION_VERDICT case=cephfs_readdir_after_snapdiff`.

- `REPRODUCED`: the conditions held, the control's second listing is its two
  names, and the second listing of `trigger` is not exactly its two names.
  Without an expectation the test fails with
  `observed defect: listing-after-snapdiff:`.
- `NOT_REPRODUCED`: the conditions held and both second listings hold their
  two names. Without an expectation the test passes. On a build claimed to
  be affected, check the receipt line: the two names must share a hash and
  the snapdiff must name the second one.
- `INCONCLUSIVE`: no verdict on the symptom; the test fails and the reason
  starts with its category. `unavailable prerequisite:` the cluster or the
  filesystem does not start, or a client run fails, for example because the
  client's `cephfs` module has no `opensnapdiff`. `condition not
  established:` a condition of the list above does not hold. `another
  fault:` the control's second listing is not its two names.

The expected verdict comes from [`reproductions.json`](reproductions.json)
beside the test. The combination whose platform and images equal the ones
the run resolves selects it, and only that verdict passes; a skip or
`INCONCLUSIVE` never does. It lists `20.2.4-roles` (linux/amd64,
`REPRODUCED`). On any other images the run is an investigation
(`expected=none combination=none`) and keeps the meaning above. With the
20.2.4 role images exported as above, the runner checks the listed
combination:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch cephfs_readdir_after_snapdiff \
  --package ./internal/integration/experiments/cephfs_readdir_after_snapdiff \
  --directory "artifacts/cephfs-readdir-after-snapdiff-$(date -u +%Y%m%dT%H%M%SZ)"
```

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
