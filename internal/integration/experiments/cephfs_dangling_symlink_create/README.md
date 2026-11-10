# libcephfs creates through a dangling symlink at the new name

Owner: CephFS. Kind: upstream bug reproducer (`native_regression`).
Test: `TestCephFSCreateAtADanglingSymlink`, optional batch
`cephfs_dangling_symlink_create`. The judgement of the reports has
Docker-free tests in the same package (`code/pure`).

## Use case and native contract

A service makes a directory, a symlink, a hard link or a node through
libcephfs at a name it expects to be unused, and treats `EEXIST` as "someone
else has it". POSIX `mkdir`, `symlink`, `link` and `mknod` do not follow a
symlink in the last component of the new name: a name occupied by a symlink
is occupied, whether or not the symlink's target exists, and nothing is made
anywhere else.

The libcephfs client resolves the new name with its path walk
(`Client::path_walk`, `src/client/Client.cc`) and asks it to accept a
missing last component (`require_target = false`), but leaves the walk's
default of following a symlink in the last component (`followsym`) in
place: `Client::mkdirat`, `Client::_symlink` and `Client::_mknod` do so,
and `Client::_link` walks the new name with the defaults and treats
`ENOENT` as "free". When the name is a symlink whose target does not exist,
the walk follows it, ends at the missing target with the target's directory
as the parent, and the operation makes its entry there. The caller gets
success, the symlink is untouched, and an entry appears at a path the
caller never named. With a symlink whose target exists the walk ends at
that target and the operations report `EEXIST`, so only a dangling symlink
shows it. Opening with `O_CREAT|O_EXCL` disables the following
(`Client::create_and_open`) and is not part of the case.

Squid 19.2.3 and 19.2.5, Tentacle 20.2.4 and 20.2.5 and Umbrella 21.2.0
have these calls in the same form. Squid 19.2.2 and earlier resolve the
directory of the new name and look the last component up without following
it, and report `EEXIST`. Upstream reference:
[tracker 82376](https://tracker.ceph.com/issues/82376).

The test fails on Squid 19.2.3 and on every later release it ran on, and
passes on Squid 19.2.2.
The failure is the expected result for an unpatched defect; it is not a
fixture failure.

## Requirements

- One cluster with one filesystem (`cephfs.Run`) and its client toolkit,
  whose `python3` has the `cephfs` module of the same release. Image roles
  `essential`, `client` and `cephfs`, of one release.
- The client mounts as `client.admin` with its defaults. The kernel client
  and `ceph-fuse` are not used: the kernel resolves the last component
  itself before the filesystem is asked, which hides this path.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.

## Tier and triggers

Deliberately failing reproducer: one run takes about 35 seconds and fails on
the role images of all three release lines. No automatic run includes it (the default checks of
a pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=cephfs_dangling_symlink_create`.
- Comment: `/ci run cephfs_dangling_symlink_create` or
  `/ci run cephfs_dangling_symlink_create on umbrella`.

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

Tentacle 20.2.5 and the Squid patches before 19.2.5 have no published role
images. Run them on the upstream aggregate that
`.github/ceph-discovery-images.json` pins, with no role variable set
(`CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.2@sha256:...`).

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestCephFSCreateAtADanglingSymlink$' \
  ./internal/integration/experiments/cephfs_dangling_symlink_create
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch cephfs_dangling_symlink_create \
  --package ./internal/integration/experiments/cephfs_dangling_symlink_create \
  --directory "artifacts/cephfs-dangling-symlink-create-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

The test logs the client's `ceph --version` and the release its `cephfs`
module reports; record the image digests with the result.

## Observations and controls

There are eight cases: each of `mkdir`, `symlink`, `link` and `mknod` (a
regular file) with a relative and with an absolute symlink target, which
the path walk resolves on different branches. One libcephfs client makes a
directory per case below `/dangling-symlink-create`, so no operation sees
what another left:

| Name | What it is |
| --- | --- |
| `there` | an empty directory |
| `link` | a symlink to `there/absent`, which does not exist |
| `kept`, `occupied`, `source` | regular files; `source` is what `link` links |
| `live` | a symlink to `kept` |

A second mount acts. Per case it reads `link` with lstat and readlink and
`there/absent` with lstat, and then runs the operation four times, keeping
each errno: at the unused name `fresh`, at `occupied`, at `live` and at
`link`. A third mount, made after the second is gone, reads every name
with lstat and readlink and lists `there`. It holds no state of the acting
client, so what it reads is what the filesystem holds.

1. Conditions: the three steps ran as three client instances; before its
   operation each `link` is a symlink with the case's target, and
   `there/absent` does not exist.
2. Control, unused name: the operation succeeds at `fresh`, and `fresh` is
   what the operation makes: an entry of its type that, for `link`, has the
   inode of `source` and, for `symlink`, the text the operation was given
   (`made-by-the-case`). The client can create in the directory.
3. Control, regular file: the operation reports `EEXIST` at `occupied`,
   which stays a regular file.
4. Control, symlink to an existing file: the operation reports `EEXIST` at
   `live`; `live` and `kept` stay as they are. A symlink at the new name is
   not refused or mishandled as such.
5. Trigger: the operation at `link` must report `EEXIST`; `link` must be
   the same symlink (inode and target), `there/absent` must not exist, and
   `there` must be empty.

## Reading the result

The results below ran on the 20.2.4 role images (essential `3c7a6d0ae445…`, client `56b5c03d450c…`, cephfs `9566841eba0a…`), the
19.2.5 role images (essential `43ee23624e7a…`, client `a69e1319ce88…`, cephfs `99f0ff6e827c…`), the 21.2.0 role images
(essential `119180026952…`, client `e5f7edab88b6…`, cephfs `7ae447d4d414…`) and the upstream aggregates of 20.2.5
(`sha256:4337ac8ac81d…`), 19.2.3 (`sha256:af0c5903e901…`) and 19.2.2 (`sha256:8214ebff6133…`), each with the client module
of its own release.

Five releases failed with `observed defect:` in all eight cases, after
every control held: Tentacle 20.2.4 in 34 s, Tentacle 20.2.5 in 34 s,
Umbrella 21.2.0 in 35 s, Squid 19.2.5 in 40 s and Squid 19.2.3 in 41 s. In
each case the operation at `link` returned
success, `link` stayed the same symlink, and `there/absent` was a directory
(`mkdir`), a symlink (`symlink`), a hard link of `source` (`link`) or a
regular file (`mknod`). Squid 19.2.2 passed (41 s): all eight
operations reported `EEXIST` and made nothing. The behaviour changes
between 19.2.2 and 19.2.3.

- **Pass:** every operation reported `EEXIST` at the dangling symlink and
  made nothing. Squid 19.2.2 passes; no later release that was run does.
- **Observed bug:** the failure starts with `observed defect:` and names
  each case in which the operation succeeded and what it makes is at the
  symlink's target, with the symlink unchanged. As at `fresh`, a file there
  counts for `link` only with the inode of `source`, and a symlink for
  `symlink` only with the given text.
- **Not reproduced:** a pass on a build claimed to be affected. Check in
  the receipts (`CEPHFS_DANGLING_SYMLINK_CREATE_RECEIPT`) that every
  `link_before` is a symlink with its target and every `target_before` is
  empty.
- **Unavailable prerequisite:** the failure starts with
  `unavailable prerequisite`: the cluster, the filesystem or the client does
  not start, or the client's `python3` cannot mount, prepare, act or
  observe.
- **Condition not established:** the failure starts with
  `condition not established`: a symlink was not dangling before its
  operation, or the steps did not run as separate mounts. It is not a
  reproduction and not a pass.
- **Control failed:** the failure starts with `control failed` and names
  the control of the list above that did not hold. The result at the
  dangling symlink then says nothing.
- **Another fault:** the failure starts with `another fault:`: at a
  dangling symlink the result is neither `EEXIST` with nothing made nor
  success with what the operation makes at the target, such as another
  errno, a replaced symlink, an entry of another type, a file that is not a
  link of `source` or a symlink with another text. It is not
  the defect of this case.

## Cleanup and limits

A fourth libcephfs mount removes everything below
`/dangling-symlink-create`, including what an operation made at a symlink's
target, and checks that the directory is gone, before the client and the
cluster are terminated, also after a failure. A failed removal is reported
in addition to the test's own failure.

Not covered: the kernel client and `ceph-fuse`, arm64, `open` with
`O_CREAT`, `rename` onto a dangling symlink, `mknod` of device nodes and
FIFOs, a symlink whose target's directory does not exist, chains of
symlinks, a client without permission to create in the target's directory,
and snapshots.
