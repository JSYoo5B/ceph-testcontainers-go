# CephFS authorization and data isolation between filesystems and subvolumes

Owner: CephFS. Kind: cross-build characterization of fixed native defects.
Test: `TestCephFSAuthorizationIsolation`, batch `topology/cephfs_authorization_isolation`.
The fixture's own regression from the same qualification, survivor affinity
after `Filesystem.Remove`, stays in `internal/integration/cephfs` as
`TestCephFSRemovalKeepsSurvivorAffinity`.

## Use case and native contract

A SUT that gives each tenant its own CephFS filesystem, path or subvolume
relies on three native checks. Each subtest uses fixture-created principals
(`ceph.Container.CreateClient`, `UpdateCaps`, `subvolume.Authorize`) and
probes with a fresh libcephfs or librados client.

| Subtest | Native contract | Builds that break it | Upstream |
| --- | --- | --- | --- |
| `caps per filesystem` | MDS caps `allow r fsname=a, allow rw fsname=b` grant each filesystem only its own clause, in either order; a principal without a clause for a filesystem cannot mount it. | Squid before 19.2.5: any `rw` clause grants writes on every filesystem the principal can mount. | [tracker 72167](https://tracker.ceph.com/issues/72167), [ceph/ceph#64550](https://github.com/ceph/ceph/pull/64550) |
| `snapshot of a deleted directory` | A principal restricted to `/dir1` that deletes a child still reads the child's bytes through an earlier `/dir1` snapshot, and is refused `/` and `/dir2`. | Squid before 19.2.3: the snapshot read is refused once the live directory is unlinked. | [tracker 66828](https://tracker.ceph.com/issues/66828), [ceph/ceph#58419](https://github.com/ceph/ceph/pull/58419), Squid [ceph/ceph#59518](https://github.com/ceph/ceph/pull/59518) |
| `same-named subvolumes` | Namespace-isolated subvolumes with one name in two groups get two RADOS namespaces; each reader's OSD caps name only its own, and it is refused the other's object. | Squid before 19.2.4: both share one namespace, so each reader reads the other's data through librados. | [tracker 70668](https://tracker.ceph.com/issues/70668), [ceph/ceph#62668](https://github.com/ceph/ceph/pull/62668), Squid [ceph/ceph#64205](https://github.com/ceph/ceph/pull/64205) |

Every supported release (19.2.5, 20.2.4, 21.2.0) is fixed, so the test passes
there. It fails on the affected builds; that failure is the documented defect,
not a fixture fault.

## Requirements

- One cluster: 1 MON, 1 MGR, 1 OSD on a 1 GiB block, filesystems `iso-a` (one
  MDS) and `iso-b` (an active and a standby-replay MDS). Image roles
  `essential`, `client` and `cephfs`, all of one release; clients are fresh
  containers of the client role with Python libcephfs and librados.
- The test bounds itself to 25 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Cheap and deterministic on the supported releases: one run takes about
105 seconds on 20.2.4.

- Pull request: only when a file of this directory changes, or with the
  `ci:full` label (a required `topology` batch).
- Comment: `/ci run cephfs_authorization_isolation [on squid|umbrella]`.
- Periodic: `rotate` can draw it on the supported releases.

## Running it

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.5 \
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=30m \
  -run '^TestCephFSAuthorizationIsolation$' \
  ./internal/integration/experiments/cephfs_authorization_isolation
```

or exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py run --category topology \
  --batch cephfs_authorization_isolation \
  --package ./internal/integration/experiments/cephfs_authorization_isolation \
  --directory "artifacts/cephfs-authorization-isolation-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Pin the image by digest when the result is evidence; every probe line
(`native probe: {...}`) is logged.

## Observations and controls

- Caps: the native caps read back after each `UpdateCaps` equal the requested
  string; both OSD data pools are writable for every variant, so a refused
  create is the MDS's. The seed read on both filesystems is the positive
  control; the principal without an `iso-b` clause is the negative control.
- Snapshot: the live child reads before the deletion and is `ENOENT` after;
  mounts of `/` and `/dir2` are refused in both phases.
- Subvolumes: the two namespaces differ and share one data pool; each reader
  reads its own file through libcephfs and its own object through librados.

## Reading the result

- **Pass:** all three contracts hold on the build.
- **Observed bug:** a create or mkdir succeeds in the read-only filesystem
  (caps), the snapshot read is refused after the deletion (snapshot), or the
  namespaces are equal or the other object reads (subvolumes).
- **Not reproduced:** a pass on a build listed as affected.
- **Unavailable prerequisite:** a filesystem, MDS or client container does not
  start; the failure names the setup step.
- **Condition not established:** a seed, a cap update or a subvolume creation
  fails, so the subtest never probed the contract.

## Cleanup and limits

Each subtest deletes its principals, subvolumes and snapshot; the harness
terminates the cluster. Mixed client and MDS builds, kernel clients and
arm64 were not run.
