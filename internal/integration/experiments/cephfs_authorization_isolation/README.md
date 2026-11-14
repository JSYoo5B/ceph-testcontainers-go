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

Each subtest is one defect and prints its own `EXPERIMENT_RESULT` line,
`case=cephfs_authorization_isolation/<defect>` with the defect
`caps-per-filesystem`, `snapshot-of-a-deleted-directory` or
`same-named-subvolumes`. The default image, Squid 19.2.2, is affected by all
three; Squid 19.2.5, Tentacle 20.2.4 and Umbrella 21.2.0 by none.

## Requirements

- One cluster: 1 MON, 1 MGR, 1 OSD on a 1 GiB block, filesystems `iso-a` (one
  MDS) and `iso-b` (an active and a standby-replay MDS). The cluster and
  every probe client run the package variable `image`, an aggregate image
  with Python libcephfs and librados; clients are fresh containers of it.
- The test bounds itself to 25 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Standalone reproducer. The test fails only when its setup or a control does
not hold; a documented symptom does not fail it. On the default image every
line says `REPRODUCED`.

- Pull request: only when a file of this directory changes, or with the
  `ci:full` label (a required `topology` batch).
- Comment: `/ci run cephfs_authorization_isolation [on squid|umbrella]`.
- Periodic: `rotate` can draw it.

Every container the test starts uses `image`, so the role images that a
batch exports do not reach it: a batch run reports on the default image.

## Running it

On the default image, which reproduces all three defects:

```sh
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=30m \
  -run '^TestCephFSAuthorizationIsolation$' \
  ./internal/integration/experiments/cephfs_authorization_isolation
```

On a fixed image, such as Tentacle 20.2.4, every line says `NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_authorization_isolation_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all" \
  -run '^TestCephFSAuthorizationIsolation$' \
  ./internal/integration/experiments/cephfs_authorization_isolation
```

Squid 19.2.3 (fixed snapshot read) and 19.2.4 (also fixed subvolume
namespaces) separate the three defects; their aggregates are the
`official-19.2.3-all` and `official-19.2.4-all` tags. Every probe line
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

- **REPRODUCED** (per defect): a create or mkdir succeeds in the read-only
  filesystem (caps); the snapshot read fails after the deletion (snapshot);
  the two subvolumes share one namespace, a reader's OSD caps name the other
  namespace, or a reader reads the other's object (subvolumes). The
  `symptoms` field names each.
- **NOT_REPRODUCED** (per defect): the contract held.
- **Failure, unavailable prerequisite:** a filesystem, MDS or client
  container does not start; the failure names the setup step. No line is
  printed.
- **Failure, condition or control:** a seed, a cap update or a subvolume
  creation fails, the writable filesystem refuses a create, a principal
  without an `iso-b` clause mounts it, `/` or `/dir2` mounts for the `/dir1`
  principal, the live child or the snapshot read fails before the deletion,
  or a reader cannot read its own data. The subtest never probed its
  contract, and no line is printed.

Native results, Linux amd64 host, bridge networking, one run per image:

- Squid 19.2.2 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:bc62130f013a…` (the default image when recorded), 112 s: all three `REPRODUCED`. Caps: in all four clause
  orders the create and the mkdir succeeded in the read-only filesystem.
  Snapshot: after the deletion the snapshot read and stat returned `EACCES`.
  Subvolumes: both got the namespace `fsvolumens_shared-name`, and each
  reader read the other's object.
- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…`,
  105 s: all three `NOT_REPRODUCED`.

## Cleanup and limits

Each subtest deletes its principals, subvolumes and snapshot; the harness
terminates the cluster. Mixed client and MDS builds, kernel clients and
arm64 were not run.
