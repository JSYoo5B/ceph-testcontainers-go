# CephFS mirror directory redistribution when a daemon joins

Owner: CephFS mirroring. Kind: upstream bug reproducer (`native_regression`).
Tests: `TestMultiClusterCephFSMirrorDaemonTopology` (bridge) and
`TestHostNetworkCephFSMirrorDaemonTopology` (host), optional batch
`native_shuffle`. The required topology batches
`topology_extensions_cephfs_mirror_bridge` and
`topology_extensions_cephfs_mirror_host` in `internal/integration/cephfs` run
the fixture's own scenario, `harness.RunCephFSMirrorDaemonTopology`, with
`RebalanceDirectories` after `AddDaemon`. This case keeps its own copy of the
scenario's first three steps, without the fixture's help.

## Use case and native contract

Several `cephfs-mirror` daemons share one peer. The MGR `mirroring` module
assigns the mirrored directories to the daemons and must reassign them when a
daemon stops, is removed or joins. This case leaves every reassignment to the
MGR: it never calls `RebalanceDirectories`.

The defect: on Tentacle 20.2.4, when another daemon joins, the module fails to
redistribute directories that are already assigned (`'DirectoryState' object
is not subscriptable` in the MGR log). Failover to a surviving daemon still
works. Tentacle's documentation also marks deployments with several mirror
daemons as untested. The [Ceph main fix](https://github.com/ceph/ceph/pull/70942)
corrects the state access and schedules redistribution after the 300-second
throttle expires. The tested supported-series release images still reproduce
the defect. [Squid 19.2.5 source](https://github.com/ceph/ceph/blob/v19.2.5/src/pybind/mgr/mirroring/fs/dir_map/policy.py)
also contains the faulty access. The native results below cover Tentacle and
Umbrella. The `cephfs/mirror` README documents the fixture's workaround.

## Requirements

- Two clusters, one OSD each, one CephFS filesystem in each, a
  `cephfsmirror.Run` link with two daemons and four directories. Both
  clusters, their client containers and the mirror daemons run the package
  variable `image`, an aggregate image.
- The host variant needs a Linux engine or Docker Desktop with host
  networking.
- Each variant's steps, the start of both clusters included, share a 15-minute
  deadline. The read of the MGR logs has its own 20-second deadline, and in
  cleanup the mirror's termination has its own two-minute deadline and the
  clusters and clients are terminated without one, so a slow log read or
  cleanup can take a variant past 15 minutes. The outer bound is the batch's
  60-minute `go test` timeout, which both variants share.

## Tier and triggers

Known-defect reproducer with a bridge and a host-network variant, optional
batch `native_shuffle` with a 60-minute budget; no automatic run includes it.
Each variant prints its own result,
`case=cephfs_mirror_daemon_redistribution/bridge` or `/host`. On its default
image, Tentacle 20.2.4, both print `REPRODUCED` (357 s bridge, 351 s host),
and the bridge variant does on 20.2.5 and 21.2.0. The `Experiments` dispatch
runs the default image. The comment run is
`/ci run native_shuffle`. The shared triggers, result checks and image
injection are in [Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterCephFSMirrorDaemonTopology$' \
  ./internal/integration/experiments/cephfs_mirror_daemon_redistribution
```

To run another supported-series release image, such as the affected Umbrella
21.2.0:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_mirror_daemon_redistribution_test.image=quay.io/ceph/ceph:v21.2.0" \
  -run '^TestMultiClusterCephFSMirrorDaemonTopology$' \
  ./internal/integration/experiments/cephfs_mirror_daemon_redistribution
```

`TestHostNetworkCephFSMirrorDaemonTopology` runs the host-network variant.
The `CEPHFS_MIRROR_DAEMON_REDISTRIBUTION_RECEIPT` line gives the MGR's
distribution after the replacement joined.

## Observations and controls

1. Control: the initial snapshots of the four directories arrive with exact
   bytes, and the admin socket's assignments agree with the MGR map.
2. Failover control: one daemon is stopped; all four directories move to the
   survivor and new snapshots arrive.
3. Trigger: the stopped daemon is removed and a replacement joins with
   `AddDaemon`. The MGR has up to three minutes to list a new daemon ID
   with the peer alongside the survivor in `fs snapshot mirror daemon
   status`. Each owned daemon's admin-socket `rados_inst` must also match
   a watcher on the filesystem's metadata pool. The distribution window
   begins once both sets are established. Each distribution read must
   account for all four directories across the watcher IDs. The service
   registrations and watcher membership must both survive the reads that
   bracket the distribution sample.

The fixture scenario continues with a zero-daemon outage and a peer-link
interruption; they are not part of this defect and this case stops after
step 3.

## Reading the result

- **REPRODUCED:** after the replacement registered, a valid distribution
  still assigns all four directories to the survivor at the end of the
  three-minute window. The MGR log may provide up to three diagnostic lines
  with `'DirectoryState' object is not subscriptable`.
- **NOT_REPRODUCED:** the MGR assigned directories to both daemons during
  the window, and the next snapshots arrived with exact bytes.
- **Failure, unavailable prerequisite:** a cluster, a filesystem or the
  mirror link does not start; the failure names the setup step.
- **Failure, condition not established:** step 1 or 2 fails, or the
  replacement never registers with the peer. Step 3 then says nothing about
  redistribution.
- **Failure, unavailable observation:** the registered pair disappears or
  changes, or the distribution omits an instance, fails to account for all
  directories or contains an unreadable count.
- **Failure, another fault:** the replacement gains every directory, the
  native map disagrees with the mirror's status, or the next snapshots do
  not arrive.

Native results, Linux amd64 host, one run per image and variant:

- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all` (the default image when recorded): `REPRODUCED` on bridge (357 s) and host (351 s). The
  replacement registered, the distribution stayed at 4 directories on the
  survivor and 0 on the replacement, and the MGR logged the
  `DirectoryState` error.
- Tentacle 20.2.5 `quay.io/ceph/ceph:v20.2.5`, bridge:
  `REPRODUCED` (372 s), the same distribution and MGR error.
- Umbrella 21.2.0 `quay.io/ceph/ceph:v21.2.0`, bridge:
  `REPRODUCED` (379 s), the same distribution and MGR error.

### Historical control before the defect

Pacific 16.2.6 predates the faulty throttle access introduced in Pacific
16.2.7 and is outside the module's [supported releases](../../../../docs/guides/releases-and-images.md#supported-releases).
A temporary checkout of `50ff53d83c14e69e48f38bcf4a96ba41f23dac12` ran
the upstream Pacific aggregate `quay.io/ceph/ceph:v16.2.6` on Linux amd64
with Docker 29.9.1, using bridge networking. The run pinned the image by
digest, recorded in the documenting pull request.

The initial run reached filesystem setup, where Pacific rejected
`fs set ... refuse_standby_for_another_fs true`. With that setting omitted,
both filesystems and both mirror daemons started. Registration polling then
failed because Pacific's `fs snapshot mirror daemon status` requires a
filesystem name.

The successful run applied two compatibility adjustments to that temporary
checkout:

- Omitted `refuse_standby_for_another_fs` during setup. Each test cluster
  has one filesystem, one active MDS and no standby.
- Passed the filesystem name to the existing daemon-status reads for mirror
  setup, assignment observation and replacement membership.

The existing membership, distribution and payload checks all ran. The initial
distribution was 2/2. Stopping one daemon moved all four directories to the
survivor. After the replacement registered, redistribution restored a 2/2
split and all four subsequent snapshots arrived with exact payload bytes.
The test passed in 231.36 seconds and emitted `NOT_REPRODUCED`. The
temporary adjustments were discarded after the run.

The Ceph main fix retains the 300-second throttle. A comparison against a
build containing that fix needs to observe redistribution after the
throttle expires. The current three-minute window can end earlier.

## Cleanup and limits

The mirror link, its daemons, the client containers and both clusters are
terminated by their cleanups; peers and directory policies stay in the
discarded clusters. The MGR hang on a directory removed during a reassignment is a different defect
and is not exercised here.
