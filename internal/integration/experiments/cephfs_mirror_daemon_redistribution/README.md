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
daemons as untested. No upstream issue or fix is recorded for the
redistribution failure; the `cephfs/mirror` README documents the fixture's
workaround.

## Requirements

- Two clusters, one OSD each, one CephFS filesystem in each, a
  `cephfsmirror.Run` link with two daemons and four directories. Both
  clusters, their client containers and the mirror daemons run the package
  variable `image`, an aggregate image.
- The host variant needs a Linux engine or Docker Desktop with host
  networking.
- The scenario bounds itself to 15 minutes; the batch timeout is 60 minutes
  for both variants.

## Tier and triggers

Standalone reproducer with a bridge and a host-network variant. The test
fails only when its setup or a control does not hold; the documented
symptom does not fail it. Each variant prints one `EXPERIMENT_RESULT` line,
`case=cephfs_mirror_daemon_redistribution/bridge` or `/host`, `REPRODUCED`
on its default image, Tentacle 20.2.4. No automatic run includes it (the
default checks of a pull request, pushes to `main`, periodic samples), and
it never gates anything. A maintainer can run it on demand, also on a pull
request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=native_shuffle`.
- Comment: `/ci run native_shuffle`.

Every container the test starts uses `image`, so the role images that a
batch exports do not reach it.

## Running it

On the default image, which reproduces the defect:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterCephFSMirrorDaemonTopology$' \
  ./internal/integration/experiments/cephfs_mirror_daemon_redistribution
```

No release is fixed. Another image runs the same way, for example Umbrella
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
   `AddDaemon`. The MGR must list the replacement with the peer (a new
   daemon ID in `fs snapshot mirror daemon status`), spread the directories
   over both daemons within three minutes (`fs snapshot mirror show
   distribution`), and the next snapshots must arrive.

The fixture scenario continues with a zero-daemon outage and a peer-link
interruption; they are not part of this defect and this case stops after
step 3.

## Reading the result

- **REPRODUCED:** steps 1 and 2 held, the replacement registered, and the
  distribution still names one instance three minutes later. The symptoms
  give that distribution and up to three MGR log lines with
  `'DirectoryState' object is not subscriptable`.
- **NOT_REPRODUCED:** the MGR spread the directories over both daemons, and
  the snapshots taken after it arrived with exact bytes.
- **Failure, unavailable prerequisite:** a cluster, a filesystem or the
  mirror link does not start; the failure names the setup step.
- **Failure, condition not established:** step 1 or 2 fails, or the
  replacement never registers with the peer. Step 3 then says nothing about
  redistribution.
- **Failure, another fault:** the directories were spread, but the native
  map disagrees with the mirror's status or the next snapshots do not
  arrive.

Native results, Linux amd64 host, one run per image and variant:

- Tentacle 20.2.4 aggregate `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded): `REPRODUCED` on bridge (357 s) and host (351 s). The
  replacement registered, the distribution stayed at 4 directories on the
  survivor and 0 on the replacement, and the MGR logged the
  `DirectoryState` error.
- Tentacle 20.2.5 `quay.io/ceph/ceph:v20.2.5@sha256:4337ac8ac81d…`, bridge:
  `REPRODUCED` (372 s), the same distribution and MGR error.
- Umbrella 21.2.0 `quay.io/ceph/ceph:v21.2.0@sha256:c9e6ed4ac3fc…`, bridge:
  `REPRODUCED` (379 s), the same distribution and MGR error.

## Cleanup and limits

The mirror link, its daemons, the client containers and both clusters are
terminated by their cleanups; peers and directory policies stay in the
discarded clusters. The MGR hang on a directory removed during a reassignment is a different defect
and is not exercised here.
