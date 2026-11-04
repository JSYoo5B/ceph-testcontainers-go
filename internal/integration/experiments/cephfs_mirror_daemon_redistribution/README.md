# CephFS mirror directory redistribution when a daemon joins

Owner: CephFS mirroring. Kind: upstream bug reproducer (`native_regression`).
Tests: `TestMultiClusterCephFSMirrorDaemonTopology` (bridge) and
`TestHostNetworkCephFSMirrorDaemonTopology` (host), optional batch
`native_shuffle`. The required topology batches
`topology_extensions_cephfs_mirror_bridge` and
`topology_extensions_cephfs_mirror_host` in `internal/integration/cephfs` run
the same scenario, `harness.RunCephFSMirrorDaemonTopology`, with the fixture's
`RebalanceDirectories` after `AddDaemon`.

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
  `cephfsmirror.Run` link with two daemons and four directories. Image roles
  `essential`, `client` and `cephfs`, all of one release.
- The host variant needs a Linux engine or Docker Desktop with host
  networking.
- The scenario bounds itself to 15 minutes; the batch timeout is 60 minutes
  for both variants.

## Tier and triggers

Deliberately failing reproducer on 20.2.4, with a bridge and a host-network
variant (NATIVE_SHUFFLE_README). It never runs on a pull request, on `main` or
in periodic samples, and never gates anything.

- Manual: `gh workflow run "Native regressions" -f batch=native_shuffle`.
- Comment: `/ci run native_shuffle`.

## Running it

```sh
CEPH_TEST_IMAGE=<aggregate image@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterCephFSMirrorDaemonTopology$' \
  ./internal/integration/experiments/cephfs_mirror_daemon_redistribution
```

or exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch native_shuffle \
  --package ./internal/integration/experiments/cephfs_mirror_daemon_redistribution \
  --directory artifacts/native-shuffle
```

Record the image digest with the result.

## Observations and controls

1. Control: the initial snapshots of the four directories arrive with exact
   bytes, and the admin socket's assignments agree with the MGR map.
2. Failover control: one daemon is stopped; all four directories move to the
   survivor and new snapshots arrive.
3. Trigger: the stopped daemon is removed and a replacement joins with
   `AddDaemon`. The MGR must spread the directories over both daemons again,
   and the next snapshots must arrive.
4. The remaining steps remove every daemon, check that no snapshot arrives
   while none runs, add one daemon and require the backlog and the initial
   snapshot bytes to arrive.

## Reading the result

- **Pass:** the MGR redistributed the directories at step 3 without the
  fixture's help.
- **Observed bug:** steps 1 and 2 pass and step 3 times out waiting for a
  two-daemon assignment, with the MGR log naming the `DirectoryState` error
  (20.2.4).
- **Not reproduced:** a pass on a build claimed to be affected.
- **Unavailable prerequisite:** the `cephfs` role, the second cluster or the
  mirror link does not start; the failure names the setup step.
- **Condition not established:** steps 1 or 2 fail. Then step 3 never ran
  and the result says nothing about redistribution.

## Cleanup and limits

The mirror link, its daemons and both clusters are terminated by their
cleanups; peers and directory policies stay in the discarded clusters. The
MGR hang on a directory removed during a reassignment is a different defect
and is not exercised here.
