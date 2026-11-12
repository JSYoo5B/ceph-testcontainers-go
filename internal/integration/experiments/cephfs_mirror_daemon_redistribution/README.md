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

Deliberately failing reproducer: it reproduces on 20.2.4, so without an
expectation it fails there. It has a bridge and a host-network variant
(about 6 minutes for the bridge variant on 20.2.4). No automatic run
includes it (the default checks of a pull request, pushes to `main`, periodic
samples), and it never gates anything. A maintainer can run it on demand, also
on a pull request's head with the comment below.

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
  --directory "artifacts/native-shuffle-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Record the image digest with the result.

## Observations and controls

The judgement of the native reading after step 3 has Docker-free tests in
the same package (`code/pure`).

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

## Verdict

The bridge test is the reproduction unit
`cephfs_mirror_daemon_redistribution` and the host-network test the unit
`cephfs_mirror_daemon_redistribution/host-network`, each with one planned
observation, `redistribution-after-join`: the native assignments after the
replacement joins at step 3. The scenario reads the MGR distribution, every
directory's map and the daemon status every second for up to three minutes,
and each test prints one line that starts with
`REPRODUCTION_VERDICT case=<unit>`.

- `REPRODUCED`: steps 1 and 2 held, two services are registered with the
  shared peer, and at the end of the wait all four directories are still
  mapped to the surviving instance, the only one with directories (20.2.4).
  The MGR's own log names the `DirectoryState` error; the test prints the
  daemons' logs and the final native topology. The scenario stops there.
  Without an expectation the test fails with
  `observed defect: redistribution-after-join:`.
- `NOT_REPRODUCED`: the MGR spread the directories over both instances,
  each with a positive count, without the fixture's help. Step 3's snapshots
  and step 4 then run, and a failure there fails the test independently of
  the verdict.
- `INCONCLUSIVE`: no verdict on the symptom; the test fails and the reason
  starts with its category. `unavailable prerequisite:` the `cephfs` role,
  the second cluster or the mirror link does not start.
  `condition not established:` the two-daemon inventory, the shared peer,
  the stop, the removal or the `AddDaemon` of the replacement did not happen
  as planned, or the replacement did not register with the shared peer.
  `control failed:` step 1 or 2 did not hold, so step 3 says nothing about
  redistribution. `another fault:` a directory is not mapped to a current
  instance, or the directories moved some other way, such as all of them to
  the replacement. A step 1 or 2 wait that times out (the shared waits for
  assignments and snapshots) reports that the test ended before the planned
  observation.

An expectation turns the verdict into the result: only the expected verdict
passes, and a skip or `INCONCLUSIVE` never does. The batch runs both
variants, so an expectation for one unit leaves the other's verdict printed
but does not let its symptom fail the run. With the 20.2.4 role images
(`essential`, `client`, `cephfs`) exported as in
[Choosing images](../../../../docs/contributing/testing.md#choosing-images):

```sh
CEPH_TEST_EXPECTED_VERDICT=cephfs_mirror_daemon_redistribution=REPRODUCED \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterCephFSMirrorDaemonTopology$' \
  ./internal/integration/experiments/cephfs_mirror_daemon_redistribution
```

## Cleanup and limits

The mirror link, its daemons and both clusters are terminated by their
cleanups; peers and directory policies stay in the discarded clusters. The
MGR hang on a directory removed during a reassignment is a different defect
and is not exercised here.
