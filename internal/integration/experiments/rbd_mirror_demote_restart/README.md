# RBD mirror demotion copy resumed after a receiver restart

Owner: RBD mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDMirrorDemotionResumesAfterReceiverRestart`, optional batch
`rbd_demote_restart`. The Docker-free judge tests in
`demote_restart_fixture_test.go` (code batch `pure`) run in every Code checks
run.

## Use case and native contract

A planned failover demotes the primary image; the receiver (`rbd-mirror`)
copies the demotion snapshot to the destination. When the receiver is killed
during that copy and its container is started again, the same snapshot must
complete, the destination must hold the primary's exact bytes and must then be
promotable without force. The replica of a demoted primary that was removed
while the receiver was down must be removed too.

Two native defects are involved:

1. Squid 19.2.3 (and Tentacle 20.2.0) classify an incomplete non-primary
   demotion snapshot as orphaned (`calc_promotion_state` in
   `src/librbd/mirror/GetInfoRequest.cc`), so the restarted receiver never
   resumes the copy. 19.2.4, 20.2.1 and later resume it.
2. On every qualified image that resumes (19.2.4, 19.2.5, 20.2.4, 21.2.0) the
   resumed copy reports complete while the object numbered
   `last_copied_object_number` at the kill keeps the previous snapshot's
   bytes. An ordinary mirror snapshot copy shows the same loss, so it is not
   specific to demotion. No upstream issue records this one yet.

Upstream references for the first defect:

- [tracker 73528](https://tracker.ceph.com/issues/73528), fix
  [ceph/ceph#65953](https://github.com/ceph/ceph/pull/65953), Squid backport
  [ceph/ceph#66165](https://github.com/ceph/ceph/pull/66165), Tentacle
  backport [ceph/ceph#66164](https://github.com/ceph/ceph/pull/66164).

Every published image that resumes the copy reproduces the second defect at
the exact-bytes step. That is the expected result for an unpatched defect; it
is not a fixture failure.

## Requirements

- Two clusters, one OSD each, bridge mode only; forward and reverse
  `rbdmirror.Run` links in snapshot mode. Image roles `essential`, `client`
  and `rbd-aux`, all of one release.
- The netem helper image (`CEPH_TEST_NETEM_IMAGE`, default
  `fault.NetemHelperImage`) limits only the source OSD's packets to the
  forward receiver to 2 Mbit/s, so each 16 MiB image (256 KiB objects) takes
  about a minute to copy.
- The test bounds itself to 25 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Unpatched-defect reproducer: one run takes about 7 minutes and
reproduces at the exact-bytes check on every published image. It passes on
the 20.2.4 role images, whose verdicts its catalogue lists, and fails on
images the catalogue does not list. No automatic run includes it
(the default checks of a pull request, pushes to `main`, periodic samples),
and it never gates anything. A maintainer can run it on demand, also on a pull
request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rbd_demote_restart`.
- Comment: `/ci run rbd_demote_restart` or `/ci run rbd_demote_restart on squid`.

## Running it

```sh
CEPH_TEST_IMAGE=<aggregate image@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestRBDMirrorDemotionResumesAfterReceiverRestart$' \
  ./internal/integration/experiments/rbd_mirror_demote_restart
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rbd_demote_restart \
  --package ./internal/integration/experiments/rbd_mirror_demote_restart \
  --directory "artifacts/rbd-demote-restart-$(date -u +%Y%m%dT%H%M%SZ)"
```

With the 20.2.4 role images (`essential`, `client`, `rbd-aux`) exported as
in [Choosing images](../../../../docs/contributing/testing.md#choosing-images)
instead of `CEPH_TEST_IMAGE`, the same commands check both reproducers
against the combination their catalogue lists (see [Verdict](#verdict)).

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Record the image digest and the `ceph --version` source SHA with the result;
`harness.StaticFacts` logs the versions the cluster runs.

## Observations and controls

1. Control: both images read back exactly on the destination before any
   fault.
2. Condition: the newest destination snapshot of each image is
   `type=mirror state=demoted complete=false` with a `primary_mirror_uuid`,
   observed before and again after the SIGKILL of the same receiver
   container. Without this the trigger is not established.
3. While the receiver is down the demoted `removed` primary is force-disabled
   and deleted, the rate limit is lifted and the same container is started.
4. Effects: the same snapshot ID completes with the same `primary_snap_id`
   (observation `resume`); the `resumed` image exports the source's exact
   bytes (observation `exact-bytes`); the destination `removed` image
   disappears; non-forced promotion succeeds; the next write and snapshot
   replicate back through the reverse link. The last three run only when
   both observations found their symptom absent, and a failure there fails
   the test whatever the verdicts.

On a byte mismatch the observation classifies every 256 KiB object as new,
old or neither, so the skipped object number is in the verdict line.

## Verdict

The test reports two units, one per defect, each in its own
`REPRODUCTION_VERDICT` line, so a combination expects a verdict of each:

- `rbd_mirror_demote_restart/demotion-resume` (defect 1, observation
  `resume`). **REPRODUCED:** the restarted receiver did not complete the same
  demotion copy within five minutes (19.2.3: status `up+unknown`, "remote
  image is not primary"). **NOT_REPRODUCED:** the copy completed with the
  same `primary_snap_id` (19.2.4, 20.2.4 and later).
- `rbd_mirror_demote_restart/resumed-bytes` (defect 2, observation
  `exact-bytes`). **REPRODUCED:** the copy completed, but whole objects keep
  the previous snapshot's bytes, in the recorded runs exactly one, at the
  `last_copied_object_number` of the kill (every published image that
  resumes). **NOT_REPRODUCED:** the destination exports the source's exact
  bytes. When `demotion-resume` is REPRODUCED this unit has no verdict, since
  no resumed copy exists.

**INCONCLUSIVE** names the category. Unavailable prerequisite: the netem
helper or the `rbd-aux` role is missing, or a cluster or link does not start
(the test ends before a verdict). Condition not established: the copy was
already complete, or not yet started, at the kill (step 2); a timeout there
is not a reproduction. Another fault: the demotion copy disappeared or
changed its primary snapshot, or the export has another size or an object
that holds neither the old nor the new bytes.

The expected verdicts come from [`reproductions.json`](reproductions.json)
beside the test. The combination whose platform and images equal the ones
the run resolves selects each unit's verdict, and only that verdict passes;
a skip or `INCONCLUSIVE` never does. It lists `20.2.4-roles` (linux/amd64)
with `NOT_REPRODUCED` for `demotion-resume` and `REPRODUCED` for
`resumed-bytes`. On any other images the run is an investigation
(`expected=none combination=none`): a reproduction of either defect fails
the test, absence of both lets the remaining effects decide, and an
inconclusive run fails it.

## Cleanup and limits

The network profile lease is restored before the restart; the links,
clusters and helper containers are terminated by their cleanups. Not run:
arm64, Tentacle 20.2.0/20.2.1 (source boundary only), a graceful stop,
journal mode, object-map/fast-diff images, and the promotion and reverse
replay steps after a correct resume.
