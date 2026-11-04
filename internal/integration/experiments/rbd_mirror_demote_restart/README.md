# RBD mirror demotion copy resumed after a receiver restart

Owner: RBD mirroring. Kind: upstream bug reproducer (`native_regression`).
Test: `TestRBDMirrorDemotionResumesAfterReceiverRestart`, optional batch
`rbd_demote_restart`.

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

The test fails on every published image at the exact-bytes step. That is the
expected result for an unpatched defect; it is not a fixture failure.

## Requirements

- Two clusters, one OSD each, bridge mode only; forward and reverse
  `rbdmirror.Run` links in snapshot mode. Image roles `essential`, `client`
  and `rbd-aux`, all of one release.
- The netem helper image (`CEPH_TEST_NETEM_IMAGE`, default
  `fault.NetemHelperImage`) limits only the source OSD's packets to the
  forward receiver to 2 Mbit/s, so each 16 MiB image (256 KiB objects) takes
  about a minute to copy.
- The test bounds itself to 25 minutes; the batch timeout is 30 minutes.

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
  --directory artifacts/rbd-demote-restart
```

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
4. Effects: the same snapshot ID completes with the same `primary_snap_id`;
   the destination `removed` image disappears; the `resumed` image exports
   the source's exact bytes; non-forced promotion succeeds; the next write and
   snapshot replicate back through the reverse link.

On a byte mismatch the failure message classifies every 256 KiB object as
new, old or neither, so the skipped object number is in the log.

## Reading the result

- **Pass:** every effect above held. No published image passes today.
- **Observed bug, defect 1:** the copy does not complete after the restart
  (19.2.3: status `up+unknown`, "remote image is not primary").
- **Observed bug, defect 2:** the copy completes, but the exact-bytes check
  fails with exactly one old object at the recorded progress number.
- **Not reproduced:** a pass on a build claimed to be affected. Check that the
  condition in step 2 was logged before treating it as fixed.
- **Unavailable prerequisite:** the netem helper or the `rbd-aux` role is
  missing, or a cluster or link does not start. The test fails before the
  condition is observed.
- **Condition not established:** the copy was already complete, or not yet
  started, at the kill. The test fails at step 2; a timeout there is not a
  reproduction.

## Cleanup and limits

The network profile lease is restored before the restart; the links,
clusters and helper containers are terminated by their cleanups. Not run:
arm64, Tentacle 20.2.0/20.2.1 (source boundary only), a graceful stop,
journal mode, object-map/fast-diff images, and the promotion and reverse
replay steps after a correct resume.
