# MON log acknowledgement through a peon

Owner: MON. Kind: upstream bug reproducer (`native_regression`).
Test: `TestMONLogAckThroughPeon`, optional batch `mon_log_ack`. The
Docker-free parser tests in `log_ack_fixture_test.go` (code batch `pure`) run
in every Code checks run.

## Use case and native contract

A daemon's LogClient sends its cluster log entries in an MLog to the MON it
has a session with and trims its queue when the matching MLogAck arrives. A
peon forwards the MLog to the leader, which commits the entries and routes the
MLogAck back through the peon. Every OSD must be acknowledged, whether its
session is with the leader or with a peon.

The defect: a leader that also calls `no_reply` for a forwarded MLog first
routes an empty reply. The peon erases its routed request and then discards
the MLogAck that follows ("don't have routed request"), so a peon-connected
daemon's queue is never trimmed. Squid 19.2.2 does not do this; 19.2.3, 19.2.5,
20.2.0 and 20.2.4 do.

Upstream references:

- Regression report [tracker 81604](https://tracker.ceph.com/issues/81604),
  duplicate [tracker 81511](https://tracker.ceph.com/issues/81511) (Squid
  boundary 19.2.2 to 19.2.3).
- Introducing change [ceph/ceph#61933](https://github.com/ceph/ceph/pull/61933),
  Squid backport [ceph/ceph#62213](https://github.com/ceph/ceph/pull/62213)
  ([tracker 70380](https://tracker.ceph.com/issues/70380)).
- Proposed correction [ceph/ceph#72555](https://github.com/ceph/ceph/pull/72555),
  not merged when this case was qualified. Do not call it a released fix.
- [tracker 54489](https://tracker.ceph.com/issues/54489), the original
  mismatched-fsid forwarded-request leak, is a different defect.

## Requirements

- One cluster: 3 MONs, 3 OSDs, one MGR, image role `essential` only.
- The test bounds itself to 15 minutes; the batch timeout is 20 minutes.
- If the OSDs' sessions do not include one with the leader and one with a
  peon, the MON holding most of them is restarted before any measurement until
  they do.

## Running it

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:v19.2.2 \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestMONLogAckThroughPeon$' ./internal/integration/experiments/mon_log_ack_peon
```

or through the tag runner:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch mon_log_ack \
  --package ./internal/integration/experiments/mon_log_ack_peon \
  --directory artifacts/mon-log-ack
```

The log line `MON_LOG_ACK ceph=...` records the version the cluster runs; pin
the image by digest when the result is evidence.

## Observations and controls

- The leader, election epoch and each OSD's session (MON and `global_id`) are
  read before the window; with `debug_mon 10` and `debug_monc 10` set, each
  OSD writes two cluster log entries (`scrub_purged_snaps`).
- Positive control: the entries reach the cluster log, and the
  leader-connected OSD is acknowledged. Otherwise the oracle is invalid and
  the test fails as such.
- The window is rejected if the leader, the election epoch or any session
  changed, or if an OSD's MonClient log shows a reset, reopen or hunt, even
  one that returns to the same MON.
- A peon-connected OSD passes only if its peon forwarded its MLog and routed
  the covering MLogAck back under that tid. The peon's "don't have routed
  request" lines are the evidence of the loss.

## Reading the result

- **Pass:** every OSD was acknowledged in a stable window (19.2.2).
- **Observed bug:** the leader-connected control is acknowledged, the
  peon-connected OSDs are not, and the peon logs a discarded route (20.2.4).
- **Not reproduced:** a pass on a build that carries the introducing change.
- **Unavailable prerequisite:** three MONs or OSDs do not start, or no layout
  with a leader and a peon session is reached.
- **Condition not established:** the leader-connected control is not
  acknowledged, or the window is rejected for a leader, epoch or session
  change. A plain timeout is not a reproduction.

## Cleanup and limits

Debug levels are set on the daemons of the test's own cluster, which the
harness terminates. Not run: a verified corrected build, a reset after a
backlog to observe retransmission, and the mismatched-fsid defect.
