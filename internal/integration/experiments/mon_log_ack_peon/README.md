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
- The case's steps share a 15-minute deadline that starts before cluster
  startup. Startup (`harness.NewSingleImageCluster`) runs on the test context,
  so the deadline does not cut it short, but the time it takes is spent from
  the 15 minutes and the steps after it get what remains. The cluster's
  termination in cleanup has its own two-minute deadline, so cleanup is not
  bounded by the deadline and can take the test past 15 minutes. The outer
  bound is the batch's 20-minute `go test` timeout.
- If the OSDs' sessions do not include one with the leader and one with a
  peon, the MON holding most of them is restarted before any measurement until
  they do.

## Tier and triggers

Known-defect reproducer, optional batch `mon_log_ack` with a 20-minute
budget; no automatic run includes it. The default image is Tentacle 20.2.4.
It is an `Experiments` case (`case=mon_log_ack_peon`) without a fixed image:
no release fixes the defect yet. The parser tests in `log_ack_fixture_test.go`
are the `code/pure` batch of this package and run in Code checks for every
change. The triggers, the result checks and the image injection are the
shared ones of
[Running an experiment case](../../../../docs/contributing/testing.md#running-an-experiment-case).

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -run '^TestMONLogAckThroughPeon$' ./internal/integration/experiments/mon_log_ack_peon
```

On another image. No release fixes the defect yet (Umbrella 21.2.0 reproduces it too); Squid 19.2.2,
which predates the introducing change, must print `result=NOT_REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=20m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/mon_log_ack_peon_test.image=ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.2-all" \
  -run '^TestMONLogAckThroughPeon$' ./internal/integration/experiments/mon_log_ack_peon
```

The log line `MON_LOG_ACK ceph=...` records the version the cluster runs.

## Observations and controls

- The leader, election epoch and each OSD's session (MON and `global_id`) are
  read before the window; with `debug_mon 10` and `debug_monc 10` set, each
  OSD writes two cluster log entries (`scrub_purged_snaps`).
- The test commits a unique marker to the cluster log before triggering the
  scrubs. Each OSD's scrub completion must appear after that marker in the
  same log tail, and the leader-connected OSD must be acknowledged. Otherwise the oracle is invalid and
  the test fails as such.
- The window is rejected if the leader, the election epoch or any session
  changed, or if an OSD's MonClient log shows a reset, reopen or hunt, even
  one that returns to the same MON.
- A peon-connected OSD passes only if its peon forwarded its MLog and routed
  the covering MLogAck back under that tid. The peon's "don't have routed
  request" lines are the evidence of the loss.

## Reading the result

The test prints one
[result line](../../../../docs/contributing/testing.md#experiment-result-lines), `case=mon_log_ack_peon`.

- **REPRODUCED:** the leader-connected control is acknowledged, a
  peon-connected OSD has an observed outstanding send, and its peon log
  ties a forwarded request from that OSD to a covering MLogAck discarded
  under the same route ID ("don't have routed request").
- **NOT_REPRODUCED:** every OSD was acknowledged in a stable window, each
  peon-connected one through its own peon.
- **Test failure (no result):** three MONs or OSDs do not start, no layout
  with a leader and a peon session is reached, the entries do not reach the
  cluster log, the leader-connected control is not acknowledged, the window
  is rejected for a leader, epoch or session change, or a peon-connected OSD
  lacks send evidence or a route attributable to its observed outcome.
  A timeout without an attributed discarded acknowledgement leaves the
  outcome inconclusive.

Recorded results using the original oracle (Linux amd64, Docker, aggregate images, one run each). Its discarded-route counts were diagnostic:

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all` (the default image when recorded) | `REPRODUCED`: osd.0 and osd.2 on peons never acknowledged; peons discarded 39 and 12 routed MLogAcks | 104 s |
| Umbrella 21.2.0, `quay.io/ceph/ceph:v21.2.0` | `REPRODUCED`: osd.2 on a peon never acknowledged | 104 s |
| Squid 19.2.2, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.2-all` | `NOT_REPRODUCED` | 46 s |

## Cleanup and limits

Debug levels are set on the daemons of the test's own cluster, which the
harness terminates. Not run: a verified corrected build, a reset after a
backlog to observe retransmission, and the mismatched-fsid defect.
