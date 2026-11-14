# RGW data sync passes data log entries after a sync lease lapse

Owner: RGW multisite. Kind: upstream bug reproducer (`native_regression`).
Test: `TestMultiClusterRGWDataSyncAfterSyncLeaseLapse`, optional batch
`rgw_sync_lease_lapse`. The log reader it relies on and its reading of a
gateway container's state have Docker-free tests in the same package
(`code/pure`).

## Use case and native contract

A source zone's gateway stops answering for a while and then answers again:
its process is frozen or overloaded, or the path to it drops packets while
the connections stay up. After it answers again, the other zone must sync
everything the source logged in the meantime without a restart of its own
gateway.

Every published image loses entries when the source does not answer for longer
than `rgw_sync_lease_period`. Two parts of RGW combine:

1. A coroutine manager runs no other coroutine while 100 of its coroutines
   wait for I/O (`RGW_ASYNC_OPS_MGR_WINDOW`, `RGWCoroutinesManager::run`):
   it only takes completions until one of the waits ends. A zone's data
   sync from one source has 128 data log shards, each of which reads the
   source's data log every `rgw_data_sync_poll_interval` (20 seconds). When
   the source does not answer, 100 of those reads wait within one interval,
   and from then on the destination's gateway starts no further read and
   renews none of the shards' sync leases, although the timers of the
   renewals expire.
2. Each shard holds a lease (`RGWContinuousLeaseCR`) that counts as held
   only while its last renewal is younger than the lease period.
   `RGWDataIncSyncShardCR` checks it at the start of a pass, where a lost
   lease ends the shard's incremental sync and the shard starts again from
   its stored marker, and inside the loop over a batch of entries it read
   from the source's data log, where a lost lease only leaves the loop. The
   code after the loop still moves the shard's in-memory marker to the last
   entry of the batch, and the pass ends as if the batch had been handled.

When the source answers again, the shards that waited in a read get their
batch before their lease is renewed (a renewal is a round trip to the
cluster), find the lease lost inside the batch and never start its entries.
The next pass finds the lease held again and reads from the moved marker.
The marker stored in the zone stays where it was, so `radosgw-admin sync
status` reports the shard behind without end, the error list stays empty,
and the bucket shards of the batch are synced only when the gateway reads
the stored marker again (a restart) or a later entry for the same bucket
shard arrives.

Both parts are the same in Squid 19.2.5, Tentacle 20.2.4 and Umbrella
21.2.0 (`src/rgw/rgw_coroutine.cc`, `src/rgw/driver/rados/rgw_data_sync.cc`,
`src/rgw/driver/rados/rgw_cr_rados.cc`). No upstream issue or fix is
recorded for it. With the default lease period of 120 seconds the source
must stay unanswering that long; fixture gateways run with 30.

Every published image reproduces it in the test's last round.

## Requirements

- Two clusters, one OSD each, bridge mode only; an RGW realm with zones `a`
  (metadata master, the source) and `b` (`multisite.Run`). Both clusters
  and both gateways run the package's `image`, one aggregate image.
- A Docker engine that can pause containers (the cgroup freezer).
- Fixture gateways run with `rgw_sync_lease_period` 30. The test raises
  `debug_rgw_sync` to 20 on zone b's gateway, whose log it reads, and sets
  `rgw_data_notify_interval_msec` to 0 on zone a's gateway at run time. With
  notifications zone b would sync a notified bucket shard apart from its
  data log position, which hides an entry that the position passed.
- The test bounds itself to 30 minutes; the batch timeout is 40 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps (the
realm, the seeds, both control rounds and the trigger's condition) fail the
test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rgw_sync_lease_lapse result=<REPRODUCED|NOT_REPRODUCED>`,
and passes either way. No automatic run includes it (the default checks of a
pull request, pushes to `main`, periodic samples), and it never gates
anything. A maintainer can run it on demand, also on a pull request's head
with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_sync_lease_lapse`.
- Comment: `/ci run rgw_sync_lease_lapse` or
  `/ci run rgw_sync_lease_lapse on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=40m \
  -run '^TestMultiClusterRGWDataSyncAfterSyncLeaseLapse$' \
  ./internal/integration/experiments/rgw_sync_lease_lapse
```

On another image, such as a patched build, injected into the package's
`image` variable; a fixed image must print `result=NOT_REPRODUCED`. No
release fixes the defect yet.

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=40m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_sync_lease_lapse_test.image=<image>" \
  -run '^TestMultiClusterRGWDataSyncAfterSyncLeaseLapse$' \
  ./internal/integration/experiments/rgw_sync_lease_lapse
```

The role image variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RGW_IMAGE`, ...) do
not select the case's images.

## Observations and controls

Six buckets are created through zone a and seeded; zone b serves the seeds
before the first round. Before a round every one of zone b's 128 shards
must have read zone a's data log anew. Each round writes one new object per
bucket to zone a while zone b's gateway is frozen, and reads from zone a's
`datalog status` which data log shards got an entry: those are the round's
shards. A round is caught up when zone b serves every object of the round
and its stored marker of every shard of the round is zone a's
(`data sync status --source-zone=a`). The gateway processes must be the
ones the test started with.

1. Control `idle`: zone b's gateway stays frozen for 36 seconds, past the
   lease period, while its shards are idle and zone a answers. Condition:
   every shard of the round found its lease lost before a read and not
   inside a batch. Effect: zone b catches up within two minutes. A lapsed
   lease alone does not lose entries.
2. Control `short`: zone a's gateway is frozen right after the writes and
   zone b's is resumed. Once 100 of zone b's shards wait in a read of zone
   a's data log, zone a's gateway stays frozen for 8 more seconds and is
   resumed. Condition: no shard of the round lost its lease, and at least
   one waited in a read over that time. Effect: zone b catches up within two
   minutes. A source that does not answer for less than the lease period
   does not lose entries.
3. Trigger `lapse`: the same as `short` with 36 seconds. Condition: at
   least one shard of the round found its lease lost inside a batch. Effect
   required: zone b catches up within three minutes.

When round 3 does not catch up, every shard still behind must show the
skipped batch in the gateway's log: the lease lost inside a batch, later
reads from zone a's newest marker, no entry of that marker ever started,
and a stored marker that is not zone a's. Only then is zone b's gateway
restarted, and the catch-up after the restart is reported with the symptom.
A shard behind without that signature fails the test as another fault, and
the gateway is not restarted.

No network fault is used: the freezes are container pauses, and every
connection stays up.

## Reading the result

- **REPRODUCED:** the symptom names the shards that passed a batch, zone
  b's missing objects and stored markers three minutes after zone a answered
  again, and whether zone b caught up after the restart of its gateway.
- **NOT_REPRODUCED:** a shard found its lease lost inside a batch, and zone
  b still caught up without a restart. The receipt
  (`RGW_SYNC_LEASE_LAPSE_RECEIPT`) names those shards; check it before
  treating the build as fixed.
- **Test failure (no result):**
  - `condition not established`: a shard of zone b read nothing before a
    round, the writes added no data log entry, fewer than 100 shards came to
    wait in a read, a control round's shards did not lose or keep their
    leases as the round requires, no shard of the round found its lease lost
    inside a batch in round 3, or a gateway container stopped while a round
    paused or resumed it. Only 100 of the 128 shards get to read while zone a
    does not answer, so a shard of the round can be among the other 28; with
    six buckets at least one has waited in a read in every run so far.
  - `control failed`: zone b did not catch up after a control round. The
    trigger was not reached.
  - Zone b did not catch up after round 3 and a shard behind does not show
    the skipped batch (another fault), or a cluster, the realm or a gateway
    does not start, or the engine cannot pause a container.

Native results (Linux amd64 host, Docker, aggregate image, one run):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7…` (the default image when recorded) | `REPRODUCED`: shards 2, 18, 34 and 66 passed a batch on a lost lease; four buckets' objects missing 3 minutes after zone a answered again; caught up 36 s after a restart of zone b's gateway | 528 s |

No fixed image exists to run.

Earlier, failing-test runs took about 9 minutes and reproduced on Tentacle
20.2.4 (527 s), Umbrella 21.2.0 (526 s) and Squid 19.2.5 (553 s), after both
controls passed.

## Cleanup and limits

A gateway counts as paused from before the engine is asked to pause it
until the engine reports its container as not paused, so a request that
failed or whose answer was lost leaves it owed a resume. Gateways still owed
one are resumed before the realm is terminated, and one that stays paused
fails the test. A round goes on only with a gateway container the engine
reports as running, paused after a pause and not paused after a resume. A
container that stopped is not frozen and is owed nothing, but the round
fails with `condition not established`: that gateway neither stays silent
with its connections up nor answers again. The realm and the clusters are
terminated by their cleanups. The settings changed at run time live in the
gateway processes and end with them.

Not covered: host networking, arm64, a source that drops packets instead of
not answering (the case in which the defect was first seen), more than one
gateway per zone, full sync, and metadata sync, whose shards use the same
lease but number 64.
