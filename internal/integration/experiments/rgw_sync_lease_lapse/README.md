# RGW data sync passes data log entries after a sync lease lapse

Owner: RGW multisite. Kind: upstream bug reproducer (`native_regression`).
Test: `TestMultiClusterRGWDataSyncAfterSyncLeaseLapse`, optional batch
`rgw_sync_lease_lapse`. The log reader it relies on has Docker-free tests in
the same package (`code/pure`).

## Use case and native contract

A source zone's gateway stops answering for a while and then answers again:
its process is frozen or overloaded, or the path to it drops packets while
the connections stay up. After it answers again, the other zone must sync
everything the source logged in the meantime without a restart of its own
gateway.

Every published image fails this when the source does not answer for longer
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

The test fails on every published image in its last round. That is the
expected result for an unpatched defect; it is not a fixture failure.

## Requirements

- Two clusters, one OSD each, bridge mode only; an RGW realm with zones `a`
  (metadata master, the source) and `b` (`multisite.Run`). Image roles
  `essential`, `client` and `rgw`; `CEPH_TEST_RGW_IMAGE` selects the gateway
  image.
- A Docker engine that can pause containers (the cgroup freezer).
- Fixture gateways run with `rgw_sync_lease_period` 30. The test raises
  `debug_rgw_sync` to 20 on zone b's gateway, whose log it reads, and sets
  `rgw_data_notify_interval_msec` to 0 on zone a's gateway at run time. With
  notifications zone b would sync a notified bucket shard apart from its
  data log position, which hides an entry that the position passed.
- The test bounds itself to 30 minutes; the batch timeout is 40 minutes.

## Tier and triggers

Deliberately failing reproducer: one run takes about 9 minutes (527 s on
Tentacle 20.2.4, 526 s on Umbrella 21.2.0, 553 s on Squid 19.2.5) and fails
in the last round on every published image, after both controls passed. No automatic
run includes it (the default checks of a pull request, pushes to `main`,
periodic samples), and it never gates anything. A maintainer can run it on
demand, also on a pull request's head with the comment below.

- Manual: `gh workflow run "Native regressions" -f batch=rgw_sync_lease_lapse`.
- Comment: `/ci run rgw_sync_lease_lapse` or
  `/ci run rgw_sync_lease_lapse on umbrella`.

## Running it

```sh
CEPH_TEST_RGW_IMAGE=<rgw image@sha256:...> \
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=40m \
  -run '^TestMultiClusterRGWDataSyncAfterSyncLeaseLapse$' \
  ./internal/integration/experiments/rgw_sync_lease_lapse
```

or, exactly as CI runs the batch:

```sh
python3 .github/scripts/tag_scenarios.py optional --batch rgw_sync_lease_lapse \
  --package ./internal/integration/experiments/rgw_sync_lease_lapse \
  --directory "artifacts/rgw-sync-lease-lapse-$(date -u +%Y%m%dT%H%M%SZ)"
```

The runner refuses an existing `--directory`, so the timestamp gives each
run, such as each release of a comparison, its own.

Record the RGW image digest and the gateways' `ceph --version` with the
result.

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
restarted, and the catch-up after the restart is reported with the failure.
A shard behind without that signature fails the test as another fault, and
the gateway is not restarted.

No network fault is used: the freezes are container pauses, and every
connection stays up.

## Reading the result

- **Pass:** a shard found its lease lost inside a batch, and zone b still
  caught up without a restart. No published image passes today.
- **Observed bug:** the failure starts with `observed defect:` and names the
  shards that passed a batch, zone b's missing objects and stored markers
  three minutes after zone a answered again, and whether zone b caught up
  after the restart of its gateway.
- **Not reproduced:** a pass on a build claimed to be affected. The receipt
  (`RGW_SYNC_LEASE_LAPSE_RECEIPT`) names the shards that found the lease
  lost inside a batch; check it before treating the build as fixed.
- **Unavailable prerequisite:** a cluster, the realm or a gateway does not
  start, or the engine cannot pause a container. The test fails before the
  first round.
- **Condition not established:** the failure starts with
  `condition not established`: a shard of zone b read nothing before a
  round, the writes added no data log entry, fewer than 100 shards came to
  wait in a read, a control round's shards did not lose or keep their
  leases as the round requires, or no shard of the round found its lease
  lost inside a batch in round 3. Only 100 of the 128 shards get to read
  while zone a does not answer, so a shard of the round can be among the
  other 28; with six buckets at least one has waited in a read in every run
  so far. It is not a reproduction and not a pass.
- **Control failed:** the failure starts with `control failed`: zone b did
  not catch up after a control round. The trigger was not reached.

## Cleanup and limits

A gateway counts as paused from before the engine is asked to pause it
until the engine reports it as running again, so a request that failed or
whose answer was lost leaves it owed a resume. Gateways still owed one are
resumed before the realm is terminated, and one that stays paused fails the
test. A round goes on only after the engine reported the pause or the
resume it asked for. The realm and the clusters are terminated by their
cleanups. The settings changed at run
time live in the gateway processes and end with them.

Not covered: host networking, arm64, a source that drops packets instead of
not answering (the case in which the defect was first seen), more than one
gateway per zone, full sync, and metadata sync, whose shards use the same
lease but number 64.
