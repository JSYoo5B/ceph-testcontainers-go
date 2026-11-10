# Ceph RGW multisite

Import `github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite` to build an RGW
multisite realm across independent disposable clusters, so that a test can
check how its S3 application behaves with replication: objects written in one
zone appearing in another, a zone outage, a planned metadata master failover,
selective sync with sync policies. Ordinary gateway use (users, tenants,
quotas) stays in [`rgw`](../README.md), accounts in
[`rgw/account`](../account/README.md) and placement in
[`rgw/placement`](../placement/README.md). See
[API purpose and purpose tags](../../docs/guides/api-purpose.md).

## Order of use

1. **Start fresh clusters** with `ceph.Run`, without gateways or RGW
   configuration, all in the same network mode.
2. **Build the realm** with `multisite.Run(ctx, img,
   multisite.WithConfig(cfg))` for two zones (`Source` is the metadata master;
   both accept writes) or `multisite.WithTopology(topology)` for more zones and
   zonegroups. `img` is the gateway image. `AddZone`, `AddZonegroup` and
   `RemoveZone` change the topology later.
3. **Hand the endpoints to the SUT**: each `Zone`'s `rgw.Gateway`
   (`Source`, `Destination`, `Zones()`) gives `S3Endpoint`, `Region` and the
   test user's `AccessKey` / `SecretKey`. Create users through the master.
4. **Shape replication** when the scenario needs it: `CreateSyncGroup`, then
   `SyncGroup.CreateFlow` / `CreatePipe` / `SetStatus` and `SyncGroup.Apply`
   for zonegroup policy; `InterruptZoneLink` and `ApplyZoneLinkProfile` cut or
   slow the link between zones; `PullSourcePeriod` / `PullDestinationPeriod`
   carry a planned master change.
5. **Observe and verify**: `WaitSyncReady` (metadata and data catch-up),
   `SyncGroup.WaitBucketPolicyReady`, `SyncGroup.WaitBucketSyncReady`, then read
   the objects through the other zone's endpoint. `WatchSync` and
   `SyncGroup.WatchBucketSync` report lag and catch-up while a fault runs;
   `SyncGroup.WatchBucketPolicy` reports the stages of one policy import.
   `ZoneAdmin` runs `radosgw-admin` in any zone.
6. **Clean up**: terminate the fixture before the clusters; a non-nil
   `*Fixture` returned with an error must still be terminated.

```golang
ms, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithConfig(multisite.Config{
	Source: source, Destination: destination,
	SourceZone: "east", DestinationZone: "west",
}))
if ms != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := ms.Terminate(ctx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}
eastEndpoint, err := ms.Source.S3Endpoint(ctx)
if err != nil {
	t.Fatal(err)
}
// Write through east, then wait for metadata in west before writing objects
// into a new bucket, and poll west for the objects.
```

## Watching sync

To see a zone fall behind and catch up while a fault runs,
`ms.WatchSync(ctx, cfg, "west", "east")` polls the same observation as
`SyncStatus`, and `group.WatchBucketSync(ctx, cfg, "pipe", "east", "west")`
the same as `BucketSyncStatus`. Each sends changes on one channel, 5 seconds
apart by default; one read takes over a second. Receive the baseline before
you stop a gateway or cut the zone link. A zone event's `Ready` is what `WaitSyncReady` waits for, for the
selected sources only; a bucket event's `Status.CaughtUp` is what
`WaitBucketSyncReady` waits for.

Each read takes the fixture's topology lock and releases it before the event
is sent, so the test can stop gateways, fault the link or read other status
while the watch runs. A failed read, a gateway that cannot be reached and a
zone that has not imported the current period are `WatchGap` events that
keep the partial report.

The baseline pins what the watch observes, and these end it instead of
reading as progress:

- a new committed period or realm epoch, which any sync policy or topology
  commit makes; start a new watch for the new period;
- for a bucket watch, a bucket recreated under the same name, and the group
  changed through the fixture or outside it.

`group.WatchBucketPolicy(ctx, cfg, "west")` reports the stages of the read
`WaitBucketPolicyReady` makes, 2 seconds apart by default: the committed
period, the group's policy and the referenced bucket instances imported into
the zone, and `Status.Imported` once all three are. Start it after the policy
change; it ends with a nil cause after it has sent the imported status, and an
imported baseline is its only event. A referenced bucket that the zone has not
imported yet is that stage, `BucketsImported` false, in a successful read, so
the watch starts however long the zone's metadata sync takes; a read that
fails is retried within `QueryTimeout` for the baseline and is a `WatchGap`
later. The watch keeps the committed period, the group's
policy, the zone and the bucket instances it captured: a new committed period,
a changed group, a group removed after the watch started, a detached zone and
a recreated bucket end it.

A bucket group's `Remove` takes effect without a commit, and the destination
imports it as any other change of the bucket's metadata. Call
`WaitBucketPolicyReady` or `WatchBucketPolicy` on the removed handle to wait
for that import: it is complete when the destination's instance of the
group's bucket no longer carries the group. The destination must be one the
group's pipes selected. Every policy import, a removal included, also needs
the destination's instance at an object version (the metadata `ver`) no older
than the master's when the observation began. A destination snapshot from
before the group was created has no group either, and only that version tells
the two apart. Write objects that the removal must exclude only
after that.

`Terminate` ends a watch with `ceph.ErrInvalidRequest`, so stop it first. No
watch proves that a particular object arrived: compare the objects through
the other zone. The lifecycle that every Watch shares (baseline, gaps,
overflow and `Stop`) is in
[watching state](../../docs/guides/contexts-and-cleanup.md#watching-state).

## Incremental sync timing

On qualified Ceph 20.2.4 gateways, opt in before the realm starts:

```golang
multisite.WithConfig(multisite.Config{
    Source: source, Destination: destination,
    Timing: multisite.TimingProfile{IncrementalSync: true},
})
```

The same `Timing` field is available in `TopologyConfig`. It sets
`rgw_meta_sync_poll_interval` and `rgw_data_sync_poll_interval` to one second,
instead of the twenty-second caught-up default, as arguments of the owned
gateways, including later zones. Native notifications can wake these loops
earlier, so the saving per operation is not fixed. Default fixtures keep native timing. The command-line values take
precedence over central and local configuration; the gateway lifetime owns the
condition, so termination ends it without a persistent configuration override.
A partial fixture returned with an error still needs termination.

This targets already-caught-up incremental polling. Keep metadata readiness and
client byte checks; bootstrap, new-bucket initial sync, lease ownership, failure
backoff, retries, readiness timeouts and fault windows are separate
conditions. See [Fixture daemon timing](../../docs/guides/daemon-timing.md)
for what every timing profile shares.

## Sync lease period

Fixture gateways start with `rgw_sync_lease_period` 30 instead of the native
120 seconds, so that sync resumes soon after a gateway restart: a killed
gateway's sync shard leases stay taken until they expire. A gateway renews
each lease every half period and counts it as lost after one period without
a renewal, and it renews none while its reads of a source zone that does not
answer wait. A zone link cut or a paused source gateway that lasts longer
than about 15 seconds can therefore cost a zone's shards their leases, and
with them reach the native defect described under
[known release-specific issues](../../docs/guides/releases-and-images.md#known-release-specific-issues),
which leaves the zone behind until its gateway restarts.

A test that keeps a zone from answering for longer selects a period of more
than twice that time, counted until the gateway's connections work again
(TCP retransmits after a cut can take as long again as the cut):

```golang
multisite.WithTopology(multisite.TopologyConfig{
    Zones:  zones,
    Timing: multisite.TimingProfile{SyncLeasePeriod: 5 * time.Minute},
})
```

The period is a whole number of seconds, at least 30, on every supported
release; it is an argument of the owned gateways, including later zones, and
ends with them. Sync after a restart of such a gateway can stall for one
period. `IncrementalSync` and `SyncLeasePeriod` are independent.

## Concepts and limits

- Both zones accept writes and deletes replicate too, so a replica is not a
  backup. Users and buckets are realm metadata owned by the metadata master: a
  bucket created through another zone is forwarded to the master, which must be
  reachable.
- Replication is asynchronous: allow minutes, especially after a gateway
  restart (sync leases must expire first). Wait for a new bucket's metadata in
  every zone before writing objects into it. Resolve `S3Endpoint` again after
  any gateway restart.
- To test a zone outage, stop one gateway, keep working through the other,
  start it again and wait with `WaitSyncReady`.
- A gateway restart in one zone, including `placement.Placement.Reload` and a
  stopped zone, interrupts the other zones' data sync with it. On Tentacle
  20.2.4 the peer gateway's data-sync thread has intermittently crashed with
  SIGSEGV (exit 139, not OOM) about one sync backoff after such an outage. This
  is a crash in RGW itself. The fixture does not restart the crashed gateway or
  repeat requests, so writes through it fail; check the gateway's state before
  blaming the restarted zone.
- The fixture owns its gateways, setup clients and the HTTP bridge between
  zones; `Terminate` leaves realm, zone configuration and data in the clusters.
  Do not edit periods or zones while topology changes or `SyncGroup.Apply` run.
  Serialize these period writers with `placement.Placement.Apply` in the same
  realm. If a placement update or commit is uncertain, retry the original
  placement handle successfully before publishing through this fixture.
- Sync policy: the fixture creates fresh groups only and checks every mutation
  against the native policy. On Ceph 20.2.4 pipe `Priority` between overlapping
  pipes and source-side denial in user mode with an ordinary user have no
  reliable effect, and S3 `PutBucketReplication` rejects account-owned requests
  with `NotImplemented`. IAM non-root principals and cross-account or
  cross-tenant principal translation are refused by the helpers.
- Releases: Tentacle 20.2.4 is the default. On Squid use 19.2.5 (19.2.6 cannot
  set up multisite); Squid replicates nothing for a user-mode pipe with an
  account root user, which `CreatePipe` refuses with `ceph.ErrUnsupported`.
- Out of scope: automatic failover, request routing between zones, recovery
  point guarantees and realistic WAN models. A zone link can still be delayed
  or made lossy in one direction with `ApplyZoneLinkProfile`, or cut with
  `InterruptZoneLink`.

## Reference

- API: [pkg.go.dev](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite)
  or `go doc -all github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite`.
- `ZoneAdmin` and RGW commands:
  [Native commands](../../docs/guides/native-commands.md#rgw-administration);
  zone links: [Fault injection](../../docs/guides/fault-injection.md);
  [Releases and images](../../docs/guides/releases-and-images.md);
  the Ceph [multisite sync policy](https://docs.ceph.com/en/tentacle/radosgw/multisite-sync-policy/)
  documentation for the native semantics.
