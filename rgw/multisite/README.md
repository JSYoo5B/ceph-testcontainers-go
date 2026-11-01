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
   the objects through the other zone's endpoint. `ZoneAdmin` runs
   `radosgw-admin` in any zone.
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
